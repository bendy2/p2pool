#!/usr/bin/env python3
import grpc
import sys
import os
from datetime import datetime
import argparse
import json
import logging
import re
import threading
import time
from queue import Queue
import psycopg2
from psycopg2 import pool

# 导入 TARI gRPC 生成的代码
try:
    from tari.wallet_grpc import wallet_pb2
    from tari.wallet_grpc import wallet_pb2_grpc
except ImportError:
    print("错误：找不到 TARI gRPC 模块。请确保已经生成 gRPC 代码。")
    sys.exit(1)

# 创建日志记录器
logger = logging.getLogger('monitor')
logger.setLevel(logging.INFO)

# 创建文件处理器
file_handler = logging.FileHandler('monitor.log')
file_handler.setLevel(logging.INFO)

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 创建格式化器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 添加处理器到记录器
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 从配置文件加载数据库配置
def load_config():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            return config.get('database', {})
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        raise

# 数据库配置
DB_CONFIG = load_config()

# 全局数据库连接池
db_pool = None

class TariWalletTest:
    def __init__(self, grpc_address="127.0.0.1:18143"):
        self.channel = grpc.insecure_channel(grpc_address)
        self.stub = wallet_pb2_grpc.WalletStub(self.channel)

    def test_connection(self):
        """测试与钱包的连接"""
        try:
            # 获取钱包信息
            request = wallet_pb2.GetIdentityRequest()
            response = self.stub.GetIdentity(request)
            print("钱包连接成功！")
            print(f"钱包公钥: {response.public_key}")
            return True
        except grpc.RpcError as e:
            print(f"gRPC 连接错误: {e}")
            return False

    def get_balance(self):
        """获取钱包余额"""
        try:
            request = wallet_pb2.GetBalanceRequest()
            response = self.stub.GetBalance(request)
            print(f"可用余额: {response.available_balance}")
            print(f"待确认余额: {response.pending_incoming_balance}")
            return response.available_balance
        except grpc.RpcError as e:
            print(f"获取余额失败: {e}")
            return None

    def test_get_address(self):
        """测试获取接收地址"""
        try:
            request = wallet_pb2.GetNewAddressRequest()
            response = self.stub.GetNewAddress(request)
            print(f"新生成的接收地址: {response.address}")
            return response.address
        except grpc.RpcError as e:
            print(f"获取地址失败: {e}")
            return None

class LogMonitorThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.running = True
        self.log_queue = Queue()
        self.log_file = './p2pool.log'
        
        # 编译正则表达式模式
        self.xmr_block_pattern = re.compile(r'got a payout of ([\d.]+) XMR in block (\d+)')
        self.tari_block_pattern = re.compile(r'Mined Tari block ([a-f0-9]+) at height (\d+)')
        
    def run(self):
        try:
            # 打开日志文件
            with open(self.log_file, 'r') as f:
                # 移动到文件末尾
                f.seek(0, 2)
                
                while self.running:
                    line = f.readline()
                    if not line:
                        # 如果没有新内容，等待一小段时间
                        time.sleep(0.1)
                        continue
                        
                    # 处理日志行
                    self.process_log_line(line)
                    
        except Exception as e:
            logger.error(f"日志监控线程错误: {str(e)}")
            
    def process_log_line(self, line):
        try:
            # 检查 XMR 爆块信息
            xmr_match = self.xmr_block_pattern.search(line)
            if xmr_match:
                reward = float(xmr_match.group(1))
                height = int(xmr_match.group(2))
                logger.info(f"检测到 XMR 爆块 - 高度: {height}, 奖励: {reward}")
                handle_xmr_block({'height': height, 'reward': reward})
                return
                
            # 检查 TARI 爆块信息
            tari_match = self.tari_block_pattern.search(line)
            if tari_match:
                height = int(tari_match.group(2))
                block_id = tari_match.group(1)
                logger.info(f"检测到 TARI 爆块 - 高度: {height}, 区块ID: {block_id}")
                handle_tari_block({'height': height, 'block_id': block_id})
                
        except Exception as e:
            logger.error(f"处理日志行时出错: {str(e)}")
            
    def stop(self):
        self.running = False

def init_db():
    """初始化数据库连接池"""
    global db_pool
    try:
        db_pool = pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=20,
            **DB_CONFIG
        )
        logger.info("Successfully connected to database")
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def handle_xmr_block(block_data):
    """处理 XMR 区块数据"""
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            # 检查区块是否已存在
            cur.execute("""
                SELECT id FROM blocks 
                WHERE block_height = %s AND type = 'xmr'
            """, (block_data['height'],))
            
            if cur.fetchone():
                logger.info(f"XMR 区块 {block_data['height']} 已存在")
                return
                
            # 插入新区块
            cur.execute("""
                INSERT INTO blocks (block_height, block_id, type, check_status, is_valid, rewards)
                VALUES (%s, %s, 'xmr', true, true, 0)
                RETURNING id
            """, (block_data['height'], str(block_data['height'])))
            
            conn.commit()
            logger.info(f"XMR 区块 {block_data['height']} 已记录")
            
    except Exception as e:
        logger.error(f"处理 XMR 区块时出错: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            db_pool.putconn(conn)

def handle_tari_block(block_data):
    """处理 TARI 区块数据"""
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            # 检查区块是否已存在
            cur.execute("""
                SELECT id FROM blocks 
                WHERE block_height = %s AND type = 'tari'
            """, (block_data['height'],))
            
            if cur.fetchone():
                logger.info(f"TARI 区块 {block_data['height']} 已存在")
                return
                
            # 插入新区块
            cur.execute("""
                INSERT INTO blocks (block_height, block_id, type, check_status, is_valid, rewards)
                VALUES (%s, %s, 'tari', false, false, 0)
                RETURNING id
            """, (block_data['height'], block_data['block_id']))
            
            conn.commit()
            logger.info(f"TARI 区块 {block_data['height']} 已记录")
            
    except Exception as e:
        logger.error(f"处理 TARI 区块时出错: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            db_pool.putconn(conn)

def main():
    """主函数"""
    try:
        # 初始化数据库连接
        init_db()
        
        # 创建并启动日志监控线程
        log_monitor = LogMonitorThread()
        log_monitor.start()
        
        # 保持程序运行
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("正在关闭程序...")
        log_monitor.stop()
    except Exception as e:
        logger.error(f"程序运行错误: {str(e)}")
    finally:
        if db_pool:
            db_pool.closeall()

if __name__ == "__main__":
    main()