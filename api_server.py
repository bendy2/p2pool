from flask import Flask, request, jsonify
import json
import logging
from datetime import datetime
import redis
from typing import Dict, Any
import os
import psycopg2
from psycopg2.extras import DictCursor
import time
import threading
import subprocess
import re
from queue import Queue
import requests

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,  # 将默认日志级别改为 WARNING
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('api_server.log')
    ]
)

logger = logging.getLogger(__name__)

# 设置第三方库的日志级别
logging.getLogger('werkzeug').setLevel(logging.WARNING)  # Flask 的日志级别
logging.getLogger('urllib3').setLevel(logging.WARNING)   # requests 的日志级别

app = Flask(__name__)

# 用户统计信息字典
user_stats = {}

# 加载配置文件
def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"加载配置文件失败: {str(e)}")
        raise

config = load_config()

# Redis连接配置
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# 初始化Redis连接
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True  # 自动将响应解码为字符串
    )
    redis_client.ping()  # 测试连接
    logger.info("Successfully connected to Redis")
except redis.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {str(e)}")
    raise

# Redis键前缀
XMR_PREFIX = "xmr:submit:"
TARI_PREFIX = "tari:submit:"

# 添加XMR爆块记录
xmr_blocks = []

# PostgreSQL数据库连接
def get_db_connection():
    return psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['database'],
        user=config['database']['user'],
        password=config['database']['password']
    )

def get_chain_key(username: str, chain: str) -> str:
    """获取Redis键名"""
    prefix = XMR_PREFIX if chain.lower() == 'xmr' else TARI_PREFIX
    return f"{prefix}{username}"

def increment_submit_count(username: str) -> Dict[str, int]:
    """同时增加用户XMR和TARI链的提交计数"""
    try:
        # 同时增加两条链的计数
        xmr_key = get_chain_key(username, 'xmr')
        tari_key = get_chain_key(username, 'tari')
        
        xmr_count = redis_client.incr(xmr_key)
        tari_count = redis_client.incr(tari_key)
        
        # 设置过期时间(30天)
        redis_client.expire(xmr_key, 30 * 24 * 60 * 60)
        redis_client.expire(tari_key, 30 * 24 * 60 * 60)
        
        return {
            'xmr': xmr_count,
            'tari': tari_count
        }
    except redis.RedisError as e:
        logger.error(f"Redis error while incrementing submit counts: {str(e)}")
        raise

def get_submit_counts(username: str) -> Dict[str, int]:
    """获取用户两条链的提交计数"""
    try:
        xmr_key = get_chain_key(username, 'xmr')
        tari_key = get_chain_key(username, 'tari')
        
        xmr_count = int(redis_client.get(xmr_key) or 0)
        tari_count = int(redis_client.get(tari_key) or 0)
        
        return {
            'xmr': xmr_count,
            'tari': tari_count
        }
    except redis.RedisError as e:
        logger.error(f"Redis error while getting submit counts: {str(e)}")
        raise

def handle_submit(params: Dict[str, Any]) -> Dict[str, Any]:
    """处理submit方法的请求"""
    try:
        username = params.get('username')
        if not username:
            logger.warning(f"Invalid submission: missing username")
            return {
                'error': {
                    'code': -32602,
                    'message': 'Invalid params: username is required'
                }
            }
        
        # 同时增加两条链的提交计数
        submit_counts = increment_submit_count(username)
        
        # 记录提交
        submission = {
            'username': username,
            'timestamp': datetime.now().isoformat(),
            'submit_counts': submit_counts
        }
        
        #logger.info(f"Share submitted - User: {username}, XMR submits: {submit_counts['xmr']}, TARI submits: {submit_counts['tari']}")
        
        return {
            'result': {
                'status': 'OK',
                'message': 'Submission recorded successfully',
                'submit_counts': submit_counts
            }
        }
    except Exception as e:
        logger.error(f"Error processing submission: {str(e)}")
        return {
            'error': {
                'code': -32000,
                'message': f'Internal error: {str(e)}'
            }
        }

def handle_xmr_block(params):
    """处理XMR爆块信息"""
    logger.info(f"处理 XMR 区块 {params.get('height')} : {params.get('reward')} 信息")
    try:
        block_height = params.get('height')
        reward = params.get('reward')
        
        if not block_height or not reward:
            return {'error': '缺少必要参数'}
            
        # 首先检查数据库中是否已存在该区块
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("SELECT COUNT(*) FROM blocks WHERE block_height = %s AND type = 'xmr'", (block_height,))
            exists = cur.fetchone()[0] > 0
            
            if exists:
                logger.info(f"XMR 区块 {block_height} 已存在于数据库中，跳过处理")
                return {
                    'success': True,
                    'message': 'Block already exists in database',
                    'block_height': block_height
                }
            
            # 1. 统计XMR链的submit总数
            total_shares = 0
            user_shares = {}
            for key in redis_client.keys('xmr:submit:*'):
                username = key.split(':')[-1]
                shares = int(redis_client.get(key) or 0)
                total_shares += shares
                user_shares[username] = shares
                
            if total_shares == 0:
                return {'error': '没有找到提交记录'}
                
            # 2. 将区块信息写入数据库
            current_time = datetime.now()
            value = reward / total_shares
            # 插入区块记录
            cur.execute("""
                INSERT INTO blocks (block_height, rewards, type, total_shares, time, value, is_valid, check_status)
                VALUES (%s, %s, 'xmr', %s, %s, %s, %s, True)
                ON CONFLICT (block_height) DO NOTHING
            """, (block_height, reward, total_shares, current_time, value, True))
            
            # 3. 计算用户奖励
            fee = config['pool_fees']
            
            # 4. 记录用户奖励
            for username, shares in user_shares.items():
                if shares > 0:
                    # 计算用户奖励比例
                    user_reward = value * shares * (1 - fee)
                    
                    # 检查用户是否存在，不存在则创建
                    cur.execute("""
                        INSERT INTO account (username, xmr_balance, tari_balance, fee)
                        VALUES (%s, 0, 0, %s)
                        ON CONFLICT (username) DO NOTHING
                    """, (username, fee))
                    
                    # 检查是否已存在该用户的奖励记录
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM rewards 
                        WHERE block_height = %s 
                        AND type = 'xmr' 
                        AND username = %s
                    """, (block_height, username))
                    
                    if cur.fetchone()[0] == 0:
                        # 插入奖励记录
                        cur.execute("""
                            INSERT INTO rewards (block_height, type, username, reward, shares)
                            VALUES (%s, 'xmr', %s, %s, %s)
                        """, (block_height, username, user_reward, shares))
                        
                        # 更新用户余额
                        cur.execute("""
                            UPDATE account 
                            SET xmr_balance = xmr_balance + %s
                            WHERE username = %s
                        """, (user_reward, username))
                    else:
                        logger.info(f"用户 {username} 的 XMR 区块 {block_height} 奖励记录已存在，跳过")
            
            conn.commit()
            
            # 5. 清空Redis中的XMR提交记录
            for key in redis_client.keys('xmr:submit:*'):
                redis_client.delete(key)
                
            return {
                'success': True,
                'block_height': block_height,
                'total_shares': total_shares,
                'time': current_time.isoformat()
            }
            
        except Exception as e:
            conn.rollback()
            logging.error(f"数据库操作失败: {str(e)}")
            return {'error': f'数据库操作失败: {str(e)}'}
            
        finally:
            cur.close()
            conn.close()
            
    except Exception as e:
        logging.error(f"处理XMR区块信息失败: {str(e)}")
        return {'error': f'处理失败: {str(e)}'}

def handle_tari_block(params):
    """处理TARI爆块信息"""
    logger.info(f"处理 TARI 区块 {params.get('height')} 信息")
    try:
        block_height = params.get('height')
        block_id = params.get('block_id')
        if not block_height:
            return {'error': '缺少必要参数'}
            
        # 首先检查数据库中是否已存在该区块
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("SELECT COUNT(*) FROM blocks WHERE block_height = %s AND type = 'tari'", (block_height,))
            exists = cur.fetchone()[0] > 0
            
            if exists:
                logger.info(f"TARI 区块 {block_height} 已存在于数据库中，跳过处理")
                return {
                    'success': True,
                    'message': 'Block already exists in database',
                    'block_height': block_height,
                    'block_id': block_id
                }
            
            # 1. 统计TARI链的submit总数
            total_shares = 0
            user_shares = {}
            for key in redis_client.keys('tari:submit:*'):
                username = key.split(':')[-1]
                shares = int(redis_client.get(key) or 0)
                total_shares += shares
                user_shares[username] = shares
                
            if total_shares == 0:
                return {'error': '没有找到提交记录'}
                
            # 2. 将区块信息写入数据库
            # 从配置文件获取TARI区块奖励
            reward = config['rewards']['tari_block_reward']
            value = reward / total_shares
            current_time = datetime.now()
            
            # 插入区块记录
            cur.execute("""
                INSERT INTO blocks (block_height, rewards, type, total_shares, time, value, is_valid, check_status, block_id)
                VALUES (%s, %s, 'tari', %s, %s, %s, %s, False, %s)
                ON CONFLICT (block_height) DO NOTHING
            """, (block_height, reward, total_shares, current_time, value, False, block_id))
            
            # 3. 计算用户奖励
            fee = config['pool_fees']
            
            # 4. 记录用户奖励
            for username, shares in user_shares.items():
                if shares > 0:
                    # 计算用户奖励比例
                    user_reward = value * shares * (1 - fee)
                    
                    # 检查用户是否存在，不存在则创建
                    cur.execute("""
                        INSERT INTO account (username, tari_balance, xmr_balance, fee)
                        VALUES (%s, 0, 0, %s)
                        ON CONFLICT (username) DO NOTHING
                    """, (username, fee))
                    
                    # 检查是否已存在该用户的奖励记录
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM rewards 
                        WHERE block_height = %s 
                        AND type = 'tari' 
                        AND username = %s
                    """, (block_height, username))
                    
                    if cur.fetchone()[0] == 0:
                        # 插入奖励记录
                        cur.execute("""
                            INSERT INTO rewards (block_height, type, username, reward, shares)
                            VALUES (%s, 'tari', %s, %s, %s)
                        """, (block_height, username, user_reward, shares))
                        
                        # 更新用户余额
                        cur.execute("""
                            UPDATE account 
                            SET tari_balance = tari_balance + %s
                            WHERE username = %s
                        """, (user_reward, username))
                    else:
                        logger.info(f"用户 {username} 的 TARI 区块 {block_height} 奖励记录已存在，跳过")
            
            conn.commit()
            
            # 5. 清空Redis中的TARI提交记录
            for key in redis_client.keys('tari:submit:*'):
                redis_client.delete(key)
                
            return {
                'success': True,
                'block_height': block_height,
                'total_shares': total_shares,
                'reward': reward,
                'time': current_time.isoformat()
            }
            
        except Exception as e:
            conn.rollback()
            logging.error(f"数据库操作失败: {str(e)}")
            return {'error': f'数据库操作失败: {str(e)}'}
            
        finally:
            cur.close()
            conn.close()
            
    except Exception as e:
        logging.error(f"处理TARI区块信息失败: {str(e)}")
        return {'error': f'处理失败: {str(e)}'}

def handle_json_rpc(data):
    """处理JSON-RPC请求"""
    try:
        if not isinstance(data, dict):
            raise ValueError("Invalid request format")
        
        if data.get('jsonrpc') != '2.0':
            raise ValueError("Invalid JSON-RPC version")
        
        method = data.get('method')
        params = data.get('params', {})
        request_id = data.get('id')
        
        if not method:
            raise ValueError("Method is required")
        
        result = None
        if method == 'submit':
            result = handle_submit(params)
        elif method == 'xmr_block':
            result = handle_xmr_block(params)
        elif method == 'tari_block':
            result = handle_tari_block(params)
        else:
            raise ValueError(f"Method {method} not found")
        
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': result
        }
    except Exception as e:
        logger.error(f"Error handling JSON-RPC request: {str(e)}")
        return {
            'jsonrpc': '2.0',
            'id': data.get('id'),
            'error': {
                'code': -32000,
                'message': str(e)
            }
        }

@app.route('/json_rpc', methods=['POST'])
def json_rpc():
    """处理JSON-RPC请求"""
    try:
        # 获取请求数据
        data = request.get_json()
        
        # 验证JSON-RPC 2.0请求格式
        if not isinstance(data, dict):
            return jsonify({
                'jsonrpc': '2.0',
                'error': {
                    'code': -32700,
                    'message': 'Parse error'
                },
                'id': None
            })
        
        # 提取请求参数
        method = data.get('method')
        params = data.get('params', {})
        request_id = data.get('id')
        
        # 验证必要字段
        if not method:
            return jsonify({
                'jsonrpc': '2.0',
                'error': {
                    'code': -32600,
                    'message': 'Invalid Request: method is required'
                },
                'id': request_id
            })
        
        # 根据方法名调用相应的处理函数
        if method == 'submit':
            result = handle_submit(params)
        #elif method == 'xmr_block':
        #    #result = handle_xmr_block(params)
        #elif method == 'tari_block':
        #    #result = handle_tari_block(params)
        else:
            return jsonify({
                'jsonrpc': '2.0',
                'error': {
                    'code': -32601,
                    'message': f'Method not found: {method}'
                },
                'id': request_id
            })
        
        # 返回响应
        response = {
            'jsonrpc': '2.0',
            'id': request_id
        }
        response.update(result)
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return jsonify({
            'jsonrpc': '2.0',
            'error': {
                'code': -32000,
                'message': f'Internal error: {str(e)}'
            },
            'id': request.get_json().get('id') if request.is_json else None
        })

@app.route('/stats', methods=['GET'])
def get_stats():
    try:
        # 从Redis获取所有提交记录
        xmr_keys = redis_client.keys(f"{XMR_PREFIX}*")
        tari_keys = redis_client.keys(f"{TARI_PREFIX}*")
        
        logger.debug(f"Found {len(xmr_keys)} XMR keys and {len(tari_keys)} TARI keys in Redis")
        
        # 计算活跃用户数（有提交记录的用户）
        active_users = set()
        for key in xmr_keys + tari_keys:
            username = key.split(':')[-1]
            active_users.add(username)
        
        # 计算总提交数
        total_shares = 0
        for key in xmr_keys + tari_keys:
            shares = int(redis_client.get(key) or 0)
            total_shares += shares
            logger.debug(f"User {key.split(':')[-1]} has {shares} shares")
        
        logger.info(f"Stats requested. Active users: {len(active_users)}, Total shares: {total_shares}")
        
        return jsonify({
            "active_users": len(active_users),
            "total_shares": total_shares
        })
        
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/users', methods=['GET'])
def get_users():
    try:
        # 从Redis获取所有提交记录
        xmr_keys = redis_client.keys(f"{XMR_PREFIX}*")
        tari_keys = redis_client.keys(f"{TARI_PREFIX}*")
        
        logger.debug(f"Found {len(xmr_keys)} XMR keys and {len(tari_keys)} TARI keys in Redis")
        
        active_users = {}
        
        # 处理XMR提交
        for key in xmr_keys:
            username = key.split(':')[-1]
            shares = int(redis_client.get(key) or 0)
            
            if username not in active_users:
                active_users[username] = {
                    "xmr_shares": shares,
                    "tari_shares": 0,
                    "total_shares": shares
                }
                logger.debug(f"New user found: {username} with {shares} XMR shares")
            else:
                active_users[username]["xmr_shares"] = shares
                active_users[username]["total_shares"] += shares
                logger.debug(f"Updated user {username} XMR shares to {shares}")
        
        # 处理TARI提交
        for key in tari_keys:
            username = key.split(':')[-1]
            shares = int(redis_client.get(key) or 0)
            
            if username not in active_users:
                active_users[username] = {
                    "xmr_shares": 0,
                    "tari_shares": shares,
                    "total_shares": shares
                }
                logger.debug(f"New user found: {username} with {shares} TARI shares")
            else:
                active_users[username]["tari_shares"] = shares
                active_users[username]["total_shares"] += shares
                logger.debug(f"Updated user {username} TARI shares to {shares}")
        
        logger.info(f"User list requested. Active users: {len(active_users)}")
        
        return jsonify({"users": active_users})
        
    except Exception as e:
        logger.error(f"Error getting user list: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/xmr_stats')
def xmr_stats():
    """获取XMR爆块统计信息"""
    try:
        total_blocks = len(xmr_blocks)
        total_reward = sum(block['reward'] for block in xmr_blocks)
        
        return jsonify({
            'total_blocks': total_blocks,
            'total_reward': total_reward,
            'blocks': xmr_blocks
        })
    except Exception as e:
        logger.error(f"Error getting XMR stats: {str(e)}")
        return jsonify({
            'error': str(e)
        }), 500

def init_database():
    """初始化数据库表结构"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 创建account表(如果不存在)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS account (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                xmr_balance DECIMAL(20,12) DEFAULT 0,
                tari_balance DECIMAL(20,12) DEFAULT 0,
                xmr_wallet VARCHAR(255),
                tari_wallet VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建blocks表(如果不存在)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS blocks (
                id SERIAL PRIMARY KEY,
                block_height BIGINT UNIQUE NOT NULL,
                rewards DECIMAL(20,12) NOT NULL,
                type VARCHAR(10) NOT NULL,
                total_shares BIGINT NOT NULL,
                time TIMESTAMP NOT NULL
            )
        """)
        
        # 创建rewards表(如果不存在)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rewards (
                id SERIAL PRIMARY KEY,
                block_height BIGINT NOT NULL,
                type VARCHAR(10) NOT NULL,
                username VARCHAR(255) NOT NULL,
                reward DECIMAL(20,12) NOT NULL,
                shares BIGINT NOT NULL,
                time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (block_height) REFERENCES blocks(block_height),
                FOREIGN KEY (username) REFERENCES account(username)
            )
        """)
        
        conn.commit()
        logger.info("数据库表结构初始化成功")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库表结构初始化失败: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

def init_base_data():
    """初始化基础数据"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 检查是否已存在基础数据
        cur.execute("SELECT COUNT(*) FROM account")
        count = cur.fetchone()[0]
        
        if count == 0:
            # 插入基础账户数据
            cur.execute("""
                INSERT INTO account (username, xmr_wallet, tari_wallet)
                VALUES 
                ('miner1', 'XMR_WALLET_ADDRESS_1', 'TARI_WALLET_ADDRESS_1'),
                ('miner2', 'XMR_WALLET_ADDRESS_2', 'TARI_WALLET_ADDRESS_2')
            """)
            
            conn.commit()
            logger.info("基础数据初始化成功")
        else:
            logger.info("基础数据已存在，跳过初始化")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"基础数据初始化失败: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

# 在应用启动时初始化数据库和基础数据
init_database()
init_base_data()

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
                        
                    # 将日志行放入队列
                    self.log_queue.put(line)
                    
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
                # 直接调用处理函数，让处理函数进行数据库检查
                handle_xmr_block({'height': height, 'reward': reward})
                return
                
            # 检查 TARI 爆块信息
            tari_match = self.tari_block_pattern.search(line)
            if tari_match:
                height = int(tari_match.group(2))
                block_id = tari_match.group(1)
                logger.info(f"检测到 TARI 爆块 - 高度: {height}, 区块ID: {block_id}")
                # 直接调用处理函数，让处理函数进行数据库检查
                handle_tari_block({'height': height, 'block_id': block_id})
                
        except Exception as e:
            logger.error(f"处理日志行时出错: {str(e)}")
            
    def stop(self):
        self.running = False

# 创建并启动日志监控线程
log_monitor = LogMonitorThread()
log_monitor.start()

def process_block(block_data):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 设置check_status
        check_status = block_data['type'].lower() == 'xmr'
        
        cur.execute("""
            INSERT INTO blocks (block_id, height, timestamp, type, reward, is_valid, check_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            block_data['block_id'],
            block_data['height'],
            block_data['timestamp'],
            block_data['type'],
            block_data['reward'],
            True,
            check_status
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return True
    except Exception as e:
        logger.error(f"处理区块时发生错误: {str(e)}")
        return False

class TariBlockChecker(threading.Thread):
    def __init__(self, db_config):
        super().__init__()
        self.daemon = True
        self.running = True
        self.db_config = db_config
        self.api_url = "https://explore.tari.com/blocks/{height}?json"
        self.check_interval = 60  # 检查间隔（秒）

    def buffer_to_hex(self, buffer_data):
        """将 Buffer 数据转换为十六进制字符串"""
        if not isinstance(buffer_data, dict) or 'data' not in buffer_data:
            return ''
        return ''.join([f'{x:02x}' for x in buffer_data['data']])
    def get_block_data(block_height: int) -> Dict[str, Any]:
        """获取指定高度的区块数据"""
        url = f'https://textexplore.tari.com/blocks/{block_height}?json'
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"获取区块数据失败: {e}")
            return None
    def get_block_from_api(self, height):
        """从 API 获取区块数据"""
        try:
            url = f'https://textexplore.tari.com/blocks/{height}?json'
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # 检查响应内容类型
            content_type = response.headers.get('content-type', '')
            if 'application/json' not in content_type:
                logger.warning(f"API 响应不是 JSON 格式: {content_type}")
                return None
                
            # 尝试解析 JSON
            try:
                data = response.json()
                if not data:
                    logger.warning(f"API 返回空数据: {response.text[:100]}")
                    return None
                return data
            except json.JSONDecodeError as e:
                logger.warning(f"JSON 解析错误: {e}, 响应内容: {response.text[:100]}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"获取区块 {height} 数据失败: {e}")
            return None
        except Exception as e:
            logger.error(f"处理 API 响应时发生未知错误: {e}")
            return None

    def get_unchecked_block(self):
        """从数据库获取一个未检查的区块"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, block_height, block_id 
                FROM blocks 
                WHERE check_status = false 
                AND type = 'tari'
                ORDER BY block_height ASC 
                LIMIT 1
            """)
            block = cur.fetchone()
            cur.close()
            conn.close()
            return block
        except Exception as e:
            logger.error(f"获取未检查区块失败: {e}")
            return None

    def update_block_status(self, block_id, is_valid, remote_hash=None):
        """更新区块状态"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            if is_valid:
                cur.execute("""
                    UPDATE blocks 
                    SET check_status = true, 
                        is_valid = true
                    WHERE id = %s
                """, (block_id,))
            else:
                cur.execute("""
                    UPDATE blocks 
                    SET check_status = true, 
                        is_valid = false
                    WHERE id = %s
                """, (block_id,))
            
            conn.commit()
            logger.info(f"区块 {block_id} 状态已更新: is_valid={is_valid}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"更新区块状态失败: {e}")
        finally:
            cur.close()
            conn.close()

    def check_block(self):
        """检查一个区块"""
        block = self.get_unchecked_block()
        if not block:
            logger.info("没有需要检查的区块")
            return

        logger.info(f"开始检查区块 {block[1]}")  # block[1] 是 block_height
        api_data = self.get_block_from_api(block[1])
        
        if not api_data:
            logger.info(f"远程未找到区块 {block[1]}，跳过")
            return

        try:
            header = api_data.get('header', {})
            remote_hash = self.buffer_to_hex(header.get('hash', {}))
            
            if not remote_hash:
                logger.warning(f"区块 {block[1]} 远程哈希无效")
                return

            # 更新区块状态
            self.update_block_status(block[0], True, remote_hash)
            logger.info(f"区块 {block[1]} 验证成功")

        except Exception as e:
            logger.error(f"检查区块 {block[1]} 时发生错误: {e}")
            # 如果发生错误，将区块标记为无效
            self.handle_invalid_block(block[0], block[1])

    def handle_invalid_block(self, block_id, block_height):
        """处理无效区块"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            # 1. 更新区块状态为无效
            cur.execute("""
                UPDATE blocks 
                SET check_status = true, 
                    is_valid = false
                WHERE id = %s
            """, (block_id,))
            
            # 2. 获取该区块的所有奖励记录
            cur.execute("""
                SELECT username, reward, type
                FROM rewards 
                WHERE block_height = %s
            """, (block_height,))
            rewards = cur.fetchall()
            
            # 3. 回滚用户余额
            for reward in rewards:
                username, amount, reward_type = reward
                if reward_type == 'tari':
                    cur.execute("""
                        UPDATE account 
                        SET tari_balance = tari_balance - %s
                        WHERE username = %s
                    """, (amount, username))
                elif reward_type == 'xmr':
                    cur.execute("""
                        UPDATE account 
                        SET xmr_balance = xmr_balance - %s
                        WHERE username = %s
                    """, (amount, username))
            
            # 4. 删除奖励记录
            cur.execute("""
                DELETE FROM rewards 
                WHERE block_height = %s
            """, (block_height,))
            
            # 5. 删除区块记录
            cur.execute("""
                DELETE FROM blocks 
                WHERE id = %s
            """, (block_id,))
            
            conn.commit()
            logger.info(f"区块 {block_height} 已标记为无效并清理相关数据")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"处理无效区块 {block_height} 时发生错误: {e}")
        finally:
            cur.close()
            conn.close()

    def run(self):
        """运行检查器"""
        while self.running:
            try:
                self.check_block()
            except Exception as e:
                logger.error(f"检查器运行错误: {e}")
            time.sleep(self.check_interval)

    def stop(self):
        """停止检查器"""
        self.running = False

# 在 main 函数中添加检查器的启动代码
if __name__ == '__main__':
    logger.info("Starting API server...")
    try:
        # 启动 Tari 区块检查器
        tari_checker = TariBlockChecker(config['database'])
        tari_checker.start()
        
        # 启动 API 服务器
        app.run(host='0.0.0.0', port=5000)
    finally:
        # 确保在服务器关闭时停止所有线程
        log_monitor.stop()
        if 'tari_checker' in locals():
            tari_checker.stop() 