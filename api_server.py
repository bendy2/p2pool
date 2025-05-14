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
            
            # 插入区块记录
            cur.execute("""
                INSERT INTO blocks (block_height, rewards, type, total_shares, time)
                VALUES (%s, %s, 'xmr', %s, %s)
                ON CONFLICT (block_height) DO NOTHING
            """, (block_height, reward, total_shares, current_time))
            
            # 3. 计算用户奖励
            xmr_fee = config['pool_fees']['xmr_fee']
            net_reward = reward * (1 - xmr_fee)  # 扣除矿池费用后的奖励
            
            # 4. 记录用户奖励
            for username, shares in user_shares.items():
                if shares > 0:
                    # 计算用户奖励比例
                    reward_ratio = shares / total_shares
                    user_reward = net_reward * reward_ratio
                    
                    # 检查用户是否存在，不存在则创建
                    cur.execute("""
                        INSERT INTO account (username, xmr_balance)
                        VALUES (%s, 0)
                        ON CONFLICT (username) DO NOTHING
                    """, (username,))
                    
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
                'net_reward': net_reward,
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
                    'block_height': block_height
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
            current_time = datetime.now()
            
            # 插入区块记录
            cur.execute("""
                INSERT INTO blocks (block_height, rewards, type, total_shares, time)
                VALUES (%s, %s, 'tari', %s, %s)
                ON CONFLICT (block_height) DO NOTHING
            """, (block_height, reward, total_shares, current_time))
            
            # 3. 计算用户奖励
            tari_fee = config['pool_fees']['tari_fee']
            net_reward = reward * (1 - tari_fee)  # 扣除矿池费用后的奖励
            
            # 4. 记录用户奖励
            for username, shares in user_shares.items():
                if shares > 0:
                    # 计算用户奖励比例
                    reward_ratio = shares / total_shares
                    user_reward = net_reward * reward_ratio
                    
                    # 检查用户是否存在，不存在则创建
                    cur.execute("""
                        INSERT INTO account (username, tari_balance)
                        VALUES (%s, 0)
                        ON CONFLICT (username) DO NOTHING
                    """, (username,))
                    
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
                'net_reward': net_reward,
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
        elif method == 'xmr_block':
            result = handle_xmr_block(params)
        elif method == 'tari_block':
            result = handle_tari_block(params)
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
        self.tari_block_pattern = re.compile(r'Mined Tari block [a-f0-9]+ at height (\d+)')
        
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
                height = int(tari_match.group(1))
                logger.info(f"检测到 TARI 爆块 - 高度: {height}")
                # 直接调用处理函数，让处理函数进行数据库检查
                handle_tari_block({'height': height})
                
        except Exception as e:
            logger.error(f"处理日志行时出错: {str(e)}")
            
    def stop(self):
        self.running = False

# 创建并启动日志监控线程
log_monitor = LogMonitorThread()
log_monitor.start()

if __name__ == '__main__':
    logger.info("Starting API server...")
    try:
        app.run(host='0.0.0.0', port=5000)
    finally:
        # 确保在服务器关闭时停止日志监控线程
        log_monitor.stop() 