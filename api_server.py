from flask import Flask, request, jsonify
import json
import logging
from datetime import datetime
import redis
from typing import Dict, Any
import os
import psycopg2
from psycopg2.extras import DictCursor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

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
        
        logger.info(f"Received submission from user: {username}, XMR submits: {submit_counts['xmr']}, TARI submits: {submit_counts['tari']}")
        
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
    try:
        block_height = params.get('height')
        reward = params.get('reward')
        
        if not block_height or not reward:
            return {'error': '缺少必要参数'}
            
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
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
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
    try:
        block_height = params.get('height')
        
        if not block_height:
            return {'error': '缺少必要参数'}
            
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
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
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
    """获取提交统计信息"""
    try:
        # 获取所有用户的提交统计
        xmr_stats = {}
        tari_stats = {}
        
        # 获取所有XMR提交记录
        xmr_keys = redis_client.keys(f"{XMR_PREFIX}*")
        for key in xmr_keys:
            username = key.replace(XMR_PREFIX, '')
            count = int(redis_client.get(key) or 0)
            xmr_stats[username] = count
            
        # 获取所有TARI提交记录
        tari_keys = redis_client.keys(f"{TARI_PREFIX}*")
        for key in tari_keys:
            username = key.replace(TARI_PREFIX, '')
            count = int(redis_client.get(key) or 0)
            tari_stats[username] = count
        
        return jsonify({
            'xmr_stats': {
                'total_users': len(xmr_stats),
                'user_stats': xmr_stats
            },
            'tari_stats': {
                'total_users': len(tari_stats),
                'user_stats': tari_stats
            }
        })
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return jsonify({
            'error': str(e)
        }), 500

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

if __name__ == '__main__':
    logger.info("Starting API server...")
    app.run(host='127.0.0.1', port=5000) 