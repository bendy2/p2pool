from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
import asyncpg
import aioredis
import uvicorn
from typing import Dict, Any, List
import json
import logging
from datetime import datetime
import asyncio
import redis
import psycopg2
from psycopg2.extras import DictCursor
import time
import threading
import subprocess
import re
from queue import Queue
import requests
import aiofiles
import aioredis
from functools import lru_cache
from aiopg.pool import create_pool
from fastapi.logger import logger as fastapi_logger

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('api_server.log')
    ]
)

logger = logging.getLogger(__name__)

app = FastAPI()

# 设置 FastAPI 的日志级别为 WARNING
fastapi_logger.setLevel(logging.WARNING)

# 同时设置 uvicorn 的日志级别
logging.getLogger("uvicorn").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局变量
redis_client = None
db_pool = None

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

# 修改 Redis 初始化函数
async def init_redis():
    global redis_client
    try:
        # 使用新的 aioredis API
        redis_client = await aioredis.from_url(
            'redis://localhost',
            encoding='utf-8',
            max_connections=10
        )
        await FastAPILimiter.init(redis_client)
        logger.info("Successfully connected to Redis")
    except Exception as e:
        logger.error(f"Redis connection failed: {str(e)}")
        raise

# Redis键前缀
XMR_PREFIX = "xmr:submit:"
TARI_PREFIX = "tari:submit:"

# 添加XMR爆块记录
xmr_blocks = []

# PostgreSQL数据库连接
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=config['database']['host'],
        port=config['database']['port'],
        user=config['database']['user'],
        password=config['database']['password'],
        database=config['database']['database'],
        min_size=5,
        max_size=20
    )
    logger.info("Successfully connected to PostgreSQL")

def get_chain_key(username: str, chain: str) -> str:
    """获取Redis键名"""
    prefix = XMR_PREFIX if chain.lower() == 'xmr' else TARI_PREFIX
    return f"{prefix}{username}"

async def increment_submit_count(username: str) -> Dict[str, int]:
    """同时增加用户XMR和TARI链的提交计数"""
    try:
        # 同时增加两条链的计数
        xmr_key = get_chain_key(username, 'xmr')
        tari_key = get_chain_key(username, 'tari')
        
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.incr(xmr_key)
            await pipe.incr(tari_key)
            await pipe.expire(xmr_key, 30 * 24 * 60 * 60)
            await pipe.expire(tari_key, 30 * 24 * 60 * 60)
            xmr_count, tari_count, _, _ = await pipe.execute()
        
        return {
            'xmr': xmr_count,
            'tari': tari_count
        }
    except Exception as e:
        logger.error(f"Redis error while incrementing submit counts: {str(e)}")
        raise

async def get_submit_counts(username: str) -> Dict[str, int]:
    """获取用户两条链的提交计数"""
    try:
        xmr_key = get_chain_key(username, 'xmr')
        tari_key = get_chain_key(username, 'tari')
        
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.get(xmr_key)
            await pipe.get(tari_key)
            xmr_count, tari_count = await pipe.execute()
        
        return {
            'xmr': int(xmr_count or 0),
            'tari': int(tari_count or 0)
        }
    except Exception as e:
        logger.error(f"Redis error while getting submit counts: {str(e)}")
        raise

async def handle_submit(params: Dict[str, Any]):
    try:
        username = params.get('username')
        if not username:
            raise HTTPException(status_code=400, detail="Username is required")

        # 使用异步Redis操作
        xmr_key = get_chain_key(username, 'xmr')
        tari_key = get_chain_key(username, 'tari')
        
        # 使用新的 Redis API 进行事务操作
        async with redis_client.pipeline(transaction=True) as pipe:
            # 增加计数
            await pipe.incr(xmr_key)
            await pipe.incr(tari_key)
            # 设置过期时间
            await pipe.expire(xmr_key, 30 * 24 * 60 * 60)
            await pipe.expire(tari_key, 30 * 24 * 60 * 60)
            # 执行事务
            xmr_count, tari_count, _, _ = await pipe.execute()

        return {
            'status': 'OK',
            'message': 'Submission recorded successfully',
            'submit_counts': {
                'xmr': xmr_count,
                'tari': tari_count
            }
        }
    except Exception as e:
        logger.error(f"Error in handle_submit: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def handle_xmr_block(params):
    """处理XMR爆块信息"""
    logger.info(f"处理 XMR 区块 {params.get('height')} : {params.get('reward')} 信息")
    try:
        block_height = params.get('height')
        reward = params.get('reward')
        
        if not block_height or not reward:
            return {'error': '缺少必要参数'}
            
        # 使用异步数据库连接
        async with db_pool.acquire() as conn:
            # 首先检查数据库中是否已存在该区块
            exists = await conn.fetchval(
                "SELECT COUNT(*) FROM blocks WHERE block_height = $1 AND type = 'xmr'",
                block_height
            )
            
            if exists > 0:
                logger.info(f"XMR 区块 {block_height} 已存在于数据库中，跳过处理")
                return {
                    'success': True,
                    'message': 'Block already exists in database',
                    'block_height': block_height
                }
            
            # 1. 统计XMR链的submit总数
            total_shares = 0
            user_shares = {}
            xmr_wallet = {}
            tari_wallet = {}
            
            # 获取所有XMR提交记录
            xmr_keys = await redis_client.keys('xmr:submit:*')
            for key in xmr_keys:
                # 只删除前缀，保留完整的用户名
                data = key.replace(XMR_PREFIX, '')
                
                # 判断用户名长度
                if len(data) > 50:
                    username = data.split(':')[1]
                    xmr_wallet[username] = data.split(':')[0]
                    tari_wallet[username] = data.split(':')[1]
                else:
                    username = data
                    xmr_wallet[username] = ""
                    tari_wallet[username] = ""

                # 从数据库获取用户的钱包地址
                wallet_info = await conn.fetchrow("""
                    SELECT xmr_wallet, tari_wallet 
                    FROM account 
                    WHERE username = $1 and xmr_wallet != ''
                """, username)
                
                if wallet_info:
                    xmr_wallet[username], tari_wallet[username] = wallet_info
                else:
                    if xmr_wallet[username]:
                        await conn.execute("""
                            UPDATE account 
                            SET xmr_wallet = $1,
                                tari_wallet = $2
                            WHERE username = $3
                        """, xmr_wallet[username], tari_wallet[username], username)
                    else:
                        xmr_wallet[username] = ""
                        tari_wallet[username] = ""
                        
                shares = int(await redis_client.get(key) or 0)
                total_shares += shares
                user_shares[username] = shares
                
            if total_shares == 0:
                return {'error': '没有找到提交记录'}
                
            # 2. 将区块信息写入数据库
            current_time = datetime.now()
            value = reward / total_shares
            
            # 插入区块记录
            await conn.execute("""
                INSERT INTO blocks (block_height, rewards, type, total_shares, time, value, is_valid, check_status)
                VALUES ($1, $2, 'xmr', $3, $4, $5, $6, True)
                ON CONFLICT (block_height) DO NOTHING
            """, block_height, reward, total_shares, current_time, value, True)
            
            # 3. 计算用户奖励
            fee = config['pool_fees']
            
            # 4. 记录用户奖励
            for username, shares in user_shares.items():
                if shares > 0:
                    # 计算用户奖励比例
                    user_reward = value * shares * (1 - fee)
                    
                    # 检查用户是否存在，不存在则创建
                    await conn.execute("""
                        INSERT INTO account (username, xmr_wallet, tari_wallet, xmr_balance, tari_balance, fee)
                        VALUES ($1, $2, $3, 0, 0, $4)
                        ON CONFLICT (username) DO NOTHING
                    """, username, xmr_wallet[username], tari_wallet[username], fee)
                    
                    # 检查是否已存在该用户的奖励记录
                    exists = await conn.fetchval("""
                        SELECT COUNT(*) 
                        FROM rewards 
                        WHERE block_height = $1 
                        AND type = 'xmr' 
                        AND username = $2
                    """, block_height, username)
                    
                    if not exists:
                        # 插入奖励记录
                        await conn.execute("""
                            INSERT INTO rewards (block_height, type, username, reward, shares)
                            VALUES ($1, 'xmr', $2, $3, $4)
                        """, block_height, username, user_reward, shares)
                        
                        # 更新用户余额
                        await conn.execute("""
                            UPDATE account 
                            SET xmr_balance = xmr_balance + $1
                            WHERE username = $2
                        """, user_reward, username)
                    else:
                        logger.info(f"用户 {username} 的 XMR 区块 {block_height} 奖励记录已存在，跳过")
            
            # 5. 清空Redis中的XMR提交记录
            for key in xmr_keys:
                await redis_client.delete(key)
                
            return {
                'success': True,
                'block_height': block_height,
                'total_shares': total_shares,
                'time': current_time.isoformat()
            }
            
    except Exception as e:
        logger.error(f"处理XMR区块信息失败: {str(e)}")
        return {'error': f'处理失败: {str(e)}'}

async def handle_tari_block(params):
    """处理TARI爆块信息"""
    logger.info(f"处理 TARI 区块 {params.get('height')} 信息")
    try:
        block_height = params.get('height')
        block_id = params.get('block_id')
        if not block_height:
            return {'error': '缺少必要参数'}
            
        # 使用异步数据库连接
        async with db_pool.acquire() as conn:
            # 首先检查数据库中是否已存在该区块
            exists = await conn.fetchval(
                "SELECT COUNT(*) FROM blocks WHERE block_height = $1 AND type = 'tari'",
                block_height
            )
            
            if exists > 0:
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
            xmr_wallet = {}
            tari_wallet = {}
            
            # 获取所有TARI提交记录
            tari_keys = await redis_client.keys('tari:submit:*')
            for key in tari_keys:
                # 只删除前缀，保留完整的用户名
                data = key.replace(TARI_PREFIX, '')
                
                # 判断用户名长度
                if len(data) > 50:
                    username = data.split(':')[1]
                    xmr_wallet[username] = data.split(':')[0]
                    tari_wallet[username] = data.split(':')[1]
                else:
                    username = data
                    xmr_wallet[username] = ""
                    tari_wallet[username] = ""

                # 从数据库获取用户的钱包地址
                wallet_info = await conn.fetchrow("""
                    SELECT xmr_wallet, tari_wallet 
                    FROM account 
                    WHERE username = $1 and xmr_wallet != ''
                """, username)
                
                if wallet_info:
                    xmr_wallet[username], tari_wallet[username] = wallet_info
                else:
                    if xmr_wallet[username]:
                        await conn.execute("""
                            UPDATE account 
                            SET xmr_wallet = $1,
                                tari_wallet = $2
                            WHERE username = $3
                        """, xmr_wallet[username], tari_wallet[username], username)
                    else:
                        xmr_wallet[username] = ""
                        tari_wallet[username] = ""
                        
                shares = int(await redis_client.get(key) or 0)
                total_shares += shares
                user_shares[username] = shares
                
            if total_shares == 0:
                return {'error': '没有找到提交记录'}
                
            # 2. 将区块信息写入数据库
            reward = config['rewards']['tari_block_reward']
            value = reward / total_shares
            current_time = datetime.now()
            
            # 插入区块记录
            await conn.execute("""
                INSERT INTO blocks (block_height, rewards, type, total_shares, time, value, is_valid, check_status, block_id)
                VALUES ($1, $2, 'tari', $3, $4, $5, $6, False, $7)
                ON CONFLICT (block_height) DO NOTHING
            """, block_height, reward, total_shares, current_time, value, False, block_id)
            
            # 3. 计算用户奖励
            fee = config['pool_fees']
            
            # 4. 记录用户奖励
            for username, shares in user_shares.items():
                if shares > 0:
                    # 计算用户奖励比例
                    user_reward = value * shares * (1 - fee)
                    
                    # 检查用户是否存在，不存在则创建
                    await conn.execute("""
                        INSERT INTO account (username, xmr_wallet, tari_wallet, tari_balance, xmr_balance, fee)
                        VALUES ($1, $2, $3, 0, 0, $4)
                        ON CONFLICT (username) DO NOTHING
                    """, username, xmr_wallet[username], tari_wallet[username], fee)
                    
                    # 检查是否已存在该用户的奖励记录
                    exists = await conn.fetchval("""
                        SELECT COUNT(*) 
                        FROM rewards 
                        WHERE block_height = $1 
                        AND type = 'tari' 
                        AND username = $2
                    """, block_height, username)
                    
                    if not exists:
                        # 插入奖励记录
                        await conn.execute("""
                            INSERT INTO rewards (block_height, type, username, reward, shares)
                            VALUES ($1, 'tari', $2, $3, $4)
                        """, block_height, username, user_reward, shares)
                        
                        # 更新用户余额
                        await conn.execute("""
                            UPDATE account 
                            SET tari_balance = tari_balance + $1
                            WHERE username = $2
                        """, user_reward, username)
                    else:
                        logger.info(f"用户 {username} 的 TARI 区块 {block_height} 奖励记录已存在，跳过")
            
            # 5. 清空Redis中的TARI提交记录
            for key in tari_keys:
                await redis_client.delete(key)
                
            return {
                'success': True,
                'block_height': block_height,
                'total_shares': total_shares,
                'reward': reward,
                'time': current_time.isoformat()
            }
            
    except Exception as e:
        logger.error(f"处理TARI区块信息失败: {str(e)}")
        return {'error': f'处理失败: {str(e)}'}

@app.post("/json_rpc")
async def handle_json_rpc(request: Dict[str, Any]):
    try:
        method = request.get('method')
        params = request.get('params', {})
        request_id = request.get('id')

        if not method:
            raise HTTPException(status_code=400, detail="Method is required")

        result = None
        if method == 'submit':
            result = await handle_submit(params)

        else:
            raise HTTPException(status_code=404, detail=f"Method {method} not found")

        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': result
        }
    except Exception as e:
        logger.error(f"Error handling JSON-RPC request: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                'jsonrpc': '2.0',
                'id': request.get('id'),
                'error': {
                    'code': -32000,
                    'message': str(e)
                }
            }
        )

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
            # 只删除前缀，保留完整的用户名
            username = key.replace(XMR_PREFIX, '')
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
            # 只删除前缀，保留完整的用户名
            username = key.replace(TARI_PREFIX, '')
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

# 修改数据库连接函数
async def get_db_connection():
    """获取数据库连接"""
    return await asyncpg.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        user=config['database']['user'],
        password=config['database']['password'],
        database=config['database']['database']
    )

# 修改初始化数据库函数为异步
async def init_database():
    """初始化数据库表结构"""
    conn = None
    try:
        conn = await get_db_connection()
        
        # 创建account表(如果不存在)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS account (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                xmr_balance DECIMAL(20,12) DEFAULT 0,
                tari_balance DECIMAL(20,12) DEFAULT 0,
                xmr_wallet VARCHAR(255),
                tari_wallet VARCHAR(255),
                fee DECIMAL(5,2) DEFAULT 0.01,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建blocks表(如果不存在)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS blocks (
                id SERIAL PRIMARY KEY,
                block_height BIGINT NOT NULL,
                block_id VARCHAR(255),
                rewards DECIMAL(20,12) NOT NULL,
                type VARCHAR(10) NOT NULL,
                total_shares BIGINT NOT NULL,
                time TIMESTAMP NOT NULL,
                value DECIMAL(20,12) NOT NULL,
                is_valid BOOLEAN DEFAULT true,
                check_status BOOLEAN DEFAULT false,
                UNIQUE(block_height, type)
            )
        """)
        
        # 创建rewards表(如果不存在)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rewards (
                id SERIAL PRIMARY KEY,
                block_height BIGINT NOT NULL,
                type VARCHAR(10) NOT NULL,
                username VARCHAR(255) NOT NULL,
                reward DECIMAL(20,12) NOT NULL,
                shares BIGINT NOT NULL,
                time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (block_height, type) REFERENCES blocks(block_height, type),
                FOREIGN KEY (username) REFERENCES account(username)
            )
        """)
        
        # 创建payments表(如果不存在)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                tx_id VARCHAR(255) NOT NULL,
                type VARCHAR(10) NOT NULL,
                amount DECIMAL(20,12) NOT NULL,
                time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (username) REFERENCES account(username)
            )
        """)
        
        # 创建算力历史记录表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS hashrate_history (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                hashrate BIGINT NOT NULL
            )
        """)
        
        logger.info("数据库表结构初始化成功")
        
    except Exception as e:
        logger.error(f"数据库表结构初始化失败: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()

# 修改初始化基础数据函数为异步
async def init_base_data():
    """初始化基础数据"""
    conn = None
    try:
        conn = await get_db_connection()
        
        # 检查是否已存在基础数据
        count = await conn.fetchval("SELECT COUNT(*) FROM account")
        
        if count == 0:
            # 插入基础账户数据
            await conn.execute("""
                INSERT INTO account (username, xmr_wallet, tari_wallet)
                VALUES 
                ('miner1', 'XMR_WALLET_ADDRESS_1', 'TARI_WALLET_ADDRESS_1')
            """)
            
            logger.info("基础数据初始化成功")
        else:
            logger.info("基础数据已存在，跳过初始化")
            
    except Exception as e:
        logger.error(f"基础数据初始化失败: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()

# 修改启动事件处理函数
@app.on_event("startup")
async def startup():
    await init_redis()
    await init_db()
    await init_database()
    await init_base_data()



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
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                # 运行异步处理函数
                loop.run_until_complete(handle_xmr_block({'height': height, 'reward': reward}))
                loop.close()
                return
                
            # 检查 TARI 爆块信息
            tari_match = self.tari_block_pattern.search(line)
            if tari_match:
                height = int(tari_match.group(2))
                block_id = tari_match.group(1)
                logger.info(f"检测到 TARI 爆块 - 高度: {height}, 区块ID: {block_id}")
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                # 运行异步处理函数
                loop.run_until_complete(handle_tari_block({'height': height, 'block_id': block_id}))
                loop.close()
                
        except Exception as e:
            logger.error(f"处理日志行时出错: {str(e)}")
            
    def stop(self):
        self.running = False

class TariBlockChecker(threading.Thread):
    def __init__(self, db_config):
        super().__init__()
        self.daemon = True
        self.running = True
        self.db_config = db_config
        self.api_url = "https://explore.tari.com/blocks/{height}?json"
        self.check_interval = 60  # 检查间隔（秒）
        self.db_pool = None  # 添加数据库连接池属性

    async def init_db_pool(self):
        """初始化数据库连接池"""
        try:
            self.db_pool = await asyncpg.create_pool(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                min_size=5,
                max_size=20
            )
            logger.info("TariBlockChecker: 数据库连接池初始化成功")
        except Exception as e:
            logger.error(f"TariBlockChecker: 数据库连接池初始化失败: {str(e)}")
            raise

    def buffer_to_hex(self, buffer_data):
        """将 Buffer 数据转换为十六进制字符串"""
        if not isinstance(buffer_data, dict) or 'data' not in buffer_data:
            return ''
        return ''.join([f'{x:02x}' for x in buffer_data['data']])

    async def get_block_data(self, block_height: int) -> Dict[str, Any]:
        """获取指定高度的区块数据"""
        url = f'https://textexplore.tari.com/blocks/{block_height}?json'
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.json()
        except Exception as e:
            logger.error(f"获取区块数据失败: {e}")
            return None

    async def get_block_from_api(self, height):
        """从 API 获取区块数据"""
        try:
            url = f'https://textexplore.tari.com/blocks/{height}?json'
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    response.raise_for_status()
                    
                    # 检查响应内容类型
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' not in content_type:
                        logger.warning(f"API 响应不是 JSON 格式: {content_type}")
                        return None
                        
                    # 尝试解析 JSON
                    try:
                        data = await response.json()
                        if not data:
                            logger.warning(f"API 返回空数据: {await response.text()[:100]}")
                            return None
                        return data
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON 解析错误: {e}, 响应内容: {await response.text()[:100]}")
                        return None
                    
        except Exception as e:
            logger.error(f"处理 API 响应时发生未知错误: {e}")
            return None

    async def check_block(self):
        """检查一个区块"""
        if not self.db_pool:
            await self.init_db_pool()

        async with self.db_pool.acquire() as conn:
            block = await conn.fetchrow("""
                SELECT id, block_height, block_id 
                FROM blocks 
                WHERE check_status = false 
                AND type = 'tari'
                ORDER BY block_height ASC 
                LIMIT 1
            """)
            
            if not block:
                logger.info("没有需要检查的区块")
                return
                
            block_hash = block['block_id']
            logger.info(f"开始检查区块 {block['block_height']}")
            
            api_data = await self.get_block_from_api(block['block_height'])
            
            if not api_data:
                logger.info(f"远程未找到区块 {block['block_height']}，跳过")
                return

            try:
                header = api_data.get('header', {})
                remote_hash = self.buffer_to_hex(header.get('hash', {}))
                
                if not remote_hash or remote_hash != block_hash:
                    logger.warning(f"区块 {block['block_height']} 远程哈希无效")
                    await self.handle_invalid_block(block['id'], block['block_height'])
                    return

                # 更新区块状态
                await self.update_block_status(block['id'], True, remote_hash)
                logger.info(f"区块 {block['block_height']} 验证成功")

            except Exception as e:
                logger.error(f"检查区块 {block['block_height']} 时发生错误: {e}")
                # 如果发生错误，将区块标记为无效
                await self.handle_invalid_block(block['id'], block['block_height'])

    async def update_block_status(self, block_id, is_valid, remote_hash=None):
        """更新区块状态"""
        if not self.db_pool:
            await self.init_db_pool()

        try:
            async with self.db_pool.acquire() as conn:
                if is_valid:
                    await conn.execute("""
                        UPDATE blocks 
                        SET check_status = true, 
                            is_valid = true
                        WHERE id = $1
                    """, block_id)
                else:
                    await conn.execute("""
                        UPDATE blocks 
                        SET check_status = true, 
                            is_valid = false
                        WHERE id = $1
                    """, block_id)
                
                logger.info(f"区块 {block_id} 状态已更新: is_valid={is_valid} remote_hash={remote_hash}")
                
        except Exception as e:
            logger.error(f"更新区块状态失败: {e}")

    async def handle_invalid_block(self, block_id, block_height):
        """处理无效区块"""
        if not self.db_pool:
            await self.init_db_pool()

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # 1. 更新区块状态为无效
                    await conn.execute("""
                        UPDATE blocks 
                        SET check_status = true, 
                            is_valid = false
                        WHERE id = $1
                    """, block_id)
                    
                    # 2. 获取该区块的所有奖励记录
                    rewards = await conn.fetch("""
                        SELECT username, reward, type
                        FROM rewards 
                        WHERE block_height = $1
                    """, block_height)
                    
                    # 3. 回滚用户余额
                    for reward in rewards:
                        if reward['type'] == 'tari':
                            await conn.execute("""
                                UPDATE account 
                                SET tari_balance = tari_balance - $1
                                WHERE username = $2
                            """, reward['reward'], reward['username'])
                        elif reward['type'] == 'xmr':
                            await conn.execute("""
                                UPDATE account 
                                SET xmr_balance = xmr_balance - $1
                                WHERE username = $2
                            """, reward['reward'], reward['username'])
                    
                    # 4. 删除奖励记录
                    await conn.execute("""
                        UPDATE rewards 
                        SET reward = 0
                        WHERE block_height = $1
                    """, block_height)
                    
                    logger.info(f"区块 {block_height} 已标记为无效并清理相关数据")
                    
        except Exception as e:
            logger.error(f"处理无效区块 {block_height} 时发生错误: {e}")

    def run(self):
        """运行检查器"""
        while self.running:
            try:
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                # 运行检查
                loop.run_until_complete(self.check_block())
                loop.close()
            except Exception as e:
                logger.error(f"检查器运行错误: {e}")
            time.sleep(self.check_interval)

    def stop(self):
        """停止检查器"""
        self.running = False
        if self.db_pool:
            asyncio.run(self.db_pool.close())
        

# 修改主函数
if __name__ == '__main__':
    logger.info("Starting API server...")
    log_monitor = None
    tari_checker = None
    
    try:
        # 创建并启动日志监控线程
        log_monitor = LogMonitorThread()
        log_monitor.start()
        
        # 启动 Tari 区块检查器
        tari_checker = TariBlockChecker(config['database'])
        tari_checker.start()
        
        # 启动 API 服务器，添加日志配置
        uvicorn.run(
            "api_server:app",
            host="0.0.0.0",
            port=5000,
            workers=4,
            loop="uvloop",
            limit_concurrency=1000,
            backlog=2048,
            reload=True,
            log_level="warning"  # 设置 uvicorn 的日志级别
        )
    except Exception as e:
        logger.error(f"服务器启动失败: {str(e)}")
        raise
    finally:
        # 确保在服务器关闭时停止所有线程
        if log_monitor:
            log_monitor.stop()
        if tari_checker:
            tari_checker.stop()