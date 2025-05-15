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
fastapi_logger = logging.getLogger('fastapi')
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

# Redis键前缀
XMR_PREFIX = "xmr:submit:"
TARI_PREFIX = "tari:submit:"

# 修改 Redis 初始化函数
async def init_redis():
    global redis_client
    try:
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

# PostgreSQL数据库连接
async def init_db():
    global db_pool
    try:
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
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def get_chain_key(username: str, chain: str) -> str:
    """获取Redis键名"""
    prefix = XMR_PREFIX if chain.lower() == 'xmr' else TARI_PREFIX
    return f"{prefix}{username}"

async def increment_submit_count(username: str) -> Dict[str, int]:
    """同时增加用户XMR和TARI链的提交计数"""
    try:
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

        xmr_key = get_chain_key(username, 'xmr')
        tari_key = get_chain_key(username, 'tari')
        
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.incr(xmr_key)
            await pipe.incr(tari_key)
            await pipe.expire(xmr_key, 30 * 24 * 60 * 60)
            await pipe.expire(tari_key, 30 * 24 * 60 * 60)
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

@app.get('/stats')
async def get_stats():
    try:
        xmr_keys = await redis_client.keys(f"{XMR_PREFIX}*")
        tari_keys = await redis_client.keys(f"{TARI_PREFIX}*")
        
        logger.debug(f"Found {len(xmr_keys)} XMR keys and {len(tari_keys)} TARI keys in Redis")
        
        active_users = set()
        for key in xmr_keys + tari_keys:
            username = key.split(':')[-1]
            active_users.add(username)
        
        total_shares = 0
        for key in xmr_keys + tari_keys:
            shares = int(await redis_client.get(key) or 0)
            total_shares += shares
            logger.debug(f"User {key.split(':')[-1]} has {shares} shares")
        
        logger.info(f"Stats requested. Active users: {len(active_users)}, Total shares: {total_shares}")
        
        return {
            "active_users": len(active_users),
            "total_shares": total_shares
        }
        
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get('/users')
async def get_users():
    try:
        xmr_keys = await redis_client.keys(f"{XMR_PREFIX}*")
        tari_keys = await redis_client.keys(f"{TARI_PREFIX}*")
        
        logger.debug(f"Found {len(xmr_keys)} XMR keys and {len(tari_keys)} TARI keys in Redis")
        
        active_users = {}
        
        for key in tari_keys:
            username = key.replace(TARI_PREFIX, '')
            shares = int(await redis_client.get(key) or 0)
            
            if username not in active_users:
                active_users[username] = {
                    "xmr_shares": 0,
                    "tari_shares": shares,
                    "total_shares": shares
                }
            else:
                active_users[username]["tari_shares"] = shares
                active_users[username]["total_shares"] += shares
        
        logger.info(f"User list requested. Active users: {len(active_users)}")
        
        return {"users": active_users}
        
    except Exception as e:
        logger.error(f"Error getting user list: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.on_event("startup")
async def startup():
    await init_redis()
    await init_db()

if __name__ == '__main__':
    logger.info("Starting API server...")
    try:
        uvicorn.run(
            "api_server:app",
            host="0.0.0.0",
            port=5000,
            workers=4,
            loop="uvloop",
            limit_concurrency=1000,
            backlog=2048,
            reload=True,
            log_level="warning"
        )
    except Exception as e:
        logger.error(f"服务器启动失败: {str(e)}")
        raise