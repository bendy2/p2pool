from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import redis
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import json
import os
import logging
import threading
import time
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

# 配置日志
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('web_server.log')
    ]
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Tari-Cpu TPOOL分享池")

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载静态文件
app.mount("/static", StaticFiles(directory="web/static"), name="static")

# 配置模板
templates = Jinja2Templates(directory="web/templates")

# Redis连接配置
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# 缓存键名常量
CACHE_KEYS = {
    'POOL_STATS': 'cached:pool_stats',
    'ACTIVE_MINERS': 'cached:active_miners',
    'STRATUM_DATA': 'cached:stratum_data',
    'ONLINE_MINERS': 'cached:online_miners'
}

# 缓存过期时间（秒）
CACHE_EXPIRE = {
    'POOL_STATS': 30,      # 矿池状态缓存30秒
    'ACTIVE_MINERS': 10,   # 活跃矿工数缓存10秒
    'STRATUM_DATA': 5,     # stratum数据缓存5秒
    'ONLINE_MINERS': 10    # 在线矿工列表缓存10秒
}

# Pydantic模型
class Block(BaseModel):
    timestamp: str
    height: int
    type: str
    reward: str
    block_id: str
    is_valid: bool
    check_status: bool

class Miner(BaseModel):
    username: str
    hashrate: float
    xmr_share: int
    tari_share: int

class PoolStatus(BaseModel):
    hashrate_15m: float
    hashrate_1h: float
    hashrate_24h: float
    active_miners: int
    total_rewards_xmr: float
    total_rewards_tari: float
    total_paid_xmr: float
    total_paid_tari: float
    online_miners: List[Miner]

class UserInfo(BaseModel):
    username: str
    xmr_balance: float
    tari_balance: float
    xmr_payed: float
    tari_payed: float
    created_at: str
    current_hashrate: float
    xmr_wallet: str
    tari_wallet: str
    fee: float
    frozen_tari: float
    rewards: List[Dict[str, Any]]
    payments: List[Dict[str, Any]]

def get_cached_data(key: str, calculate_func, expire_time: int) -> Any:
    """获取缓存数据，如果不存在则计算并缓存"""
    cached_data = redis_client.get(key)
    if cached_data:
        return json.loads(cached_data)
    
    data = calculate_func()
    redis_client.setex(key, expire_time, json.dumps(data))
    return data

def calculate_pool_stats() -> Dict[str, float]:
    """计算矿池统计数据"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        cur.execute("""
            SELECT 
                SUM(CASE WHEN type = 'xmr' AND is_valid = TRUE THEN rewards ELSE 0 END) as total_rewards_xmr,
                SUM(CASE WHEN type = 'tari' AND is_valid = TRUE THEN rewards ELSE 0 END) as total_rewards_tari,
                (SELECT COALESCE(SUM(amount), 0) FROM payment WHERE type = 'xmr') as total_paid_xmr,
                (SELECT COALESCE(SUM(amount), 0) FROM payment WHERE type = 'tari' AND status = 'completed') as total_paid_tari
            FROM blocks
        """)
        stats = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return {
            'total_rewards_xmr': float(stats['total_rewards_xmr'] or 0),
            'total_rewards_tari': float(stats['total_rewards_tari'] or 0),
            'total_paid_xmr': float(stats['total_paid_xmr'] or 0),
            'total_paid_tari': float(stats['total_paid_tari'] or 0)
        }
    except Exception as e:
        logger.error(f"计算矿池统计数据失败: {str(e)}")
        return {
            'total_rewards_xmr': 0,
            'total_rewards_tari': 0,
            'total_paid_xmr': 0,
            'total_paid_tari': 0
        }

def get_cached_stratum_data() -> Dict[str, Any]:
    """获取缓存的stratum数据"""
    return get_cached_data(
        CACHE_KEYS['STRATUM_DATA'],
        read_stratum_data,
        CACHE_EXPIRE['STRATUM_DATA']
    )

def get_cached_active_miners() -> int:
    """获取缓存的活跃矿工数"""
    return get_cached_data(
        CACHE_KEYS['ACTIVE_MINERS'],
        lambda: len(set(worker.split(',')[4] for worker in get_cached_stratum_data()['workers'] if len(worker.split(',')) >= 5)),
        CACHE_EXPIRE['ACTIVE_MINERS']
    )

def get_cached_online_miners() -> List[Miner]:
    """获取缓存的在线矿工列表"""
    def calculate_online_miners():
        stratum_data = get_cached_stratum_data()
        miner_hashrates = {}
        
        for worker in stratum_data['workers']:
            try:
                parts = worker.split(',')
                if len(parts) >= 5:
                    username = parts[4]
                    hashrate = float(parts[3])
                    miner_hashrates[username] = miner_hashrates.get(username, 0) + hashrate
            except:
                continue
        
        pipe = redis_client.pipeline()
        for username in miner_hashrates:
            pipe.get(get_chain_key(username, 'xmr'))
            pipe.get(get_chain_key(username, 'tari'))
        redis_results = pipe.execute()
        
        online_miners = []
        for i, (username, hashrate) in enumerate(miner_hashrates.items()):
            xmr_count = int(redis_results[i*2] or 0)
            tari_count = int(redis_results[i*2+1] or 0)
            online_miners.append(Miner(
                username=format_username(username),
                hashrate=hashrate,
                xmr_share=xmr_count,
                tari_share=tari_count
            ))
        
        online_miners.sort(key=lambda x: x.xmr_share, reverse=True)
        return online_miners[:20]
    
    return get_cached_data(
        CACHE_KEYS['ONLINE_MINERS'],
        calculate_online_miners,
        CACHE_EXPIRE['ONLINE_MINERS']
    )

# 路由处理
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/u/")
async def user_search(username: Optional[str] = None):
    if username:
        return {"redirect": f"/u/{username}"}
    return {"redirect": "/"}

@app.get("/u/{username}", response_class=HTMLResponse)
async def user_page(request: Request, username: str):
    if len(username) > 100:
        raise HTTPException(status_code=400, detail="用户名长度超过100位")
    return templates.TemplateResponse("user.html", {"request": request, "username": username})

@app.get("/api/pool_status", response_model=PoolStatus)
async def pool_status():
    try:
        pool_stats = get_cached_data(
            CACHE_KEYS['POOL_STATS'],
            calculate_pool_stats,
            CACHE_EXPIRE['POOL_STATS']
        )
        stratum_data = get_cached_stratum_data()
        active_miners = get_cached_active_miners()
        online_miners = get_cached_online_miners()

        return PoolStatus(
            hashrate_15m=stratum_data['hashrate_15m'],
            hashrate_1h=stratum_data['hashrate_1h'],
            hashrate_24h=stratum_data['hashrate_24h'],
            active_miners=active_miners,
            total_rewards_xmr=pool_stats['total_rewards_xmr'],
            total_rewards_tari=pool_stats['total_rewards_tari'],
            total_paid_xmr=pool_stats['total_paid_xmr'],
            total_paid_tari=pool_stats['total_paid_tari'],
            online_miners=online_miners
        )
    except Exception as e:
        logger.error(f"获取矿池状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/user/{username}", response_model=UserInfo)
async def user_info(username: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        cur.execute("""
            SELECT username, xmr_balance, tari_balance, created_at, xmr_wallet, tari_wallet, fee
            FROM account 
            WHERE username = %s
        """, (username,))
        account = cur.fetchone()
        
        if not account:
            raise HTTPException(status_code=404, detail="用户不存在")
        
        # 计算18小时内的TARI冻结金额
        cur.execute("""
            SELECT COALESCE(SUM(reward), 0) as frozen_tari
            FROM rewards 
            WHERE username = %s 
            AND type = 'tari'
            AND created_at >= NOW() - INTERVAL '18 hours'
        """, (username,))
        frozen_result = cur.fetchone()
        frozen_tari = float(frozen_result['frozen_tari']) if frozen_result else 0

        # 计算已支付的TARI
        cur.execute("""
            SELECT COALESCE(SUM(amount), 0) as tari_payed
            FROM payment 
            WHERE username = %s 
            AND type = 'tari'
            AND status = 'completed'
        """, (username,))   
        tari_payed_result = cur.fetchone()
        tari_payed = float(tari_payed_result['tari_payed']) if tari_payed_result else 0

        # 计算已支付的XMR
        cur.execute("""
            SELECT COALESCE(SUM(amount), 0) as xmr_payed
            FROM payment 
            WHERE username = %s 
            AND type = 'xmr'
        """, (username,))
        xmr_payed_result = cur.fetchone()
        xmr_payed = float(xmr_payed_result['xmr_payed']) if xmr_payed_result else 0

        # 获取用户奖励历史
        cur.execute("""
            SELECT 
                r.block_height as height,
                r.type,
                r.reward as amount,
                r.shares,
                b.time as timestamp,
                b.total_shares
            FROM rewards r
            JOIN blocks b ON r.block_height = b.block_height
            WHERE r.username = %s 
            ORDER BY b.time DESC 
            LIMIT 50
        """, (username,))
        rewards = []
        for row in cur.fetchall():
            reward = dict(row)
            reward['amount'] = float(reward['amount'])
            reward['shares'] = float(reward['shares'])
            reward['total_shares'] = float(reward['total_shares'])
            rewards.append(reward)
        
        # 获取用户支付历史
        cur.execute("""
            SELECT 
                time as timestamp,
                txid,
                amount,
                type
            FROM payment 
            WHERE username = %s 
            ORDER BY time DESC 
            LIMIT 20
        """, (username,))
        payments = []
        for row in cur.fetchall():
            payment = dict(row)
            payment['amount'] = float(payment['amount'])
            payments.append(payment)
        
        # 获取用户当前算力
        current_hashrate = get_user_hashrate(username)

        cur.close()
        conn.close()
        
        return UserInfo(
            username=username,
            xmr_balance=float(account['xmr_balance']),
            tari_balance=float(account['tari_balance'])-frozen_tari,
            xmr_payed=xmr_payed,
            tari_payed=tari_payed,
            created_at=account['created_at'].isoformat(),
            current_hashrate=current_hashrate,
            xmr_wallet=account['xmr_wallet'],
            tari_wallet=account['tari_wallet'],
            fee=float(account['fee']),
            frozen_tari=frozen_tari,
            rewards=rewards,
            payments=payments
        )
    except Exception as e:
        logger.error(f"获取用户信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/blocks", response_model=List[Block])
async def get_blocks():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT time as timestamp, block_height as height, type, rewards as reward, 
                   block_id, is_valid, check_status
            FROM blocks
            ORDER BY time DESC
            LIMIT 100
        """)
        blocks = cursor.fetchall()
        cursor.close()
        conn.close()

        formatted_blocks = []
        for block in blocks:
            timestamp, height, block_type, reward, block_id, is_valid, check_status = block
            
            if timestamp:
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            if block_type == 'xmr':
                reward = f"{float(reward):.6f} XMR"
            else:  # TARI
                reward = f"{float(reward):.2f} XTM"
            
            formatted_blocks.append(Block(
                timestamp=timestamp,
                height=height,
                type=block_type,
                reward=reward,
                block_id=block_id,
                is_valid=is_valid,
                check_status=check_status
            ))

        return formatted_blocks
    except Exception as e:
        logger.error(f"获取区块列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hashrate/history")
async def get_hashrate_history(hours: int = 24):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT timestamp, hashrate
            FROM hashrate_history
            WHERE timestamp >= NOW() - INTERVAL '%s hours'
            ORDER BY timestamp ASC
        """, (hours,))
        
        history = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            'history': [{
                'timestamp': record[0].isoformat(),
                'hashrate': record[1]
            } for record in history]
        }
        
    except Exception as e:
        logger.error(f"获取算力历史数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 启动算力历史记录线程
def record_hashrate_history():
    while True:
        try:
            stratum_data = read_stratum_data()
            if not stratum_data:
                time.sleep(300)
                continue
                
            total_hashrate = stratum_data.get('hashrate_15m', 0)
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO hashrate_history (timestamp, hashrate)
                VALUES (NOW(), %s)
            """, (total_hashrate,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"记录算力历史数据: {total_hashrate/1000:.2f} KH/s")
            
        except Exception as e:
            logger.error(f"记录算力历史数据失败: {str(e)}")
            if 'conn' in locals():
                conn.close()
        
        time.sleep(300)

# 启动后台线程
hashrate_thread = threading.Thread(target=record_hashrate_history, daemon=True)
hashrate_thread.start()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080) 