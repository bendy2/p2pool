from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import redis
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import json
import os
import logging
import asyncio
import web
from typing import List, Optional, Dict, Any

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

app = FastAPI(title="Tari-Cpu TPOOL")

# 挂载静态文件
app.mount("/static", StaticFiles(directory="static"), name="static")

# 配置模板
templates = Jinja2Templates(directory="templates")

# Redis连接配置
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# 加载配置文件
def load_config():
    try:
        with open('../config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        raise

config = load_config()

# 数据库连接配置
def get_db_connection():
    return psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['database'],
        user=config['database']['user'],
        password=config['database']['password']
    )

# 数据模型
class Block(BaseModel):
    height: int
    timestamp: int
    type: str
    reward: float
    status: str
    block_id: Optional[str] = None
    is_valid: Optional[bool] = None

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

# 工具函数
async def read_stratum_data():
    try:
        with open('./api/local/stratum', 'r') as f:
            data = json.load(f)
            return {
                'hashrate_15m': data.get('hashrate_15m', 0),
                'hashrate_1h': data.get('hashrate_1h', 0),
                'hashrate_24h': data.get('hashrate_24h', 0),
                'workers': data.get('workers', [])
            }
    except Exception as e:
        logger.error(f"读取stratum数据失败: {str(e)}")
        return None

def get_chain_key(username: str, chain: str) -> str:
    xmr_prefix = "xmr:submit:"
    tari_prefix = "tari:submit:"
    if chain.lower() == 'xmr':
        return f"{xmr_prefix}{username}"
    else:
        return f"{tari_prefix}{username}"

def format_username(username: str) -> str:
    if len(username) <= 20:
        prefix = username[:4].ljust(4, '*')
        return f"{prefix}****"
    else:
        suffix = username[-4:]
        return f"****{suffix}"

# 路由处理
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/u/")
async def user_search(username: Optional[str] = None):
    if username:
        return RedirectResponse(url=f"/u/{username}")
    return RedirectResponse(url="/")

@app.get("/u/{username}", response_class=HTMLResponse)
async def user_page(request: Request, username: str):
    if len(username) > 100:
        raise HTTPException(status_code=400, detail="用户名长度超过100位")
    return templates.TemplateResponse("user.html", {"request": request, "username": username})

@app.get("/api/pool_status", response_model=PoolStatus)
async def pool_status():
    try:
        # 获取活跃矿工数
        active_miners = await get_active_miners()

        # 从数据库获取总奖励
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 获取XMR总奖励
        cur.execute("""
            SELECT COALESCE(SUM(rewards), 0) as total
            FROM blocks 
            WHERE type = 'xmr' AND is_valid = TRUE
        """)
        total_rewards_xmr = float(cur.fetchone()['total'] or 0)

        # 获取TARI总奖励
        cur.execute("""
            SELECT COALESCE(SUM(rewards), 0) as total
            FROM blocks 
            WHERE type = 'tari' AND is_valid = TRUE
        """)
        total_rewards_tari = float(cur.fetchone()['total'] or 0)

        # 获取XMR已支付金额
        cur.execute("""
            SELECT COALESCE(SUM(amount), 0) as total_paid
            FROM payment 
            WHERE type = 'xmr'
        """)
        total_paid_xmr = float(cur.fetchone()[0] or 0)

        # 获取TARI已支付金额
        cur.execute("""
            SELECT COALESCE(SUM(amount), 0) as total_paid
            FROM payment 
            WHERE type = 'tari' and status = 'completed'
        """)
        total_paid_tari = float(cur.fetchone()[0] or 0)

        cur.close()
        conn.close()

        # 从stratum文件读取算力数据
        stratum_data = await read_stratum_data()
        
        # 获取在线矿工列表
        online_miners = []
        miner_hashrates = {}
        
        for worker in stratum_data['workers']:
            try:
                parts = worker.split(',')
                if len(parts) >= 5:
                    username = parts[4]
                    hashrate = float(parts[3])
                    
                    if username in miner_hashrates:
                        miner_hashrates[username] += hashrate
                    else:
                        miner_hashrates[username] = hashrate
            except:
                continue
        
        for username, hashrate in miner_hashrates.items():
            xmr_key = get_chain_key(username, 'xmr')
            tari_key = get_chain_key(username, 'tari')
            xmr_count = int(redis_client.get(xmr_key) or 0)
            tari_count = int(redis_client.get(tari_key) or 0)
            online_miners.append(Miner(
                username=format_username(username),
                hashrate=hashrate,
                xmr_share=xmr_count,
                tari_share=tari_count
            ))
        
        online_miners.sort(key=lambda x: x.xmr_share, reverse=True)
        online_miners = online_miners[:20]

        return PoolStatus(
            hashrate_15m=stratum_data['hashrate_15m'],
            hashrate_1h=stratum_data['hashrate_1h'],
            hashrate_24h=stratum_data['hashrate_24h'],
            active_miners=active_miners,
            total_rewards_xmr=total_rewards_xmr,
            total_rewards_tari=total_rewards_tari,
            total_paid_xmr=total_paid_xmr,
            total_paid_tari=total_paid_tari,
            online_miners=online_miners
        )
    except Exception as e:
        logger.error(f"获取矿池状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/blocks", response_model=List[Block])
async def get_blocks():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        cur.execute("""
            SELECT height, timestamp, type, rewards, block_id, is_valid
            FROM blocks
            ORDER BY timestamp DESC
            LIMIT 50
        """)
        
        blocks = []
        for row in cur.fetchall():
            blocks.append(Block(
                height=row['height'],
                timestamp=row['timestamp'],
                type=row['type'],
                reward=float(row['rewards']),
                status='有效' if row['is_valid'] else '无效',
                block_id=row['block_id'],
                is_valid=row['is_valid']
            ))
        
        cur.close()
        conn.close()
        
        return blocks
    except Exception as e:
        logger.error(f"获取区块数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hashrate/history")
async def get_hashrate_history():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        cur.execute("""
            SELECT timestamp, hashrate
            FROM hashrate_history
            ORDER BY timestamp DESC
            LIMIT 100
        """)
        
        history = [{'timestamp': row['timestamp'], 'hashrate': float(row['hashrate'])} 
                  for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {'history': history}
    except Exception as e:
        logger.error(f"获取算力历史失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 启动服务器
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 