 from flask import Flask, render_template
import psycopg2
import json
import logging
from datetime import datetime, timedelta
import redis

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 加载配置
def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        raise

# Redis配置
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# 数据库连接
def get_db_connection():
    config = load_config()
    return psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['database'],
        user=config['database']['user'],
        password=config['database']['password']
    )

def get_pool_stats():
    """获取矿池统计信息"""
    try:
        # 连接数据库
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 获取今日时间范围
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)
        
        # XMR 统计
        xmr_stats = {
            'active_miners': 0,
            'total_hashrate': 0,
            'blocks_today': 0,
            'total_blocks': 0
        }
        
        # TARI 统计
        tari_stats = {
            'active_miners': 0,
            'total_hashrate': 0,
            'blocks_today': 0,
            'total_blocks': 0
        }
        
        # 获取活跃矿工数
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        
        # XMR 活跃矿工
        xmr_keys = redis_client.keys('xmr:submit:*')
        xmr_stats['active_miners'] = len(xmr_keys)
        
        # TARI 活跃矿工
        tari_keys = redis_client.keys('tari:submit:*')
        tari_stats['active_miners'] = len(tari_keys)
        
        # 获取区块统计
        cur.execute("""
            SELECT type, 
                   COUNT(*) as total_blocks,
                   COUNT(CASE WHEN time >= %s AND time < %s THEN 1 END) as blocks_today
            FROM blocks 
            GROUP BY type
        """, (today, tomorrow))
        
        for row in cur.fetchall():
            if row[0] == 'xmr':
                xmr_stats['total_blocks'] = row[1]
                xmr_stats['blocks_today'] = row[2]
            elif row[0] == 'tari':
                tari_stats['total_blocks'] = row[1]
                tari_stats['blocks_today'] = row[2]
        
        # 获取最近区块
        cur.execute("""
            SELECT block_height, type, rewards, time
            FROM blocks
            ORDER BY time DESC
            LIMIT 10
        """)
        
        recent_blocks = []
        for row in cur.fetchall():
            recent_blocks.append({
                'height': row[0],
                'type': row[1],
                'reward': row[2],
                'time': row[3].strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return {
            'xmr_stats': xmr_stats,
            'tari_stats': tari_stats,
            'recent_blocks': recent_blocks
        }
        
    except Exception as e:
        logger.error(f"获取矿池统计信息失败: {str(e)}")
        return {
            'xmr_stats': {'active_miners': 0, 'total_hashrate': 0, 'blocks_today': 0, 'total_blocks': 0},
            'tari_stats': {'active_miners': 0, 'total_hashrate': 0, 'blocks_today': 0, 'total_blocks': 0},
            'recent_blocks': []
        }
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.route('/')
def index():
    """首页"""
    stats = get_pool_stats()
    return render_template('index.html', **stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)