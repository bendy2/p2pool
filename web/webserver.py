from flask import Flask, render_template, jsonify, request, redirect, url_for
import redis
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import json
import os
import logging
import threading
import time

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

app = Flask(__name__)

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

def get_cached_data(key, calculate_func, expire_time):
    """获取缓存数据，如果不存在则计算并缓存"""
    cached_data = redis_client.get(key)
    if cached_data:
        return json.loads(cached_data)
    
    data = calculate_func()
    redis_client.setex(key, expire_time, json.dumps(data))
    return data

def calculate_pool_stats():
    """计算矿池统计数据"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 使用单个查询获取所有统计数据
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

def get_cached_stratum_data():
    """获取缓存的stratum数据"""
    return get_cached_data(
        CACHE_KEYS['STRATUM_DATA'],
        read_stratum_data,
        CACHE_EXPIRE['STRATUM_DATA']
    )

def get_cached_active_miners():
    """获取缓存的活跃矿工数"""
    return get_cached_data(
        CACHE_KEYS['ACTIVE_MINERS'],
        lambda: len(set(worker.split(',')[4] for worker in get_cached_stratum_data()['workers'] if len(worker.split(',')) >= 5)),
        CACHE_EXPIRE['ACTIVE_MINERS']
    )

def get_cached_online_miners():
    """获取缓存的在线矿工列表"""
    def calculate_online_miners():
        stratum_data = get_cached_stratum_data()
        miner_hashrates = {}
        
        # 收集所有矿工数据
        for worker in stratum_data['workers']:
            try:
                parts = worker.split(',')
                if len(parts) >= 5:
                    username = parts[4]
                    hashrate = float(parts[3])
                    miner_hashrates[username] = miner_hashrates.get(username, 0) + hashrate
            except:
                continue
        
        # 批量获取Redis数据
        pipe = redis_client.pipeline()
        for username in miner_hashrates:
            pipe.get(get_chain_key(username, 'xmr'))
            pipe.get(get_chain_key(username, 'tari'))
        redis_results = pipe.execute()
        
        # 处理结果
        online_miners = []
        for i, (username, hashrate) in enumerate(miner_hashrates.items()):
            xmr_count = int(redis_results[i*2] or 0)
            tari_count = int(redis_results[i*2+1] or 0)
            online_miners.append({
                'username': format_username(username),
                'hashrate': hashrate,
                'xmr_share': xmr_count,
                'tari_share': tari_count
            })
        
        # 按算力排序并只取前20名
        online_miners.sort(key=lambda x: x['xmr_share'], reverse=True)
        return online_miners[:20]
    
    return get_cached_data(
        CACHE_KEYS['ONLINE_MINERS'],
        calculate_online_miners,
        CACHE_EXPIRE['ONLINE_MINERS']
    )

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

def read_stratum_data():
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
    """获取Redis键名"""
    xmr_prefix = "xmr:submit:"
    tari_prefix = "tari:submit:"
    if chain.lower() == 'xmr':
        return f"{xmr_prefix}{username}"
    else:
        return f"{tari_prefix}{username}"

def get_user_hashrate(username):
    stratum_data = read_stratum_data()
    total_hashrate = 0

    for worker in stratum_data['workers']:
        try:
            # 解析worker数据: "IP:PORT,HASHRATE,SHARES,DIFFICULTY,USERNAME"
            parts = worker.split(',')
            check_str = username[:5]
            if check_str in parts[4]:
                total_hashrate += int(parts[3])
        except:
            continue
    
    return total_hashrate

# 缓存活跃矿工数
def get_active_miners():
    # 尝试从缓存获取
    cached_count = redis_client.get('cached:active_miners')
    if cached_count is not None:
        return int(cached_count)
    
    try:
        # 从stratum文件读取数据
        with open('./api/local/stratum', 'r') as f:
            data = json.load(f)
            workers = data.get('workers', [])
            
            # 统计不同用户名的矿工数
            unique_users = set()
            for worker in workers:
                try:
                    # 解析worker数据: "IP:PORT,HASHRATE,SHARES,DIFFICULTY,USERNAME"
                    parts = worker.split(',')
                    if len(parts) >= 5:
                        username = parts[4]  # 获取用户名
                        unique_users.add(username)
                except:
                    continue
            
            # 更新缓存，设置10秒过期
            count = len(unique_users)
            redis_client.setex('cached:active_miners', 10, count)
            return count
    except Exception as e:
        logger.error(f"统计活跃矿工数失败: {str(e)}")
        return 0

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/u/')
def user_search():
    username = request.args.get('username')
    if username:
        return redirect(url_for('user_page', username=username))
    return redirect(url_for('index'))

@app.route('/u/<username>')
def user_page(username):
    if len(username) > 100:
        return jsonify({'error': "用户名长度超过100位"}), 400
    return render_template('user.html', username=username)

def format_username(username):
    """格式化用户名显示"""
    if len(username) <= 20:
        # 长度不足20位，显示前4位（不足补*）加4个*
        prefix = username[:4].ljust(4, '*')
        return f"{prefix}****"
    else:
        # 长度超过20位，显示4个*加后4位
        suffix = username[-4:]
        return f"****{suffix}"

@app.route('/api/pool_status')
def pool_status():
    try:
        # 获取所有缓存数据
        pool_stats = get_cached_data(
            CACHE_KEYS['POOL_STATS'],
            calculate_pool_stats,
            CACHE_EXPIRE['POOL_STATS']
        )
        stratum_data = get_cached_stratum_data()
        active_miners = get_cached_active_miners()
        online_miners = get_cached_online_miners()

        return jsonify({
            'hashrate_15m': stratum_data['hashrate_15m'],
            'hashrate_1h': stratum_data['hashrate_1h'],
            'hashrate_24h': stratum_data['hashrate_24h'],
            'active_miners': active_miners,
            'total_rewards_xmr': pool_stats['total_rewards_xmr'],
            'total_rewards_tari': pool_stats['total_rewards_tari'],
            'total_paid_xmr': pool_stats['total_paid_xmr'],
            'total_paid_tari': pool_stats['total_paid_tari'],
            'online_miners': online_miners
        })
    except Exception as e:
        logger.error(f"获取矿池状态失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/user/<username>')
def user_info(username):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 获取用户账户信息
        cur.execute("""
            SELECT username, xmr_balance, tari_balance, created_at, xmr_wallet, tari_wallet, fee
            FROM account 
            WHERE username = %s
        """, (username,))
        account = cur.fetchone()
        
        if not account:
            return jsonify({'error': '用户不存在'}), 404
        
            
        # 计算18小时内的 TARI 冻结金额
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
        #计算已支付的XMR
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
        
        return jsonify({
            'username': username,
            'xmr_balance': float(account['xmr_balance']),
            'tari_balance': float(account['tari_balance'])-frozen_tari,
            'xmr_payed': xmr_payed,
            'tari_payed': tari_payed,
            'created_at': account['created_at'].isoformat(),
            'current_hashrate': current_hashrate,
            'xmr_wallet': account['xmr_wallet'],
            'tari_wallet': account['tari_wallet'],
            'fee': float(account['fee']),
            'frozen_tari': frozen_tari,  # 添
            'rewards': rewards,
            'payments': payments
        })
    except Exception as e:
        logger.error(f"获取用户信息失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/blocks')
def get_blocks():
    try:
        # 获取最近的区块记录
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

        # 格式化区块数据
        formatted_blocks = []
        for block in blocks:
            timestamp, height, block_type, reward, block_id, is_valid, check_status = block
            
            # 格式化时间
            if timestamp:
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            # 格式化奖励
            if block_type == 'xmr':
                reward = f"{float(reward):.6f} XMR"
            else:  # TARI
                reward = f"{float(reward):.2f} XTM"
            
            formatted_blocks.append({
                'timestamp': timestamp,
                'height': height,
                'type': block_type,
                'reward': reward,
                'block_id': block_id,
                'is_valid': is_valid,
                'check_status': check_status
            })

        return jsonify(formatted_blocks)
    except Exception as e:
        logger.error(f"获取区块列表失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 创建blocks表
    cur.execute('''
        CREATE TABLE IF NOT EXISTS blocks (
            id SERIAL PRIMARY KEY,
            block_id VARCHAR(255),
            height INTEGER,
            timestamp TIMESTAMP,
            type VARCHAR(10),
            reward DECIMAL(20, 8),
            is_valid BOOLEAN DEFAULT TRUE,
            check_status BOOLEAN DEFAULT FALSE
        )
    ''')
    
    # 为现有数据设置check_status
    cur.execute('''
        UPDATE blocks 
        SET check_status = TRUE 
        WHERE check_status IS NULL
    ''')
    
    conn.commit()
    cur.close()
    conn.close()

def record_hashrate_history():
    while True:
        try:
            # 从stratum文件读取算力数据
            stratum_data = read_stratum_data()
            if not stratum_data:
                time.sleep(300)
                continue
                
            # 获取15分钟平均算力
            total_hashrate = stratum_data.get('hashrate_15m', 0)
            
            # 记录到数据库
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
        
        # 每5分钟记录一次
        time.sleep(300)

# 启动算力历史记录线程
hashrate_thread = threading.Thread(target=record_hashrate_history, daemon=True)
hashrate_thread.start()

@app.route('/api/hashrate/history')
def get_hashrate_history():
    try:
        # 获取查询参数
        hours = request.args.get('hours', default=24, type=int)  # 默认显示24小时
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 计算时间范围
        cursor.execute("""
            SELECT timestamp, hashrate
            FROM hashrate_history
            WHERE timestamp >= NOW() - INTERVAL '%s hours'
            ORDER BY timestamp ASC
        """, (hours,))
        
        history = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            'history': [{
                'timestamp': record[0].isoformat(),
                'hashrate': record[1]
            } for record in history]
        })
        
    except Exception as e:
        logger.error(f"获取算力历史数据失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True) 