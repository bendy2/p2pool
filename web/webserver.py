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

def get_user_hashrate(username):
    stratum_data = read_stratum_data()
    total_hashrate = 0
    
    for worker in stratum_data['workers']:
        try:
            # 解析worker数据: "IP:PORT,HASHRATE,SHARES,DIFFICULTY,USERNAME"
            parts = worker.split(',')
            n = min(len(username), 10)
            if len(parts) >= 5 and parts[4] == username:
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
        # 获取活跃矿工数（带缓存）
        active_miners = get_active_miners()

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

        cur.close()
        conn.close()

        # 从stratum文件读取算力数据
        stratum_data = read_stratum_data()
        
        # 获取在线矿工列表
        online_miners = []
        miner_hashrates = {}  # 用于合并同一用户的算力
        
        for worker in stratum_data['workers']:
            try:
                parts = worker.split(',')
                if len(parts) >= 5:
                    username = parts[4]
                    hashrate = float(parts[3])
                    
                    # 合并同一用户的算力
                    if username in miner_hashrates:
                        miner_hashrates[username] += hashrate
                    else:
                        miner_hashrates[username] = hashrate
            except:
                continue
        
        # 转换为列表并格式化用户名
        online_miners = [
            {
                'username': format_username(username),
                'hashrate': hashrate
            }
            for username, hashrate in miner_hashrates.items()
        ]
        
        # 按算力排序并只取前20名
        online_miners.sort(key=lambda x: x['hashrate'], reverse=True)
        online_miners = online_miners[:20]

        return jsonify({
            'hashrate_15m': stratum_data['hashrate_15m'],
            'hashrate_1h': stratum_data['hashrate_1h'],
            'hashrate_24h': stratum_data['hashrate_24h'],
            'active_miners': active_miners,
            'total_rewards_xmr': total_rewards_xmr,
            'total_rewards_tari': total_rewards_tari,
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
            LIMIT 20
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
            'tari_balance': float(account['tari_balance']),
            'created_at': account['created_at'].isoformat(),
            'current_hashrate': current_hashrate,
            'xmr_wallet': account['xmr_wallet'],
            'tari_wallet': account['tari_wallet'],
            'fee': float(account['fee']),
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
            LIMIT 50
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