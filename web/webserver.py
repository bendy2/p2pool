from flask import Flask, render_template, jsonify, request, redirect, url_for
import redis
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import json
import os
import logging

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
        return {
            'hashrate_15m': 0,
            'hashrate_1h': 0,
            'hashrate_24h': 0,
            'workers': []
        }

def get_user_hashrate(username):
    stratum_data = read_stratum_data()
    total_hashrate = 0
    
    for worker in stratum_data['workers']:
        try:
            # 解析worker数据: "IP:PORT,HASHRATE,SHARES,DIFFICULTY,USERNAME"
            parts = worker.split(',')
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
            
            # 统计不同IP的矿工数
            unique_ips = set()
            for worker in workers:
                try:
                    # 解析worker数据: "IP:PORT,HASHRATE,SHARES,DIFFICULTY,USERNAME"
                    parts = worker.split(',')
                    if len(parts) >= 5:
                        ip = parts[0].split(':')[0]  # 只取IP部分
                        unique_ips.add(ip)
                except:
                    continue
            
            # 更新缓存，设置10秒过期
            count = len(unique_ips)
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

        return jsonify({
            'hashrate_15m': stratum_data['hashrate_15m'],
            'hashrate_1h': stratum_data['hashrate_1h'],
            'hashrate_24h': stratum_data['hashrate_24h'],
            'active_miners': active_miners,
            'total_rewards_xmr': total_rewards_xmr,
            'total_rewards_tari': total_rewards_tari
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
            SELECT username, xmr_balance, tari_balance, created_at
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
            SELECT time as timestamp, block_height as height, type, rewards as reward, block_id, is_valid
            FROM blocks
            ORDER BY time DESC
            LIMIT 50
        """)
        blocks = cursor.fetchall()
        cursor.close()

        # 格式化区块数据
        formatted_blocks = []
        for block in blocks:
            timestamp, height, block_type, reward, block_id, is_valid = block
            
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
                'is_valid': is_valid
            })

        return jsonify(formatted_blocks)
    except Exception as e:
        logger.error(f"获取区块列表失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True) 