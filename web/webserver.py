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
                total_hashrate += int(parts[1])
        except:
            continue
    
    return total_hashrate

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
        # 从Redis获取XMR链的活跃矿工数量
        active_miners = redis_client.scard('xmr:active_miners') or 0

        # 从数据库获取总奖励
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 获取XMR总奖励
        cur.execute("""
            SELECT COALESCE(SUM(rewards), 0) as total
            FROM blocks 
            WHERE type = 'xmr'
        """)
        total_rewards_xmr = float(cur.fetchone()['total'] or 0)

        # 获取TARI总奖励
        cur.execute("""
            SELECT COALESCE(SUM(rewards), 0) as total
            FROM blocks 
            WHERE type = 'tari'
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
            'active_miners': int(active_miners),
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
            SELECT * FROM account 
            WHERE username = %s
        """, (username,))
        account = cur.fetchone()
        
        if not account:
            return jsonify({'error': '用户不存在'}), 404
        
        # 获取用户奖励历史
        cur.execute("""
            SELECT r.block_height as height, r.type, r.reward as amount, r.shares, b.time as timestamp
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
            rewards.append(reward)
        
        # 获取用户支付历史
        cur.execute("""
            SELECT time as timestamp, txid, amount, type
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
            'current_hashrate': current_hashrate,
            'rewards': rewards,
            'payments': payments
        })
    except Exception as e:
        logger.error(f"获取用户信息失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/blocks/<chain_type>')
def get_blocks(chain_type):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 从数据库获取区块数据
        cur.execute("""
            SELECT block_height as height, time as timestamp, rewards as reward 
            FROM blocks 
            WHERE type = %s 
            ORDER BY block_height DESC 
            LIMIT 20
        """, (chain_type,))
        
        blocks = []
        for row in cur.fetchall():
            block = dict(row)
            block['reward'] = float(block['reward'])
            blocks.append(block)
        
        cur.close()
        conn.close()
        return jsonify({'blocks': blocks})
    except Exception as e:
        logger.error(f"获取区块数据失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True) 