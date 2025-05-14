from flask import Flask, render_template, jsonify
import redis
import sqlite3
from datetime import datetime
import json
import os

app = Flask(__name__)

# Redis连接配置
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# 数据库连接配置
def get_db_connection():
    conn = sqlite3.connect('pool_data.db')
    conn.row_factory = sqlite3.Row
    return conn

def read_stratum_data():
    try:
        with open('/tmp/p2pool/local/stratum', 'r') as f:
            data = json.load(f)
            return {
                'hashrate_15m': data.get('hashrate_15m', 0),
                'hashrate_1h': data.get('hashrate_1h', 0),
                'hashrate_24h': data.get('hashrate_24h', 0)
            }
    except Exception as e:
        print(f"读取stratum数据失败: {str(e)}")
        return {
            'hashrate_15m': 0,
            'hashrate_1h': 0,
            'hashrate_24h': 0
        }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/pool_status')
def pool_status():
    try:
        # 从Redis获取实时数据
        active_miners = redis_client.get('pool:active_miners') or 0
        total_rewards = redis_client.get('pool:total_rewards') or 0

        # 从stratum文件读取算力数据
        stratum_data = read_stratum_data()

        return jsonify({
            'hashrate_15m': stratum_data['hashrate_15m'],
            'hashrate_1h': stratum_data['hashrate_1h'],
            'hashrate_24h': stratum_data['hashrate_24h'],
            'active_miners': int(active_miners),
            'total_rewards': float(total_rewards)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/blocks/<chain_type>')
def get_blocks(chain_type):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 从数据库获取区块数据
        cursor.execute('''
            SELECT height, timestamp, reward 
            FROM blocks 
            WHERE chain_type = ? 
            ORDER BY height DESC 
            LIMIT 20
        ''', (chain_type,))
        
        blocks = []
        for row in cursor.fetchall():
            blocks.append({
                'height': row['height'],
                'timestamp': row['timestamp'],
                'reward': row['reward']
            })
        
        conn.close()
        return jsonify({'blocks': blocks})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True) 