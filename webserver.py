@app.route('/api/blocks')
def get_blocks():
    try:
        # 获取最近的区块记录
        cursor = conn.cursor()
        cursor.execute("""
            SELECT timestamp, height, type, reward, block_id, is_valid
            FROM blocks
            ORDER BY timestamp DESC
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
            if block_type == 'XMR':
                reward = f"{float(reward):.6f} XMR"
            else:  # TARI
                reward = f"{float(reward):.2f} TARI"
            
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

@app.route('/api/pool_status')
def get_pool_status():
    try:
        cursor = conn.cursor()
        
        # 获取总算力
        cursor.execute("""
            SELECT SUM(hashrate) as total_hashrate
            FROM miners
            WHERE last_seen > datetime('now', '-5 minutes')
        """)
        total_hashrate = cursor.fetchone()[0] or 0
        
        # 获取XMR余额
        cursor.execute("""
            SELECT SUM(reward) as total_xmr
            FROM blocks
            WHERE type = 'XMR'
            AND is_valid = 1
        """)
        xmr_balance = cursor.fetchone()[0] or 0
        
        # 获取TARI余额
        cursor.execute("""
            SELECT SUM(reward) as total_tari
            FROM blocks
            WHERE type = 'TARI'
            AND is_valid = 1
        """)
        tari_balance = cursor.fetchone()[0] or 0
        
        cursor.close()
        
        return jsonify({
            'total_hashrate': f"{total_hashrate/1000:.2f} KH/s",
            'xmr_balance': f"{xmr_balance:.6f} XMR",
            'tari_balance': f"{tari_balance:.2f} TARI"
        })
    except Exception as e:
        logger.error(f"获取矿池状态失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/user/<username>/hashrate')
def get_user_hashrate(username):
    try:
        # 计算用户名长度，如果超过10则取10
        n = min(len(username), 10)
        
        # 获取用户算力数据
        cursor = conn.cursor()
        cursor.execute("""
            SELECT hashrate, last_seen
            FROM miners
            WHERE SUBSTR(username, -%s) = SUBSTR(?, -%s)
            AND last_seen > datetime('now', '-5 minutes')
        """, (n, username, n))
        
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            hashrate, last_seen = result
            return jsonify({
                'hashrate': hashrate,
                'last_seen': last_seen
            })
        else:
            return jsonify({
                'hashrate': 0,
                'last_seen': None
            })
            
    except Exception as e:
        logger.error(f"获取用户算力失败: {str(e)}")
        return jsonify({'error': str(e)}), 500 