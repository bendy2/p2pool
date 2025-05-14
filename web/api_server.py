import json
from log import logger

def create_account(username, xmr_wallet, tari_wallet):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 从配置文件读取fee值
        with open('config.json', 'r') as f:
            config = json.load(f)
            fee = config.get('fee', 0.08)
        
        cur.execute("""
            INSERT INTO account (username, xmr_wallet, tari_wallet, fee)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (username) DO UPDATE
            SET xmr_wallet = EXCLUDED.xmr_wallet,
                tari_wallet = EXCLUDED.tari_wallet,
                fee = EXCLUDED.fee
            RETURNING username
        """, (username, xmr_wallet, tari_wallet, fee))
        
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return result[0] if result else None
    except Exception as e:
        logger.error(f"创建账户失败: {str(e)}")
        return None

def add_block(block_height, rewards, time, total_shares):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 计算value值
        value = rewards / total_shares if total_shares > 0 else 0
        
        cur.execute("""
            INSERT INTO blocks (block_height, rewards, time, total_shares, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (block_height) DO UPDATE
            SET rewards = EXCLUDED.rewards,
                time = EXCLUDED.time,
                total_shares = EXCLUDED.total_shares,
                value = EXCLUDED.value
            RETURNING block_height
        """, (block_height, rewards, time, total_shares, value))
        
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return result[0] if result else None
    except Exception as e:
        logger.error(f"添加区块失败: {str(e)}")
        return None

def add_reward(block_height, username, shares, reward_type):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 获取区块value值和用户fee值
        cur.execute("""
            SELECT b.value, a.fee
            FROM blocks b
            CROSS JOIN account a
            WHERE b.block_height = %s AND a.username = %s
        """, (block_height, username))
        
        result = cur.fetchone()
        if not result:
            return None
            
        value, fee = result
        
        # 计算实际奖励金额：shares * value * (1 - fee)
        actual_reward = shares * value * (1 - fee)
        
        cur.execute("""
            INSERT INTO rewards (block_height, username, shares, type, reward)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (block_height, username, type) DO UPDATE
            SET shares = EXCLUDED.shares,
                reward = EXCLUDED.reward
            RETURNING block_height, username, type
        """, (block_height, username, shares, reward_type, actual_reward))
        
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return result if result else None
    except Exception as e:
        logger.error(f"添加奖励失败: {str(e)}")
        return None 