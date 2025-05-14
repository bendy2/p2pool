#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import DictCursor
import logging
import json
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        raise

def get_db_connection():
    config = load_config()
    return psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['database'],
        user=config['database']['user'],
        password=config['database']['password']
    )

def mark_block_invalid(block_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        # 开始事务
        cursor.execute("BEGIN")
        
        try:
            # 获取区块信息
            cursor.execute("""
                SELECT type, rewards, block_height
                FROM blocks
                WHERE block_id = %s
            """, (block_id,))
            
            block_info = cursor.fetchone()
            if not block_info:
                logger.error(f"未找到区块: {block_id}")
                return False
                
            block_type = block_info['type']
            rewards = block_info['rewards']
            block_height = block_info['block_height']
            
            # 获取该区块的所有奖励记录
            cursor.execute("""
                SELECT username, reward
                FROM rewards
                WHERE block_height = %s AND type = %s
            """, (block_height, block_type))
            
            rewards = cursor.fetchall()
            logger.info(f"找到 {len(rewards)} 条奖励记录")
            
            # 更新奖励记录
            for reward in rewards:
                old_reward = float(reward['reward'])
                cursor.execute("""
                    UPDATE rewards
                    SET reward = 0
                    WHERE block_height = %s 
                    AND type = %s
                    AND username = %s
                """, (block_height, block_type, reward['username']))
                
                # 更新用户余额
                if block_type == 'xmr':
                    cursor.execute("""
                        UPDATE account
                        SET xmr_balance = xmr_balance - %s
                        WHERE username = %s
                    """, (old_reward, reward['username']))
                else:  # tari
                    cursor.execute("""
                        UPDATE account
                        SET tari_balance = tari_balance - %s
                        WHERE username = %s
                    """, (old_reward, reward['username']))
            
            # 更新区块状态
            cursor.execute("""
                UPDATE blocks
                SET is_valid = FALSE,
                    rewards = 0
                WHERE block_id = %s
            """, (block_id,))
            
            # 提交事务
            conn.commit()
            logger.info(f"成功将区块 {block_id} 标记为无效")
            return True
            
        except Exception as e:
            # 回滚事务
            conn.rollback()
            logger.error(f"标记区块无效时发生错误: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"数据库操作失败: {str(e)}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    if len(sys.argv) != 2:
        print("使用方法: python delete_block.py <block_id>")
        sys.exit(1)
        
    block_id = sys.argv[1]
    if mark_block_invalid(block_id):
        print(f"成功将区块 {block_id} 标记为无效")
    else:
        print(f"标记区块 {block_id} 无效失败")
        sys.exit(1)

if __name__ == "__main__":
    main() 