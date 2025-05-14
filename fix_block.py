#!/usr/bin/env python3
import sqlite3
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fix_block():
    try:
        conn = sqlite3.connect('p2pool.db')
        cursor = conn.cursor()
        
        # 开始事务
        cursor.execute("BEGIN TRANSACTION")
        
        try:
            # 1. 更新区块奖励
            cursor.execute("""
                UPDATE blocks
                SET reward = 13850
                WHERE height = 6379 AND type = 'TARI'
            """)
            
            if cursor.rowcount == 0:
                logger.error("未找到指定区块")
                return False
                
            # 2. 获取该区块的所有奖励记录
            cursor.execute("""
                SELECT username, reward
                FROM rewards
                WHERE block_height = 6379 AND type = 'TARI'
            """)
            
            rewards = cursor.fetchall()
            logger.info(f"找到 {len(rewards)} 条奖励记录")
            
            # 3. 更新奖励记录
            for username, old_reward in rewards:
                new_reward = old_reward * 13.85
                cursor.execute("""
                    UPDATE rewards
                    SET reward = ?
                    WHERE block_height = 6379 
                    AND type = 'TARI'
                    AND username = ?
                """, (new_reward, username))
                
                # 4. 更新用户余额
                cursor.execute("""
                    UPDATE users
                    SET tari_balance = tari_balance - ? + ?
                    WHERE username = ?
                """, (old_reward, new_reward, username))
            
            # 5. 记录操作日志
            cursor.execute("""
                INSERT INTO block_operations (
                    block_id,
                    operation_type,
                    operation_time,
                    details
                ) VALUES (?, ?, ?, ?)
            """, (
                'TARI-6379',
                'FIX_REWARD',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                f"修复区块 6379 的奖励金额为 13850 XTM，用户奖励调整为 13.85 倍"
            ))
            
            # 提交事务
            conn.commit()
            logger.info("成功修复区块奖励")
            return True
            
        except Exception as e:
            # 回滚事务
            conn.rollback()
            logger.error(f"修复区块时发生错误: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"数据库操作失败: {str(e)}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    if fix_block():
        print("区块修复成功")
    else:
        print("区块修复失败")
        exit(1)

if __name__ == "__main__":
    main() 