#!/usr/bin/env python3
import sqlite3
import sys
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def mark_block_invalid(block_id):
    try:
        conn = sqlite3.connect('p2pool.db')
        cursor = conn.cursor()
        
        # 开始事务
        cursor.execute("BEGIN TRANSACTION")
        
        try:
            # 获取区块信息
            cursor.execute("""
                SELECT type, reward, height
                FROM blocks
                WHERE block_id = ?
            """, (block_id,))
            
            block_info = cursor.fetchone()
            if not block_info:
                logger.error(f"未找到区块: {block_id}")
                return False
                
            block_type, reward, height = block_info
            
            # 获取该区块的所有奖励记录
            cursor.execute("""
                SELECT username, reward
                FROM rewards
                WHERE block_id = ?
            """, (block_id,))
            
            rewards = cursor.fetchall()
            logger.info(f"找到 {len(rewards)} 条奖励记录")
            
            # 删除奖励记录
            cursor.execute("""
                DELETE FROM rewards
                WHERE block_id = ?
            """, (block_id,))
            
            logger.info(f"已删除 {len(rewards)} 条奖励记录")
            
            # 更新区块状态
            cursor.execute("""
                UPDATE blocks
                SET is_valid = 0,
                    reward = 0
                WHERE block_id = ?
            """, (block_id,))
            
            # 更新用户余额
            if block_type == 'XMR':
                cursor.execute("""
                    UPDATE users
                    SET xmr_balance = xmr_balance - ?
                    WHERE xmr_balance > 0
                """, (reward,))
            else:  # TARI
                cursor.execute("""
                    UPDATE users
                    SET tari_balance = tari_balance - ?
                    WHERE tari_balance > 0
                """, (reward,))
            
            # 记录操作日志
            cursor.execute("""
                INSERT INTO block_operations (
                    block_id,
                    operation_type,
                    operation_time,
                    details
                ) VALUES (?, ?, ?, ?)
            """, (
                block_id,
                'MARK_INVALID',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                f"区块 {height} 被标记为无效，类型: {block_type}，删除了 {len(rewards)} 条奖励记录"
            ))
            
            # 输出受影响的用户信息
            for username, reward_amount in rewards:
                logger.info(f"用户 {username} 的奖励 {reward_amount} {block_type} 已被删除")
            
            # 提交事务
            conn.commit()
            logger.info(f"成功标记区块 {block_id} 为无效")
            return True
            
        except Exception as e:
            # 回滚事务
            conn.rollback()
            logger.error(f"处理区块 {block_id} 时发生错误: {str(e)}")
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
        print(f"区块 {block_id} 已成功标记为无效")
    else:
        print(f"处理区块 {block_id} 失败")
        sys.exit(1)

if __name__ == "__main__":
    main() 