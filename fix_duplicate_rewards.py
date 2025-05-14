#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import DictCursor
import logging
import json
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fix_rewards.log')
    ]
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

def fix_duplicate_rewards():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 1. 查找重复的奖励记录
        cur.execute("""
            WITH duplicates AS (
                SELECT 
                    block_height,
                    type,
                    username,
                    COUNT(*) as count,
                    MIN(id) as keep_id
                FROM rewards
                GROUP BY block_height, type, username
                HAVING COUNT(*) > 1
            )
            SELECT r.*
            FROM rewards r
            JOIN duplicates d ON 
                r.block_height = d.block_height AND 
                r.type = d.type AND 
                r.username = d.username AND 
                r.id != d.keep_id
            ORDER BY r.block_height, r.type, r.username, r.id
        """)
        
        duplicates = cur.fetchall()
        logger.info(f"找到 {len(duplicates)} 条重复的奖励记录")
        
        # 2. 删除重复记录
        for dup in duplicates:
            cur.execute("""
                DELETE FROM rewards 
                WHERE id = %s
            """, (dup['id'],))
            logger.info(f"删除重复记录: 用户={dup['username']}, 区块={dup['block_height']}, 类型={dup['type']}")
        
        # 3. 重新计算用户余额
        cur.execute("""
            WITH user_balances AS (
                SELECT 
                    username,
                    SUM(CASE WHEN type = 'xmr' THEN reward ELSE 0 END) as xmr_balance,
                    SUM(CASE WHEN type = 'tari' THEN reward ELSE 0 END) as tari_balance
                FROM rewards
                GROUP BY username
            )
            UPDATE account a
            SET 
                xmr_balance = COALESCE(ub.xmr_balance, 0),
                tari_balance = COALESCE(ub.tari_balance, 0)
            FROM user_balances ub
            WHERE a.username = ub.username
        """)
        
        # 4. 提交更改
        conn.commit()
        logger.info("成功修复重复记录并更新用户余额")
        
        # 5. 验证修复结果
        cur.execute("""
            WITH duplicates AS (
                SELECT 
                    block_height,
                    type,
                    username,
                    COUNT(*) as count
                FROM rewards
                GROUP BY block_height, type, username
                HAVING COUNT(*) > 1
            )
            SELECT COUNT(*) as remaining_duplicates
            FROM duplicates
        """)
        
        remaining = cur.fetchone()['remaining_duplicates']
        if remaining == 0:
            logger.info("验证通过：没有发现重复记录")
        else:
            logger.warning(f"警告：仍然存在 {remaining} 组重复记录")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"修复过程中出错: {str(e)}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        raise

if __name__ == '__main__':
    logger.info("开始修复重复奖励记录...")
    try:
        fix_duplicate_rewards()
        logger.info("修复完成")
    except Exception as e:
        logger.error(f"修复失败: {str(e)}") 