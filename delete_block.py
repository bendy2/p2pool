#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import DictCursor
import logging
import json
import sys
import argparse
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('delete_block.log')
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

def delete_block(block_height, block_type='tari'):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 1. 首先检查区块是否存在
        cur.execute("""
            SELECT * FROM blocks 
            WHERE block_height = %s AND type = %s
        """, (block_height, block_type))
        
        block = cur.fetchone()
        if not block:
            logger.error(f"区块 {block_height} ({block_type}) 不存在")
            return False
            
        logger.info(f"找到区块: 高度={block_height}, 类型={block_type}, 奖励={block['rewards']}")
        
        # 2. 获取该区块的所有奖励记录
        cur.execute("""
            SELECT * FROM rewards 
            WHERE block_height = %s AND type = %s
        """, (block_height, block_type))
        
        rewards = cur.fetchall()
        logger.info(f"找到 {len(rewards)} 条奖励记录")
        
        # 3. 删除奖励记录
        cur.execute("""
            DELETE FROM rewards 
            WHERE block_height = %s AND type = %s
        """, (block_height, block_type))
        
        logger.info(f"已删除 {len(rewards)} 条奖励记录")
        
        # 4. 删除区块记录
        cur.execute("""
            DELETE FROM blocks 
            WHERE block_height = %s AND type = %s
        """, (block_height, block_type))
        
        logger.info(f"已删除区块记录")
        
        # 5. 重新计算用户余额
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
        
        # 6. 提交更改
        conn.commit()
        logger.info("成功删除区块记录并更新用户余额")
        
        # 7. 输出受影响的用户信息
        logger.info("受影响的用户奖励记录:")
        for reward in rewards:
            logger.info(f"用户: {reward['username']}, 奖励: {reward['reward']} {block_type.upper()}")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"删除区块过程中出错: {str(e)}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return False

def main():
    parser = argparse.ArgumentParser(description='删除指定区块及其相关奖励记录')
    parser.add_argument('height', type=int, help='要删除的区块高度')
    parser.add_argument('--type', type=str, default='tari', choices=['xmr', 'tari'], help='区块类型 (默认: tari)')
    
    args = parser.parse_args()
    
    logger.info(f"开始删除区块 {args.height} ({args.type})...")
    
    if delete_block(args.height, args.type):
        logger.info("删除完成")
    else:
        logger.error("删除失败")
        sys.exit(1)

if __name__ == '__main__':
    main() 