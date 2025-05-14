#!/usr/bin/env python3
import psycopg2
import logging
import json

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('update_blocks.log')
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

def update_blocks_table():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. 添加新列
        cur.execute("""
            ALTER TABLE blocks 
            ADD COLUMN IF NOT EXISTS block_id VARCHAR(64),
            ADD COLUMN IF NOT EXISTS is_valid BOOLEAN DEFAULT TRUE
        """)
        
        # 2. 更新现有记录
        # XMR区块全部设为有效
        cur.execute("""
            UPDATE blocks 
            SET is_valid = TRUE 
            WHERE type = 'xmr'
        """)
        
        # TARI区块暂时全部设为有效
        cur.execute("""
            UPDATE blocks 
            SET is_valid = TRUE 
            WHERE type = 'tari'
        """)
        
        conn.commit()
        logger.info("成功更新blocks表结构")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"更新数据库结构失败: {str(e)}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        raise

if __name__ == '__main__':
    logger.info("开始更新数据库结构...")
    try:
        update_blocks_table()
        logger.info("更新完成")
    except Exception as e:
        logger.error(f"更新失败: {str(e)}") 