#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import DictCursor
import logging
import json
import sys

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

def update_blocks_check_status():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # 开始事务
        cur.execute("BEGIN")
        
        try:
            # 1. 添加check_status列（如果不存在）
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'blocks' 
                        AND column_name = 'check_status'
                    ) THEN
                        ALTER TABLE blocks ADD COLUMN check_status BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
            """)
            
            # 2. 更新现有数据
            # XMR类型的区块设置为TRUE
            cur.execute("""
                UPDATE blocks 
                SET check_status = TRUE 
            """)
            xmr_count = cur.rowcount
            
            # 提交事务
            conn.commit()
            
            logger.info(f"更新完成：")
            logger.info(f"- XMR区块：{xmr_count} 个已设置为已检查")
            
            return True
            
        except Exception as e:
            # 回滚事务
            conn.rollback()
            logger.error(f"更新区块状态时发生错误: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"数据库操作失败: {str(e)}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    logger.info("开始更新区块检查状态...")
    if update_blocks_check_status():
        logger.info("更新成功完成")
    else:
        logger.error("更新失败")
        sys.exit(1)

if __name__ == "__main__":
    main() 