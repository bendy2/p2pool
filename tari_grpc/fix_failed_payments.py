#!/usr/bin/env python3
import json
import logging
import psycopg2
from decimal import Decimal
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fix_failed_payments.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PaymentFixer:
    def __init__(self):
        self.config = self.load_config()
        self.init_database()

    def load_config(self):
        """加载配置文件"""
        try:
            with open('../config.json', 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

    def init_database(self):
        """初始化数据库连接"""
        try:
            db_config = self.config.get('database', {})
            self.conn = psycopg2.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 5432),
                database=db_config.get('database', 'payment'),
                user=db_config.get('user', 'postgres'),
                password=db_config.get('password', '')
            )
            self.cursor = self.conn.cursor()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise

    def get_failed_payments(self):
        """获取所有失败的支付记录"""
        try:
            self.cursor.execute("""
                SELECT username, amount, txid, created_at
                FROM payment
                WHERE txid = 'FAILED'
                ORDER BY created_at
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"获取失败支付记录时出错: {str(e)}")
            return []

    def fix_user_balance(self, username, amount):
        """修复用户余额"""
        try:
            # 更新用户余额
            self.cursor.execute("""
                UPDATE account
                SET tari_balance = tari_balance + %s
                WHERE username = %s
                RETURNING tari_balance
            """, (amount, username))
            
            new_balance = self.cursor.fetchone()
            if new_balance:
                logger.info(f"用户 {username} 余额已更新: {new_balance[0]}")
                return True
            else:
                logger.warning(f"未找到用户 {username}")
                return False
        except Exception as e:
            logger.error(f"更新用户 {username} 余额时出错: {str(e)}")
            return False

    def mark_payment_fixed(self, txid, username, amount):
        """标记支付记录为已修复"""
        try:
            self.cursor.execute("""
                UPDATE payment
                SET txid = 'FIXED',
                    updated_at = CURRENT_TIMESTAMP
                WHERE txid = 'FAILED'
                AND username = %s
                AND amount = %s
            """, (username, amount))
            return True
        except Exception as e:
            logger.error(f"标记支付记录为已修复时出错: {str(e)}")
            return False

    def fix_payments(self):
        """修复所有失败的支付"""
        try:
            # 开始事务
            self.cursor.execute("BEGIN")
            
            # 获取所有失败的支付记录
            failed_payments = self.get_failed_payments()
            if not failed_payments:
                logger.info("没有找到失败的支付记录")
                return
            
            logger.info(f"找到 {len(failed_payments)} 条失败的支付记录")
            
            # 处理每条失败的支付记录
            success_count = 0
            fail_count = 0
            
            for username, amount, txid, created_at in failed_payments:
                logger.info(f"处理用户 {username} 的失败支付: {amount} TARI (创建于 {created_at})")
                
                # 修复用户余额
                if self.fix_user_balance(username, amount):
                    # 标记支付记录为已修复
                    if self.mark_payment_fixed(txid, username, amount):
                        success_count += 1
                        logger.info(f"成功修复用户 {username} 的支付记录")
                    else:
                        fail_count += 1
                        logger.error(f"标记支付记录失败: {username}")
                else:
                    fail_count += 1
                    logger.error(f"修复用户余额失败: {username}")
            
            # 提交事务
            self.conn.commit()
            logger.info(f"修复完成: 成功 {success_count} 条, 失败 {fail_count} 条")
            
        except Exception as e:
            # 回滚事务
            self.conn.rollback()
            logger.error(f"修复过程中发生错误: {str(e)}")
            raise
        finally:
            # 关闭数据库连接
            self.cursor.close()
            self.conn.close()

def main():
    try:
        fixer = PaymentFixer()
        fixer.fix_payments()
    except Exception as e:
        logger.error(f"程序运行失败: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 