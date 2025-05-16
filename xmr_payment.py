#!/usr/bin/env python3
import psycopg2
import json
import logging
import requests
import time
import argparse
from decimal import Decimal
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('xmr_payment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 加载配置
def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        raise

# 数据库连接
def get_db_connection():
    config = load_config()
    return psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['database'],
        user=config['database']['user'],
        password=config['database']['password']
    )

def confirm_action(message, interactive):
    """交互确认操作"""
    if not interactive:
        return True
        
    response = input(f"\n{message} (y/n): ").lower().strip()
    return response == 'y'

class XMRPayment:
    def __init__(self, interactive=True):
        self.config = load_config()
        self.min_payout = Decimal(str(self.config.get('xmr_min_payout', 0.01)))
        self.wallet_rpc_url = self.config.get('monero_wallet_rpc', 'http://127.0.0.1:18082/json_rpc')
        self.wallet_rpc_user = self.config.get('monero_wallet_rpc_user', '')
        self.wallet_rpc_password = self.config.get('monero_wallet_rpc_password', '')
        self.interactive = interactive

    def get_pending_payments(self):
        """获取待支付的用户列表，限制合并数量"""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # 获取待支付用户，限制数量为20个
            cur.execute("""
                SELECT username, xmr_balance, xmr_wallet 
                FROM account 
                WHERE xmr_balance >= %s 
                AND xmr_wallet IS NOT NULL
                ORDER BY xmr_balance DESC
                LIMIT 10
            """, (self.min_payout,))
            
            pending_payments = []
            total_amount = Decimal('0')
            
            for username, balance, wallet in cur.fetchall():
                # 将支付金额精确到小数点后3位
                payment_amount = Decimal(str(int(balance * 1000) / 1000))
                remaining_balance = balance - payment_amount
                
                # 检查总金额是否超过1 XMR
                #if total_amount + payment_amount > Decimal('1'):
                #    break
                    
                pending_payments.append({
                    'username': username,
                    'total_balance': balance,
                    'payment_amount': payment_amount,
                    'remaining_balance': remaining_balance,
                    'wallet': wallet
                })
                total_amount += payment_amount
            
            logger.info(f"找到 {len(pending_payments)} 个待支付用户，总金额: {total_amount:.3f} XMR")
            
            if self.interactive:
                print("\n待支付用户列表:")
                for payment in pending_payments:
                    print(f"用户: {payment['username']}")
                    print(f"总余额: {payment['total_balance']:.12f} XMR")
                    print(f"本次支付: {payment['payment_amount']:.3f} XMR")
                    print(f"剩余余额: {payment['remaining_balance']:.12f} XMR")
                    print(f"钱包地址: {payment['wallet']}")
                    print("-" * 50)
                print(f"\n总支付金额: {total_amount:.3f} XMR")
                
                if not confirm_action("是否继续处理这些支付？", self.interactive):
                    logger.info("用户取消了支付处理")
                    return []
            
            return pending_payments
            
        finally:
            cur.close()
            conn.close()

    def make_rpc_request(self, method, params=None):
        """发送RPC请求到Monero钱包"""
        headers = {'content-type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "id": "0",
            "method": method,
            "params": params or {}
        }
        
        auth = None
        if self.wallet_rpc_user and self.wallet_rpc_password:
            auth = (self.wallet_rpc_user, self.wallet_rpc_password)
            
        try:
            response = requests.post(
                self.wallet_rpc_url,
                json=payload,
                headers=headers,
                auth=auth,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"RPC请求失败: {str(e)}")
            raise

    def check_wallet_balance(self, total_amount):
        """检查钱包余额是否足够"""
        try:
            result = self.make_rpc_request("get_balance")
            if "result" in result:
                balance = Decimal(str(result["result"]["balance"])) / Decimal('1e12')
                unlocked_balance = Decimal(str(result["result"]["unlocked_balance"])) / Decimal('1e12')
                
                if self.interactive:
                    print(f"\n钱包余额信息:")
                    print(f"总余额: {balance:.12f} XMR")
                    print(f"可用余额: {unlocked_balance:.12f} XMR")
                    print(f"需支付金额: {total_amount:.12f} XMR")
                
                if unlocked_balance < total_amount:
                    logger.error(f"钱包可用余额不足: {unlocked_balance:.12f} XMR, 需要: {total_amount:.12f} XMR")
                    return False
                
                if self.interactive and not confirm_action("钱包余额确认，是否继续？", self.interactive):
                    return False
                    
                return True
        except Exception as e:
            logger.error(f"检查钱包余额失败: {str(e)}")
            return False

    def process_payment(self, payment_info):
        """处理合并支付"""
        try:
            # 准备合并支付
            destinations = []
            total_amount = Decimal('0')
            
            for payment in payment_info:
                username = payment['username']
                amount = payment['payment_amount']
                address = payment['wallet']
                
                # 转换为atomic units
                atomic_amount = int(amount * Decimal('1e12'))
                total_amount += amount
                
                destinations.append({
                    "amount": atomic_amount,
                    "address": address
                })
            
            if self.interactive:
                print(f"\n准备合并支付:")
                print(f"总金额: {total_amount:.3f} XMR")
                print(f"支付用户数: {len(destinations)}")
                
                if not confirm_action("确认进行合并支付？", self.interactive):
                    logger.info("用户取消了合并支付")
                    return False
            
            # 执行合并支付
            params = {
                "destinations": destinations,
                "priority": 1,
                "ring_size": 16
            }
            
            result = self.make_rpc_request("transfer", params)
            
            if "result" in result:
                tx_hash = result["result"]["tx_hash"]
                fee = Decimal(str(result["result"]["fee"])) / Decimal('1e12')
                
                if self.interactive:
                    print(f"\n合并支付成功:")
                    print(f"交易哈希: {tx_hash}")
                    print(f"手续费: {fee:.12f} XMR")
                    
                    if not confirm_action("确认记录此笔支付？", self.interactive):
                        logger.warning("用户取消了对合并支付的记录")
                        return False
                
                # 记录所有用户的支付
                for payment in payment_info:
                    self.record_payment(
                        payment['username'],
                        payment['payment_amount'],
                        tx_hash,
                        fee / len(payment_info)  # 平均分配手续费
                    )
                
                logger.info(f"合并支付成功 - 总金额: {total_amount:.3f} XMR, 用户数: {len(destinations)}, 交易哈希: {tx_hash}")
                return True
            else:
                logger.error(f"合并支付失败 - 错误: {result.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"处理合并支付时出错: {str(e)}")
            return False

    def record_payment(self, username, amount, txid, fee):
        """记录支付信息并更新用户余额"""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            
            # 插入支付记录
            cur.execute("""
                INSERT INTO payment (username, type, amount, txid, time)
                VALUES (%s, 'xmr', %s, %s, %s)
            """, (username, amount, txid, datetime.now()))
            
            # 更新用户余额（只减去实际支付的金额和手续费）
            cur.execute("""
                UPDATE account 
                SET xmr_balance = xmr_balance - %s 
                WHERE username = %s
            """, (amount + fee, username))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"记录支付信息时出错: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    def run(self):
        """运行支付处理"""
        logger.info("开始处理XMR支付...")
        
        try:
            while True:
                # 获取待支付列表
                pending_payments = self.get_pending_payments()
                
                if not pending_payments:
                    logger.info("没有待支付的用户")
                    break
                
                # 检查钱包余额
                total_amount = sum(payment['payment_amount'] for payment in pending_payments)
                if not self.check_wallet_balance(total_amount):
                    break
                
                # 处理合并支付
                success = self.process_payment(pending_payments)
                
                if success:
                    logger.info("合并支付处理成功")
                else:
                    logger.error("合并支付处理失败")
                    break
                
                # 支付间隔
                if self.interactive:
                    input("\n按回车键继续下一批支付...")
                else:
                    time.sleep(1)
                
        except Exception as e:
            logger.error(f"支付处理过程中出错: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='XMR支付处理程序')
    parser.add_argument('--no-interactive', action='store_true', help='非交互模式运行')
    args = parser.parse_args()
    
    payment_processor = XMRPayment(interactive=not args.no_interactive)
    payment_processor.run()

if __name__ == "__main__":
    main()