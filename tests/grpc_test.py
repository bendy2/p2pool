#!/usr/bin/env python3
import grpc
import base64
import logging
import psycopg2
from psycopg2.extras import DictCursor
from decimal import Decimal
import time
import argparse
import json
from typing import List, Dict
import wallet_pb2
import wallet_pb2_grpc

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TariWalletClient:
    def __init__(self, config_path: str):
        # 加载配置
        with open(config_path) as f:
            self.config = json.load(f)
        
        # gRPC 配置
        self.grpc_host = self.config['tari_wallet']['grpc_host']
        self.grpc_port = self.config['tari_wallet']['grpc_port']
        self.username = self.config['tari_wallet']['username']
        self.password = self.config['tari_wallet']['password']
        
        # 数据库配置
        self.db_config = self.config['database']
        
        # 支付配置
        self.min_payment = Decimal(str(self.config['payment']['min_tari_payment']))
        self.fee_per_gram = self.config['payment']['fee_per_gram']

    def get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(**self.db_config)

    def get_auth_metadata(self) -> List[tuple]:
        """生成 gRPC 认证元数据"""
        credentials = f"{self.username}:{self.password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return [('authorization', f'Basic {encoded_credentials}')]

    def get_pending_payments(self) -> List[Dict]:
        """获取待支付的用户列表"""
        conn = self.get_db_connection()
        cur = conn.cursor(cursor_factory=DictCursor)
        try:
            # 查询余额超过最小支付金额的用户
            cur.execute("""
                SELECT username, tari_wallet, tari_balance
                FROM account
                WHERE tari_balance >= %s
                AND tari_wallet != ''
                ORDER BY tari_balance DESC
            """, (self.min_payment,))
            
            return [dict(row) for row in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

    def process_payment(self, payment: Dict, interactive: bool = True) -> bool:
        """处理单个支付"""
        username = payment['username']
        address = payment['tari_wallet']
        amount = int(payment['tari_balance'] * 1000000)  # 转换为 microTari

        if interactive:
            confirm = input(f"确认向 {username} ({address}) 支付 {amount/1000000:.6f} TARI? (y/n): ")
            if confirm.lower() != 'y':
                logger.info(f"跳过支付给用户 {username}")
                return False

        try:
            # 创建 gRPC channel
            channel = grpc.insecure_channel(f"{self.grpc_host}:{self.grpc_port}")
            stub = wallet_pb2_grpc.WalletStub(channel)

            # 创建转账请求
            transfer_request = wallet_pb2.TransferRequest(
                recipients=[
                    wallet_pb2.TransferRecipient(
                        address=address,
                        amount=amount,
                        fee_per_gram=self.fee_per_gram,
                        message=f"Pool payment to {username}"
                    )
                ]
            )

            # 发送转账请求
            response = stub.Transfer(
                transfer_request,
                metadata=self.get_auth_metadata()
            )

            if response.success:
                logger.info(f"成功支付给用户 {username}, 交易ID: {response.transaction_id}")
                self.record_payment(username, amount, response.transaction_id)
                return True
            else:
                logger.error(f"支付失败: {response.message}")
                return False

        except grpc.RpcError as e:
            logger.error(f"gRPC错误: {e}")
            return False
        except Exception as e:
            logger.error(f"支付过程中出现错误: {e}")
            return False

    def record_payment(self, username: str, amount: int, txid: str):
        """记录支付信息到数据库"""
        conn = self.get_db_connection()
        cur = conn.cursor()
        try:
            # 添加支付记录
            cur.execute("""
                INSERT INTO payment (username, amount, txid, type, time)
                VALUES (%s, %s, %s, 'tari', NOW())
            """, (username, amount/1000000, txid))

            # 更新用户余额
            cur.execute("""
                UPDATE account
                SET tari_balance = tari_balance - %s
                WHERE username = %s
            """, (amount/1000000, username))

            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"记录支付信息时出错: {e}")
        finally:
            cur.close()
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='TARI 钱包自动转账工具')
    parser.add_argument('--config', default='config.json', help='配置文件路径')
    parser.add_argument('--no-interactive', action='store_true', help='非交互式模式')
    args = parser.parse_args()

    client = TariWalletClient(args.config)
    
    while True:
        try:
            # 获取待支付列表
            pending_payments = client.get_pending_payments()
            
            if not pending_payments:
                logger.info("没有待支付的用户")
                break

            logger.info(f"找到 {len(pending_payments)} 个待支付用户")
            
            # 处理每个支付
            for payment in pending_payments:
                client.process_payment(payment, not args.no_interactive)
                time.sleep(1)  # 支付间隔

        except KeyboardInterrupt:
            logger.info("用户中断，程序退出")
            break
        except Exception as e:
            logger.error(f"程序运行出错: {e}")
            break

if __name__ == "__main__":
    main()