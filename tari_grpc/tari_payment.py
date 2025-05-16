#!/usr/bin/env python3
import json
import logging
import time
import grpc
import math
import psycopg2
from datetime import datetime, timedelta
from decimal import Decimal
from google.protobuf.json_format import MessageToDict

# 导入 Tari gRPC 相关模块
from tari.wallet_grpc import wallet_pb2
from tari.wallet_grpc import wallet_pb2_grpc
from tari.wallet_grpc import types_pb2
from tari.wallet_grpc import transaction_pb2

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tari_payment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def convert_buffer_to_readable(buffer_data):
    """将 buffer 数据转换为可读格式"""
    try:
        # 转换为十六进制字符串
        hex_str = buffer_data.hex()
        # 转换为 base64
        import base64
        base64_str = base64.b64encode(buffer_data).decode('utf-8')
        return {
            'hex': hex_str,
            'base64': base64_str
        }
    except Exception as e:
        logger.error(f"转换 buffer 数据时出错: {str(e)}")
        return None

class TariPayment:
    def __init__(self):
        self.config = self.load_config()        
        self.min_payout = Decimal(str(self.config.get('tari_min_payout', 100)))
        # 创建 gRPC 通道
        self.channel = grpc.insecure_channel('127.0.0.1:18143')
        
        # 创建 gRPC 存根
        self.stub = wallet_pb2_grpc.WalletStub(self.channel)
        # 初始化数据库连接
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
            # 从配置文件获取数据库连接信息
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

    def get_next_payment_target(self):
        """获取下一个支付目标"""
        try:
            self.cursor.execute('''
                SELECT username, tari_balance, tari_wallet 
                FROM account 
                WHERE tari_balance >= %s 
                AND tari_wallet IS NOT NULL
                ORDER BY tari_balance DESC
                LIMIT 1
            ''', (self.min_payout,))
            target = self.cursor.fetchone()
            return target
        except Exception as e:
            logger.error(f"获取支付目标失败: {str(e)}")
            return None

    def record_payment(self, user_id, address, amount, tx_id, status, block_height):
        """记录支付结果"""
        try:
            # 开始事务
            self.conn.autocommit = False
            try:
                # 插入支付记录
                self.cursor.execute('''
                    INSERT INTO payment_records 
                    (user_id, address, amount, tx_id, status, block_height, coin_type) 
                    VALUES (%s, %s, %s, %s, %s, %s, 'tari')
                ''', (user_id, address, amount, tx_id, status, block_height))
                
                # 如果支付成功，更新支付目标状态
                if status == 1:
                    self.cursor.execute('''
                        UPDATE payment_targets 
                        SET status = 1, updated_at = CURRENT_TIMESTAMP 
                        WHERE address = %s AND user_id = %s AND coin_type = 'tari' AND status = 0
                    ''', (address, user_id))
                
                # 提交事务
                self.conn.commit()
                logger.info(f"用户 {user_id} 支付记录已保存: 地址={address}, 金额={amount}, 状态={status}")
                
            except Exception as e:
                # 回滚事务
                self.conn.rollback()
                logger.error(f"记录用户 {user_id} 支付结果失败: {str(e)}")
                raise
            finally:
                # 恢复自动提交
                self.conn.autocommit = True
                
        except Exception as e:
            logger.error(f"记录用户 {user_id} 支付结果失败: {str(e)}")

    def send_transaction(self, address, amount):
        """发送交易"""
        try:
            logger.info(f"开始发送交易到 {address}, 金额: {amount} TARI")
            # 创建转账请求
            message = "payment from tpool"
            recipient = wallet_pb2.PaymentRecipient(
                address=address,
                amount= int(amount * 1e6),
                fee_per_gram=25,
                payment_type=1,  # 使用单向支付类型
                payment_id=message.encode('utf-8')  # 将消息作为 payment_id
            )
            # 创建转账请求
            transfer_request = wallet_pb2.TransferRequest(
                recipients=[recipient]  # 添加接收者到列表中
            )
            
            # 发送请求
            response_data = self.stub.Transfer(transfer_request)
            
            logger.info(f"响应内容: {response_data}")
            results = response_data.results[0]
            
            if results.is_success:
                logger.info(f"交易发送成功:")
                logger.info(f"交易ID: {results.transaction_id}")
                logger.info(f"目标地址: {results.address}")
                return results.transaction_id
            else:
                logger.error("交易发送失败")
                return None
                
        except Exception as e:
            logger.error(f"发送交易时出错: {str(e)}")
            return None

    def check_transaction(self, txid):
        """检查交易状态"""
        try:
            # 创建查询请求
            request = wallet_pb2.GetTransactionInfoRequest(
                transaction_ids=[int(txid)]
            )
            
            # 发送请求
            response = self.stub.GetTransactionInfo(request)
            
            if response.transactions:
                tx_info = response.transactions[0]
                # 打印完整的交易信息
                logger.info("交易详情:")
                logger.info(f"交易ID: {tx_info.tx_id}")
                logger.info(f"状态: {tx_info.status}")
                logger.info(f"区块高度: {tx_info.mined_in_block_height}")
                return tx_info
            return None
        except Exception as e:
            logger.error(f"检查交易状态失败: {str(e)}")
            return None

    def get_available_balance(self, user_id, total_balance):
        """获取指定用户的可用余额（总余额减去最近18小时的奖励）"""
        try:
            # 获取当前时间
            current_time = datetime.now()
            # 计算18小时前的时间
            time_threshold = current_time - timedelta(hours=18)
            
            # 查询指定用户最近18小时的奖励总额
            self.cursor.execute('''
                SELECT COALESCE(SUM(reward), 0) 
                FROM rewards 
                WHERE type = 'tari' 
                AND username = %s
                AND time >= %s
            ''', (user_id, time_threshold))
            recent_rewards = self.cursor.fetchone()[0]

            # 计算可用余额
            available_balance = total_balance - Decimal(str(recent_rewards))
            logger.info(f"用户 {user_id} 总余额: {total_balance} TARI")
            logger.info(f"用户 {user_id} 最近18小时奖励: {recent_rewards} TARI")
            logger.info(f"用户 {user_id} 可用余额: {available_balance} TARI")
            
            return math.floor(available_balance)
        except Exception as e:
            logger.error(f"计算用户 {user_id} 可用余额失败: {str(e)}")
            return Decimal('0')

    def run(self):
        """运行自动支付程序"""
        logger.info("启动自动支付程序...")
        
        while True:
            try:
                # 获取下一个支付目标
                user_id = "bendy"
                address = "12GiRMnB7vcFMvmoW1wdm7wyfvRnAuBRnjP4GaLuWrhb5NKuyxda3xQckhVJ4S4mPBvhoSfixTDk3BFMvVjmr166539"
                available_balance = 0.1
                """

                target = self.get_next_payment_target()
                if not target:
                    logger.info("没有待支付的目标，等待10秒后重试...")
                    time.sleep(10)
                    exit()
                    continue
                
                user_id, amount, address = target
                
                # 获取用户可用余额
                available_balance = self.get_available_balance(user_id, amount)
                if available_balance <= 0:
                    logger.info(f"用户 {user_id} 可用余额不足，等待10秒后重试...")
                    time.sleep(10)
                    exit()
                    continue
                
                logger.info(f"开始处理用户 {user_id} 的支付目标: ID={user_id}, 地址={address}, 金额={available_balance}")

                """
                # 发送交易
                txid = self.send_transaction(address, available_balance)
                
                if txid:
                    # 等待交易确认
                    logger.info("等待交易确认...")
                    time.sleep(5)
                    
                    # 检查交易状态
                    tx_info = self.check_transaction(txid)
                    print(tx_info);
                    exit()
                    if tx_info and tx_info.status == 1:  # 状态为1表示确认成功
                        logger.info(f"交易已确认，区块高度: {tx_info}")
                        # 记录支付结果
                        self.record_payment(
                            user_id,
                            address, 
                            available_balance, 
                            txid, 
                            1,  # 成功状态
                            tx_info
                        )
                    else:
                        logger.error("交易未确认")
                        # 记录支付结果
                        self.record_payment(
                            user_id,
                            address, 
                            available_balance, 
                            txid, 
                            0,  # 失败状态
                            0
                        )
                
                # 等待10秒后进行下一次支付
                logger.info("等待10秒后进行下一次支付...")
                exit()
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"运行出错: {str(e)}")
                time.sleep(10)  # 出错后等待10秒再重试

    def __del__(self):
        """清理资源"""
        try:
            if hasattr(self, 'conn'):
                self.conn.close()
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {str(e)}")

def main():
    try:
        payment = TariPayment()
        payment.run()
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")

if __name__ == "__main__":
    main()
