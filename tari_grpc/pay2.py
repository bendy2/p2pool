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
import argparse

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

def is_valid_tari_address(address):
    """验证Tari钱包地址
    - 长度必须为92字符
    - 必须以'12'开头
    """
    allowed_chars = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    if not set(address).issubset(allowed_chars):
        return False
    if len(address) not in  [91,90]:
        logger.info(len(address))
        return False
        
    if not address.startswith('12'):
        return False
        
    return True

class TariPayment:
    def __init__(self, auto_confirm=False):
        self.config = self.load_config()        
        self.min_payout = Decimal(str(self.config.get('tari_min_payout', 100)))
        self.auto_confirm = auto_confirm
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
            self.db_config = self.config.get('database', {})
            self.conn = None
            self.cursor = None
            self.ensure_db_connection()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise

    def ensure_db_connection(self):
        """确保数据库连接是活跃的"""
        try:
            # 如果连接不存在或已关闭，重新连接
            if self.conn is None or self.conn.closed:
                self.conn = psycopg2.connect(
                    host=self.db_config.get('host', 'localhost'),
                    port=self.db_config.get('port', 5432),
                    database=self.db_config.get('database', 'payment'),
                    user=self.db_config.get('user', 'postgres'),
                    password=self.db_config.get('password', '')
                )
                self.cursor = self.conn.cursor()
            else:
                # 测试连接是否有效
                try:
                    self.cursor.execute('SELECT 1')
                except (psycopg2.OperationalError, psycopg2.InterfaceError):
                    # 如果测试失败，关闭旧连接并重新连接
                    self.close_db_connection()
                    self.conn = psycopg2.connect(
                        host=self.db_config.get('host', 'localhost'),
                        port=self.db_config.get('port', 5432),
                        database=self.db_config.get('database', 'payment'),
                        user=self.db_config.get('user', 'postgres'),
                        password=self.db_config.get('password', '')
                    )
                    self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error(f"确保数据库连接时出错: {str(e)}")
            raise

    def close_db_connection(self):
        """安全关闭数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except Exception as e:
            logger.error(f"关闭数据库连接时出错: {str(e)}")

    def get_next_payment_target(self):
        """获取下一个支付目标"""
        try:
            self.cursor.execute('''
                SELECT username, amount 
                FROM account 
                WHERE txid='FAILED'
                ORDER BY amount DESC
                LIMIT 1
            ''')
            target = self.cursor.fetchone()
            return target
        except Exception as e:
            logger.error(f"获取支付目标失败: {str(e)}")
            return None

    def record_payment(self, username, amount, txid, s, note):
        """记录支付信息并更新用户余额"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.ensure_db_connection()
                self.cursor.execute("BEGIN")
                status = "completed" if s == 0 else "pending"
                
                # 如果note是TransactionInfo对象，转换为JSON
                if hasattr(note, 'tx_id'):  # 检查是否是TransactionInfo对象
                    note_dict = MessageToDict(note)
                    note = json.dumps(note_dict, ensure_ascii=False)
                
                # 插入支付记录
                self.cursor.execute("""
                    INSERT INTO payment (username, type, amount, txid, time, status, note)
                    VALUES (%s, 'tari', %s, %s, %s, %s, %s)
                """, (username, amount, txid, datetime.now(), status, note))
                
                # 更新用户余额（只减去实际支付的金额和手续费）
                self.cursor.execute("""
                    UPDATE account 
                    SET tari_balance = tari_balance - %s 
                    WHERE username = %s
                """, (amount, username))
                
                self.conn.commit()
                logger.info(f"成功记录支付信息: 用户={username}, 金额={amount}, 交易ID={txid}")
                break
                
            except Exception as e:
                self.conn.rollback()
                retry_count += 1
                logger.error(f"记录支付信息时出错 (尝试 {retry_count}/{max_retries}): {str(e)}")
                if retry_count >= max_retries:
                    raise
                time.sleep(1)  # 等待1秒后重试
                
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
            self.ensure_db_connection()
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

    def get_all_payment_targets(self):
        """获取所有有效的支付目标"""
        try:
            self.ensure_db_connection()
            self.cursor.execute('''
                SELECT username, tari_balance, tari_wallet 
                FROM account 
                WHERE tari_wallet IS NOT NULL
                AND tari_balance > 0
                ORDER BY tari_balance DESC
            ''')
            targets = self.cursor.fetchall()
            
            # 过滤出有效的钱包地址
            valid_targets = []
            for target in targets:
                username, balance, wallet = target
                if not is_valid_tari_address(wallet):
                    logger.warning(f"用户 {username} 的钱包地址无效: {wallet}")
                    continue
                valid_targets.append(target)
                
            return valid_targets
            
        except Exception as e:
            logger.error(f"获取支付目标失败: {str(e)}")
            return []

    def confirm_action(self, message):
        """确认操作"""
        if self.auto_confirm:
            logger.info(f"自动确认: {message}")
            return True
        response = input(f"\n{message} (y/n): ").lower().strip()
        return response == 'y'
    
    def confirm_action_force(self, message):
        """确认操作"""
        response = input(f"\n{message} (y/n): ").lower().strip()
        return response == 'y'
    
    def format_username(self, username):
        """格式化用户名显示（显示前5位和后5位）"""
        if len(username) <= 10:
            return username
        return f"{username[:5]}...{username[-5:]}"

    def create_pending_payment(self, username, amount):
        """创建待处理的支付记录"""
        try:
            self.ensure_db_connection()
            self.cursor.execute("BEGIN")
            
            # 插入待处理的支付记录
            self.cursor.execute("""
                INSERT INTO payment (username, type, amount, txid, time, status, note)
                VALUES (%s, 'tari', %s, '-', %s, 'pending', '待发送')
            """, (username, amount, datetime.now()))
            
            # 更新用户余额
            self.cursor.execute("""
                UPDATE account 
                SET tari_balance = tari_balance - %s 
                WHERE username = %s
            """, (amount, username))
            
            self.conn.commit()
            logger.info(f"创建待处理支付记录: 用户={username}, 金额={amount}")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"创建待处理支付记录失败: {str(e)}")
            return False

    def update_payment_status(self, username, amount, txid, tx_info, status):
        """更新支付状态"""
        try:
            self.ensure_db_connection()
            self.cursor.execute("BEGIN")
            
            # 转换交易信息为JSON
            note = None
            if tx_info:
                note_dict = MessageToDict(tx_info)
                note = json.dumps(note_dict, ensure_ascii=False)
            
            # 更新支付记录
            self.cursor.execute("""
                UPDATE payment 
                SET txid = %s,
                    status = %s,
                    note = %s
                WHERE username = %s 
                AND amount = %s 
                AND status = 'pending' 
                AND txid = '-'
                RETURNING id
            """, (txid, status, note, username, amount))
            
            result = self.cursor.fetchone()
            if not result:
                raise Exception("找不到匹配的待处理支付记录")
            
            self.conn.commit()
            logger.info(f"更新支付状态成功: 用户={username}, 交易ID={txid}, 状态={status}")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"更新支付状态失败: {str(e)}")
            return False

    def run(self): 
        """运行自动支付程序"""
        logger.info("启动Tari支付程序...")
        
        try:
            # 1. 获取所有有效的支付目标
            targets = self.get_all_payment_targets()
            if not targets:
                logger.info("没有找到任何待支付的用户")
                return

            # 2. 计算每个用户的可用余额并筛选满足条件的用户
            payment_list = []
            total_payment_amount = Decimal('0')
            
            for username, total_balance in targets:
                # 计算可用余额
                wallet = username
                available_balance = self.get_available_balance(username, total_balance)
                
                # 检查是否满足最小支付额度
                if available_balance >= self.min_payout:
                    payment_list.append({
                        'username': username,
                        'available_balance': available_balance,
                        'wallet': wallet
                    })
                    total_payment_amount += Decimal(str(available_balance))
            
            # 3. 显示待支付信息
            if payment_list:
                print("\n待支付列表:")
                print("-" * 50)
                print(f"{'序号':<6} {'用户名':<20} {'支付金额(TARI)':<15}")
                print("-" * 50)
                
                for i, payment in enumerate(payment_list, 1):
                    formatted_username = self.format_username(payment['username'])
                    print(f"{i:<6} {formatted_username:<20} {payment['available_balance']:<15.2f}")
                
                print("-" * 50)
                print(f"总计待支付: {len(payment_list)} 笔")
                print(f"总计金额: {total_payment_amount:.2f} TARI")
            else:
                logger.info("没有满足支付条件的用户")
                return
                
            # 4. 确认是否继续支付（即使自动确认模式也需要此确认）
            if not self.confirm_action_force("是否确认开始支付?"):
                logger.info("操作员取消了支付操作")
                return
            
            # 5. 逐个处理支付
            for i, payment in enumerate(payment_list, 1):
                username = payment['username']
                amount = payment['available_balance']
                address = payment['wallet']
                
                formatted_username = self.format_username(username)
                print(f"\n[{i}/{len(payment_list)}] 准备支付: {formatted_username} - {amount:.2f} TARI")
                
                # 在自动确认模式下跳过单笔支付确认
                if not self.auto_confirm and not self.confirm_action("是否继续这笔支付?"):
                    logger.info(f"跳过用户 {username} 的支付")
                    continue
                
                # 先创建待处理的支付记录
                if not self.create_pending_payment(username, amount):
                    logger.error(f"创建待处理支付记录失败，跳过此次支付")
                    continue
                
                # 发送交易
                txid = self.send_transaction(address, amount)
                
                if txid:
                    # 等待交易确认
                    logger.info("等待交易确认...")
                    time.sleep(10)
                    
                    # 检查交易状态
                    tx_info = self.check_transaction(txid)
                    if tx_info and (tx_info.status == 1 or tx_info.status == "TRANSACTION_STATUS_BROADCAST"):
                        logger.info(f"交易已确认: {txid}")
                        self.update_payment_status(username, amount, txid, tx_info, "completed")
                    else:
                        logger.error("交易未确认")
                        self.update_payment_status(username, amount, txid, tx_info, "failed")
                else:
                    logger.error("发送交易失败")
                    self.update_payment_status(username, amount, "FAILED", None, "failed")
                
                # 每笔交易后等待
                time.sleep(5)
                
            logger.info("所有支付处理完成")
            
        except Exception as e:
            logger.error(f"运行出错: {str(e)}")
            raise

    def __del__(self):
        """清理资源"""
        self.close_db_connection()

def main():
    parser = argparse.ArgumentParser(description='Tari支付程序')
    parser.add_argument('-y', '--yes', action='store_true', help='自动确认所有支付操作（除了初始确认）')
    args = parser.parse_args()
    
    try:
        payment = TariPayment(auto_confirm=args.yes)
        payment.run()
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")

if __name__ == "__main__":
    main()
