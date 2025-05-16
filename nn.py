#!/usr/bin/env python3
import json
import logging
import time
import argparse
from decimal import Decimal
import grpc

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
        logging.FileHandler('tari_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
class TariTest:
    def __init__(self):
        
        # 创建 gRPC 通道
        self.channel = grpc.insecure_channel('127.0.0.1:18143')
        
        # 创建 gRPC 存根
        self.stub = wallet_pb2_grpc.WalletStub(self.channel)

    def send_transaction(self, address, amount):
        """发送交易"""
        try:
            # 创建 PaymentRecipient
            message = "test"
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
            response = self.stub.Transfer(transfer_request)
            
            # 打印响应
            logger.info(f"gRPC响应: {response}")
            logger.info(f"type: {type(response)}")
            results = response.results[0]

            if results.is_success:
                logger.info(f"交易发送成功:")
                logger.info(f"交易ID: {convert_buffer_to_readable(results.transaction_id)}")
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
                logger.info(f"源地址: {tx_info.source_address}")
                logger.info(f"目标地址: {tx_info.dest_address}")
                logger.info(f"状态: {tx_info.status}")
                logger.info(f"方向: {tx_info.direction}")
                logger.info(f"金额: {tx_info.amount / 1e6:.6f} TARI")
                logger.info(f"手续费: {tx_info.fee / 1e6:.6f} TARI")
                logger.info(f"是否取消: {tx_info.is_cancelled}")
                logger.info(f"时间戳: {tx_info.timestamp}")
                logger.info(f"支付ID: {convert_buffer_to_readable(tx_info.payment_id)}")
                logger.info(f"区块高度: {tx_info.mined_in_block_height}")
                for attr_name in dir(tx_info):
                    attr_value = getattr(response, attr_name)
                    if isinstance(attr_value, (bytes, bytearray, memoryview)):
                        logger.info(f"发现 buffer 数据在属性 {attr_name}:")
                        readable_data = convert_buffer_to_readable(attr_value)
                        if readable_data:
                            logger.info(f"可读格式: {json.dumps(readable_data, indent=2, ensure_ascii=False)}")
                return tx_info
            return None
        except Exception as e:
            logger.error(f"检查交易状态失败: {str(e)}")
            return None

def main():
    parser = argparse.ArgumentParser(description='Tari交易测试程序')
    parser.add_argument('--send', action='store_true', help='发送交易')
    parser.add_argument('--check', help='检查交易ID')
    parser.add_argument('--address', help='接收地址')
    parser.add_argument('--amount', type=float, help='发送金额')
    args = parser.parse_args()
    
    test = TariTest()
    
    if args.send:
        if not args.address or not args.amount:
            logger.error("发送交易需要提供地址和金额")
            parser.print_help()
            return
            
        logger.info(f"发送交易到 {args.address}, 金额: {args.amount} TARI")
        txid = test.send_transaction(args.address, args.amount)
        if txid:
            logger.info("等待5秒后检查交易状态...")
            time.sleep(5)
            test.check_transaction(txid)
            
    elif args.check:
        logger.info(f"检查交易: {args.check}")
        test.check_transaction(args.check)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()