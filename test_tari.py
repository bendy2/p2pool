#!/usr/bin/env python3
import grpc
import sys
import os
from datetime import datetime
import argparse
from google.protobuf.empty_pb2 import Empty
from tari.wallet_grpc import wallet_pb2
from tari.wallet_grpc import wallet_pb2_grpc
from tari.wallet_grpc import types_pb2
from tari.wallet_grpc import transaction_pb2

def test_transfer(address: str, amount: float, message: str = "Test transfer"):
    try:
        # 创建 gRPC channel
        channel = grpc.insecure_channel('127.0.0.1:18143')
        stub = wallet_pb2_grpc.WalletStub(channel)

        # 首先检查钱包状态和余额
        print("检查钱包状态...")
        state = stub.GetState(Empty())
        print(f"当前可用余额: {state.balance.available_balance} µT")
        
        # 确保有足够的余额
        amount_in_microtari = int(amount * 1e6)  # 转换为 microTari
        if state.balance.available_balance < amount_in_microtari:
            print(f"错误: 余额不足. 需要 {amount} T, 但只有 {state.balance.available_balance/1e9} T")
            return

        # 创建转账请求
        print(f"\n准备发送 {amount} T 到地址: {address}")
        print(f"消息: {message}")
        
        # 创建 PaymentRecipient
        recipient = wallet_pb2.PaymentRecipient(
            address=address,
            amount=amount_in_microtari,
            fee_per_gram=25,
            payment_type=1,  # 使用单向支付类型
            payment_id=message.encode('utf-8')  # 将消息作为 payment_id
        )

        # 创建转账请求
        transfer_request = wallet_pb2.TransferRequest(
            recipients=[recipient]  # 添加接收者到列表中
        )


        # 发送转账请求
        print("\n发送转账请求...")
        response = stub.Transfer(transfer_request)
        
        # 打印转账结果
        print("\n转账结果:")
        print(f"交易ID: {response}")
        print("\n等待交易确认...")

        txid = response.results.transaction_id
        tx_query = { "transaction_ids": [txid] }
        
        max_attempts = 30
        for i in range(max_attempts):
            tx_status = stub.GetTransactionInfo(tx_query)
            print(f"交易状态: {tx_status.status}")
            if tx_status.status == "COMPLETED":
                print("交易已确认!")
                break
            if i == max_attempts - 1:
                print("等待超时，请稍后检查交易状态")
            import time
            time.sleep(2)

    except grpc.RpcError as e:
        print(f"gRPC错误: {e.code()}: {e.details()}")
    except Exception as e:
        print(f"发生错误: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='测试 Tari 钱包转账功能')
    parser.add_argument('--address', required=True, help='接收地址')
    parser.add_argument('--amount', type=float, required=True, help='发送金额(Tari)')
    parser.add_argument('--message', default='Test transfer', help='转账备注信息')
    
    args = parser.parse_args()
    test_transfer(args.address, args.amount, args.message)