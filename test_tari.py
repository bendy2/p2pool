#!/usr/bin/env python3
import grpc
import sys
import os
from datetime import datetime
import argparse

# 导入 TARI gRPC 生成的代码
try:
    from tari.wallet_grpc import wallet_pb2
    from tari.wallet_grpc import wallet_pb2_grpc
except ImportError:
    print("错误：找不到 TARI gRPC 模块。请确保已经生成 gRPC 代码。")
    sys.exit(1)

class TariWalletTest:
    def __init__(self, grpc_address="127.0.0.1:18143"):
        self.channel = grpc.insecure_channel(grpc_address)
        self.stub = wallet_pb2_grpc.WalletStub(self.channel)

    def test_connection(self):
        """测试与钱包的连接"""
        try:
            # 获取钱包信息
            request = wallet_pb2.GetIdentityRequest()
            response = self.stub.GetIdentity(request)
            print("钱包连接成功！")
            print(f"钱包公钥: {response.public_key}")
            return True
        except grpc.RpcError as e:
            print(f"gRPC 连接错误: {e}")
            return False

    def get_balance(self):
        """获取钱包余额"""
        try:
            request = wallet_pb2.GetBalanceRequest()
            response = self.stub.GetBalance(request)
            print(f"可用余额: {response.available_balance}")
            print(f"待确认余额: {response.pending_incoming_balance}")
            return response.available_balance
        except grpc.RpcError as e:
            print(f"获取余额失败: {e}")
            return None

    def test_get_address(self):
        """测试获取接收地址"""
        try:
            request = wallet_pb2.GetNewAddressRequest()
            response = self.stub.GetNewAddress(request)
            print(f"新生成的接收地址: {response.address}")
            return response.address
        except grpc.RpcError as e:
            print(f"获取地址失败: {e}")
            return None

def main():
    parser = argparse.ArgumentParser(description='TARI 钱包 gRPC 测试脚本')
    parser.add_argument('--grpc-address', default='127.0.0.1:18143', help='gRPC 服务器地址')
    args = parser.parse_args()

    print("开始 TARI 钱包 gRPC 测试...")
    wallet = TariWalletTest(args.grpc_address)

    # 测试连接
    if not wallet.test_connection():
        print("连接测试失败，退出测试")
        return

    # 测试获取余额
    print("\n测试获取余额:")
    wallet.get_balance()

    # 测试获取新地址
    print("\n测试获取新地址:")
    wallet.test_get_address()

if __name__ == "__main__":
    main()