const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const fs = require('fs');

// 配置参数
const WALLET_ADDRESS = 'localhost:18143';
const PROTO_PATH = './proto/wallet.proto';

// 存储区块高度的数组
let blockHeights = [];

// 加载 Protocol Buffers
console.log('正在加载 Protocol Buffers...');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const streamingProto = grpc.loadPackageDefinition(packageDefinition).tari.rpc;

// 创建客户端
console.log('正在创建 gRPC 客户端...');
const client = new streamingProto.Wallet(WALLET_ADDRESS, grpc.credentials.createInsecure());

// 保存数据到文件
function saveToFile() {
    const data = {
        block_heights: blockHeights,
        timestamp: new Date().toISOString(),
        total_blocks: blockHeights.length
    };
    
    fs.writeFileSync('block.json', JSON.stringify(data, null, 2));
    console.log(`数据已保存到 block.json，共 ${blockHeights.length} 个区块高度`);
}

// 处理接收到的数据
function handleTransaction(transaction) {
    if (transaction.mined_in_block_height) {
        const blockHeight = transaction.mined_in_block_height;
        if (!blockHeights.includes(blockHeight)) {
            blockHeights.push(blockHeight);
            console.log(`发现新区块高度: ${blockHeight}`);
            // 每收到新的区块高度就保存一次
            saveToFile();
        }
    }
}

const request = {};
const call = client.GetCompletedTransactions(request);

call.on('data', (response) => {
    if (response.transaction) {
        handleTransaction(response.transaction);
    }
});

call.on('end', () => {
    console.log('数据流结束');
    // 最后保存一次
    saveToFile();
});

call.on('error', (err) => {
    console.error('连接错误:', err);
});

call.on('status', (status) => {
    console.log('连接状态:', status);
}); 