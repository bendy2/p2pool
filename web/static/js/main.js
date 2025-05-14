// 格式化数字
function formatNumber(num) {
    return new Intl.NumberFormat('zh-CN').format(num);
}

// 格式化时间（北京时间）
function formatTime(timestamp) {
    if (!timestamp) return '未知时间';
    
    try {
        // 直接解析GMT时间字符串
        const date = new Date(timestamp);
        
        // 检查日期是否有效
        if (isNaN(date.getTime())) return '无效时间';
        
        // 转换为北京时间（UTC+8）
        const beijingTime = new Date(date.getTime() + 8 * 60 * 60 * 1000);
        
        return beijingTime.toLocaleString('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        });
    } catch (error) {
        console.error('时间格式化错误:', error);
        return '时间错误';
    }
}

// 格式化XMR金额
function formatXMR(amount) {
    return amount.toFixed(6) + ' XMR';
}

// 格式化TARI金额
function formatTARI(amount) {
    return amount.toFixed(2) + ' TARI';
}

// 格式化算力
function formatHashrate(hashrate) {
    if (hashrate >= 1e9) {
        return (hashrate / 1e9).toFixed(2) + ' GH/s';
    } else if (hashrate >= 1e6) {
        return (hashrate / 1e6).toFixed(2) + ' MH/s';
    } else if (hashrate >= 1e3) {
        return (hashrate / 1e3).toFixed(2) + ' KH/s';
    } else {
        return hashrate.toFixed(2) + ' H/s';
    }
}

// 更新区块列表
function updateBlocks() {
    fetch('/api/blocks')
        .then(response => response.json())
        .then(data => {
            const blocksList = document.getElementById('blocks-list');
            blocksList.innerHTML = '';
            
            data.forEach(block => {
                const row = document.createElement('tr');
                row.className = block.type === 'XMR' ? 'block-xmr' : 'block-tari';
                
                // 转换为北京时间
                const time = new Date(block.timestamp);
                const beijingTime = new Date(time.getTime() + 8 * 60 * 60 * 1000);
                const formattedTime = beijingTime.toLocaleString('zh-CN', {
                    year: 'numeric',
                    month: '2-digit',
                    day: '2-digit',
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit',
                    hour12: false
                });
                
                const typeClass = block.type === 'XMR' ? 'block-type-xmr' : 'block-type-tari';
                const statusClass = block.is_valid ? 'status-valid' : 'status-invalid';
                const statusText = block.is_valid ? '有效' : '无效';
                
                // 修改奖励显示
                let reward = block.reward;
                if (block.type === 'XMR') {
                    // 提取数字部分并格式化为6位小数
                    const amount = parseFloat(reward);
                    reward = `${amount.toFixed(6)} XMR`;
                } else {
                    // TARI 保持原样显示
                    reward = reward.replace('TARI', 'XTM');
                }
                
                row.innerHTML = `
                    <td>${formattedTime}</td>
                    <td>${block.height}</td>
                    <td><span class="block-id">${block.block_id || '-'}</span></td>
                    <td><span class="block-type ${typeClass}">${block.type}</span></td>
                    <td>${reward}</td>
                    <td><span class="block-status ${statusClass}">${statusText}</span></td>
                `;
                
                blocksList.appendChild(row);
            });
        })
        .catch(error => console.error('获取区块数据失败:', error));
}

// 更新矿池状态
function updatePoolStatus() {
    fetch('/api/pool_status')
        .then(response => response.json())
        .then(data => {
            // 格式化算力显示
            const formatHashrate = (hashrate) => {
                if (hashrate >= 1e9) {
                    return (hashrate / 1e9).toFixed(2) + ' GH/s';
                } else if (hashrate >= 1e6) {
                    return (hashrate / 1e6).toFixed(2) + ' MH/s';
                } else if (hashrate >= 1e3) {
                    return (hashrate / 1e3).toFixed(2) + ' KH/s';
                } else {
                    return hashrate.toFixed(2) + ' H/s';
                }
            };

            // 格式化余额显示
            const formatBalance = (balance, type) => {
                if (type === 'XMR') {
                    return parseFloat(balance).toFixed(6) + ' XMR';
                } else {
                    return parseFloat(balance).toFixed(2) + ' XTM';
                }
            };

            // 更新显示
            document.getElementById('total-hashrate').textContent = formatHashrate(data.hashrate_15m);
            document.getElementById('xmr-balance').textContent = formatBalance(data.total_rewards_xmr, 'XMR');
            document.getElementById('tari-balance').textContent = formatBalance(data.total_rewards_tari, 'TARI');
        })
        .catch(error => console.error('获取矿池状态失败:', error));
}

// 定期更新数据
setInterval(updateBlocks, 10000);
setInterval(updatePoolStatus, 10000);

// 页面加载时立即更新一次
updateBlocks();
updatePoolStatus(); 