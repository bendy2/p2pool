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
            
            // 用于存储XMR和TARI区块的时间间隔数据
            const xmrIntervals = [];
            const tariIntervals = [];
            
            data.forEach((block, index) => {
                const row = document.createElement('tr');
                const blockType = block.type.toLowerCase();
                row.className = blockType === 'xmr' ? 'block-xmr' : 'block-tari';
                
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
                
                // 计算区块时间间隔
                if (index > 0) {
                    const prevBlock = data[index - 1];
                    const prevTime = new Date(prevBlock.timestamp);
                    const timeDiff = (time - prevTime) / (1000 * 60); // 转换为分钟
                    
                    const intervalData = [beijingTime.getTime(), timeDiff];
                    if (blockType === 'xmr') {
                        xmrIntervals.push(intervalData);
                    } else {
                        tariIntervals.push(intervalData);
                    }
                }
                
                const typeClass = blockType === 'xmr' ? 'block-type-xmr' : 'block-type-tari';
                
                // 修改状态显示逻辑
                let statusClass, statusText;
                if (!block.check_status) {
                    statusClass = 'status-pending';
                    statusText = '待检查';
                } else {
                    statusClass = block.is_valid ? 'status-valid' : 'status-invalid';
                    statusText = block.is_valid ? '有效' : '无效';
                }
                
                // 修改奖励显示
                let reward = block.reward;
                if (blockType === 'xmr') {
                    const amount = parseFloat(reward);
                    reward = `${amount.toFixed(6)} XMR`;
                } else {
                    reward = reward.replace('TARI', 'XTM');
                }
                
                row.innerHTML = `
                    <td>${formattedTime}</td>
                    <td>${block.height}</td>
                    <td><span class="block-id">${block.block_id || '-'}</span></td>
                    <td><span class="block-type ${typeClass}">${block.type.toUpperCase()}</span></td>
                    <td>${reward}</td>
                    <td><span class="block-status ${statusClass}">${statusText}</span></td>
                `;
                
                blocksList.appendChild(row);
            });

            // 更新区块时间间隔图表
            const xmrChart = echarts.getInstanceByDom(document.getElementById('xmrBlockIntervalChart'));
            const tariChart = echarts.getInstanceByDom(document.getElementById('tariBlockIntervalChart'));
            
            if (xmrChart) {
                xmrChart.setOption({
                    series: [{
                        data: xmrIntervals
                    }]
                });
            }
            
            if (tariChart) {
                tariChart.setOption({
                    series: [{
                        data: tariIntervals
                    }]
                });
            }

            // 在更新完区块列表后，调用updateBlockStats更新区块统计图表
            if (typeof updateBlockStats === 'function') {
                updateBlockStats();
            }
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
            document.getElementById('active-miners').textContent = data.active_miners;
            document.getElementById('xmr-balance').textContent = formatBalance(data.total_rewards_xmr, 'XMR');
            document.getElementById('tari-balance').textContent = formatBalance(data.total_rewards_tari, 'TARI');
            document.getElementById('xmr-paid').textContent = formatBalance(data.total_paid_xmr, 'XMR');
            document.getElementById('tari-paid').textContent = formatBalance(data.total_paid_tari, 'TARI');

            // 更新在线矿工列表
            const minersList = document.getElementById('online-miners-list');
            minersList.innerHTML = '';
            
            data.online_miners.forEach(miner => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td><a href="/u/${miner.username}">${miner.username}</a></td>
                    <td>${formatHashrate(miner.hashrate)}</td>
                    <td>${miner.xmr_share}</td>
                    <td>${miner.tari_share}</td>
                `;
                minersList.appendChild(row);
            });
        })
        .catch(error => console.error('获取矿池状态失败:', error));
}


// 格式化数字，保留指定小数位
function formatNumber(number, decimals) {
    return number.toFixed(decimals);
}

// 格式化算力显示
function formatHashrate(hashrate) {
    if (hashrate >= 1e9) {
        return (hashrate / 1e9).toFixed(2) + ' GH/s';
    }
    if (hashrate >= 1e6) {
        return (hashrate / 1e6).toFixed(2) + ' MH/s';
    }
    if (hashrate >= 1e3) {
        return (hashrate / 1e3).toFixed(2) + ' KH/s';
    }
    return hashrate.toFixed(2) + ' H/s';
}

// 每60秒更新一次数据
setInterval(updatePoolStatus, 60000);
setInterval(updateBlocks, 30000);  // 每30秒更新一次区块数据
setInterval(updateHashrateChart, 30000);  // 每30秒更新一次算力走势图

// 页面加载时立即更新一次
document.addEventListener('DOMContentLoaded', () => {
    updatePoolStatus();
    updateBlocks();
    updateHashrateChart();
});



// 更新算力走势图
function updateHashrateChart() {
    fetch('/api/hashrate/history')
        .then(response => response.json())
        .then(data => {
            const chart = echarts.getInstanceByDom(document.getElementById('hashrateChart'));
            if (chart) {
                const formattedData = data.history.map(item => [
                    item.timestamp,
                    item.hashrate
                ]);
                chart.setOption({
                    series: [{
                        data: formattedData
                    }]
                });
            }
        })
        .catch(error => console.error('更新算力走势图失败:', error));
}

