// 格式化数字
function formatNumber(num) {
    return new Intl.NumberFormat('zh-CN').format(num);
}

// 格式化时间（北京时间）
function formatTime(timestamp) {
    const date = new Date(timestamp * 1000);
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
}

// 格式化XMR金额
function formatXMR(amount) {
    return (amount / 1e12).toFixed(12) + ' XMR';
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

// 更新矿池状态
async function updatePoolStatus() {
    try {
        const response = await fetch('/api/pool_status');
        const data = await response.json();
        
        if (data.error) {
            console.error('获取矿池状态失败:', data.error);
            return;
        }

        document.getElementById('hashrate-15m').textContent = formatHashrate(data.hashrate_15m);
        document.getElementById('hashrate-1h').textContent = formatHashrate(data.hashrate_1h);
        document.getElementById('hashrate-24h').textContent = formatHashrate(data.hashrate_24h);
        document.getElementById('active-miners').textContent = formatNumber(data.active_miners);
        document.getElementById('total-rewards').textContent = formatXMR(data.total_rewards);
    } catch (error) {
        console.error('获取矿池状态失败:', error);
    }
}

// 更新区块列表
async function updateBlocks(type) {
    try {
        const response = await fetch(`/api/blocks/${type}`);
        const data = await response.json();
        
        if (data.error) {
            console.error(`获取${type}区块列表失败:`, data.error);
            return;
        }

        const tbody = document.getElementById(`${type}-blocks`);
        tbody.innerHTML = '';
        
        data.blocks.forEach(block => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${formatNumber(block.height)}</td>
                <td>${formatTime(block.timestamp)}</td>
                <td>${formatXMR(block.reward)}</td>
            `;
            tbody.appendChild(tr);
        });
    } catch (error) {
        console.error(`获取${type}区块列表失败:`, error);
    }
}

// 定期更新数据
function startUpdates() {
    // 立即更新一次
    updatePoolStatus();
    updateBlocks('xmr');
    updateBlocks('tari');

    // 每10秒更新一次
    setInterval(() => {
        updatePoolStatus();
        updateBlocks('xmr');
        updateBlocks('tari');
    }, 10000);
}

// 页面加载完成后开始更新
document.addEventListener('DOMContentLoaded', startUpdates); 