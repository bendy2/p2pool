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

// 更新用户信息
async function updateUserInfo() {
    try {
        const username = document.querySelector('.navbar-brand').textContent.split(' - ')[0];
        const response = await fetch(`/api/user/${username}`);
        const data = await response.json();
        
        if (data.error) {
            console.error('获取用户信息失败:', data.error);
            return;
        }

        // 更新用户信息
        document.getElementById('current-hashrate').textContent = formatHashrate(data.current_hashrate);
        document.getElementById('xmr-balance').textContent = formatXMR(data.xmr_balance);
        document.getElementById('tari-balance').textContent = formatTARI(data.tari_balance);

        // 更新奖励历史
        const rewardsList = document.getElementById('rewards-list');
        rewardsList.innerHTML = '';
        data.rewards.forEach(reward => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${formatTime(reward.timestamp)}</td>
                <td>${formatNumber(reward.height)}</td>
                <td>${reward.type.toUpperCase()}</td>
                <td>${reward.type === 'xmr' ? formatXMR(reward.amount) : formatTARI(reward.amount)}</td>
                <td>${formatNumber(reward.shares)}</td>
            `;
            rewardsList.appendChild(tr);
        });

        // 更新支付历史
        const paymentsList = document.getElementById('payments-list');
        paymentsList.innerHTML = '';
        data.payments.forEach(payment => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${formatTime(payment.timestamp)}</td>
                <td>${payment.txid}</td>
                <td>${payment.type.toUpperCase()}</td>
                <td>${payment.type === 'xmr' ? formatXMR(payment.amount) : formatTARI(payment.amount)}</td>
            `;
            paymentsList.appendChild(tr);
        });
    } catch (error) {
        console.error('获取用户信息失败:', error);
    }
}

// 定期更新数据
function startUpdates() {
    // 立即更新一次
    updateUserInfo();

    // 每10秒更新一次
    setInterval(updateUserInfo, 10000);
}

// 页面加载完成后开始更新
document.addEventListener('DOMContentLoaded', startUpdates); 