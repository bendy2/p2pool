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

// 更新用户信息
async function updateUserInfo() {
    try {
        const username = window.location.pathname.split('/').pop();
        const response = await fetch(`/api/user/${username}`);
        const data = await response.json();
        
        if (data.error) {
            console.error('获取用户信息失败:', data.error);
            return;
        }

        document.getElementById('current-hashrate').textContent = formatHashrate(data.current_hashrate);
        document.getElementById('total-rewards').textContent = formatXMR(data.total_rewards);
        document.getElementById('balance').textContent = formatXMR(data.balance);

        // 更新奖励历史
        const rewardsList = document.getElementById('rewards-list');
        rewardsList.innerHTML = '';
        data.rewards.forEach(reward => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${formatTime(reward.timestamp)}</td>
                <td>${formatNumber(reward.height)}</td>
                <td>${formatXMR(reward.amount)}</td>
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
                <td>${formatXMR(payment.amount)}</td>
                <td>${payment.status}</td>
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