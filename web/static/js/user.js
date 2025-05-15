// 格式化数字
function formatNumber(num) {
    return new Intl.NumberFormat('zh-CN').format(num);
}

// 格式化时间为北京时间
function formatTime(utcTime) {
    if (!utcTime) return '-';
    const date = new Date(utcTime);
    // 直接使用 toLocaleString，它会自动处理时区转换
    return date.toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZone: 'Asia/Shanghai'  // 指定时区为上海（北京时间）
    });
}

// 格式化XMR金额
function formatXMR(amount) {
    return (parseFloat(amount) || 0).toFixed(12) + ' XMR';
}

// 格式化TARI金额
function formatTARI(amount) {
    return (parseFloat(amount) || 0).toFixed(2) + ' XTM';
}

// 格式化算力
function formatHashrate(hashrate) {
    if (!hashrate) return '0 H/s';
    const units = ['H/s', 'KH/s', 'MH/s', 'GH/s', 'TH/s'];
    let value = parseFloat(hashrate);
    let unitIndex = 0;
    
    while (value >= 1000 && unitIndex < units.length - 1) {
        value /= 1000;
        unitIndex++;
    }
    
    return value.toFixed(2) + ' ' + units[unitIndex];
}

// 格式化费率显示
function formatFee(fee) {
    return (fee * 100).toFixed(2) + '%';
}

// 从URL路径中获取用户名
function getUsernameFromPath() {
    const path = window.location.pathname;
    const match = path.match(/\/u\/([^\/]+)/);
    return match ? match[1] : null;
}

// 更新奖励历史
function updateRewardsList(rewards) {
    const tbody = document.getElementById('rewards-list');
    tbody.innerHTML = '';
    
    rewards.forEach(reward => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${formatTime(reward.timestamp)}</td>
            <td>${reward.height}</td>
            <td>${reward.type.toUpperCase()}</td>
            <td>${reward.type === 'xmr' ? formatXMR(reward.amount) : formatTARI(reward.amount)}</td>
            <td>${reward.shares}</td>
        `;
        tbody.appendChild(row);
    });
}

// 更新支付历史
function updatePaymentsList(payments) {
    const tbody = document.getElementById('payments-list');
    tbody.innerHTML = '';
    
    payments.forEach(payment => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${formatTime(payment.timestamp)}</td>
            <td>${payment.txid || '-'}</td>
            <td>${payment.type.toUpperCase()}</td>
            <td>${payment.type === 'xmr' ? formatXMR(payment.amount) : formatTARI(payment.amount)}</td>
        `;
        tbody.appendChild(row);
    });
}

// 更新用户信息
function updateUserInfo() {
    const username = getUsernameFromPath();
    if (!username) {
        console.error('无法获取用户名');
        return;
    }
    
    fetch(`/api/user/${username}`)
        .then(response => response.json())
        .then(data => {
            // 更新基本信息
            document.getElementById('username').textContent = data.username;
            document.getElementById('created-at').textContent = formatTime(data.created_at);
            document.getElementById('current-hashrate').textContent = formatHashrate(data.current_hashrate);
            document.getElementById('user-fee').textContent = formatFee(data.fee);
            
            // 更新钱包地址
            document.getElementById('xmr-wallet').textContent = data.xmr_wallet || '未设置';
            document.getElementById('tari-wallet').textContent = data.tari_wallet || '未设置';
            
            // 更新余额
            document.getElementById('xmr-balance').textContent = formatXMR(data.xmr_balance);
            document.getElementById('tari-balance').textContent = formatTARI(data.tari_balance);
            
            // 更新奖励历史
            updateRewardsList(data.rewards);
            
            // 更新支付历史
            updatePaymentsList(data.payments);
        })
        .catch(error => console.error('获取用户信息失败:', error));
}

// 初始化复制按钮
document.addEventListener('DOMContentLoaded', function() {
    // 为所有复制按钮添加点击事件
    document.querySelectorAll('.copy-btn').forEach(button => {
        button.addEventListener('click', function() {
            const targetId = this.getAttribute('data-clipboard-target');
            const text = document.querySelector(targetId).textContent;
            
            // 复制到剪贴板
            navigator.clipboard.writeText(text).then(() => {
                // 显示复制成功提示
                const originalText = this.textContent;
                this.textContent = '已复制';
                this.classList.remove('btn-outline-primary');
                this.classList.add('btn-success');
                
                // 2秒后恢复按钮状态
                setTimeout(() => {
                    this.textContent = originalText;
                    this.classList.remove('btn-success');
                    this.classList.add('btn-outline-primary');
                }, 2000);
            }).catch(err => {
                console.error('复制失败:', err);
            });
        });
    });
    
    // 开始定期更新
    updateUserInfo();
    setInterval(updateUserInfo, 10000);
}); 