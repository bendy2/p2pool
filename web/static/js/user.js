// 格式化数字
function formatNumber(num) {
    return new Intl.NumberFormat('zh-CN').format(num);
}

// 格式化时间为北京时间
function formatTime(utcTime) {
    if (!utcTime) return '-';
    const date = new Date(utcTime);
    // 转换为北京时间 (UTC+8)
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

// 格式化XMR金额（6位小数）
function formatXMR(amount) {
    return parseFloat(amount).toFixed(6) + ' XMR';
}

// 格式化TARI金额（2位小数）
function formatTARI(amount) {
    return parseFloat(amount).toFixed(2) + ' XTM';
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
function updateRewardsHistory(rewards) {
    const rewardsList = document.getElementById('rewards-list');
    rewardsList.innerHTML = '';
    
    rewards.forEach(reward => {
        const row = document.createElement('tr');
        // 根据类型格式化金额
        const formattedAmount = reward.type === 'xmr' 
            ? formatXMR(reward.amount)
            : formatTARI(reward.amount);
            
        // 根据类型格式化份额
        const formattedShares = formatNumber(reward.shares);
        
        row.innerHTML = `
            <td>${formatTime(reward.timestamp)}</td>
            <td>${reward.height}</td>
            <td>
                <span class="block-type block-type-${reward.type.toLowerCase()}">
                    ${reward.type === 'tari' ? 'XTM' : reward.type.toUpperCase()}
                </span>
            </td>
            <td>${formattedAmount}</td>
            <td>${formattedShares}</td>
        `;
        rewardsList.appendChild(row);
    });
}

// 更新支付历史
function updatePaymentsHistory(payments) {
    const paymentsList = document.getElementById('payments-list');
    paymentsList.innerHTML = '';
    
    payments.forEach(payment => {
        const row = document.createElement('tr');
        // 根据类型格式化金额
        const formattedAmount = payment.type === 'xmr' 
            ? formatXMR(payment.amount)
            : formatTARI(payment.amount);
            
        row.innerHTML = `
            <td>${formatTime(payment.timestamp)}</td>
            <td>
                <a href="https://explorer.tari.com/transaction/${payment.txid}" target="_blank" class="txid-link">
                    ${payment.txid}
                </a>
            </td>
            <td>
                <span class="block-type block-type-${payment.type.toLowerCase()}">
                    ${payment.type === 'tari' ? 'XTM' : payment.type.toUpperCase()}
                </span>
            </td>
            <td>${formattedAmount}</td>
        `;
        paymentsList.appendChild(row);
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
            updateRewardsHistory(data.rewards);
            
            // 更新支付历史
            updatePaymentsHistory(data.payments);
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