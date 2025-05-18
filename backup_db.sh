#!/bin/bash

# 设置变量
BACKUP_DIR="/root/backups"  # 备份文件存储目录
DB_NAME="p2pool"           # 数据库名称
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_${TIMESTAMP}.sql.gz"
CONFIG_FILE="config.json"

# 从config.json读取数据库配置
DB_USER=$(jq -r '.database.user' $CONFIG_FILE)
DB_PASS=$(jq -r '.database.password' $CONFIG_FILE)
DB_HOST=$(jq -r '.database.host' $CONFIG_FILE)
DB_PORT=$(jq -r '.database.port' $CONFIG_FILE)

# 创建备份目录（如果不存在）
mkdir -p $BACKUP_DIR

# 执行备份
PGPASSWORD=$DB_PASS pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME | gzip > $BACKUP_FILE

# 删除7天前的备份文件
find $BACKUP_DIR -name "${DB_NAME}_*.sql.gz" -mtime +7 -delete

# 记录备份日志
echo "$(date '+%Y-%m-%d %H:%M:%S') - Backup completed: $BACKUP_FILE" >> $BACKUP_DIR/backup.log 
