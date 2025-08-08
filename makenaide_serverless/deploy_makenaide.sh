#!/bin/bash
# Makenaide Trading System Deployment Script

echo "🚀 Starting Makenaide Trading System Deployment"
echo "================================================"

# 시스템 업데이트
echo "📦 Updating system packages..."
sudo yum update -y

# Python 패키지 설치
echo "🐍 Installing Python packages..."
sudo yum install -y python3 python3-pip git htop
pip3 install --user boto3 requests PyJWT pandas numpy

# 작업 디렉토리 설정
echo "📁 Setting up directories..."
mkdir -p /home/ec2-user/makenaide-trading
cd /home/ec2-user/makenaide-trading

# 로그 디렉토리
sudo mkdir -p /var/log/makenaide
sudo chown ec2-user:ec2-user /var/log/makenaide

# 환경 변수 설정
echo "🔧 Setting up environment..."
echo 'export AWS_DEFAULT_REGION=ap-northeast-2' >> ~/.bashrc
echo 'export PYTHONPATH=/home/ec2-user/makenaide-trading:$PYTHONPATH' >> ~/.bashrc

# 시간대 설정
sudo timedatectl set-timezone Asia/Seoul

# systemd 서비스 생성
echo "⚙️  Creating systemd service..."
sudo tee /etc/systemd/system/makenaide-trading.service > /dev/null << 'EOF'
[Unit]
Description=Makenaide Trading System
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/makenaide-trading
ExecStart=/usr/bin/python3 /home/ec2-user/makenaide-trading/ec2_trading_runner.py --mode loop --interval 60
Restart=on-failure
RestartSec=10
StandardOutput=append:/var/log/makenaide/trading.log
StandardError=append:/var/log/makenaide/trading-error.log

[Install]
WantedBy=multi-user.target
EOF

# 서비스 활성화
sudo systemctl daemon-reload
sudo systemctl enable makenaide-trading

echo "✅ Deployment script completed"
echo "Next steps:"
echo "1. Upload trading Python files"
echo "2. Test system with: python3 ec2_trading_runner.py --test"
echo "3. Start service with: sudo systemctl start makenaide-trading"
