
# EC2에서 실행할 명령어들
echo "🚀 Setting up Makenaide Trading System"

# AWS CLI 설치 (필요한 경우)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# S3에서 파일 다운로드
mkdir -p /home/ec2-user/makenaide-trading
cd /home/ec2-user/makenaide-trading

aws s3 cp s3://makenaide-deployment/upbit_trading_module.py . --region ap-northeast-2
aws s3 cp s3://makenaide-deployment/ec2_trading_runner.py . --region ap-northeast-2
aws s3 cp s3://makenaide-deployment/deploy_makenaide.sh . --region ap-northeast-2

chmod +x deploy_makenaide.sh
chmod +x ec2_trading_runner.py

echo "✅ Files downloaded from S3"

# 배포 스크립트 실행
echo "🔧 Running deployment setup..."
./deploy_makenaide.sh

echo "🎉 Setup completed!"
echo ""
echo "다음 단계:"
echo "1. 시스템 테스트: python3 ec2_trading_runner.py --test"
echo "2. 단일 실행 테스트: python3 ec2_trading_runner.py --mode single"
echo "3. 서비스 시작: sudo systemctl start makenaide-trading"
echo "4. 서비스 상태 확인: sudo systemctl status makenaide-trading"
echo "5. 로그 확인: tail -f /var/log/makenaide/trading.log"
