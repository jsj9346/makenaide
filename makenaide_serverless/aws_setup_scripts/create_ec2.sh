#!/bin/bash
# create_ec2.sh - EC2 인스턴스 생성 스크립트

set -e

echo "🖥️ EC2 인스턴스 생성 시작"
echo "=========================="

# 현재 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# AWS 계정 ID 및 리전 가져오기
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-northeast-2"

# 설정 파일에서 네트워크 정보 가져오기
if [ ! -f "aws_complete_config.json" ]; then
    echo "❌ aws_complete_config.json 파일을 찾을 수 없습니다."
    echo "먼저 ./run_setup.sh를 실행하세요."
    exit 1
fi

VPC_ID=$(jq -r '.vpc_id' aws_complete_config.json)
SUBNET_ID=$(jq -r '.public_subnet_id' aws_complete_config.json)
SECURITY_GROUP_ID=$(jq -r '.ec2_security_group_id' aws_complete_config.json)
EC2_INSTANCE_PROFILE=$(jq -r '.ec2_instance_profile_arn' aws_complete_config.json | cut -d'/' -f2)

# EC2 인스턴스 설정
INSTANCE_TYPE="t3.medium"  # Makenaide 실행에 적합한 사양
AMI_ID="ami-0c2d3e23e757b5d84"  # Amazon Linux 2023 (ap-northeast-2)
KEY_PAIR_NAME="makenaide-keypair"
INSTANCE_NAME="makenaide-ec2"

echo "📋 EC2 인스턴스 설정:"
echo "  - 인스턴스 타입: $INSTANCE_TYPE"
echo "  - AMI ID: $AMI_ID"
echo "  - VPC ID: $VPC_ID"
echo "  - 서브넷 ID: $SUBNET_ID"
echo "  - 보안 그룹 ID: $SECURITY_GROUP_ID"

# 1. 키 페어 생성 (SSH 접근용)
echo "🔐 키 페어 생성 중..."

if aws ec2 describe-key-pairs --key-names $KEY_PAIR_NAME &>/dev/null; then
    echo "✅ 키 페어가 이미 존재합니다: $KEY_PAIR_NAME"
else
    aws ec2 create-key-pair \
        --key-name $KEY_PAIR_NAME \
        --query 'KeyMaterial' \
        --output text > ${KEY_PAIR_NAME}.pem
    
    chmod 400 ${KEY_PAIR_NAME}.pem
    echo "✅ 키 페어 생성 완료: $KEY_PAIR_NAME"
fi

# 2. 사용자 데이터 스크립트 생성 (부팅 시 자동 실행)
echo "📝 사용자 데이터 스크립트 생성 중..."

cat > user_data.sh << 'EOF'
#!/bin/bash
# EC2 인스턴스 부팅 시 자동 실행 스크립트

# 로그 파일 설정
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
echo "$(date): 사용자 데이터 스크립트 시작"

# 시스템 업데이트
yum update -y
echo "$(date): 시스템 업데이트 완료"

# Python 3.11 설치
yum install -y python3.11 python3.11-pip git
echo "$(date): Python 3.11 설치 완료"

# pip 업그레이드
python3.11 -m pip install --upgrade pip
echo "$(date): pip 업그레이드 완료"

# 필요한 패키지 설치
yum install -y postgresql15 postgresql15-devel gcc
echo "$(date): PostgreSQL 클라이언트 설치 완료"

# makenaide 사용자 생성
useradd -m -s /bin/bash makenaide
usermod -aG wheel makenaide
echo "$(date): makenaide 사용자 생성 완료"

# makenaide 프로젝트 디렉토리 생성
mkdir -p /home/makenaide/app
chown -R makenaide:makenaide /home/makenaide/app
echo "$(date): 프로젝트 디렉토리 생성 완료"

# 자동 실행 스크립트 생성
cat > /home/makenaide/run_makenaide.sh << 'SCRIPT_EOF'
#!/bin/bash
# Makenaide 자동 실행 스크립트

# 로그 설정
LOG_FILE="/home/makenaide/makenaide_auto.log"
echo "$(date): Makenaide 자동 실행 시작" >> $LOG_FILE

# 프로젝트 디렉토리로 이동
cd /home/makenaide/app

# GitHub에서 최신 코드 가져오기 (첫 실행 시)
if [ ! -d ".git" ]; then
    echo "$(date): GitHub에서 프로젝트 클론 중..." >> $LOG_FILE
    git clone https://github.com/13ruce/makenaide.git .
    echo "$(date): 프로젝트 클론 완료" >> $LOG_FILE
else
    echo "$(date): 최신 코드 업데이트 중..." >> $LOG_FILE
    git pull origin main
    echo "$(date): 코드 업데이트 완료" >> $LOG_FILE
fi

# Python 가상환경 생성 및 활성화
if [ ! -d "venv" ]; then
    echo "$(date): Python 가상환경 생성 중..." >> $LOG_FILE
    python3.11 -m venv venv
    echo "$(date): 가상환경 생성 완료" >> $LOG_FILE
fi

source venv/bin/activate
echo "$(date): 가상환경 활성화 완료" >> $LOG_FILE

# 의존성 설치
echo "$(date): 의존성 설치 중..." >> $LOG_FILE
pip install --upgrade pip
pip install -r requirements.txt
echo "$(date): 의존성 설치 완료" >> $LOG_FILE

# 환경변수 파일 복사 (AWS 환경)
if [ -f "env.aws" ]; then
    cp env.aws .env
    echo "$(date): AWS 환경변수 파일 적용" >> $LOG_FILE
else
    echo "$(date): 경고 - env.aws 파일이 없습니다" >> $LOG_FILE
fi

# DB 초기화 (필요한 경우)
echo "$(date): DB 연결 테스트 중..." >> $LOG_FILE
python3.11 -c "
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    print('DB 연결 성공')
    conn.close()
except Exception as e:
    print(f'DB 연결 실패: {e}')
" >> $LOG_FILE 2>&1

# Makenaide 실행
echo "$(date): Makenaide 실행 시작" >> $LOG_FILE
python3.11 makenaide.py >> $LOG_FILE 2>&1

# 실행 완료
echo "$(date): Makenaide 실행 완료" >> $LOG_FILE

# 실행 완료 후 인스턴스 자동 종료 시그널 생성
echo "$(date): 실행 완료 시그널 생성" >> $LOG_FILE
touch /tmp/makenaide_completed

# 10분 후 자동 종료 (안전장치)
echo "$(date): 10분 후 자동 종료 예약" >> $LOG_FILE
sudo shutdown -h +10 "Makenaide 실행 완료 후 자동 종료" &

SCRIPT_EOF

# 실행 권한 부여
chown makenaide:makenaide /home/makenaide/run_makenaide.sh
chmod +x /home/makenaide/run_makenaide.sh
echo "$(date): 자동 실행 스크립트 생성 완료"

# cron에 부팅 시 자동 실행 등록
echo "@reboot /home/makenaide/run_makenaide.sh" | crontab -u makenaide -
echo "$(date): cron 자동 실행 등록 완료"

# AWS CLI 설치
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
echo "$(date): AWS CLI 설치 완료"

echo "$(date): 사용자 데이터 스크립트 완료"
EOF

# 3. EC2 인스턴스 시작
echo "🚀 EC2 인스턴스 시작 중..."

INSTANCE_ID=$(aws ec2 run-instances \
    --image-id $AMI_ID \
    --count 1 \
    --instance-type $INSTANCE_TYPE \
    --key-name $KEY_PAIR_NAME \
    --security-group-ids $SECURITY_GROUP_ID \
    --subnet-id $SUBNET_ID \
    --iam-instance-profile Name=$EC2_INSTANCE_PROFILE \
    --user-data file://user_data.sh \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME},{Key=Project,Value=Makenaide},{Key=AutoShutdown,Value=true}]" \
    --query 'Instances[0].InstanceId' \
    --output text)

echo "✅ EC2 인스턴스 생성 완료: $INSTANCE_ID"

# 4. 인스턴스 상태 확인
echo "⏳ 인스턴스 시작 대기 중..."
aws ec2 wait instance-running --instance-ids $INSTANCE_ID
echo "✅ 인스턴스 실행 중: $INSTANCE_ID"

# 5. 퍼블릭 IP 가져오기
PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text)

echo "🌐 퍼블릭 IP: $PUBLIC_IP"

# 6. EC2 설정 정보 저장
cat > aws_ec2_config.json << EOF
{
  "instance_id": "$INSTANCE_ID",
  "instance_name": "$INSTANCE_NAME",
  "instance_type": "$INSTANCE_TYPE",
  "ami_id": "$AMI_ID",
  "key_pair_name": "$KEY_PAIR_NAME",
  "public_ip": "$PUBLIC_IP",
  "vpc_id": "$VPC_ID",
  "subnet_id": "$SUBNET_ID",
  "security_group_id": "$SECURITY_GROUP_ID",
  "instance_profile": "$EC2_INSTANCE_PROFILE",
  "status": "running",
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
}
EOF

echo "💾 EC2 설정 정보 저장 완료: aws_ec2_config.json"

# 임시 파일 정리
rm -f user_data.sh

echo ""
echo "🎉 EC2 인스턴스 생성 완료!"
echo "=========================="
echo "인스턴스 ID: $INSTANCE_ID"
echo "인스턴스 이름: $INSTANCE_NAME"
echo "퍼블릭 IP: $PUBLIC_IP"
echo "키 페어: $KEY_PAIR_NAME.pem"
echo "=========================="
echo ""
echo "📝 SSH 접속 방법:"
echo "ssh -i $KEY_PAIR_NAME.pem ec2-user@$PUBLIC_IP"
echo ""
echo "⚠️ 주의사항:"
echo "- 키 페어 파일($KEY_PAIR_NAME.pem)을 안전하게 보관하세요"
echo "- 인스턴스는 부팅 시 자동으로 Makenaide를 실행합니다"
echo "- 실행 완료 후 10분 뒤 자동으로 종료됩니다" 