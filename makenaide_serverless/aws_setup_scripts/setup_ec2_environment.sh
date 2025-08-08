#!/bin/bash
# setup_ec2_environment.sh - EC2에서 Makenaide 운영 환경 설정

echo "🚀 EC2 Makenaide 운영 환경 설정"
echo "=================================="

# 시스템 업데이트
echo "1️⃣ 시스템 업데이트 중..."
sudo yum update -y

# PostgreSQL 클라이언트 설치
echo "2️⃣ PostgreSQL 클라이언트 설치 중..."
sudo yum install -y postgresql

# Python 및 pip 확인/설치
echo "3️⃣ Python 환경 확인 중..."
if ! command -v python3 &> /dev/null; then
    sudo yum install -y python3
fi

if ! command -v pip3 &> /dev/null; then
    sudo yum install -y python3-pip
fi

# Git 설치 (코드 배포용)
echo "4️⃣ Git 설치 중..."
sudo yum install -y git

# 네트워크 도구 설치
echo "5️⃣ 네트워크 도구 설치 중..."
sudo yum install -y nc telnet

# Makenaide 디렉토리 생성 및 권한 설정
echo "6️⃣ Makenaide 디렉토리 설정 중..."
sudo mkdir -p /opt/makenaide
sudo chown ec2-user:ec2-user /opt/makenaide
cd /opt/makenaide

# Python 가상환경 생성
echo "7️⃣ Python 가상환경 생성 중..."
python3 -m venv venv
source venv/bin/activate

# 환경변수 파일 준비
echo "8️⃣ 환경변수 파일 준비 중..."
echo "다음 단계를 수행하세요:"
echo "1. 로컬에서 코드를 git push"
echo "2. EC2에서 git clone"
echo "3. env.aws.template를 .env로 복사"
echo "4. .env 파일에 실제 비밀번호와 API 키 입력"

echo ""
echo "📋 코드 배포 명령어:"
echo "git clone https://github.com/your-username/makenaide.git ."
echo "cp env.aws.template .env"
echo "# .env 파일 편집 후"
echo "pip install -r requirements.txt"

echo ""
echo "🔐 보안 설정 권장사항:"
echo "1. .env 파일 권한: chmod 600 .env"
echo "2. 로그 디렉토리: mkdir -p /opt/makenaide/logs"
echo "3. 백업 디렉토리: mkdir -p /opt/makenaide/backups"

echo ""
echo "✅ EC2 환경 설정 완료!"
echo "다음 단계: RDS 연결 테스트 실행"
echo "./test_rds_connection.sh" 