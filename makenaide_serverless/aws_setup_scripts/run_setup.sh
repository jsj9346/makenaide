#!/bin/bash
# run_setup.sh - 1단계 AWS 환경 설정 메인 스크립트

set -e  # 오류 발생 시 스크립트 중단

echo "🚀 Makenaide AWS 환경 설정 시작"
echo "=================================="
echo "1단계: AWS 환경 설정"
echo "=================================="

# 현재 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# AWS CLI 확인
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI가 설치되지 않았습니다."
    echo "설치 방법: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    exit 1
fi

# AWS 자격 증명 확인
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS 자격 증명이 설정되지 않았습니다."
    echo "설정 방법: aws configure"
    exit 1
fi

echo "✅ AWS CLI 및 자격 증명 확인 완료"

# 1. VPC 및 보안 그룹 생성
echo ""
echo "📦 1/3 VPC 및 보안 그룹 생성 중..."
if [ -f "create_vpc.sh" ]; then
    chmod +x create_vpc.sh
    ./create_vpc.sh
else
    echo "❌ create_vpc.sh 파일을 찾을 수 없습니다."
    exit 1
fi

# 2. RDS PostgreSQL 인스턴스 생성
echo ""
echo "🗄️ 2/3 RDS PostgreSQL 인스턴스 생성 중..."
if [ -f "create_rds.sh" ]; then
    chmod +x create_rds.sh
    ./create_rds.sh
else
    echo "❌ create_rds.sh 파일을 찾을 수 없습니다."
    exit 1
fi

# 3. IAM 역할 및 정책 설정
echo ""
echo "🔐 3/3 IAM 역할 및 정책 설정 중..."
if [ -f "create_iam_roles.sh" ]; then
    chmod +x create_iam_roles.sh
    ./create_iam_roles.sh
else
    echo "❌ create_iam_roles.sh 파일을 찾을 수 없습니다."
    exit 1
fi

# 설정 정보 통합
echo ""
echo "💾 설정 정보 통합 중..."
if [ -f "aws_vpc_config.json" ] && [ -f "aws_rds_config.json" ] && [ -f "aws_iam_config.json" ]; then
    jq -s '.[0] * .[1] * .[2]' aws_vpc_config.json aws_rds_config.json aws_iam_config.json > aws_complete_config.json
    echo "✅ 통합 설정 정보 저장 완료: aws_complete_config.json"
else
    echo "⚠️ 일부 설정 파일이 누락되었습니다."
fi

# 환경변수 파일 생성
echo ""
echo "📝 AWS 환경변수 파일 생성 중..."
if [ -f "aws_rds_config.json" ]; then
    DB_ENDPOINT=$(jq -r '.db_endpoint' aws_rds_config.json)
    DB_PORT=$(jq -r '.db_port' aws_rds_config.json)
    DB_NAME=$(jq -r '.db_name' aws_rds_config.json)
    DB_USERNAME=$(jq -r '.db_username' aws_rds_config.json)
    DB_PASSWORD=$(jq -r '.db_password' aws_rds_config.json)
    
    cat > ../env.aws.template << EOF
# =============================================================================
# AWS 환경 변수 설정
# =============================================================================

# AWS 설정
AWS_REGION=ap-northeast-2
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key

# RDS PostgreSQL 설정
DB_HOST=$DB_ENDPOINT
DB_PORT=$DB_PORT
DB_NAME=$DB_NAME
DB_USER=$DB_USERNAME
DB_PASSWORD=$DB_PASSWORD

# Upbit API 설정 (EIP 고정 필요)
UPBIT_ACCESS_KEY=your_upbit_access_key
UPBIT_SECRET_KEY=your_upbit_secret_key

# OpenAI API 설정
OPENAI_API_KEY=your_openai_api_key

# AWS 환경 설정
MAKENAIDE_ENV=production
AWS_OPTIMIZATION_ENABLED=true
LOG_RETENTION_DAYS=7
DATA_RETENTION_DAYS=451

# 성능 설정
MAX_MEMORY_USAGE_MB=2048
BATCH_SIZE=50
API_SLEEP_TIME=0.1
EOF

    echo "✅ AWS 환경변수 파일 생성 완료: ../env.aws.template"
fi

# 최종 결과 출력
echo ""
echo "🎉 1단계 AWS 환경 설정 완료!"
echo "=================================="
echo "✅ VPC 및 보안 그룹 생성 완료"
echo "✅ RDS PostgreSQL 인스턴스 생성 완료"
echo "✅ IAM 역할 및 정책 설정 완료"
echo "✅ 설정 정보 파일 생성 완료"
echo "=================================="
echo ""
echo "📋 다음 단계 준비사항:"
echo "1. ../env.aws.template 파일을 ../env.aws로 복사"
echo "2. 실제 API 키와 비밀번호로 값 변경"
echo "3. 데이터베이스 초기화: ./init_database.sh"
echo "4. 2단계 리소스 최적화 진행"
echo ""
echo "⚠️ 중요:"
echo "- 데이터베이스 비밀번호를 안전하게 보관하세요"
echo "- API 키는 절대 Git에 커밋하지 마세요"
echo "- 실제 운영 전에 보안 설정을 검토하세요"
echo ""
echo "다음 명령어로 2단계를 진행하세요:"
echo "cd .. && ./aws_setup_scripts/optimize_resources.sh" 