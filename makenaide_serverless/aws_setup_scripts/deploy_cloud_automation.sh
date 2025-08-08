#!/bin/bash
# deploy_cloud_automation.sh - Makenaide AWS 클라우드 자동화 배포 스크립트 (2단계)

set -e

echo "🚀 Makenaide AWS 클라우드 자동화 배포 시작"
echo "============================================="
echo "2단계: 클라우드 자동화 시스템 구축"
echo "============================================="

# 현재 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# AWS CLI 확인
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI가 설치되지 않았습니다."
    exit 1
fi

# AWS 자격 증명 확인
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS 자격 증명이 설정되지 않았습니다."
    exit 1
fi

# 1단계 완료 확인
if [ ! -f "aws_complete_config.json" ]; then
    echo "❌ 1단계 설정이 완료되지 않았습니다."
    echo "먼저 ./run_setup.sh를 실행하세요."
    exit 1
fi

echo "✅ 사전 요구사항 확인 완료"

# 1. Lambda 함수 생성
echo ""
echo "🔧 1/4 Lambda 함수 생성 중..."
if [ -f "create_lambda.sh" ]; then
    chmod +x create_lambda.sh
    ./create_lambda.sh
else
    echo "❌ create_lambda.sh 파일을 찾을 수 없습니다."
    exit 1
fi

# 2. EventBridge 스케줄 설정
echo ""
echo "⏰ 2/4 EventBridge 스케줄 설정 중..."
if [ -f "create_eventbridge.sh" ]; then
    chmod +x create_eventbridge.sh
    ./create_eventbridge.sh
else
    echo "❌ create_eventbridge.sh 파일을 찾을 수 없습니다."
    exit 1
fi

# 3. EC2 인스턴스 생성
echo ""
echo "🖥️ 3/4 EC2 인스턴스 생성 중..."
if [ -f "create_ec2.sh" ]; then
    chmod +x create_ec2.sh
    ./create_ec2.sh
else
    echo "❌ create_ec2.sh 파일을 찾을 수 없습니다."
    exit 1
fi

# 4. 환경변수 파일 업데이트
echo ""
echo "📝 4/4 AWS 환경변수 파일 업데이트 중..."

# 최신 설정 정보 통합
if [ -f "aws_lambda_config.json" ] && [ -f "aws_eventbridge_config.json" ] && [ -f "aws_ec2_config.json" ]; then
    jq -s '.[0] * .[1] * .[2] * .[3]' \
        aws_complete_config.json \
        aws_lambda_config.json \
        aws_eventbridge_config.json \
        aws_ec2_config.json > aws_final_config.json
    
    echo "✅ 최종 설정 정보 통합 완료: aws_final_config.json"
else
    echo "⚠️ 일부 설정 파일이 누락되었습니다."
fi

# 최종 환경변수 파일 생성
if [ -f "aws_final_config.json" ]; then
    DB_ENDPOINT=$(jq -r '.db_endpoint' aws_final_config.json)
    DB_PORT=$(jq -r '.db_port' aws_final_config.json)
    DB_NAME=$(jq -r '.db_name' aws_final_config.json)
    DB_USERNAME=$(jq -r '.db_username' aws_final_config.json)
    DB_PASSWORD=$(jq -r '.db_password' aws_final_config.json)
    INSTANCE_ID=$(jq -r '.instance_id' aws_final_config.json)
    PUBLIC_IP=$(jq -r '.public_ip' aws_final_config.json)
    
    cat > ../env.aws << EOF
# =============================================================================
# AWS 클라우드 자동화 환경 변수 설정
# 생성일: $(date)
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

# PostgreSQL 환경변수 (호환성)
PG_HOST=$DB_ENDPOINT
PG_PORT=$DB_PORT
PG_DATABASE=$DB_NAME
PG_USER=$DB_USERNAME
PG_PASSWORD=$DB_PASSWORD

# Upbit API 설정 (실제 값으로 변경 필요)
UPBIT_ACCESS_KEY=your_upbit_access_key
UPBIT_SECRET_KEY=your_upbit_secret_key

# OpenAI API 설정 (실제 값으로 변경 필요)
OPENAI_API_KEY=your_openai_api_key

# AWS 클라우드 자동화 설정
MAKENAIDE_ENV=production
AWS_OPTIMIZATION_ENABLED=true
AUTO_SHUTDOWN_ENABLED=true
CLOUD_AUTOMATION=true

# 로그 및 데이터 관리
LOG_RETENTION_DAYS=7
DATA_RETENTION_DAYS=451
CLEANUP_ON_STARTUP=true

# 성능 최적화 설정
MAX_MEMORY_USAGE_MB=2048
BATCH_SIZE=50
API_SLEEP_TIME=0.1
PARALLEL_WORKERS=4

# EC2 인스턴스 정보
EC2_INSTANCE_ID=$INSTANCE_ID
EC2_PUBLIC_IP=$PUBLIC_IP

# 알림 설정 (선택사항)
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
NOTIFICATION_ENABLED=false
EOF

    echo "✅ 최종 AWS 환경변수 파일 생성 완료: ../env.aws"
fi

# 설정 요약 출력
echo ""
echo "📊 클라우드 자동화 시스템 구성 완료!"
echo "============================================="

if [ -f "aws_final_config.json" ]; then
    LAMBDA_FUNCTION=$(jq -r '.lambda_function_name' aws_final_config.json)
    START_RULE=$(jq -r '.start_rule_name' aws_final_config.json)
    SHUTDOWN_RULE=$(jq -r '.shutdown_rule_name' aws_final_config.json)
    INSTANCE_ID=$(jq -r '.instance_id' aws_final_config.json)
    PUBLIC_IP=$(jq -r '.public_ip' aws_final_config.json)
    
    echo "✅ Lambda 함수: $LAMBDA_FUNCTION"
    echo "✅ EventBridge 시작 규칙: $START_RULE (4시간 간격)"
    echo "✅ EventBridge 종료 규칙: $SHUTDOWN_RULE"
    echo "✅ EC2 인스턴스: $INSTANCE_ID"
    echo "✅ 퍼블릭 IP: $PUBLIC_IP"
fi

echo "============================================="
echo ""
echo "🎯 클라우드 운영 플로우:"
echo "1. EventBridge → 4시간마다 트리거"
echo "2. Lambda → EC2 인스턴스 자동 On"
echo "3. EC2 부팅 시 → makenaide 자동 실행"
echo "4. 전체 파이프라인 수행"
echo "5. 작업 완료 시 → EC2 인스턴스 자동 Off"
echo "6. 다음 주기까지 대기"
echo ""
echo "📋 다음 작업 사항:"
echo "1. ../env.aws 파일에서 실제 API 키 설정"
echo "2. Upbit API 키 (UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)"
echo "3. OpenAI API 키 (OPENAI_API_KEY)"
echo "4. AWS 액세스 키 (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)"
echo "5. 데이터베이스 비밀번호 확인 및 업데이트"
echo ""
echo "⚠️ 보안 주의사항:"
echo "- API 키는 절대 Git에 커밋하지 마세요"
echo "- 환경변수 파일 권한을 적절히 설정하세요 (chmod 600)"
echo "- 정기적으로 API 키를 교체하세요"
echo ""
echo "🔧 테스트 방법:"
echo "1. Lambda 함수 수동 테스트:"
echo "   aws lambda invoke --function-name makenaide-controller --payload '{\"action\":\"start\"}' response.json"
echo ""
echo "2. EventBridge 규칙 상태 확인:"
echo "   aws events list-rules --name-prefix makenaide"
echo ""
echo "3. EC2 인스턴스 상태 확인:"
echo "   aws ec2 describe-instances --instance-ids $INSTANCE_ID"
echo ""
echo "🎉 Makenaide AWS 클라우드 자동화 시스템 구축 완료!" 