#!/bin/bash
# DB Initializer Lambda 배포 스크립트
# AWS Lambda에 DB 초기화 함수 배포

set -e

FUNCTION_NAME="makenaide-db-initializer"
REGION="ap-northeast-2"
ROLE_ARN="arn:aws:iam::901361833359:role/makenaide-lambda-execution-role"

echo "🚀 DB 초기화 Lambda 배포 시작..."

# 기존 패키지 정리
rm -f lambda-db-init.zip

# Lambda 패키지 생성
echo "📦 Lambda 패키지 생성 중..."
zip -r lambda-db-init.zip . -x "*.sh" "*.md" "__pycache__/*" "test_*"

# 패키지 크기 확인
PACKAGE_SIZE=$(du -h lambda-db-init.zip | cut -f1)
echo "📊 패키지 크기: $PACKAGE_SIZE"

# Lambda 함수 존재 여부 확인
echo "🔍 기존 Lambda 함수 확인 중..."
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION &>/dev/null; then
    echo "🔄 기존 함수 업데이트 중..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://lambda-db-init.zip \
        --region $REGION
    echo "✅ 함수 코드 업데이트 완료"
else
    echo "🆕 새로운 Lambda 함수 생성 중..."
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime python3.11 \
        --role $ROLE_ARN \
        --handler lambda_function.lambda_handler \
        --zip-file fileb://lambda-db-init.zip \
        --timeout 300 \
        --memory-size 256 \
        --region $REGION \
        --layers arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-aws-psycopg2:1 \
        --description "Makenaide DB Schema Initializer Lambda Function"
    echo "✅ 새 Lambda 함수 생성 완료"
fi

# 환경 변수 설정 (생략 - DB 연결 정보는 코드에 하드코딩되어 있음)
echo "🔧 Lambda 함수 설정 업데이트..."
aws lambda update-function-configuration \
    --function-name $FUNCTION_NAME \
    --runtime python3.11 \
    --timeout 300 \
    --memory-size 256 \
    --layers arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-aws-psycopg2:1 \
    --region $REGION

echo "🧪 Lambda 함수 테스트 중..."

# 테스트 이벤트 생성
cat > test_event.json << EOF
{
    "operation_type": "check_schema",
    "force_recreate": false,
    "include_backtest": true
}
EOF

# Lambda 함수 실행 테스트
aws lambda invoke \
    --function-name $FUNCTION_NAME \
    --payload fileb://test_event.json \
    --region $REGION \
    response_db_init.json

# 응답 확인
echo "📋 테스트 결과:"
cat response_db_init.json | python3 -m json.tool

echo "✅ DB 초기화 Lambda 배포 완료!"
echo "📍 Lambda 함수명: $FUNCTION_NAME"
echo "🌍 리전: $REGION"