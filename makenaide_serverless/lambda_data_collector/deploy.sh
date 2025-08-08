#!/bin/bash
# Lambda Data Collector 배포 스크립트
# Phase 2 아키텍처 개선용 자동 배포

set -e

# 설정
FUNCTION_NAME="makenaide-data-collector"
RUNTIME="python3.11"
HANDLER="lambda_function.lambda_handler"
TIMEOUT=900  # 15분 (데이터 수집용)
MEMORY_SIZE=512  # 충분한 메모리로 설정
DESCRIPTION="Makenaide Phase 2 - 데이터 수집 Lambda 함수"

# 색상 출력용
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Makenaide Lambda Data Collector 배포 시작${NC}"
echo "=================================="

# 1. 작업 디렉토리 확인
if [ ! -f "lambda_function.py" ]; then
    echo -e "${RED}❌ lambda_function.py 파일이 없습니다. 올바른 디렉토리에서 실행하세요.${NC}"
    exit 1
fi

echo -e "${BLUE}📁 현재 디렉토리:${NC} $(pwd)"

# 2. 가상환경 생성 및 활성화
echo -e "${YELLOW}🔧 Python 가상환경 설정...${NC}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

source venv/bin/activate

# 3. 종속성 설치
echo -e "${YELLOW}📦 종속성 설치 중...${NC}"
pip install --upgrade pip
pip install -r requirements.txt -t .

# 4. 불필요한 파일 제거 (Lambda 패키지 크기 최적화)
echo -e "${YELLOW}🧹 불필요한 파일 정리...${NC}"
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name "*.pyo" -delete 2>/dev/null || true
find . -name ".DS_Store" -delete 2>/dev/null || true
rm -rf venv/ 2>/dev/null || true
rm -rf *.egg-info/ 2>/dev/null || true

# 5. ZIP 패키지 생성
echo -e "${YELLOW}📦 배포 패키지 생성...${NC}"
zip -r ${FUNCTION_NAME}.zip . -x "venv/*" "*.git*" "deploy.sh" "*.md" "test_*"

# 패키지 크기 확인
PACKAGE_SIZE=$(stat -f%z "${FUNCTION_NAME}.zip" 2>/dev/null || stat -c%s "${FUNCTION_NAME}.zip")
PACKAGE_SIZE_MB=$((PACKAGE_SIZE / 1024 / 1024))

echo -e "${BLUE}📊 패키지 크기:${NC} ${PACKAGE_SIZE_MB}MB"

if [ $PACKAGE_SIZE_MB -gt 50 ]; then
    echo -e "${YELLOW}⚠️ 패키지 크기가 50MB를 초과합니다. Lambda Layer 사용을 고려하세요.${NC}"
fi

# 6. Lambda 함수 존재 여부 확인
echo -e "${YELLOW}🔍 기존 Lambda 함수 확인...${NC}"
if aws lambda get-function --function-name $FUNCTION_NAME >/dev/null 2>&1; then
    echo -e "${GREEN}📝 기존 함수 업데이트...${NC}"
    
    # 함수 코드 업데이트
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://${FUNCTION_NAME}.zip
    
    # 함수 설정 업데이트
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --handler $HANDLER \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --description "$DESCRIPTION"
        
else
    echo -e "${GREEN}🆕 새 Lambda 함수 생성...${NC}"
    
    # IAM Role ARN (실제 환경에서는 적절한 Role 사용)
    ROLE_ARN="arn:aws:iam::901361833359:role/makenaide-lambda-execution-role"
    
    # 새 함수 생성
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --role $ROLE_ARN \
        --handler $HANDLER \
        --zip-file fileb://${FUNCTION_NAME}.zip \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --description "$DESCRIPTION"
fi

# 7. 환경변수 설정
echo -e "${YELLOW}🔧 환경변수 설정...${NC}"
aws lambda update-function-configuration \
    --function-name $FUNCTION_NAME \
    --environment Variables='{
        "DB_HOST":"makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com",
        "DB_PORT":"5432",
        "DB_NAME":"makenaide",
        "DB_USER":"bruce",
        "DB_PASSWORD":"0asis314."
    }'

# 8. 함수 정보 출력
echo -e "${GREEN}✅ 배포 완료!${NC}"
echo "=================================="
echo -e "${BLUE}함수 이름:${NC} $FUNCTION_NAME"
echo -e "${BLUE}런타임:${NC} $RUNTIME"
echo -e "${BLUE}핸들러:${NC} $HANDLER"
echo -e "${BLUE}타임아웃:${NC} ${TIMEOUT}초"
echo -e "${BLUE}메모리:${NC} ${MEMORY_SIZE}MB"

# 9. 테스트 호출 (선택사항)
echo ""
read -p "배포된 함수를 테스트하시겠습니까? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}🧪 함수 테스트 중...${NC}"
    
    # 테스트 페이로드
    cat > test_payload.json << EOF
{
    "collection_type": "ohlcv_daily",
    "tickers": ["KRW-BTC"],
    "force_fetch": false
}
EOF
    
    # 함수 호출
    aws lambda invoke \
        --function-name $FUNCTION_NAME \
        --payload file://test_payload.json \
        response.json
    
    echo -e "${BLUE}📋 응답:${NC}"
    cat response.json | jq '.'
    
    # 정리
    rm -f test_payload.json response.json
fi

# 10. 정리
echo -e "${YELLOW}🧹 임시 파일 정리...${NC}"
rm -f ${FUNCTION_NAME}.zip

echo -e "${GREEN}🎉 Lambda Data Collector 배포 완료!${NC}"
echo ""
echo -e "${BLUE}다음 단계:${NC}"
echo "1. Step Functions와 연결"
echo "2. EventBridge 트리거 설정"
echo "3. CloudWatch 모니터링 확인"
echo ""
echo -e "${YELLOW}💡 팁:${NC} CloudWatch Logs에서 실행 로그를 확인할 수 있습니다."