#!/bin/bash

# =============================================================================
# EC2 환경변수 설정 스크립트
# =============================================================================

echo "🚀 EC2 Makenaide 환경변수 설정 시작"

# 작업 디렉토리로 이동
cd /home/ec2-user/makenaide

# .env 파일이 이미 있다면 백업
if [ -f ".env" ]; then
    echo "📁 기존 .env 파일 백업"
    cp .env .env.backup.$(date +%Y%m%d_%H%M%S)
fi

# .env 파일 생성
echo "📝 .env 파일 생성 중..."
cat > .env << 'EOF'
# =============================================================================
# MAKENAIDE 실제 환경변수 설정 (EC2 전용)
# =============================================================================

# RDS PostgreSQL 설정
PG_HOST=makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com
PG_PORT=5432
PG_DATABASE=makenaide
PG_USER=bruce
PG_PASSWORD=0asis314.

# DB 연결 호환성
DB_HOST=makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com
DB_PORT=5432
DB_NAME=makenaide
DB_USER=bruce
DB_PASSWORD=0asis314.

# OpenAI API 설정 - ⚠️ 실제 API 키로 변경 필요
OPENAI_API_KEY=REPLACE_WITH_ACTUAL_OPENAI_API_KEY

# Upbit API 설정 - ⚠️ 실제 API 키로 변경 필요
UPBIT_ACCESS_KEY=REPLACE_WITH_ACTUAL_UPBIT_ACCESS_KEY
UPBIT_SECRET_KEY=REPLACE_WITH_ACTUAL_UPBIT_SECRET_KEY

# AWS 설정
AWS_REGION=ap-northeast-2
AWS_DEFAULT_REGION=ap-northeast-2

# 운영 환경 설정
ENVIRONMENT=production
DEPLOYMENT_MODE=cloud
MAKENAIDE_ENV=production

# 로그 설정
LOG_LEVEL=INFO
LOG_FILE_PATH=log/makenaide.log
LOGGING_LEVEL=INFO

# 성능 최적화 설정
MAX_MEMORY_USAGE_MB=2048
BATCH_SIZE=50
API_SLEEP_TIME=0.1
PARALLEL_WORKERS=4

# 자동화 설정
AUTO_SHUTDOWN_ENABLED=true
CLOUD_AUTOMATION=true
CLEANUP_ON_STARTUP=true

# 데이터 보존 설정
LOG_RETENTION_DAYS=7
DATA_RETENTION_DAYS=451

# 기술적 분석 설정
TECHNICAL_ANALYSIS_ENABLED=true
INDICATOR_CACHE_TTL=300

# GPT 분석 설정
GPT_ANALYSIS_ENABLED=true
GPT_MODEL=gpt-4o
GPT_MAX_TOKENS=1000

# 알림 설정 (필요시 활성화)
NOTIFICATION_ENABLED=false
EMAIL_ENABLED=false

# 보안 설정
SENSITIVE_INFO_MASKING=true
ENCRYPTION_ENABLED=true

# 캐시 설정
CACHE_ENABLED=true
CACHE_TTL=3600

# 모니터링 설정
PERFORMANCE_MONITORING=true
METRICS_ENABLED=true
HEALTH_CHECK_INTERVAL=60

EOF

# 파일 권한 설정 (보안상 600으로 설정)
chmod 600 .env

echo "✅ .env 파일 생성 완료"
echo ""
echo "⚠️  중요: 다음 API 키들을 실제 값으로 변경해야 합니다:"
echo "   - OPENAI_API_KEY"
echo "   - UPBIT_ACCESS_KEY"
echo "   - UPBIT_SECRET_KEY"
echo ""
echo "💡 편집 방법:"
echo "   nano .env"
echo "   또는"
echo "   vi .env"
echo ""
echo "🔒 보안: .env 파일 권한은 600으로 설정되었습니다."
echo ""

# Python 환경에서 .env 파일 로드 테스트
echo "🧪 환경변수 로드 테스트 중..."
python3 -c "
from dotenv import load_dotenv
import os

load_dotenv()

# 필수 환경변수 확인
required_vars = ['PG_HOST', 'PG_PASSWORD', 'OPENAI_API_KEY', 'UPBIT_ACCESS_KEY']
missing_vars = []
placeholder_vars = []

for var in required_vars:
    value = os.getenv(var)
    if not value:
        missing_vars.append(var)
    elif any(placeholder in value for placeholder in ['REPLACE_WITH', 'your_', 'ACTUAL']):
        placeholder_vars.append(var)

if missing_vars:
    print(f'❌ 누락된 환경변수: {missing_vars}')
elif placeholder_vars:
    print(f'⚠️  실제 값으로 변경 필요: {placeholder_vars}')
else:
    print('✅ 모든 필수 환경변수 설정 완료')
"

echo ""
echo "🚀 설정 완료! makenaide.py 실행 준비됨" 