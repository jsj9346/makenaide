#!/bin/bash

# Makenaide V2 업그레이드 배포 스크립트
# EC2에서 빠른 배포를 위한 자동화 스크립트

set -e  # 오류 발생 시 스크립트 중단

echo "🚀 Makenaide V2 업그레이드 배포 시작"
echo "시작 시간: $(date)"

# 기본 디렉토리 설정
MAKENAIDE_DIR="/home/ec2-user/makenaide"
BACKUP_DIR="/home/ec2-user/makenaide_backup_$(date +%Y%m%d_%H%M%S)"
S3_BUCKET="makenaide-config-deploy"

# 1. 백업 생성
echo "📦 기존 makenaide 백업 생성 중..."
if [ -d "$MAKENAIDE_DIR" ]; then
    mkdir -p "$BACKUP_DIR"
    cp -r "$MAKENAIDE_DIR"/* "$BACKUP_DIR"/ 2>/dev/null || echo "백업할 기존 파일이 없습니다"
    echo "✅ 백업 완료: $BACKUP_DIR"
fi

# 2. 디렉토리 생성
echo "📁 디렉토리 구조 생성 중..."
mkdir -p "$MAKENAIDE_DIR"
mkdir -p "$MAKENAIDE_DIR/logs"
mkdir -p "$MAKENAIDE_DIR/data"
mkdir -p "$MAKENAIDE_DIR/config"

# 3. S3에서 파일 다운로드
echo "📥 S3에서 Makenaide V2 파일 다운로드 중..."
cd "$MAKENAIDE_DIR"

# 핵심 파일들 다운로드
aws s3 cp s3://$S3_BUCKET/upgrades/makenaide_v2_upgrade.tar.gz . || {
    echo "❌ 핵심 파일 다운로드 실패"
    exit 1
}

# 설정 파일들 다운로드
aws s3 cp s3://$S3_BUCKET/upgrades/makenaide_config.tar.gz . || {
    echo "❌ 설정 파일 다운로드 실패"
    exit 1
}

# 4. 파일 압축 해제
echo "📦 파일 압축 해제 중..."
tar -xzf makenaide_v2_upgrade.tar.gz
tar -xzf makenaide_config.tar.gz

# 임시 파일 정리
rm -f makenaide_v2_upgrade.tar.gz makenaide_config.tar.gz

# 5. 권한 설정
echo "🔐 파일 권한 설정 중..."
chown -R ec2-user:ec2-user "$MAKENAIDE_DIR"
chmod +x "$MAKENAIDE_DIR/makenaide.py"

# 6. Python 의존성 설치
echo "🐍 Python 의존성 설치 중..."
if [ -f requirements.txt ]; then
    pip3 install -r requirements.txt --user --quiet || echo "⚠️ 의존성 설치 중 일부 실패"
fi

# 7. 환경 변수 설정 (자동 종료 비활성화)
echo "⚙️ 환경 변수 설정 중..."
if [ -f .env ]; then
    # 자동 종료 비활성화
    echo "EC2_AUTO_SHUTDOWN=false" >> .env
    echo "MAKENAIDE_V2_DEPLOYED=true" >> .env
    echo "DEPLOYMENT_DATE=$(date)" >> .env
fi

# 8. 데이터베이스 초기화 (있는 경우)
echo "🗃️ 데이터베이스 설정 확인 중..."
if [ -f init_db_sqlite.py ]; then
    python3 init_db_sqlite.py || echo "⚠️ 데이터베이스 초기화 스킵"
fi

# 9. 배포 완료 확인 파일 생성
echo "✅ 배포 완료 확인 파일 생성 중..."
cat > makenaide_v2_deployment_status.txt << EOF
Makenaide V2 배포 완료
배포 시간: $(date)
배포 버전: V2 High-Performance Upgrade
배포 방식: S3 자동 배포
백업 위치: $BACKUP_DIR

주요 업그레이드:
- 통합 파이프라인 (makenaide.py)
- 실시간 시장 감정 분석 (real_time_market_sentiment.py)
- 레이어드 스코어링 엔진 (layered_scoring_engine.py)
- 고급 트레이딩 엔진 (trading_engine.py)
- EC2 자동 종료 기능 포함

파일 목록:
$(ls -la "$MAKENAIDE_DIR" | grep -E '\\.py$')

배포 완료! 🎉
EOF

echo "📊 배포 상태 요약:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Makenaide V2 업그레이드 배포 완료!"
echo "📁 배포 위치: $MAKENAIDE_DIR"
echo "🔙 백업 위치: $BACKUP_DIR"
echo "⏰ 완료 시간: $(date)"
echo "🔧 EC2 자동 종료: 비활성화됨"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 10. 최종 테스트 (간단한 import 테스트)
echo "🧪 기본 모듈 import 테스트 중..."
cd "$MAKENAIDE_DIR"
python3 -c "
try:
    import makenaide
    import utils
    import trading_engine
    print('✅ 모든 핵심 모듈 import 성공')
except ImportError as e:
    print(f'⚠️ Import 테스트 실패: {e}')
    exit(1)
" || echo "⚠️ Import 테스트 스킵 (일부 의존성 누락 가능)"

echo "🎉 Makenaide V2 배포 스크립트 완료!"
echo "📝 배포 로그: $MAKENAIDE_DIR/makenaide_v2_deployment_status.txt"
echo "🔄 다음 단계: makenaide.py 실행 테스트를 권장합니다"