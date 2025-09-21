#!/bin/bash
#
# EC2 User Data 스크립트 - Makenaide 자동 실행
# EC2 인스턴스 시작 시 자동으로 Makenaide 파이프라인 실행
#
# 🎯 기능:
# 1. EC2 부팅 완료 후 자동 실행 (User Data)
# 2. ec2-user 권한으로 makenaide.py 실행
# 3. 환경 변수 설정 (EC2_AUTO_SHUTDOWN=true)
# 4. 안전장치: 최대 2시간 타임아웃
# 5. 로그 기록 및 모니터링
# 6. 실행 완료 후 EC2 자동 종료
#
# 📋 사용법:
# - AWS 콘솔에서 EC2 User Data에 이 스크립트 전체 복사/붙여넣기
# - 또는 Lambda에서 modify-instance-attribute로 설정
#
# ⚠️ 주의:
# - 이 스크립트는 EC2 시작 시마다 실행됩니다
# - makenaide.py 내부에서 최종 EC2 종료 처리
#

# 로그 설정
LOG_FILE="/home/ec2-user/makenaide/logs/auto_execution.log"
ERROR_LOG="/home/ec2-user/makenaide/logs/auto_execution_error.log"

# 로그 디렉토리 생성
mkdir -p /home/ec2-user/makenaide/logs

# 로그 함수 정의
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1" | tee -a $LOG_FILE
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a $LOG_FILE >> $ERROR_LOG
}

log_warning() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1" | tee -a $LOG_FILE
}

# 시작 로그
{
    echo "=================================================="
    echo "🚀 Makenaide EC2 자동 실행 시작"
    echo "=================================================="
    echo "시작 시간: $(date)"
    echo "인스턴스 ID: $(curl -s http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || echo 'unknown')"
    echo "인스턴스 타입: $(curl -s http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo 'unknown')"
    echo "가용 영역: $(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone 2>/dev/null || echo 'unknown')"
    echo "=================================================="
} >> $LOG_FILE 2>&1

# 에러 핸들링
set -e
trap 'log_error "스크립트 실행 중 오류 발생 (Line: $LINENO)"; emergency_shutdown' ERR

# 긴급 종료 함수
emergency_shutdown() {
    log_error "긴급 상황으로 인한 EC2 종료 (1분 후)"
    sudo shutdown -h +1
}

# 1. 시스템 상태 확인
log_info "📋 시스템 상태 확인"

# 디스크 공간 확인
DISK_USAGE=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 85 ]; then
    log_warning "디스크 사용량이 높습니다: ${DISK_USAGE}%"
fi

# 메모리 확인
FREE_MEM=$(free -m | awk 'NR==2{printf "%.1f", $7*100/$2}')
log_info "사용 가능한 메모리: ${FREE_MEM}%"

# 2. Makenaide 디렉토리 확인
log_info "📂 Makenaide 디렉토리 확인"

if [ ! -d "/home/ec2-user/makenaide" ]; then
    log_error "Makenaide 디렉토리가 존재하지 않습니다: /home/ec2-user/makenaide"
    emergency_shutdown
    exit 1
fi

cd /home/ec2-user/makenaide
log_info "작업 디렉토리: $(pwd)"

# 3. 파일 소유권 확인 및 수정
log_info "🔒 파일 권한 설정"
chown -R ec2-user:ec2-user /home/ec2-user/makenaide
chmod +x /home/ec2-user/makenaide/makenaide.py 2>/dev/null || true

# 4. 중복 실행 방지
LOCK_FILE="/tmp/makenaide_auto_execution.lock"
if [ -f "$LOCK_FILE" ]; then
    log_warning "이미 Makenaide가 실행 중입니다. 중복 실행을 방지합니다."
    exit 0
fi

# Lock 파일 생성
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"; log_info "Lock 파일 정리 완료"' EXIT

# 5. Python 환경 확인
log_info "🐍 Python 환경 확인"

# ec2-user로 전환하여 Python 환경 체크
sudo -u ec2-user bash << 'PYTHON_CHECK'
cd /home/ec2-user/makenaide

# Python 버전 확인
PYTHON_VERSION=$(python3 --version 2>&1)
echo "Python 버전: $PYTHON_VERSION" >> /home/ec2-user/makenaide/logs/auto_execution.log

# 가상환경 확인 및 활성화
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "✅ Python 가상환경 활성화" >> /home/ec2-user/makenaide/logs/auto_execution.log
else
    echo "⚠️ Python 가상환경이 없습니다. 시스템 Python 사용" >> /home/ec2-user/makenaide/logs/auto_execution.log
fi

# 필수 패키지 확인
REQUIRED_PACKAGES=("pyupbit" "requests" "pandas" "numpy")
for package in "${REQUIRED_PACKAGES[@]}"; do
    if python3 -c "import $package" 2>/dev/null; then
        echo "✅ $package 패키지 확인됨" >> /home/ec2-user/makenaide/logs/auto_execution.log
    else
        echo "❌ $package 패키지 누락" >> /home/ec2-user/makenaide/logs/auto_execution.log
    fi
done
PYTHON_CHECK

# 6. 환경 변수 설정
log_info "🔧 환경 변수 설정"

# 환경 변수 파일 생성
cat > /tmp/makenaide_env << 'ENV_EOF'
export EC2_AUTO_SHUTDOWN=true
export PYTHONPATH=/home/ec2-user/makenaide:$PYTHONPATH
export MAKENAIDE_LOG_LEVEL=INFO
export MAKENAIDE_EXECUTION_MODE=production
ENV_EOF

log_info "환경 변수 설정 완료"

# 7. 안전장치 설정 (2시간 타임아웃)
log_info "⏰ 안전장치 설정 (2시간 타임아웃)"

# 2시간 후 강제 종료 스케줄 (백그라운드)
(
    sleep 7200  # 2시간 = 7200초
    if [ -f "$LOCK_FILE" ]; then
        log_error "타임아웃으로 인한 강제 종료 (2시간)"
        sudo shutdown -h now
    fi
) &

TIMEOUT_PID=$!
log_info "타임아웃 프로세스 시작됨 (PID: $TIMEOUT_PID)"

# 8. Makenaide 파이프라인 실행
log_info "🎯 Makenaide 파이프라인 실행 시작"

# ec2-user로 실행
sudo -u ec2-user bash << 'MAKENAIDE_EXECUTION'
set -e

cd /home/ec2-user/makenaide

# 환경 변수 로드
source /tmp/makenaide_env

# 로그 파일 설정
LOG_FILE="/home/ec2-user/makenaide/logs/auto_execution.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🎯 Makenaide 실행 시작 (ec2-user)" >> $LOG_FILE

# 가상환경 활성화 (있다면)
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Python 가상환경 활성화" >> $LOG_FILE
fi

# 실행 전 상태 출력
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 📊 실행 환경:" >> $LOG_FILE
echo "   - PWD: $(pwd)" >> $LOG_FILE
echo "   - USER: $(whoami)" >> $LOG_FILE
echo "   - EC2_AUTO_SHUTDOWN: $EC2_AUTO_SHUTDOWN" >> $LOG_FILE
echo "   - PYTHONPATH: $PYTHONPATH" >> $LOG_FILE

# SQLite DB 존재 확인
if [ -f "data/makenaide_local.db" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ SQLite DB 존재 확인" >> $LOG_FILE
    # DB 파일 크기도 확인
    DB_SIZE=$(du -h data/makenaide_local.db | cut -f1)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 📊 DB 파일 크기: $DB_SIZE" >> $LOG_FILE
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️ SQLite DB가 없습니다. 새로 생성될 예정" >> $LOG_FILE
    mkdir -p data
fi

# API 키 확인
if [ -f ".env" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ .env 파일 존재 확인" >> $LOG_FILE
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ .env 파일이 없습니다!" >> $LOG_FILE
    exit 1
fi

# Makenaide 파이프라인 실행
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🚀 파이프라인 실행..." >> $LOG_FILE

# 타임아웃과 함께 실행 (1.5시간)
timeout 5400 python3 makenaide.py --risk-level moderate >> $LOG_FILE 2>&1

# 실행 결과 확인
PIPELINE_EXIT_CODE=$?
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 📊 파이프라인 종료 코드: $PIPELINE_EXIT_CODE" >> $LOG_FILE

if [ $PIPELINE_EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Makenaide 파이프라인 성공적으로 완료" >> $LOG_FILE
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🔌 EC2 자동 종료 처리는 makenaide.py 내부에서 수행됩니다" >> $LOG_FILE
elif [ $PIPELINE_EXIT_CODE -eq 124 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ⏰ 파이프라인 타임아웃 (1.5시간)" >> $LOG_FILE
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🔌 타임아웃으로 인한 강제 종료" >> $LOG_FILE
    sudo shutdown -h +1  # 1분 후 종료
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ Makenaide 파이프라인 실행 실패" >> $LOG_FILE
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🔌 실패로 인한 강제 종료" >> $LOG_FILE
    sudo shutdown -h +1  # 1분 후 종료
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🏁 자동 실행 스크립트 완료" >> $LOG_FILE

MAKENAIDE_EXECUTION

# 실행 결과 확인
EXECUTION_RESULT=$?

# 9. 타임아웃 프로세스 정리
if kill -0 $TIMEOUT_PID 2>/dev/null; then
    kill $TIMEOUT_PID 2>/dev/null || true
    log_info "타임아웃 프로세스 정리 완료"
fi

# 10. 최종 상태 로그
{
    echo "=================================================="
    echo "🏁 Makenaide EC2 자동 실행 완료"
    echo "=================================================="
    echo "완료 시간: $(date)"
    echo "실행 결과: $EXECUTION_RESULT"
    echo "스크립트 실행 시간: $SECONDS 초"
    echo "=================================================="
} >> $LOG_FILE 2>&1

# 11. 정리 작업
log_info "🧹 임시 파일 정리"
rm -f /tmp/makenaide_env

if [ $EXECUTION_RESULT -eq 0 ]; then
    log_info "✅ User Data 스크립트 성공적으로 완료"
else
    log_error "❌ User Data 스크립트 실행 실패 (코드: $EXECUTION_RESULT)"
fi

# Lock 파일은 trap에서 자동 정리됨

# 주의: EC2 종료는 makenaide.py 내부의 safe_shutdown_ec2() 메서드에서 처리됨
# 이 스크립트는 실행 환경 설정 및 파이프라인 시작까지만 담당

exit $EXECUTION_RESULT