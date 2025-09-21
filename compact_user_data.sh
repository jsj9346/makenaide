#!/bin/bash
#
# EC2 User Data 스크립트 (간소화) - Makenaide 자동 시작
# 핵심: 환경변수 설정 + 자동 시작

# 로그 설정
LOG_FILE="/home/ec2-user/makenaide/logs/auto_execution.log"
mkdir -p /home/ec2-user/makenaide/logs

log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1" | tee -a $LOG_FILE
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a $LOG_FILE
}

# 시스템 초기화
log_info "🚀 Makenaide EC2 자동 시작 (간소화 버전)"

# 1. 시스템 전역 환경변수 설정 (/etc/environment)
log_info "📝 시스템 전역 환경변수 설정"

# 기존 Makenaide 관련 환경변수 제거 (있다면)
grep -v "^EC2_AUTO_SHUTDOWN\|^MAKENAIDE_" /etc/environment > /tmp/environment_clean 2>/dev/null || touch /tmp/environment_clean

# 새로운 환경변수 추가
cat >> /tmp/environment_clean << 'ENV_GLOBAL'
EC2_AUTO_SHUTDOWN=true
MAKENAIDE_LOG_LEVEL=INFO
MAKENAIDE_EXECUTION_MODE=production
ENV_GLOBAL

# /etc/environment 백업 및 업데이트
cp /etc/environment /etc/environment.backup.$(date +%Y%m%d_%H%M%S) 2>/dev/null || true
mv /tmp/environment_clean /etc/environment
chmod 644 /etc/environment

log_info "✅ 시스템 전역 환경변수 설정 완료"

# 2. ec2-user 환경변수 설정 (.bashrc)
log_info "📝 ec2-user 환경변수 설정"

# .bashrc 백업
sudo -u ec2-user cp /home/ec2-user/.bashrc /home/ec2-user/.bashrc.backup.$(date +%Y%m%d_%H%M%S) 2>/dev/null || true

# 기존 Makenaide 관련 환경변수 제거 (있다면)
sudo -u ec2-user bash -c 'grep -v "^export EC2_AUTO_SHUTDOWN\|^export MAKENAIDE_\|^# Makenaide 환경변수" ~/.bashrc > /tmp/bashrc_clean 2>/dev/null || cp ~/.bashrc /tmp/bashrc_clean'

# 새로운 환경변수 추가
cat >> /tmp/bashrc_clean << 'ENV_USER'

# Makenaide 환경변수 (자동 추가됨)
export EC2_AUTO_SHUTDOWN=true
export MAKENAIDE_LOG_LEVEL=INFO
export MAKENAIDE_EXECUTION_MODE=production
export PYTHONPATH=/home/ec2-user/makenaide:$PYTHONPATH
ENV_USER

# .bashrc 업데이트
sudo -u ec2-user mv /tmp/bashrc_clean /home/ec2-user/.bashrc
sudo -u ec2-user chmod 644 /home/ec2-user/.bashrc

log_info "✅ ec2-user 환경변수 설정 완료"

# 3. 현재 세션 환경변수 로드
export EC2_AUTO_SHUTDOWN=true
export MAKENAIDE_LOG_LEVEL=INFO
export MAKENAIDE_EXECUTION_MODE=production
export PYTHONPATH=/home/ec2-user/makenaide:$PYTHONPATH

log_info "✅ 현재 세션 환경변수 로드 완료"

# 4. 환경변수 설정 검증
log_info "🔍 환경변수 설정 검증"

{
    echo "=== 현재 세션 환경변수 ==="
    echo "EC2_AUTO_SHUTDOWN: ${EC2_AUTO_SHUTDOWN:-'NOT_SET'}"
    echo "MAKENAIDE_LOG_LEVEL: ${MAKENAIDE_LOG_LEVEL:-'NOT_SET'}"
    echo "MAKENAIDE_EXECUTION_MODE: ${MAKENAIDE_EXECUTION_MODE:-'NOT_SET'}"
    echo "PYTHONPATH: ${PYTHONPATH:-'NOT_SET'}"

    echo "=== /etc/environment 확인 ==="
    grep "EC2_AUTO_SHUTDOWN\|MAKENAIDE_" /etc/environment || echo "환경변수 없음"

    echo "=== ec2-user .bashrc 확인 ==="
    sudo -u ec2-user grep "EC2_AUTO_SHUTDOWN\|MAKENAIDE_" /home/ec2-user/.bashrc || echo "환경변수 없음"

} >> $LOG_FILE 2>&1

# 5. Makenaide 디렉토리 확인 및 권한 설정
if [ ! -d "/home/ec2-user/makenaide" ]; then
    log_error "Makenaide 디렉토리가 존재하지 않습니다: /home/ec2-user/makenaide"
    exit 1
fi

cd /home/ec2-user/makenaide
chown -R ec2-user:ec2-user /home/ec2-user/makenaide
chmod +x /home/ec2-user/makenaide/makenaide.py 2>/dev/null || true

log_info "✅ 디렉토리 및 권한 설정 완료"

# 6. 안전장치 설정 (2시간 타임아웃)
log_info "⏰ 안전장치 설정 (2시간 타임아웃)"

(
    sleep 7200  # 2시간 = 7200초
    if [ -f "/tmp/makenaide_auto_execution.lock" ]; then
        log_error "타임아웃으로 인한 강제 종료 (2시간)"
        sudo shutdown -h now
    fi
) &

TIMEOUT_PID=$!
log_info "타임아웃 프로세스 시작됨 (PID: $TIMEOUT_PID)"

# 7. 중복 실행 방지
LOCK_FILE="/tmp/makenaide_auto_execution.lock"
if [ -f "$LOCK_FILE" ]; then
    log_error "이미 Makenaide가 실행 중입니다. 중복 실행을 방지합니다."
    exit 0
fi

# Lock 파일 생성
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"; log_info "Lock 파일 정리 완료"' EXIT

# 8. Makenaide 파이프라인 실행
log_info "🎯 Makenaide 파이프라인 실행 시작"

# ec2-user로 실행 (환경변수를 .bashrc에서 로드)
sudo -u ec2-user bash << 'MAKENAIDE_EXECUTION'
set -e

cd /home/ec2-user/makenaide

# .bashrc 환경변수 로드
source /home/ec2-user/.bashrc

# 로그 파일 설정
LOG_FILE="/home/ec2-user/makenaide/logs/auto_execution.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🎯 Makenaide 실행 시작 (ec2-user)" >> $LOG_FILE

# 실행 전 상태 추가
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 📊 실행 환경:" >> $LOG_FILE
echo "   - PWD: $(pwd)" >> $LOG_FILE
echo "   - USER: $(whoami)" >> $LOG_FILE
echo "   - EC2_AUTO_SHUTDOWN: ${EC2_AUTO_SHUTDOWN:-'NOT_SET'}" >> $LOG_FILE
echo "   - MAKENAIDE_LOG_LEVEL: ${MAKENAIDE_LOG_LEVEL:-'NOT_SET'}" >> $LOG_FILE
echo "   - MAKENAIDE_EXECUTION_MODE: ${MAKENAIDE_EXECUTION_MODE:-'NOT_SET'}" >> $LOG_FILE
echo "   - PYTHONPATH: ${PYTHONPATH:-'NOT_SET'}" >> $LOG_FILE

# 가상환경 활성화 (있으면)
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Python 가상환경 활성화" >> $LOG_FILE
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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🔌 EC2 자동 종료 준비됨, makenaide.py 종료로 인해 수행됩니다" >> $LOG_FILE
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
    echo "🏁 Makenaide EC2 자동 실행 완료 (간소화 버전)"
    echo "=================================================="
    echo "완료 시간: $(date)"
    echo "실행 결과: $EXECUTION_RESULT"
    echo "=================================================="
} >> $LOG_FILE 2>&1

# 정리 작업
rm -f /tmp/environment_clean /tmp/bashrc_clean

if [ $EXECUTION_RESULT -eq 0 ]; then
    log_info "✅ User Data 스크립트 성공적으로 완료 (간소화 버전)"
else
    log_error "❌ User Data 스크립트 실행 실패 (코드: $EXECUTION_RESULT)"
fi

exit $EXECUTION_RESULT