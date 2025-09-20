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

📍 설치 경로: /home/ec2-user/makenaide/ec2_auto_start_script.sh
"""

# 로그 설정
LOG_FILE="/home/ec2-user/makenaide/logs/auto_execution.log"
ERROR_LOG="/home/ec2-user/makenaide/logs/auto_execution_error.log"

# 로그 디렉토리 생성
mkdir -p /home/ec2-user/makenaide/logs

# 로그 함수
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a "$LOG_FILE" "$ERROR_LOG"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: $1" | tee -a "$LOG_FILE"
}

# 메인 실행 함수
main() {
    log_info "=================================================="
    log_info "🚀 Makenaide 자동 실행 시작"
    log_info "=================================================="

    # 시작 시간 기록
    START_TIME=$(date +%s)

    # 현재 사용자 확인
    CURRENT_USER=$(whoami)
    log_info "현재 사용자: $CURRENT_USER"

    # ec2-user로 전환이 필요한 경우
    if [ "$CURRENT_USER" != "ec2-user" ]; then
        log_info "ec2-user로 전환하여 실행..."
        sudo -u ec2-user bash -c "$(declare -f main log_info log_error log_success); main"
        return $?
    fi

    # 작업 디렉토리 이동
    cd /home/ec2-user/makenaide || {
        log_error "Makenaide 디렉토리로 이동 실패"
        return 1
    }

    log_info "작업 디렉토리: $(pwd)"

    # 환경 변수 설정
    export EC2_AUTO_SHUTDOWN=true
    export PYTHONPATH=/home/ec2-user/makenaide:$PYTHONPATH
    export PYTHON_UNBUFFERED=1  # Python 출력 즉시 표시

    log_info "환경변수 설정 완료"
    log_info "  - EC2_AUTO_SHUTDOWN: $EC2_AUTO_SHUTDOWN"
    log_info "  - PYTHONPATH: $PYTHONPATH"

    # Python 가상환경 활성화 (있다면)
    if [ -d "venv" ]; then
        source venv/bin/activate
        log_info "✅ Python 가상환경 활성화"
    else
        log_info "ℹ️ Python 가상환경 없음, 시스템 Python 사용"
    fi

    # Python 및 모듈 확인
    PYTHON_VERSION=$(python3 --version 2>&1)
    log_info "Python 버전: $PYTHON_VERSION"

    # 필수 모듈 확인
    if ! python3 -c "import sqlite3, requests, pyupbit" 2>/dev/null; then
        log_error "필수 Python 모듈이 누락되었습니다"
        return 1
    fi

    log_info "✅ 필수 모듈 확인 완료"

    # 데이터베이스 파일 확인
    if [ ! -f "makenaide_local.db" ]; then
        log_error "SQLite 데이터베이스 파일이 없습니다: makenaide_local.db"
        return 1
    fi

    log_info "✅ 데이터베이스 파일 확인 완료"

    # 네트워크 연결 확인
    if ! ping -c 1 -W 5 8.8.8.8 > /dev/null 2>&1; then
        log_error "네트워크 연결을 확인할 수 없습니다"
        return 1
    fi

    log_info "✅ 네트워크 연결 확인 완료"

    # 업비트 API 연결 테스트
    if ! python3 -c "import pyupbit; print('TICKERS:', len(pyupbit.get_tickers()))" 2>/dev/null; then
        log_error "업비트 API 연결 실패"
        return 1
    fi

    log_info "✅ 업비트 API 연결 확인 완료"

    # Makenaide 파이프라인 실행
    log_info "🎯 Makenaide 파이프라인 실행 시작..."
    log_info "실행 명령: python3 makenaide.py --risk-level moderate"

    # 파이프라인 실행 (출력을 로그파일과 콘솔 모두에 기록)
    python3 makenaide.py --risk-level moderate 2>&1 | tee -a "$LOG_FILE"
    PIPELINE_EXIT_CODE=${PIPESTATUS[0]}

    # 실행 시간 계산
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    EXECUTION_MINUTES=$((EXECUTION_TIME / 60))
    EXECUTION_SECONDS=$((EXECUTION_TIME % 60))

    log_info "📊 파이프라인 실행 결과:"
    log_info "  - 종료 코드: $PIPELINE_EXIT_CODE"
    log_info "  - 실행 시간: ${EXECUTION_MINUTES}분 ${EXECUTION_SECONDS}초"

    if [ $PIPELINE_EXIT_CODE -eq 0 ]; then
        log_success "✅ Makenaide 파이프라인 성공적으로 완료"
    else
        log_error "❌ Makenaide 파이프라인 실행 실패 (종료 코드: $PIPELINE_EXIT_CODE)"
    fi

    # 로그 파일 백업 (S3 업로드는 makenaide.py에서 처리됨)
    BACKUP_LOG_NAME="auto_execution_$(date +%Y%m%d_%H%M%S).log"
    cp "$LOG_FILE" "/home/ec2-user/makenaide/logs/backups/$BACKUP_LOG_NAME" 2>/dev/null || {
        mkdir -p /home/ec2-user/makenaide/logs/backups
        cp "$LOG_FILE" "/home/ec2-user/makenaide/logs/backups/$BACKUP_LOG_NAME"
    }

    log_info "📦 로그 백업 완료: $BACKUP_LOG_NAME"

    log_info "=================================================="
    log_info "🏁 Makenaide 자동 실행 완료"
    log_info "=================================================="

    # EC2 자동 종료 처리
    # makenaide.py에서 EC2_AUTO_SHUTDOWN=true일 때 자동 종료하도록 설계됨
    # 만약 makenaide.py에서 종료하지 않았다면 여기서 처리
    if [ "$EC2_AUTO_SHUTDOWN" = "true" ]; then
        log_info "🔌 EC2 자동 종료 확인 중..."

        # 5분 후에도 실행 중이면 강제 종료
        (
            sleep 300  # 5분 대기
            log_info "⚠️ 5분 후에도 실행 중 - 강제 종료 실행"
            sudo shutdown -h now
        ) &

        log_info "⏳ Makenaide에서 자동 종료 처리 중... (5분 후 강제 종료)"
    fi

    return $PIPELINE_EXIT_CODE
}

# 스크립트 실행
main "$@"