#!/bin/bash

# 로컬 프로젝트 경로
PROJECT_DIR="/Users/13ruce/makenaide"
PROJECT_DIR_ALT="/Users/13ruce/Makenaide"

# 로그 파일 경로
RUN_LOG="$PROJECT_DIR/makenaide_run.log"
CRON_LOG="$PROJECT_DIR/makenaide_cron.log"

# Python 실행 경로
PYTHON_EXEC="python3"

while true
do
    echo "[$(date)] makenaide 실행 시작" > "$CRON_LOG"

    cd "$PROJECT_DIR" || exit 1

    # __pycache__ 디렉토리 삭제 (makenaide, Makenaide 모두)
    if [ -d "$PROJECT_DIR/__pycache__" ]; then
        rm -rf "$PROJECT_DIR/__pycache__"
        echo "__pycache__ 디렉토리 삭제됨: $PROJECT_DIR/__pycache__" >> "$CRON_LOG"
    fi
    if [ -d "$PROJECT_DIR_ALT/__pycache__" ]; then
        rm -rf "$PROJECT_DIR_ALT/__pycache__"
        echo "__pycache__ 디렉토리 삭제됨: $PROJECT_DIR_ALT/__pycache__" >> "$CRON_LOG"
    fi

    # 기존 로그 파일 덮어쓰기
    > "$RUN_LOG"

    # makenaide 실행 (백그라운드로)
    $PYTHON_EXEC makenaide.py > "$RUN_LOG" 2>&1 &

    # makenaide 프로세스 PID
    MAKENAIDE_PID=$!

    # 로그 실시간 출력 시작
    tail -f "$RUN_LOG" &
    TAIL_PID=$!

    # makenaide 실행 완료될 때까지 대기
    wait $MAKENAIDE_PID

    # tail 중단
    kill $TAIL_PID

    echo "[$(date)] makenaide 실행 완료, 4시간 대기" >> "$CRON_LOG"

    sleep 14400
done
