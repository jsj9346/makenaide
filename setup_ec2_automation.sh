#!/bin/bash
#
# Makenaide EC2 자동화 설정 스크립트
# 빠른 설정을 위한 원클릭 스크립트
#
# 🎯 기능:
# 1. Lambda 함수 배포
# 2. EventBridge 스케줄 설정
# 3. EC2 User Data 설정
# 4. 권한 설정
# 5. 테스트 실행
#

set -e  # 오류 발생 시 중단

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 설정 검증
check_prerequisites() {
    log_info "📋 사전 조건 확인 중..."

    # AWS CLI 설치 확인
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI가 설치되지 않았습니다."
        exit 1
    fi

    # Python 설치 확인
    if ! command -v python3 &> /dev/null; then
        log_error "Python3가 설치되지 않았습니다."
        exit 1
    fi

    # AWS 인증 확인
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS 인증이 설정되지 않았습니다."
        log_info "aws configure를 실행하여 인증을 설정하세요."
        exit 1
    fi

    # 필요한 파일들 확인
    local required_files=(
        "lambda_ec2_starter.py"
        "user_data_script.sh"
        "makenaide.py"
    )

    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            log_error "필수 파일이 없습니다: $file"
            exit 1
        fi
    done

    log_success "모든 사전 조건이 충족되었습니다."
}

# Lambda 함수 배포
deploy_lambda() {
    log_info "🚀 Lambda 함수 배포 중..."

    # Lambda 배포 패키지 생성
    cp lambda_ec2_starter.py lambda_function.py
    zip -q makenaide-ec2-starter.zip lambda_function.py
    rm lambda_function.py

    # Lambda 함수 생성/업데이트
    aws lambda create-function \
        --function-name makenaide-ec2-starter \
        --runtime python3.11 \
        --role arn:aws:iam::901361833359:role/lambda-execution-role \
        --handler lambda_function.lambda_handler \
        --zip-file fileb://makenaide-ec2-starter.zip \
        --description "Makenaide EC2 auto starter" \
        --timeout 60 \
        --memory-size 128 \
        > /dev/null 2>&1 || {

        # 함수가 이미 존재하면 업데이트
        log_info "기존 Lambda 함수 업데이트 중..."
        aws lambda update-function-code \
            --function-name makenaide-ec2-starter \
            --zip-file fileb://makenaide-ec2-starter.zip \
            > /dev/null
    }

    # 정리
    rm makenaide-ec2-starter.zip

    log_success "Lambda 함수 배포 완료"
}

# EventBridge 스케줄 설정
setup_eventbridge() {
    log_info "⏰ EventBridge 스케줄 설정 중..."

    # 스케줄 정의 (KST 기준)
    declare -A schedules=(
        ["makenaide-schedule-02-00"]="cron(0 17 * * ? *)"  # UTC 17:00 = KST 02:00
        ["makenaide-schedule-09-00"]="cron(0 0 * * ? *)"   # UTC 00:00 = KST 09:00
        ["makenaide-schedule-15-00"]="cron(0 6 * * ? *)"   # UTC 06:00 = KST 15:00
        ["makenaide-schedule-18-00"]="cron(0 9 * * ? *)"   # UTC 09:00 = KST 18:00
        ["makenaide-schedule-21-00"]="cron(0 12 * * ? *)"  # UTC 12:00 = KST 21:00
        ["makenaide-schedule-23-00"]="cron(0 14 * * ? *)"  # UTC 14:00 = KST 23:00
    )

    local created_count=0

    for rule_name in "${!schedules[@]}"; do
        local schedule_expression="${schedules[$rule_name]}"

        # EventBridge 규칙 생성
        aws events put-rule \
            --name "$rule_name" \
            --schedule-expression "$schedule_expression" \
            --description "Makenaide 자동 실행 스케줄" \
            --state ENABLED \
            > /dev/null

        # Lambda 권한 부여
        aws lambda add-permission \
            --function-name makenaide-ec2-starter \
            --statement-id "${rule_name}-permission" \
            --action lambda:InvokeFunction \
            --principal events.amazonaws.com \
            --source-arn "arn:aws:events:ap-northeast-2:901361833359:rule/${rule_name}" \
            > /dev/null 2>&1 || true

        # 타겟 설정
        local input_json=$(cat <<EOF
{
    "pipeline_type": "main_trading",
    "schedule_name": "$rule_name",
    "kst_time": "${rule_name#*-schedule-}",
    "market_timing": "automated"
}
EOF
)

        aws events put-targets \
            --rule "$rule_name" \
            --targets "Id=1,Arn=arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-ec2-starter,Input='$input_json'" \
            > /dev/null

        ((created_count++))
        log_info "  ✅ $rule_name 생성 완료"
    done

    log_success "EventBridge 스케줄 설정 완료 (${created_count}개)"
}

# EC2 User Data 설정
setup_ec2_userdata() {
    log_info "🖥️ EC2 User Data 설정 중..."

    # User Data Base64 인코딩
    local user_data_encoded=$(base64 -i user_data_script.sh)

    # EC2 User Data 업데이트
    aws ec2 modify-instance-attribute \
        --instance-id i-082bf343089af62d3 \
        --user-data Value="$user_data_encoded"

    log_success "EC2 User Data 설정 완료"
}

# 시스템 테스트
test_system() {
    log_info "🧪 시스템 테스트 중..."

    # Lambda 함수 테스트
    local test_result=$(aws lambda invoke \
        --function-name makenaide-ec2-starter \
        --payload '{"pipeline_type":"test","schedule_name":"manual_test","kst_time":"TEST","market_timing":"test"}' \
        --output text \
        --query 'StatusCode' \
        /tmp/lambda_test_output.json 2>/dev/null)

    if [ "$test_result" = "200" ]; then
        log_success "Lambda 함수 테스트 성공"
    else
        log_warning "Lambda 함수 테스트 실패"
    fi

    # EventBridge 규칙 확인
    local rule_count=$(aws events list-rules --name-prefix makenaide-schedule --query 'length(Rules)' --output text)
    log_info "생성된 EventBridge 규칙: ${rule_count}개"

    # 정리
    rm -f /tmp/lambda_test_output.json
}

# 상태 표시
show_status() {
    log_info "📊 시스템 상태 확인"
    echo "=" * 50

    # 현재 시간 (KST)
    local current_kst=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S KST')
    log_info "현재 시각: $current_kst"

    # 다음 실행 시간들
    log_info "다음 자동 실행 시간:"
    log_info "  - 02:00 KST (아시아 심야 + 유럽 저녁)"
    log_info "  - 09:00 KST (한국/일본 장 시작)"
    log_info "  - 15:00 KST (아시아 오후 + 유럽 오전)"
    log_info "  - 18:00 KST (한국 퇴근시간 + 유럽 점심)"
    log_info "  - 21:00 KST (아시아 저녁 골든타임)"
    log_info "  - 23:00 KST (아시아 밤 + 미국 동부 오전)"

    echo
    log_info "모니터링 방법:"
    log_info "  - CloudWatch Logs: /aws/lambda/makenaide-ec2-starter"
    log_info "  - EC2 SSH: ssh -i /Users/13ruce/aws/makenaide-key.pem ec2-user@52.78.186.226"
    log_info "  - 로그 파일: ~/makenaide/logs/auto_execution.log"
}

# 메인 실행
main() {
    echo "=" * 60
    log_info "🚀 Makenaide EC2 자동화 설정 시작"
    echo "=" * 60

    # 1. 사전 조건 확인
    check_prerequisites

    # 2. Lambda 함수 배포
    deploy_lambda

    # 3. EventBridge 스케줄 설정
    setup_eventbridge

    # 4. EC2 User Data 설정
    setup_ec2_userdata

    # 5. 시스템 테스트
    test_system

    # 6. 상태 표시
    show_status

    echo "=" * 60
    log_success "🎉 Makenaide EC2 자동화 설정 완료!"
    echo "=" * 60

    log_info "🎯 다음 단계:"
    log_info "1. EC2에 최신 Makenaide 코드 업로드"
    log_info "2. .env 파일 및 API 키 설정 확인"
    log_info "3. 첫 번째 자동 실행 대기"

    echo
    log_warning "⚠️ 주의사항:"
    log_warning "- EC2 인스턴스가 중지된 상태에서 시작됩니다"
    log_warning "- 파이프라인 완료 후 자동으로 종료됩니다"
    log_warning "- 비용 절약을 위해 실행 시간만 과금됩니다"
}

# 스크립트 실행
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi