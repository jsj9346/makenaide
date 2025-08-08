#!/bin/bash
# run_cloud_setup.sh - Makenaide AWS 클라우드 자동화 통합 설정 스크립트

set -e

echo "🚀 Makenaide AWS 클라우드 자동화 시스템 구축"
echo "================================================"
echo "Knowledge 파일 기반 클라우드 운영 방식 구현"
echo "================================================"

# 현재 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# AWS CLI 확인
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI가 설치되지 않았습니다."
    echo "설치 방법: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    exit 1
fi

# AWS 자격 증명 확인
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS 자격 증명이 설정되지 않았습니다."
    echo "설정 방법: aws configure"
    exit 1
fi

echo "✅ 사전 요구사항 확인 완료"

# 1단계와 2단계를 순차적으로 실행
echo ""
echo "🔧 1단계: 기본 AWS 인프라 설정 시작..."
if [ -d "aws_setup_scripts" ]; then
    cd aws_setup_scripts
    
    # 1단계 실행
    if [ -f "run_setup.sh" ]; then
        chmod +x run_setup.sh
        ./run_setup.sh
        echo "✅ 1단계 완료: VPC, RDS, IAM 설정 완료"
    else
        echo "❌ aws_setup_scripts/run_setup.sh 파일을 찾을 수 없습니다."
        exit 1
    fi
    
    echo ""
    echo "🚀 2단계: 클라우드 자동화 시스템 구축 시작..."
    
    # 2단계 실행
    if [ -f "deploy_cloud_automation.sh" ]; then
        chmod +x deploy_cloud_automation.sh
        ./deploy_cloud_automation.sh
        echo "✅ 2단계 완료: Lambda, EventBridge, EC2 자동화 설정 완료"
    else
        echo "❌ aws_setup_scripts/deploy_cloud_automation.sh 파일을 찾을 수 없습니다."
        exit 1
    fi
    
    cd ..
else
    echo "❌ aws_setup_scripts 디렉토리를 찾을 수 없습니다."
    exit 1
fi

echo ""
echo "🎉 Makenaide AWS 클라우드 자동화 시스템 구축 완료!"
echo "================================================"

# 최종 상태 확인
if [ -f "aws_setup_scripts/aws_final_config.json" ]; then
    echo "📊 구축된 시스템 요약:"
    
    # jq가 있으면 JSON 파싱, 없으면 기본 출력
    if command -v jq &> /dev/null; then
        LAMBDA_FUNCTION=$(jq -r '.lambda_function_name // "makenaide-controller"' aws_setup_scripts/aws_final_config.json)
        START_RULE=$(jq -r '.start_rule_name // "makenaide-schedule"' aws_setup_scripts/aws_final_config.json)
        INSTANCE_ID=$(jq -r '.instance_id // "확인 필요"' aws_setup_scripts/aws_final_config.json)
        PUBLIC_IP=$(jq -r '.public_ip // "확인 필요"' aws_setup_scripts/aws_final_config.json)
        
        echo "✅ Lambda 함수: $LAMBDA_FUNCTION"
        echo "✅ EventBridge 규칙: $START_RULE (4시간 간격)"
        echo "✅ EC2 인스턴스: $INSTANCE_ID"
        echo "✅ 퍼블릭 IP: $PUBLIC_IP"
    else
        echo "✅ Lambda 함수: makenaide-controller"
        echo "✅ EventBridge 규칙: makenaide-schedule (4시간 간격)"
        echo "✅ EC2 인스턴스: 생성 완료"
        echo "✅ 자동화 시스템: 구축 완료"
    fi
else
    echo "⚠️ 설정 파일을 찾을 수 없어 상세 정보를 표시할 수 없습니다."
fi

echo "================================================"
echo ""
echo "🎯 클라우드 운영 플로우 (Knowledge 파일 기준):"
echo "1. EventBridge → 4시간마다 트리거"
echo "2. Lambda → EC2 인스턴스 자동 On"
echo "3. EC2 부팅 시 → makenaide 자동 실행"
echo "4. 전체 파이프라인 수행"
echo "5. 작업 완료 시 → EC2 인스턴스 자동 Off"
echo "6. 다음 주기까지 대기"
echo ""
echo "📋 다음 작업 사항:"
echo "1. env.aws 파일에서 실제 API 키 설정"
echo "   - Upbit API 키 (UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)"
echo "   - OpenAI API 키 (OPENAI_API_KEY)"
echo "   - AWS 액세스 키 (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)"
echo "2. 데이터베이스 비밀번호 확인 및 업데이트"
echo "3. 시스템 테스트 실행"
echo ""
echo "🔧 시스템 테스트 방법:"
echo "1. Lambda 함수 수동 테스트:"
echo "   aws lambda invoke --function-name makenaide-controller --payload '{\"action\":\"start\"}' response.json"
echo ""
echo "2. EventBridge 규칙 상태 확인:"
echo "   aws events list-rules --name-prefix makenaide"
echo ""
echo "3. EC2 인스턴스 상태 확인:"
echo "   aws ec2 describe-instances --filters Name=tag:Name,Values=makenaide-ec2"
echo ""
echo "⚠️ 보안 주의사항:"
echo "- API 키는 절대 Git에 커밋하지 마세요"
echo "- 환경변수 파일 권한을 적절히 설정하세요 (chmod 600 env.aws)"
echo "- 정기적으로 API 키를 교체하세요"
echo ""
echo "🎉 설정이 완료되었습니다! 안전한 거래하세요! 🚀" 