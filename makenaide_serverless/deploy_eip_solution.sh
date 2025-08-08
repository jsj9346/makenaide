#!/bin/bash
# deploy_eip_solution.sh - EIP 자동 관리 솔루션 배포

set -e

echo "🚀 Makenaide EIP 자동 관리 솔루션 배포"
echo "======================================"
echo "EC2 인스턴스 재시작 시 IP 변동 문제 완전 해결"
echo "======================================"

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
echo ""

# 1단계: 현재 상태 확인
echo "🔍 1단계: 현재 AWS 인프라 상태 확인"
echo "=================================="

python aws_eip_manager.py --check-status
echo ""

# 2단계: EC2 인스턴스 확인 및 생성
echo "🏗️ 2단계: EC2 인스턴스 확인"
echo "=========================="

# EC2 인스턴스 존재 확인
INSTANCE_COUNT=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=makenaide-ec2" "Name=instance-state-name,Values=running,stopped,pending,stopping" \
    --query "length(Reservations[0].Instances)" \
    --output text 2>/dev/null || echo "0")

if [ "$INSTANCE_COUNT" = "0" ] || [ "$INSTANCE_COUNT" = "None" ]; then
    echo "⚠️ makenaide-ec2 인스턴스가 없습니다."
    echo "ℹ️ 전체 AWS 인프라를 먼저 구축해야 합니다."
    echo ""
    echo "📋 필요한 작업:"
    echo "1. aws_setup_scripts 디렉토리로 이동"
    echo "2. ./run_setup.sh 실행 (VPC, RDS, IAM 생성)"
    echo "3. ./deploy_cloud_automation.sh 실행 (EC2, Lambda 생성)"
    echo "4. 이 스크립트 재실행"
    echo ""
    echo "🔧 자동 실행 옵션:"
    echo "cd aws_setup_scripts && ./run_setup.sh && ./deploy_cloud_automation.sh"
    echo ""
    exit 1
else
    echo "✅ makenaide-ec2 인스턴스 발견"
    
    # 인스턴스 정보 출력
    aws ec2 describe-instances \
        --filters "Name=tag:Name,Values=makenaide-ec2" \
        --query "Reservations[0].Instances[0].{InstanceId:InstanceId,State:State.Name,PublicIP:PublicIpAddress,PrivateIP:PrivateIpAddress}" \
        --output table
fi

echo ""

# 3단계: Elastic IP 할당 및 연결
echo "📍 3단계: Elastic IP 관리"
echo "======================="

python aws_eip_manager.py --associate
echo ""

# 4단계: Lambda 함수 업데이트 (EIP 자동 관리 기능 포함)
echo "🔧 4단계: Lambda 함수 업데이트"
echo "=========================="

if [ -d "aws_setup_scripts" ]; then
    cd aws_setup_scripts
    
    # EIP 자동 관리 기능이 포함된 Lambda 함수 배포
    if [ -f "../aws_setup_scripts/create_lambda_enhanced.sh" ]; then
        chmod +x create_lambda_enhanced.sh
        ./create_lambda_enhanced.sh
        echo "✅ Lambda 함수 EIP 자동 관리 기능 추가 완료"
    else
        echo "⚠️ 개선된 Lambda 스크립트를 찾을 수 없어 기본 Lambda로 진행합니다"
        chmod +x create_lambda.sh
        ./create_lambda.sh
    fi
    
    cd ..
else
    echo "⚠️ aws_setup_scripts 디렉토리를 찾을 수 없습니다"
fi

echo ""

# 5단계: IAM 권한 확인 및 업데이트
echo "🔐 5단계: IAM 권한 업데이트"
echo "======================="

# Lambda 역할에 EIP 관리 권한 추가
LAMBDA_ROLE_NAME="makenaide-lambda-role"

echo "📋 Lambda 역할에 EIP 관리 권한 추가 중..."

# EIP 관리를 위한 추가 정책 생성
cat > eip-management-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:AllocateAddress",
                "ec2:AssociateAddress",
                "ec2:DisassociateAddress",
                "ec2:DescribeAddresses",
                "ec2:CreateTags",
                "ec2:DescribeTags"
            ],
            "Resource": "*"
        }
    ]
}
EOF

# 정책 생성 및 역할에 연결
POLICY_NAME="MakenaideEIPManagementPolicy"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

# 기존 정책이 있는지 확인
if aws iam get-policy --policy-arn "$POLICY_ARN" >/dev/null 2>&1; then
    echo "ℹ️ 기존 EIP 관리 정책이 있어 업데이트합니다"
    aws iam create-policy-version \
        --policy-arn "$POLICY_ARN" \
        --policy-document file://eip-management-policy.json \
        --set-as-default
else
    echo "🆕 새로운 EIP 관리 정책을 생성합니다"
    aws iam create-policy \
        --policy-name "$POLICY_NAME" \
        --policy-document file://eip-management-policy.json \
        --description "Makenaide Elastic IP 관리 권한"
fi

# 역할에 정책 연결
aws iam attach-role-policy \
    --role-name "$LAMBDA_ROLE_NAME" \
    --policy-arn "$POLICY_ARN"

echo "✅ IAM 권한 업데이트 완료"

# 임시 파일 정리
rm -f eip-management-policy.json

echo ""

# 6단계: 시스템 테스트
echo "🧪 6단계: 시스템 통합 테스트"
echo "========================"

echo "📊 현재 상태 재확인..."
python aws_eip_manager.py --check-status

echo ""
echo "🔧 Lambda 함수 테스트..."

# Lambda 함수 테스트
FUNCTION_NAME="makenaide-controller"

if aws lambda get-function --function-name "$FUNCTION_NAME" >/dev/null 2>&1; then
    echo "✅ Lambda 함수 발견, EIP 연결 테스트 실행..."
    
    # EIP 연결 상태 확인 테스트
    aws lambda invoke \
        --function-name "$FUNCTION_NAME" \
        --payload '{"action":"check_eip"}' \
        lambda_test_response.json
    
    echo "📋 Lambda 테스트 결과:"
    cat lambda_test_response.json | jq '.' 2>/dev/null || cat lambda_test_response.json
    echo ""
    
    # 임시 파일 정리
    rm -f lambda_test_response.json
else
    echo "⚠️ Lambda 함수를 찾을 수 없습니다"
fi

echo ""

# 7단계: 자동화 스크립트 생성
echo "🤖 7단계: 자동화 스크립트 생성"
echo "=========================="

# EIP 모니터링 스크립트 생성
cat > monitor_eip.sh << 'EOF'
#!/bin/bash
# monitor_eip.sh - EIP 상태 모니터링 스크립트

echo "🔍 EIP 상태 모니터링 시작"
echo "========================"

# 5분 간격으로 모니터링 (백그라운드 실행 가능)
python aws_eip_manager.py --monitor --interval 300
EOF

chmod +x monitor_eip.sh

# EC2 시작 스크립트 생성
cat > start_ec2_with_eip.sh << 'EOF'
#!/bin/bash
# start_ec2_with_eip.sh - EC2 시작 및 EIP 자동 연결

echo "🚀 EC2 인스턴스 시작 및 EIP 연결"
echo "=========================="

# Lambda 함수를 통한 EC2 시작 (EIP 자동 연결 포함)
aws lambda invoke \
    --function-name makenaide-controller \
    --payload '{"action":"start"}' \
    start_response.json

echo "📋 실행 결과:"
cat start_response.json | jq '.' 2>/dev/null || cat start_response.json
rm -f start_response.json

echo ""
echo "📊 최종 상태 확인:"
python aws_eip_manager.py --check-status
EOF

chmod +x start_ec2_with_eip.sh

echo "✅ 자동화 스크립트 생성 완료:"
echo "  - monitor_eip.sh: EIP 모니터링"
echo "  - start_ec2_with_eip.sh: EC2 시작 및 EIP 연결"

echo ""

# 최종 상태 보고서
echo "🎉 EIP 자동 관리 솔루션 배포 완료!"
echo "=================================="
echo ""
echo "📋 구축된 시스템 요약:"
echo "✅ Elastic IP 자동 할당 및 관리"
echo "✅ EC2 재시작 시 IP 고정 보장"
echo "✅ Lambda 기반 자동화"
echo "✅ 실시간 모니터링"
echo ""
echo "🔧 주요 기능:"
echo "1. 💡 자동 EIP 할당 (없을 경우)"
echo "2. 🔗 EC2 시작 시 EIP 자동 연결"
echo "3. 📊 IP 상태 실시간 모니터링"
echo "4. 🔄 IP 변경 감지 및 자동 복구"
echo ""
echo "🎯 클라우드 운영 플로우 (수정됨):"
echo "1. EventBridge → 4시간마다 트리거"
echo "2. Lambda → EC2 인스턴스 자동 On + EIP 연결"
echo "3. EC2 부팅 시 → 고정 IP로 makenaide 실행"
echo "4. 전체 파이프라인 수행 (IP 고정 보장)"
echo "5. 작업 완료 시 → EC2 인스턴스 자동 Off"
echo "6. 다음 주기까지 대기 (EIP는 유지)"
echo ""
echo "📱 테스트 명령어:"
echo "# EIP 상태 확인"
echo "python aws_eip_manager.py --check-status"
echo ""
echo "# EC2 시작 + EIP 연결"
echo "./start_ec2_with_eip.sh"
echo ""
echo "# EIP 모니터링 시작"
echo "./monitor_eip.sh"
echo ""
echo "🚨 중요사항:"
echo "- EC2 재시작 시 IP 변동 문제 완전 해결"
echo "- Elastic IP 비용: 인스턴스 연결 시 무료, 미연결 시 시간당 과금"
echo "- 모든 작업이 자동화되어 수동 개입 불필요"
echo ""
echo "✅ 배포 완료: 안전한 고정 IP 트레이딩 환경 구축!" 