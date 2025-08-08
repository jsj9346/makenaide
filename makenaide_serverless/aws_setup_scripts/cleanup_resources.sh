#!/bin/bash
# cleanup_resources.sh - 기존 AWS 리소스 정리 스크립트

set -e

echo "🧹 AWS 리소스 정리 시작"
echo "=================================="

REGION="ap-northeast-2"

# 기존 리소스 ID들을 찾아서 정리
echo "🔍 기존 리소스 검색 중..."

# 1. 보안 그룹 정리
echo "🔒 보안 그룹 정리 중..."
SG_IDS=$(aws ec2 describe-security-groups \
    --region $REGION \
    --filters "Name=group-name,Values=makenaide-*" \
    --query 'SecurityGroups[*].GroupId' \
    --output text)

if [ ! -z "$SG_IDS" ]; then
    for SG_ID in $SG_IDS; do
        echo "보안 그룹 삭제 중: $SG_ID"
        aws ec2 delete-security-group --group-id $SG_ID --region $REGION 2>/dev/null || echo "보안 그룹 $SG_ID 삭제 실패 (이미 삭제됨 또는 의존성 존재)"
    done
else
    echo "삭제할 보안 그룹이 없습니다."
fi

# 2. 서브넷 정리
echo "🏠 서브넷 정리 중..."
SUBNET_IDS=$(aws ec2 describe-subnets \
    --region $REGION \
    --filters "Name=tag:Name,Values=makenaide-*" \
    --query 'Subnets[*].SubnetId' \
    --output text)

if [ ! -z "$SUBNET_IDS" ]; then
    for SUBNET_ID in $SUBNET_IDS; do
        echo "서브넷 삭제 중: $SUBNET_ID"
        aws ec2 delete-subnet --subnet-id $SUBNET_ID --region $REGION 2>/dev/null || echo "서브넷 $SUBNET_ID 삭제 실패 (이미 삭제됨 또는 의존성 존재)"
    done
else
    echo "삭제할 서브넷이 없습니다."
fi

# 3. 라우팅 테이블 정리
echo "🛣️ 라우팅 테이블 정리 중..."
RT_IDS=$(aws ec2 describe-route-tables \
    --region $REGION \
    --filters "Name=tag:Name,Values=makenaide-*" \
    --query 'RouteTables[*].RouteTableId' \
    --output text)

if [ ! -z "$RT_IDS" ]; then
    for RT_ID in $RT_IDS; do
        echo "라우팅 테이블 삭제 중: $RT_ID"
        aws ec2 delete-route-table --route-table-id $RT_ID --region $REGION 2>/dev/null || echo "라우팅 테이블 $RT_ID 삭제 실패 (이미 삭제됨 또는 의존성 존재)"
    done
else
    echo "삭제할 라우팅 테이블이 없습니다."
fi

# 4. 인터넷 게이트웨이 정리
echo "🌐 인터넷 게이트웨이 정리 중..."
IGW_IDS=$(aws ec2 describe-internet-gateways \
    --region $REGION \
    --filters "Name=tag:Name,Values=makenaide-*" \
    --query 'InternetGateways[*].InternetGatewayId' \
    --output text)

if [ ! -z "$IGW_IDS" ]; then
    for IGW_ID in $IGW_IDS; do
        echo "인터넷 게이트웨이 분리 중: $IGW_ID"
        VPC_ID=$(aws ec2 describe-internet-gateways \
            --internet-gateway-ids $IGW_ID \
            --region $REGION \
            --query 'InternetGateways[0].Attachments[0].VpcId' \
            --output text)
        
        if [ "$VPC_ID" != "None" ] && [ ! -z "$VPC_ID" ]; then
            aws ec2 detach-internet-gateway --internet-gateway-id $IGW_ID --vpc-id $VPC_ID --region $REGION 2>/dev/null || echo "인터넷 게이트웨이 $IGW_ID 분리 실패"
        fi
        
        echo "인터넷 게이트웨이 삭제 중: $IGW_ID"
        aws ec2 delete-internet-gateway --internet-gateway-id $IGW_ID --region $REGION 2>/dev/null || echo "인터넷 게이트웨이 $IGW_ID 삭제 실패"
    done
else
    echo "삭제할 인터넷 게이트웨이가 없습니다."
fi

# 5. VPC 정리
echo "🌐 VPC 정리 중..."
VPC_IDS=$(aws ec2 describe-vpcs \
    --region $REGION \
    --filters "Name=tag:Name,Values=makenaide-vpc" \
    --query 'Vpcs[*].VpcId' \
    --output text)

if [ ! -z "$VPC_IDS" ]; then
    for VPC_ID in $VPC_IDS; do
        echo "VPC 삭제 중: $VPC_ID"
        aws ec2 delete-vpc --vpc-id $VPC_ID --region $REGION 2>/dev/null || echo "VPC $VPC_ID 삭제 실패 (의존성 존재)"
    done
else
    echo "삭제할 VPC가 없습니다."
fi

# 6. 설정 파일 정리
echo "📁 설정 파일 정리 중..."
if [ -f "aws_vpc_config.json" ]; then
    rm aws_vpc_config.json
    echo "✅ aws_vpc_config.json 삭제 완료"
fi

echo ""
echo "🎉 AWS 리소스 정리 완료!"
echo "=================================="
echo "이제 ./create_vpc.sh를 다시 실행할 수 있습니다." 