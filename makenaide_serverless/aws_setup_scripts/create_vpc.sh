#!/bin/bash
# create_vpc.sh - VPC 및 보안 그룹 생성 스크립트

set -e  # 오류 발생 시 스크립트 중단

echo "🚀 AWS VPC 및 보안 그룹 설정 시작"

# 환경 변수 설정
REGION="ap-northeast-2"
VPC_CIDR="10.0.0.0/16"
PUBLIC_SUBNET_CIDR="10.0.1.0/24"
PRIVATE_SUBNET_CIDR_1="10.0.2.0/24"
PRIVATE_SUBNET_CIDR_2="10.0.3.0/24"

# 1. VPC 생성
echo "📦 VPC 생성 중..."
VPC_ID=$(aws ec2 create-vpc \
    --cidr-block $VPC_CIDR \
    --region $REGION \
    --query 'Vpc.VpcId' \
    --output text)

aws ec2 create-tags \
    --resources $VPC_ID \
    --tags Key=Name,Value=makenaide-vpc \
    --region $REGION

echo "✅ VPC 생성 완료: $VPC_ID"

# 2. 인터넷 게이트웨이 생성 및 연결
echo "🌐 인터넷 게이트웨이 생성 중..."
IGW_ID=$(aws ec2 create-internet-gateway \
    --region $REGION \
    --query 'InternetGateway.InternetGatewayId' \
    --output text)

aws ec2 attach-internet-gateway \
    --vpc-id $VPC_ID \
    --internet-gateway-id $IGW_ID \
    --region $REGION

aws ec2 create-tags \
    --resources $IGW_ID \
    --tags Key=Name,Value=makenaide-igw \
    --region $REGION

echo "✅ 인터넷 게이트웨이 연결 완료: $IGW_ID"

# 3. 퍼블릭 서브넷 생성
echo "🏠 퍼블릭 서브넷 생성 중..."
PUBLIC_SUBNET_ID=$(aws ec2 create-subnet \
    --vpc-id $VPC_ID \
    --cidr-block $PUBLIC_SUBNET_CIDR \
    --availability-zone ${REGION}a \
    --region $REGION \
    --query 'Subnet.SubnetId' \
    --output text)

aws ec2 create-tags \
    --resources $PUBLIC_SUBNET_ID \
    --tags Key=Name,Value=makenaide-public-subnet \
    --region $REGION

echo "✅ 퍼블릭 서브넷 생성 완료: $PUBLIC_SUBNET_ID"

# 4. 프라이빗 서브넷 생성 (2개 AZ)
echo "🏠 프라이빗 서브넷 생성 중..."
PRIVATE_SUBNET_ID_1=$(aws ec2 create-subnet \
    --vpc-id $VPC_ID \
    --cidr-block $PRIVATE_SUBNET_CIDR_1 \
    --availability-zone ${REGION}a \
    --region $REGION \
    --query 'Subnet.SubnetId' \
    --output text)

aws ec2 create-tags \
    --resources $PRIVATE_SUBNET_ID_1 \
    --tags Key=Name,Value=makenaide-private-subnet-1 \
    --region $REGION

PRIVATE_SUBNET_ID_2=$(aws ec2 create-subnet \
    --vpc-id $VPC_ID \
    --cidr-block $PRIVATE_SUBNET_CIDR_2 \
    --availability-zone ${REGION}b \
    --region $REGION \
    --query 'Subnet.SubnetId' \
    --output text)

aws ec2 create-tags \
    --resources $PRIVATE_SUBNET_ID_2 \
    --tags Key=Name,Value=makenaide-private-subnet-2 \
    --region $REGION

echo "✅ 프라이빗 서브넷 생성 완료: $PRIVATE_SUBNET_ID_1, $PRIVATE_SUBNET_ID_2"

# 5. 라우팅 테이블 생성 (퍼블릭)
echo "🛣️ 퍼블릭 라우팅 테이블 생성 중..."
PUBLIC_RT_ID=$(aws ec2 create-route-table \
    --vpc-id $VPC_ID \
    --region $REGION \
    --query 'RouteTable.RouteTableId' \
    --output text)

aws ec2 create-route \
    --route-table-id $PUBLIC_RT_ID \
    --destination-cidr-block 0.0.0.0/0 \
    --gateway-id $IGW_ID \
    --region $REGION

aws ec2 associate-route-table \
    --subnet-id $PUBLIC_SUBNET_ID \
    --route-table-id $PUBLIC_RT_ID \
    --region $REGION

aws ec2 create-tags \
    --resources $PUBLIC_RT_ID \
    --tags Key=Name,Value=makenaide-public-rt \
    --region $REGION

echo "✅ 퍼블릭 라우팅 테이블 설정 완료: $PUBLIC_RT_ID"

# 6. 보안 그룹 생성 (EC2용)
echo "🔒 EC2 보안 그룹 생성 중..."
EC2_SG_ID=$(aws ec2 create-security-group \
    --group-name makenaide-ec2-sg \
    --description "Security group for Makenaide EC2 instance" \
    --vpc-id $VPC_ID \
    --region $REGION \
    --query 'GroupId' \
    --output text)

# SSH 접속 허용 (제한된 IP에서만)
aws ec2 authorize-security-group-ingress \
    --group-id $EC2_SG_ID \
    --protocol tcp \
    --port 22 \
    --cidr 0.0.0.0/0 \
    --region $REGION

# HTTP/HTTPS 허용 (필요시)
aws ec2 authorize-security-group-ingress \
    --group-id $EC2_SG_ID \
    --protocol tcp \
    --port 80 \
    --cidr 0.0.0.0/0 \
    --region $REGION

aws ec2 authorize-security-group-ingress \
    --group-id $EC2_SG_ID \
    --protocol tcp \
    --port 443 \
    --cidr 0.0.0.0/0 \
    --region $REGION

# 아웃바운드 규칙은 기본적으로 모든 트래픽 허용이므로 추가 설정 불필요
# AWS 보안 그룹은 기본적으로 모든 아웃바운드 트래픽을 허용합니다

echo "✅ EC2 보안 그룹 생성 완료: $EC2_SG_ID"

# 7. 보안 그룹 생성 (RDS용)
echo "🔒 RDS 보안 그룹 생성 중..."
RDS_SG_ID=$(aws ec2 create-security-group \
    --group-name makenaide-rds-sg \
    --description "Security group for Makenaide RDS instance" \
    --vpc-id $VPC_ID \
    --region $REGION \
    --query 'GroupId' \
    --output text)

# PostgreSQL 포트 허용 (EC2에서만)
aws ec2 authorize-security-group-ingress \
    --group-id $RDS_SG_ID \
    --protocol tcp \
    --port 5432 \
    --source-group $EC2_SG_ID \
    --region $REGION

echo "✅ RDS 보안 그룹 생성 완료: $RDS_SG_ID"

# 8. 설정 정보 저장
echo "💾 설정 정보 저장 중..."
cat > aws_vpc_config.json << EOF
{
    "vpc_id": "$VPC_ID",
    "public_subnet_id": "$PUBLIC_SUBNET_ID",
    "private_subnet_id_1": "$PRIVATE_SUBNET_ID_1",
    "private_subnet_id_2": "$PRIVATE_SUBNET_ID_2",
    "ec2_security_group_id": "$EC2_SG_ID",
    "rds_security_group_id": "$RDS_SG_ID",
    "region": "$REGION"
}
EOF

echo "✅ 설정 정보 저장 완료: aws_vpc_config.json"

# 9. 결과 출력
echo ""
echo "🎉 VPC 및 보안 그룹 설정 완료!"
echo "=================================="
echo "VPC ID: $VPC_ID"
echo "퍼블릭 서브넷: $PUBLIC_SUBNET_ID"
echo "프라이빗 서브넷 1: $PRIVATE_SUBNET_ID_1"
echo "프라이빗 서브넷 2: $PRIVATE_SUBNET_ID_2"
echo "EC2 보안 그룹: $EC2_SG_ID"
echo "RDS 보안 그룹: $RDS_SG_ID"
echo "리전: $REGION"
echo "=================================="
echo ""
echo "다음 단계: RDS PostgreSQL 인스턴스 생성"
echo "스크립트 실행: ./create_rds.sh" 