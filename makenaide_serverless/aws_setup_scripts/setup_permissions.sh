#!/bin/bash
# setup_permissions.sh - AWS 사용자 권한 설정 스크립트

set -e  # 오류 발생 시 스크립트 중단

echo "🔐 AWS 사용자 권한 설정 시작"

# 현재 사용자 정보 확인
CURRENT_USER=$(aws sts get-caller-identity --query Arn --output text)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-northeast-2"

echo "현재 사용자: $CURRENT_USER"
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"

# 1. Makenaide 관리자 정책 생성
echo "📋 Makenaide 관리자 정책 생성 중..."
cat > makenaide-admin-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:*",
                "rds:*",
                "iam:*",
                "s3:*",
                "lambda:*",
                "events:*",
                "cloudwatch:*",
                "logs:*",
                "ssm:*",
                "secretsmanager:*"
            ],
            "Resource": "*"
        }
    ]
}
EOF

# 정책 생성
aws iam create-policy \
    --policy-name MakenaideAdminPolicy \
    --policy-document file://makenaide-admin-policy.json

echo "✅ Makenaide 관리자 정책 생성 완료"

# 2. 사용자에게 정책 연결
echo "🔗 사용자에게 정책 연결 중..."
USER_NAME=$(echo $CURRENT_USER | cut -d'/' -f2)

aws iam attach-user-policy \
    --user-name $USER_NAME \
    --policy-arn arn:aws:iam::$ACCOUNT_ID:policy/MakenaideAdminPolicy

echo "✅ 사용자에게 정책 연결 완료: $USER_NAME"

# 3. 임시 파일 정리
rm -f makenaide-admin-policy.json

# 4. 권한 확인
echo ""
echo "🔍 권한 확인 중..."
sleep 5  # IAM 권한 전파 대기

if aws ec2 describe-vpcs --max-items 1 &> /dev/null; then
    echo "✅ EC2 권한 확인 완료"
else
    echo "❌ EC2 권한 확인 실패"
    exit 1
fi

if aws rds describe-db-instances --max-items 1 &> /dev/null; then
    echo "✅ RDS 권한 확인 완료"
else
    echo "❌ RDS 권한 확인 실패"
    exit 1
fi

if aws iam list-roles --max-items 1 &> /dev/null; then
    echo "✅ IAM 권한 확인 완료"
else
    echo "❌ IAM 권한 확인 실패"
    exit 1
fi

echo ""
echo "🎉 AWS 사용자 권한 설정 완료!"
echo "=================================="
echo "사용자: $USER_NAME"
echo "정책: MakenaideAdminPolicy"
echo "계정: $ACCOUNT_ID"
echo "리전: $REGION"
echo "=================================="
echo ""
echo "이제 1단계 AWS 환경 설정을 진행할 수 있습니다:"
echo "./run_setup.sh" 