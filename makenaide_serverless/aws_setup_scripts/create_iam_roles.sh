#!/bin/bash
# create_iam_roles.sh - IAM 역할 및 정책 설정 스크립트

set -e  # 오류 발생 시 스크립트 중단

echo "🚀 IAM 역할 및 정책 설정 시작"

# 환경 변수 설정
REGION="ap-northeast-2"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# 1. Lambda 실행 역할 생성
echo "🔐 Lambda 실행 역할 생성 중..."
LAMBDA_ROLE_NAME="makenaide-lambda-role"

# 신뢰 정책 생성
cat > lambda-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

# Lambda 역할 생성
aws iam create-role \
    --role-name $LAMBDA_ROLE_NAME \
    --assume-role-policy-document file://lambda-trust-policy.json

# Lambda 기본 정책 연결
aws iam attach-role-policy \
    --role-name $LAMBDA_ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Lambda EC2 제어 정책 생성
cat > lambda-ec2-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:StartInstances",
                "ec2:StopInstances",
                "ec2:DescribeInstances",
                "ec2:DescribeInstanceStatus",
                "ec2:DescribeInstanceAttribute"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
EOF

# Lambda EC2 제어 정책 생성 및 연결
aws iam create-policy \
    --policy-name makenaide-lambda-ec2-policy \
    --policy-document file://lambda-ec2-policy.json

aws iam attach-role-policy \
    --role-name $LAMBDA_ROLE_NAME \
    --policy-arn arn:aws:iam::$ACCOUNT_ID:policy/makenaide-lambda-ec2-policy

echo "✅ Lambda 실행 역할 생성 완료: $LAMBDA_ROLE_NAME"

# 2. EC2 실행 역할 생성
echo "🔐 EC2 실행 역할 생성 중..."
EC2_ROLE_NAME="makenaide-ec2-role"

# EC2 신뢰 정책 생성
cat > ec2-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

# EC2 역할 생성
aws iam create-role \
    --role-name $EC2_ROLE_NAME \
    --assume-role-policy-document file://ec2-trust-policy.json

# EC2 기본 정책 연결
aws iam attach-role-policy \
    --role-name $EC2_ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

# EC2 S3 접근 정책 생성
cat > ec2-s3-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::makenaide-bucket",
                "arn:aws:s3:::makenaide-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:StopInstances"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "ec2:ResourceTag/Name": "makenaide-bot"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
EOF

# EC2 S3 접근 정책 생성 및 연결
aws iam create-policy \
    --policy-name makenaide-ec2-s3-policy \
    --policy-document file://ec2-s3-policy.json

aws iam attach-role-policy \
    --role-name $EC2_ROLE_NAME \
    --policy-arn arn:aws:iam::$ACCOUNT_ID:policy/makenaide-ec2-s3-policy

# EC2 인스턴스 프로파일 생성
aws iam create-instance-profile \
    --instance-profile-name makenaide-ec2-profile

aws iam add-role-to-instance-profile \
    --instance-profile-name makenaide-ec2-profile \
    --role-name $EC2_ROLE_NAME

echo "✅ EC2 실행 역할 생성 완료: $EC2_ROLE_NAME"

# 3. EventBridge 역할 생성
echo "🔐 EventBridge 역할 생성 중..."
EVENTBRIDGE_ROLE_NAME="makenaide-eventbridge-role"

# EventBridge 신뢰 정책 생성
cat > eventbridge-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

# EventBridge 역할 생성
aws iam create-role \
    --role-name $EVENTBRIDGE_ROLE_NAME \
    --assume-role-policy-document file://eventbridge-trust-policy.json

# EventBridge Lambda 호출 정책 생성
cat > eventbridge-lambda-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": "arn:aws:lambda:$REGION:$ACCOUNT_ID:function:makenaide-controller"
        }
    ]
}
EOF

# EventBridge Lambda 호출 정책 생성 및 연결
aws iam create-policy \
    --policy-name makenaide-eventbridge-lambda-policy \
    --policy-document file://eventbridge-lambda-policy.json

aws iam attach-role-policy \
    --role-name $EVENTBRIDGE_ROLE_NAME \
    --policy-arn arn:aws:iam::$ACCOUNT_ID:policy/makenaide-eventbridge-lambda-policy

echo "✅ EventBridge 역할 생성 완료: $EVENTBRIDGE_ROLE_NAME"

# 4. S3 버킷 생성 (로그 및 백업용)
echo "🪣 S3 버킷 생성 중..."
BUCKET_NAME="makenaide-bucket-$ACCOUNT_ID"

aws s3 mb s3://$BUCKET_NAME --region $REGION

# 버킷 버전 관리 활성화
aws s3api put-bucket-versioning \
    --bucket $BUCKET_NAME \
    --versioning-configuration Status=Enabled

# 수명 주기 정책 설정
cat > s3-lifecycle-policy.json << EOF
{
    "Rules": [
        {
            "ID": "LogRetention",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "logs/"
            },
            "Expiration": {
                "Days": 30
            }
        },
        {
            "ID": "BackupRetention",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "backups/"
            },
            "Expiration": {
                "Days": 90
            }
        }
    ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
    --bucket $BUCKET_NAME \
    --lifecycle-configuration file://s3-lifecycle-policy.json

echo "✅ S3 버킷 생성 완료: $BUCKET_NAME"

# 5. 설정 정보 저장
echo "💾 IAM 설정 정보 저장 중..."
cat > aws_iam_config.json << EOF
{
    "lambda_role_arn": "arn:aws:iam::$ACCOUNT_ID:role/$LAMBDA_ROLE_NAME",
    "ec2_role_arn": "arn:aws:iam::$ACCOUNT_ID:role/$EC2_ROLE_NAME",
    "ec2_instance_profile_arn": "arn:aws:iam::$ACCOUNT_ID:instance-profile/makenaide-ec2-profile",
    "eventbridge_role_arn": "arn:aws:iam::$ACCOUNT_ID:role/$EVENTBRIDGE_ROLE_NAME",
    "s3_bucket_name": "$BUCKET_NAME",
    "account_id": "$ACCOUNT_ID",
    "region": "$REGION"
}
EOF

echo "✅ IAM 설정 정보 저장 완료: aws_iam_config.json"

# 6. 임시 파일 정리
rm -f lambda-trust-policy.json lambda-ec2-policy.json
rm -f ec2-trust-policy.json ec2-s3-policy.json
rm -f eventbridge-trust-policy.json eventbridge-lambda-policy.json
rm -f s3-lifecycle-policy.json

# 7. 결과 출력
echo ""
echo "🎉 IAM 역할 및 정책 설정 완료!"
echo "=================================="
echo "Lambda 역할: $LAMBDA_ROLE_NAME"
echo "EC2 역할: $EC2_ROLE_NAME"
echo "EC2 인스턴스 프로파일: makenaide-ec2-profile"
echo "EventBridge 역할: $EVENTBRIDGE_ROLE_NAME"
echo "S3 버킷: $BUCKET_NAME"
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "=================================="
echo ""
echo "다음 단계: 2단계 리소스 최적화"
echo "스크립트 실행: ./optimize_resources.sh" 