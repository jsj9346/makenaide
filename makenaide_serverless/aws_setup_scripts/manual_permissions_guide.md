# AWS 권한 설정 가이드

## 🔐 AWS 콘솔에서 권한 설정

현재 사용자에게 필요한 권한이 없으므로, AWS 관리자 또는 콘솔에서 직접 권한을 설정해야 합니다.

### 1. AWS IAM 콘솔 접속
1. AWS 콘솔에 로그인
2. IAM 서비스로 이동
3. 사용자 목록에서 `bruce` 사용자 선택

### 2. 필요한 정책 연결

다음 AWS 관리형 정책들을 사용자에게 연결하세요:

#### 필수 정책들:
- **AmazonEC2FullAccess** - EC2 리소스 관리
- **AmazonRDSFullAccess** - RDS 데이터베이스 관리
- **IAMFullAccess** - IAM 역할 및 정책 관리
- **AmazonS3FullAccess** - S3 버킷 관리
- **AWSLambda_FullAccess** - Lambda 함수 관리
- **CloudWatchFullAccess** - 모니터링 및 로그 관리
- **AmazonEventBridgeFullAccess** - EventBridge 규칙 관리

### 3. 정책 연결 방법

1. IAM 콘솔에서 사용자 `bruce` 선택
2. "권한" 탭 클릭
3. "권한 추가" 버튼 클릭
4. "기존 정책 직접 연결" 선택
5. 위의 정책들을 검색하여 선택
6. "다음" 클릭 후 "권한 추가" 완료

### 4. 권한 확인

권한 설정 후 다음 명령어로 확인:

```bash
# EC2 권한 확인
aws ec2 describe-vpcs --max-items 1

# RDS 권한 확인
aws rds describe-db-instances --max-items 1

# IAM 권한 확인
aws iam list-roles --max-items 1
```

### 5. 보안 고려사항

⚠️ **중요**: 위 정책들은 전체 관리자 권한을 포함합니다. 
실제 운영 환경에서는 최소 권한 원칙에 따라 필요한 권한만 부여하는 것을 권장합니다.

### 6. 최소 권한 정책 (선택사항)

더 안전한 운영을 위해 다음 커스텀 정책을 사용할 수 있습니다:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateVpc",
                "ec2:CreateSubnet",
                "ec2:CreateSecurityGroup",
                "ec2:CreateRouteTable",
                "ec2:CreateInternetGateway",
                "ec2:AttachInternetGateway",
                "ec2:CreateRoute",
                "ec2:AssociateRouteTable",
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:AuthorizeSecurityGroupEgress",
                "ec2:CreateTags",
                "ec2:Describe*",
                "rds:CreateDBInstance",
                "rds:CreateDBSubnetGroup",
                "rds:CreateDBParameterGroup",
                "rds:ModifyDBParameterGroup",
                "rds:Describe*",
                "rds:WaitFor*",
                "iam:CreateRole",
                "iam:CreatePolicy",
                "iam:AttachRolePolicy",
                "iam:CreateInstanceProfile",
                "iam:AddRoleToInstanceProfile",
                "iam:AttachUserPolicy",
                "iam:GetUser",
                "iam:List*",
                "s3:CreateBucket",
                "s3:PutBucketVersioning",
                "s3:PutBucketLifecycleConfiguration",
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "lambda:CreateFunction",
                "lambda:UpdateFunctionCode",
                "lambda:AddPermission",
                "lambda:GetFunction",
                "events:PutRule",
                "events:PutTargets",
                "events:DescribeRule",
                "cloudwatch:PutMetricData",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "ssm:GetParameter",
                "ssm:PutParameter"
            ],
            "Resource": "*"
        }
    ]
}
```

### 7. 권한 설정 완료 후

권한 설정이 완료되면 다음 명령어로 1단계 설정을 진행하세요:

```bash
./run_setup.sh
```

---

## 📞 AWS 관리자 연락 정보

권한 설정에 문제가 있는 경우 AWS 관리자에게 다음 정보를 제공하세요:

- **사용자명**: bruce
- **계정 ID**: 901361833359
- **필요 권한**: 위에 명시된 정책들
- **목적**: Makenaide 암호화폐 자동매매 시스템 AWS 배포 