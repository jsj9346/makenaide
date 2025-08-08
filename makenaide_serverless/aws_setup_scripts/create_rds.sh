#!/bin/bash
# create_rds.sh - RDS PostgreSQL 인스턴스 생성 스크립트

set -e  # 오류 발생 시 스크립트 중단

echo "🚀 RDS PostgreSQL 인스턴스 생성 시작"

# VPC 설정 정보 로드
if [ ! -f "aws_vpc_config.json" ]; then
    echo "❌ aws_vpc_config.json 파일이 없습니다. 먼저 VPC를 생성하세요."
    exit 1
fi

VPC_ID=$(jq -r '.vpc_id' aws_vpc_config.json)
PRIVATE_SUBNET_ID_1=$(jq -r '.private_subnet_id_1' aws_vpc_config.json)
PRIVATE_SUBNET_ID_2=$(jq -r '.private_subnet_id_2' aws_vpc_config.json)
RDS_SG_ID=$(jq -r '.rds_security_group_id' aws_vpc_config.json)
REGION=$(jq -r '.region' aws_vpc_config.json)

# 환경 변수 설정
DB_INSTANCE_IDENTIFIER="makenaide-db"
DB_NAME="dbname"
DB_USERNAME="username"
DB_PASSWORD="password"  # 실제 운영시에는 더 강력한 비밀번호 사용

# 1. 서브넷 그룹 생성
echo "🏠 RDS 서브넷 그룹 생성 중..."
SUBNET_GROUP_NAME="makenaide-subnet-group"

aws rds create-db-subnet-group \
    --db-subnet-group-name $SUBNET_GROUP_NAME \
    --db-subnet-group-description "Subnet group for Makenaide RDS" \
    --subnet-ids $PRIVATE_SUBNET_ID_1 $PRIVATE_SUBNET_ID_2 \
    --region $REGION

echo "✅ 서브넷 그룹 생성 완료: $SUBNET_GROUP_NAME"

# 2. 파라미터 그룹 생성 (성능 최적화)
echo "⚙️ 파라미터 그룹 생성 중..."
PARAMETER_GROUP_NAME="makenaide-params"

aws rds create-db-parameter-group \
    --db-parameter-group-name $PARAMETER_GROUP_NAME \
    --db-parameter-group-family postgres13 \
    --description "Parameter group for Makenaide PostgreSQL" \
    --region $REGION

# 성능 최적화 파라미터 설정 (중괄호가 포함된 값은 제외)
aws rds modify-db-parameter-group \
    --db-parameter-group-name $PARAMETER_GROUP_NAME \
    --parameters \
        ParameterName=shared_preload_libraries,ParameterValue=pg_stat_statements,ApplyMethod=pending-reboot \
        ParameterName=max_connections,ParameterValue=200,ApplyMethod=pending-reboot \
        ParameterName=work_mem,ParameterValue=1024,ApplyMethod=pending-reboot \
        ParameterName=maintenance_work_mem,ParameterValue=65536,ApplyMethod=pending-reboot \
        ParameterName=checkpoint_completion_target,ParameterValue=0.9,ApplyMethod=pending-reboot \
        ParameterName=wal_buffers,ParameterValue=16384,ApplyMethod=pending-reboot \
        ParameterName=default_statistics_target,ParameterValue=100,ApplyMethod=pending-reboot \
        ParameterName=random_page_cost,ParameterValue=1.1,ApplyMethod=pending-reboot \
        ParameterName=effective_io_concurrency,ParameterValue=200,ApplyMethod=pending-reboot \
        ParameterName=min_wal_size,ParameterValue=1048576,ApplyMethod=pending-reboot \
        ParameterName=max_wal_size,ParameterValue=4194304,ApplyMethod=pending-reboot \
        ParameterName=max_worker_processes,ParameterValue=8,ApplyMethod=pending-reboot \
        ParameterName=max_parallel_workers_per_gather,ParameterValue=4,ApplyMethod=pending-reboot \
        ParameterName=max_parallel_workers,ParameterValue=8,ApplyMethod=pending-reboot \
        ParameterName=max_parallel_maintenance_workers,ParameterValue=4,ApplyMethod=pending-reboot \
    --region $REGION

echo "✅ 파라미터 그룹 생성 완료: $PARAMETER_GROUP_NAME"

# 3. RDS 인스턴스 생성
echo "🗄️ RDS PostgreSQL 인스턴스 생성 중..."
aws rds create-db-instance \
    --db-instance-identifier $DB_INSTANCE_IDENTIFIER \
    --db-instance-class db.t3.micro \
    --engine postgres \
    --engine-version 13.10 \
    --master-username $DB_USERNAME \
    --master-user-password $DB_PASSWORD \
    --allocated-storage 20 \
    --storage-type gp3 \
    --storage-encrypted \
    --db-name $DB_NAME \
    --vpc-security-group-ids $RDS_SG_ID \
    --db-subnet-group-name $SUBNET_GROUP_NAME \
    --db-parameter-group-name $PARAMETER_GROUP_NAME \
    --backup-retention-period 7 \
    --preferred-backup-window "03:00-04:00" \
    --preferred-maintenance-window "sun:04:00-sun:05:00" \
    --auto-minor-version-upgrade \
    --deletion-protection \
    --region $REGION

echo "✅ RDS 인스턴스 생성 요청 완료: $DB_INSTANCE_IDENTIFIER"

# 4. 인스턴스 상태 확인 (최대 20분 대기)
echo "⏳ RDS 인스턴스 생성 완료 대기 중..."
aws rds wait db-instance-available \
    --db-instance-identifier $DB_INSTANCE_IDENTIFIER \
    --region $REGION

echo "✅ RDS 인스턴스 생성 완료!"

# 5. 엔드포인트 정보 조회
DB_ENDPOINT=$(aws rds describe-db-instances \
    --db-instance-identifier $DB_INSTANCE_IDENTIFIER \
    --region $REGION \
    --query 'DBInstances[0].Endpoint.Address' \
    --output text)

DB_PORT=$(aws rds describe-db-instances \
    --db-instance-identifier $DB_INSTANCE_IDENTIFIER \
    --region $REGION \
    --query 'DBInstances[0].Endpoint.Port' \
    --output text)

# 6. 설정 정보 업데이트
echo "💾 RDS 설정 정보 저장 중..."
cat > aws_rds_config.json << EOF
{
    "db_instance_identifier": "$DB_INSTANCE_IDENTIFIER",
    "db_endpoint": "$DB_ENDPOINT",
    "db_port": "$DB_PORT",
    "db_name": "$DB_NAME",
    "db_username": "$DB_USERNAME",
    "db_password": "$DB_PASSWORD",
    "subnet_group_name": "$SUBNET_GROUP_NAME",
    "parameter_group_name": "$PARAMETER_GROUP_NAME",
    "region": "$REGION"
}
EOF

echo "✅ RDS 설정 정보 저장 완료: aws_rds_config.json"

# 7. 데이터베이스 초기화 스크립트 생성
echo "📝 데이터베이스 초기화 스크립트 생성 중..."
cat > init_database.sh << EOF
#!/bin/bash
# init_database.sh - 데이터베이스 초기화 스크립트

echo "🗄️ Makenaide 데이터베이스 초기화 시작"

# 환경 변수 설정
export PG_HOST="$DB_ENDPOINT"
export PG_PORT="$DB_PORT"
export PG_DATABASE="$DB_NAME"
export PG_USER="$DB_USERNAME"
export PG_PASSWORD="$DB_PASSWORD"

# Python 스크립트 실행
python3 init_db_pg.py

echo "✅ 데이터베이스 초기화 완료"
EOF

chmod +x init_database.sh

# 8. 결과 출력
echo ""
echo "🎉 RDS PostgreSQL 설정 완료!"
echo "=================================="
echo "인스턴스 ID: $DB_INSTANCE_IDENTIFIER"
echo "엔드포인트: $DB_ENDPOINT"
echo "포트: $DB_PORT"
echo "데이터베이스: $DB_NAME"
echo "사용자: $DB_USERNAME"
echo "서브넷 그룹: $SUBNET_GROUP_NAME"
echo "파라미터 그룹: $PARAMETER_GROUP_NAME"
echo "리전: $REGION"
echo "=================================="
echo ""
echo "⚠️ 중요: 데이터베이스 비밀번호를 안전하게 보관하세요!"
echo ""
echo "다음 단계: IAM 역할 및 정책 설정"
echo "스크립트 실행: ./create_iam_roles.sh" 
