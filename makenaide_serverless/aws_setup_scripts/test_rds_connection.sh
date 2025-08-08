#!/bin/bash
# test_rds_connection.sh - EC2에서 RDS 연결 테스트

echo "🔍 EC2에서 RDS 연결 테스트 시작"
echo "=================================="

# RDS 정보
RDS_HOST="makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com"
RDS_PORT="5432"
RDS_DB="makenaide"
RDS_USER="bruce"

echo "📍 연결 정보:"
echo "  Host: $RDS_HOST"
echo "  Port: $RDS_PORT"
echo "  Database: $RDS_DB"
echo "  User: $RDS_USER"
echo ""

# 1. 네트워크 연결 테스트
echo "1️⃣ 네트워크 연결 테스트 (telnet 대신 nc 사용)"
if command -v nc &> /dev/null; then
    if timeout 10 nc -z $RDS_HOST $RDS_PORT; then
        echo "✅ 네트워크 연결 성공"
    else
        echo "❌ 네트워크 연결 실패"
        exit 1
    fi
else
    echo "⚠️  nc 명령어가 없습니다. 설치 필요: sudo yum install -y nc"
fi

echo ""

# 2. PostgreSQL 클라이언트 확인
echo "2️⃣ PostgreSQL 클라이언트 확인"
if command -v psql &> /dev/null; then
    echo "✅ psql 클라이언트 설치됨"
    psql --version
else
    echo "❌ psql 클라이언트가 설치되지 않았습니다."
    echo "설치 명령어:"
    echo "  Amazon Linux 2: sudo yum install -y postgresql"
    echo "  Ubuntu: sudo apt-get install -y postgresql-client"
    exit 1
fi

echo ""

# 3. 환경변수 파일 확인
echo "3️⃣ 환경변수 파일 확인"
if [ -f "../.env" ]; then
    echo "✅ .env 파일 존재"
    if grep -q "DB_PASSWORD=" "../.env"; then
        echo "✅ DB_PASSWORD 설정됨"
    else
        echo "❌ DB_PASSWORD가 설정되지 않았습니다."
    fi
else
    echo "❌ .env 파일이 없습니다."
    echo "env.aws.template를 복사하여 .env 파일을 생성하세요."
fi

echo ""

# 4. 실제 데이터베이스 연결 테스트
echo "4️⃣ 데이터베이스 연결 테스트"
echo "비밀번호를 입력하거나 .env 파일에서 읽어오세요."

# .env 파일에서 비밀번호 읽기 시도
if [ -f "../.env" ]; then
    source "../.env"
    if [ ! -z "$DB_PASSWORD" ] && [ "$DB_PASSWORD" != "REPLACE_WITH_ACTUAL_PASSWORD" ]; then
        echo "비밀번호를 .env에서 읽어옵니다..."
        export PGPASSWORD="$DB_PASSWORD"
        
        if psql -h $RDS_HOST -p $RDS_PORT -U $RDS_USER -d $RDS_DB -c "SELECT version();" 2>/dev/null; then
            echo "✅ 데이터베이스 연결 성공!"
            echo ""
            echo "📊 데이터베이스 정보:"
            psql -h $RDS_HOST -p $RDS_PORT -U $RDS_USER -d $RDS_DB -c "SELECT version();"
            echo ""
            psql -h $RDS_HOST -p $RDS_PORT -U $RDS_USER -d $RDS_DB -c "\l"
        else
            echo "❌ 데이터베이스 연결 실패"
            echo "비밀번호나 권한을 확인하세요."
        fi
        unset PGPASSWORD
    else
        echo "⚠️  .env 파일에 올바른 DB_PASSWORD가 설정되지 않았습니다."
        echo "수동으로 연결 테스트하려면:"
        echo "psql -h $RDS_HOST -p $RDS_PORT -U $RDS_USER -d $RDS_DB"
    fi
else
    echo "수동으로 연결 테스트하려면:"
    echo "psql -h $RDS_HOST -p $RDS_PORT -U $RDS_USER -d $RDS_DB"
fi

echo ""
echo "🎉 연결 테스트 완료!" 