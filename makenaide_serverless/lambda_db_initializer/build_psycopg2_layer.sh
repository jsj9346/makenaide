#!/bin/bash
# AWS Lambda용 psycopg2 Layer 생성 스크립트
# Docker를 사용하여 AWS Lambda 환경과 동일한 환경에서 빌드

set -e

LAYER_NAME="makenaide-psycopg2-fixed"
REGION="ap-northeast-2"

echo "🐳 Docker를 사용한 psycopg2 Layer 생성 시작..."

# 임시 디렉토리 생성
mkdir -p /tmp/psycopg2-layer
cd /tmp/psycopg2-layer

# Dockerfile 생성
cat > Dockerfile << 'EOF'
FROM public.ecr.aws/lambda/python:3.11

# 필요한 패키지 설치
RUN yum update -y && \
    yum install -y gcc postgresql-devel python3-devel

# 작업 디렉토리 설정
WORKDIR /opt

# python 디렉토리 생성
RUN mkdir -p python

# psycopg2-binary 설치
RUN pip install psycopg2-binary==2.9.9 -t python/

# 불필요한 파일 제거 (크기 최적화)
RUN find python -type f -name "*.pyc" -delete && \
    find python -type f -name "*.pyo" -delete && \
    find python -type d -name "__pycache__" -exec rm -rf {} + || true

# 압축 파일 생성
RUN zip -r psycopg2-layer.zip python/
EOF

echo "📦 Docker 이미지 빌드 중..."
docker build -t psycopg2-layer-builder .

echo "📤 Layer 파일 추출 중..."
docker run --rm -v $(pwd):/output psycopg2-layer-builder cp /opt/psycopg2-layer.zip /output/

# Layer 크기 확인
LAYER_SIZE=$(du -h psycopg2-layer.zip | cut -f1)
echo "📊 Layer 크기: $LAYER_SIZE"

# AWS Lambda Layer 배포
echo "🚀 AWS Lambda Layer 배포 중..."
aws lambda publish-layer-version \
    --layer-name $LAYER_NAME \
    --description "psycopg2-binary built with Docker for AWS Lambda Python 3.11" \
    --zip-file fileb://psycopg2-layer.zip \
    --compatible-runtimes python3.11 \
    --region $REGION

echo "✅ psycopg2 Layer 생성 완료!"
echo "📍 Layer명: $LAYER_NAME"

# 정리
cd ..
rm -rf /tmp/psycopg2-layer

echo "🔧 이제 Lambda 함수에 새 Layer를 적용하세요:"
echo "aws lambda update-function-configuration --function-name makenaide-db-initializer --layers arn:aws:lambda:$REGION:901361833359:layer:$LAYER_NAME:1 --region $REGION"