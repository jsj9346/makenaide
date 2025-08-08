#!/bin/bash
# 시간대 분석 모듈을 Lambda 레이어로 패키징

echo "🌏 TimezoneMarketAnalyzer Lambda 레이어 패키징 시작..."

# 작업 디렉토리 생성
mkdir -p lambda_layer/python
cd lambda_layer

# timezone_market_analyzer.py 복사
cp ../timezone_market_analyzer.py python/

# pytz 라이브러리 설치 (시간대 변환에 필요)
pip install pytz -t python/

# ZIP 파일 생성
zip -r timezone_analyzer_layer.zip python/

# 파일 크기 확인
echo "📦 패키지 생성 완료:"
ls -lh timezone_analyzer_layer.zip

# 원본 위치로 이동
mv timezone_analyzer_layer.zip ../

# 작업 디렉토리 정리
cd ..
rm -rf lambda_layer

echo "✅ Lambda 레이어 패키징 완료: timezone_analyzer_layer.zip"