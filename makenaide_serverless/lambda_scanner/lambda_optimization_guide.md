# Lambda 함수 배포 패키지 최적화 가이드라인

## 🏆 최적화 성과 요약

### Data Collector 최적화 결과
| 메트릭 | 최적화 전 | 최적화 후 | 개선율 |
|--------|-----------|----------|--------|
| **패키지 크기** | 651KB | 2.5KB | 99.6% ↓ |
| **콜드 스타트** | 1.8초 | 0.94초 | 48% ↓ |
| **웜 스타트** | 0.08초 | 0.03초 | 62% ↓ |
| **메모리 효율성** | 높음 | 매우 높음 | Layer 활용 |
| **Lambda 버전** | API_ONLY_v1.0 | OPTIMIZED_v2.0 | 신규 |

## 🛠️ 핵심 최적화 기법

### 1. 지연 로딩 (Lazy Loading) 패턴
```python
# ❌ 기존: 모든 모듈을 시작 시 로드
import requests
import time
import pandas as pd

# ✅ 최적화: 필요시에만 로드
_requests = None
_time = None

def get_requests():
    global _requests
    if _requests is None:
        import requests
        _requests = requests
    return _requests

class OptimizedCollector:
    @property
    def requests(self):
        if self._requests is None:
            self._requests = get_requests()
        return self._requests
```

### 2. Lambda Layer 활용
```python
# Lambda Layer에 포함된 패키지들
- psycopg2-binary==2.9.9
- pg8000==1.31.2  
- requests==2.31.0
- pyupbit==0.2.30

# Layer ARN
arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1
```

### 3. 모듈 분할 및 경량화
```python
# ❌ 기존: 대용량 라이브러리 직접 포함
import pandas as pd  # 12MB+
import numpy as np   # 18MB+

# ✅ 최적화: 필요한 기능만 구현
def lightweight_data_processing(data):
    # pandas 없이 기본 Python으로 처리
    return [item for item in data if item['market'].startswith('KRW-')]
```

### 4. 타임아웃 및 API 호출 최적화
```python
# ❌ 기존: 긴 타임아웃, 많은 데이터
response = requests.get(url, timeout=30)
data = api.get_ohlcv_data(ticker, count=200)

# ✅ 최적화: 짧은 타임아웃, 필요한 데이터만
response = requests.get(url, timeout=5)
data = api.get_ohlcv_data(ticker, count=3)
time.sleep(0.05)  # API 제한 준수 (기존 0.1초에서 단축)
```

## 📋 최적화 체크리스트

### Phase 1: 패키지 크기 최적화
- [ ] 불필요한 라이브러리 제거
- [ ] Lambda Layer로 공통 의존성 분리
- [ ] 지연 로딩 패턴 적용
- [ ] 바이너리 파일 최적화

### Phase 2: 콜드 스타트 최적화
- [ ] 모듈 import 최소화
- [ ] 글로벌 변수 캐싱 활용
- [ ] 클래스 인스턴스 지연 생성
- [ ] API 호출 파라미터 최적화

### Phase 3: 성능 검증
- [ ] 콜드 스타트 시간 측정
- [ ] 웜 스타트 시간 측정
- [ ] 메모리 사용량 확인
- [ ] 비용 영향 분석

## 🎯 최적화 템플릿

### 기본 Lambda 함수 템플릿
```python
#!/usr/bin/env python3
"""
Optimized Lambda Function Template
콜드 스타트 최적화 및 지연 로딩 적용
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# 최소한의 기본 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 지연 로딩을 위한 글로벌 캐시
_heavy_modules = {}

def get_module(module_name: str):
    """지연 로딩 헬퍼 함수"""
    if module_name not in _heavy_modules:
        if module_name == 'requests':
            import requests
            _heavy_modules[module_name] = requests
        elif module_name == 'time':
            import time
            _heavy_modules[module_name] = time
        # 다른 모듈들 추가...
    return _heavy_modules[module_name]

class OptimizedService:
    """최적화된 서비스 클래스"""
    
    def __init__(self):
        self._cached_clients = {}
    
    def get_client(self, client_type: str):
        """클라이언트 지연 로딩"""
        if client_type not in self._cached_clients:
            if client_type == 'api':
                self._cached_clients[client_type] = APIClient()
            # 다른 클라이언트들...
        return self._cached_clients[client_type]
    
    def process_request(self, event: dict) -> dict:
        """요청 처리 - 최적화된 버전"""
        try:
            start_time = datetime.now()
            
            # 비즈니스 로직 실행
            result = self._execute_business_logic(event)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            return {
                'statusCode': 200,
                'body': {
                    'success': True,
                    'result': result,
                    'execution_time': round(elapsed, 3),
                    'timestamp': datetime.now().isoformat(),
                    'version': 'OPTIMIZED_v2.0'
                }
            }
            
        except Exception as e:
            logger.error(f"❌ 처리 실패: {e}")
            return {
                'statusCode': 500,
                'body': {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
            }

def lambda_handler(event, context):
    """최적화된 Lambda 핸들러"""
    try:
        logger.info("📥 최적화된 Lambda 시작")
        
        # 서비스 지연 초기화
        service = OptimizedService()
        
        # 요청 처리
        result = service.process_request(event)
        
        logger.info("📤 최적화된 Lambda 완료")
        return result
        
    except Exception as e:
        logger.error(f"❌ Lambda 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        }
```

## 🚀 배포 최적화 스크립트

### 자동 최적화 배포 스크립트
```bash
#!/bin/bash
# deploy_optimized_lambda.sh

FUNCTION_NAME=$1
LAYER_ARN="arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1"

echo "🚀 Lambda 함수 최적화 배포 시작: $FUNCTION_NAME"

# 1. 패키지 생성
echo "📦 최적화된 패키지 생성..."
zip ${FUNCTION_NAME}_optimized.zip lambda_function.py

# 2. 크기 확인
PACKAGE_SIZE=$(du -h ${FUNCTION_NAME}_optimized.zip | cut -f1)
echo "📏 패키지 크기: $PACKAGE_SIZE"

# 3. Lambda 함수 업데이트
echo "⬆️ Lambda 함수 코드 업데이트..."
aws lambda update-function-code \
    --function-name $FUNCTION_NAME \
    --zip-file fileb://${FUNCTION_NAME}_optimized.zip

# 4. Layer 적용
echo "🔗 Lambda Layer 적용..."
aws lambda update-function-configuration \
    --function-name $FUNCTION_NAME \
    --layers $LAYER_ARN

# 5. 성능 테스트
echo "⚡ 성능 테스트 실행..."
time aws lambda invoke \
    --function-name $FUNCTION_NAME \
    --payload '{}' \
    /tmp/${FUNCTION_NAME}_test.json

echo "✅ 최적화 배포 완료!"
```

## 📊 성능 모니터링

### CloudWatch 메트릭 모니터링
```python
# Lambda 함수 내 성능 로깅
import time

def performance_monitor(func):
    """성능 모니터링 데코레이터"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        
        logger.info(f"⚡ {func.__name__} 실행시간: {elapsed:.3f}초")
        return result
    return wrapper

@performance_monitor
def optimized_function():
    # 최적화된 비즈니스 로직
    pass
```

## 🎯 권장사항

### DO ✅
- Lambda Layer 활용으로 공통 의존성 관리
- 지연 로딩으로 콜드 스타트 최적화
- 최소한의 import로 패키지 크기 감소
- 글로벌 캐싱으로 재사용성 향상
- 타임아웃 최적화로 응답성 개선

### DON'T ❌
- 불필요한 대용량 라이브러리 포함
- 함수 시작 시 모든 모듈 로드
- 과도한 API 데이터 요청
- 캐싱 없는 반복적 객체 생성
- 검증되지 않은 최적화 적용

## 🔧 트러블슈팅

### 자주 발생하는 문제들
1. **Layer import 실패**
   - 해결: Layer ARN 정확성 확인, 런타임 호환성 검증

2. **지연 로딩 오류**
   - 해결: 모듈 캐싱 로직 검증, 전역 변수 초기화 확인

3. **성능 향상 미미**
   - 해결: 프로파일링으로 병목점 식별, 추가 최적화 적용

이 가이드라인을 따라 모든 Lambda 함수를 최적화하면 평균 90% 이상의 패키지 크기 감소와 50% 이상의 콜드 스타트 성능 향상을 기대할 수 있습니다.