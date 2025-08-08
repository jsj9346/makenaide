# 🚀 Lambda Functions API Documentation

## 📊 함수 개요 및 최적화 현황

| 함수명 | 역할 | 크기 | 메모리 | 최적화 | Layer | 상태 |
|--------|------|------|--------|--------|-------|------|
| **makenaide-data-collector** | 데이터 수집 | 2.5KB | 512MB | 🔥 99.6%↓ | ✅ Core | 운영 |
| **makenaide-scanner** | 티커 스캐닝 | 4.7KB | 256MB | ✅ 완료 | ✅ Core | 운영 |
| **makenaide-db-initializer** | DB 초기화 | 3KB | 256MB | ✅ 완료 | ✅ Core | 운영 |
| **makenaide-integrated-orchestrator** | 통합 제어 | 7.7KB | 512MB | ⏳ 대기 | ❌ 없음 | 대기 |

---

## 🔥 makenaide-data-collector

### 📝 **함수 정보**
- **ARN**: `arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-data-collector`
- **Runtime**: Python 3.11
- **Handler**: `lambda_function.lambda_handler`
- **Timeout**: 900초 (15분)
- **Memory**: 512MB
- **Layer**: makenaide-core-layer:1

### 🎯 **주요 기능**
Upbit API로부터 암호화폐 시장 데이터를 수집하는 최적화된 Lambda 함수

### 📥 **Input Specification**
```json
{
  "collection_type": "api_test|market_list|ohlcv_simple",
  "tickers": ["KRW-BTC", "KRW-ETH", ...],  // Optional
  "count": 3  // Optional, OHLCV 데이터 개수 (기본: 3)
}
```

#### **Collection Types**
- **`api_test`**: API 연결 테스트 (기본값)
- **`market_list`**: 전체 마켓 리스트 조회
- **`ohlcv_simple`**: 지정된 티커의 OHLCV 데이터 수집

### 📤 **Output Specification**
```json
{
  "statusCode": 200,
  "body": {
    "success": true,
    "collection_type": "api_test",
    "processed_tickers": 0,
    "execution_time": 0.937,
    "results": {
      "api_connection": "success",
      "market_count": 10,
      "sample_markets": [
        {
          "market": "KRW-WAXP",
          "korean_name": "왁스", 
          "english_name": "WAX"
        }
      ],
      "optimization_applied": true
    },
    "timestamp": "2025-08-05T01:06:36.063911",
    "lambda_version": "OPTIMIZED_v2.0",
    "optimization": "cold_start_optimized"
  }
}
```

### ⚡ **성능 최적화**
- **지연 로딩**: requests, time 모듈 필요시 로드
- **API 최적화**: 타임아웃 8초, 데이터 개수 3개로 제한
- **콜드 스타트**: 1.8초 → 0.94초 (48% 향상)
- **패키지 크기**: 651KB → 2.5KB (99.6% 감소)

### 🧪 **테스트 예제**
```bash
# API 연결 테스트
aws lambda invoke --function-name makenaide-data-collector \
  --payload '{"collection_type": "api_test"}' response.json

# 마켓 리스트 조회
aws lambda invoke --function-name makenaide-data-collector \
  --payload '{"collection_type": "market_list"}' response.json

# OHLCV 데이터 수집
aws lambda invoke --function-name makenaide-data-collector \
  --payload '{"collection_type": "ohlcv_simple", "tickers": ["KRW-BTC"]}' response.json
```

---

## 🔍 makenaide-scanner

### 📝 **함수 정보**
- **ARN**: `arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-scanner`
- **Runtime**: Python 3.11
- **Handler**: `lambda_function.lambda_handler`
- **Timeout**: 300초 (5분)
- **Memory**: 256MB
- **Layer**: makenaide-core-layer:1

### 🎯 **주요 기능**
Upbit API로부터 티커 정보를 스캔하고 PostgreSQL RDS에 저장하는 함수

### 📥 **Input Specification**
```json
{
  "action": "full_scan|quick_scan|status_check",
  "force_update": true,  // Optional
  "specific_tickers": ["KRW-BTC", "KRW-ETH"]  // Optional
}
```

#### **Action Types**
- **`full_scan`**: 전체 티커 스캔 및 DB 업데이트 (기본값)
- **`quick_scan`**: 변경된 티커만 스캔
- **`status_check`**: 현재 스캔 상태 확인

### 📤 **Output Specification**
```json
{
  "statusCode": 200,
  "body": {
    "success": true,
    "result": {
      "action": "full_scan",
      "status": "success",
      "update_result": {
        "action": "update_tickers",
        "status": "success", 
        "new_tickers": 0,
        "updated_tickers": 183,
        "total_active": 183,
        "blacklisted": 2,
        "timestamp": "2025-08-05T00:50:04.833350"
      },
      "blacklist_result": {
        "action": "sync_blacklist",
        "status": "success",
        "blacklisted_count": 2,
        "updated_count": 2,
        "timestamp": "2025-08-05T00:50:04.834668"
      },
      "timestamp": "2025-08-05T00:50:04.836587"
    },
    "api_method": "direct_requests",
    "pyupbit_available": false,
    "pg8000_available": true,
    "psycopg2_available": true,
    "version": "PG8000_DIRECT_API_v1.1"
  }
}
```

### 🗄️ **Database Operations**
- **tickers 테이블**: 티커 정보 저장 및 업데이트
- **blacklist 관리**: 비활성 티커 블랙리스트 동기화
- **중복 방지**: UPSERT 패턴으로 데이터 무결성 보장

### ⚡ **최적화 특징**
- **이중 DB 드라이버**: psycopg2 + pg8000 fallback
- **직접 API 호출**: pyupbit 의존성 제거
- **스마트 업데이트**: 24시간 주기 자동 업데이트 로직

### 🧪 **테스트 예제**
```bash
# 전체 스캔 실행
echo '{"action": "full_scan"}' | base64 | \
aws lambda invoke --function-name makenaide-scanner \
  --payload file:///dev/stdin response.json

# 강제 업데이트
echo '{"action": "full_scan", "force_update": true}' | base64 | \
aws lambda invoke --function-name makenaide-scanner \
  --payload file:///dev/stdin response.json
```

---

## 🏗️ makenaide-db-initializer

### 📝 **함수 정보**
- **ARN**: `arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-db-initializer`
- **Runtime**: Python 3.11
- **Handler**: `db_init_aws_psycopg2.lambda_handler`
- **Timeout**: 300초 (5분)
- **Memory**: 256MB
- **Layer**: makenaide-core-layer:1

### 🎯 **주요 기능**
PostgreSQL RDS 데이터베이스에 Makenaide 스키마 및 테이블을 초기화하는 함수

### 📥 **Input Specification**
```json
{
  "force_recreate": false,  // Optional
  "validate_only": false    // Optional
}
```

#### **Parameters**
- **`force_recreate`**: 기존 테이블 삭제 후 재생성
- **`validate_only`**: 테이블 존재 여부만 확인

### 📤 **Output Specification**
```json
{
  "statusCode": 200,
  "body": {
    "success": true,
    "message": "DB 스키마 초기화 성공",
    "schema_created": true,
    "validation": {
      "required_tables": ["performance_summary", "trade_log", "tickers"],
      "existing_tables": ["performance_summary", "trade_log", "tickers"],
      "all_present": true,
      "missing_tables": []
    },
    "psycopg2_available": true,
    "timestamp": "2025-08-05T00:50:12.441712",
    "version": "AWS_PSYCOPG2_v1.0"
  }
}
```

### 🗄️ **Database Schema**
```sql
-- 핵심 테이블 구조
CREATE TABLE tickers (
    symbol VARCHAR(20) PRIMARY KEY,
    korean_name VARCHAR(100),
    english_name VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE trade_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    symbol VARCHAR(20),
    side VARCHAR(10),
    amount DECIMAL(20,8),
    price DECIMAL(20,8),
    total_value DECIMAL(20,8)
);

CREATE TABLE performance_summary (
    id SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    total_value DECIMAL(20,8),
    profit_loss DECIMAL(20,8),
    roi_percentage DECIMAL(10,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### ⚡ **최적화 특징**
- **이중 드라이버 지원**: psycopg2 우선, pg8000 fallback
- **트랜잭션 안전성**: COMMIT/ROLLBACK 패턴
- **스키마 검증**: 테이블 존재 여부 자동 확인

### 🧪 **테스트 예제**
```bash
# 기본 초기화
aws lambda invoke --function-name makenaide-db-initializer \
  --payload '{}' response.json

# 강제 재생성
aws lambda invoke --function-name makenaide-db-initializer \
  --payload '{"force_recreate": true}' response.json

# 검증만 수행
aws lambda invoke --function-name makenaide-db-initializer \
  --payload '{"validate_only": true}' response.json
```

---

## ⏳ makenaide-integrated-orchestrator

### 📝 **함수 정보**
- **ARN**: `arn:aws:lambda:ap-northeast-2:901361833359:function:makenaide-integrated-orchestrator`
- **Runtime**: Python 3.11
- **Timeout**: 900초 (15분)
- **Memory**: 512MB
- **Layer**: ❌ 미적용 (최적화 대상)

### 🎯 **주요 기능**
다른 Lambda 함수들을 조율하고 전체 트레이딩 워크플로우를 관리하는 오케스트레이터

### ⚠️ **최적화 필요**
- **현재 상태**: 7.7KB, Layer 미적용
- **예상 개선**: 90% 패키지 크기 감소 가능
- **우선순위**: 중간 (기본 기능 안정화 후 진행)

---

## 🏗️ Lambda Layer Architecture

### 📦 **makenaide-core-layer:1**
- **ARN**: `arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1`
- **크기**: 3.9MB (50MB 제한의 8%)
- **Runtime**: Python 3.11
- **아키텍처**: x86_64

### 📚 **포함된 패키지**
```yaml
Core Dependencies:
  - psycopg2-binary==2.9.9    # PostgreSQL 드라이버 (바이너리)
  - pg8000==1.31.2           # Pure Python PostgreSQL 드라이버
  - requests==2.31.0         # HTTP 라이브러리
  - pyupbit==0.2.30          # Upbit API 라이브러리
  - python-dateutil==2.8.2   # 날짜/시간 처리

Supporting Libraries:
  - urllib3, certifi, charset-normalizer
  - idna, asn1crypto, scramp, six
```

### 🔄 **Fallback Pattern**
```python
# 이중 DB 드라이버 패턴 (모든 함수에서 사용)
try:
    import psycopg2
    DB_DRIVER = 'psycopg2'
except ImportError:
    import pg8000.native as pg8000
    DB_DRIVER = 'pg8000'
```

---

## 🎯 **Common Patterns & Best Practices**

### 🔄 **Error Handling Pattern**
```python
def lambda_handler(event, context):
    try:
        # 비즈니스 로직 실행
        result = process_request(event)
        
        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'result': result,
                'timestamp': datetime.now().isoformat(),
                'version': 'OPTIMIZED_v2.0'
            }
        }
        
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

### ⚡ **Performance Optimization Pattern**
```python
# 지연 로딩 글로벌 캐시
_heavy_modules = {}

def get_module(module_name: str):
    if module_name not in _heavy_modules:
        if module_name == 'requests':
            import requests
            _heavy_modules[module_name] = requests
    return _heavy_modules[module_name]

class OptimizedService:
    @property
    def requests(self):
        if self._requests is None:
            self._requests = get_module('requests')
        return self._requests
```

### 🗄️ **Database Connection Pattern**
```python
class DatabaseManager:
    def __init__(self):
        self.config = self._load_db_config()
        self.connection = None
        
    def get_connection(self):
        if self.connection is None:
            try:
                import psycopg2
                self.connection = psycopg2.connect(**self.config)
            except ImportError:
                import pg8000.native as pg8000
                self.connection = pg8000.Connection(**self.config)
        return self.connection
```

---

## 📊 **Monitoring & Observability**

### 📈 **CloudWatch Metrics**
- **Duration**: 함수 실행 시간
- **Errors**: 오류 발생 횟수  
- **Throttles**: 동시 실행 제한
- **Cold Starts**: 콜드 스타트 횟수

### 📝 **Logging Pattern**
```python
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 구조화된 로깅
logger.info(f"🚀 {function_name} 시작")
logger.info(f"📊 처리 완료: {processed_count}개")
logger.error(f"❌ 오류 발생: {error}")
logger.info(f"✅ {function_name} 완료: {elapsed:.3f}초")
```

### 🎯 **성능 메트릭**
```python
def performance_monitor(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.info(f"⚡ {func.__name__}: {elapsed:.3f}초")
        return result
    return wrapper
```

---

## 🚀 **Deployment Guide**

### 📦 **최적화된 배포 스크립트**
```bash
#!/bin/bash
# deploy_optimized_lambda.sh
FUNCTION_NAME=$1
LAYER_ARN="arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1"

# 1. 패키지 생성
zip ${FUNCTION_NAME}_optimized.zip lambda_function.py

# 2. 함수 업데이트
aws lambda update-function-code \
    --function-name $FUNCTION_NAME \
    --zip-file fileb://${FUNCTION_NAME}_optimized.zip

# 3. Layer 적용
aws lambda update-function-configuration \
    --function-name $FUNCTION_NAME \
    --layers $LAYER_ARN
```

### 🧪 **테스트 자동화**
```bash
# 전체 함수 테스트
for func in makenaide-data-collector makenaide-scanner makenaide-db-initializer; do
    echo "Testing $func..."
    aws lambda invoke --function-name $func --payload '{}' /tmp/${func}_test.json
    echo "✅ $func 테스트 완료"
done
```

---

**Last Updated**: 2025-08-05  
**API Version**: v2.0  
**Optimization Status**: 3/4 함수 최적화 완료 (99.6% 패키지 크기 감소 달성)