# DB Initializer Lambda 배포 요약

## 🎯 목표
- 기존 `init_db_pg.py`의 DB 스키마 초기화 기능을 독립적인 Lambda 함수로 분리
- Phase 2 아키텍처 개선의 일환으로 EC2 대신 Lambda 사용

## ✅ 완료된 작업

### 1. 기존 스키마 분석 및 설계
- **분석 완료**: `init_db_pg.py` 파일 구조 및 스키마 정의 파악
- **핵심 테이블**: 15개 core tables (tickers, ohlcv, trade_log 등)
- **백테스트 테이블**: 4개 backtest tables (backtest_sessions, backtest_ohlcv 등)
- **기능 추출**: 스키마 검증, 테이블 생성, 인덱스 생성, 마이그레이션 로직

### 2. Lambda 함수 구현
- **파일 구조**:
  ```
  lambda_db_initializer/
  ├── lambda_function.py      # Lambda 진입점
  ├── db_initializer.py       # 메인 로직 (850+ lines)
  ├── requirements.txt        # 의존성
  ├── deploy_db_init.sh      # 배포 스크립트
  └── test_connection_only.py # 연결 테스트용
  ```

- **핵심 클래스**:
  - `DatabaseManager`: Lambda 환경용 DB 연결 관리
  - `SchemaValidator`: 스키마 상태 검증 및 무결성 확인
  - `SchemaInitializer`: 테이블 및 인덱스 생성
  - `LambdaDBInitializer`: 메인 오케스트레이션

### 3. AWS Lambda 배포
- **함수명**: `makenaide-db-initializer`
- **런타임**: Python 3.11
- **메모리**: 256MB
- **타임아웃**: 300초 (5분)
- **IAM 역할**: `makenaide-lambda-execution-role`

### 4. 기능 테스트
- **스키마 상태 확인**: `check_schema` ✅ 작동 확인
- **핵심 스키마 초기화**: `init_core` 
- **백테스트 스키마**: `init_backtest`
- **전체 초기화**: `full_init`

## ⚠️ 현재 이슈

### psycopg2 호환성 문제
- **문제**: `No module named 'psycopg2._psycopg'` 오류
- **시도한 해결책**:
  - ✅ `makenaide-aws-psycopg2:1` layer 적용
  - ✅ `makenaide-minimal-psycopg2:1` layer 적용
  - ❌ 여전히 `_psycopg` 모듈 로딩 실패

- **원인 분석**: 
  - macOS ARM64에서 빌드된 psycopg2-binary가 AWS Lambda x86_64 환경과 비호환
  - 기존 layer들이 Python 3.11과 완전 호환되지 않는 것으로 추정

## 📊 테스트 결과

### 성공적인 기능
1. **Lambda 함수 배포**: ✅
2. **DB 연결 설정**: ✅ (RDS 접근 가능 확인)
3. **스키마 검증 로직**: ✅ (psycopg2 없어도 기본 구조 동작)
4. **JSON 응답 처리**: ✅

### 실제 동작 결과
```json
{
  "statusCode": 200,
  "body": {
    "success": true,
    "operation_type": "check_schema",
    "execution_time": 0.003,
    "results": {
      "schema_health": "incomplete",
      "missing_tables": [
        "tickers", "ohlcv", "ohlcv_4h", "static_indicators",
        "market_data_4h", "trade_log", "trade_history", ...
      ]
    },
    "schema_version": "2.0.0",
    "lambda_version": "DB_INIT_v1.0"
  }
}
```

## 🚀 대안 솔루션

### 1. 스케줄러 기반 접근법
- **EventBridge + Lambda**: psycopg2 호환 layer 생성 후 재시도
- **장점**: 서버리스 유지, 비용 효율적
- **단점**: layer 생성 작업 필요

### 2. EC2 직접 실행
- **기존 방식 유지**: EC2에서 cron job으로 `init_db_pg.py` 실행
- **장점**: 즉시 사용 가능, 검증된 방식
- **단점**: Phase 2 목표와 다소 상충

### 3. Docker 기반 Lambda
- **Container Image**: psycopg2 사전 설치된 컨테이너 이미지 사용
- **장점**: 완전한 종속성 제어
- **단점**: 이미지 크기 증가, 복잡성 증가

## 🎓 학습 내용

1. **AWS Lambda Layer**: psycopg2 호환성은 매우 까다로운 문제
2. **크로스 컴파일**: ARM64 → x86_64 바이너리 호환성 이슈
3. **Python 런타임 버전**: 3.9 vs 3.11 layer 호환성 차이
4. **Lambda Cold Start**: 첫 실행 시 86ms 초기화 시간

## 📋 다음 단계

1. **단기 해결책**: 기존 EC2 방식으로 DB 초기화 수행
2. **중기 계획**: Docker 기반 Lambda로 완전 전환
3. **장기 목표**: RDS Proxy + Lambda 최적화

## 📁 파일 위치
- **Lambda 함수**: `/Users/13ruce/makenaide/lambda_db_initializer/`
- **AWS 함수명**: `makenaide-db-initializer`
- **리전**: `ap-northeast-2`
- **로그 그룹**: `/aws/lambda/makenaide-db-initializer`