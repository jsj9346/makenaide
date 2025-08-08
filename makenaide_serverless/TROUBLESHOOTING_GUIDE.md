# 🚨 Makenaide 트러블슈팅 가이드

## 📋 문제 해결 개요

### 🎯 트러블슈팅 철학
- **체계적 접근**: 증상 → 원인 → 해결 → 검증의 4단계 프로세스
- **예방 우선**: 문제 해결과 동시에 재발 방지 조치 적용
- **문서화**: 모든 문제와 해결책을 상세히 기록
- **학습**: 각 문제를 통한 시스템 개선 기회로 활용

---

## 🔥 Critical Issues (P0) - 즉시 해결 필요

### 🚨 **Lambda 함수 완전 실패**

#### 증상
- Lambda 함수가 응답하지 않음
- HTTP 500 에러 지속 발생
- CloudWatch에 함수 로그 없음

#### 진단 단계
```bash
# 1. Lambda 함수 상태 확인
aws lambda get-function --function-name makenaide-data-collector

# 2. 최근 에러 로그 확인
aws logs filter-log-events \
    --log-group-name /aws/lambda/makenaide-data-collector \
    --start-time $(date -d '1 hour ago' +%s)000

# 3. Lambda Layer 연결 상태 확인
aws lambda get-function-configuration --function-name makenaide-data-collector | grep -A 5 "Layers"
```

#### 해결 방법
```bash
# 방법 1: 함수 재배포
./deploy_optimized_lambda.sh makenaide-data-collector

# 방법 2: Layer 재연결
LAYER_ARN="arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1"
aws lambda update-function-configuration \
    --function-name makenaide-data-collector \
    --layers $LAYER_ARN

# 방법 3: 환경 변수 복구
aws lambda update-function-configuration \
    --function-name makenaide-data-collector \
    --environment Variables='{
        "DB_HOST":"makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com",
        "DB_PORT":"5432",
        "DB_NAME":"makenaide",
        "DB_USER":"postgres"
    }'
```

### 🗄️ **RDS 연결 완전 실패**

#### 증상
- 모든 Lambda에서 DB 연결 실패
- "Can't create a connection to host" 에러
- 데이터 저장/조회 불가

#### 진단 단계
```bash
# 1. RDS 인스턴스 상태 확인
aws rds describe-db-instances --db-instance-identifier makenaide

# 2. 보안 그룹 설정 확인
aws ec2 describe-security-groups --group-ids sg-0357846ae2bbac7c6

# 3. 네트워크 연결성 테스트
aws lambda invoke --function-name makenaide-db-initializer \
    --payload '{"validate_only": true}' /tmp/db_connection_test.json
```

#### 해결 방법
```bash
# 방법 1: RDS 재시작
aws rds reboot-db-instance --db-instance-identifier makenaide

# 방법 2: 보안 그룹 수정 (0.0.0.0/0 허용)
aws ec2 authorize-security-group-ingress \
    --group-id sg-0357846ae2bbac7c6 \
    --protocol tcp \
    --port 5432 \
    --cidr 0.0.0.0/0

# 방법 3: RDS 강제 시작 (중지된 경우)
aws rds start-db-instance --db-instance-identifier makenaide
```

### 💸 **API 할당량 초과**

#### 증상
- Upbit API 429 에러 (Too Many Requests)
- 데이터 수집 중단
- "API rate limit exceeded" 메시지

#### 진단 단계
```bash
# API 호출 로그 분석
aws logs filter-log-events \
    --log-group-name /aws/lambda/makenaide-data-collector \
    --filter-pattern "429" \
    --start-time $(date -d '1 hour ago' +%s)000
```

#### 해결 방법
```python
# 긴급 패치: API 호출 간격 증가
import time

class EmergencyRateLimiter:
    def __init__(self):
        self.last_call = 0
        self.min_interval = 0.2  # 200ms로 증가 (기본 50ms)
    
    def wait_if_needed(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()

# 임시 조치: 데이터 수집 빈도 감소
# lambda_data_collector에서 count=3을 count=1로 변경
```

---

## ⚠️ High Priority Issues (P1) - 4시간 내 해결

### 🐌 **Lambda 성능 저하**

#### 증상
- 콜드 스타트 시간 >3초 (정상: <1초)
- 웜 스타트 시간 >0.1초 (정상: <0.03초)
- 타임아웃 에러 간헐적 발생

#### 진단 단계
```bash
# CloudWatch 메트릭 확인
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Duration \
    --dimensions Name=FunctionName,Value=makenaide-data-collector \
    --start-time $(date -d '1 day ago' --iso-8601) \
    --end-time $(date --iso-8601) \
    --period 3600 \
    --statistics Average,Maximum
```

#### 해결 방법
```bash
# 1. 메모리 할당 최적화
aws lambda update-function-configuration \
    --function-name makenaide-data-collector \
    --memory-size 512  # 256MB에서 512MB로 증가

# 2. 타임아웃 조정
aws lambda update-function-configuration \
    --function-name makenaide-data-collector \
    --timeout 30  # 15초에서 30초로 증가

# 3. 지연 로딩 최적화 확인
grep -n "import" /path/to/lambda_function.py
# 모든 import가 함수 내부에 있는지 확인
```

### 📊 **데이터 품질 문제**

#### 증상
- 티커 데이터 누락
- OHLCV 데이터 불일치
- 타임스탬프 오류

#### 진단 단계
```sql
-- PostgreSQL에서 데이터 품질 점검
-- 1. 최근 24시간 데이터 확인
SELECT 
    symbol,
    last_updated,
    EXTRACT(EPOCH FROM (NOW() - last_updated))/3600 as hours_ago
FROM tickers 
WHERE last_updated < NOW() - INTERVAL '24 hours';

-- 2. 중복 데이터 확인
SELECT symbol, COUNT(*) 
FROM tickers 
GROUP BY symbol 
HAVING COUNT(*) > 1;

-- 3. NULL 값 확인
SELECT 
    COUNT(*) as total_records,
    COUNT(korean_name) as korean_name_count,
    COUNT(english_name) as english_name_count
FROM tickers;
```

#### 해결 방법
```python
# 데이터 정리 스크립트
import psycopg2
from datetime import datetime, timedelta

def cleanup_ticker_data():
    """티커 데이터 정리 및 복구"""
    conn = psycopg2.connect(
        host="makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com",
        database="makenaide",
        user="postgres",
        password="your_password"
    )
    
    cursor = conn.cursor()
    
    # 1. 중복 제거
    cursor.execute("""
        DELETE FROM tickers a USING tickers b 
        WHERE a.id < b.id AND a.symbol = b.symbol
    """)
    
    # 2. 오래된 데이터 업데이트 마킹
    cursor.execute("""
        UPDATE tickers 
        SET is_active = false 
        WHERE last_updated < NOW() - INTERVAL '7 days'
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("✅ 데이터 정리 완료")

# Lambda 함수에서 실행
aws lambda invoke --function-name makenaide-scanner \
    --payload '{"action": "full_scan", "force_update": true}' response.json
```

---

## 📝 Medium Priority Issues (P2) - 24시간 내 해결

### 💰 **비정상적인 비용 증가**

#### 증상
- AWS 비용이 예상보다 높음 (>$50/월)
- Lambda 실행 횟수 급증
- RDS 사용량 비정상적 증가

#### 진단 단계
```bash
# 1. Lambda 실행 횟수 확인
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Invocations \
    --dimensions Name=FunctionName,Value=makenaide-data-collector \
    --start-time $(date -d '7 days ago' --iso-8601) \
    --end-time $(date --iso-8601) \
    --period 86400 \
    --statistics Sum

# 2. RDS 연결 수 확인
aws cloudwatch get-metric-statistics \
    --namespace AWS/RDS \
    --metric-name DatabaseConnections \
    --dimensions Name=DBInstanceIdentifier,Value=makenaide \
    --start-time $(date -d '1 day ago' --iso-8601) \
    --end-time $(date --iso-8601) \
    --period 3600 \
    --statistics Average,Maximum
```

#### 해결 방법
```bash
# 1. Lambda 실행 빈도 조정 (EventBridge 규칙 확인)
aws events list-rules --name-prefix makenaide

# 2. RDS 자동 중지 설정 (개발 환경)
aws rds stop-db-instance --db-instance-identifier makenaide

# 3. CloudWatch 로그 보존 기간 단축
aws logs put-retention-policy \
    --log-group-name /aws/lambda/makenaide-data-collector \
    --retention-in-days 7
```

### 🔒 **보안 경고**

#### 증상
- AWS 보안 알림 수신
- 비정상적인 접근 패턴
- API 키 남용 의심

#### 진단 단계
```bash
# 1. CloudTrail 이벤트 확인
aws cloudtrail lookup-events \
    --lookup-attributes AttributeKey=EventName,AttributeValue=AssumeRole \
    --start-time $(date -d '1 day ago' --iso-8601) \
    --end-time $(date --iso-8601)

# 2. IAM 역할 권한 검토
aws iam get-role --role-name lambda-execution-role
aws iam list-attached-role-policies --role-name lambda-execution-role
```

#### 해결 방법
```bash
# 1. API 키 재생성 (Upbit)
# Upbit 웹사이트에서 API 키 재발급

# 2. IAM 권한 최소화
aws iam create-policy \
    --policy-name MakenaideLambdaMinimal \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream", 
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }'

# 3. 환경 변수 암호화
aws lambda update-function-configuration \
    --function-name makenaide-data-collector \
    --kms-key-arn arn:aws:kms:ap-northeast-2:901361833359:key/your-key-id
```

---

## 🔧 Low Priority Issues (P3) - 1주일 내 해결

### 📈 **모니터링 개선**

#### 문제
- 알람이 너무 자주 발생
- 중요하지 않은 메트릭 알림
- 대시보드 가독성 부족

#### 해결책
```bash
# 알람 임계값 조정
aws cloudwatch put-metric-alarm \
    --alarm-name "Makenaide-Lambda-Error-Rate-Adjusted" \
    --alarm-description "조정된 에러율 알람" \
    --metric-name Errors \
    --namespace AWS/Lambda \
    --statistic Sum \
    --period 900 \
    --threshold 5 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 2 \
    --treat-missing-data notBreaching
```

### 📝 **로깅 최적화**

#### 문제
- 로그 메시지 불일치
- 디버깅 정보 부족
- 로그 비용 증가

#### 해결책
```python
# 통합 로깅 표준
import logging
import json
from datetime import datetime

class MakenaideLogs:
    """Makenaide 표준 로깅"""
    
    def __init__(self, function_name):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.function_name = function_name
    
    def log_start(self, event):
        """함수 시작 로그"""
        self.logger.info(f"🚀 {self.function_name} 시작", extra={
            'function': self.function_name,
            'event': event,
            'timestamp': datetime.now().isoformat()
        })
    
    def log_success(self, result, duration):
        """성공 로그"""
        self.logger.info(f"✅ {self.function_name} 완료: {duration:.3f}초", extra={
            'function': self.function_name,
            'status': 'success',
            'duration': duration,
            'result_summary': str(result)[:200]
        })
    
    def log_error(self, error, duration):
        """에러 로그"""
        self.logger.error(f"❌ {self.function_name} 실패: {error}", extra={
            'function': self.function_name,
            'status': 'error',
            'duration': duration,
            'error': str(error)
        })
```

---

## 📋 일반적인 문제 해결 체크리스트

### 🔍 **1단계: 기본 진단**
```yaml
시스템_상태_확인:
  - [ ] AWS 서비스 상태 페이지 확인
  - [ ] RDS 인스턴스 상태 확인 (available/stopped/starting)
  - [ ] Lambda 함수 활성 상태 확인
  - [ ] 보안 그룹 규칙 확인
  - [ ] 네트워크 연결성 테스트

로그_분석:
  - [ ] CloudWatch 최신 로그 확인
  - [ ] 에러 패턴 분석
  - [ ] 타임스탬프 기반 이벤트 순서 확인
  - [ ] 관련 서비스 로그 상호 참조

메트릭_검토:
  - [ ] 성능 지표 임계값 확인
  - [ ] 리소스 사용률 분석
  - [ ] 에러율 및 성공률 계산
  - [ ] 비용 증가 패턴 분석
```

### 🔧 **2단계: 임시 조치**
```yaml
즉시_조치:
  - [ ] 영향 범위 격리
  - [ ] 자동화된 복구 시도
  - [ ] 수동 백업 생성
  - [ ] 사용자 알림 (필요시)

성능_복구:
  - [ ] 리소스 할당 임시 증가
  - [ ] 타임아웃 값 조정
  - [ ] 재시도 로직 활성화
  - [ ] 캐시 플러시

데이터_보호:
  - [ ] 현재 데이터 상태 스냅샷
  - [ ] 트랜잭션 롤백 준비
  - [ ] 데이터 일관성 확인
  - [ ] 백업 무결성 검증
```

### 🎯 **3단계: 근본 원인 분석**
```yaml
원인_분석:
  - [ ] 로그 타임라인 재구성
  - [ ] 외부 의존성 영향 분석
  - [ ] 코드 변경 이력 검토
  - [ ] 인프라 변경 이력 검토

재현_테스트:
  - [ ] 동일 조건 재현 시도
  - [ ] 다양한 시나리오 테스트
  - [ ] 부하 테스트 실시
  - [ ] 경계값 테스트

영향_평가:
  - [ ] 비즈니스 영향도 평가
  - [ ] 데이터 손실 여부 확인
  - [ ] 보안 영향 분석
  - [ ] 규정 준수 영향 검토
```

### ✅ **4단계: 영구 해결 및 예방**
```yaml
영구_해결:
  - [ ] 근본 원인 제거
  - [ ] 코드/설정 수정
  - [ ] 인프라 개선
  - [ ] 프로세스 개선

예방_조치:
  - [ ] 모니터링 강화
  - [ ] 알람 임계값 조정
  - [ ] 자동화 개선
  - [ ] 문서화 업데이트

검증_및_배포:
  - [ ] 테스트 환경 검증
  - [ ] 단계적 배포
  - [ ] 성능 모니터링
  - [ ] 롤백 계획 준비
```

---

## 📞 **긴급 대응 프로토콜**

### 🚨 **P0 Critical - 즉시 대응**
```yaml
대응_시간: 15분 이내
대응_절차:
  1. 영향 범위 즉시 격리
  2. 자동 복구 시스템 활성화
  3. 수동 개입으로 서비스 복구
  4. 관련 팀 즉시 알림
  5. 상황 업데이트 (30분마다)

복구_목표:
  - RTO (Recovery Time Objective): 30분
  - RPO (Recovery Point Objective): 15분
```

### ⚠️ **P1 High - 4시간 내 대응**
```yaml
대응_시간: 1시간 이내
대응_절차:
  1. 문제 재현 및 분석
  2. 임시 해결책 적용
  3. 근본 원인 조사 시작
  4. 영향도 평가 및 보고
  5. 영구 해결책 계획 수립

복구_목표:
  - RTO: 4시간
  - RPO: 1시간
```

### 📝 **사후 분석 (Post-Mortem)**
```yaml
분석_범위:
  - [ ] 타임라인 상세 재구성
  - [ ] 근본 원인 상세 분석
  - [ ] 대응 과정 효과성 평가
  - [ ] 예방 조치 효과성 검토

개선_계획:
  - [ ] 프로세스 개선 방안
  - [ ] 기술적 개선 방안
  - [ ] 모니터링 개선 방안
  - [ ] 교육 및 훈련 계획

문서화:
  - [ ] 인시던트 리포트 작성
  - [ ] 해결책 문서 업데이트
  - [ ] 예방 가이드 작성
  - [ ] 지식 베이스 업데이트
```

---

## 🎓 **학습된 해결책 아카이브**

### 🏆 **성공 사례**

#### **Case 1: 99.6% 패키지 크기 최적화**
```yaml
문제: Lambda 패키지 크기 651KB로 콜드 스타트 지연
해결책: 지연 로딩 + Lambda Layer 분리
결과: 2.5KB로 99.6% 감소, 콜드 스타트 48% 개선
교훈: 의존성 분리와 지연 로딩의 강력한 효과
```

#### **Case 2: RDS 연결 타임아웃 해결**
```yaml
문제: "Can't create a connection to host" 에러
해결책: 보안 그룹에 0.0.0.0/0 규칙 추가
결과: 즉시 연결 복구, 안정적 DB 접근
교훈: 네트워크 설정의 중요성
```

#### **Case 3: psycopg2 의존성 문제**
```yaml
문제: Lambda에서 psycopg2 모듈 로드 실패
해결책: psycopg2-binary + pg8000 이중 드라이버
결과: 100% 가용성 확보, 안정적 DB 연결
교훈: Fallback 메커니즘의 중요성
```

### 📚 **지식 베이스**
- **Lambda 최적화**: 지연 로딩 > Layer 분리 > API 최적화
- **DB 연결**: 이중 드라이버 > 연결 풀링 > 재시도 로직
- **API 제한**: 속도 제한 > 지수적 백오프 > 캐싱
- **모니터링**: 예방적 알람 > 실시간 대시보드 > 자동 복구

---

**문서 정보**:
- **작성일**: 2025-08-05
- **버전**: v1.0
- **다음 업데이트**: 문제 발생시 즉시
- **최적화 달성**: 99.6% 패키지 크기 감소, 48% 성능 향상