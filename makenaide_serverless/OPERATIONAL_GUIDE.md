# 🛠️ Makenaide 운영 가이드

## 📋 시스템 운영 개요

### 🎯 운영 철학
- **안정성 우선**: 시스템 안정성을 최우선으로 하는 운영
- **예방적 모니터링**: 문제 발생 전 사전 탐지 및 대응
- **투명한 로깅**: 모든 운영 활동의 완전한 추적성 보장
- **자동화 우선**: 수동 작업 최소화를 통한 휴먼 에러 방지

---

## 🚀 일일 운영 체크리스트

### 🌅 **Morning Checklist** (9:00 AM)
```yaml
시스템_상태_점검:
  - [ ] AWS RDS 인스턴스 상태 확인 ("available" 상태)
  - [ ] Lambda 함수 상태 확인 (4개 함수 모두 "Active")
  - [ ] Lambda Layer 연결 상태 확인 (makenaide-core-layer:1)
  - [ ] CloudWatch 대시보드 확인

데이터_무결성_점검:
  - [ ] PostgreSQL 연결 테스트 실행
  - [ ] 티커 데이터 최신성 확인 (24시간 이내 업데이트)
  - [ ] 트레이딩 로그 연속성 확인

성능_모니터링:
  - [ ] Lambda 콜드 스타트 횟수 확인 (<10회/일)
  - [ ] API 응답 시간 확인 (<2초 평균)
  - [ ] 에러율 확인 (<1% 목표)
```

### 🌆 **Evening Checklist** (6:00 PM)
```yaml
거래_성과_분석:
  - [ ] 일일 거래 성과 리포트 확인
  - [ ] ROI 추이 분석 (목표: 월 +5% 이상)
  - [ ] 리스크 지표 점검 (최대 손실 -8% 이내)

백업_및_보안:
  - [ ] 데이터베이스 백업 상태 확인
  - [ ] 보안 로그 이상 징후 점검
  - [ ] API 키 유효성 확인

다음날_준비:
  - [ ] 시장 일정 확인 (휴장일, 중요 이벤트)
  - [ ] 시스템 리소스 여유분 확인
  - [ ] 알람 설정 확인
```

---

## ⚡ 시스템 시작 & 종료 절차

### 🟢 **시스템 시작 절차**
```bash
# 1. RDS 인스턴스 시작
echo "🔄 RDS 인스턴스 시작..."
aws rds start-db-instance --db-instance-identifier makenaide

# 2. RDS 상태 확인 (약 3-5분 소요)
echo "⏳ RDS 시작 대기 중..."
aws rds wait db-instance-available --db-instance-identifier makenaide

# 3. Lambda 함수 워밍업 (콜드 스타트 방지)
echo "🔥 Lambda 함수 워밍업..."
for func in makenaide-data-collector makenaide-scanner makenaide-db-initializer; do
    aws lambda invoke --function-name $func --payload '{"warmup": true}' /tmp/${func}_warmup.json
    echo "✅ $func 워밍업 완료"
done

# 4. 데이터베이스 연결 테스트
echo "🗄️ 데이터베이스 연결 테스트..."
aws lambda invoke --function-name makenaide-db-initializer \
    --payload '{"validate_only": true}' /tmp/db_test.json

echo "🚀 Makenaide 시스템 시작 완료!"
```

### 🔴 **시스템 종료 절차**
```bash
# 1. 활성 거래 확인 및 안전 종료
echo "⚠️ 활성 거래 확인 중..."
# 진행 중인 거래가 있다면 완료 대기

# 2. 마지막 데이터 백업
echo "💾 최종 데이터 백업..."
pg_dump -h makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com \
    -U postgres -d makenaide > backup_$(date +%Y%m%d_%H%M%S).sql

# 3. RDS 인스턴스 중지 (비용 절약)
echo "💤 RDS 인스턴스 중지..."
aws rds stop-db-instance --db-instance-identifier makenaide

echo "🛑 Makenaide 시스템 안전 종료 완료!"
```

---

## 📊 모니터링 & 알람 설정

### 🎯 **핵심 모니터링 지표**
```yaml
Lambda_성능_지표:
  콜드_스타트:
    목표: <1초
    경고: >2초  
    위험: >5초
  
  에러율:
    목표: <0.5%
    경고: >1%
    위험: >5%
  
  실행_시간:
    목표: <30초
    경고: >60초
    위험: >120초

RDS_성능_지표:
  연결_성공률:
    목표: >99.9%
    경고: <99%
    위험: <95%
  
  쿼리_응답시간:
    목표: <100ms
    경고: >500ms
    위험: >1초

거래_성과_지표:
  월간_ROI:
    목표: >5%
    경고: <1%
    위험: <-5%
  
  최대_손실:
    안전: <5%
    경고: >8%
    위험: >15%
```

### 🔔 **CloudWatch 알람 설정**
```bash
# Lambda 에러율 알람
aws cloudwatch put-metric-alarm \
    --alarm-name "Makenaide-Lambda-Error-Rate" \
    --alarm-description "Lambda 함수 에러율 모니터링" \
    --metric-name Errors \
    --namespace AWS/Lambda \
    --statistic Sum \
    --period 300 \
    --threshold 10 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 2

# RDS 연결 실패 알람
aws cloudwatch put-metric-alarm \
    --alarm-name "Makenaide-RDS-Connection-Failures" \
    --alarm-description "RDS 연결 실패 모니터링" \
    --metric-name DatabaseConnections \
    --namespace AWS/RDS \
    --statistic Average \
    --period 300 \
    --threshold 0 \
    --comparison-operator LessThanThreshold \
    --evaluation-periods 1
```

### 📈 **커스텀 대시보드 구성**
```yaml
Makenaide_Dashboard:
  패널_1_시스템_상태:
    - Lambda 함수 상태 (4개)
    - RDS 인스턴스 상태
    - API 응답 시간
  
  패널_2_성능_지표:
    - 콜드 스타트 횟수
    - 평균 실행 시간
    - 에러 발생 횟수
  
  패널_3_거래_성과:
    - 일일 거래 횟수
    - 누적 수익률
    - 리스크 지표
  
  패널_4_비용_최적화:
    - Lambda 비용 추이
    - RDS 사용률
    - 최적화 효과
```

---

## 🔧 정기 유지보수 작업

### 📅 **주간 유지보수** (매주 일요일)
```yaml
코드_품질_점검:
  - [ ] 코드 리뷰 및 품질 점검
  - [ ] 테스트 커버리지 확인 (>80% 목표)
  - [ ] 보안 취약점 스캔

성능_최적화:
  - [ ] Lambda 최적화 효과 분석
  - [ ] 데이터베이스 쿼리 성능 점검
  - [ ] 캐싱 효율성 분석

데이터_정리:
  - [ ] 오래된 로그 데이터 아카이브 (90일 이상)
  - [ ] 임시 파일 정리
  - [ ] 백업 파일 정리 (30일 이상)
```

### 📅 **월간 유지보수** (매월 첫째 주)
```yaml
전략_성과_분석:
  - [ ] 월간 거래 성과 리포트 생성
  - [ ] 전략 효과성 분석
  - [ ] 파라미터 최적화 검토

시스템_업데이트:
  - [ ] 의존성 패키지 업데이트 검토
  - [ ] AWS 서비스 업데이트 적용
  - [ ] 보안 패치 적용

용량_계획:
  - [ ] 시스템 리소스 사용량 분석
  - [ ] 확장성 요구사항 검토
  - [ ] 비용 최적화 기회 분석
```

### 📅 **분기별 유지보수** (분기 첫째 주)
```yaml
아키텍처_리뷰:
  - [ ] 전체 시스템 아키텍처 검토
  - [ ] 성능 병목 지점 분석
  - [ ] 확장성 및 가용성 개선 방안

보안_감사:
  - [ ] 전체 보안 정책 검토
  - [ ] 접근 권한 감사
  - [ ] 침투 테스트 실시

비즈니스_성과_분석:
  - [ ] 분기별 ROI 분석
  - [ ] 전략 효과성 평가
  - [ ] 목표 달성도 검토
```

---

## 🔍 성능 튜닝 가이드

### ⚡ **Lambda 성능 최적화**
```python
# 최적화된 Lambda 모니터링 코드
import time
import logging
from functools import wraps

logger = logging.getLogger()

def performance_monitor(func):
    """성능 모니터링 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            
            logger.info(f"✅ {func.__name__}: {elapsed:.3f}초")
            
            # 성능 임계값 체크
            if elapsed > 30:  # 30초 초과시 경고
                logger.warning(f"⚠️ 긴 실행시간: {func.__name__} - {elapsed:.3f}초")
            
            return result
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ {func.__name__} 실패: {e} (실행시간: {elapsed:.3f}초)")
            raise
            
    return wrapper

# 사용 예제
@performance_monitor
def optimized_data_collection():
    """최적화된 데이터 수집 함수"""
    # 지연 로딩으로 필요시에만 모듈 임포트
    requests = get_cached_module('requests')
    
    # API 호출 최적화
    response = requests.get(url, timeout=5)  # 짧은 타임아웃
    return response.json()
```

### 🗄️ **데이터베이스 성능 최적화**
```sql
-- 인덱스 성능 분석 쿼리
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats 
WHERE schemaname = 'public';

-- 느린 쿼리 탐지
SELECT 
    query,
    mean_time,
    calls,
    total_time
FROM pg_stat_statements 
WHERE mean_time > 100  -- 100ms 이상
ORDER BY mean_time DESC;

-- 테이블 크기 및 사용량 분석
SELECT 
    relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_relation_size(relid)) AS table_size,
    n_tup_ins AS inserts,
    n_tup_upd AS updates,
    n_tup_del AS deletes
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;
```

### 📊 **API 호출 최적화**
```python
# 최적화된 API 클라이언트
class OptimizedUpbitAPI:
    """최적화된 Upbit API 클라이언트"""
    
    def __init__(self):
        self.session = None
        self.last_call_time = 0
        self.min_interval = 0.05  # 50ms 최소 간격
    
    def get_session(self):
        """세션 지연 로딩"""
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.timeout = 5  # 5초 타임아웃
        return self.session
    
    def rate_limit(self):
        """API 속도 제한"""
        current_time = time.time()
        elapsed = current_time - self.last_call_time
        
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()
    
    def safe_api_call(self, url, retries=3):
        """안전한 API 호출"""
        session = self.get_session()
        
        for attempt in range(retries):
            try:
                self.rate_limit()
                response = session.get(url)
                response.raise_for_status()
                return response.json()
                
            except Exception as e:
                logger.warning(f"API 호출 실패 (시도 {attempt + 1}/{retries}): {e}")
                if attempt == retries - 1:
                    raise
                time.sleep(0.1 * (attempt + 1))  # 지수적 백오프
```

---

## 🎯 비용 최적화 전략

### 💰 **현재 최적화 성과**
```yaml
Lambda_비용_절감:
  패키지_크기: 651KB → 2.5KB (99.6% 감소)
  실행_시간: 1.8초 → 0.94초 (48% 단축)
  예상_월간_절감: $50-100

RDS_비용_최적화:
  인스턴스_타입: db.t3.micro (프리티어)
  스토리지: 20GB (최소 요구사항)
  백업_기간: 7일 (필수 최소)
  
예상_총_월간_비용: $15-25 (최적화 전 $100-150)
```

### 📈 **추가 비용 절감 기회**
```yaml
Lambda_최적화:
  - [ ] 나머지 1개 함수 최적화 (makenaide-integrated-orchestrator)
  - [ ] 실행 빈도 최적화 (필요시에만 실행)
  - [ ] 메모리 할당 최적화 (과도한 메모리 할당 방지)

RDS_최적화:
  - [ ] 자동 시작/중지 스케줄링 (야간 중지)
  - [ ] 읽기 전용 복제본 제거 (개발 단계에서 불필요)
  - [ ] 스토리지 자동 증가 비활성화

네트워크_최적화:
  - [ ] VPC 엔드포인트 사용 (NAT 게이트웨이 비용 절약)
  - [ ] CloudWatch 로그 보존 기간 단축 (14일 → 7일)
```

### 🔄 **자동화된 비용 모니터링**
```bash
# 비용 알람 설정
aws cloudwatch put-metric-alarm \
    --alarm-name "Makenaide-Monthly-Cost" \
    --alarm-description "월간 비용 초과 알람" \
    --metric-name EstimatedCharges \
    --namespace AWS/Billing \
    --statistic Maximum \
    --period 86400 \
    --threshold 50 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1

# 비용 리포트 자동 생성 스크립트
#!/bin/bash
# generate_cost_report.sh
echo "📊 Makenaide 비용 리포트 생성 중..."

# Lambda 비용 조회
aws ce get-cost-and-usage \
    --time-period Start=2025-08-01,End=2025-08-05 \
    --granularity MONTHLY \
    --metrics "UnblendedCost" \
    --group-by Type=DIMENSION,Key=SERVICE \
    --filter '{"Dimensions":{"Key":"SERVICE","Values":["AWS Lambda"]}}' \
    > lambda_cost_report.json

# RDS 비용 조회
aws ce get-cost-and-usage \
    --time-period Start=2025-08-01,End=2025-08-05 \
    --granularity MONTHLY \
    --metrics "UnblendedCost" \
    --group-by Type=DIMENSION,Key=SERVICE \
    --filter '{"Dimensions":{"Key":"SERVICE","Values":["Amazon Relational Database Service"]}}' \
    > rds_cost_report.json

echo "✅ 비용 리포트 생성 완료"
```

---

## 📚 **운영 지식 베이스**

### 🔑 **핵심 운영 원칙**
1. **안전 우선**: 데이터 손실 방지가 최우선
2. **투명성**: 모든 운영 활동의 완전한 로깅
3. **자동화**: 수동 작업 최소화로 휴먼 에러 방지
4. **효율성**: 비용 효율적인 리소스 사용
5. **모니터링**: 사전 예방적 문제 탐지

### 📖 **주요 참고 문서**
- **[TROUBLESHOOTING_GUIDE.md](#)** - 문제 해결 가이드
- **[LAMBDA_FUNCTIONS_API.md](./LAMBDA_FUNCTIONS_API.md)** - Lambda API 문서
- **[OPTIMIZATION_ACHIEVEMENT_REPORT.md](./OPTIMIZATION_ACHIEVEMENT_REPORT.md)** - 최적화 성과
- **[PROJECT_INDEX.md](./PROJECT_INDEX.md)** - 프로젝트 전체 구조

### 🚨 **긴급 연락처**
```yaml
시스템_관리자:
  이름: "Claude Assistant"
  역할: "시스템 아키텍트 및 최적화 전문가"
  전문분야: "Lambda 최적화, AWS 인프라, PostgreSQL"

기술_지원:
  AWS_지원: "AWS Support (프리티어 기본 지원)"
  PostgreSQL_지원: "PostgreSQL 커뮤니티"
  Upbit_API: "Upbit 개발자 문서"
```

---

**문서 정보**:
- **작성일**: 2025-08-05
- **버전**: v1.0
- **다음 업데이트**: 2025-08-12 (주간 리뷰)
- **최적화 상태**: 99.6% 패키지 크기 감소 달성