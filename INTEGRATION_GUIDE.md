# makenaide_local.py LayeredScoringEngine 통합 가이드

## 🎯 통합 목적

기존 hybrid_technical_filter.py를 새로운 LayeredScoringEngine으로 완전 교체하여 0% 통과율 문제를 해결하고 100% 처리 성공률을 달성합니다.

## 📋 통합 전 체크리스트

### 1. 필수 파일 확인
```bash
# 다음 파일들이 /Users/13ruce/makenaide/ 디렉토리에 있는지 확인
✅ layered_scoring_engine.py      # 핵심 엔진
✅ basic_scoring_modules.py       # 9개 점수 모듈
✅ adaptive_scoring_config.py     # 적응형 설정
✅ integrated_scoring_system.py   # 통합 시스템
✅ compare_scoring_systems.py     # 성능 비교 도구
✅ test_layered_scoring_system.py # 테스트 프레임워크
```

### 2. 의존성 확인
```python
# 필요한 Python 패키지들
pandas>=2.0.0
numpy>=1.24.0
sqlite3 (내장)
asyncio (내장)
logging (내장)
```

## 🔧 단계별 통합 방법

### Step 1: 백업 생성
```bash
# 기존 makenaide_local.py 백업
cp makenaide_local.py makenaide_local_backup_$(date +%Y%m%d).py

# 기존 hybrid_technical_filter.py 백업 (참고용)
cp hybrid_technical_filter.py hybrid_technical_filter_backup.py
```

### Step 2: Import 구문 수정

#### 기존 코드 (makenaide_local.py)
```python
# 삭제할 import
from hybrid_technical_filter import HybridTechnicalFilter

# 또는
import hybrid_technical_filter
```

#### 새로운 코드 (makenaide_local.py)
```python
# 추가할 import
from integrated_scoring_system import IntegratedScoringSystem, IntegratedFilterResult
import asyncio
```

### Step 3: 초기화 코드 수정

#### 기존 코드
```python
# Phase 2: Technical Filter
def run_technical_filter():
    filter = HybridTechnicalFilter()
    # ... 기존 로직
```

#### 새로운 코드
```python
# Phase 2: LayeredScoringEngine (새로운 점수제 시스템)
async def run_layered_scoring():
    """새로운 점수제 필터링 시스템 실행"""
    print("📊 Phase 2: LayeredScoringEngine 점수제 분석 시작...")

    # 시스템 초기화
    scoring_system = IntegratedScoringSystem()

    # 시장 상황 설정 (필요시)
    # scoring_system.update_market_conditions(
    #     MarketRegime.SIDEWAYS,      # 실제 시장 분석 결과로 교체
    #     InvestorProfile.MODERATE     # 설정에 따라 조정
    # )

    return scoring_system
```

### Step 4: 분석 로직 수정

#### 기존 코드 (단일 ticker 분석)
```python
def analyze_single_ticker(ticker):
    filter = HybridTechnicalFilter()
    result = filter.analyze_ticker(ticker)
    if result:
        stage_result, gate_result = result
        # 결과 처리
```

#### 새로운 코드 (단일 ticker 분석)
```python
async def analyze_single_ticker(scoring_system, ticker):
    result = await scoring_system.analyze_ticker(ticker)
    if result:
        # IntegratedFilterResult 객체
        print(f"✅ {ticker}: {result.total_score:.1f}점, {result.recommendation}")
        return result
    else:
        print(f"❌ {ticker}: 분석 실패")
        return None
```

#### 기존 코드 (배치 분석)
```python
def run_batch_analysis():
    filter = HybridTechnicalFilter()
    results = []
    for ticker in ticker_list:
        result = filter.analyze_ticker(ticker)
        if result:
            results.append(result)
```

#### 새로운 코드 (배치 분석)
```python
async def run_batch_analysis(scoring_system, ticker_list):
    """배치 분석 - 병렬 처리로 성능 최적화"""
    print(f"📊 {len(ticker_list)}개 ticker 병렬 분석 시작...")

    # 병렬 분석 실행
    results = await scoring_system.analyze_multiple_tickers(ticker_list)

    # 결과 SQLite 저장
    scoring_system.save_results_to_db(results)

    print(f"✅ 분석 완료: {len(results)}개 성공")
    return results
```

### Step 5: 결과 처리 로직 수정

#### 기존 코드
```python
def process_filter_results():
    # hybrid_technical_filter 결과 처리
    stage = result.current_stage
    quality = result.quality_score
    recommendation = result.recommendation
```

#### 새로운 코드
```python
def process_scoring_results(results: List[IntegratedFilterResult]):
    """LayeredScoringEngine 결과 처리"""
    buy_candidates = []
    watch_candidates = []

    for result in results:
        if result.recommendation == "BUY" and result.quality_gates_passed:
            buy_candidates.append({
                'ticker': result.ticker,
                'total_score': result.total_score,
                'confidence': result.confidence,
                'stage': result.stage,
                'macro_score': result.macro_score,
                'structural_score': result.structural_score,
                'micro_score': result.micro_score
            })
        elif result.recommendation == "WATCH":
            watch_candidates.append(result)

    print(f"🎯 BUY 후보: {len(buy_candidates)}개")
    print(f"👀 WATCH 후보: {len(watch_candidates)}개")

    return buy_candidates, watch_candidates
```

### Step 6: 메인 파이프라인 수정

#### 새로운 메인 함수 구조
```python
async def main_pipeline():
    """새로운 통합 파이프라인"""
    try:
        # Phase 0: Ticker Scanner (기존 유지)
        run_ticker_scanner()

        # Phase 1: Data Collection (기존 유지)
        run_data_collection()

        # Phase 2: LayeredScoringEngine (새로 교체)
        scoring_system = await run_layered_scoring()

        # 전체 ticker 목록 조회
        ticker_list = get_active_tickers()

        # 배치 분석 실행
        results = await run_batch_analysis(scoring_system, ticker_list)

        # 결과 처리
        buy_candidates, watch_candidates = process_scoring_results(results)

        # Phase 3: GPT Analysis (기존 유지, 후보에 대해서만)
        if buy_candidates:
            await run_gpt_analysis(buy_candidates)

        # 이후 Kelly Calculator, Trading Engine 등은 기존 유지

    except Exception as e:
        print(f"❌ 파이프라인 실행 오류: {e}")
        raise

# 실행
if __name__ == "__main__":
    asyncio.run(main_pipeline())
```

## 🔄 호환성 유지 방법

### 1. 기존 인터페이스 호환성
```python
class LegacyCompatibilityWrapper:
    """기존 코드와의 호환성을 위한 래퍼 클래스"""

    def __init__(self):
        self.scoring_system = IntegratedScoringSystem()

    def analyze_ticker(self, ticker):
        """기존 동기 방식으로 호출 가능하도록 래핑"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                self.scoring_system.analyze_ticker(ticker)
            )

            if result:
                # 기존 형식으로 변환
                stage_result = type('StageResult', (), {
                    'current_stage': result.stage,
                    'stage_confidence': result.confidence
                })()

                gate_result = type('GateResult', (), {
                    'quality_score': result.quality_score,
                    'recommendation': result.recommendation
                })()

                return (stage_result, gate_result)
            else:
                return None
        finally:
            loop.close()

# 기존 코드가 수정되기 전까지 임시 사용
# filter = LegacyCompatibilityWrapper()
# result = filter.analyze_ticker(ticker)
```

### 2. 데이터베이스 호환성
```python
# 기존 테이블과 새 테이블 모두 지원
def save_compatible_results(results):
    """기존 DB 스키마와 호환되도록 저장"""
    for result in results:
        # 새로운 테이블 (makenaide_technical_analysis)
        save_to_new_table(result)

        # 기존 테이블도 업데이트 (필요시)
        save_to_legacy_table(result)
```

## 📊 검증 및 테스트

### 1. 통합 전 테스트
```bash
# 새 시스템 단독 테스트
python integrated_scoring_system.py

# 기존 vs 새 시스템 비교
python compare_scoring_systems.py
```

### 2. 통합 후 검증
```python
async def integration_test():
    """통합 후 전체 시스템 검증"""
    # 1. 시스템 초기화 확인
    scoring_system = IntegratedScoringSystem()

    # 2. 소수 ticker로 테스트
    test_tickers = ["KRW-BTC", "KRW-ETH", "KRW-ADA"]
    results = await scoring_system.analyze_multiple_tickers(test_tickers)

    # 3. 결과 검증
    assert len(results) == len(test_tickers), "모든 ticker 처리되어야 함"
    for result in results:
        assert 0 <= result.total_score <= 100, "점수 범위 검증"
        assert result.recommendation in ["BUY", "WATCH", "AVOID"], "추천 값 검증"

    print("✅ 통합 검증 완료")

# 실행
asyncio.run(integration_test())
```

## 🚨 주의사항 및 롤백 계획

### 주의사항
1. **비동기 처리**: 새 시스템은 async/await 기반이므로 메인 함수도 비동기로 변경 필요
2. **SQLite 스키마**: 새로운 테이블 (makenaide_technical_analysis) 자동 생성됨
3. **성능 향상**: 4ms/ticker로 빨라졌지만 병렬 처리로 메모리 사용량 약간 증가
4. **임계값 조정**: 기존 60점 기준에서 50점으로 조정 검토 필요

### 롤백 계획
```bash
# 문제 발생시 즉시 롤백
cp makenaide_local_backup_$(date +%Y%m%d).py makenaide_local.py

# 새 테이블 데이터 백업 (롤백 전)
sqlite3 makenaide_local.db ".dump makenaide_technical_analysis" > layered_scoring_backup.sql
```

## 📈 성능 모니터링

### 모니터링 지표
```python
def monitor_performance():
    """성능 모니터링 지표 수집"""
    stats = scoring_system.get_statistics()

    # 주요 지표
    - 처리 성공률: stats['total_analyzed'] 중 오류 없는 비율
    - 평균 처리 시간: 4ms/ticker 목표
    - Quality Gate 통과율: 13-23% 범위 목표
    - 메모리 사용량: 기존 대비 ±10% 이내
    - BUY 추천 개수: 일일 2-5개 적정
```

### 로그 모니터링
```python
# 중요 로그 패턴 모니터링
- "✅ 분석 완료": 정상 처리 확인
- "❌ 분석 실패": 오류 발생 추적
- "🎯 매수 후보": 거래 기회 모니터링
- "🚪 Quality Gate": 통과율 추적
```

## 🎯 최종 체크리스트

### 통합 완료 확인사항
- [ ] 모든 필수 파일 배치 완료
- [ ] Import 구문 수정 완료
- [ ] 메인 파이프라인 비동기 전환 완료
- [ ] 기존 hybrid_technical_filter 참조 제거
- [ ] SQLite 스키마 호환성 확인
- [ ] 통합 테스트 실행 및 통과
- [ ] 성능 모니터링 설정 완료
- [ ] 롤백 계획 준비 완료

### 운영 시작 전 확인
- [ ] 테스트 환경에서 24시간 안정성 확인
- [ ] 임계값 조정 완료 (60점 → 50점 검토)
- [ ] 알림 시스템 연동 확인
- [ ] 백업 시스템 동작 확인

---

**준비 완료일**: 2025-09-19
**통합 예상 소요 시간**: 1-2시간
**안정화 기간**: 1주일 모니터링 권장