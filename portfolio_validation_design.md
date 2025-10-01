# Portfolio Validation System - Design Specification

**Version**: 1.0
**Date**: 2025-10-01
**Author**: Claude Code (via /sc:design)
**Status**: Design Complete → Ready for Implementation

---

## 1. Executive Summary

### Problem Statement
`trading_engine.py`의 `_detect_portfolio_mismatch()` 메서드가 Upbit API 에러 응답을 처리하지 못해 런타임 오류 발생:
```
ERROR: ❌ 포트폴리오 불일치 감지 실패: string indices must be integers, not 'str'
```

### Root Cause
```python
# Line 606-629: Current implementation
balances = self.upbit.get_balances()  # ⚠️ Can return dict on error

for balance in balances:  # ❌ If dict, iterates over keys (strings)
    if balance['currency'] != 'KRW':  # ❌ string['currency'] fails
```

**Issue**: Upbit API returns different types:
- **Success**: `List[Dict]` - 정상 잔고 리스트
- **Error**: `Dict` - `{'error': {'message': '...', 'name': '...'}}`

코드는 항상 List를 가정하여 dict 반환 시 크래시 발생.

### Impact Assessment
- **Severity**: Medium (Non-blocking, system continues)
- **Frequency**: Occasional (API errors, IP restrictions)
- **User Experience**: Warning logs, but portfolio sync still functional
- **Risk**: Potential data inconsistency if undetected

---

## 2. Design Goals

### Primary Objectives
1. **Robust API Response Handling**: Handle both List and Dict responses safely
2. **Clear Error Reporting**: Distinguish API errors from portfolio mismatches
3. **Graceful Degradation**: Continue operation even when detection fails
4. **Maintainability**: Single source of truth for Upbit balance parsing

### Non-Goals
- IP restriction 우회 (보안 정책 문제)
- Upbit API 재설계 (외부 의존성)
- 실시간 포트폴리오 모니터링 (별도 기능)

---

## 3. Pipeline Placement Strategy

### Current Pipeline Structure
```
Phase 0: scanner.py          (업비트 종목 스캔)
Phase 1: data_collector.py   (OHLCV 데이터 수집)
Phase 2: technical_filter.py (기술적 분석 필터링)
Phase 3: gpt_analyzer.py     (GPT 패턴 분석 - 선택적)
Kelly:   kelly_calculator.py (포지션 사이징)
Market:  market_sentiment.py (시장 감정 분석)
Trading: trading_engine.py   (매수/매도 실행)
         └─ Portfolio Validation ⬅️ 현재 위치
```

### Placement Decision: **Trading Engine 초기화 단계 (현재 위치 유지)** ✅

#### Rationale

**Option A: Trading Engine 초기화 시점** (선택됨)
```python
# makenaide.py:240-255
self.trading_engine = LocalTradingEngine(trading_config, dry_run=self.config.dry_run)

# 포트폴리오 동기화 검증 및 자동 동기화
sync_success, sync_details = self.trading_engine.validate_and_sync_portfolio(
    auto_sync=self.config.auto_sync_enabled,
    sync_policy=self.config.sync_policy
)
```

**장점**:
- ✅ 거래 시작 전 포트폴리오 상태 확인
- ✅ 직접 매수 종목 감지 (Line 958) 이전 실행
- ✅ 기존 구조 유지 (Backward Compatibility)
- ✅ 자동 동기화 정책 활용 가능

**단점**:
- ⚠️ Phase 0-2 실패 시 실행 안됨 (무관)

**Option B: Phase 0 이전 독립 실행**
```python
# 파이프라인 시작 전 독립 검증
if not self.validate_portfolio_before_trading():
    logger.warning("Portfolio validation failed")
```

**장점**:
- ✅ 파이프라인 독립성

**단점**:
- ❌ Trading Engine 의존성 필요 (초기화 중복)
- ❌ Phase 0-2 실패 시 불필요한 검증
- ❌ 기존 구조 변경 필요

**Option C: Kelly Calculator 이후**
```python
# Kelly 포지션 사이징 후 검증
kelly_positions = self.kelly_calculator.calculate()
self.validate_portfolio_against_kelly(kelly_positions)
```

**장점**:
- ✅ Kelly 계산 결과 활용 가능

**단점**:
- ❌ 거래 직전 검증 → 너무 늦음
- ❌ Kelly 계산 자체가 포트폴리오 상태 필요

#### Final Decision: **Option A (현재 위치 유지)**

**이유**:
1. Trading Engine 초기화 시점이 가장 논리적
2. 기존 자동 동기화 정책 활용 가능
3. 직접 매수 종목 감지 전에 실행되어 완전성 보장
4. Backward Compatibility 유지

---

## 4. Technical Design

### 4.1 Improved API Response Handling

#### New Helper Method: `_parse_upbit_balances()`

**Purpose**: Centralized Upbit API response parsing with error handling

**Implementation**:
```python
def _parse_upbit_balances(self) -> Tuple[bool, List[Dict], Optional[str]]:
    """
    Upbit API 응답 파싱 (에러 핸들링 포함)

    Returns:
        Tuple[success: bool, balances: List[Dict], error_msg: Optional[str]]

    Success case:
        (True, [{'ticker': 'KRW-BTC', 'balance': 0.5, 'avg_buy_price': 50000000}, ...], None)

    Error cases:
        (False, [], "API Error: This is not a verified IP")
        (False, [], "Invalid response type: str")
    """
    try:
        response = self.upbit.get_balances()

        # Case 1: API 에러 응답 (dict with 'error' key)
        if isinstance(response, dict):
            if 'error' in response:
                error_msg = response['error'].get('message', 'Unknown error')
                error_name = response['error'].get('name', 'unknown')
                logger.warning(f"⚠️ Upbit API 에러: {error_name} - {error_msg}")
                return False, [], f"API Error: {error_msg}"
            else:
                # Unexpected dict format
                logger.error(f"❌ 예상치 못한 Upbit 응답 형식: {response}")
                return False, [], f"Invalid response format: {type(response)}"

        # Case 2: 정상 응답 (list)
        if isinstance(response, list):
            crypto_balances = []

            for balance in response:
                # Skip KRW and zero balances
                if balance.get('currency') == 'KRW':
                    continue

                balance_amount = float(balance.get('balance', 0))
                if balance_amount <= 0:
                    continue

                # Parse balance data
                crypto_balances.append({
                    'ticker': f"KRW-{balance['currency']}",
                    'balance': balance_amount,
                    'avg_buy_price': float(balance.get('avg_buy_price', 0))
                })

            return True, crypto_balances, None

        # Case 3: Unexpected type
        logger.error(f"❌ Upbit API 응답 타입 오류: {type(response)}")
        return False, [], f"Invalid response type: {type(response)}"

    except Exception as e:
        logger.error(f"❌ Upbit 잔고 조회 실패: {e}")
        return False, [], f"Exception: {str(e)}"
```

### 4.2 Refactored `_detect_portfolio_mismatch()`

**Before** (Lines 602-636):
```python
def _detect_portfolio_mismatch(self) -> List[Dict]:
    """포트폴리오 불일치 감지"""
    try:
        # Upbit 잔고 조회
        balances = self.upbit.get_balances()  # ❌ No error handling
        upbit_balances = []

        for balance in balances:  # ❌ Assumes list
            if balance['currency'] != 'KRW' and float(balance['balance']) > 0:
                upbit_balances.append({...})

        # ... (rest of logic)
        return missing_trades

    except Exception as e:
        logger.error(f"❌ 포트폴리오 불일치 감지 실패: {e}")
        return []
```

**After** (Proposed):
```python
def _detect_portfolio_mismatch(self) -> List[Dict]:
    """
    포트폴리오 불일치 감지 (개선된 에러 핸들링)

    Returns:
        List[Dict]: 누락된 거래 목록
            - 성공: [{'ticker': 'KRW-BTC', 'balance': 0.5, 'avg_buy_price': 50000000}, ...]
            - API 에러: [] (빈 리스트, 경고 로그 출력)
            - DB 에러: [] (빈 리스트, 에러 로그 출력)
    """
    try:
        # 1. Upbit 잔고 조회 (개선된 파싱)
        success, upbit_balances, error_msg = self._parse_upbit_balances()

        if not success:
            logger.warning(f"⚠️ Upbit 잔고 조회 실패: {error_msg}")
            logger.warning("포트폴리오 불일치 감지를 건너뜁니다.")
            return []

        if not upbit_balances:
            logger.info("✅ Upbit 잔고 없음 - 포트폴리오 불일치 없음")
            return []

        # 2. 데이터베이스 거래 기록 조회
        with get_db_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT ticker FROM trades
                WHERE order_type = 'BUY' AND status = 'FULL_FILLED'
            """)
            db_tickers = {row[0] for row in cursor.fetchall()}

        # 3. 누락된 거래 찾기
        missing_trades = []
        for balance in upbit_balances:
            if balance['ticker'] not in db_tickers:
                missing_trades.append(balance)
                logger.warning(f"⚠️ 포트폴리오 불일치: {balance['ticker']} (Upbit에 있지만 DB에 없음)")

        if missing_trades:
            logger.warning(f"⚠️ 총 {len(missing_trades)}개 종목 불일치 감지")
        else:
            logger.info("✅ 포트폴리오 일치 확인 완료")

        return missing_trades

    except Exception as e:
        logger.error(f"❌ 포트폴리오 불일치 감지 실패: {e}")
        logger.error(f"   에러 타입: {type(e).__name__}")
        logger.error(f"   에러 위치: {e.__traceback__.tb_lineno if hasattr(e, '__traceback__') else 'unknown'}")
        return []
```

### 4.3 Error Categorization

**Error Types**:

| Error Type | Cause | Return Value | Action |
|------------|-------|--------------|--------|
| **API Error** | IP restriction, auth failure, rate limit | `(False, [], "API Error: ...")` | Log warning, skip validation |
| **Invalid Format** | Unexpected response type | `(False, [], "Invalid response type: ...")` | Log error, skip validation |
| **Network Error** | Connection timeout, DNS failure | `(False, [], "Exception: ...")` | Log error, skip validation |
| **Database Error** | SQL error, connection failure | `[]` (from catch block) | Log error, return empty list |
| **Success** | Normal operation | `(True, [balances], None)` | Proceed with validation |

---

## 5. Implementation Plan

### Phase 1: Error Handling Fix (1-2 hours)

**Files to Modify**:
- `trading_engine.py` (Lines 602-636)

**Changes**:
1. Add `_parse_upbit_balances()` method (~50 lines)
2. Refactor `_detect_portfolio_mismatch()` (~40 lines)
3. Improve error logging with categorization

**Testing**:
```bash
# Unit test cases
1. Normal API response (List[Dict])
2. API error response ({'error': {...}})
3. Network timeout
4. Invalid response type
5. Database connection failure
```

### Phase 2: Integration Testing (30 min)

**Test Scenarios**:
```bash
# Local dry-run
python3 makenaide.py --dry-run --no-gpt

# EC2 production test
ssh -i ~/aws/makenaide-key.pem ubuntu@52.78.186.226
python3 makenaide.py --dry-run --no-gpt
```

**Validation Criteria**:
- ✅ No crash on API error
- ✅ Clear error messages in logs
- ✅ Portfolio sync continues normally
- ✅ Auto-sync policy still functional

### Phase 3: Production Deployment (15 min)

**Deployment Checklist**:
- [ ] Backup `trading_engine.py` on EC2
- [ ] Upload modified file via scp
- [ ] Run dry-run test
- [ ] Monitor first production run
- [ ] Create deployment report

---

## 6. Testing Strategy

### 6.1 Unit Tests

**Test File**: `test_portfolio_validation.py`

```python
import unittest
from unittest.mock import Mock, patch, MagicMock
from trading_engine import LocalTradingEngine, TradingConfig

class TestPortfolioValidation(unittest.TestCase):

    def setUp(self):
        """테스트 환경 설정"""
        config = TradingConfig()
        self.engine = LocalTradingEngine(config, dry_run=True)

    def test_parse_upbit_balances_success(self):
        """정상 API 응답 파싱 테스트"""
        self.engine.upbit.get_balances = Mock(return_value=[
            {'currency': 'BTC', 'balance': '0.5', 'avg_buy_price': '50000000'},
            {'currency': 'ETH', 'balance': '2.0', 'avg_buy_price': '3000000'},
            {'currency': 'KRW', 'balance': '1000000', 'avg_buy_price': '0'}
        ])

        success, balances, error = self.engine._parse_upbit_balances()

        self.assertTrue(success)
        self.assertIsNone(error)
        self.assertEqual(len(balances), 2)  # KRW excluded
        self.assertEqual(balances[0]['ticker'], 'KRW-BTC')
        self.assertEqual(balances[0]['balance'], 0.5)

    def test_parse_upbit_balances_api_error(self):
        """API 에러 응답 처리 테스트"""
        self.engine.upbit.get_balances = Mock(return_value={
            'error': {
                'message': 'This is not a verified IP.',
                'name': 'no_authorization_ip'
            }
        })

        success, balances, error = self.engine._parse_upbit_balances()

        self.assertFalse(success)
        self.assertEqual(balances, [])
        self.assertIn('API Error', error)
        self.assertIn('verified IP', error)

    def test_parse_upbit_balances_invalid_type(self):
        """잘못된 응답 타입 처리 테스트"""
        self.engine.upbit.get_balances = Mock(return_value="invalid")

        success, balances, error = self.engine._parse_upbit_balances()

        self.assertFalse(success)
        self.assertEqual(balances, [])
        self.assertIn('Invalid response type', error)

    def test_detect_portfolio_mismatch_api_error(self):
        """API 에러 시 불일치 감지 건너뛰기 테스트"""
        self.engine._parse_upbit_balances = Mock(return_value=(
            False, [], "API Error: IP not verified"
        ))

        missing = self.engine._detect_portfolio_mismatch()

        self.assertEqual(missing, [])  # 빈 리스트 반환

    def test_detect_portfolio_mismatch_found(self):
        """포트폴리오 불일치 감지 테스트"""
        # Mock Upbit balances
        self.engine._parse_upbit_balances = Mock(return_value=(
            True,
            [{'ticker': 'KRW-BTC', 'balance': 0.5, 'avg_buy_price': 50000000}],
            None
        ))

        # Mock DB trades (empty)
        with patch('trading_engine.get_db_connection_context') as mock_db:
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = []
            mock_db.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            missing = self.engine._detect_portfolio_mismatch()

            self.assertEqual(len(missing), 1)
            self.assertEqual(missing[0]['ticker'], 'KRW-BTC')

if __name__ == '__main__':
    unittest.main()
```

### 6.2 Integration Test Scenarios

**Scenario 1: Normal Operation**
```bash
# Expected: No errors, portfolio validation succeeds
python3 makenaide.py --dry-run --no-gpt
```

**Scenario 2: IP Restriction**
```bash
# Expected: Warning log, skip validation, continue pipeline
# (Run from non-whitelisted IP)
python3 makenaide.py --dry-run --no-gpt
```

**Scenario 3: Network Timeout**
```bash
# Expected: Error log, skip validation, continue pipeline
# (Simulate network issue)
python3 makenaide.py --dry-run --no-gpt
```

---

## 7. Rollback Plan

### Backup Strategy
```bash
# EC2에서 백업 생성
cp trading_engine.py trading_engine.py.backup_portfolio_validation_20251001
```

### Rollback Procedure
```bash
# 문제 발생 시 원본 복구
mv trading_engine.py trading_engine.py.failed_portfolio_validation
mv trading_engine.py.backup_portfolio_validation_20251001 trading_engine.py

# 시스템 재시작
python3 makenaide.py --dry-run --no-gpt
```

---

## 8. Success Criteria

### Functional Requirements
- ✅ No crash on Upbit API errors
- ✅ Clear distinction between API errors and portfolio mismatches
- ✅ Existing portfolio sync functionality preserved
- ✅ Auto-sync policy continues to work

### Non-Functional Requirements
- ✅ Code maintainability improved (single source of truth)
- ✅ Error logging enhanced with categorization
- ✅ Unit test coverage for edge cases
- ✅ Backward compatibility maintained

### Performance Requirements
- Response time: < 2 seconds (same as before)
- No additional API calls
- Minimal code overhead (~100 lines total)

---

## 9. Future Enhancements

### Phase 2 (Optional, Post-Fix)
1. **Retry Logic**: Exponential backoff for transient API errors
2. **Circuit Breaker**: Disable validation after N consecutive failures
3. **Health Metrics**: Track API error rates for monitoring
4. **Alerting**: SNS notification on repeated API failures

### Phase 3 (Long-term)
1. **Portfolio Snapshot**: Regular portfolio state snapshots
2. **Drift Detection**: Alert on unexpected portfolio changes
3. **Multi-Account Support**: Validate multiple Upbit accounts

---

## 10. Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| New bugs introduced | Low | Medium | Comprehensive unit tests, dry-run validation |
| API behavior change | Low | High | Monitor Upbit API changes, versioned responses |
| Performance degradation | Very Low | Low | Minimal code overhead, same API calls |
| Backward incompatibility | Very Low | High | Careful refactoring, existing tests pass |

---

## 11. Approval & Sign-off

**Design Status**: ✅ Complete
**Ready for Implementation**: Yes
**Estimated Effort**: 2-3 hours total
**Risk Level**: Low (Non-breaking change)

**Next Steps**:
1. Review design with stakeholder
2. Proceed with Phase 1 implementation
3. Run unit tests and integration tests
4. Deploy to EC2 production
5. Monitor first production run

---

**Document End**
