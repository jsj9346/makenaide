#!/usr/bin/env python3
"""
Trading Engine - EC2 Local Architecture
로컬 환경 기반 암호화폐 거래 실행 엔진

🎯 핵심 기능:
- SQLite 기반 거래 기록 및 포트폴리오 관리
- Kelly 공식 기반 포지션 사이징
- 시장 감정 기반 거래 조정
- 7-8% 손절, 20-25% 익절 자동화
- 실시간 포트폴리오 동기화

💰 거래 전략:
- 스탠 와인스타인 Stage 2 진입점 매수
- 마크 미너비니 7-8% 손절 규칙
- 윌리엄 오닐 차트 패턴 기반 익절

📊 참조: 업비트 API 직접 연동, SQLite 로컬 저장
"""

import os
import time
import sqlite3
import logging
import pyupbit
import json
import struct
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

# 블랙리스트 기능 임포트
from utils import load_blacklist
from dotenv import load_dotenv

# 프로젝트 모듈 import
from utils import logger, setup_restricted_logger, retry
from db_manager_sqlite import get_db_connection_context
from kelly_calculator import KellyCalculator, PatternType
from market_sentiment import MarketSentiment
from pyramid_state_manager import PyramidStateManager
from trade_status import TradeStatus, TradeResult

# 환경 변수 로드
load_dotenv()

# 사용하지 않는 기존 피라미딩 타입 정의들은 PyramidStateManager로 대체됨

# 로거 설정
logger = setup_restricted_logger('trading_engine')

class OrderType(Enum):
    """주문 타입"""
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(Enum):
    """주문 상태 (trade_executor.py 기반 세분화)"""
    SUCCESS = "SUCCESS"
    SUCCESS_NO_AVG_PRICE = "SUCCESS_NO_AVG_PRICE"  # 체결되었으나 trades 정보 없음
    SUCCESS_PARTIAL = "SUCCESS_PARTIAL"            # 부분 체결 완료
    SUCCESS_PARTIAL_NO_AVG = "SUCCESS_PARTIAL_NO_AVG"  # 부분 체결 + trades 정보 없음
    FAILURE = "FAILURE"
    SKIPPED = "SKIPPED"
    API_ERROR = "API_ERROR"  # API 오류 (IP 인증 실패 등)

# TradeResult는 trade_status.py에서 import됨

@dataclass
class PositionInfo:
    """포지션 정보"""
    ticker: str
    quantity: float
    avg_buy_price: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_percent: float
    buy_timestamp: datetime
    hold_days: int

@dataclass
class TradingConfig:
    """거래 설정"""
    min_order_amount_krw: float = 10000  # 최소 주문 금액
    max_positions: int = 8  # 최대 동시 보유 종목
    stop_loss_percent: float = -8.0  # 손절 비율 (%)
    take_profit_percent: float = 20.0  # 익절 비율 (%) - 윌리엄 오닐 20-25% 규칙
    taker_fee_rate: float = 0.00139  # Taker 수수료 (0.139%)
    maker_fee_rate: float = 0.0005  # Maker 수수료 (0.05%)
    api_rate_limit_delay: float = 0.5  # API 호출 간격 (초)

# PyramidingManager 클래스는 PyramidStateManager로 대체됨

class TrailingStopManager:
    """
    ATR 기반 트레일링 스탑 관리자 (trade_executor.py에서 이식)

    Min/Max 클램핑 로직 추가 (Quick Win #2):
    - 최소 손절: 5% (너무 타이트한 스탑 방지)
    - 최대 손절: 15% (과도한 손실 방지)
    """

    def __init__(
        self,
        atr_multiplier: float = 1.0,
        per_ticker_config: Optional[Dict[str, float]] = None,
        min_stop_pct: float = 0.05,  # 최소 5% 손절
        max_stop_pct: float = 0.15   # 최대 15% 손절
    ):
        self.atr_multiplier = atr_multiplier
        self.per_ticker_config = per_ticker_config or {}
        self.min_stop_pct = min_stop_pct
        self.max_stop_pct = max_stop_pct
        self.entry_price = {}
        self.highest_price = {}
        self.atr = {}
        self.stop_price = {}
        self.stop_type = {}  # 손절 타입 추적용

    def get_atr_multiplier(self, ticker: str) -> float:
        """티커별 ATR 배수 반환"""
        return self.per_ticker_config.get(ticker, self.atr_multiplier)

    def _apply_stop_clamping(
        self,
        ticker: str,
        trail_price: float,
        fixed_stop: float,
        entry_price: float
    ) -> Tuple[float, str]:
        """
        Min/Max 클램핑을 적용한 최종 손절가 계산

        Args:
            ticker: 종목 코드
            trail_price: 트레일링 스탑 가격
            fixed_stop: 고정 손절 가격
            entry_price: 진입가

        Returns:
            Tuple[final_stop_price, stop_type]
            stop_type: 'atr_trailing', 'atr_fixed', 'clamped_min', 'clamped_max'
        """
        # Step 1: ATR 기반 손절가 선택 (트레일링 vs 고정)
        atr_based_stop = max(trail_price, fixed_stop)
        is_trailing = trail_price > fixed_stop

        # Step 2: 손절 비율 계산
        stop_pct = (entry_price - atr_based_stop) / entry_price

        # Step 3: 클램핑 적용
        if stop_pct < self.min_stop_pct:
            # 너무 타이트한 스탑 → 최소값으로 클램핑
            final_stop = entry_price * (1 - self.min_stop_pct)
            stop_type = 'clamped_min'
            logger.info(
                f"🔒 {ticker} 손절가 최소 클램핑: {stop_pct*100:.2f}% → {self.min_stop_pct*100:.0f}% "
                f"(ATR 기반: {atr_based_stop:.0f}, 클램핑 후: {final_stop:.0f})"
            )

        elif stop_pct > self.max_stop_pct:
            # 너무 루즈한 스탑 → 최대값으로 클램핑
            final_stop = entry_price * (1 - self.max_stop_pct)
            stop_type = 'clamped_max'
            logger.info(
                f"🔒 {ticker} 손절가 최대 클램핑: {stop_pct*100:.2f}% → {self.max_stop_pct*100:.0f}% "
                f"(ATR 기반: {atr_based_stop:.0f}, 클램핑 후: {final_stop:.0f})"
            )

        else:
            # 정상 범위 → ATR 로직 그대로 사용
            final_stop = atr_based_stop
            stop_type = 'atr_trailing' if is_trailing else 'atr_fixed'

        return final_stop, stop_type

    def get_atr_with_fallback(self, ticker: str, current_price: float, db_path: str = "./makenaide_local.db") -> float:
        """
        ATR 조회 with 이중화 백업 로직

        Args:
            ticker: 종목 코드
            current_price: 현재가
            db_path: SQLite DB 경로

        Returns:
            float: ATR 값
        """
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # 🎯 Primary: technical_analysis 테이블에서 ATR 조회
                cursor.execute("""
                    SELECT atr, 'technical_analysis' as source
                    FROM technical_analysis
                    WHERE ticker = ? AND atr IS NOT NULL
                    ORDER BY created_at DESC LIMIT 1
                """, (ticker,))
                result = cursor.fetchone()

                if result and result[0] is not None:
                    atr_value = float(result[0])
                    logger.info(f"📊 {ticker} ATR 조회 성공 ({result[1]}): {atr_value:.2f}")
                    return atr_value

                # 🔄 Fallback: ohlcv_data 테이블에서 직접 ATR 조회
                cursor.execute("""
                    SELECT atr, 'ohlcv_data' as source
                    FROM ohlcv_data
                    WHERE ticker = ? AND atr IS NOT NULL
                    ORDER BY date DESC LIMIT 1
                """, (ticker,))
                backup_result = cursor.fetchone()

                if backup_result and backup_result[0] is not None:
                    atr_value = float(backup_result[0])
                    logger.info(f"🔄 {ticker} 백업 ATR 조회 성공 ({backup_result[1]}): {atr_value:.2f}")
                    return atr_value

                # 🚨 최종 기본값: 현재가의 3%
                default_atr = current_price * 0.03
                logger.warning(f"⚠️ {ticker} ATR 데이터 없음, 기본값 3% 사용: {default_atr:.2f}")
                return default_atr

        except Exception as e:
            # 🚨 예외 발생시 최종 기본값
            default_atr = current_price * 0.03
            logger.error(f"❌ {ticker} ATR 조회 실패, 기본값 3% 사용: {e}")
            return default_atr

    def update(self, ticker: str, current_price: float, db_path: str = "./makenaide_local.db") -> bool:
        """
        트레일링 스탑 업데이트 및 청산 신호 확인

        Args:
            ticker: 종목 코드
            current_price: 현재가
            db_path: SQLite DB 경로

        Returns:
            bool: True면 청산 신호, False면 보유 유지
        """
        # 첫 업데이트 시 ATR 값을 SQLite에서 조회 (이중화 백업 로직)
        if ticker not in self.highest_price:
            atr_value = self.get_atr_with_fallback(ticker, current_price, db_path)

            # 초기 설정 (ATR 조회 성공/실패와 관계없이 실행)
            self.entry_price[ticker] = current_price
            self.highest_price[ticker] = current_price
            self.atr[ticker] = atr_value

            # ATR 배수 조회
            atr_multiplier = self.get_atr_multiplier(ticker)

            # 초기 손절가 계산: 진입가 - (ATR × 배수)
            # 트레일링 스탑은 최고가 기준이지만 초기에는 진입가 = 최고가
            trail_price = current_price - (atr_value * atr_multiplier)
            # 고정 손절가: 진입가 - ATR
            fixed_stop = current_price - atr_value

            # 🎯 Quick Win #2: 초기 손절가에도 클램핑 적용
            final_stop, stop_type = self._apply_stop_clamping(
                ticker=ticker,
                trail_price=trail_price,
                fixed_stop=fixed_stop,
                entry_price=current_price
            )

            self.stop_price[ticker] = final_stop
            self.stop_type[ticker] = stop_type

            logger.info(
                f"🎯 {ticker} 트레일링 스탑 초기화: "
                f"진입가={current_price:.0f}, ATR={atr_value:.2f}, "
                f"손절가={final_stop:.0f} ({stop_type})"
            )
            return False

        # 최고가 업데이트
        if current_price > self.highest_price[ticker]:
            self.highest_price[ticker] = current_price

        # 동적 손절 레벨 계산
        atr_value = self.atr[ticker]
        atr_multiplier = self.get_atr_multiplier(ticker)

        # 트레일링 스탑 가격: 최고가 - (ATR × 배수)
        trail_price = self.highest_price[ticker] - (atr_value * atr_multiplier)

        # 고정 손절 가격: 진입가 - ATR
        fixed_stop = self.entry_price[ticker] - atr_value

        # 🎯 Quick Win #2: Min/Max 클램핑 적용
        final_stop, stop_type = self._apply_stop_clamping(
            ticker=ticker,
            trail_price=trail_price,
            fixed_stop=fixed_stop,
            entry_price=self.entry_price[ticker]
        )

        # 손절가 및 타입 업데이트
        self.stop_price[ticker] = final_stop
        self.stop_type[ticker] = stop_type

        # 현재가가 손절가 아래로 떨어지면 청산 신호
        should_exit = current_price <= self.stop_price[ticker]

        if should_exit:
            logger.info(
                f"🚨 {ticker} 트레일링 스탑 청산 신호: "
                f"현재가={current_price:.0f} ≤ 손절가={self.stop_price[ticker]:.0f}, "
                f"손절타입={stop_type}"
            )

        return should_exit

class LocalTradingEngine:
    """로컬 아키텍처 기반 거래 엔진"""

    def __init__(self, config: TradingConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.db_path = "./makenaide_local.db"

        # 업비트 API 초기화
        self.upbit = None
        self.initialize_upbit_client()

        # 트레일링 스탑 관리자 초기화
        self.trailing_stop_manager = TrailingStopManager(
            atr_multiplier=1.0,  # 기본 ATR 배수
            per_ticker_config={}  # 티커별 설정 (필요시 확장)
        )

        # 피라미딩 상태 관리자 초기화 (pyramid_state 테이블 기반)
        self.pyramid_state_manager = PyramidStateManager(
            db_path=self.db_path
        )

        # 거래 통계 (개선된 버전)
        self.trading_stats = {
            'session_start': datetime.now(),
            'orders_attempted': 0,
            'orders_successful': 0,  # 전량 체결만 카운팅
            'orders_partial_filled': 0,  # 부분 체결 카운팅
            'orders_partial_cancelled': 0,  # 부분 체결 후 취소 카운팅
            'total_volume_krw': 0.0,
            'total_fees_krw': 0.0
        }

    def _safe_convert_to_int(self, value, default=0):
        """바이너리 데이터를 포함한 다양한 타입을 안전하게 int로 변환"""
        if value is None:
            return default

        if isinstance(value, int):
            return value

        if isinstance(value, (float, str)):
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return default

        # 바이너리 데이터 처리 (8바이트 little-endian)
        if isinstance(value, bytes):
            try:
                if len(value) == 8:
                    # 8바이트 little-endian 64비트 정수로 해석
                    return struct.unpack('<Q', value)[0]
                elif len(value) == 4:
                    # 4바이트 little-endian 32비트 정수로 해석
                    return struct.unpack('<I', value)[0]
                else:
                    return default
            except struct.error:
                return default

        return default

    def _safe_convert_to_float(self, value, default=0.0):
        """바이너리 데이터를 포함한 다양한 타입을 안전하게 float로 변환"""
        if value is None:
            return default

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        # 바이너리 데이터 처리 (8바이트 little-endian double)
        if isinstance(value, bytes):
            try:
                if len(value) == 8:
                    # 8바이트를 64비트 정수로 변환한 후 float로
                    int_val = struct.unpack('<Q', value)[0]
                    return float(int_val)
                elif len(value) == 4:
                    # 4바이트를 32비트 정수로 변환한 후 float로
                    int_val = struct.unpack('<I', value)[0]
                    return float(int_val)
                else:
                    return default
            except struct.error:
                return default

        return default

    def initialize_upbit_client(self) -> bool:
        """업비트 클라이언트 초기화"""
        try:
            access_key = os.getenv('UPBIT_ACCESS_KEY')
            secret_key = os.getenv('UPBIT_SECRET_KEY')

            if not access_key or not secret_key:
                logger.error("❌ 업비트 API 키가 설정되지 않았습니다")
                return False

            self.upbit = pyupbit.Upbit(access_key, secret_key)
            logger.info("✅ 업비트 거래 클라이언트 초기화 완료")
            return True

        except Exception as e:
            logger.error(f"❌ 업비트 클라이언트 초기화 실패: {e}")
            return False

    def convert_to_new_trade_result(self, old_trade_result, ticker: str,
                                   is_pyramid: bool = False,
                                   requested_amount: float = 0) -> TradeResult:
        """기존 TradeResult를 새로운 TradeResult로 변환"""

        # OrderStatus를 TradeStatus로 변환
        status_mapping = {
            OrderStatus.SUCCESS: TradeStatus.FULL_FILLED,
            OrderStatus.SUCCESS_NO_AVG_PRICE: TradeStatus.FULL_FILLED,
            OrderStatus.SUCCESS_PARTIAL: TradeStatus.PARTIAL_FILLED,
            OrderStatus.SUCCESS_PARTIAL_NO_AVG: TradeStatus.PARTIAL_FILLED,
            OrderStatus.FAILURE: TradeStatus.FAILED,
            OrderStatus.SKIPPED: TradeStatus.CANCELLED,
            OrderStatus.API_ERROR: TradeStatus.FAILED
        }

        new_status = status_mapping.get(old_trade_result.status, TradeStatus.FAILED)

        return TradeResult(
            ticker=ticker,
            order_id=old_trade_result.order_id or "unknown",
            status=new_status,
            requested_amount=requested_amount or (old_trade_result.amount_krw or 0),
            requested_quantity=old_trade_result.quantity or 0,  # 기존 로직에서는 구분이 없음
            filled_amount=old_trade_result.amount_krw or 0,
            filled_quantity=old_trade_result.quantity or 0,
            average_price=old_trade_result.price,
            timestamp=old_trade_result.timestamp or datetime.now(),
            fees=old_trade_result.fee or 0,  # fee → fees
            error_message=old_trade_result.error_message,
            is_pyramid=is_pyramid
        )

    def save_trade_record(self, trade_result, ticker: str = None, is_pyramid: bool = False, requested_amount: float = 0, trade_type: str = 'BUY'):
        """거래 기록을 새로운 trades 테이블에 저장"""
        try:
            # 기존 TradeResult인지 새로운 TradeResult인지 판별
            if hasattr(trade_result, 'filled_amount'):
                # 이미 새로운 TradeResult
                new_trade_result = trade_result
            else:
                # 기존 TradeResult를 새로운 형태로 변환
                new_trade_result = self.convert_to_new_trade_result(
                    trade_result, ticker or trade_result.ticker, is_pyramid, requested_amount
                )

            with get_db_connection_context() as conn:
                cursor = conn.cursor()

                # 새로운 trades 테이블 구조에 맞게 저장
                cursor.execute("""
                    INSERT INTO trades (
                        ticker, order_type, status, order_id,
                        requested_quantity, filled_quantity,
                        requested_amount, filled_amount, average_price,
                        is_pyramid, is_pyramid_eligible,
                        fee, error_message, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    new_trade_result.ticker,
                    trade_type,  # 매수/매도 구분
                    new_trade_result.status.value,
                    new_trade_result.order_id,
                    new_trade_result.requested_quantity,
                    new_trade_result.filled_quantity,
                    new_trade_result.requested_amount,
                    new_trade_result.filled_amount,
                    new_trade_result.average_price,
                    new_trade_result.is_pyramid,
                    new_trade_result.is_pyramid_eligible,
                    new_trade_result.fees,
                    new_trade_result.error_message,
                    new_trade_result.timestamp.isoformat() if hasattr(new_trade_result.timestamp, 'isoformat') else str(new_trade_result.timestamp)
                ))

                conn.commit()
                logger.info(f"📝 {new_trade_result.ticker} 거래 기록 저장 완료 (상태: {new_trade_result.status.value}, 피라미딩: {new_trade_result.is_pyramid})")

        except Exception as e:
            logger.error(f"❌ 거래 기록 저장 실패: {e}")
            logger.error(f"   TradeResult 내용: {trade_result}")
            # 에러가 발생해도 거래는 계속 진행되어야 하므로 raise하지 않음

    def process_trade_result(self, old_trade_result, ticker: str, is_pyramid: bool = False,
                           requested_amount: float = 0) -> TradeResult:
        """거래 결과를 처리하고 통합 워크플로우 실행"""
        try:
            # 1. 새로운 TradeResult 객체 생성
            new_trade_result = self.convert_to_new_trade_result(
                old_trade_result, ticker, is_pyramid, requested_amount
            )

            # 2. trades 테이블에 저장
            self.save_trade_record(new_trade_result, ticker, is_pyramid=is_pyramid, requested_amount=requested_amount)

            # 3. PyramidStateManager에 상태 업데이트
            if new_trade_result.status.is_successful:
                self.pyramid_state_manager.update_after_trade(new_trade_result)
                logger.info(f"✅ {ticker} 거래 성공: PyramidStateManager 업데이트 완료 (피라미딩: {is_pyramid})")

            # 4. 상세 결과 로깅
            from trade_status import log_trade_result
            log_trade_result(new_trade_result, is_pyramid)

            return new_trade_result

        except Exception as e:
            logger.error(f"❌ {ticker} 거래 결과 처리 실패: {e}")
            # 기본적인 TradeResult라도 반환
            return self.convert_to_new_trade_result(old_trade_result, ticker, is_pyramid, requested_amount)

    def validate_portfolio_sync(self) -> bool:
        """포트폴리오 동기화 상태 검증 (레거시 메서드 - 호환성 유지용)"""
        sync_result = self.validate_and_sync_portfolio(auto_sync=False)
        return sync_result[0]

    def validate_and_sync_portfolio(self, auto_sync: bool = True, sync_policy: str = 'aggressive') -> Tuple[bool, Dict]:
        """포트폴리오 검증 및 자동 동기화

        Args:
            auto_sync: 자동 동기화 활성화 여부
            sync_policy: 동기화 정책 ('conservative', 'moderate', 'aggressive')

        Returns:
            Tuple[bool, Dict]: (동기화 성공 여부, 동기화 결과 상세)
        """
        try:
            logger.info("🔍 포트폴리오 동기화 상태 검증 시작...")

            # Phase 1: 포트폴리오 불일치 감지
            missing_trades = self._detect_portfolio_mismatch()

            if not missing_trades:
                logger.info("✅ 포트폴리오 동기화 상태 정상")
                return True, {'status': 'synced', 'missing_trades': []}

            # Phase 2: 불일치 감지됨 - 로깅
            total_missing_value = sum(trade['balance'] * trade['avg_buy_price'] for trade in missing_trades)
            logger.warning(f"⚠️ 포트폴리오 동기화 불일치 감지: {len(missing_trades)}개 종목")
            logger.warning(f"📊 누락된 총 투자금액: {total_missing_value:,.0f} KRW")

            for trade in missing_trades:
                logger.warning(f"  - {trade['ticker']}: {trade['balance']:.8f} @ {trade['avg_buy_price']:,.0f}")

            # Phase 3: 자동 동기화 수행 여부 결정
            if not auto_sync:
                logger.warning("portfolio_sync_tool.py를 실행하여 동기화하세요.")
                return False, {
                    'status': 'mismatch_detected',
                    'missing_trades': missing_trades,
                    'auto_sync_disabled': True
                }

            # Phase 4: 동기화 위험도 평가 및 실행
            sync_result = self._execute_safe_portfolio_sync(missing_trades, sync_policy)

            if sync_result['success']:
                logger.info("🎉 포트폴리오 자동 동기화 완료")
                return True, sync_result
            else:
                logger.error("❌ 포트폴리오 자동 동기화 실패")
                logger.warning("portfolio_sync_tool.py를 수동으로 실행하여 동기화하세요.")
                return False, sync_result

        except Exception as e:
            logger.warning(f"⚠️ 포트폴리오 동기화 검증 실패: {e}")
            return False, {'status': 'error', 'error': str(e)}

    def _detect_portfolio_mismatch(self) -> List[Dict]:
        """포트폴리오 불일치 감지"""
        try:
            # Upbit 잔고 조회
            balances = self.upbit.get_balances()
            upbit_balances = []

            for balance in balances:
                if balance['currency'] != 'KRW' and float(balance['balance']) > 0:
                    upbit_balances.append({
                        'ticker': f"KRW-{balance['currency']}",
                        'balance': float(balance['balance']),
                        'avg_buy_price': float(balance['avg_buy_price']) if balance['avg_buy_price'] else 0
                    })

            # 데이터베이스 거래 기록 조회
            with get_db_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT ticker FROM trades
                    WHERE order_type = 'BUY' AND status = 'FULL_FILLED'
                """)
                db_tickers = {row[0] for row in cursor.fetchall()}

            # 누락된 거래 찾기
            missing_trades = []
            for balance in upbit_balances:
                if balance['ticker'] not in db_tickers:
                    missing_trades.append(balance)

            return missing_trades

        except Exception as e:
            logger.error(f"❌ 포트폴리오 불일치 감지 실패: {e}")
            return []

    def _execute_safe_portfolio_sync(self, missing_trades: List[Dict], sync_policy: str) -> Dict:
        """안전한 포트폴리오 동기화 실행"""
        try:
            # 동기화 정책별 최대 금액 설정
            policy_limits = {
                'conservative': 500_000,    # 50만원
                'moderate': 2_000_000,      # 200만원
                'aggressive': float('inf')  # 무제한
            }

            max_sync_amount = policy_limits.get(sync_policy, 500_000)
            logger.info(f"📋 동기화 정책: {sync_policy} (최대 {max_sync_amount:,.0f} KRW)")

            # 동기화 대상 필터링
            sync_targets = []
            total_sync_value = 0

            for trade in missing_trades:
                trade_value = trade['balance'] * trade['avg_buy_price']

                if trade_value <= max_sync_amount:
                    sync_targets.append(trade)
                    total_sync_value += trade_value
                else:
                    logger.warning(f"⚠️ {trade['ticker']} 제외: 금액 초과 ({trade_value:,.0f} > {max_sync_amount:,.0f})")

            if not sync_targets:
                return {
                    'success': False,
                    'reason': 'no_safe_targets',
                    'message': f'동기화 정책 {sync_policy}에 맞는 안전한 대상이 없습니다.'
                }

            logger.info(f"🎯 동기화 대상: {len(sync_targets)}개 종목 (총 {total_sync_value:,.0f} KRW)")

            # 실제 동기화 실행
            success_count = 0
            for trade in sync_targets:
                if self._create_sync_trade_record(trade):
                    success_count += 1

            return {
                'success': success_count == len(sync_targets),
                'synced_count': success_count,
                'total_targets': len(sync_targets),
                'total_value': total_sync_value,
                'policy': sync_policy,
                'synced_trades': sync_targets[:success_count]
            }

        except Exception as e:
            logger.error(f"❌ 포트폴리오 동기화 실행 실패: {e}")
            return {'success': False, 'error': str(e)}

    def _create_sync_trade_record(self, trade: Dict) -> bool:
        """동기화용 거래 기록 생성"""
        try:
            with get_db_connection_context() as conn:
                cursor = conn.cursor()

                # 추정 매수 시간 (현재 시간 - 1일)
                estimated_buy_time = datetime.now() - timedelta(days=1)
                estimated_buy_time = estimated_buy_time.replace(hour=9, minute=0, second=0, microsecond=0)

                # 거래 기록 생성
                cursor.execute("""
                    INSERT INTO trades (
                        ticker, order_type, status, order_id,
                        requested_quantity, filled_quantity,
                        requested_amount, filled_amount, average_price,
                        fill_rate, is_pyramid, is_pyramid_eligible,
                        fee, timestamp, created_at, updated_at, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade['ticker'], 'BUY', 'FULL_FILLED',
                    f'AUTO-SYNC-{trade["ticker"]}-{int(datetime.now().timestamp())}',
                    trade['balance'], trade['balance'],
                    trade['balance'] * trade['avg_buy_price'],
                    trade['balance'] * trade['avg_buy_price'],
                    trade['avg_buy_price'],
                    1.0, False, True,
                    trade['balance'] * trade['avg_buy_price'] * 0.0005,  # 0.05% 수수료 추정
                    estimated_buy_time.isoformat(),
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                    'AUTO_SYNC'
                ))

                conn.commit()
                logger.info(f"✅ {trade['ticker']} 동기화 완료: {trade['balance']:.8f} @ {trade['avg_buy_price']:,.0f}")
                return True

        except Exception as e:
            logger.error(f"❌ {trade['ticker']} 동기화 실패: {e}")
            return False

    def get_current_positions(self) -> List[PositionInfo]:
        """현재 보유 포지션 조회 (블랙리스트 필터링 적용)"""
        positions = []

        try:
            if self.dry_run:
                logger.info("🧪 DRY RUN: 포지션 조회 건너뜀")
                return positions

            # 블랙리스트 로드
            blacklist = load_blacklist()
            if not blacklist:
                logger.warning("⚠️ 블랙리스트 로드 실패, 필터링 없이 진행")
                blacklist = {}

            # 업비트에서 잔고 조회
            balances = self.upbit.get_balances()

            if not balances:
                logger.info("📭 보유 포지션이 없습니다")
                return positions

            # API 오류 응답 처리
            if isinstance(balances, dict) and 'error' in balances:
                error_info = balances['error']
                logger.warning(f"⚠️ 업비트 API 오류: {error_info.get('name', 'unknown')} - {error_info.get('message', 'no message')}")
                return positions

            # balances가 리스트가 아닌 경우 처리
            if not isinstance(balances, list):
                logger.warning(f"⚠️ 예상치 못한 balances 타입: {type(balances)}")
                return positions

            # 블랙리스트 통계를 위한 카운터
            total_balances = 0
            blacklisted_positions = 0

            for balance in balances:
                currency = balance['currency']

                # KRW는 제외
                if currency == 'KRW':
                    continue

                quantity = float(balance['balance'])
                avg_buy_price = float(balance['avg_buy_price'])

                if quantity <= 0:
                    continue

                ticker = f"KRW-{currency}"
                total_balances += 1

                # 블랙리스트 확인
                if ticker in blacklist:
                    blacklisted_positions += 1
                    logger.info(f"⛔️ {ticker}: 블랙리스트에 등록되어 포트폴리오 관리 제외")
                    continue

                try:
                    # 현재가 조회
                    current_price = pyupbit.get_current_price(ticker)
                    if not current_price:
                        continue

                    # 포지션 정보 계산
                    market_value = quantity * current_price
                    cost_basis = quantity * avg_buy_price
                    unrealized_pnl = market_value - cost_basis
                    unrealized_pnl_percent = (unrealized_pnl / cost_basis) * 100

                    # 보유 일수 계산 (SQLite에서 매수 시점 조회)
                    buy_timestamp = self.get_last_buy_timestamp(ticker)
                    hold_days = 0
                    if buy_timestamp:
                        # 시간 차이 계산 (최소 1일로 설정)
                        time_diff = datetime.now() - buy_timestamp
                        hold_days = max(1, time_diff.days)

                        # 같은 날 매수한 경우에도 시간이 충분히 지났으면 1일로 계산
                        if time_diff.days == 0 and time_diff.total_seconds() > 3600:  # 1시간 이상
                            hold_days = 1

                    position = PositionInfo(
                        ticker=ticker,
                        quantity=quantity,
                        avg_buy_price=avg_buy_price,
                        current_price=current_price,
                        market_value=market_value,
                        unrealized_pnl=unrealized_pnl,
                        unrealized_pnl_percent=unrealized_pnl_percent,
                        buy_timestamp=buy_timestamp or datetime.now(),
                        hold_days=hold_days
                    )

                    positions.append(position)

                except Exception as e:
                    logger.warning(f"⚠️ {ticker} 포지션 정보 조회 실패: {e}")
                    continue

            logger.info(f"📊 포트폴리오 관리 대상: {len(positions)}개 포지션")
            if blacklisted_positions > 0:
                logger.info(f"⛔️ 블랙리스트 필터링: {blacklisted_positions}개 포지션 제외 (총 {total_balances}개 중)")
            return positions

        except Exception as e:
            logger.error(f"❌ 포지션 조회 실패: {e}")
            return positions

    def get_last_buy_timestamp(self, ticker: str) -> Optional[datetime]:
        """마지막 매수 시점 조회"""
        try:
            with get_db_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT created_at FROM trades
                    WHERE ticker = ? AND order_type = 'BUY' AND status IN ('FULL_FILLED', 'PARTIAL_FILLED')
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (ticker,))

                result = cursor.fetchone()
                if result:
                    return datetime.fromisoformat(result[0])
                return None

        except Exception as e:
            logger.warning(f"⚠️ {ticker} 매수 시점 조회 실패: {e}")
            return None

    def detect_and_initialize_direct_purchases(self) -> List[str]:
        """
        직접 매수 종목 감지 및 자동 초기화 시스템

        업비트 API에서 감지된 포지션 중 trades 테이블에 매수 기록이 없는 경우를
        '직접 매수 종목'으로 분류하고 자동으로 데이터베이스에 초기화합니다.

        Returns:
            List[str]: 감지된 직접 매수 종목 티커 리스트
        """
        direct_purchases = []

        try:
            if self.dry_run:
                logger.info("🧪 DRY RUN: 직접 매수 종목 감지 건너뜀")
                return direct_purchases

            # 블랙리스트 로드
            blacklist = load_blacklist()
            if not blacklist:
                blacklist = {}

            # 업비트에서 잔고 조회
            balances = self.upbit.get_balances()

            if not balances or not isinstance(balances, list):
                return direct_purchases

            logger.info("🔍 직접 매수 종목 감지 시작...")

            for balance in balances:
                currency = balance['currency']

                # KRW는 제외
                if currency == 'KRW':
                    continue

                quantity = float(balance['balance'])
                if quantity <= 0:
                    continue

                ticker = f"KRW-{currency}"

                # 블랙리스트 확인
                if ticker in blacklist:
                    continue

                # trades 테이블에서 매수 기록 확인
                last_buy_timestamp = self.get_last_buy_timestamp(ticker)

                # 매수 기록이 없으면 직접 매수 종목으로 분류
                if last_buy_timestamp is None:
                    logger.warning(f"🔍 직접 매수 종목 감지: {ticker} (보유량: {quantity:.8f})")

                    # 직접 매수 종목 데이터베이스 초기화
                    success = self._initialize_direct_purchase_record(ticker, balance)

                    if success:
                        direct_purchases.append(ticker)
                        logger.info(f"✅ {ticker} 직접 매수 종목 데이터베이스 초기화 완료")
                    else:
                        logger.error(f"❌ {ticker} 직접 매수 종목 초기화 실패")

            if direct_purchases:
                logger.info(f"🎯 총 {len(direct_purchases)}개 직접 매수 종목 감지 및 초기화: {', '.join(direct_purchases)}")
            else:
                logger.info("✅ 직접 매수 종목 없음 - 모든 포지션이 시스템을 통해 매수됨")

            return direct_purchases

        except Exception as e:
            logger.error(f"❌ 직접 매수 종목 감지 실패: {e}")
            return direct_purchases

    def _initialize_direct_purchase_record(self, ticker: str, balance_info: dict) -> bool:
        """
        직접 매수 종목의 초기 데이터베이스 레코드 생성

        Args:
            ticker: 종목 코드 (예: KRW-NEAR)
            balance_info: 업비트 잔고 정보

        Returns:
            bool: 초기화 성공 여부
        """
        try:
            currency = ticker.replace("KRW-", "")
            quantity = float(balance_info['balance'])
            avg_buy_price = float(balance_info['avg_buy_price'])

            if quantity <= 0 or avg_buy_price <= 0:
                logger.error(f"❌ {ticker} 잘못된 잔고 정보: 수량={quantity}, 평균매수가={avg_buy_price}")
                return False

            # 총 매수 금액 계산
            total_amount = quantity * avg_buy_price

            # 현재 시간을 매수 시점으로 설정 (실제 매수 시점은 알 수 없음)
            purchase_timestamp = datetime.now().isoformat()

            # 가상의 거래 ID 생성 (직접 매수 종목 식별용)
            virtual_order_id = f"DIRECT_PURCHASE_{ticker}_{int(datetime.now().timestamp())}"

            with get_db_connection_context() as conn:
                cursor = conn.cursor()

                # trades 테이블에 직접 매수 기록 삽입 (새 스키마 적용)
                cursor.execute("""
                    INSERT INTO trades (
                        ticker, order_type, status, order_id,
                        requested_quantity, filled_quantity,
                        requested_amount, filled_amount, average_price,
                        is_pyramid, is_pyramid_eligible,
                        fee, error_message, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ticker,
                    'BUY',
                    'FULL_FILLED',  # 새 스키마의 상태값
                    virtual_order_id,
                    quantity,        # requested_quantity
                    quantity,        # filled_quantity (직접 매수는 전량 체결)
                    total_amount,    # requested_amount
                    total_amount,    # filled_amount (직접 매수는 전량 체결)
                    avg_buy_price,   # average_price
                    False,          # is_pyramid (직접 매수는 피라미딩 아님)
                    True,           # is_pyramid_eligible (향후 피라미딩 가능)
                    0.0,            # fee (수수료 정보 없음)
                    None,           # error_message
                    purchase_timestamp
                ))

                conn.commit()

            logger.info(f"📝 {ticker} 직접 매수 기록 생성: {quantity:.8f}개 @ {avg_buy_price:,.0f}원 (총 {total_amount:,.0f}원)")
            return True

        except Exception as e:
            logger.error(f"❌ {ticker} 직접 매수 초기화 실패: {e}")
            return False

    def get_total_balance_krw(self) -> float:
        """총 보유 자산 KRW 환산 (개선된 디버깅 버전)"""
        try:
            logger.debug("🔍 총 자산 조회 시작")

            if self.dry_run:
                logger.debug("🧪 DRY RUN 모드: 1,000,000원 반환")
                return 1000000.0  # DRY RUN 시 100만원으로 가정

            logger.debug("📡 업비트 API 잔고 조회 시작")
            balances = self.upbit.get_balances()

            if not balances:
                logger.warning("⚠️ 업비트 잔고 조회 결과가 비어있음")
                return 0.0

            logger.debug(f"📊 조회된 잔고 수: {len(balances) if isinstance(balances, list) else 'N/A'}")

            # API 오류 응답 처리
            if isinstance(balances, dict) and 'error' in balances:
                error_info = balances['error']
                logger.error(f"❌ 업비트 API 오류: {error_info.get('name', 'unknown')} - {error_info.get('message', 'no message')}")
                return 0.0

            # balances가 리스트가 아닌 경우 처리
            if not isinstance(balances, list):
                logger.error(f"❌ 예상치 못한 balances 타입: {type(balances)}, 값: {balances}")
                return 0.0

            total_krw = 0.0
            processed_currencies = []

            for balance in balances:
                try:
                    currency = balance.get('currency', 'UNKNOWN')
                    quantity = float(balance.get('balance', 0))

                    if quantity <= 0:
                        continue

                    if currency == 'KRW':
                        total_krw += quantity
                        processed_currencies.append(f"KRW: {quantity:,.0f}원")
                        logger.debug(f"💰 KRW 잔고: {quantity:,.0f}원")
                    else:
                        # 암호화폐는 현재가로 환산
                        ticker = f"KRW-{currency}"
                        current_price = pyupbit.get_current_price(ticker)
                        if current_price:
                            crypto_value_krw = quantity * current_price
                            total_krw += crypto_value_krw
                            processed_currencies.append(f"{currency}: {quantity:.8f} @ {current_price:,.0f}원 = {crypto_value_krw:,.0f}원")
                            logger.debug(f"🪙 {currency}: {quantity:.8f}개 × {current_price:,.0f}원 = {crypto_value_krw:,.0f}원")
                        else:
                            logger.warning(f"⚠️ {ticker} 현재가 조회 실패")
                            continue

                except Exception as balance_error:
                    ticker = f"KRW-{currency}" if 'currency' in locals() and currency != 'KRW' else currency if 'currency' in locals() else 'UNKNOWN'
                    logger.warning(f"❌ {ticker}: 잔고 처리 실패 - {balance_error}")
                    logger.debug(f"🔍 {ticker}: balance 구조 - {balance if 'balance' in locals() else 'N/A'}")
                    continue

            logger.info(f"💰 총 자산 조회 완료: {total_krw:,.0f}원 (처리된 자산: {len(processed_currencies)}개)")
            logger.debug(f"📋 자산 상세: {', '.join(processed_currencies) if processed_currencies else '없음'}")

            return total_krw

        except Exception as e:
            logger.error(f"❌ 총 자산 조회 함수 전체 실패: {type(e).__name__}: {str(e)}")
            import traceback
            logger.debug(f"🔍 상세 스택 트레이스:\n{traceback.format_exc()}")
            return 0.0

    def calculate_position_size(self, ticker: str, kelly_percentage: float,
                              market_sentiment_adjustment: float) -> float:
        """포지션 사이즈 계산"""
        try:
            # 총 자산 조회
            total_balance = self.get_total_balance_krw()

            if total_balance <= 0:
                logger.warning("⚠️ 총 자산이 0원 이하입니다")
                return 0.0

            # Kelly 비율에 시장 감정 조정 적용
            adjusted_percentage = kelly_percentage * market_sentiment_adjustment

            # 최소/최대 포지션 제한
            adjusted_percentage = max(1.0, min(adjusted_percentage, 8.0))

            # 실제 투자 금액 계산
            position_amount = total_balance * (adjusted_percentage / 100)

            # 최소 주문 금액 확인
            if position_amount < self.config.min_order_amount_krw:
                logger.info(f"⏭️ {ticker}: 계산된 포지션 ({position_amount:,.0f}원) < 최소 주문 금액")
                return 0.0

            logger.info(f"💰 {ticker}: 포지션 사이즈 {adjusted_percentage:.1f}% ({position_amount:,.0f}원)")
            return position_amount

        except Exception as e:
            logger.error(f"❌ {ticker} 포지션 사이즈 계산 실패: {e}")
            return 0.0

    @retry(max_attempts=3, initial_delay=1, backoff=2)
    def execute_buy_order(self, ticker: str, amount_krw: float, is_pyramid: bool = False) -> TradeResult:
        """매수 주문 실행"""
        trade_result = TradeResult(
            ticker=ticker,
            order_id="PENDING",  # 임시 order_id, 실제 주문 후 업데이트
            status=TradeStatus.FAILED,
            requested_amount=amount_krw,
            requested_quantity=0.0,  # 현재가 조회 후 계산됨
            is_pyramid=is_pyramid  # 피라미딩 상태 설정
        )

        try:
            self.trading_stats['orders_attempted'] += 1

            # DRY RUN 모드
            if self.dry_run:
                current_price = pyupbit.get_current_price(ticker)
                if current_price:
                    requested_quantity = amount_krw / current_price
                    trade_result.requested_quantity = requested_quantity
                    trade_result.order_id = f"DRY_RUN_{int(datetime.now().timestamp())}"
                    trade_result.status = TradeStatus.FULL_FILLED
                    trade_result.filled_amount = amount_krw
                    trade_result.filled_quantity = requested_quantity
                    trade_result.average_price = current_price
                    logger.info(f"🧪 DRY RUN: {ticker} 매수 주문 ({amount_krw:,.0f}원)")
                    self.trading_stats['orders_successful'] += 1
                    self.save_trade_record(trade_result, ticker, is_pyramid=is_pyramid, requested_amount=amount_krw)
                    return trade_result
                else:
                    trade_result.error_message = "DRY RUN 모드에서 현재가 조회 실패"
                    return trade_result

            # 최소 주문 금액 확인
            if amount_krw < self.config.min_order_amount_krw:
                trade_result.error_message = f"주문 금액 부족: {amount_krw:,.0f} < {self.config.min_order_amount_krw:,.0f}"
                trade_result.status = TradeStatus.CANCELLED
                logger.warning(f"⚠️ {ticker}: {trade_result.error_message}")
                self.save_trade_record(trade_result, ticker, is_pyramid=is_pyramid, requested_amount=amount_krw)
                return trade_result

            # 현재가 조회
            current_price = pyupbit.get_current_price(ticker)
            if not current_price:
                trade_result.error_message = "현재가 조회 실패"
                logger.error(f"❌ {ticker}: {trade_result.error_message}")
                self.save_trade_record(trade_result, ticker, is_pyramid=is_pyramid, requested_amount=amount_krw)
                return trade_result

            # requested_quantity 계산
            trade_result.requested_quantity = amount_krw / current_price

            # 수수료를 고려한 실제 주문 금액
            order_amount = amount_krw / (1 + self.config.taker_fee_rate)

            logger.info(f"🚀 {ticker} 시장가 매수 주문: {order_amount:,.0f}원 (현재가: {current_price:,.0f})")

            # 업비트 매수 주문
            response = self.upbit.buy_market_order(ticker, order_amount)

            # 강화된 API 응답 검증 (다중 필드 검증)
            order_id = None
            if response:
                # 다양한 주문 ID 필드명 시도
                order_id = response.get('uuid') or response.get('order_id') or response.get('id') or response.get('orderId')

            if not response:
                trade_result.error_message = f"주문 접수 실패: API 응답 없음"
                logger.error(f"❌ {ticker} 매수 주문 접수 실패 - 응답 없음")
                self.save_trade_record(trade_result, ticker, is_pyramid=is_pyramid, requested_amount=amount_krw)
                return trade_result
            elif not order_id:
                # 상세한 응답 로깅으로 디버깅 지원
                logger.warning(f"⚠️ {ticker} 주문 응답에서 주문ID 필드를 찾을 수 없음. 응답 구조: {response}")
                trade_result.error_message = f"주문 ID 추출 실패. 응답: {response}"
                logger.error(f"❌ {ticker} 매수 주문 접수 실패 - 주문ID 없음")

                # 재검증 시도: 3초 후 주문 목록에서 최근 주문 확인
                logger.info(f"🔄 {ticker} 주문 재검증 시도...")
                time.sleep(3)
                try:
                    # 최근 주문 목록에서 확인 시도
                    recent_orders = self.upbit.get_orders(state='done', limit=5)
                    if recent_orders:
                        for order in recent_orders:
                            if (order.get('market') == ticker and
                                order.get('side') == 'bid' and
                                abs(float(order.get('volume', 0)) * float(order.get('price', 0)) - order_amount) < 1000):
                                order_id = order.get('uuid')
                                logger.info(f"✅ {ticker} 주문ID 재검증 성공: {order_id}")
                                break

                    if not order_id:
                        self.save_trade_record(trade_result, ticker, is_pyramid=is_pyramid, requested_amount=amount_krw)
                        return trade_result

                except Exception as retry_error:
                    logger.error(f"❌ {ticker} 주문 재검증 실패: {retry_error}")
                    self.save_trade_record(trade_result, ticker, is_pyramid=is_pyramid, requested_amount=amount_krw)
                    return trade_result
            trade_result.order_id = order_id

            logger.info(f"✅ {ticker} 매수 주문 접수 성공 (주문ID: {order_id})")

            # 주문 체결 확인 (최대 10초 대기)
            time.sleep(5)
            order_detail = self.upbit.get_order(order_id)

            if order_detail and order_detail.get('state') == 'done':
                executed_quantity = float(order_detail.get('executed_volume', 0))
                trades = order_detail.get('trades', [])

                if trades and executed_quantity > 0:
                    # 평균 체결가 계산
                    total_value = sum(float(trade['price']) * float(trade['volume']) for trade in trades)
                    total_volume = sum(float(trade['volume']) for trade in trades)

                    if total_volume > 0:
                        avg_price = total_value / total_volume
                        fee = total_value * self.config.taker_fee_rate

                        # 전량 체결 또는 부분 체결 판단
                        if executed_quantity >= trade_result.requested_quantity * 0.99:  # 99% 이상이면 전량 체결로 간주
                            trade_result.status = TradeStatus.FULL_FILLED
                            self.trading_stats['orders_successful'] += 1  # 전량 체결만 성공으로 카운팅
                        else:
                            trade_result.status = TradeStatus.PARTIAL_FILLED
                            self.trading_stats['orders_partial_filled'] += 1  # 부분 체결로 분류

                        trade_result.filled_quantity = executed_quantity
                        trade_result.average_price = avg_price
                        trade_result.filled_amount = total_value
                        trade_result.fees = fee
                        self.trading_stats['total_volume_krw'] += total_value
                        self.trading_stats['total_fees_krw'] += fee

                        # 통합된 거래 결과 처리 (새로운 TradeResult 활용)
                        self.process_trade_result(
                            trade_result,
                            ticker,
                            is_pyramid=is_pyramid,  # 매개변수로 전달받은 값 사용
                            requested_amount=amount_krw
                        )

                        logger.info(f"💰 {ticker} 매수 체결 완료: {executed_quantity:.8f}개, 평균가 {avg_price:,.0f}원")
                    else:
                        trade_result.error_message = f"체결 내역 있으나 총 체결 수량 0. OrderID: {order_id}"
                        logger.error(f"❌ {ticker} 매수 체결 정보 오류: {trade_result.error_message}")

                elif executed_quantity > 0:
                    # trades 정보 없지만 executed_volume은 있는 경우 (업비트에서 가끔 발생)
                    filled_amount = executed_quantity * current_price
                    fee = filled_amount * self.config.taker_fee_rate

                    # 전량 체결 또는 부분 체결 판단
                    if executed_quantity >= trade_result.requested_quantity * 0.99:
                        trade_result.status = TradeStatus.FULL_FILLED
                        self.trading_stats['orders_successful'] += 1  # 전량 체결만 성공으로 카운팅
                    else:
                        trade_result.status = TradeStatus.PARTIAL_FILLED
                        self.trading_stats['orders_partial_filled'] += 1  # 부분 체결로 분류

                    trade_result.filled_quantity = executed_quantity
                    trade_result.average_price = current_price  # 현재가로 대체
                    trade_result.filled_amount = filled_amount
                    trade_result.fees = fee
                    trade_result.error_message = "Trades 정보 없음 - 현재가로 평균단가 대체"
                    self.trading_stats['total_volume_krw'] += filled_amount
                    self.trading_stats['total_fees_krw'] += fee

                    logger.warning(f"⚠️ {ticker} 매수 체결 완료 (trades 정보 없음): {executed_quantity:.8f}개, 현재가 {current_price:,.0f}원으로 기록")
                else:
                    trade_result.error_message = f"주문 'done'이지만 executed_volume=0이고 trades 없음. OrderID: {order_id}"
                    logger.error(f"❌ {ticker} 매수 체결 오류: {trade_result.error_message}")

            elif order_detail:
                # cancel 상태에서도 체결된 수량이 있으면 부분 체결 성공 처리
                order_state = order_detail.get('state', 'unknown')
                executed_quantity = float(order_detail.get('executed_volume', 0))

                if order_state == 'cancel' and executed_quantity > 0:
                    trades = order_detail.get('trades', [])

                    if trades:
                        total_value = sum(float(trade['price']) * float(trade['volume']) for trade in trades)
                        total_volume = sum(float(trade['volume']) for trade in trades)

                        if total_volume > 0:
                            avg_price = total_value / total_volume
                            fee = total_value * self.config.taker_fee_rate

                            trade_result.status = TradeStatus.PARTIAL_FILLED
                            trade_result.filled_quantity = executed_quantity
                            trade_result.average_price = avg_price
                            trade_result.filled_amount = total_value
                            trade_result.fees = fee
                            trade_result.error_message = "부분 체결 후 취소"

                            self.trading_stats['orders_partial_cancelled'] += 1  # 부분 체결 후 취소로 분류
                            self.trading_stats['total_volume_krw'] += total_value
                            self.trading_stats['total_fees_krw'] += fee

                            logger.info(f"💰 {ticker} 매수 부분 체결 완료 (cancel): {executed_quantity:.8f}개, 평균가 {avg_price:,.0f}원")

                            # 부분 체결도 성공이므로 PyramidStateManager에 반영
                            self.process_trade_result(
                                trade_result,
                                ticker,
                                is_pyramid=is_pyramid,
                                requested_amount=amount_krw
                            )
                        else:
                            # trades 있지만 volume 합계가 0인 경우
                            fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                            trade_result.status = TradeStatus.PARTIAL_FILLED_NO_AVG
                            trade_result.filled_quantity = executed_quantity
                            trade_result.average_price = current_price
                            trade_result.filled_amount = executed_quantity * current_price
                            trade_result.fees = fee
                            trade_result.error_message = "부분 체결 후 취소, trades 정보 불완전"

                            self.trading_stats['orders_partial_cancelled'] += 1  # 부분 체결 후 취소로 분류
                            self.trading_stats['total_volume_krw'] += trade_result.amount_krw
                            self.trading_stats['total_fees_krw'] += fee

                            logger.warning(f"⚠️ {ticker} 매수 부분 체결 (trades 정보 불완전): {executed_quantity:.8f}개, 현재가로 기록")

                            # 부분 체결도 성공이므로 PyramidStateManager에 반영
                            self.process_trade_result(
                                trade_result,
                                ticker,
                                is_pyramid=is_pyramid,
                                requested_amount=amount_krw
                            )
                    else:
                        # trades 없지만 executed_quantity는 있는 경우
                        fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                        trade_result.status = TradeStatus.PARTIAL_FILLED
                        trade_result.filled_quantity = executed_quantity
                        trade_result.average_price = current_price
                        trade_result.filled_amount = executed_quantity * current_price
                        trade_result.fees = fee
                        trade_result.error_message = "부분 체결 후 취소, trades 정보 없음"

                        self.trading_stats['orders_partial_cancelled'] += 1  # 부분 체결 후 취소로 분류
                        self.trading_stats['total_volume_krw'] += trade_result.amount_krw
                        self.trading_stats['total_fees_krw'] += fee

                        logger.warning(f"⚠️ {ticker} 매수 부분 체결 (trades 정보 없음): {executed_quantity:.8f}개, 현재가로 기록")

                        # 부분 체결도 성공이므로 PyramidStateManager에 반영
                        self.process_trade_result(
                            trade_result,
                            ticker,
                            is_pyramid=is_pyramid,
                            requested_amount=amount_krw
                        )
                else:
                    # 실제로 실패한 경우
                    trade_result.error_message = f"주문 미체결: state={order_state}, executed_volume={executed_quantity}, OrderID: {order_id}"
                    logger.error(f"❌ {ticker} 매수 주문 체결 실패: {trade_result.error_message}")

            else:
                trade_result.error_message = f"주문 상세 정보 조회 실패. OrderID: {order_id}"
                logger.error(f"❌ {ticker} 매수 주문 상세 정보 조회 실패")

            # API 레이트 리미트 고려
            time.sleep(self.config.api_rate_limit_delay)

        except pyupbit.UpbitError as ue:
            trade_result.error_message = f"업비트 API 오류: {str(ue)}"
            logger.error(f"❌ {ticker} 매수 중 업비트 API 오류: {ue}")
            # API 관련 오류는 재시도할 수 있으므로 raise하여 @retry가 처리하도록 함
            raise

        except Exception as e:
            trade_result.error_message = f"매수 실행 중 예상치 못한 오류: {e}"
            logger.error(f"❌ {ticker} 매수 실행 중 예상치 못한 오류: {e}")
            # Exception 케이스는 저장 후 return 필요
            self.save_trade_record(trade_result, ticker, is_pyramid=is_pyramid, requested_amount=amount_krw)
            return trade_result

        finally:
            # 중복 저장 방지:
            # 1. 성공 케이스는 process_trade_result에서 이미 저장됨
            # 2. 실패 케이스는 각각의 early return에서 이미 저장됨
            # 3. Exception 케이스도 이제 저장 후 return됨
            # finally에서는 저장하지 않음 (중복 방지)
            pass

        return trade_result

    @retry(max_attempts=3, initial_delay=1, backoff=2)
    def execute_sell_order(self, ticker: str, quantity: Optional[float] = None) -> TradeResult:
        """매도 주문 실행"""
        trade_result = TradeResult(
            ticker=ticker,
            order_id="PENDING",  # 임시 order_id, 실제 주문 후 업데이트
            status=TradeStatus.FAILED,
            requested_amount=0.0,  # 매도는 금액이 아닌 수량 기준이므로 0으로 초기화
            requested_quantity=quantity or 0.0,
            timestamp=datetime.now()
        )

        try:
            self.trading_stats['orders_attempted'] += 1

            # 티커 형식 확인
            if not ticker.startswith("KRW-"):
                ticker = f"KRW-{ticker}"

            currency = ticker.replace("KRW-", "")

            # DRY RUN 모드
            if self.dry_run:
                logger.info(f"🧪 DRY RUN: {ticker} 매도 주문")
                trade_result.status = TradeStatus.FULL_FILLED
                dry_run_quantity = quantity or 1.0
                trade_result.requested_quantity = dry_run_quantity
                trade_result.filled_quantity = dry_run_quantity
                trade_result.average_price = pyupbit.get_current_price(ticker)
                self.trading_stats['orders_successful'] += 1
                self.save_trade_record(trade_result, ticker, is_pyramid=False, requested_amount=0, trade_type='SELL')
                return trade_result

            # 보유 수량 확인
            balance = self.upbit.get_balance(currency)

            # API 오류 응답 처리
            if isinstance(balance, dict) and 'error' in balance:
                error_info = balance['error']
                trade_result.error_message = f"API 오류: {error_info.get('name', 'unknown')}"
                trade_result.status = TradeStatus.FAILED
                logger.warning(f"⚠️ {ticker}: 업비트 API 오류 - {error_info.get('message', 'no message')}")
                self.save_trade_record(trade_result, trade_type='SELL')
                return trade_result

            if not balance or balance <= 0:
                trade_result.error_message = "보유 수량 없음"
                trade_result.status = TradeStatus.CANCELLED
                logger.warning(f"⚠️ {ticker}: 보유 수량 없음")
                self.save_trade_record(trade_result, trade_type='SELL')
                return trade_result

            # 매도 수량 결정
            sell_quantity = quantity if quantity and quantity <= balance else balance

            # TradeResult에 실제 요청 수량 반영 (DB 제약 조건 충족용)
            trade_result.requested_quantity = sell_quantity

            # 현재가 조회
            current_price = pyupbit.get_current_price(ticker)
            if not current_price:
                trade_result.error_message = "현재가 조회 실패"
                logger.error(f"❌ {ticker}: {trade_result.error_message}")
                self.save_trade_record(trade_result, trade_type='SELL')
                return trade_result

            # 최소 매도 금액 확인
            estimated_value = sell_quantity * current_price
            net_value = estimated_value * (1 - self.config.taker_fee_rate)

            if net_value < 5000:  # 업비트 최소 매도 금액
                trade_result.error_message = f"매도 금액 부족: {net_value:,.0f} < 5,000원"
                trade_result.status = TradeStatus.CANCELLED
                logger.warning(f"⚠️ {ticker}: {trade_result.error_message}")
                self.save_trade_record(trade_result, trade_type='SELL')
                return trade_result

            logger.info(f"🚀 {ticker} 시장가 매도 주문: {sell_quantity:.8f}개 (현재가: {current_price:,.0f})")

            # 업비트 매도 주문
            response = self.upbit.sell_market_order(ticker, sell_quantity)

            if not response or not response.get('uuid'):
                trade_result.error_message = f"주문 접수 실패: {response}"
                logger.error(f"❌ {ticker} 매도 주문 접수 실패")
                self.save_trade_record(trade_result, trade_type='SELL')
                return trade_result

            order_id = response['uuid']
            trade_result.order_id = order_id

            logger.info(f"✅ {ticker} 매도 주문 접수 성공 (주문ID: {order_id})")

            # 주문 체결 확인
            time.sleep(5)
            order_detail = self.upbit.get_order(order_id)

            if order_detail and order_detail.get('state') == 'done':
                executed_quantity = float(order_detail.get('executed_volume', 0))
                trades = order_detail.get('trades', [])

                if trades and executed_quantity > 0:
                    # 평균 체결가 계산
                    total_value = sum(float(trade['price']) * float(trade['volume']) for trade in trades)
                    total_volume = sum(float(trade['volume']) for trade in trades)

                    if total_volume > 0:
                        avg_price = total_value / total_volume
                        fee = total_value * self.config.taker_fee_rate

                        trade_result.status = TradeStatus.FULL_FILLED
                        trade_result.filled_quantity = executed_quantity
                        trade_result.average_price = avg_price
                        trade_result.filled_amount = total_value
                        trade_result.fees = fee

                        self.trading_stats['orders_successful'] += 1
                        self.trading_stats['total_volume_krw'] += total_value
                        self.trading_stats['total_fees_krw'] += fee

                        logger.info(f"💰 {ticker} 매도 체결 완료: {executed_quantity:.8f}개, 평균가 {avg_price:,.0f}원")
                    else:
                        trade_result.error_message = f"매도 체결 내역 있으나 총 체결 수량 0. OrderID: {order_id}"
                        logger.error(f"❌ {ticker} 매도 체결 정보 오류: {trade_result.error_message}")

                elif executed_quantity > 0:
                    # trades 정보 없지만 executed_volume은 있는 경우
                    fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                    trade_result.status = TradeStatus.FULL_FILLED
                    trade_result.filled_quantity = executed_quantity
                    trade_result.average_price = current_price
                    trade_result.filled_amount = executed_quantity * current_price
                    trade_result.fees = fee
                    trade_result.error_message = "Trades 정보 없음 - 현재가로 평균단가 대체"

                    self.trading_stats['orders_successful'] += 1
                    self.trading_stats['total_volume_krw'] += trade_result.filled_amount
                    self.trading_stats['total_fees_krw'] += fee

                    logger.warning(f"⚠️ {ticker} 매도 체결 완료 (trades 정보 없음): {executed_quantity:.8f}개, 현재가로 기록")
                else:
                    trade_result.error_message = f"매도 주문 'done'이지만 executed_volume=0이고 trades 없음. OrderID: {order_id}"
                    logger.error(f"❌ {ticker} 매도 체결 오류: {trade_result.error_message}")

            elif order_detail:
                # cancel 상태에서도 체결된 수량이 있으면 부분 체결 성공 처리
                order_state = order_detail.get('state', 'unknown')
                executed_quantity = float(order_detail.get('executed_volume', 0))

                if order_state == 'cancel' and executed_quantity > 0:
                    trades = order_detail.get('trades', [])

                    if trades:
                        total_value = sum(float(trade['price']) * float(trade['volume']) for trade in trades)
                        total_volume = sum(float(trade['volume']) for trade in trades)

                        if total_volume > 0:
                            avg_price = total_value / total_volume
                            fee = total_value * self.config.taker_fee_rate

                            trade_result.status = TradeStatus.PARTIAL_FILLED
                            trade_result.filled_quantity = executed_quantity
                            trade_result.average_price = avg_price
                            trade_result.filled_amount = total_value
                            trade_result.fees = fee
                            trade_result.error_message = "부분 체결 후 취소"

                            self.trading_stats['orders_partial_cancelled'] += 1  # 부분 체결 후 취소로 분류
                            self.trading_stats['total_volume_krw'] += total_value
                            self.trading_stats['total_fees_krw'] += fee

                            logger.info(f"💰 {ticker} 매도 부분 체결 완료 (cancel): {executed_quantity:.8f}개, 평균가 {avg_price:,.0f}원")
                        else:
                            fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                            trade_result.status = TradeStatus.PARTIAL_FILLED_NO_AVG
                            trade_result.filled_quantity = executed_quantity
                            trade_result.average_price = current_price
                            trade_result.filled_amount = executed_quantity * current_price
                            trade_result.fees = fee
                            trade_result.error_message = "부분 체결 후 취소, trades 정보 불완전"

                            self.trading_stats['orders_partial_cancelled'] += 1  # 부분 체결 후 취소로 분류
                            self.trading_stats['total_volume_krw'] += trade_result.amount_krw
                            self.trading_stats['total_fees_krw'] += fee

                            logger.warning(f"⚠️ {ticker} 매도 부분 체결 (trades 정보 불완전): {executed_quantity:.8f}개, 현재가로 기록")
                    else:
                        fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                        trade_result.status = TradeStatus.PARTIAL_FILLED
                        trade_result.filled_quantity = executed_quantity
                        trade_result.average_price = current_price
                        trade_result.filled_amount = executed_quantity * current_price
                        trade_result.fees = fee
                        trade_result.error_message = "부분 체결 후 취소, trades 정보 없음"

                        self.trading_stats['orders_partial_cancelled'] += 1  # 부분 체결 후 취소로 분류
                        self.trading_stats['total_volume_krw'] += trade_result.amount_krw
                        self.trading_stats['total_fees_krw'] += fee

                        logger.warning(f"⚠️ {ticker} 매도 부분 체결 (trades 정보 없음): {executed_quantity:.8f}개, 현재가로 기록")
                else:
                    trade_result.error_message = f"매도 주문 미체결: state={order_state}, executed_volume={executed_quantity}, OrderID: {order_id}"
                    logger.error(f"❌ {ticker} 매도 주문 체결 실패: {trade_result.error_message}")

            else:
                trade_result.error_message = f"매도 주문 상세 정보 조회 실패. OrderID: {order_id}"
                logger.error(f"❌ {ticker} 매도 주문 상세 정보 조회 실패")

            # API 레이트 리미트 고려
            time.sleep(self.config.api_rate_limit_delay)

        except pyupbit.UpbitError as ue:
            trade_result.error_message = f"업비트 API 오류: {str(ue)}"
            logger.error(f"❌ {ticker} 매도 중 업비트 API 오류: {ue}")
            # API 관련 오류는 재시도할 수 있으므로 raise하여 @retry가 처리하도록 함
            raise

        except Exception as e:
            trade_result.error_message = f"매도 실행 중 예상치 못한 오류: {e}"
            logger.error(f"❌ {ticker} 매도 실행 중 예상치 못한 오류: {e}")

        finally:
            self.save_trade_record(trade_result)

        return trade_result

    def calculate_fee_adjusted_amount(self, amount_krw: float, is_buy: bool = True) -> Dict[str, float]:
        """
        수수료를 고려한 정확한 주문 금액 계산 (trade_executor.py에서 이식)

        Args:
            amount_krw: 목표 거래 금액
            is_buy: True면 매수, False면 매도

        Returns:
            Dict: {
                'target_amount': 목표 금액,
                'order_amount': 실제 주문 금액,
                'expected_fee': 예상 수수료,
                'net_amount': 순 거래 금액
            }
        """
        fee_rate = self.config.taker_fee_rate  # 시장가 주문은 항상 Taker

        if is_buy:
            # 매수: 목표 금액을 위해 수수료만큼 추가 주문 필요
            # 목표 금액 = 주문 금액 / (1 + 수수료율)
            order_amount = amount_krw / (1 + fee_rate)
            expected_fee = order_amount * fee_rate
            net_amount = order_amount
        else:
            # 매도: 매도 금액에서 수수료 차감
            order_amount = amount_krw
            expected_fee = amount_krw * fee_rate
            net_amount = amount_krw - expected_fee

        return {
            'target_amount': amount_krw,
            'order_amount': order_amount,
            'expected_fee': expected_fee,
            'net_amount': net_amount
        }

    def should_exit_trade(self, ticker: str, current_price: float,
                         gpt_analysis: Optional[str] = None) -> Tuple[bool, str]:
        """
        기술적 지표와 GPT 분석을 바탕으로 매도 여부 판단 (trade_executor.py에서 이식)

        Args:
            ticker: 종목 코드
            current_price: 현재가
            gpt_analysis: GPT 분석 결과 (선택)

        Returns:
            Tuple[bool, str]: (매도 여부, 사유)
        """
        try:
            # 현재가 유효성 검증
            if current_price is None:
                logger.warning(f"⚠️ {ticker} 현재가가 None입니다. 매도 조건 확인을 건너뜁니다.")
                return False, "현재가 정보 없음"

            # 1. 트레일링 스탑 확인 (최우선)
            trailing_exit = self.trailing_stop_manager.update(ticker, current_price, self.db_path)
            if trailing_exit:
                return True, "ATR 기반 트레일링 스탑 청산"

            # 2. SQLite에서 기술적 지표 조회
            market_data = {}
            with get_db_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT supertrend, macd_histogram, support_level, adx
                    FROM technical_analysis
                    WHERE ticker = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (ticker,))
                result = cursor.fetchone()

                if result:
                    market_data = {
                        'price': current_price,
                        'supertrend': self._safe_convert_to_float(result[0], None),
                        'macd_histogram': self._safe_convert_to_float(result[1], 0.0),
                        'support': self._safe_convert_to_float(result[2], None),
                        'adx': self._safe_convert_to_float(result[3], 0.0)
                    }

            # 3. 지지선 하회 조건 (최우선 매도 신호)
            if (market_data.get("support") is not None and
                current_price is not None and
                current_price < market_data.get("support")):
                return True, f"지지선 하회 (현재가: {current_price:.0f} < 지지선: {market_data.get('support'):.0f})"

            # 4. 기술적 매도 조건
            tech_exit = False
            tech_reason = ""

            # Supertrend 하향 돌파
            if (market_data.get("supertrend") is not None and
                current_price is not None and
                current_price < market_data.get("supertrend")):
                tech_exit = True
                tech_reason = f"Supertrend 하향 돌파 (현재가: {current_price:.0f} < ST: {market_data.get('supertrend'):.0f})"

            # MACD 히스토그램 음전환
            elif market_data.get("macd_histogram", 0) < 0:
                tech_exit = True
                tech_reason = f"MACD 히스토그램 음전환 ({market_data.get('macd_histogram'):.4f})"

            # 5. GPT 분석 기반 매도 신호
            gpt_exit = False
            if gpt_analysis and "sell" in gpt_analysis.lower():
                gpt_exit = True

            # 6. 종합 판단
            if tech_exit and gpt_exit:
                return True, f"기술적 + GPT 매도 신호 ({tech_reason})"
            elif tech_exit:
                return True, f"기술적 매도 신호 ({tech_reason})"

            return False, "매도 조건 미충족"

        except Exception as e:
            logger.error(f"❌ {ticker} 매도 조건 확인 실패: {e}")
            return False, f"매도 조건 확인 오류: {e}"

    def check_sell_conditions(self, position: PositionInfo) -> Tuple[bool, str]:
        """
        매도 조건 확인 (전략적 매도 조건 + 기술적 매도 조건 통합)

        우선순위:
        1. 트레일링 스탑 및 기술적 신호 (should_exit_trade)
        2. 전략적 손절/익절 조건
        3. 장기 보유 익절 조건
        """
        try:
            # 1. 고급 기술적 매도 조건 확인 (GPT 분석 포함)
            gpt_analysis = None
            try:
                with get_db_connection_context() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT analysis_result FROM gpt_analysis
                        WHERE ticker = ?
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (position.ticker,))
                    result = cursor.fetchone()
                    if result:
                        gpt_analysis = result[0]
            except Exception as gpt_e:
                logger.warning(f"⚠️ {position.ticker} GPT 분석 조회 실패: {gpt_e}")

            # should_exit_trade 함수로 고급 매도 조건 확인
            advanced_exit, advanced_reason = self.should_exit_trade(
                position.ticker, position.current_price, gpt_analysis
            )

            if advanced_exit:
                return True, f"고급 매도 신호: {advanced_reason}"

            # 2. 전략적 손절 조건 (마크 미너비니 7-8% 규칙)
            if position.unrealized_pnl_percent <= self.config.stop_loss_percent:
                return True, f"전략적 손절 ({position.unrealized_pnl_percent:.1f}% ≤ {self.config.stop_loss_percent}%)"

            # 3. 전략적 익절 조건 (윌리엄 오닐 20-25% 규칙)
            if position.unrealized_pnl_percent >= self.config.take_profit_percent:
                return True, f"전략적 익절 ({position.unrealized_pnl_percent:.1f}% ≥ {self.config.take_profit_percent}%)"

            # 4. 장기 보유 익절 조건 (30일 이상 보유, 10% 이상 수익)
            if position.hold_days >= 30 and position.unrealized_pnl_percent >= 10.0:
                return True, f"장기 보유 익절 ({position.hold_days}일 보유, {position.unrealized_pnl_percent:.1f}% 수익)"

            return False, "매도 조건 미충족"

        except Exception as e:
            logger.error(f"❌ {position.ticker} 매도 조건 확인 실패: {e}")
            return False, f"매도 조건 확인 오류: {e}"

    def process_portfolio_management(self) -> Dict[str, Any]:
        """포트폴리오 관리 및 매도 실행"""
        management_result = {
            'positions_checked': 0,
            'sell_orders_executed': 0,
            'errors': []
        }

        try:
            logger.info("📊 포트폴리오 관리 시작")

            # 포트폴리오 동기화 상태 검증
            if not self.validate_portfolio_sync():
                logger.warning("⚠️ 포트폴리오 동기화 불일치로 인해 일부 기능이 제한될 수 있습니다.")

            positions = self.get_current_positions()

            if not positions:
                logger.info("📭 관리할 포지션이 없습니다")
                return management_result

            management_result['positions_checked'] = len(positions)

            for position in positions:
                try:
                    logger.info(f"📈 {position.ticker}: {position.unrealized_pnl_percent:+.1f}% "
                              f"({position.hold_days}일 보유)")

                    # 매도 조건 확인
                    should_sell, reason = self.check_sell_conditions(position)

                    if should_sell:
                        logger.info(f"💹 {position.ticker} 매도 실행: {reason}")

                        # 매도 주문 실행
                        sell_result = self.execute_sell_order(position.ticker)

                        if sell_result.status in [
                            OrderStatus.SUCCESS,
                            OrderStatus.SUCCESS_NO_AVG_PRICE,
                            OrderStatus.SUCCESS_PARTIAL,
                            OrderStatus.SUCCESS_PARTIAL_NO_AVG
                        ]:
                            management_result['sell_orders_executed'] += 1
                            logger.info(f"✅ {position.ticker} 매도 성공")

                            # 포트폴리오 업데이트 로그
                            realized_pnl = (sell_result.price - position.avg_buy_price) * sell_result.quantity
                            realized_pnl_percent = (realized_pnl / (position.avg_buy_price * sell_result.quantity)) * 100

                            logger.info(f"💰 실현 손익: {realized_pnl:+,.0f}원 ({realized_pnl_percent:+.1f}%)")

                        else:
                            error_msg = f"{position.ticker} 매도 실패: {sell_result.error_message}"
                            management_result['errors'].append(error_msg)
                            logger.warning(f"⚠️ {error_msg}")

                    else:
                        logger.info(f"✅ {position.ticker}: {reason}")

                    # API 레이트 리미트
                    time.sleep(0.2)

                except Exception as e:
                    error_msg = f"{position.ticker} 관리 중 오류: {e}"
                    management_result['errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")

            logger.info(f"✅ 포트폴리오 관리 완료: {management_result['sell_orders_executed']}개 매도 실행")

        except Exception as e:
            error_msg = f"포트폴리오 관리 실패: {e}"
            management_result['errors'].append(error_msg)
            logger.error(f"❌ {error_msg}")

        return management_result

    def get_trading_statistics(self) -> Dict[str, Any]:
        """거래 통계 조회"""
        try:
            session_duration = datetime.now() - self.trading_stats['session_start']

            stats = {
                'session_duration_minutes': session_duration.total_seconds() / 60,
                'orders_attempted': self.trading_stats['orders_attempted'],
                'orders_successful': self.trading_stats['orders_successful'],  # 전량 체결만
                'orders_partial_filled': self.trading_stats['orders_partial_filled'],  # 부분 체결
                'orders_partial_cancelled': self.trading_stats['orders_partial_cancelled'],  # 부분 체결 후 취소
                'success_rate': (  # 전량 체결률
                    self.trading_stats['orders_successful'] / self.trading_stats['orders_attempted'] * 100
                    if self.trading_stats['orders_attempted'] > 0 else 0
                ),
                'completion_rate': (  # 전량+부분 체결률 (어떤 형태든 체결됨)
                    (self.trading_stats['orders_successful'] + self.trading_stats['orders_partial_filled'] + self.trading_stats['orders_partial_cancelled'])
                    / self.trading_stats['orders_attempted'] * 100
                    if self.trading_stats['orders_attempted'] > 0 else 0
                ),
                'total_volume_krw': self.trading_stats['total_volume_krw'],
                'total_fees_krw': self.trading_stats['total_fees_krw'],
                'average_order_size': (
                    self.trading_stats['total_volume_krw'] / (self.trading_stats['orders_successful'] + self.trading_stats['orders_partial_filled'] + self.trading_stats['orders_partial_cancelled'])
                    if (self.trading_stats['orders_successful'] + self.trading_stats['orders_partial_filled'] + self.trading_stats['orders_partial_cancelled']) > 0 else 0
                )
            }

            return stats

        except Exception as e:
            logger.error(f"❌ 거래 통계 조회 실패: {e}")
            return {}

    def execute_trading_session(self, candidates: List[str], position_sizes: Dict[str, float],
                               market_sentiment_adjustment: float) -> Dict[str, Any]:
        """거래 세션 실행"""
        session_result = {
            'buy_orders': [],
            'sell_orders': [],
            'portfolio_management': {},
            'statistics': {}
        }

        try:
            logger.info(f"🚀 거래 세션 시작: {len(candidates)}개 매수 후보")

            # 1. 매수 주문 실행
            for ticker in candidates:
                if ticker not in position_sizes:
                    continue

                kelly_percentage = position_sizes[ticker]
                amount_krw = self.calculate_position_size(ticker, kelly_percentage, market_sentiment_adjustment)

                if amount_krw > 0:
                    buy_result = self.execute_buy_order(ticker, amount_krw, is_pyramid=False)
                    session_result['buy_orders'].append({
                        'ticker': ticker,
                        'result': buy_result,
                        'target_amount': amount_krw
                    })

            # 2. 포트폴리오 관리 (매도 조건 확인)
            portfolio_result = self.process_portfolio_management()
            session_result['portfolio_management'] = portfolio_result

            # 3. 거래 통계
            session_result['statistics'] = self.get_trading_statistics()

            # 4. 세션 결과 요약 (모든 성공 상태 포함)
            successful_buys = sum(1 for order in session_result['buy_orders']
                                if order['result'].status in [
                                    OrderStatus.SUCCESS,
                                    OrderStatus.SUCCESS_NO_AVG_PRICE,
                                    OrderStatus.SUCCESS_PARTIAL,
                                    OrderStatus.SUCCESS_PARTIAL_NO_AVG
                                ])
            successful_sells = portfolio_result.get('sell_orders_executed', 0)

            logger.info("="*60)
            logger.info("📊 거래 세션 결과 요약")
            logger.info("="*60)
            logger.info(f"💸 매수 성공: {successful_buys}개")
            logger.info(f"💹 매도 성공: {successful_sells}개")
            logger.info(f"📈 총 거래량: {session_result['statistics'].get('total_volume_krw', 0):,.0f}원")
            logger.info(f"💰 총 수수료: {session_result['statistics'].get('total_fees_krw', 0):,.0f}원")
            logger.info(f"🎯 성공률: {session_result['statistics'].get('success_rate', 0):.1f}%")
            logger.info("="*60)

        except Exception as e:
            logger.error(f"❌ 거래 세션 실행 실패: {e}")

        return session_result

    def check_pyramid_opportunities(self) -> Dict[str, Dict]:
        """PyramidStateManager를 통한 피라미딩 기회 확인"""
        try:
            # PyramidStateManager의 get_pyramid_opportunities() 메서드 사용
            pyramid_opportunities = self.pyramid_state_manager.get_pyramid_opportunities()

            if pyramid_opportunities:
                logger.info(f"📈 총 {len(pyramid_opportunities)}개 종목에서 피라미딩 기회 발견")
            else:
                logger.info("📊 현재 피라미딩 기회 없음")

            return pyramid_opportunities

        except Exception as e:
            logger.error(f"❌ 피라미딩 기회 확인 실패: {e}")
            return {}

    def execute_pyramid_trades(self, pyramid_opportunities: Dict[str, Dict]) -> Dict[str, Any]:
        """피라미딩 거래 실행"""
        pyramid_results = {
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'details': []
        }

        try:
            for ticker, opportunity in pyramid_opportunities.items():
                try:
                    pyramid_results['attempted'] += 1

                    # 총 자산 조회
                    total_balance = self.get_total_balance_krw()
                    if total_balance <= 0:
                        continue

                    # 추가 투자 금액 계산
                    additional_position_pct = opportunity['additional_position_pct']
                    additional_amount = total_balance * (additional_position_pct / 100)

                    if additional_amount < self.config.min_order_amount_krw:
                        logger.info(f"⏭️ {ticker}: 피라미딩 금액 부족 ({additional_amount:,.0f}원)")
                        continue

                    logger.info(f"🔺 {ticker} 피라미딩 매수 실행: {additional_position_pct:.1f}% ({additional_amount:,.0f}원)")

                    # 피라미딩 매수 실행
                    trade_result = self.execute_buy_order(ticker, additional_amount, is_pyramid=True)

                    if trade_result.status in [
                        OrderStatus.SUCCESS,
                        OrderStatus.SUCCESS_NO_AVG_PRICE,
                        OrderStatus.SUCCESS_PARTIAL,
                        OrderStatus.SUCCESS_PARTIAL_NO_AVG
                    ]:
                        # 통합된 피라미딩 거래 결과 처리 (새로운 TradeResult 활용)
                        pyramid_trade_result = self.process_trade_result(
                            trade_result,
                            ticker,
                            is_pyramid=True,  # 이것은 피라미딩 거래
                            requested_amount=additional_amount
                        )

                        pyramid_results['successful'] += 1
                        pyramid_results['details'].append({
                            'ticker': ticker,
                            'status': 'success',
                            'amount': additional_amount,
                            'price': trade_result.price
                        })

                        logger.info(f"✅ {ticker} 피라미딩 매수 성공")
                    else:
                        pyramid_results['failed'] += 1
                        pyramid_results['details'].append({
                            'ticker': ticker,
                            'status': 'failed',
                            'error': trade_result.error_message
                        })

                        logger.warning(f"❌ {ticker} 피라미딩 매수 실패: {trade_result.error_message}")

                    # API 레이트 리미트
                    time.sleep(self.config.api_rate_limit_delay)

                except Exception as e:
                    pyramid_results['failed'] += 1
                    pyramid_results['details'].append({
                        'ticker': ticker,
                        'status': 'error',
                        'error': str(e)
                    })
                    logger.error(f"❌ {ticker} 피라미딩 실행 오류: {e}")

            # 피라미딩 결과 요약
            if pyramid_results['attempted'] > 0:
                logger.info("="*50)
                logger.info("🔺 피라미딩 거래 결과 요약")
                logger.info("="*50)
                logger.info(f"시도: {pyramid_results['attempted']}건")
                logger.info(f"성공: {pyramid_results['successful']}건")
                logger.info(f"실패: {pyramid_results['failed']}건")
                logger.info("="*50)

        except Exception as e:
            logger.error(f"❌ 피라미딩 거래 실행 실패: {e}")

        return pyramid_results

    def process_enhanced_portfolio_management(self) -> Dict[str, Any]:
        """향상된 포트폴리오 관리 (피라미딩 + 트레일링 스탑)"""
        management_result = {
            'positions_checked': 0,
            'sell_orders_executed': 0,
            'pyramid_trades': {},
            'errors': []
        }

        try:
            logger.info("🎯 향상된 포트폴리오 관리 시작 (피라미딩 + 트레일링 스탑)")

            # 1. 기본 포트폴리오 관리 (매도 조건 확인)
            basic_result = self.process_portfolio_management()
            management_result.update(basic_result)

            # 2. 피라미딩 기회 확인 및 실행
            pyramid_opportunities = self.check_pyramid_opportunities()

            if pyramid_opportunities:
                logger.info(f"🔺 {len(pyramid_opportunities)}개 종목에서 피라미딩 실행")
                pyramid_results = self.execute_pyramid_trades(pyramid_opportunities)
                management_result['pyramid_trades'] = pyramid_results
            else:
                logger.info("📊 피라미딩 기회 없음")
                management_result['pyramid_trades'] = {'attempted': 0, 'successful': 0, 'failed': 0}

            # 3. 결과 요약
            total_trades = (management_result['sell_orders_executed'] +
                           management_result['pyramid_trades'].get('successful', 0))

            logger.info("🎯 향상된 포트폴리오 관리 완료")
            logger.info(f"   매도: {management_result['sell_orders_executed']}건")
            logger.info(f"   피라미딩: {management_result['pyramid_trades'].get('successful', 0)}건")
            logger.info(f"   총 거래: {total_trades}건")

        except Exception as e:
            error_msg = f"향상된 포트폴리오 관리 실패: {e}"
            management_result['errors'].append(error_msg)
            logger.error(f"❌ {error_msg}")

        return management_result

def main():
    """테스트용 메인 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='Trading Engine 테스트')
    parser.add_argument('--dry-run', action='store_true', help='DRY RUN 모드')
    parser.add_argument('--test-portfolio', action='store_true', help='포트폴리오 관리 테스트')

    args = parser.parse_args()

    # 설정 생성
    config = TradingConfig()

    # 거래 엔진 초기화
    engine = LocalTradingEngine(config, dry_run=args.dry_run)

    if args.test_portfolio:
        # 포트폴리오 관리 테스트
        logger.info("🧪 포트폴리오 관리 테스트")
        result = engine.process_portfolio_management()
        logger.info(f"결과: {result}")
    else:
        # 기본 기능 테스트
        logger.info("🧪 거래 엔진 기본 기능 테스트")

        # 테스트 데이터
        test_candidates = ["KRW-BTC"]
        test_position_sizes = {"KRW-BTC": 2.0}  # 2%
        test_market_adjustment = 1.0

        # 거래 세션 실행
        result = engine.execute_trading_session(
            test_candidates,
            test_position_sizes,
            test_market_adjustment
        )

        logger.info(f"세션 결과: {result}")

if __name__ == "__main__":
    main()