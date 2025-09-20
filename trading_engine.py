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

# 환경 변수 로드
load_dotenv()

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

@dataclass
class TradeResult:
    """거래 결과"""
    status: OrderStatus
    ticker: str
    order_type: OrderType
    order_id: Optional[str] = None
    quantity: Optional[float] = None
    price: Optional[float] = None
    amount_krw: Optional[float] = None
    fee: Optional[float] = None
    error_message: Optional[str] = None
    timestamp: Optional[datetime] = None

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
    take_profit_percent: float = 0  # 익절 비율 (%)
    taker_fee_rate: float = 0.00139  # Taker 수수료 (0.139%)
    maker_fee_rate: float = 0.0005  # Maker 수수료 (0.05%)
    api_rate_limit_delay: float = 0.5  # API 호출 간격 (초)

class PyramidingManager:
    """Stage 2 전고점 돌파 시 피라미딩 매수 관리자"""

    def __init__(self, max_pyramids: int = 3, pyramid_multiplier: float = 0.5):
        """
        Args:
            max_pyramids: 최대 피라미드 횟수
            pyramid_multiplier: 추가 매수 시 포지션 사이즈 배수 (0.5 = 50% 크기)
        """
        self.max_pyramids = max_pyramids
        self.pyramid_multiplier = pyramid_multiplier

        # 티커별 피라미딩 상태 추적
        self.initial_buy_price = {}      # 최초 매수가
        self.pyramid_levels = {}         # 현재 피라미드 레벨
        self.last_breakout_price = {}    # 마지막 돌파 가격
        self.highest_after_buy = {}      # 마지막 매수 후 최고가

    def register_initial_buy(self, ticker: str, buy_price: float):
        """초기 매수 등록"""
        self.initial_buy_price[ticker] = buy_price
        self.pyramid_levels[ticker] = 1
        self.last_breakout_price[ticker] = buy_price
        self.highest_after_buy[ticker] = buy_price

        logger.info(f"🏗️ {ticker} 피라미딩 초기화: 진입가={buy_price:.0f}원, 레벨=1")

    def check_pyramid_opportunity(self, ticker: str, current_price: float,
                                db_path: str = "./makenaide_local.db") -> Tuple[bool, float]:
        """
        피라미딩 기회 확인

        Args:
            ticker: 종목 코드
            current_price: 현재가
            db_path: SQLite DB 경로

        Returns:
            Tuple[bool, float]: (추가매수 여부, 추가매수 포지션 비율)
        """
        try:
            # 기본 조건 확인
            if ticker not in self.initial_buy_price:
                return False, 0.0

            if self.pyramid_levels[ticker] >= self.max_pyramids:
                return False, 0.0

            # 최고가 업데이트
            if current_price > self.highest_after_buy[ticker]:
                self.highest_after_buy[ticker] = current_price

            # Stage 2 상태 확인
            stage2_confirmed = self._check_stage2_status(ticker, db_path)
            if not stage2_confirmed:
                return False, 0.0

            # 전고점 돌파 확인 (5% 이상 상승)
            min_breakout_threshold = self.last_breakout_price[ticker] * 1.05

            if current_price >= min_breakout_threshold:
                # 추가 매수 조건 만족
                additional_position = self._calculate_pyramid_size(ticker)

                logger.info(f"🚀 {ticker} 피라미딩 기회 감지:")
                logger.info(f"   현재가: {current_price:.0f}원")
                logger.info(f"   돌파 기준: {min_breakout_threshold:.0f}원")
                logger.info(f"   현재 레벨: {self.pyramid_levels[ticker]}")
                logger.info(f"   추가 포지션: {additional_position:.1f}%")

                return True, additional_position

            return False, 0.0

        except Exception as e:
            logger.error(f"❌ {ticker} 피라미딩 기회 확인 실패: {e}")
            return False, 0.0

    def _check_stage2_status(self, ticker: str, db_path: str) -> bool:
        """Stage 2 상태 확인"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT current_stage, ma200_trend, volume_surge, breakout_strength
                    FROM technical_analysis
                    WHERE ticker = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (ticker,))
                result = cursor.fetchone()

                if not result:
                    return False

                stage, ma200_trend, volume_surge, breakout_strength = result

                # Stage 2 조건
                is_stage2 = (stage == 2 and
                           ma200_trend and ma200_trend > 0 and
                           volume_surge and volume_surge >= 1.3)

                return is_stage2

        except Exception as e:
            logger.warning(f"⚠️ {ticker} Stage 2 상태 확인 실패: {e}")
            return False

    def _calculate_pyramid_size(self, ticker: str) -> float:
        """피라미드 추가 매수 크기 계산"""
        current_level = self.pyramid_levels.get(ticker, 1)

        # 첫 번째 추가매수: 2.0% (초기 포지션의 50%)
        # 두 번째 추가매수: 1.0% (초기 포지션의 25%)
        # 세 번째 추가매수: 0.5% (초기 포지션의 12.5%)
        base_size = 2.0
        return base_size * (self.pyramid_multiplier ** (current_level - 1))

    def execute_pyramid_buy(self, ticker: str, current_price: float) -> bool:
        """피라미딩 매수 실행 후 상태 업데이트"""
        try:
            self.pyramid_levels[ticker] += 1
            self.last_breakout_price[ticker] = current_price
            self.highest_after_buy[ticker] = current_price

            logger.info(f"✅ {ticker} 피라미딩 매수 완료: 레벨={self.pyramid_levels[ticker]}")
            return True

        except Exception as e:
            logger.error(f"❌ {ticker} 피라미딩 상태 업데이트 실패: {e}")
            return False

    def get_pyramid_info(self, ticker: str) -> Dict[str, Any]:
        """피라미딩 정보 조회"""
        if ticker not in self.initial_buy_price:
            return {}

        return {
            'initial_buy_price': self.initial_buy_price[ticker],
            'current_level': self.pyramid_levels[ticker],
            'max_levels': self.max_pyramids,
            'last_breakout_price': self.last_breakout_price[ticker],
            'highest_after_buy': self.highest_after_buy[ticker]
        }

class TrailingStopManager:
    """ATR 기반 트레일링 스탑 관리자 (trade_executor.py에서 이식)"""

    def __init__(self, atr_multiplier: float = 1.0, per_ticker_config: Optional[Dict[str, float]] = None):
        self.atr_multiplier = atr_multiplier
        self.per_ticker_config = per_ticker_config or {}
        self.entry_price = {}
        self.highest_price = {}
        self.atr = {}
        self.stop_price = {}

    def get_atr_multiplier(self, ticker: str) -> float:
        """티커별 ATR 배수 반환"""
        return self.per_ticker_config.get(ticker, self.atr_multiplier)

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
        # 첫 업데이트 시 ATR 값을 SQLite에서 조회
        if ticker not in self.highest_price:
            atr_value = current_price * 0.01  # 기본값: 현재가의 1%

            try:
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    # technical_analysis 테이블에서 ATR 조회
                    cursor.execute("""
                        SELECT atr FROM technical_analysis
                        WHERE ticker = ?
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (ticker,))
                    result = cursor.fetchone()

                    if result and result[0] is not None:
                        atr_value = float(result[0])
                        logger.info(f"📊 {ticker} SQLite에서 ATR 조회: {atr_value:.2f}")
                    else:
                        logger.warning(f"⚠️ {ticker} ATR 데이터 없음. 기본값(현재가의 1%) 사용: {atr_value:.0f}")

            except Exception as e:
                logger.warning(f"⚠️ {ticker} ATR 조회 실패, 기본값 사용: {e}")

            # 초기 설정 (ATR 조회 성공/실패와 관계없이 실행)
            self.entry_price[ticker] = current_price
            self.highest_price[ticker] = current_price
            self.atr[ticker] = atr_value

            # 초기 손절가: 진입가 - 1 ATR
            self.stop_price[ticker] = current_price - atr_value

            logger.info(f"🎯 {ticker} 트레일링 스탑 초기화: 진입가={current_price:.0f}, ATR={atr_value:.2f}, 초기 손절가={self.stop_price[ticker]:.0f}")
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

        # 더 높은 가격(더 타이트한 스탑) 선택
        new_stop = max(trail_price, fixed_stop)
        self.stop_price[ticker] = new_stop

        # 현재가가 손절가 아래로 떨어지면 청산 신호
        should_exit = current_price <= self.stop_price[ticker]

        if should_exit:
            logger.info(f"🚨 {ticker} 트레일링 스탑 청산 신호: 현재가={current_price:.0f} ≤ 손절가={self.stop_price[ticker]:.0f}")

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

        # 피라미딩 관리자 초기화
        self.pyramiding_manager = PyramidingManager(
            max_pyramids=3,  # 최대 3단계까지 피라미딩
            pyramid_multiplier=0.5  # 추가 매수 시 50% 크기
        )

        # 거래 통계
        self.trading_stats = {
            'session_start': datetime.now(),
            'orders_attempted': 0,
            'orders_successful': 0,
            'total_volume_krw': 0.0,
            'total_fees_krw': 0.0
        }

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

    def save_trade_record(self, trade_result: TradeResult):
        """거래 기록을 SQLite에 저장"""
        try:
            with get_db_connection_context() as conn:
                cursor = conn.cursor()

                # trades 테이블에 거래 기록 저장
                cursor.execute("""
                    INSERT INTO trades (
                        ticker, order_type, status, order_id,
                        quantity, price, amount_krw, fee,
                        error_message, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade_result.ticker,
                    trade_result.order_type.value,
                    trade_result.status.value,
                    trade_result.order_id,
                    trade_result.quantity,
                    trade_result.price,
                    trade_result.amount_krw,
                    trade_result.fee,
                    trade_result.error_message,
                    trade_result.timestamp or datetime.now()
                ))

                conn.commit()
                logger.info(f"📝 {trade_result.ticker} 거래 기록 저장 완료")

        except Exception as e:
            logger.error(f"❌ 거래 기록 저장 실패: {e}")

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
                        hold_days = (datetime.now() - buy_timestamp).days

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
                    WHERE ticker = ? AND order_type = 'BUY' AND status = 'SUCCESS'
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

    def get_total_balance_krw(self) -> float:
        """총 보유 자산 KRW 환산"""
        try:
            if self.dry_run:
                return 1000000.0  # DRY RUN 시 100만원으로 가정

            balances = self.upbit.get_balances()
            if not balances:
                return 0.0

            # API 오류 응답 처리
            if isinstance(balances, dict) and 'error' in balances:
                error_info = balances['error']
                logger.warning(f"⚠️ 업비트 API 오류: {error_info.get('name', 'unknown')} - {error_info.get('message', 'no message')}")
                return 0.0

            # balances가 리스트가 아닌 경우 처리
            if not isinstance(balances, list):
                logger.warning(f"⚠️ 예상치 못한 balances 타입: {type(balances)}")
                return 0.0

            total_krw = 0.0

            for balance in balances:
                currency = balance['currency']
                quantity = float(balance['balance'])

                if quantity <= 0:
                    continue

                if currency == 'KRW':
                    total_krw += quantity
                else:
                    # 암호화폐는 현재가로 환산
                    ticker = f"KRW-{currency}"
                    current_price = pyupbit.get_current_price(ticker)
                    if current_price:
                        total_krw += quantity * current_price

            return total_krw

        except Exception as e:
            logger.error(f"❌ 총 자산 조회 실패: {e}")
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
    def execute_buy_order(self, ticker: str, amount_krw: float) -> TradeResult:
        """매수 주문 실행"""
        trade_result = TradeResult(
            status=OrderStatus.FAILURE,
            ticker=ticker,
            order_type=OrderType.BUY,
            timestamp=datetime.now()
        )

        try:
            self.trading_stats['orders_attempted'] += 1

            # DRY RUN 모드
            if self.dry_run:
                logger.info(f"🧪 DRY RUN: {ticker} 매수 주문 ({amount_krw:,.0f}원)")
                trade_result.status = OrderStatus.SUCCESS
                trade_result.amount_krw = amount_krw
                trade_result.quantity = amount_krw / pyupbit.get_current_price(ticker)
                trade_result.price = pyupbit.get_current_price(ticker)
                self.trading_stats['orders_successful'] += 1
                self.save_trade_record(trade_result)
                return trade_result

            # 최소 주문 금액 확인
            if amount_krw < self.config.min_order_amount_krw:
                trade_result.error_message = f"주문 금액 부족: {amount_krw:,.0f} < {self.config.min_order_amount_krw:,.0f}"
                trade_result.status = OrderStatus.SKIPPED
                logger.warning(f"⚠️ {ticker}: {trade_result.error_message}")
                self.save_trade_record(trade_result)
                return trade_result

            # 현재가 조회
            current_price = pyupbit.get_current_price(ticker)
            if not current_price:
                trade_result.error_message = "현재가 조회 실패"
                logger.error(f"❌ {ticker}: {trade_result.error_message}")
                self.save_trade_record(trade_result)
                return trade_result

            # 수수료를 고려한 실제 주문 금액
            order_amount = amount_krw / (1 + self.config.taker_fee_rate)

            logger.info(f"🚀 {ticker} 시장가 매수 주문: {order_amount:,.0f}원 (현재가: {current_price:,.0f})")

            # 업비트 매수 주문
            response = self.upbit.buy_market_order(ticker, order_amount)

            if not response or not response.get('uuid'):
                trade_result.error_message = f"주문 접수 실패: {response}"
                logger.error(f"❌ {ticker} 매수 주문 접수 실패")
                self.save_trade_record(trade_result)
                return trade_result

            order_id = response['uuid']
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

                        trade_result.status = OrderStatus.SUCCESS
                        trade_result.quantity = executed_quantity
                        trade_result.price = avg_price
                        trade_result.amount_krw = total_value
                        trade_result.fee = fee

                        self.trading_stats['orders_successful'] += 1
                        self.trading_stats['total_volume_krw'] += total_value
                        self.trading_stats['total_fees_krw'] += fee

                        # 피라미딩 초기화 (첫 매수 시)
                        self.pyramiding_manager.register_initial_buy(ticker, avg_price)

                        logger.info(f"💰 {ticker} 매수 체결 완료: {executed_quantity:.8f}개, 평균가 {avg_price:,.0f}원")
                    else:
                        trade_result.error_message = f"체결 내역 있으나 총 체결 수량 0. OrderID: {order_id}"
                        logger.error(f"❌ {ticker} 매수 체결 정보 오류: {trade_result.error_message}")

                elif executed_quantity > 0:
                    # trades 정보 없지만 executed_volume은 있는 경우 (업비트에서 가끔 발생)
                    fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                    trade_result.status = OrderStatus.SUCCESS_NO_AVG_PRICE
                    trade_result.quantity = executed_quantity
                    trade_result.price = current_price  # 현재가로 대체
                    trade_result.amount_krw = executed_quantity * current_price
                    trade_result.fee = fee
                    trade_result.error_message = "Trades 정보 없음 - 현재가로 평균단가 대체"

                    self.trading_stats['orders_successful'] += 1
                    self.trading_stats['total_volume_krw'] += trade_result.amount_krw
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

                            trade_result.status = OrderStatus.SUCCESS_PARTIAL
                            trade_result.quantity = executed_quantity
                            trade_result.price = avg_price
                            trade_result.amount_krw = total_value
                            trade_result.fee = fee
                            trade_result.error_message = "부분 체결 후 취소"

                            self.trading_stats['orders_successful'] += 1
                            self.trading_stats['total_volume_krw'] += total_value
                            self.trading_stats['total_fees_krw'] += fee

                            logger.info(f"💰 {ticker} 매수 부분 체결 완료 (cancel): {executed_quantity:.8f}개, 평균가 {avg_price:,.0f}원")
                        else:
                            # trades 있지만 volume 합계가 0인 경우
                            fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                            trade_result.status = OrderStatus.SUCCESS_PARTIAL_NO_AVG
                            trade_result.quantity = executed_quantity
                            trade_result.price = current_price
                            trade_result.amount_krw = executed_quantity * current_price
                            trade_result.fee = fee
                            trade_result.error_message = "부분 체결 후 취소, trades 정보 불완전"

                            self.trading_stats['orders_successful'] += 1
                            self.trading_stats['total_volume_krw'] += trade_result.amount_krw
                            self.trading_stats['total_fees_krw'] += fee

                            logger.warning(f"⚠️ {ticker} 매수 부분 체결 (trades 정보 불완전): {executed_quantity:.8f}개, 현재가로 기록")
                    else:
                        # trades 없지만 executed_quantity는 있는 경우
                        fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                        trade_result.status = OrderStatus.SUCCESS_PARTIAL_NO_AVG
                        trade_result.quantity = executed_quantity
                        trade_result.price = current_price
                        trade_result.amount_krw = executed_quantity * current_price
                        trade_result.fee = fee
                        trade_result.error_message = "부분 체결 후 취소, trades 정보 없음"

                        self.trading_stats['orders_successful'] += 1
                        self.trading_stats['total_volume_krw'] += trade_result.amount_krw
                        self.trading_stats['total_fees_krw'] += fee

                        logger.warning(f"⚠️ {ticker} 매수 부분 체결 (trades 정보 없음): {executed_quantity:.8f}개, 현재가로 기록")
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

        finally:
            self.save_trade_record(trade_result)

        return trade_result

    @retry(max_attempts=3, initial_delay=1, backoff=2)
    def execute_sell_order(self, ticker: str, quantity: Optional[float] = None) -> TradeResult:
        """매도 주문 실행"""
        trade_result = TradeResult(
            status=OrderStatus.FAILURE,
            ticker=ticker,
            order_type=OrderType.SELL,
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
                trade_result.status = OrderStatus.SUCCESS
                trade_result.quantity = quantity or 1.0
                trade_result.price = pyupbit.get_current_price(ticker)
                self.trading_stats['orders_successful'] += 1
                self.save_trade_record(trade_result)
                return trade_result

            # 보유 수량 확인
            balance = self.upbit.get_balance(currency)

            # API 오류 응답 처리
            if isinstance(balance, dict) and 'error' in balance:
                error_info = balance['error']
                trade_result.error_message = f"API 오류: {error_info.get('name', 'unknown')}"
                trade_result.status = OrderStatus.API_ERROR
                logger.warning(f"⚠️ {ticker}: 업비트 API 오류 - {error_info.get('message', 'no message')}")
                self.save_trade_record(trade_result)
                return trade_result

            if not balance or balance <= 0:
                trade_result.error_message = "보유 수량 없음"
                trade_result.status = OrderStatus.SKIPPED
                logger.warning(f"⚠️ {ticker}: 보유 수량 없음")
                self.save_trade_record(trade_result)
                return trade_result

            # 매도 수량 결정
            sell_quantity = quantity if quantity and quantity <= balance else balance

            # 현재가 조회
            current_price = pyupbit.get_current_price(ticker)
            if not current_price:
                trade_result.error_message = "현재가 조회 실패"
                logger.error(f"❌ {ticker}: {trade_result.error_message}")
                self.save_trade_record(trade_result)
                return trade_result

            # 최소 매도 금액 확인
            estimated_value = sell_quantity * current_price
            net_value = estimated_value * (1 - self.config.taker_fee_rate)

            if net_value < 5000:  # 업비트 최소 매도 금액
                trade_result.error_message = f"매도 금액 부족: {net_value:,.0f} < 5,000원"
                trade_result.status = OrderStatus.SKIPPED
                logger.warning(f"⚠️ {ticker}: {trade_result.error_message}")
                self.save_trade_record(trade_result)
                return trade_result

            logger.info(f"🚀 {ticker} 시장가 매도 주문: {sell_quantity:.8f}개 (현재가: {current_price:,.0f})")

            # 업비트 매도 주문
            response = self.upbit.sell_market_order(ticker, sell_quantity)

            if not response or not response.get('uuid'):
                trade_result.error_message = f"주문 접수 실패: {response}"
                logger.error(f"❌ {ticker} 매도 주문 접수 실패")
                self.save_trade_record(trade_result)
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

                        trade_result.status = OrderStatus.SUCCESS
                        trade_result.quantity = executed_quantity
                        trade_result.price = avg_price
                        trade_result.amount_krw = total_value
                        trade_result.fee = fee

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

                    trade_result.status = OrderStatus.SUCCESS_NO_AVG_PRICE
                    trade_result.quantity = executed_quantity
                    trade_result.price = current_price
                    trade_result.amount_krw = executed_quantity * current_price
                    trade_result.fee = fee
                    trade_result.error_message = "Trades 정보 없음 - 현재가로 평균단가 대체"

                    self.trading_stats['orders_successful'] += 1
                    self.trading_stats['total_volume_krw'] += trade_result.amount_krw
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

                            trade_result.status = OrderStatus.SUCCESS_PARTIAL
                            trade_result.quantity = executed_quantity
                            trade_result.price = avg_price
                            trade_result.amount_krw = total_value
                            trade_result.fee = fee
                            trade_result.error_message = "부분 체결 후 취소"

                            self.trading_stats['orders_successful'] += 1
                            self.trading_stats['total_volume_krw'] += total_value
                            self.trading_stats['total_fees_krw'] += fee

                            logger.info(f"💰 {ticker} 매도 부분 체결 완료 (cancel): {executed_quantity:.8f}개, 평균가 {avg_price:,.0f}원")
                        else:
                            fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                            trade_result.status = OrderStatus.SUCCESS_PARTIAL_NO_AVG
                            trade_result.quantity = executed_quantity
                            trade_result.price = current_price
                            trade_result.amount_krw = executed_quantity * current_price
                            trade_result.fee = fee
                            trade_result.error_message = "부분 체결 후 취소, trades 정보 불완전"

                            self.trading_stats['orders_successful'] += 1
                            self.trading_stats['total_volume_krw'] += trade_result.amount_krw
                            self.trading_stats['total_fees_krw'] += fee

                            logger.warning(f"⚠️ {ticker} 매도 부분 체결 (trades 정보 불완전): {executed_quantity:.8f}개, 현재가로 기록")
                    else:
                        fee = (executed_quantity * current_price) * self.config.taker_fee_rate

                        trade_result.status = OrderStatus.SUCCESS_PARTIAL_NO_AVG
                        trade_result.quantity = executed_quantity
                        trade_result.price = current_price
                        trade_result.amount_krw = executed_quantity * current_price
                        trade_result.fee = fee
                        trade_result.error_message = "부분 체결 후 취소, trades 정보 없음"

                        self.trading_stats['orders_successful'] += 1
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
                        'supertrend': result[0],
                        'macd_histogram': result[1],
                        'support': result[2],
                        'adx': result[3]
                    }

            # 3. 지지선 하회 조건 (최우선 매도 신호)
            if (market_data.get("support") is not None and
                current_price < market_data.get("support")):
                return True, f"지지선 하회 (현재가: {current_price:.0f} < 지지선: {market_data.get('support'):.0f})"

            # 4. 기술적 매도 조건
            tech_exit = False
            tech_reason = ""

            # Supertrend 하향 돌파
            if (market_data.get("supertrend") is not None and
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
                'orders_successful': self.trading_stats['orders_successful'],
                'success_rate': (
                    self.trading_stats['orders_successful'] / self.trading_stats['orders_attempted'] * 100
                    if self.trading_stats['orders_attempted'] > 0 else 0
                ),
                'total_volume_krw': self.trading_stats['total_volume_krw'],
                'total_fees_krw': self.trading_stats['total_fees_krw'],
                'average_order_size': (
                    self.trading_stats['total_volume_krw'] / self.trading_stats['orders_successful']
                    if self.trading_stats['orders_successful'] > 0 else 0
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
                    buy_result = self.execute_buy_order(ticker, amount_krw)
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
        """모든 보유 포지션에서 피라미딩 기회 확인"""
        pyramid_opportunities = {}

        try:
            positions = self.get_current_positions()

            for position in positions:
                try:
                    # 피라미딩 기회 확인
                    should_pyramid, additional_position = self.pyramiding_manager.check_pyramid_opportunity(
                        position.ticker, position.current_price, self.db_path
                    )

                    if should_pyramid and additional_position > 0:
                        pyramid_opportunities[position.ticker] = {
                            'current_price': position.current_price,
                            'additional_position_pct': additional_position,
                            'pyramid_info': self.pyramiding_manager.get_pyramid_info(position.ticker)
                        }

                        logger.info(f"🔺 {position.ticker} 피라미딩 기회 발견: 추가 {additional_position:.1f}% 포지션")

                except Exception as e:
                    logger.error(f"❌ {position.ticker} 피라미딩 기회 확인 실패: {e}")

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
                    trade_result = self.execute_buy_order(ticker, additional_amount)

                    if trade_result.status in [
                        OrderStatus.SUCCESS,
                        OrderStatus.SUCCESS_NO_AVG_PRICE,
                        OrderStatus.SUCCESS_PARTIAL,
                        OrderStatus.SUCCESS_PARTIAL_NO_AVG
                    ]:
                        # 피라미딩 상태 업데이트
                        self.pyramiding_manager.execute_pyramid_buy(ticker, opportunity['current_price'])

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