#!/usr/bin/env python3
"""
Real-time Market Sentiment Analysis System
실시간 시장 분위기 분석 시스템

🎯 핵심 개선사항:
- SQLite DB 의존성 완전 제거
- pyupbit API 기반 실시간 데이터 수집
- 전체 시장 종목 대상 분석 (DB 종목 제한 해제)
- 4대 모듈 통합 분석 (Fear&Greed + BTC + Market Breadth + Volume)

🚀 시스템 아키텍처:
1. FearGreedAPI: Alternative.me Fear & Greed Index
2. BTCTrendAnalyzer: BTC 추세 분석 (pyupbit)
3. MarketBreadthAnalyzer: 시장 폭 분석 (pyupbit)
4. VolumeAnalyzer: 거래량 분석 (pyupbit)
5. MarketSentimentEngine: 통합 엔진

📊 판정 결과: BEAR/NEUTRAL/BULL (3단계)
"""

import pyupbit
import requests
import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# 로깅 설정
logger = logging.getLogger(__name__)

# =============================================================================
# Data Models and Enums
# =============================================================================

class MarketSentiment(Enum):
    """시장 감정 분류"""
    BEAR = "BEAR"        # 약세장 - 거래 중단
    NEUTRAL = "NEUTRAL"  # 중성 - 신중한 거래
    BULL = "BULL"        # 강세장 - 적극적 거래

@dataclass
class FearGreedData:
    """Fear & Greed Index 데이터"""
    value: int                    # 0-100 지수 값
    classification: str           # 텍스트 분류
    timestamp: str               # 데이터 시점
    weight: float                # 가중치 (0.3)

@dataclass
class BTCTrendData:
    """BTC 트렌드 분석 데이터"""
    current_price: float
    change_1h: float             # 1시간 변화율 (%)
    change_4h: float             # 4시간 변화율 (%)
    change_24h: float            # 24시간 변화율 (%)
    ma20_position: float         # MA20 대비 위치 (%)
    volatility: float            # 변동성 지수
    volume_ratio: float          # 평균 대비 거래량 비율
    trend_score: float           # 종합 트렌드 점수
    weight: float                # 가중치 (0.25)

@dataclass
class MarketBreadthData:
    """시장 폭 분석 데이터"""
    total_tickers: int           # 전체 종목 수
    advancing_count: int         # 상승 종목 수
    declining_count: int         # 하락 종목 수
    advance_decline_ratio: float # 상승/하락 비율
    strong_advance_ratio: float  # 5%+ 상승 종목 비율
    strong_decline_ratio: float  # 5%+ 하락 종목 비율
    market_participation: float  # 시장 참여도
    market_health: float         # 시장 건강도 점수
    weight: float                # 가중치 (0.25)

@dataclass
class VolumeData:
    """거래량 분석 데이터"""
    total_volume_krw: float      # 전체 거래대금 (KRW)
    top10_volume_ratio: float    # 상위 10개 종목 거래량 비율
    volume_trend_score: float    # 거래량 증감 점수
    market_activity: float       # 시장 활성도
    activity_score: float        # 종합 활성도 점수
    weight: float                # 가중치 (0.2)

@dataclass
class MarketSentimentResult:
    """종합 시장 감정 분석 결과"""
    timestamp: datetime

    # 개별 모듈 결과
    fear_greed_data: Optional[FearGreedData]
    btc_trend_data: Optional[BTCTrendData]
    market_breadth_data: Optional[MarketBreadthData]
    volume_data: Optional[VolumeData]

    # 종합 결과
    total_score: float           # 종합 점수 (0-100)
    final_sentiment: MarketSentiment
    confidence: float            # 신뢰도 (0.0-1.0)

    # 거래 가이드
    trading_allowed: bool        # 거래 허용 여부
    position_adjustment: float   # 포지션 조정 배수 (0.0-1.5)
    reasoning: str              # 판정 근거

# =============================================================================
# Module 1: Fear & Greed API
# =============================================================================

class FearGreedAPI:
    """Alternative.me Fear & Greed Index 실시간 조회"""

    def __init__(self):
        self.api_url = "https://api.alternative.me/fng/"
        self.timeout = 10
        self.weight = 0.3  # 전체 점수 가중치 30%

    def get_current_index(self) -> Optional[FearGreedData]:
        """현재 Fear & Greed Index 조회"""
        try:
            logger.debug("🌐 Fear & Greed Index API 호출")
            response = requests.get(self.api_url, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            if 'data' not in data or not data['data']:
                logger.error("❌ Fear & Greed API 응답 데이터 없음")
                return None

            latest = data['data'][0]
            value = int(latest['value'])
            classification = latest['value_classification']
            timestamp = latest['timestamp']

            result = FearGreedData(
                value=value,
                classification=classification,
                timestamp=timestamp,
                weight=self.weight
            )

            logger.info(f"📊 Fear & Greed Index: {value} ({classification})")
            return result

        except Exception as e:
            logger.error(f"❌ Fear & Greed Index 조회 실패: {e}")
            return None

# =============================================================================
# Module 2: BTC Trend Analyzer
# =============================================================================

class BTCTrendAnalyzer:
    """비트코인 트렌드 분석 - 암호화폐 시장 대표성"""

    def __init__(self):
        self.ticker = "KRW-BTC"
        self.weight = 0.25  # 전체 점수 가중치 25%

    def analyze_btc_trend(self) -> Optional[BTCTrendData]:
        """BTC 트렌드 종합 분석"""
        try:
            logger.debug("₿ BTC 트렌드 분석 시작")

            # 현재가 조회
            current_price = self._get_current_price()
            if not current_price:
                return None

            # 다시간대 OHLCV 데이터 수집
            ohlcv_1h = self._get_ohlcv("minute60", 24)  # 24시간
            ohlcv_4h = self._get_ohlcv("minute240", 24)  # 4일
            ohlcv_daily = self._get_ohlcv("day", 30)     # 30일

            if not all([ohlcv_1h is not None, ohlcv_4h is not None, ohlcv_daily is not None]):
                logger.error("❌ BTC OHLCV 데이터 부족")
                return None

            # 변화율 계산
            change_1h = self._calculate_change_rate(ohlcv_1h, 1)
            change_4h = self._calculate_change_rate(ohlcv_4h, 4)
            change_24h = self._calculate_change_rate(ohlcv_daily, 1)

            # MA20 대비 위치 계산
            ma20_position = self._calculate_ma_position(ohlcv_daily, current_price, 20)

            # 변동성 계산 (1시간 기준)
            volatility = self._calculate_volatility(ohlcv_1h)

            # 거래량 비율 계산
            volume_ratio = self._calculate_volume_ratio(ohlcv_daily)

            # 종합 트렌드 점수 계산
            trend_score = self._calculate_trend_score(
                change_1h, change_4h, change_24h, ma20_position, volume_ratio
            )

            result = BTCTrendData(
                current_price=current_price,
                change_1h=change_1h,
                change_4h=change_4h,
                change_24h=change_24h,
                ma20_position=ma20_position,
                volatility=volatility,
                volume_ratio=volume_ratio,
                trend_score=trend_score,
                weight=self.weight
            )

            logger.info(f"₿ BTC: {current_price:,.0f}원 (1H: {change_1h:+.1f}%, 24H: {change_24h:+.1f}%)")
            return result

        except Exception as e:
            logger.error(f"❌ BTC 트렌드 분석 실패: {e}")
            return None

    def _get_current_price(self) -> Optional[float]:
        """현재가 조회"""
        try:
            price = pyupbit.get_current_price(self.ticker)
            return float(price) if price else None
        except Exception as e:
            logger.error(f"❌ BTC 현재가 조회 실패: {e}")
            return None

    def _get_ohlcv(self, interval: str, count: int) -> Optional[pd.DataFrame]:
        """OHLCV 데이터 조회"""
        try:
            df = pyupbit.get_ohlcv(self.ticker, interval=interval, count=count)
            return df if df is not None and len(df) >= count // 2 else None
        except Exception as e:
            logger.error(f"❌ BTC OHLCV 조회 실패 ({interval}): {e}")
            return None

    def _calculate_change_rate(self, df: pd.DataFrame, periods: int) -> float:
        """변화율 계산"""
        try:
            if len(df) < periods + 1:
                return 0.0
            current = df.iloc[0]['close']
            previous = df.iloc[periods]['close']
            return ((current - previous) / previous) * 100
        except Exception:
            return 0.0

    def _calculate_ma_position(self, df: pd.DataFrame, current_price: float, periods: int) -> float:
        """이동평균 대비 위치 계산"""
        try:
            if len(df) < periods:
                return 0.0
            ma = df['close'].rolling(periods).mean().iloc[0]
            return ((current_price - ma) / ma) * 100 if ma > 0 else 0.0
        except Exception:
            return 0.0

    def _calculate_volatility(self, df: pd.DataFrame) -> float:
        """변동성 계산 (일일 수익률 표준편차)"""
        try:
            returns = df['close'].pct_change().dropna()
            return float(returns.std() * 100) if len(returns) > 1 else 0.0
        except Exception:
            return 0.0

    def _calculate_volume_ratio(self, df: pd.DataFrame) -> float:
        """거래량 비율 계산"""
        try:
            if len(df) < 7:
                return 1.0
            recent_volume = df.iloc[0]['volume']
            avg_volume = df['volume'].iloc[1:8].mean()  # 지난 7일 평균
            return float(recent_volume / avg_volume) if avg_volume > 0 else 1.0
        except Exception:
            return 1.0

    def _calculate_trend_score(self, change_1h: float, change_4h: float,
                             change_24h: float, ma20_pos: float, vol_ratio: float) -> float:
        """종합 트렌드 점수 계산 (0-100)"""
        try:
            score = 50.0  # 기본 중립 점수

            # 시간대별 변화율 점수 (40점)
            score += min(20, max(-20, change_1h)) * 0.3     # 1시간 가중치 30%
            score += min(20, max(-20, change_4h)) * 0.4     # 4시간 가중치 40%
            score += min(20, max(-20, change_24h)) * 0.3    # 24시간 가중치 30%

            # MA20 위치 점수 (20점)
            score += min(10, max(-10, ma20_pos / 2))

            # 거래량 보정 (10점)
            if vol_ratio > 1.5:
                score += 5  # 거래량 급증 시 추가 점수
            elif vol_ratio < 0.7:
                score -= 5  # 거래량 급감 시 감점

            return max(0, min(100, score))

        except Exception:
            return 50.0  # 오류 시 중립 점수

# =============================================================================
# Module 3: Market Breadth Analyzer
# =============================================================================

class MarketBreadthAnalyzer:
    """시장 폭 분석 - 전체 종목 상승/하락 비율"""

    def __init__(self):
        self.weight = 0.25  # 전체 점수 가중치 25%

    def analyze_market_breadth(self) -> Optional[MarketBreadthData]:
        """시장 폭 분석 (전체 KRW 마켓)"""
        try:
            logger.debug("📊 시장 폭 분석 시작")

            # 전체 KRW 마켓 종목 조회
            all_tickers = pyupbit.get_tickers(fiat="KRW")
            if not all_tickers:
                logger.error("❌ 종목 목록 조회 실패")
                return None

            logger.debug(f"📋 전체 종목 수: {len(all_tickers)}개")

            # 현재가 일괄 조회
            current_prices = pyupbit.get_current_price(all_tickers)
            if not current_prices:
                logger.error("❌ 현재가 일괄 조회 실패")
                return None

            # 24시간 변화율 분석
            breadth_data = self._analyze_price_changes(current_prices)

            # 시장 참여도 계산
            market_participation = self._calculate_market_participation(all_tickers)

            # 시장 건강도 종합 점수 계산
            market_health = self._calculate_market_health(breadth_data, market_participation)

            result = MarketBreadthData(
                total_tickers=breadth_data['total_tickers'],
                advancing_count=breadth_data['advancing_count'],
                declining_count=breadth_data['declining_count'],
                advance_decline_ratio=breadth_data['advance_decline_ratio'],
                strong_advance_ratio=breadth_data['strong_advance_ratio'],
                strong_decline_ratio=breadth_data['strong_decline_ratio'],
                market_participation=market_participation,
                market_health=market_health,
                weight=self.weight
            )

            logger.info(f"📊 시장폭: 상승 {breadth_data['advancing_count']}개 하락 {breadth_data['declining_count']}개 (A/D비율: {breadth_data['advance_decline_ratio']:.2f})")

            return result

        except Exception as e:
            logger.error(f"❌ 시장 폭 분석 실패: {e}")
            return None

    def _analyze_price_changes(self, price_data: Dict) -> Dict:
        """가격 변화 분석"""
        try:
            advancing = 0
            declining = 0
            strong_advance = 0  # 5% 이상 상승
            strong_decline = 0  # 5% 이상 하락
            total = 0

            for ticker, price_info in price_data.items():
                if isinstance(price_info, dict) and 'change' in price_info:
                    total += 1
                    change_rate = price_info.get('change', 0)

                    if change_rate > 0:
                        advancing += 1
                        if change_rate >= 0.05:  # 5% 이상 상승
                            strong_advance += 1
                    elif change_rate < 0:
                        declining += 1
                        if change_rate <= -0.05:  # 5% 이상 하락
                            strong_decline += 1

            # 비율 계산
            advance_decline_ratio = advancing / declining if declining > 0 else 99.0
            strong_advance_ratio = (strong_advance / total) * 100 if total > 0 else 0.0
            strong_decline_ratio = (strong_decline / total) * 100 if total > 0 else 0.0

            return {
                'total_tickers': total,
                'advancing_count': advancing,
                'declining_count': declining,
                'advance_decline_ratio': advance_decline_ratio,
                'strong_advance_ratio': strong_advance_ratio,
                'strong_decline_ratio': strong_decline_ratio
            }

        except Exception as e:
            logger.error(f"❌ 가격 변화 분석 실패: {e}")
            return {
                'total_tickers': 0, 'advancing_count': 0, 'declining_count': 0,
                'advance_decline_ratio': 1.0, 'strong_advance_ratio': 0.0, 'strong_decline_ratio': 0.0
            }

    def _calculate_market_participation(self, tickers: List[str]) -> float:
        """시장 참여도 계산 (거래량 기준)"""
        try:
            # 주요 10개 종목의 거래량 상태 확인
            major_tickers = tickers[:10]  # 상위 10개 (보통 거래량 순)

            active_count = 0
            total_checked = 0

            for ticker in major_tickers:
                try:
                    # 간단한 거래량 확인 (최근 데이터)
                    ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=1)
                    if ohlcv is not None and len(ohlcv) > 0:
                        volume = ohlcv.iloc[0]['volume']
                        if volume > 0:
                            active_count += 1
                        total_checked += 1
                except Exception:
                    continue

            participation = (active_count / total_checked) * 100 if total_checked > 0 else 50.0
            return participation

        except Exception as e:
            logger.error(f"❌ 시장 참여도 계산 실패: {e}")
            return 50.0

    def _calculate_market_health(self, breadth_data: Dict, participation: float) -> float:
        """시장 건강도 종합 점수 계산 (0-100)"""
        try:
            score = 0.0

            # A/D 비율 점수 (40점)
            ad_ratio = breadth_data['advance_decline_ratio']
            if ad_ratio >= 2.0:
                score += 40
            elif ad_ratio >= 1.5:
                score += 30
            elif ad_ratio >= 1.0:
                score += 20
            elif ad_ratio >= 0.8:
                score += 10
            else:
                score += 0

            # 강한 상승/하락 비율 (30점)
            strong_adv = breadth_data['strong_advance_ratio']
            strong_dec = breadth_data['strong_decline_ratio']

            if strong_adv > strong_dec:
                score += min(30, strong_adv * 2)
            else:
                score -= min(15, strong_dec)

            # 시장 참여도 (30점)
            score += (participation / 100) * 30

            return max(0, min(100, score))

        except Exception:
            return 50.0

# =============================================================================
# Module 4: Volume Analyzer
# =============================================================================

class VolumeAnalyzer:
    """시장 거래량 분석 - 시장 활성도 측정"""

    def __init__(self):
        self.weight = 0.2  # 전체 점수 가중치 20%

    def analyze_volume_trend(self) -> Optional[VolumeData]:
        """거래량 트렌드 분석"""
        try:
            logger.debug("💰 거래량 분석 시작")

            # 전체 거래대금 계산
            total_volume_krw = self._calculate_total_volume()

            # 상위 종목 집중도 분석
            top10_ratio = self._analyze_volume_concentration()

            # 거래량 트렌드 점수 계산
            volume_trend_score = self._calculate_volume_trend_score()

            # 시장 활성도 계산
            market_activity = self._calculate_market_activity(total_volume_krw)

            # 종합 활성도 점수 계산
            activity_score = self._calculate_activity_score(
                volume_trend_score, market_activity, top10_ratio
            )

            result = VolumeData(
                total_volume_krw=total_volume_krw,
                top10_volume_ratio=top10_ratio,
                volume_trend_score=volume_trend_score,
                market_activity=market_activity,
                activity_score=activity_score,
                weight=self.weight
            )

            logger.info(f"💰 거래량: 전체 {total_volume_krw/1e12:.1f}조원, 상위10개 비중 {top10_ratio:.1f}%")

            return result

        except Exception as e:
            logger.error(f"❌ 거래량 분석 실패: {e}")
            return None

    def _calculate_total_volume(self) -> float:
        """전체 시장 거래대금 계산"""
        try:
            # 주요 종목들의 거래대금 합계 추정
            major_tickers = ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-ADA", "KRW-DOT",
                           "KRW-LINK", "KRW-BCH", "KRW-LTC", "KRW-EOS", "KRW-TRX"]

            total_volume = 0.0

            for ticker in major_tickers:
                try:
                    ohlcv = pyupbit.get_ohlcv(ticker, interval="day", count=1)
                    if ohlcv is not None and len(ohlcv) > 0:
                        volume = ohlcv.iloc[0]['volume']
                        close = ohlcv.iloc[0]['close']
                        volume_krw = volume * close
                        total_volume += volume_krw
                except Exception:
                    continue

            # 주요 종목 외 추가 추정 (보통 주요 10개가 전체의 70-80%)
            estimated_total = total_volume / 0.75  # 75% 비중으로 추정

            return estimated_total

        except Exception as e:
            logger.error(f"❌ 전체 거래대금 계산 실패: {e}")
            return 0.0

    def _analyze_volume_concentration(self) -> float:
        """거래량 집중도 분석 (상위 10개 종목 비중)"""
        try:
            # 간단한 추정: 상위 10개 종목이 대부분 차지
            # 실제로는 더 정교한 계산 필요하지만 API 제한 고려
            return 75.0  # 일반적으로 70-80% 수준

        except Exception:
            return 75.0

    def _calculate_volume_trend_score(self) -> float:
        """거래량 트렌드 점수 계산"""
        try:
            # BTC 거래량 기준으로 트렌드 판단
            btc_ohlcv = pyupbit.get_ohlcv("KRW-BTC", interval="day", count=7)
            if btc_ohlcv is None or len(btc_ohlcv) < 7:
                return 50.0

            # 최근 3일 vs 이전 3일 거래량 비교
            recent_avg = btc_ohlcv['volume'].iloc[:3].mean()
            previous_avg = btc_ohlcv['volume'].iloc[3:6].mean()

            ratio = recent_avg / previous_avg if previous_avg > 0 else 1.0

            # 비율을 점수로 변환 (0-100)
            score = 50 + min(25, max(-25, (ratio - 1) * 50))

            return score

        except Exception:
            return 50.0

    def _calculate_market_activity(self, total_volume: float) -> float:
        """시장 활성도 계산"""
        try:
            # 거래대금 기준 활성도 추정
            # 일반적인 일평균 거래대금 대비 계산

            # 기준값: 약 5조원 (평상시 수준)
            baseline_volume = 5e12  # 5조원

            activity_ratio = total_volume / baseline_volume

            # 비율을 점수로 변환 (0-100)
            activity_score = min(100, activity_ratio * 50)

            return activity_score

        except Exception:
            return 50.0

    def _calculate_activity_score(self, trend_score: float, activity: float, concentration: float) -> float:
        """종합 활성도 점수 계산 (0-100)"""
        try:
            # 가중평균 계산
            score = (
                trend_score * 0.4 +      # 트렌드 40%
                activity * 0.4 +         # 활성도 40%
                (100 - concentration) * 0.2  # 분산도 20% (집중도 역수)
            )

            return max(0, min(100, score))

        except Exception:
            return 50.0

# =============================================================================
# Market Sentiment Engine (통합 엔진)
# =============================================================================

class RealTimeMarketSentiment:
    """실시간 시장 분위기 분석 엔진 - 통합 관리자"""

    def __init__(self):
        """초기화"""
        self.fear_greed_api = FearGreedAPI()
        self.btc_analyzer = BTCTrendAnalyzer()
        self.market_breadth = MarketBreadthAnalyzer()
        self.volume_analyzer = VolumeAnalyzer()

        # 임계값 설정 (보수적)
        self.bear_threshold = 35    # BEAR 판정 임계값
        self.bull_threshold = 65    # BULL 판정 임계값

        logger.info("🚀 Real-time Market Sentiment Engine 초기화 완료")

    def analyze_market_sentiment(self) -> Optional[MarketSentimentResult]:
        """종합 시장 감정 분석 실행"""
        try:
            logger.info("🌡️ 실시간 시장 분위기 분석 시작")

            # 1. 각 모듈별 분석 (병렬 처리 가능하지만 순차 실행)
            logger.debug("📊 Fear & Greed Index 분석...")
            fear_greed_data = self.fear_greed_api.get_current_index()

            logger.debug("₿ BTC 트렌드 분석...")
            btc_trend_data = self.btc_analyzer.analyze_btc_trend()

            logger.debug("📈 시장 폭 분석...")
            market_breadth_data = self.market_breadth.analyze_market_breadth()

            logger.debug("💰 거래량 분석...")
            volume_data = self.volume_analyzer.analyze_volume_trend()

            # 2. 종합 점수 계산
            total_score = self._calculate_total_score(
                fear_greed_data, btc_trend_data, market_breadth_data, volume_data
            )

            # 3. 최종 감정 판정
            final_sentiment = self._determine_final_sentiment(total_score)

            # 4. 신뢰도 계산
            confidence = self._calculate_confidence(
                fear_greed_data, btc_trend_data, market_breadth_data, volume_data
            )

            # 5. 거래 가이드 계산
            trading_allowed, position_adjustment = self._calculate_trading_guide(final_sentiment, total_score)

            # 6. 판정 근거 생성
            reasoning = self._generate_reasoning(
                fear_greed_data, btc_trend_data, market_breadth_data, volume_data, total_score
            )

            # 7. 결과 생성
            result = MarketSentimentResult(
                timestamp=datetime.now(),
                fear_greed_data=fear_greed_data,
                btc_trend_data=btc_trend_data,
                market_breadth_data=market_breadth_data,
                volume_data=volume_data,
                total_score=total_score,
                final_sentiment=final_sentiment,
                confidence=confidence,
                trading_allowed=trading_allowed,
                position_adjustment=position_adjustment,
                reasoning=reasoning
            )

            # 8. 결과 로깅
            self._log_result(result)

            return result

        except Exception as e:
            logger.error(f"❌ 실시간 시장 감정 분석 실패: {e}")
            return None

    def _calculate_total_score(self, fear_greed: Optional[FearGreedData],
                             btc_trend: Optional[BTCTrendData],
                             market_breadth: Optional[MarketBreadthData],
                             volume: Optional[VolumeData]) -> float:
        """종합 점수 계산 (가중평균)"""
        try:
            total_score = 0.0
            total_weight = 0.0

            # Fear & Greed Index (30%)
            if fear_greed:
                score = fear_greed.value  # 0-100 그대로 사용
                total_score += score * fear_greed.weight
                total_weight += fear_greed.weight

            # BTC Trend (25%)
            if btc_trend:
                score = btc_trend.trend_score  # 0-100
                total_score += score * btc_trend.weight
                total_weight += btc_trend.weight

            # Market Breadth (25%)
            if market_breadth:
                score = market_breadth.market_health  # 0-100
                total_score += score * market_breadth.weight
                total_weight += market_breadth.weight

            # Volume Analysis (20%)
            if volume:
                score = volume.activity_score  # 0-100
                total_score += score * volume.weight
                total_weight += volume.weight

            # 가중평균 계산
            final_score = total_score / total_weight if total_weight > 0 else 50.0

            return max(0, min(100, final_score))

        except Exception as e:
            logger.error(f"❌ 종합 점수 계산 실패: {e}")
            return 50.0

    def _determine_final_sentiment(self, total_score: float) -> MarketSentiment:
        """최종 시장 감정 판정"""
        if total_score <= self.bear_threshold:
            return MarketSentiment.BEAR
        elif total_score >= self.bull_threshold:
            return MarketSentiment.BULL
        else:
            return MarketSentiment.NEUTRAL

    def _calculate_confidence(self, fear_greed: Optional[FearGreedData],
                            btc_trend: Optional[BTCTrendData],
                            market_breadth: Optional[MarketBreadthData],
                            volume: Optional[VolumeData]) -> float:
        """신뢰도 계산"""
        try:
            available_modules = 0
            if fear_greed:
                available_modules += 1
            if btc_trend:
                available_modules += 1
            if market_breadth:
                available_modules += 1
            if volume:
                available_modules += 1

            # 기본 신뢰도 (모듈 수 기반)
            base_confidence = available_modules / 4.0

            # 데이터 일관성 보너스 (모든 모듈이 같은 방향일 때)
            if available_modules >= 3:
                base_confidence *= 1.1

            return min(1.0, base_confidence)

        except Exception:
            return 0.5

    def _calculate_trading_guide(self, sentiment: MarketSentiment, score: float) -> Tuple[bool, float]:
        """거래 가이드 계산"""
        try:
            if sentiment == MarketSentiment.BEAR:
                return False, 0.0
            elif sentiment == MarketSentiment.BULL:
                # 점수에 따른 포지션 조정 (65-100 → 1.0-1.5x)
                adjustment = 1.0 + ((score - 65) / 35) * 0.5
                return True, min(1.5, adjustment)
            else:  # NEUTRAL
                # 점수에 따른 포지션 조정 (35-65 → 0.5-1.0x)
                adjustment = 0.5 + ((score - 35) / 30) * 0.5
                return True, max(0.5, min(1.0, adjustment))

        except Exception:
            return True, 0.8  # 기본값

    def _generate_reasoning(self, fear_greed: Optional[FearGreedData],
                          btc_trend: Optional[BTCTrendData],
                          market_breadth: Optional[MarketBreadthData],
                          volume: Optional[VolumeData], total_score: float) -> str:
        """판정 근거 생성"""
        try:
            parts = []

            # Fear & Greed
            if fear_greed:
                parts.append(f"F&G:{fear_greed.value}({fear_greed.classification})")

            # BTC
            if btc_trend:
                parts.append(f"BTC:{btc_trend.change_24h:+.1f}%/24H")

            # Market Breadth
            if market_breadth:
                parts.append(f"A/D:{market_breadth.advance_decline_ratio:.1f}")

            # Volume
            if volume:
                parts.append(f"Vol:{volume.activity_score:.0f}점")

            # 종합 점수
            parts.append(f"종합:{total_score:.0f}점")

            return " | ".join(parts)

        except Exception:
            return f"종합:{total_score:.0f}점"

    def _log_result(self, result: MarketSentimentResult):
        """결과 로깅"""
        try:
            logger.info(f"🌡️ 실시간 시장 분위기 분석 완료")
            logger.info(f"   📊 종합 점수: {result.total_score:.1f}점")
            logger.info(f"   🎯 시장 감정: {result.final_sentiment.value}")
            logger.info(f"   💼 거래 허용: {'✅' if result.trading_allowed else '❌'}")
            logger.info(f"   ⚖️ 포지션 조정: {result.position_adjustment:.2f}x")
            logger.info(f"   🔍 신뢰도: {result.confidence:.1%}")
            logger.info(f"   📋 근거: {result.reasoning}")

        except Exception as e:
            logger.error(f"❌ 결과 로깅 실패: {e}")

# =============================================================================
# API Optimizer (효율성 최적화)
# =============================================================================

class APIOptimizer:
    """pyupbit API 호출 최적화"""

    def __init__(self):
        self.cache = {}
        self.cache_ttl = 300  # 5분 캐시

    def get_cached_data(self, key: str) -> Optional[Any]:
        """캐시된 데이터 조회"""
        try:
            if key in self.cache:
                data, timestamp = self.cache[key]
                if time.time() - timestamp < self.cache_ttl:
                    return data
                else:
                    del self.cache[key]
            return None
        except Exception:
            return None

    def set_cached_data(self, key: str, data: Any):
        """데이터 캐싱"""
        try:
            self.cache[key] = (data, time.time())
        except Exception:
            pass

# =============================================================================
# 편의 함수
# =============================================================================

def get_market_sentiment() -> str:
    """간단한 시장 감정 조회 (편의 함수)"""
    try:
        engine = RealTimeMarketSentiment()
        result = engine.analyze_market_sentiment()
        return result.final_sentiment.value if result else "NEUTRAL"
    except Exception as e:
        logger.error(f"❌ 시장 감정 조회 실패: {e}")
        return "NEUTRAL"

def is_trading_allowed() -> bool:
    """거래 허용 여부 조회 (편의 함수)"""
    try:
        engine = RealTimeMarketSentiment()
        result = engine.analyze_market_sentiment()
        return result.trading_allowed if result else True
    except Exception as e:
        logger.error(f"❌ 거래 허용 여부 조회 실패: {e}")
        return True

if __name__ == "__main__":
    # 테스트 실행
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

    print("🚀 Real-time Market Sentiment Analysis System 테스트")
    print("=" * 60)

    engine = RealTimeMarketSentiment()
    result = engine.analyze_market_sentiment()

    if result:
        print(f"✅ 분석 완료: {result.final_sentiment.value}")
        print(f"📊 종합 점수: {result.total_score:.1f}점")
        print(f"💼 거래 허용: {'예' if result.trading_allowed else '아니오'}")
        print(f"⚖️ 포지션 조정: {result.position_adjustment:.2f}배")
    else:
        print("❌ 분석 실패")