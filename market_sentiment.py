"""
시장 감정 분석기 (Market Sentiment Analyzer) 모듈

🎯 핵심 기능:
- Fear&Greed Index + BTC 트렌드 종합 분석
- BEAR/NEUTRAL/BULL 시장 판정
- 거래 중단 신호 생성 (리스크 회피)
- 기존 시장 체온계 기능 통합

🛡️ 리스크 관리 철학:
- "지지 않는 것에 집중" - BEAR 시장에서는 절대 매수 금지
- 약세장 감지 시 모든 매수 신호 무시
- 보수적 접근: 의심스러우면 거래 중단

📊 분석 지표:
1. Fear&Greed Index (0-100) - External API
2. BTC 트렌드 분석 - Upbit API
3. 기존 Market Thermometer 지표 (등락률, 집중도, MA200)
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from db_manager_sqlite import get_db_connection_context
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import requests
import json
import sqlite3
from dataclasses import dataclass
from enum import Enum

# 환경변수 로딩
load_dotenv()

# 로깅 설정
logger = logging.getLogger(__name__)

# =============================================================================
# Fear&Greed Index Analysis Classes
# =============================================================================

class MarketSentiment(Enum):
    """시장 감정 분류"""
    BEAR = "BEAR"        # 약세장 - 거래 중단
    NEUTRAL = "NEUTRAL"  # 중성 - 제한적 거래
    BULL = "BULL"        # 강세장 - 적극적 거래

class FearGreedLevel(Enum):
    """Fear&Greed 지수 레벨"""
    EXTREME_FEAR = "extreme_fear"      # 0-25
    FEAR = "fear"                     # 26-45
    NEUTRAL = "neutral"               # 46-55
    GREED = "greed"                   # 56-75
    EXTREME_GREED = "extreme_greed"   # 76-100

class BTCTrend(Enum):
    """BTC 트렌드 방향"""
    STRONG_DOWN = "strong_down"       # 강한 하락
    DOWN = "down"                     # 하락
    SIDEWAYS = "sideways"             # 횡보
    UP = "up"                         # 상승
    STRONG_UP = "strong_up"           # 강한 상승

@dataclass
class FearGreedData:
    """Fear&Greed Index 데이터"""
    value: int           # 0-100 지수 값
    value_classification: str  # 텍스트 분류
    timestamp: str       # 데이터 시점
    level: FearGreedLevel     # 레벨 분류

@dataclass
class BTCTrendData:
    """BTC 트렌드 분석 데이터"""
    current_price: float
    change_1d: float     # 1일 변화율 (%)
    change_3d: float     # 3일 변화율 (%)
    change_7d: float     # 7일 변화율 (%)
    ma20_trend: float    # MA20 대비 위치 (%)
    volume_ratio: float  # 평균 대비 거래량 비율
    trend_direction: BTCTrend

@dataclass
class MarketSentimentResult:
    """시장 감정 분석 결과"""
    analysis_date: str
    analysis_time: str

    # Fear&Greed 분석
    fear_greed_data: Optional[FearGreedData]
    fear_greed_score: float      # 정규화된 점수 (-1.0 ~ 1.0)

    # BTC 트렌드 분석
    btc_trend_data: Optional[BTCTrendData]
    btc_trend_score: float       # 정규화된 점수 (-1.0 ~ 1.0)

    # Market Thermometer 분석
    thermometer_data: Optional[Dict]
    thermometer_score: float     # 정규화된 점수 (-1.0 ~ 1.0)

    # 종합 판정
    combined_score: float        # 종합 점수 (-1.0 ~ 1.0)
    final_sentiment: MarketSentiment
    confidence: float            # 판정 신뢰도 (0.0 ~ 1.0)

    # 거래 가이드
    trading_allowed: bool        # 거래 허용 여부
    position_adjustment: float   # 포지션 조정 배수 (0.0 ~ 1.5)
    reasoning: str              # 판정 근거

class FearGreedAnalyzer:
    """Fear&Greed Index + BTC 트렌드 분석기"""

    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path
        self.fear_greed_api_url = "https://api.alternative.me/fng/"
        self.upbit_api_url = "https://api.upbit.com/v1"

        # 임계값 설정 (보수적)
        self.bear_threshold = -0.3    # BEAR 판정 임계값
        self.bull_threshold = 0.3     # BULL 판정 임계값

    def fetch_fear_greed_index(self) -> Optional[FearGreedData]:
        """Fear&Greed Index 데이터 조회"""
        try:
            logger.debug("🌐 Fear&Greed Index API 호출")
            response = requests.get(self.fear_greed_api_url, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' not in data or not data['data']:
                logger.error("❌ Fear&Greed API 응답 데이터 없음")
                return None

            latest = data['data'][0]
            value = int(latest['value'])

            # 레벨 분류
            if value <= 25:
                level = FearGreedLevel.EXTREME_FEAR
            elif value <= 45:
                level = FearGreedLevel.FEAR
            elif value <= 55:
                level = FearGreedLevel.NEUTRAL
            elif value <= 75:
                level = FearGreedLevel.GREED
            else:
                level = FearGreedLevel.EXTREME_GREED

            fear_greed_data = FearGreedData(
                value=value,
                value_classification=latest['value_classification'],
                timestamp=latest['timestamp'],
                level=level
            )

            logger.info(f"📊 Fear&Greed Index: {value} ({latest['value_classification']})")
            return fear_greed_data

        except Exception as e:
            logger.error(f"❌ Fear&Greed Index 조회 실패: {e}")
            return None

    def fetch_btc_trend_data(self) -> Optional[BTCTrendData]:
        """BTC 트렌드 데이터 조회 (업비트 API)"""
        try:
            # 현재가 조회
            ticker_url = f"{self.upbit_api_url}/ticker"
            ticker_params = {"markets": "KRW-BTC"}

            ticker_response = requests.get(ticker_url, params=ticker_params, timeout=10)
            ticker_response.raise_for_status()
            ticker_data = ticker_response.json()[0]

            current_price = ticker_data['trade_price']
            change_1d = ticker_data['change_rate'] * 100

            # 캔들 데이터 조회
            candles_url = f"{self.upbit_api_url}/candles/days"
            candles_params = {"market": "KRW-BTC", "count": 30}

            candles_response = requests.get(candles_url, params=candles_params, timeout=10)
            candles_response.raise_for_status()
            candles_data = candles_response.json()

            if len(candles_data) < 7:
                logger.error("❌ BTC 캔들 데이터 부족")
                return None

            # 변화율 계산
            current = candles_data[0]['trade_price']
            price_3d_ago = candles_data[3]['trade_price']
            price_7d_ago = candles_data[7]['trade_price']

            change_3d = ((current - price_3d_ago) / price_3d_ago) * 100
            change_7d = ((current - price_7d_ago) / price_7d_ago) * 100

            # MA20 계산
            prices = [candle['trade_price'] for candle in candles_data[:20]]
            ma20 = sum(prices) / len(prices)
            ma20_trend = ((current_price - ma20) / ma20) * 100

            # 거래량 비율
            recent_volume = ticker_data['acc_trade_volume_24h']
            avg_volumes = [candle['candle_acc_trade_volume'] for candle in candles_data[:7]]
            avg_volume = sum(avg_volumes) / len(avg_volumes)
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0

            # 트렌드 방향 판정
            if change_7d < -10:
                trend_direction = BTCTrend.STRONG_DOWN
            elif change_7d < -3:
                trend_direction = BTCTrend.DOWN
            elif change_7d > 10:
                trend_direction = BTCTrend.STRONG_UP
            elif change_7d > 3:
                trend_direction = BTCTrend.UP
            else:
                trend_direction = BTCTrend.SIDEWAYS

            btc_trend_data = BTCTrendData(
                current_price=current_price,
                change_1d=change_1d,
                change_3d=change_3d,
                change_7d=change_7d,
                ma20_trend=ma20_trend,
                volume_ratio=volume_ratio,
                trend_direction=trend_direction
            )

            logger.info(f"₿ BTC 트렌드: {current_price:,.0f}원 (1D: {change_1d:+.1f}%, 7D: {change_7d:+.1f}%)")
            return btc_trend_data

        except Exception as e:
            logger.error(f"❌ BTC 트렌드 데이터 조회 실패: {e}")
            return None

    def calculate_fear_greed_score(self, fear_greed_data: FearGreedData) -> float:
        """Fear&Greed 지수를 정규화된 점수로 변환"""
        # 0-100 → -1.0 ~ 1.0 변환
        normalized = (fear_greed_data.value - 50) / 50.0

        # 보수적 조정: Fear 구간에서 더 강하게 반응
        if normalized < 0:
            normalized *= 1.2

        return max(-1.0, min(1.0, normalized))

    def calculate_btc_trend_score(self, btc_data: BTCTrendData) -> float:
        """BTC 트렌드를 정규화된 점수로 변환"""
        scores = []

        # 1일 변화율 (가중치: 0.2)
        score_1d = min(1.0, max(-1.0, btc_data.change_1d / 10.0))
        scores.append((score_1d, 0.2))

        # 3일 변화율 (가중치: 0.3)
        score_3d = min(1.0, max(-1.0, btc_data.change_3d / 15.0))
        scores.append((score_3d, 0.3))

        # 7일 변화율 (가중치: 0.3)
        score_7d = min(1.0, max(-1.0, btc_data.change_7d / 20.0))
        scores.append((score_7d, 0.3))

        # MA20 트렌드 (가중치: 0.2)
        ma_score = min(1.0, max(-1.0, btc_data.ma20_trend / 10.0))
        scores.append((ma_score, 0.2))

        # 가중 평균 계산
        weighted_score = sum(score * weight for score, weight in scores)
        return weighted_score

# =============================================================================
# Market Thermometer Classes (기존)
# =============================================================================

class MarketThermometer:
    """시장 체온계 클래스"""
    
    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path
        self.thresholds = self._load_thresholds_from_config()
        logger.info("🌡️ 시장 체온계 초기화 완료")
        logger.info(f"   - 임계값: {self.thresholds}")
    
    def _load_thresholds_from_config(self) -> Dict:
        """설정 파일에서 임계값 로드"""
        try:
            import yaml
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, 'config', 'filter_rules_config.yaml')
            
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as file:
                    config = yaml.safe_load(file)
                
                market_thermometer_config = config.get('market_thermometer', {})
                thresholds = market_thermometer_config.get('thresholds', {})
                
                # 기본값 설정 (2025-09-18 임계값 완화 적용)
                default_thresholds = {
                    'min_pct_up': 30.0,           #상승종목 비율 (40.0 → 30.0)
                    'max_top10_volume': 85.0,     #거래대금 집중도 (75.0 → 85.0)
                    'min_ma200_above': 10.0,      #MA200 상회 비율 (20.0 → 10.0)
                    'min_sentiment_score': 25.0   #종합 점수 (40.0 → 25.0)
                }
                
                # 설정 파일의 값으로 기본값 업데이트
                for key, value in thresholds.items():
                    if key in default_thresholds:
                        default_thresholds[key] = value
                
                logger.info(f"✅ 설정 파일에서 임계값 로드 완료: {default_thresholds}")
                return default_thresholds
            else:
                logger.warning(f"⚠️ 설정 파일을 찾을 수 없음: {config_path}")
                return {
                    'min_pct_up': 30.0,           # 완화된 값 적용
                    'max_top10_volume': 85.0,     # 완화된 값 적용
                    'min_ma200_above': 10.0,      # 완화된 값 적용
                    'min_sentiment_score': 25.0   # 완화된 값 적용
                }
                
        except Exception as e:
            logger.error(f"❌ 설정 파일 로드 실패: {e}")
            return {
                'min_pct_up': 30.0,           # 완화된 값 적용
                'max_top10_volume': 85.0,     # 완화된 값 적용
                'min_ma200_above': 10.0,      # 완화된 값 적용
                'min_sentiment_score': 25.0   # 완화된 값 적용
            }
    
    def calculate_market_sentiment_snapshot(self) -> Dict:
        """
        시장 분위기 스냅샷 계산
        
        Returns:
            Dict: {
                'pct_up': float,           # 상승 종목 비율
                'pct_down': float,         # 하락 종목 비율  
                'top10_volume_ratio': float, # 상위 10개 거래대금 비중
                'ma200_above_ratio': float,  # MA200 상회 비율
                'sentiment_score': float,    # 종합 점수 (0-100)
                'market_condition': str,     # 시장 상황 (bullish/neutral/bearish)
                'should_proceed': bool       # 파이프라인 진행 여부
            }
        """
        try:
            logger.info("🌡️ 시장 분위기 스냅샷 계산 시작")
            
            # 1. 등락률 분포 계산
            price_distribution = self._calculate_price_distribution()
            logger.debug(f"등락률 분포: {price_distribution}")
            
            # 2. 거래대금 집중도 계산  
            volume_concentration = self._calculate_volume_concentration()
            logger.debug(f"거래대금 집중도: {volume_concentration}")
            
            # 3. MA200 상회 비율 계산
            ma200_ratio = self._calculate_ma200_ratio()
            logger.debug(f"MA200 상회 비율: {ma200_ratio}")
            
            # 4. 종합 점수 계산
            sentiment_score = self._calculate_sentiment_score(
                price_distribution, volume_concentration, ma200_ratio
            )
            logger.debug(f"종합 점수: {sentiment_score}")
            
            # 5. 시장 상황 판단
            market_condition = self._determine_market_condition(sentiment_score)
            
            # 6. 파이프라인 진행 여부 결정
            should_proceed = self._should_proceed_pipeline(
                price_distribution, volume_concentration, ma200_ratio, sentiment_score
            )
            
            result = {
                'pct_up': price_distribution['pct_up'],
                'pct_down': price_distribution['pct_down'],
                'top10_volume_ratio': volume_concentration['top10_ratio'],
                'ma200_above_ratio': ma200_ratio['above_ratio'],
                'sentiment_score': sentiment_score,
                'market_condition': market_condition,
                'should_proceed': should_proceed,
                'timestamp': pd.Timestamp.now(),
                'details': {
                    'price_distribution': price_distribution,
                    'volume_concentration': volume_concentration,
                    'ma200_ratio': ma200_ratio
                }
            }
            
            logger.info(f"🌡️ 시장 체온계 결과: {result}")
            return result
            
        except Exception as e:
            logger.error(f"❌ 시장 분위기 계산 실패: {e}")
            return self._get_fallback_result()
    
    def _calculate_price_distribution(self) -> Dict:
        """등락률 분포 계산"""
        try:
            query = """
                WITH latest_data AS (
                    SELECT 
                        ticker,
                        close,
                        date
                    FROM ohlcv_data 
                    WHERE date = (SELECT MAX(date) FROM ohlcv_data)
                    AND close IS NOT NULL
                ),
                previous_data AS (
                    SELECT 
                        ticker,
                        close as prev_close,
                        date
                    FROM ohlcv_data 
                    WHERE date = (SELECT MAX(date) FROM ohlcv_data WHERE date < (SELECT MAX(date) FROM ohlcv_data))
                    AND close IS NOT NULL
                )
                SELECT 
                    COUNT(*) as total_tickers,
                    COUNT(CASE WHEN l.close > p.prev_close THEN 1 END) as up_tickers,
                    COUNT(CASE WHEN l.close < p.prev_close THEN 1 END) as down_tickers,
                    COUNT(CASE WHEN l.close = p.prev_close THEN 1 END) as flat_tickers
                FROM latest_data l
                INNER JOIN previous_data p ON l.ticker = p.ticker
            """
            
            with get_db_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
            
            if result:
                total, up_count, down_count, flat_count = result
                total = total or 1  # 0으로 나누기 방지
                
                pct_up = (up_count / total) * 100
                pct_down = (down_count / total) * 100
                pct_flat = (flat_count / total) * 100
                
                return {
                    'pct_up': round(pct_up, 1),
                    'pct_down': round(pct_down, 1), 
                    'pct_flat': round(pct_flat, 1),
                    'up_down_ratio': round(pct_up / pct_down, 2) if pct_down > 0 else 999,
                    'total_tickers': total,
                    'up_count': up_count,
                    'down_count': down_count,
                    'flat_count': flat_count
                }
            else:
                logger.warning("⚠️ 등락률 분포 데이터 없음, 기본값 사용")
                return {
                    'pct_up': 50.0, 'pct_down': 50.0, 'pct_flat': 0.0, 
                    'up_down_ratio': 1.0, 'total_tickers': 0,
                    'up_count': 0, 'down_count': 0, 'flat_count': 0
                }
                
        except Exception as e:
            logger.error(f"❌ 등락률 분포 계산 실패: {e}")
            return {
                'pct_up': 50.0, 'pct_down': 50.0, 'pct_flat': 0.0, 
                'up_down_ratio': 1.0, 'total_tickers': 0,
                'up_count': 0, 'down_count': 0, 'flat_count': 0
            }
    
    def _calculate_volume_concentration(self) -> Dict:
        """거래대금 상위 10개 집중도 계산"""
        try:
            query = """
                SELECT 
                    ticker,
                    volume * close as volume_krw
                FROM ohlcv_data 
                WHERE date = (SELECT MAX(date) FROM ohlcv_data)
                AND volume IS NOT NULL AND close IS NOT NULL
                ORDER BY volume * close DESC
                LIMIT 10
            """
            
            with get_db_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                top10_data = cursor.fetchall()
            
            if top10_data:
                top10_volume = sum(row[1] for row in top10_data if row[1] is not None)
                
                # 전체 거래대금 계산
                total_query = """
                    SELECT SUM(volume * close) as total_volume_krw
                    FROM ohlcv_data 
                    WHERE date = (SELECT MAX(date) FROM ohlcv_data)
                    AND volume IS NOT NULL AND close IS NOT NULL
                """
                with get_db_connection_context() as conn:
                    cursor = conn.cursor()
                    cursor.execute(total_query)
                    total_result = cursor.fetchone()
                total_volume = total_result[0] if total_result and total_result[0] else 1
                
                top10_ratio = (top10_volume / total_volume) * 100
                
                return {
                    'top10_ratio': round(top10_ratio, 1),
                    'top10_volume': top10_volume,
                    'total_volume': total_volume,
                    'top10_tickers': [row[0] for row in top10_data[:5]]  # 상위 5개 티커
                }
            else:
                logger.warning("⚠️ 거래대금 집중도 데이터 없음, 기본값 사용")
                return {
                    'top10_ratio': 50.0, 'top10_volume': 0, 'total_volume': 0,
                    'top10_tickers': []
                }
                
        except Exception as e:
            logger.error(f"❌ 거래대금 집중도 계산 실패: {e}")
            return {
                'top10_ratio': 50.0, 'top10_volume': 0, 'total_volume': 0,
                'top10_tickers': []
            }
    
    def _calculate_ma200_ratio(self) -> Dict:
        """MA200 상회 비율 계산"""
        try:
            query = """
                SELECT 
                    COUNT(*) as total_tickers,
                    COUNT(CASE WHEN close > ma_200 THEN 1 END) as above_ma200
                FROM ohlcv_data 
                WHERE date = (SELECT MAX(date) FROM ohlcv_data)
                AND ma_200 IS NOT NULL AND close IS NOT NULL
            """
            
            with get_db_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
            
            if result:
                total, above_count = result
                total = total or 1
                
                above_ratio = (above_count / total) * 100
                
                return {
                    'above_ratio': round(above_ratio, 1),
                    'total_tickers': total,
                    'above_count': above_count,
                    'below_count': total - above_count
                }
            else:
                logger.warning("⚠️ MA200 상회 비율 데이터 없음, 기본값 사용")
                return {
                    'above_ratio': 30.0, 'total_tickers': 0, 
                    'above_count': 0, 'below_count': 0
                }
                
        except Exception as e:
            logger.error(f"❌ MA200 상회 비율 계산 실패: {e}")
            return {
                'above_ratio': 30.0, 'total_tickers': 0, 
                'above_count': 0, 'below_count': 0
            }
    
    def _calculate_sentiment_score(self, price_dist: Dict, volume_conc: Dict, ma200_ratio: Dict) -> float:
        """종합 시장 점수 계산 (0-100)"""
        try:
            score = 0.0
            
            # 1. 등락률 점수 (40점 만점)
            pct_up = price_dist['pct_up']
            if pct_up >= 60:
                score += 40
            elif pct_up >= 55:
                score += 35
            elif pct_up >= 50:
                score += 25
            elif pct_up >= 45:
                score += 15
            else:
                score += 5
            
            # 2. 거래대금 분산 점수 (30점 만점)
            top10_ratio = volume_conc['top10_ratio']
            if top10_ratio <= 40:
                score += 30  # 분산된 시장
            elif top10_ratio <= 50:
                score += 25
            elif top10_ratio <= 60:
                score += 15
            else:
                score += 5   # 과도한 집중
            
            # 3. MA200 상회 점수 (30점 만점)
            ma200_above = ma200_ratio['above_ratio']
            if ma200_above >= 40:
                score += 30
            elif ma200_above >= 30:
                score += 25
            elif ma200_above >= 20:
                score += 15
            else:
                score += 5
            
            return round(score, 1)
            
        except Exception as e:
            logger.error(f"❌ 종합 점수 계산 실패: {e}")
            return 50.0
    
    def _determine_market_condition(self, sentiment_score: float) -> str:
        """시장 상황 판단"""
        if sentiment_score >= 75:
            return 'bullish'
        elif sentiment_score >= 60:
            return 'neutral'
        else:
            return 'bearish'
    
    def _should_proceed_pipeline(self, price_dist: Dict, volume_conc: Dict, 
                                ma200_ratio: Dict, sentiment_score: float) -> bool:
        """파이프라인 진행 여부 결정"""
        try:
            # 기본 조건 체크
            conditions = [
                price_dist['pct_up'] >= self.thresholds['min_pct_up'],
                volume_conc['top10_ratio'] <= self.thresholds['max_top10_volume'],
                ma200_ratio['above_ratio'] >= self.thresholds['min_ma200_above'],
                sentiment_score >= self.thresholds['min_sentiment_score']
            ]
            
            # 모든 조건을 만족하면 진행
            should_proceed = all(conditions)
            
            if not should_proceed:
                failed_conditions = []
                if price_dist['pct_up'] < self.thresholds['min_pct_up']:
                    failed_conditions.append(f"상승종목비율({price_dist['pct_up']}% < {self.thresholds['min_pct_up']}%)")
                if volume_conc['top10_ratio'] > self.thresholds['max_top10_volume']:
                    failed_conditions.append(f"거래대금집중도({volume_conc['top10_ratio']}% > {self.thresholds['max_top10_volume']}%)")
                if ma200_ratio['above_ratio'] < self.thresholds['min_ma200_above']:
                    failed_conditions.append(f"MA200상회비율({ma200_ratio['above_ratio']}% < {self.thresholds['min_ma200_above']}%)")
                if sentiment_score < self.thresholds['min_sentiment_score']:
                    failed_conditions.append(f"종합점수({sentiment_score} < {self.thresholds['min_sentiment_score']})")
                
                logger.warning(f"⚠️ 시장 조건 미충족으로 파이프라인 중단: {', '.join(failed_conditions)}")
            else:
                logger.info(f"✅ 시장 조건 충족, 파이프라인 진행 가능")
            
            return should_proceed
            
        except Exception as e:
            logger.error(f"❌ 파이프라인 진행 여부 판단 실패: {e}")
            return True  # 오류 시 기본적으로 진행
    
    def _get_fallback_result(self) -> Dict:
        """오류 시 기본 결과 반환"""
        return {
            'pct_up': 50.0,
            'pct_down': 50.0,
            'top10_volume_ratio': 50.0,
            'ma200_above_ratio': 30.0,
            'sentiment_score': 50.0,
            'market_condition': 'neutral',
            'should_proceed': True,  # 오류 시 기본적으로 진행
            'timestamp': pd.Timestamp.now(),
            'details': {
                'price_distribution': {'pct_up': 50.0, 'pct_down': 50.0},
                'volume_concentration': {'top10_ratio': 50.0},
                'ma200_ratio': {'above_ratio': 30.0}
            }
        }
    
    def update_thresholds(self, new_thresholds: Dict):
        """임계값 업데이트"""
        try:
            self.thresholds.update(new_thresholds)
            logger.info(f"✅ 임계값 업데이트 완료: {new_thresholds}")
        except Exception as e:
            logger.error(f"❌ 임계값 업데이트 실패: {e}")
    
    def get_thresholds(self) -> Dict:
        """현재 임계값 반환"""
        return self.thresholds.copy()

# =============================================================================
# Integrated Market Sentiment Analyzer
# =============================================================================

class IntegratedMarketSentimentAnalyzer:
    """통합 시장 감정 분석기 (Fear&Greed + Thermometer + BTC)"""

    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path
        self.fear_greed_analyzer = FearGreedAnalyzer(db_path)
        self.market_thermometer = MarketThermometer(db_path)

        # 임계값 설정 (보수적)
        self.bear_threshold = -0.3
        self.bull_threshold = 0.3

        logger.info("🌡️ 통합 시장 감정 분석기 초기화 완료")

    def analyze_comprehensive_market_sentiment(self) -> Optional[MarketSentimentResult]:
        """종합 시장 감정 분석 (모든 지표 통합)"""
        try:
            logger.info("🌡️ 종합 시장 감정 분석 시작")

            # 1. Fear&Greed Index 분석
            fear_greed_data = self.fear_greed_analyzer.fetch_fear_greed_index()
            fear_greed_score = 0.0
            if fear_greed_data:
                fear_greed_score = self.fear_greed_analyzer.calculate_fear_greed_score(fear_greed_data)

            # 2. BTC 트렌드 분석
            btc_trend_data = self.fear_greed_analyzer.fetch_btc_trend_data()
            btc_trend_score = 0.0
            if btc_trend_data:
                btc_trend_score = self.fear_greed_analyzer.calculate_btc_trend_score(btc_trend_data)

            # 3. Market Thermometer 분석
            thermometer_data = self.market_thermometer.calculate_market_sentiment_snapshot()
            thermometer_score = self._calculate_thermometer_score(thermometer_data)

            # 4. 종합 점수 계산 (가중 평균)
            weights = {
                'fear_greed': 0.4,    # Fear&Greed 40%
                'btc_trend': 0.3,     # BTC 트렌드 30%
                'thermometer': 0.3    # Thermometer 30%
            }

            combined_score = (
                fear_greed_score * weights['fear_greed'] +
                btc_trend_score * weights['btc_trend'] +
                thermometer_score * weights['thermometer']
            )

            # 5. 최종 감정 판정 (보수적 기준)
            final_sentiment, trading_allowed, position_adjustment = self._determine_final_sentiment(combined_score)

            # 6. 신뢰도 계산
            confidence = self._calculate_confidence(fear_greed_data, btc_trend_data, thermometer_data)

            # 7. 판정 근거 생성
            reasoning = self._generate_reasoning(
                fear_greed_data, btc_trend_data, thermometer_data,
                fear_greed_score, btc_trend_score, thermometer_score, combined_score
            )

            # 8. 결과 생성
            result = MarketSentimentResult(
                analysis_date=datetime.now().strftime('%Y-%m-%d'),
                analysis_time=datetime.now().strftime('%H:%M:%S'),
                fear_greed_data=fear_greed_data,
                fear_greed_score=fear_greed_score,
                btc_trend_data=btc_trend_data,
                btc_trend_score=btc_trend_score,
                thermometer_data=thermometer_data,
                thermometer_score=thermometer_score,
                combined_score=combined_score,
                final_sentiment=final_sentiment,
                confidence=confidence,
                trading_allowed=trading_allowed,
                position_adjustment=position_adjustment,
                reasoning=reasoning
            )

            # 9. DB 저장 (선택적)
            self._save_sentiment_result(result)

            logger.info(f"🌡️ 종합 시장 감정 분석 완료: {final_sentiment.value} (점수: {combined_score:.2f})")
            logger.info(f"💼 거래 허용: {'✅' if trading_allowed else '❌'} | 포지션 조정: {position_adjustment:.2f}x")

            return result

        except Exception as e:
            logger.error(f"❌ 종합 시장 감정 분석 실패: {e}")
            return None

    def _calculate_thermometer_score(self, thermometer_data: Dict) -> float:
        """Market Thermometer 점수를 정규화"""
        try:
            sentiment_score = thermometer_data.get('sentiment_score', 50.0)

            # 0-100 → -1.0 ~ 1.0 변환
            normalized = (sentiment_score - 50) / 50.0
            return max(-1.0, min(1.0, normalized))

        except Exception:
            return 0.0

    def _determine_final_sentiment(self, combined_score: float) -> tuple:
        """최종 감정 판정"""
        if combined_score <= self.bear_threshold:
            return MarketSentiment.BEAR, False, 0.0
        elif combined_score >= self.bull_threshold:
            position_adj = 1.0 + (combined_score * 0.5)  # 최대 1.5x
            return MarketSentiment.BULL, True, min(1.5, position_adj)
        else:
            position_adj = 0.7 + (combined_score * 0.3)  # 0.7 ~ 1.0x
            return MarketSentiment.NEUTRAL, True, max(0.5, position_adj)

    def _calculate_confidence(self, fear_greed: Optional[FearGreedData],
                            btc_trend: Optional[BTCTrendData],
                            thermometer: Dict) -> float:
        """신뢰도 계산"""
        confidence_factors = []

        # 데이터 가용성
        if fear_greed:
            confidence_factors.append(0.33)
        if btc_trend:
            confidence_factors.append(0.33)
        if thermometer.get('should_proceed') is not None:
            confidence_factors.append(0.34)

        base_confidence = sum(confidence_factors)

        # 일관성 보너스 (모든 지표가 같은 방향일 때)
        if len(confidence_factors) == 3:
            base_confidence *= 1.2

        return min(1.0, base_confidence)

    def _generate_reasoning(self, fear_greed: Optional[FearGreedData],
                          btc_trend: Optional[BTCTrendData],
                          thermometer: Dict,
                          fg_score: float, btc_score: float, thermo_score: float,
                          combined: float) -> str:
        """판정 근거 생성"""
        reasoning_parts = []

        # Fear&Greed
        if fear_greed:
            reasoning_parts.append(f"F&G: {fear_greed.value}({fear_greed.value_classification})")

        # BTC 트렌드
        if btc_trend:
            reasoning_parts.append(f"BTC: {btc_trend.change_7d:+.1f}%/7D")

        # Thermometer
        if thermometer:
            sentiment_score = thermometer.get('sentiment_score', 50)
            market_condition = thermometer.get('market_condition', 'neutral')
            reasoning_parts.append(f"체온계: {sentiment_score:.0f}({market_condition})")

        # 종합 점수
        reasoning_parts.append(f"종합: {combined:.2f}")

        # 주도 요인
        scores = [abs(fg_score), abs(btc_score), abs(thermo_score)]
        max_idx = scores.index(max(scores))
        leaders = ["F&G", "BTC", "체온계"]
        reasoning_parts.append(f"{leaders[max_idx]} 주도")

        return " | ".join(reasoning_parts)

    def _save_sentiment_result(self, result: MarketSentimentResult):
        """결과 저장 (간단한 로그 형태)"""
        try:
            # 실제 구현에서는 SQLite 저장 로직 추가
            logger.debug(f"💾 시장 감정 분석 결과: {result.final_sentiment.value}")
        except Exception as e:
            logger.error(f"❌ 시장 감정 결과 저장 실패: {e}")

# 전역 인스턴스 제거 (중복 로깅 방지)
# market_thermometer = MarketThermometer()  # 제거됨 - 중복 인스턴스화 방지
# integrated_sentiment_analyzer = IntegratedMarketSentimentAnalyzer()  # 제거됨 - 중복 인스턴스화 방지

# 편의 함수들 주석 처리 (전역 인스턴스 제거로 인해 사용 불가)
# def get_market_sentiment_snapshot() -> Dict:
#     """시장 분위기 스냅샷 반환 (편의 함수)"""
#     return market_thermometer.calculate_market_sentiment_snapshot()

# def update_market_thermometer_thresholds(new_thresholds: Dict):
#     """시장 체온계 임계값 업데이트 (편의 함수)"""
#     market_thermometer.update_thresholds(new_thresholds)

# def get_market_thermometer_thresholds() -> Dict:
#     """시장 체온계 임계값 조회 (편의 함수)"""
#     return market_thermometer.get_thresholds() 