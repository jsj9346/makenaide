"""
시장 체온계(Market Thermometer) 모듈

🎯 핵심 기능:
- 4개 핵심 지표로 시장 분위기 빠른 파악
- 파이프라인 진입 여부 결정
- 경량화된 시장 분석

📊 지표 구성:
1. 등락률 분포 (pct_up, pct_down)
2. 거래대금 상위 10개 집중도 (top10_volume_ratio)  
3. MA200 상회 비율 (ma200_above_ratio)
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from db_manager import get_db_manager
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# 환경변수 로딩
load_dotenv()

# 로깅 설정
logger = logging.getLogger(__name__)

class MarketThermometer:
    """시장 체온계 클래스"""
    
    def __init__(self):
        self.db_manager = get_db_manager()
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
                
                # 기본값 설정
                default_thresholds = {
                    'min_pct_up': 45.0,
                    'max_top10_volume': 70.0,
                    'min_ma200_above': 25.0,
                    'min_sentiment_score': 60.0
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
                    'min_pct_up': 45.0,
                    'max_top10_volume': 70.0,
                    'min_ma200_above': 25.0,
                    'min_sentiment_score': 60.0
                }
                
        except Exception as e:
            logger.error(f"❌ 설정 파일 로드 실패: {e}")
            return {
                'min_pct_up': 45.0,
                'max_top10_volume': 70.0,
                'min_ma200_above': 25.0,
                'min_sentiment_score': 60.0
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
                    FROM ohlcv 
                    WHERE date = (SELECT MAX(date) FROM ohlcv)
                    AND close IS NOT NULL
                ),
                previous_data AS (
                    SELECT 
                        ticker,
                        close as prev_close,
                        date
                    FROM ohlcv 
                    WHERE date = (SELECT MAX(date) FROM ohlcv WHERE date < (SELECT MAX(date) FROM ohlcv))
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
            
            result = self.db_manager.execute_query(query, fetchone=True)
            
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
                FROM ohlcv 
                WHERE date = (SELECT MAX(date) FROM ohlcv)
                AND volume IS NOT NULL AND close IS NOT NULL
                ORDER BY volume * close DESC
                LIMIT 10
            """
            
            top10_data = self.db_manager.execute_query(query)
            
            if top10_data:
                top10_volume = sum(row[1] for row in top10_data if row[1] is not None)
                
                # 전체 거래대금 계산
                total_query = """
                    SELECT SUM(volume * close) as total_volume_krw
                    FROM ohlcv 
                    WHERE date = (SELECT MAX(date) FROM ohlcv)
                    AND volume IS NOT NULL AND close IS NOT NULL
                """
                total_result = self.db_manager.execute_query(total_query, fetchone=True)
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
                FROM ohlcv 
                WHERE date = (SELECT MAX(date) FROM ohlcv)
                AND ma_200 IS NOT NULL AND close IS NOT NULL
            """
            
            result = self.db_manager.execute_query(query, fetchone=True)
            
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

# 전역 인스턴스
market_thermometer = MarketThermometer()

def get_market_sentiment_snapshot() -> Dict:
    """시장 분위기 스냅샷 반환 (편의 함수)"""
    return market_thermometer.calculate_market_sentiment_snapshot()

def update_market_thermometer_thresholds(new_thresholds: Dict):
    """시장 체온계 임계값 업데이트 (편의 함수)"""
    market_thermometer.update_thresholds(new_thresholds)

def get_market_thermometer_thresholds() -> Dict:
    """시장 체온계 임계값 조회 (편의 함수)"""
    return market_thermometer.get_thresholds() 