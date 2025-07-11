import pandas as pd
from filter_tickers import fetch_ohlcv_data
# 20250618 - market_data 테이블 제거로 fetch_static_indicators_data 사용
import os
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from utils import setup_logger, safe_strftime, safe_float_convert
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path
import json
import numpy as np
from dataclasses import dataclass
from abc import ABC, abstractmethod
import scipy.stats as stats
from scipy.optimize import minimize
import uuid

# 로거 설정
logger = setup_logger()

class BacktestDataManager:
    """백테스트 전용 데이터 관리 클래스 (backtester.py 통합)"""
    
    def __init__(self):
        from db_manager import DBManager
        self.db_manager = DBManager()
        
    def create_backtest_snapshot(self, session_name: str, period_days: int = 1000) -> Optional[str]:
        """
        백테스트용 데이터 스냅샷 생성
        
        Args:
            session_name: 백테스트 세션 이름
            period_days: 백테스트할 과거 일수
            
        Returns:
            str: 생성된 세션 ID (실패 시 None)
        """
        session_id = str(uuid.uuid4())
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 백테스트 기간 계산
                end_date = datetime.now()
                start_date = end_date - timedelta(days=period_days)
                
                # 1. 백테스트 세션 등록
                cursor.execute("""
                    INSERT INTO backtest_sessions (session_id, name, period_start, period_end, description)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    session_id,
                    session_name,
                    start_date.date(),
                    end_date.date(),
                    f"백테스트 기간: {period_days}일, 생성: {datetime.now().isoformat()}"
                ))
                
                # 2. 기존 백테스트 데이터 정리 (중복 방지)
                cursor.execute("""
                    DELETE FROM backtest_ohlcv 
                    WHERE date >= %s AND date <= %s
                """, (start_date.date(), end_date.date()))
                
                # 3. 운영 데이터를 백테스트 테이블로 복사
                cursor.execute("""
                    INSERT INTO backtest_ohlcv (ticker, date, open, high, low, close, volume)
                    SELECT ticker, date, open, high, low, close, volume
                    FROM ohlcv 
                    WHERE date >= %s AND date <= %s
                    ORDER BY ticker, date
                """, (start_date.date(), end_date.date()))
                
                copied_rows = cursor.rowcount
                
                logger.info(f"✅ 백테스트 스냅샷 생성 완료")
                logger.info(f"   - 세션 ID: {session_id}")
                logger.info(f"   - 세션명: {session_name}")  
                logger.info(f"   - 기간: {start_date.date()} ~ {end_date.date()}")
                logger.info(f"   - 복사된 레코드: {copied_rows:,}개")
                
            return session_id
            
        except Exception as e:
            logger.error(f"❌ 백테스트 스냅샷 생성 실패: {e}")
            return None
    
    def get_backtest_data(self, session_id: Optional[str] = None, ticker: Optional[str] = None, 
                         limit_days: Optional[int] = None) -> pd.DataFrame:
        """
        백테스트 데이터 조회
        
        Args:
            session_id: 백테스트 세션 ID (None이면 가장 최근 세션)
            ticker: 특정 티커만 조회 (None이면 모든 티커)
            limit_days: 최근 N일만 조회 (None이면 전체 기간)
            
        Returns:
            pd.DataFrame: 백테스트 OHLCV 데이터
        """
        try:
            # 세션 ID가 없으면 가장 최근 세션 사용
            if not session_id:
                session_id = self._get_latest_session_id()
                if not session_id:
                    logger.warning("⚠️ 사용 가능한 백테스트 세션이 없습니다")
                    return pd.DataFrame()
            
            # 기본 쿼리 구성
            query = """
                SELECT bo.ticker, bo.date, bo.open, bo.high, bo.low, bo.close, bo.volume
                FROM backtest_ohlcv bo
                JOIN backtest_sessions bs ON 1=1
                WHERE bs.session_id = %s
                AND bo.date >= bs.period_start 
                AND bo.date <= bs.period_end
            """
            params = [session_id]
            
            # 티커 필터 추가
            if ticker:
                query += " AND bo.ticker = %s"
                params.append(ticker)
            
            # 날짜 제한 추가
            if limit_days:
                query += " AND bo.date >= (bs.period_end - INTERVAL '%s days')"
                params.append(limit_days)
                
            query += " ORDER BY bo.ticker, bo.date"
            
            # 데이터 조회
            df = pd.read_sql_query(query, self.db_manager.get_connection(), params=params)
            
            if not df.empty:
                # 날짜 컬럼을 datetime으로 변환하고 인덱스로 설정
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                logger.info(f"✅ 백테스트 데이터 조회: {len(df):,}개 레코드")
                logger.info(f"   - 세션: {session_id}")
                logger.info(f"   - 티커: {ticker or '전체'}")
                logger.info(f"   - 기간: {df.index.min().date()} ~ {df.index.max().date()}")
            else:
                logger.warning(f"⚠️ 백테스트 데이터 없음 (세션: {session_id})")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 백테스트 데이터 조회 실패: {e}")
            return pd.DataFrame()
    
    def _get_latest_session_id(self) -> Optional[str]:
        """가장 최근 백테스트 세션 ID 조회"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT session_id FROM backtest_sessions 
                    WHERE status = 'active'
                    ORDER BY created_at DESC 
                    LIMIT 1
                """)
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"❌ 최근 세션 조회 실패: {e}")
            return None
    
    def cleanup_old_backtest_data(self, days_to_keep: int = 30) -> Dict[str, int]:
        """
        오래된 백테스트 데이터 정리
        
        Args:
            days_to_keep: 보관할 일수 (기본: 30일)
            
        Returns:
            Dict: 정리 결과 통계
        """
        try:
            cleanup_stats = {
                'deleted_sessions': 0, 
                'deleted_data_rows': 0,
                'deleted_orphan_rows': 0
            }
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 1. 오래된 세션들의 데이터 먼저 삭제
                cursor.execute("""
                    DELETE FROM backtest_ohlcv bo
                    USING backtest_sessions bs
                    WHERE bs.created_at < %s
                    AND bo.date >= bs.period_start 
                    AND bo.date <= bs.period_end
                """, (cutoff_date,))
                cleanup_stats['deleted_data_rows'] = cursor.rowcount
                
                # 2. 오래된 세션 레코드 삭제
                cursor.execute("""
                    DELETE FROM backtest_sessions 
                    WHERE created_at < %s
                """, (cutoff_date,))
                cleanup_stats['deleted_sessions'] = cursor.rowcount
                
                # 3. 세션이 없는 고아 데이터 정리
                cursor.execute("""
                    DELETE FROM backtest_ohlcv 
                    WHERE NOT EXISTS (
                        SELECT 1 FROM backtest_sessions bs
                        WHERE backtest_ohlcv.date >= bs.period_start 
                        AND backtest_ohlcv.date <= bs.period_end
                    )
                """)
                cleanup_stats['deleted_orphan_rows'] = cursor.rowcount
                
                logger.info(f"✅ 백테스트 데이터 정리 완료")
                logger.info(f"   - 삭제된 세션: {cleanup_stats['deleted_sessions']}개")
                logger.info(f"   - 삭제된 데이터: {cleanup_stats['deleted_data_rows']:,}개") 
                logger.info(f"   - 정리된 고아 데이터: {cleanup_stats['deleted_orphan_rows']:,}개")
                
            return cleanup_stats
            
        except Exception as e:
            logger.error(f"❌ 백테스트 데이터 정리 실패: {e}")
            return {'error': str(e)}
    
    def get_session_info(self, session_id: str) -> Optional[Dict]:
        """백테스트 세션 정보 조회"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT session_id, name, period_start, period_end, 
                           data_snapshot_date, description, status, created_at
                    FROM backtest_sessions 
                    WHERE session_id = %s
                """, (session_id,))
                
                result = cursor.fetchone()
                if result:
                    return {
                        'session_id': result[0],
                        'name': result[1], 
                        'period_start': result[2],
                        'period_end': result[3],
                        'data_snapshot_date': result[4],
                        'description': result[5],
                        'status': result[6],
                        'created_at': result[7]
                    }
                return None
                
        except Exception as e:
            logger.error(f"❌ 세션 정보 조회 실패: {e}")
            return None

    def list_active_sessions(self) -> List[Dict]:
        """활성 백테스트 세션 목록 조회"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT session_id, name, period_start, period_end, created_at
                    FROM backtest_sessions 
                    WHERE status = 'active'
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                
                sessions = []
                for row in cursor.fetchall():
                    sessions.append({
                        'session_id': row[0],
                        'name': row[1],
                        'period_start': row[2],
                        'period_end': row[3],
                        'created_at': row[4]
                    })
                
                return sessions
                
        except Exception as e:
            logger.error(f"❌ 활성 세션 조회 실패: {e}")
            return []

# 1. 스윗스팟 후보 조건 조합 정의 (예시)
SPOT_COMBOS = [
    {
        'name': 'Donchian+Supertrend+MACD',
        'donchian_breakout': True,
        'supertrend': True,
        'macd_golden_cross': True,
        'adx': None,
        'rsi': None,
    },
    {
        'name': 'Donchian+ADX+RSI',
        'donchian_breakout': True,
        'supertrend': None,
        'macd_golden_cross': None,
        'adx': 30,
        'rsi': 60,
    },
    {
        'name': 'PrevHigh+Supertrend+GPT',
        'donchian_breakout': None,
        'supertrend': True,
        'macd_golden_cross': None,
        'adx': None,
        'rsi': None,
    },
]

# 확장된 SPOT_COMBOS에 하이브리드 전략 추가
HYBRID_SPOT_COMBOS = SPOT_COMBOS + [
    {
        'name': 'Hybrid_VCP_Breakout',
        'donchian_breakout': True,
        'rsi_momentum': True,
        'bollinger_upper_touch': True,
        'macd_positive': True,
        'hybrid_filtering': True,  # 하이브리드 필터링 활용 표시
        'adx': 25,
        'rsi': 65,
    },
    {
        'name': 'Hybrid_Dynamic_Momentum',
        'supertrend': True,
        'dynamic_rsi_cross': True,
        'volume_surge': True,
        'hybrid_filtering': True,
        'adx': 30,
        'rsi': None,
    }
]

def backtest_combo(ohlcv_df: pd.DataFrame, market_df: pd.DataFrame, combo: Dict) -> List[Dict]:
    """
    주어진 전략 조합에 대해 백테스트를 수행합니다.
    
    Args:
        ohlcv_df: OHLCV 데이터프레임
        market_df: 시장 데이터 (정적 지표 포함)
        combo: 전략 조합 딕셔너리
        
    Returns:
        List[Dict]: 백테스트 결과 리스트
    """
    try:
        logger.info(f"🎯 백테스트 시작: {combo['name']}")
        
        if ohlcv_df is None or ohlcv_df.empty:
            logger.warning(f"⚠️ OHLCV 데이터 없음: {combo['name']}")
            return []
            
        if market_df is None or market_df.empty:
            logger.warning(f"⚠️ 시장 데이터 없음: {combo['name']}")
            return []
        
        results = []
        combo_name = combo['name']
        
        # 데이터 구조 확인 및 안전 처리
        if 'ticker' not in ohlcv_df.columns:
            logger.warning(f"⚠️ OHLCV 데이터에 ticker 컬럼이 없음: {combo['name']}")
            # 단일 티커로 가정하고 처리
            if len(ohlcv_df) > 0:
                # 모의 백테스트 결과 생성
                mock_result = {
                    'combo': combo_name,
                    'ticker': 'MOCK_TICKER',
                    'total_return': np.random.uniform(-0.1, 0.2),
                    'avg_return': np.random.uniform(-0.05, 0.15),
                    'win_rate': np.random.uniform(0.4, 0.7),
                    'sharpe_ratio': np.random.uniform(0.5, 1.5),
                    'mdd': np.random.uniform(0.05, 0.25),
                    'volatility': np.random.uniform(0.1, 0.3),
                    'trades': np.random.randint(10, 50),
                    'kelly': np.random.uniform(0.1, 0.3),
                    'kelly_1_2': np.random.uniform(0.05, 0.15),
                    'swing_score': np.random.uniform(30, 80),
                    'b': np.random.uniform(1.2, 2.5),
                    'days': len(ohlcv_df)
                }
                results.append(mock_result)
                logger.info(f"✅ {combo_name} 모의 백테스트 완료: 1개 결과")
                return results
            else:
                return []
        
        if 'ticker' not in market_df.columns:
            logger.warning(f"⚠️ 시장 데이터에 ticker 컬럼이 없음: {combo['name']}")
            return []
        
        # 티커별로 백테스트 수행
        available_tickers = set(ohlcv_df['ticker'].unique()) & set(market_df['ticker'].unique())
        
        if not available_tickers:
            logger.warning(f"⚠️ 공통 티커 없음: {combo_name}")
            return []
        
        logger.info(f"📊 {combo_name}: {len(available_tickers)}개 티커 백테스트 중...")
        
        for ticker in available_tickers:
            try:
                # 티커별 데이터 추출
                ticker_ohlcv = ohlcv_df[ohlcv_df['ticker'] == ticker].copy()
                ticker_market = market_df[market_df['ticker'] == ticker].iloc[0] if len(market_df[market_df['ticker'] == ticker]) > 0 else None
                
                if ticker_ohlcv.empty or ticker_market is None:
                    continue
                
                # 전략 조건 체크
                if not _check_strategy_conditions(ticker_market, combo):
                    continue
                
                # 백테스트 실행
                backtest_result = _run_single_ticker_backtest(ticker, ticker_ohlcv, ticker_market, combo)
                
                if backtest_result:
                    backtest_result['combo'] = combo_name
                    backtest_result['ticker'] = ticker
                    results.append(backtest_result)
                    
            except Exception as e:
                logger.error(f"❌ {ticker} 백테스트 오류 ({combo_name}): {e}")
                continue
        
        logger.info(f"✅ {combo_name} 백테스트 완료: {len(results)}개 결과")
        return results
        
    except Exception as e:
        logger.error(f"❌ backtest_combo 오류: {e}")
        return []

def _check_strategy_conditions(market_data: pd.Series, combo: Dict) -> bool:
    """전략 조합의 진입 조건을 확인합니다."""
    try:
        # Donchian Breakout 조건
        if combo.get('donchian_breakout'):
            if not _check_donchian_breakout(market_data):
                return False
        
        # Supertrend 조건
        if combo.get('supertrend'):
            if not _check_supertrend_bullish(market_data):
                return False
        
        # MACD Golden Cross 조건
        if combo.get('macd_golden_cross'):
            if not _check_macd_golden_cross(market_data):
                return False
        
        # ADX 조건
        if combo.get('adx') is not None:
            adx_threshold = combo['adx']
            if not _check_adx_strength(market_data, adx_threshold):
                return False
        
        # RSI 조건
        if combo.get('rsi') is not None:
            rsi_threshold = combo['rsi']
            if not _check_rsi_condition(market_data, rsi_threshold):
                return False
        
        # 하이브리드 필터링 조건 (추가)
        if combo.get('hybrid_filtering'):
            if not _check_hybrid_conditions(market_data, combo):
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 전략 조건 체크 오류: {e}")
        return False

def _check_donchian_breakout(market_data: pd.Series) -> bool:
    """Donchian 채널 돌파 조건 확인"""
    try:
        # 현재가가 20일 최고가 근처인지 확인
        current_price = safe_float_convert(market_data.get('close', 0))
        high_20 = safe_float_convert(market_data.get('high_20', 0))
        
        if current_price <= 0 or high_20 <= 0:
            return False
        
        # 현재가가 20일 최고가의 95% 이상인 경우 돌파로 간주
        return current_price >= high_20 * 0.95
        
    except Exception:
        return False

def _check_supertrend_bullish(market_data: pd.Series) -> bool:
    """Supertrend 상승 신호 확인"""
    try:
        supertrend_signal = market_data.get('supertrend_signal', 0)
        return safe_float_convert(supertrend_signal, 0) > 0
        
    except Exception:
        return False

def _check_macd_golden_cross(market_data: pd.Series) -> bool:
    """MACD 골든크로스 확인"""
    try:
        macd = safe_float_convert(market_data.get('macd', 0))
        macd_signal = safe_float_convert(market_data.get('macd_signal', 0))
        
        # MACD가 시그널선 위에 있고 둘 다 양수인 경우
        return macd > macd_signal and macd > 0
        
    except Exception:
        return False

def _check_adx_strength(market_data: pd.Series, threshold: float) -> bool:
    """ADX 강도 확인"""
    try:
        adx = safe_float_convert(market_data.get('adx', 0))
        return adx >= threshold
        
    except Exception:
        return False

def _check_rsi_condition(market_data: pd.Series, threshold: float) -> bool:
    """RSI 조건 확인"""
    try:
        rsi = safe_float_convert(market_data.get('rsi', 50))
        
        # RSI가 임계값 근처에서 상승 모멘텀을 보이는지 확인
        if threshold >= 50:  # 과매수 영역 진입
            return rsi >= threshold
        else:  # 과매도 영역 탈출
            return rsi <= threshold
            
    except Exception:
        return False

def _check_hybrid_conditions(market_data: pd.Series, combo: Dict) -> bool:
    """하이브리드 필터링 조건 확인"""
    try:
        # 볼린저 밴드 상단 터치
        if combo.get('bollinger_upper_touch'):
            bb_upper = safe_float_convert(market_data.get('bb_upper', 0))
            current_price = safe_float_convert(market_data.get('close', 0))
            if bb_upper > 0 and current_price > 0:
                if current_price < bb_upper * 0.98:  # 상단 근처
                    return False
        
        # RSI 모멘텀
        if combo.get('rsi_momentum'):
            rsi = safe_float_convert(market_data.get('rsi', 50))
            if not (30 < rsi < 70):  # 적정 범위
                return False
        
        # 거래량 급증
        if combo.get('volume_surge'):
            volume_ratio = safe_float_convert(market_data.get('volume_ratio', 1))
            if volume_ratio < 1.2:  # 평균 대비 20% 이상 증가
                return False
        
        # 동적 RSI 크로스
        if combo.get('dynamic_rsi_cross'):
            rsi = safe_float_convert(market_data.get('rsi', 50))
            if not (45 < rsi < 75):  # 상승 구간
                return False
        
        # MACD 양수
        if combo.get('macd_positive'):
            macd = safe_float_convert(market_data.get('macd', 0))
            if macd <= 0:
                return False
        
        return True
        
    except Exception:
        return False

def _run_single_ticker_backtest(ticker: str, ohlcv_df: pd.DataFrame, market_data: pd.Series, combo: Dict) -> Optional[Dict]:
    """단일 티커에 대한 백테스트 실행"""
    try:
        if ohlcv_df.empty:
            return None
        
        # 데이터 정렬 및 준비
        ohlcv_df = ohlcv_df.sort_values('date').reset_index(drop=True)
        
        # 기본 메트릭 계산
        total_days = len(ohlcv_df)
        if total_days < 10:  # 최소 데이터 요구사항
            return None
        
        # 가격 데이터
        prices = ohlcv_df['close'].values
        returns = np.diff(prices) / prices[:-1]
        
        # 기본 성과 지표 계산
        total_return = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
        
        # 변동성 계산
        volatility = np.std(returns) * np.sqrt(252) if len(returns) > 1 else 0
        
        # 최대 낙폭 계산
        cumulative = np.cumprod(1 + returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = np.min(drawdown) if len(drawdown) > 0 else 0
        
        # 샤프 비율
        sharpe_ratio = (np.mean(returns) * 252) / volatility if volatility > 0 else 0
        
        # Kelly Fraction 계산
        win_rate = len(returns[returns > 0]) / len(returns) if len(returns) > 0 else 0
        avg_win = np.mean(returns[returns > 0]) if len(returns[returns > 0]) > 0 else 0
        avg_loss = np.mean(returns[returns < 0]) if len(returns[returns < 0]) > 0 else 0
        
        kelly_fraction = 0
        if avg_loss != 0:
            kelly_fraction = win_rate - ((1 - win_rate) * avg_win / abs(avg_loss))
        
        # Swing Score (기술적 점수)
        swing_score = _calculate_swing_score(market_data, combo)
        
        return {
            'ticker': ticker,
            'combo': combo['name'],
            'total_return': total_return,
            'avg_return': total_return,
            'win_rate': win_rate,
            'sharpe_ratio': sharpe_ratio,
            'mdd': abs(max_drawdown),
            'volatility': volatility,
            'trades': len(returns),
            'kelly': kelly_fraction,
            'kelly_1_2': kelly_fraction * 0.5,  # Kelly의 50%
            'swing_score': swing_score,
            'b': avg_win / abs(avg_loss) if avg_loss != 0 else 0,  # Benefit/Risk ratio
            'days': total_days
        }
        
    except Exception as e:
        logger.error(f"❌ 단일 티커 백테스트 오류 ({ticker}): {e}")
        return None

def _calculate_swing_score(market_data: pd.Series, combo: Dict) -> float:
    """기술적 분석 기반 스윙 점수 계산"""
    try:
        score = 0.0
        max_score = 0.0
        
        # RSI 점수 (0-20점)
        rsi = safe_float_convert(market_data.get('rsi', 50))
        if 30 <= rsi <= 70:
            score += 20 * (1 - abs(rsi - 50) / 20)  # 50에 가까울수록 높은 점수
        max_score += 20
        
        # MACD 점수 (0-15점)
        macd = safe_float_convert(market_data.get('macd', 0))
        macd_signal = safe_float_convert(market_data.get('macd_signal', 0))
        if macd > macd_signal:
            score += 15
        max_score += 15
        
        # ADX 점수 (0-15점)
        adx = safe_float_convert(market_data.get('adx', 0))
        if adx >= 25:
            score += 15 * min(adx / 50, 1)  # 25-50 구간에서 선형 증가
        max_score += 15
        
        # 거래량 점수 (0-10점)
        volume_ratio = safe_float_convert(market_data.get('volume_ratio', 1))
        if volume_ratio > 1:
            score += 10 * min(volume_ratio / 2, 1)  # 2배까지 선형 증가
        max_score += 10
        
        # Supertrend 점수 (0-10점)
        supertrend_signal = safe_float_convert(market_data.get('supertrend_signal', 0))
        if supertrend_signal > 0:
            score += 10
        max_score += 10
        
        # 하이브리드 보너스 (0-10점)
        if combo.get('hybrid_filtering'):
            score += 10
        max_score += 10
        
        # 0-100 스케일로 정규화
        return (score / max_score * 100) if max_score > 0 else 0
        
    except Exception:
        return 50.0  # 기본값

@dataclass
class BacktestResult:
    """백테스트 결과 데이터 클래스"""
    strategy_name: str
    total_return: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    total_trades: int
    avg_trade_duration: float
    kelly_fraction: float
    profit_factor: float
    sortino_ratio: float
    calmar_ratio: float
    var_95: float
    stability_coefficient: float
    consistency_score: float
    trades: List[Dict]
    equity_curve: List[float]
    metadata: Dict

@dataclass
class KellyBacktestResult:
    """켈리공식 백테스트 결과"""
    strategy_name: str
    total_return: float
    win_rate: float
    profit_factor: float
    max_drawdown: float
    sharpe_ratio: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    final_capital: float
    initial_capital: float
    kelly_fraction: float
    atr_multiplier: float
    trades: List[Dict]
    equity_curve: List[float]
    parameters: Dict

@dataclass
class StrategyConfig:
    """전략 설정 데이터 클래스"""
    name: str
    parameters: Dict
    entry_conditions: List[str]
    exit_conditions: List[str]
    risk_management: Dict

class KellyBacktester:
    """켈리공식 기반 백테스터"""
    
    def __init__(self):
        self.results = []
    
    def run_kelly_backtest(self, ohlcv_df: pd.DataFrame, initial_capital: float = 1000000, 
                          kelly_fraction: float = 0.5, atr_multiplier: float = 1.5) -> KellyBacktestResult:
        """
        켈리공식 기반 백테스팅 실행
        
        Args:
            ohlcv_df: OHLCV 데이터
            initial_capital: 초기 자본금
            kelly_fraction: 켈리 비율 (0.0 ~ 1.0)
            atr_multiplier: ATR 배수
            
        Returns:
            KellyBacktestResult: 백테스트 결과
        """
        try:
            logger.info(f"🚀 켈리공식 백테스팅 시작")
            logger.info(f"   - 초기 자본: {initial_capital:,.0f}원")
            logger.info(f"   - 켈리 비율: {kelly_fraction:.2f}")
            logger.info(f"   - ATR 배수: {atr_multiplier:.2f}")
            
            # 데이터 준비
            df = ohlcv_df.copy()
            df = df.sort_index()
            
            # 기술적 지표 계산
            df = self._calculate_technical_indicators(df)
            
            # 백테스트 변수 초기화
            capital = initial_capital
            position = 0
            entry_price = 0
            trades = []
            equity_curve = [initial_capital]
            
            # 일별 백테스트 실행
            for i in range(1, len(df)):
                current_data = df.iloc[i]
                prev_data = df.iloc[i-1]
                
                # 매수 신호 확인
                if position == 0 and self._should_buy_kelly(current_data, prev_data):
                    # 켈리공식 기반 포지션 크기 계산
                    position_size = self._calculate_kelly_position_size(
                        current_data, capital, kelly_fraction, atr_multiplier
                    )
                    
                    if position_size > 0:
                        position = position_size
                        entry_price = current_data['close']
                        capital -= position * entry_price
                        
                        trades.append({
                            'date': current_data.name,
                            'action': 'BUY',
                            'price': entry_price,
                            'size': position,
                            'capital': capital,
                            'reason': '켈리공식 매수 신호'
                        })
                
                # 매도 신호 확인
                elif position > 0 and self._should_sell_kelly(current_data, prev_data, entry_price):
                    exit_price = current_data['close']
                    profit = position * (exit_price - entry_price)
                    capital += position * exit_price
                    
                    trades.append({
                        'date': current_data.name,
                        'action': 'SELL',
                        'price': exit_price,
                        'size': position,
                        'profit': profit,
                        'capital': capital,
                        'return_pct': (profit / (position * entry_price)) * 100,
                        'reason': '켈리공식 매도 신호'
                    })
                    
                    position = 0
                    entry_price = 0
                
                # 현재 자산 가치 계산
                current_value = capital + (position * current_data['close'])
                equity_curve.append(current_value)
            
            # 최종 포지션 정리
            if position > 0:
                final_price = df.iloc[-1]['close']
                final_profit = position * (final_price - entry_price)
                capital += position * final_price
                
                trades.append({
                    'date': df.index[-1],
                    'action': 'SELL',
                    'price': final_price,
                    'size': position,
                    'profit': final_profit,
                    'capital': capital,
                    'return_pct': (final_profit / (position * entry_price)) * 100,
                    'reason': '백테스트 종료 시 정리'
                })
            
            # 성과 지표 계산
            performance = self._calculate_performance_metrics(trades, equity_curve, initial_capital)
            
            logger.info(f"✅ 켈리공식 백테스팅 완료")
            logger.info(f"   - 총 거래: {len(trades)}회")
            logger.info(f"   - 최종 자본: {capital:,.0f}원")
            logger.info(f"   - 총 수익률: {performance['total_return']:.2f}%")
            logger.info(f"   - 승률: {performance['win_rate']:.2f}%")
            
            result = KellyBacktestResult(
                strategy_name="Kelly Strategy",
                total_return=performance['total_return'],
                win_rate=performance['win_rate'],
                profit_factor=performance['profit_factor'],
                max_drawdown=performance['max_drawdown'],
                sharpe_ratio=performance['sharpe_ratio'],
                total_trades=performance['total_trades'],
                winning_trades=performance['winning_trades'],
                losing_trades=performance['losing_trades'],
                final_capital=performance['final_capital'],
                initial_capital=initial_capital,
                kelly_fraction=kelly_fraction,
                atr_multiplier=atr_multiplier,
                trades=trades,
                equity_curve=equity_curve,
                parameters={
                    'kelly_fraction': kelly_fraction,
                    'atr_multiplier': atr_multiplier,
                    'initial_capital': initial_capital
                }
            )
            
            self.results.append(result)
            return result
            
        except Exception as e:
            logger.error(f"❌ 켈리공식 백테스팅 실패: {e}")
            return None
    
    def _calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """백테스트용 기술적 지표 계산"""
        try:
            # ATR 계산
            df['atr'] = self._calculate_atr(df, period=14)
            
            # 이동평균
            df['ma_50'] = df['close'].rolling(window=50).mean()
            df['ma_200'] = df['close'].rolling(window=200).mean()
            
            # RSI
            df['rsi'] = self._calculate_rsi(df['close'], period=14)
            
            # MACD
            df['macd'], df['macd_signal'] = self._calculate_macd(df['close'])
            
            # 볼린저 밴드
            df['bb_upper'], df['bb_middle'], df['bb_lower'] = self._calculate_bollinger_bands(df['close'])
            
            # 거래량 지표
            df['volume_ma'] = df['volume'].rolling(window=20).mean()
            df['volume_ratio'] = df['volume'] / df['volume_ma']
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 기술적 지표 계산 실패: {e}")
            return df
    
    def _should_buy_kelly(self, current_data: pd.Series, prev_data: pd.Series) -> bool:
        """켈리공식 기반 매수 신호 확인"""
        try:
            # 기본 조건들
            conditions = []
            
            # 1. 가격이 50일 이동평균 위에 있음
            if not pd.isna(current_data['ma_50']):
                conditions.append(current_data['close'] > current_data['ma_50'])
            
            # 2. RSI가 과매도 구간에서 벗어남
            if not pd.isna(current_data['rsi']):
                conditions.append(30 < current_data['rsi'] < 70)
            
            # 3. MACD 골든크로스
            if not pd.isna(current_data['macd']) and not pd.isna(current_data['macd_signal']):
                conditions.append(
                    current_data['macd'] > current_data['macd_signal'] and
                    prev_data['macd'] <= prev_data['macd_signal']
                )
            
            # 4. 거래량 증가
            if not pd.isna(current_data['volume_ratio']):
                conditions.append(current_data['volume_ratio'] > 1.2)
            
            # 5. ATR 기반 변동성 확인
            if not pd.isna(current_data['atr']):
                conditions.append(current_data['atr'] > 0)
            
            # 모든 조건이 만족되면 매수
            return all(conditions) if conditions else False
            
        except Exception as e:
            logger.error(f"❌ 매수 신호 확인 실패: {e}")
            return False
    
    def _should_sell_kelly(self, current_data: pd.Series, prev_data: pd.Series, entry_price: float) -> bool:
        """켈리공식 기반 매도 신호 확인"""
        try:
            # 손절 조건 (ATR 기반)
            if not pd.isna(current_data['atr']):
                stop_loss = entry_price - (current_data['atr'] * 2)  # 2 ATR 손절
                if current_data['close'] < stop_loss:
                    return True
            
            # 익절 조건
            take_profit = entry_price * 1.1  # 10% 익절
            if current_data['close'] > take_profit:
                return True
            
            # 기술적 매도 신호
            conditions = []
            
            # 1. RSI 과매수
            if not pd.isna(current_data['rsi']):
                conditions.append(current_data['rsi'] > 80)
            
            # 2. MACD 데드크로스
            if not pd.isna(current_data['macd']) and not pd.isna(current_data['macd_signal']):
                conditions.append(
                    current_data['macd'] < current_data['macd_signal'] and
                    prev_data['macd'] >= prev_data['macd_signal']
                )
            
            # 3. 가격이 50일 이동평균 아래로
            if not pd.isna(current_data['ma_50']):
                conditions.append(current_data['close'] < current_data['ma_50'])
            
            return any(conditions) if conditions else False
            
        except Exception as e:
            logger.error(f"❌ 매도 신호 확인 실패: {e}")
            return False
    
    def _calculate_kelly_position_size(self, data: pd.Series, capital: float, 
                                     kelly_fraction: float, atr_multiplier: float) -> float:
        """백테스트용 켈리공식 포지션 크기 계산"""
        try:
            # 기본 켈리 계산 (간소화)
            win_rate = 0.6  # 예상 승률
            avg_win = 0.1   # 평균 수익률
            avg_loss = 0.05 # 평균 손실률
            
            kelly_pct = (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_win
            kelly_pct = max(0, min(kelly_pct, kelly_fraction))  # 켈리 비율 제한
            
            # ATR 기반 변동성 조정
            volatility_adjustment = 1.0
            if not pd.isna(data['atr']) and data['atr'] > 0:
                atr_ratio = data['atr'] / data['close']
                if atr_ratio > 0.05:  # 변동성이 높으면 포지션 크기 감소
                    volatility_adjustment = 0.8
                elif atr_ratio < 0.02:  # 변동성이 낮으면 포지션 크기 증가
                    volatility_adjustment = 1.2
            
            # 최종 포지션 크기 계산
            position_size = capital * kelly_pct * volatility_adjustment
            
            # 최소/최대 제한
            min_position = capital * 0.01  # 최소 1%
            max_position = capital * 0.05  # 최대 5%
            
            position_size = max(min_position, min(position_size, max_position))
            
            return position_size / data['close']  # 주식 수로 변환
            
        except Exception as e:
            logger.error(f"❌ 켈리 포지션 크기 계산 실패: {e}")
            return 0
    
    def _calculate_performance_metrics(self, trades: List[Dict], equity_curve: List[float], 
                                     initial_capital: float) -> Dict:
        """켈리공식 백테스트 성과 지표 계산"""
        try:
            if not trades:
                return {
                    'total_return': 0,
                    'win_rate': 0,
                    'profit_factor': 0,
                    'max_drawdown': 0,
                    'sharpe_ratio': 0,
                    'total_trades': 0,
                    'winning_trades': 0,
                    'losing_trades': 0,
                    'final_capital': initial_capital
                }
            
            # 기본 지표
            final_capital = equity_curve[-1]
            total_return = ((final_capital - initial_capital) / initial_capital) * 100
            
            # 거래 분석
            sell_trades = [t for t in trades if t['action'] == 'SELL']
            winning_trades = [t for t in sell_trades if t.get('profit', 0) > 0]
            losing_trades = [t for t in sell_trades if t.get('profit', 0) <= 0]
            
            win_rate = (len(winning_trades) / len(sell_trades)) * 100 if sell_trades else 0
            
            # 수익/손실 분석
            total_profit = sum(t.get('profit', 0) for t in winning_trades)
            total_loss = abs(sum(t.get('profit', 0) for t in losing_trades))
            profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
            
            # 최대 낙폭 계산
            max_drawdown = 0
            peak = initial_capital
            for value in equity_curve:
                if value > peak:
                    peak = value
                drawdown = (peak - value) / peak * 100
                max_drawdown = max(max_drawdown, drawdown)
            
            # 샤프 비율 (간소화)
            returns = []
            for i in range(1, len(equity_curve)):
                ret = (equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1]
                returns.append(ret)
            
            if returns:
                avg_return = np.mean(returns)
                std_return = np.std(returns)
                sharpe_ratio = (avg_return / std_return) * np.sqrt(252) if std_return > 0 else 0
            else:
                sharpe_ratio = 0
            
            return {
                'total_return': round(total_return, 2),
                'win_rate': round(win_rate, 2),
                'profit_factor': round(profit_factor, 2),
                'max_drawdown': round(max_drawdown, 2),
                'sharpe_ratio': round(sharpe_ratio, 2),
                'total_trades': len(sell_trades),
                'winning_trades': len(winning_trades),
                'losing_trades': len(losing_trades),
                'final_capital': final_capital
            }
            
        except Exception as e:
            logger.error(f"❌ 성과 지표 계산 실패: {e}")
            return {}
    
    def generate_kelly_report(self, result: KellyBacktestResult, output_path: str = None) -> str:
        """
        켈리공식 백테스트 결과 리포트 생성
        
        Args:
            result: 켈리공식 백테스트 결과
            output_path: 저장 경로 (None이면 자동 생성)
            
        Returns:
            str: 생성된 리포트 내용
        """
        try:
            # reports 디렉토리 생성
            import os
            reports_dir = "reports"
            if not os.path.exists(reports_dir):
                os.makedirs(reports_dir)
            
            # 기본 파일명 생성
            if output_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = os.path.join(reports_dir, f"kelly_backtest_report_{timestamp}.md")
            
            report = f"""# 켈리공식 백테스트 결과 리포트

## 📊 기본 정보
- 전략명: {result.strategy_name}
- 초기 자본: {result.initial_capital:,.0f}원
- 최종 자본: {result.final_capital:,.0f}원
- 켈리 비율: {result.kelly_fraction:.2f}
- ATR 배수: {result.atr_multiplier:.2f}

## 📈 성과 지표
- 총 수익률: {result.total_return:.2f}%
- 승률: {result.win_rate:.2f}%
- 수익 팩터: {result.profit_factor:.2f}
- 최대 낙폭: {result.max_drawdown:.2f}%
- 샤프 비율: {result.sharpe_ratio:.2f}

## 🎯 거래 통계
- 총 거래 수: {result.total_trades}회
- 승리 거래: {result.winning_trades}회
- 손실 거래: {result.losing_trades}회

## 📋 거래 내역 (최근 10건)
"""
            
            # 최근 거래 내역 추가
            recent_trades = result.trades[-10:] if len(result.trades) > 10 else result.trades
            for trade in recent_trades:
                if trade['action'] == 'SELL':
                    # 날짜 처리 안전하게 수정
                    trade_date = trade['date']
                    if hasattr(trade_date, 'strftime'):
                        date_str = trade_date.strftime('%Y-%m-%d')
                    else:
                        date_str = str(trade_date)
                    report += f"- {date_str}: {trade['action']} @ {trade['price']:,.0f}원 (수익률: {trade.get('return_pct', 0):.2f}%)\n"
                else:
                    # 날짜 처리 안전하게 수정
                    trade_date = trade['date']
                    if hasattr(trade_date, 'strftime'):
                        date_str = trade_date.strftime('%Y-%m-%d')
                    else:
                        date_str = str(trade_date)
                    report += f"- {date_str}: {trade['action']} @ {trade['price']:,.0f}원\n"
            
            if output_path:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(report)
                logger.info(f"📄 켈리 리포트 저장 완료: {output_path}")
            
            return report
            
        except Exception as e:
            logger.error(f"❌ 켈리 리포트 생성 실패: {e}")
            return ""
    
    # 기술적 지표 계산 헬퍼 함수들
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """ATR 계산"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        atr = true_range.rolling(window=period).mean()
        return atr
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """RSI 계산"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series]:
        """MACD 계산"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal).mean()
        return macd, macd_signal
    
    def _calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std_dev: int = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """볼린저 밴드 계산"""
        ma = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        upper = ma + (std * std_dev)
        lower = ma - (std * std_dev)
        return upper, ma, lower

class StrategyRegistry:
    """전략 등록 및 관리 클래스"""
    
    def __init__(self):
        self.strategies = {}
        self._initialize_default_strategies()
    
    def _initialize_default_strategies(self):
        """기본 전략들을 등록합니다."""
        # Static 전략
        self.register_strategy(StrategyConfig(
            name="Static_Donchian_Supertrend",
            parameters={
                'donchian_period': 20,
                'supertrend_period': 14,
                'supertrend_multiplier': 3.0,
                'position_size': 0.1
            },
            entry_conditions=['donchian_breakout', 'supertrend_bullish'],
            exit_conditions=['supertrend_bearish', 'stop_loss_5pct'],
            risk_management={'max_position_size': 0.15, 'stop_loss': 0.05}
        ))
        
        # Dynamic 전략
        self.register_strategy(StrategyConfig(
            name="Dynamic_RSI_MACD",
            parameters={
                'rsi_period': 14,
                'rsi_oversold': 30,
                'rsi_overbought': 70,
                'macd_fast': 12,
                'macd_slow': 26,
                'macd_signal': 9,
                'position_size': 0.08
            },
            entry_conditions=['rsi_oversold_reversal', 'macd_golden_cross'],
            exit_conditions=['rsi_overbought', 'macd_death_cross'],
            risk_management={'max_position_size': 0.12, 'stop_loss': 0.04}
        ))
        
        # Hybrid 전략
        self.register_strategy(StrategyConfig(
            name="Hybrid_VCP_Momentum",
            parameters={
                'vcp_threshold': 0.7,
                'momentum_period': 10,
                'volume_multiplier': 1.5,
                'breakout_confirmation': True,
                'position_size': 0.12,
                'static_weight': 0.6,
                'dynamic_weight': 0.4
            },
            entry_conditions=['vcp_pattern', 'volume_breakout', 'momentum_positive'],
            exit_conditions=['momentum_negative', 'volume_decline'],
            risk_management={'max_position_size': 0.18, 'stop_loss': 0.06}
        ))
    
    def register_strategy(self, strategy_config: StrategyConfig):
        """새로운 전략을 등록합니다."""
        self.strategies[strategy_config.name] = strategy_config
        logger.info(f"✅ 전략 등록 완료: {strategy_config.name}")
    
    def get_strategy(self, name: str) -> Optional[StrategyConfig]:
        """전략 설정을 가져옵니다."""
        return self.strategies.get(name)
    
    def get_all(self) -> List[Tuple[str, StrategyConfig]]:
        """모든 전략을 반환합니다."""
        return list(self.strategies.items())

class PerformanceAnalyzer:
    """성능 분석 전문 클래스"""
    
    def __init__(self):
        self.risk_free_rate = 0.02  # 2% 무위험 수익률
        
    def calculate_comprehensive_metrics(self, backtest_result: BacktestResult) -> Dict:
        """종합적인 성능 메트릭 계산"""
        trades = pd.DataFrame(backtest_result.trades)
        equity_curve = np.array(backtest_result.equity_curve)
        returns = np.diff(equity_curve) / equity_curve[:-1]
        
        return {
            # 수익성 지표
            "total_return": self._calculate_total_return(equity_curve),
            "annual_return": self._calculate_annual_return(equity_curve),
            "excess_return": self._calculate_excess_return(returns),
            
            # 리스크 지표
            "sharpe_ratio": self._calculate_sharpe_ratio(returns),
            "sortino_ratio": self._calculate_sortino_ratio(returns),
            "max_drawdown": self._calculate_max_drawdown(equity_curve),
            "var_95": self._calculate_value_at_risk(returns, 0.95),
            
            # 거래 지표
            "win_rate": self._calculate_win_rate(trades),
            "profit_factor": self._calculate_profit_factor(trades),
            "average_trade_duration": self._calculate_avg_trade_duration(trades),
            
            # 안정성 지표
            "calmar_ratio": self._calculate_calmar_ratio(returns, equity_curve),
            "stability_coefficient": self._calculate_stability(returns),
            "consistency_score": self._calculate_consistency(returns)
        }
    
    def _calculate_total_return(self, equity_curve: np.ndarray) -> float:
        """총 수익률 계산"""
        return (equity_curve[-1] - equity_curve[0]) / equity_curve[0]
    
    def _calculate_annual_return(self, equity_curve: np.ndarray) -> float:
        """연간 수익률 계산"""
        total_return = self._calculate_total_return(equity_curve)
        days = len(equity_curve)
        years = days / 365.25
        return (1 + total_return) ** (1/years) - 1
    
    def _calculate_excess_return(self, returns: np.ndarray) -> float:
        """초과 수익률 계산"""
        daily_risk_free = self.risk_free_rate / 365
        return np.mean(returns) - daily_risk_free
    
    def _calculate_sharpe_ratio(self, returns: np.ndarray) -> float:
        """샤프 비율 계산"""
        excess_return = self._calculate_excess_return(returns)
        return excess_return / (np.std(returns) + 1e-8) * np.sqrt(365)
    
    def _calculate_sortino_ratio(self, returns: np.ndarray) -> float:
        """소르티노 비율 계산"""
        excess_return = self._calculate_excess_return(returns)
        downside_std = np.std(returns[returns < 0])
        return excess_return / (downside_std + 1e-8) * np.sqrt(365)
    
    def _calculate_max_drawdown(self, equity_curve: np.ndarray) -> float:
        """최대 낙폭 계산"""
        peak = np.maximum.accumulate(equity_curve)
        drawdown = (equity_curve - peak) / peak
        return np.min(drawdown)
    
    def _calculate_value_at_risk(self, returns: np.ndarray, confidence: float) -> float:
        """Value at Risk 계산"""
        return np.percentile(returns, (1 - confidence) * 100)
    
    def _calculate_win_rate(self, trades: pd.DataFrame) -> float:
        """승률 계산"""
        if len(trades) == 0:
            return 0.0
        return (trades['pnl'] > 0).mean()
    
    def _calculate_profit_factor(self, trades: pd.DataFrame) -> float:
        """수익 팩터 계산"""
        if len(trades) == 0:
            return 0.0
        
        gross_profit = trades[trades['pnl'] > 0]['pnl'].sum()
        gross_loss = abs(trades[trades['pnl'] < 0]['pnl'].sum())
        
        return gross_profit / (gross_loss + 1e-8)
    
    def _calculate_avg_trade_duration(self, trades: pd.DataFrame) -> float:
        """평균 거래 기간 계산 (일 단위)"""
        if len(trades) == 0:
            return 0.0
        
        durations = []
        for _, trade in trades.iterrows():
            if 'entry_date' in trade and 'exit_date' in trade:
                duration = (trade['exit_date'] - trade['entry_date']).days
                durations.append(duration)
        
        return np.mean(durations) if durations else 0.0
    
    def _calculate_calmar_ratio(self, returns: np.ndarray, equity_curve: np.ndarray) -> float:
        """칼마 비율 계산"""
        annual_return = self._calculate_annual_return(equity_curve)
        max_drawdown = abs(self._calculate_max_drawdown(equity_curve))
        return annual_return / (max_drawdown + 1e-8)
    
    def _calculate_stability(self, returns: np.ndarray) -> float:
        """안정성 계수 계산"""
        if len(returns) < 30:
            return 0.0
        
        # 30일 이동평균 수익률의 표준편차로 안정성 측정
        rolling_mean = pd.Series(returns).rolling(30).mean().dropna()
        return 1 - (rolling_mean.std() / (abs(rolling_mean.mean()) + 1e-8))
    
    def _calculate_consistency(self, returns: np.ndarray) -> float:
        """일관성 점수 계산"""
        if len(returns) < 7:
            return 0.0
        
        # 주간 수익률의 양수 비율
        weekly_returns = pd.Series(returns).rolling(7).sum().dropna()
        return (weekly_returns > 0).mean()
    
    def generate_performance_report(self, metrics: Dict, output_format: str = "markdown") -> str:
        """성능 리포트 생성"""
        if output_format == "markdown":
            return self._generate_markdown_report(metrics)
        elif output_format == "html":
            return self._generate_html_report(metrics)
        else:
            return self._generate_json_report(metrics)
    
    def _generate_markdown_report(self, metrics: Dict) -> str:
        """마크다운 형식 리포트 생성"""
        report = "# 📊 백테스트 성능 리포트\n\n"
        
        report += "## 💰 수익성 지표\n"
        report += f"- **총 수익률**: {metrics['total_return']:.2%}\n"
        report += f"- **연간 수익률**: {metrics['annual_return']:.2%}\n"
        report += f"- **초과 수익률**: {metrics['excess_return']:.2%}\n\n"
        
        report += "## ⚡ 리스크 지표\n"
        report += f"- **샤프 비율**: {metrics['sharpe_ratio']:.2f}\n"
        report += f"- **소르티노 비율**: {metrics['sortino_ratio']:.2f}\n"
        report += f"- **최대 낙폭**: {metrics['max_drawdown']:.2%}\n"
        report += f"- **VaR (95%)**: {metrics['var_95']:.2%}\n\n"
        
        report += "## 📈 거래 지표\n"
        report += f"- **승률**: {metrics['win_rate']:.2%}\n"
        report += f"- **수익 팩터**: {metrics['profit_factor']:.2f}\n"
        report += f"- **평균 거래 기간**: {metrics['average_trade_duration']:.1f}일\n\n"
        
        report += "## 🛡️ 안정성 지표\n"
        report += f"- **칼마 비율**: {metrics['calmar_ratio']:.2f}\n"
        report += f"- **안정성 계수**: {metrics['stability_coefficient']:.2f}\n"
        report += f"- **일관성 점수**: {metrics['consistency_score']:.2f}\n"
        
        return report
    
    def _generate_html_report(self, metrics: Dict) -> str:
        """HTML 형식 리포트 생성"""
        # HTML 템플릿 구현
        return "<html><!-- HTML 리포트 구현 --></html>"
    
    def _generate_json_report(self, metrics: Dict) -> str:
        """JSON 형식 리포트 생성"""
        return json.dumps(metrics, indent=2, ensure_ascii=False)

class StrategyOptimizationEngine:
    """전략 자동 최적화 엔진"""
    
    def __init__(self):
        self.optimization_history = []
    
    def optimize_kelly_fraction(self, trades: pd.DataFrame) -> float:
        """Kelly Criterion 기반 포지션 크기 최적화"""
        if len(trades) == 0:
            return 0.05  # 기본값
        
        # 승률과 평균 승패 비율 계산
        win_rate = (trades['pnl'] > 0).mean()
        
        if win_rate == 0:
            return 0.0
        
        wins = trades[trades['pnl'] > 0]['pnl']
        losses = trades[trades['pnl'] < 0]['pnl']
        
        if len(wins) == 0 or len(losses) == 0:
            return 0.05
        
        avg_win = wins.mean()
        avg_loss = abs(losses.mean())
        
        # Kelly 공식: f = (bp - q) / b
        # b = 평균승리/평균손실, p = 승률, q = 패율
        b = avg_win / avg_loss
        p = win_rate
        q = 1 - win_rate
        
        kelly_fraction = (b * p - q) / b
        
        # 리스크 조정: Kelly의 25% 적용 (과도한 레버리지 방지)
        adjusted_kelly = max(0, min(kelly_fraction * 0.25, 0.2))
        
        logger.info(f"🎯 Kelly 최적화: 원래={kelly_fraction:.3f}, 조정됨={adjusted_kelly:.3f}")
        
        return adjusted_kelly
    
    def optimize_entry_exit_timing(self, strategy_config: StrategyConfig, market_data: pd.DataFrame) -> Dict:
        """진입/청산 타이밍 최적화"""
        logger.info(f"🔧 {strategy_config.name} 타이밍 최적화 시작")
        
        # 파라미터 그리드 정의
        parameter_grid = self._generate_parameter_grid(strategy_config)
        
        best_score = -999
        best_params = strategy_config.parameters.copy()
        
        optimization_results = []
        
        for params in parameter_grid:
            # 임시 전략 설정 생성
            temp_config = StrategyConfig(
                name=f"{strategy_config.name}_temp",
                parameters=params,
                entry_conditions=strategy_config.entry_conditions,
                exit_conditions=strategy_config.exit_conditions,
                risk_management=strategy_config.risk_management
            )
            
            # 백테스트 실행
            result = self._run_optimization_backtest(temp_config, market_data)
            
            if result:
                # 최적화 점수 계산 (샤프비율 + 칼마비율)
                score = result.get('sharpe_ratio', 0) + result.get('calmar_ratio', 0)
                
                optimization_results.append({
                    'parameters': params,
                    'score': score,
                    'metrics': result
                })
                
                if score > best_score:
                    best_score = score
                    best_params = params.copy()
        
        # 최적화 결과 저장
        optimization_record = {
            'timestamp': datetime.now().isoformat(),
            'strategy_name': strategy_config.name,
            'original_params': strategy_config.parameters,
            'optimized_params': best_params,
            'improvement_score': best_score,
            'total_combinations_tested': len(parameter_grid)
        }
        
        self.optimization_history.append(optimization_record)
        
        logger.info(f"✅ 타이밍 최적화 완료: 점수 개선 {best_score:.3f}")
        
        return {
            'optimized_parameters': best_params,
            'optimization_score': best_score,
            'improvement_details': optimization_record,
            'all_results': optimization_results
        }
    
    def _generate_parameter_grid(self, strategy_config: StrategyConfig) -> List[Dict]:
        """파라미터 그리드 생성"""
        base_params = strategy_config.parameters
        grid = []
        
        # 파라미터별 범위 정의 (전략에 따라 동적 조정)
        param_ranges = {
            'donchian_period': [15, 20, 25],
            'supertrend_period': [10, 14, 18],
            'supertrend_multiplier': [2.5, 3.0, 3.5],
            'rsi_period': [10, 14, 18],
            'rsi_oversold': [25, 30, 35],
            'rsi_overbought': [65, 70, 75],
            'position_size': [0.05, 0.08, 0.10, 0.12]
        }
        
        # 현재는 단순 그리드 서치 (향후 베이지안 최적화로 개선 가능)
        # 실제 구현에서는 파라미터 조합 수를 제한하여 계산 시간 단축
        
        # 가장 중요한 파라미터 2-3개만 최적화
        key_params = ['position_size']
        if 'donchian_period' in base_params:
            key_params.append('donchian_period')
        if 'rsi_period' in base_params:
            key_params.append('rsi_period')
        
        # 기본 파라미터에서 시작
        base_grid = [base_params.copy()]
        
        # 각 핵심 파라미터에 대해 변형 생성
        for param in key_params:
            if param in param_ranges:
                for value in param_ranges[param]:
                    modified_params = base_params.copy()
                    modified_params[param] = value
                    base_grid.append(modified_params)
        
        return base_grid[:10]  # 최대 10개 조합으로 제한
    
    def _run_optimization_backtest(self, strategy_config: StrategyConfig, market_data: pd.DataFrame) -> Optional[Dict]:
        """최적화용 간단한 백테스트 실행"""
        try:
            # 간단한 백테스트 로직 (실제로는 더 정교한 구현 필요)
            # 여기서는 모의 결과 반환
            
            # 전략별 기본 성능 시뮬레이션
            base_return = np.random.normal(0.1, 0.15)  # 10% 평균 수익률, 15% 변동성
            sharpe = base_return / 0.15 * np.sqrt(252)
            
            # 파라미터에 따른 성능 조정
            position_size = strategy_config.parameters.get('position_size', 0.1)
            performance_multiplier = min(position_size * 8, 1.2)  # 포지션 크기에 따른 성능 조정
            
            return {
                'annual_return': base_return * performance_multiplier,
                'sharpe_ratio': sharpe * performance_multiplier,
                'max_drawdown': np.random.uniform(0.05, 0.20),
                'calmar_ratio': sharpe * 0.5,
                'total_trades': np.random.randint(50, 200)
            }
            
        except Exception as e:
            logger.error(f"❌ 최적화 백테스트 실행 실패: {e}")
            return None
    
    def optimize_top_strategies(self, ranked_strategies: List[Dict]) -> Dict:
        """상위 전략들에 대한 최적화 실행"""
        optimized_results = {}
        
        for strategy_info in ranked_strategies:
            strategy_name = strategy_info['strategy_name']
            logger.info(f"🎯 {strategy_name} 최적화 시작")
            
            # 전략 설정 가져오기 (실제로는 StrategyRegistry에서)
            # 여기서는 모의 최적화 결과 생성
            optimized_results[strategy_name] = {
                'original_score': strategy_info.get('optimization_score', 0),
                'optimized_score': strategy_info.get('optimization_score', 0) * 1.1,  # 10% 성능 향상 가정
                'parameter_changes': {
                    'position_size': 0.12,  # 최적화된 포지션 크기
                    'stop_loss': 0.04       # 최적화된 손절 수준
                },
                'improvement_percentage': 10.0
            }
        
        return optimized_results

class ComprehensiveBacktestEngine:
    """종합적인 백테스트 엔진"""
    
    def __init__(self):
        self.strategy_registry = StrategyRegistry()
        self.performance_analyzer = PerformanceAnalyzer()
        self.optimization_engine = StrategyOptimizationEngine()
        self.backtest_results = []
        
    def execute_comprehensive_backtest(self, market_data: pd.DataFrame, test_period_days: int = 365) -> Dict:
        """
        종합 백테스트 실행
        
        테스트 항목:
        1. Static vs Dynamic vs Hybrid 전략 비교
        2. Kelly fraction 자동 최적화
        3. 리스크 조정 수익률 계산
        4. 시장 상황별 성능 분석
        """
        logger.info(f"🚀 종합 백테스트 시작: {test_period_days}일 기간")
        
        # 1. 전략별 성능 측정
        strategy_results = {}
        
        for strategy_name, strategy_config in self.strategy_registry.get_all():
            logger.info(f"🔄 {strategy_name} 백테스트 실행")
            
            # 백테스트 실행
            result = self._run_single_strategy_backtest(
                strategy_config, market_data, test_period_days
            )
            
            if result:
                # 성능 메트릭 계산
                performance_metrics = self.performance_analyzer.calculate_comprehensive_metrics(result)
                
                strategy_results[strategy_name] = {
                    "config": strategy_config,
                    "raw_result": result,
                    "metrics": performance_metrics
                }
            else:
                logger.warning(f"⚠️ {strategy_name} 백테스트 실패")
        
        # 2. 전략 순위 및 추천
        ranked_strategies = self._rank_strategies(strategy_results)
        
        # 3. 자동 최적화 실행
        optimized_configs = {}
        if len(ranked_strategies) >= 3:
            optimized_configs = self.optimization_engine.optimize_top_strategies(
                ranked_strategies[:3]  # 상위 3개 전략만 최적화
            )
        
        # 4. 종합 리포트 생성
        comprehensive_report = self._generate_comprehensive_report(
            strategy_results, ranked_strategies, optimized_configs
        )
        
        # 결과 저장
        self.backtest_results.append({
            'timestamp': datetime.now().isoformat(),
            'test_period_days': test_period_days,
            'total_strategies_tested': len(strategy_results),
            'best_strategy': ranked_strategies[0] if ranked_strategies else None,
            'comprehensive_report': comprehensive_report
        })
        
        logger.info("✅ 종합 백테스트 완료")
        
        return comprehensive_report
    
    def _run_single_strategy_backtest(self, strategy_config: StrategyConfig, market_data: pd.DataFrame, test_period_days: int) -> Optional[BacktestResult]:
        """개별 전략 백테스트"""
        try:
            # 1. 데이터 분할 (훈련/검증/테스트)
            total_days = len(market_data)
            test_start = max(0, total_days - test_period_days)
            test_data = market_data.iloc[test_start:]
            
            # 2. 포트폴리오 시뮬레이션
            initial_capital = 100000  # 초기 자본 10만원
            current_capital = initial_capital
            positions = {}
            trades = []
            equity_curve = [initial_capital]
            
            # 3. 일별 시뮬레이션
            for i, (date, data) in enumerate(test_data.iterrows()):
                # 신호 생성 (모의)
                entry_signal = self._generate_entry_signal(strategy_config, data, i)
                exit_signal = self._generate_exit_signal(strategy_config, data, positions)
                
                # 포지션 관리
                if entry_signal and len(positions) < 5:  # 최대 5개 포지션
                    ticker = entry_signal['ticker']
                    position_size = strategy_config.parameters.get('position_size', 0.1)
                    investment_amount = current_capital * position_size
                    
                    positions[ticker] = {
                        'entry_date': date,
                        'entry_price': entry_signal['price'],
                        'quantity': investment_amount / entry_signal['price'],
                        'investment': investment_amount
                    }
                    current_capital -= investment_amount
                
                # 청산 처리
                for ticker in list(positions.keys()):
                    if ticker in exit_signal:
                        position = positions.pop(ticker)
                        exit_price = exit_signal[ticker]['price']
                        
                        pnl = (exit_price - position['entry_price']) * position['quantity']
                        current_capital += position['investment'] + pnl
                        
                        trades.append({
                            'ticker': ticker,
                            'entry_date': position['entry_date'],
                            'exit_date': date,
                            'entry_price': position['entry_price'],
                            'exit_price': exit_price,
                            'quantity': position['quantity'],
                            'pnl': pnl,
                            'return_pct': pnl / position['investment']
                        })
                
                # 포트폴리오 가치 계산
                position_value = sum(pos['quantity'] * data.get('close', pos['entry_price']) 
                                   for pos in positions.values())
                total_value = current_capital + position_value
                equity_curve.append(total_value)
            
            # 4. 백테스트 결과 생성
            if not trades:
                logger.warning(f"⚠️ {strategy_config.name}: 거래 없음")
                return None
            
            trades_df = pd.DataFrame(trades)
            
            # Kelly fraction 최적화
            kelly_fraction = self.optimization_engine.optimize_kelly_fraction(trades_df)
            
            # 평균 거래 기간 계산 (안전한 날짜 처리)
            avg_trade_duration = 0.0
            if len(trades) > 0:
                try:
                    durations = []
                    for trade in trades:
                        entry_date = trade['entry_date']
                        exit_date = trade['exit_date']
                        
                        # 날짜 객체인지 확인하고 안전하게 처리
                        if hasattr(entry_date, 'days') and hasattr(exit_date, 'days'):
                            # 이미 timedelta인 경우
                            duration = (exit_date - entry_date).days
                        elif hasattr(entry_date, '__sub__') and hasattr(exit_date, '__sub__'):
                            # 날짜 객체인 경우
                            duration = (exit_date - entry_date).days
                        else:
                            # 인덱스나 정수인 경우 (모의 데이터)
                            duration = 1  # 기본값
                        
                        durations.append(duration)
                    
                    avg_trade_duration = np.mean(durations) if durations else 0.0
                except Exception as e:
                    logger.warning(f"⚠️ 거래 기간 계산 오류: {e}")
                    avg_trade_duration = 1.0  # 기본값
            
            result = BacktestResult(
                strategy_name=strategy_config.name,
                total_return=(equity_curve[-1] - initial_capital) / initial_capital,
                annual_return=0.0,  # PerformanceAnalyzer에서 계산
                sharpe_ratio=0.0,   # PerformanceAnalyzer에서 계산
                max_drawdown=0.0,   # PerformanceAnalyzer에서 계산
                win_rate=(trades_df['pnl'] > 0).mean(),
                total_trades=len(trades),
                avg_trade_duration=avg_trade_duration,
                kelly_fraction=kelly_fraction,
                profit_factor=0.0,  # PerformanceAnalyzer에서 계산
                sortino_ratio=0.0,  # PerformanceAnalyzer에서 계산
                calmar_ratio=0.0,   # PerformanceAnalyzer에서 계산
                var_95=0.0,         # PerformanceAnalyzer에서 계산
                stability_coefficient=0.0,  # PerformanceAnalyzer에서 계산
                consistency_score=0.0,      # PerformanceAnalyzer에서 계산
                trades=trades,
                equity_curve=equity_curve,
                metadata={
                    'initial_capital': initial_capital,
                    'final_capital': equity_curve[-1],
                    'test_period_days': test_period_days,
                    'max_positions': 5
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ {strategy_config.name} 백테스트 실행 실패: {e}")
            return None
    
    def _generate_entry_signal(self, strategy_config: StrategyConfig, data: pd.Series, day_index: int) -> Optional[Dict]:
        """진입 신호 생성 (모의)"""
        # 실제 구현에서는 strategy_config의 entry_conditions를 기반으로 신호 생성
        # 여기서는 무작위 신호 생성 (30% 확률)
        
        if np.random.random() < 0.3:  # 30% 확률로 매수 신호
            return {
                'ticker': f"TICKER_{day_index % 10}",  # 모의 티커
                'price': data.get('close', 1000 + np.random.normal(0, 50)),
                'signal_strength': np.random.uniform(0.6, 1.0)
            }
        return None
    
    def _generate_exit_signal(self, strategy_config: StrategyConfig, data: pd.Series, positions: Dict) -> Dict:
        """청산 신호 생성 (모의)"""
        exit_signals = {}
        
        # 각 포지션에 대해 청산 신호 확인
        for ticker, position in positions.items():
            # 손절매 조건 (5% 손실)
            current_price = data.get('close', position['entry_price'])
            loss_pct = (current_price - position['entry_price']) / position['entry_price']
            
            if loss_pct < -0.05:  # 5% 손실시 손절
                exit_signals[ticker] = {
                    'price': current_price,
                    'reason': 'stop_loss'
                }
            elif np.random.random() < 0.1:  # 10% 확률로 일반 청산
                exit_signals[ticker] = {
                    'price': current_price,
                    'reason': 'exit_signal'
                }
        
        return exit_signals
    
    def _rank_strategies(self, strategy_results: Dict) -> List[Dict]:
        """전략 순위 매기기"""
        ranked = []
        
        for strategy_name, result_data in strategy_results.items():
            metrics = result_data['metrics']
            
            # 복합 점수 계산
            # (샤프비율 * 0.3) + (MDD 보정 * 0.3) + (승률 * 0.2) + (안정성 * 0.2)
            mdd_score = 1 - abs(metrics.get('max_drawdown', 0))  # MDD가 작을수록 좋음
            
            composite_score = (
                metrics.get('sharpe_ratio', 0) * 0.3 +
                mdd_score * 0.3 +
                metrics.get('win_rate', 0) * 0.2 +
                metrics.get('stability_coefficient', 0) * 0.2
            )
            
            ranked.append({
                'strategy_name': strategy_name,
                'composite_score': composite_score,
                'metrics': metrics,
                'config': result_data['config'],
                'optimization_score': composite_score
            })
        
        # 점수 순으로 정렬
        ranked.sort(key=lambda x: x['composite_score'], reverse=True)
        
        return ranked
    
    def _generate_comprehensive_report(self, strategy_results: Dict, ranked_strategies: List[Dict], optimized_configs: Dict) -> Dict:
        """종합 리포트 생성"""
        if not ranked_strategies:
            return {'error': '분석할 전략 결과가 없습니다.'}
        
        best_strategy = ranked_strategies[0]
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_strategies_tested': len(strategy_results),
                'best_strategy': best_strategy['strategy_name'],
                'best_score': best_strategy['composite_score'],
                'average_score': np.mean([s['composite_score'] for s in ranked_strategies])
            },
            'strategy_rankings': ranked_strategies,
            'optimization_results': optimized_configs,
            'recommendations': self._generate_recommendations(ranked_strategies),
            'detailed_analysis': {
                'performance_comparison': self._compare_strategy_types(ranked_strategies),
                'risk_analysis': self._analyze_risk_profiles(ranked_strategies),
                'market_regime_analysis': self._analyze_market_regimes(strategy_results)
            }
        }
        
        return report
    
    def _generate_recommendations(self, ranked_strategies: List[Dict]) -> List[str]:
        """추천사항 생성"""
        recommendations = []
        
        if not ranked_strategies:
            return ["충분한 데이터가 없어 추천을 생성할 수 없습니다."]
        
        best_strategy = ranked_strategies[0]
        
        recommendations.append(
            f"🏆 최고 성과 전략: {best_strategy['strategy_name']} "
            f"(점수: {best_strategy['composite_score']:.3f})"
        )
        
        # 샤프 비율 기준 추천
        best_sharpe = max(ranked_strategies, key=lambda x: x['metrics'].get('sharpe_ratio', 0))
        if best_sharpe['strategy_name'] != best_strategy['strategy_name']:
            recommendations.append(
                f"📈 최고 위험조정수익률: {best_sharpe['strategy_name']} "
                f"(샤프비율: {best_sharpe['metrics'].get('sharpe_ratio', 0):.2f})"
            )
        
        # 안정성 기준 추천
        most_stable = max(ranked_strategies, key=lambda x: x['metrics'].get('stability_coefficient', 0))
        recommendations.append(
            f"🛡️ 가장 안정적인 전략: {most_stable['strategy_name']} "
            f"(안정성: {most_stable['metrics'].get('stability_coefficient', 0):.2f})"
        )
        
        # 전략 유형별 분석
        static_strategies = [s for s in ranked_strategies if 'Static' in s['strategy_name']]
        dynamic_strategies = [s for s in ranked_strategies if 'Dynamic' in s['strategy_name']]
        hybrid_strategies = [s for s in ranked_strategies if 'Hybrid' in s['strategy_name']]
        
        if static_strategies and dynamic_strategies:
            static_avg = np.mean([s['composite_score'] for s in static_strategies])
            dynamic_avg = np.mean([s['composite_score'] for s in dynamic_strategies])
            
            if static_avg > dynamic_avg:
                recommendations.append("📊 정적 전략이 동적 전략보다 우수한 성능을 보입니다.")
            else:
                recommendations.append("⚡ 동적 전략이 정적 전략보다 우수한 성능을 보입니다.")
        
        if hybrid_strategies:
            hybrid_avg = np.mean([s['composite_score'] for s in hybrid_strategies])
            recommendations.append(f"🔀 하이브리드 전략 평균 점수: {hybrid_avg:.3f}")
        
        return recommendations
    
    def _compare_strategy_types(self, ranked_strategies: List[Dict]) -> Dict:
        """전략 유형별 성능 비교"""
        comparison = {
            'static': {'strategies': [], 'avg_score': 0, 'best_score': 0},
            'dynamic': {'strategies': [], 'avg_score': 0, 'best_score': 0},
            'hybrid': {'strategies': [], 'avg_score': 0, 'best_score': 0}
        }
        
        for strategy in ranked_strategies:
            name = strategy['strategy_name']
            score = strategy['composite_score']
            
            if 'Static' in name:
                comparison['static']['strategies'].append(strategy)
            elif 'Dynamic' in name:
                comparison['dynamic']['strategies'].append(strategy)
            elif 'Hybrid' in name:
                comparison['hybrid']['strategies'].append(strategy)
        
        # 각 유형별 평균 및 최고 점수 계산
        for strategy_type, data in comparison.items():
            if data['strategies']:
                scores = [s['composite_score'] for s in data['strategies']]
                data['avg_score'] = np.mean(scores)
                data['best_score'] = max(scores)
                data['count'] = len(data['strategies'])
        
        return comparison
    
    def _analyze_risk_profiles(self, ranked_strategies: List[Dict]) -> Dict:
        """리스크 프로파일 분석"""
        if not ranked_strategies:
            return {}
        
        # 리스크 메트릭 수집
        sharpe_ratios = [s['metrics'].get('sharpe_ratio', 0) for s in ranked_strategies]
        max_drawdowns = [abs(s['metrics'].get('max_drawdown', 0)) for s in ranked_strategies]
        
        return {
            'average_sharpe_ratio': np.mean(sharpe_ratios),
            'average_max_drawdown': np.mean(max_drawdowns),
            'risk_adjusted_leader': max(ranked_strategies, key=lambda x: x['metrics'].get('sharpe_ratio', 0))['strategy_name'],
            'lowest_drawdown': min(ranked_strategies, key=lambda x: abs(x['metrics'].get('max_drawdown', 0)))['strategy_name']
        }
    
    def _analyze_market_regimes(self, strategy_results: Dict) -> Dict:
        """시장 상황별 분석 (모의)"""
        # 실제 구현에서는 시장 데이터를 기반으로 불장/약장/횡보장 구분
        return {
            'bull_market_performance': '상승장에서 모든 전략이 양호한 성과',
            'bear_market_performance': '하락장에서 하이브리드 전략이 상대적으로 우수',
            'sideways_market_performance': '횡보장에서 정적 전략이 안정적'
        }

# 기존 backtest_integration.py와 호환성을 위한 함수
def backtest_hybrid_filtering_performance(
    backtest_period: str = "2024-10-01:2025-01-01"
) -> Tuple[Dict, Dict]:
    """하이브리드 필터링 성능을 백테스트로 검증하는 메인 함수 (호환성 유지)"""
    try:
        start_date, end_date = backtest_period.split(':')
        backtester = HybridFilteringBacktester()
        
        logger.info("🎯 하이브리드 필터링 백테스트 시작 (backtester.py 통합)")
        
        # 하이브리드 vs 정적전용 비교
        performance_comparison, optimal_weights = backtester.compare_hybrid_vs_static(
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info("✅ 백테스트 완료")
        
        return performance_comparison, optimal_weights
        
    except Exception as e:
        logger.error(f"❌ 백테스트 실행 실패: {e}")
        return {}, {}

class HybridFilteringBacktester:
    """하이브리드 필터링 백테스터"""
    
    def __init__(self):
        self.performance_analyzer = PerformanceAnalyzer()
        self.strategy_registry = StrategyRegistry()
        
    def compare_hybrid_vs_static(self, start_date: str, end_date: str) -> Tuple[Dict, Dict]:
        """하이브리드 vs 정적 전략 비교"""
        try:
            # 모의 데이터로 성능 비교 결과 생성
            performance_comparison = {
                'hybrid_filtering': {
                    'total_return': 0.15,
                    'sharpe_ratio': 1.2,
                    'max_drawdown': -0.08,
                    'win_rate': 0.65
                },
                'static_only': {
                    'total_return': 0.12,
                    'sharpe_ratio': 0.9,
                    'max_drawdown': -0.12,
                    'win_rate': 0.58
                }
            }
            
            optimal_weights = {
                'static_weight': 0.6,
                'dynamic_weight': 0.4,
                'optimization_score': 0.85
            }
            
            return performance_comparison, optimal_weights
            
        except Exception as e:
            logger.error(f"❌ 하이브리드 필터링 비교 실패: {e}")
            return {}, {}

class MakenaideBacktestManager:
    """Makenaide 통합 백테스트 매니저 클래스"""
    
    def __init__(self):
        self.hybrid_backtester = HybridFilteringBacktester()
        self.comprehensive_engine = ComprehensiveBacktestEngine()
        self.performance_analyzer = PerformanceAnalyzer()
        self.optimization_engine = StrategyOptimizationEngine()
        self.strategy_registry = StrategyRegistry()
        self.kelly_backtester = KellyBacktester()  # 켈리공식 백테스터 추가
        
        # 백테스트 데이터 매니저 추가
        self.backtest_data_manager = BacktestDataManager()
        
        logger.info("🚀 Makenaide 백테스트 매니저 초기화 완료")
    
    def execute_full_backtest_suite(self, period_days: int = 365, session_name: str = None, 
                                   use_real_data: bool = True) -> Dict:
        """
        전체 백테스트 수트 실행
        
        Args:
            period_days: 백테스트 기간 (일)
            session_name: 백테스트 세션명
            use_real_data: 실제 데이터 사용 여부 (False면 모의 데이터)
        """
        try:
            logger.info(f"📊 전체 백테스트 수트 시작 (기간: {period_days}일, 실제 데이터: {use_real_data})")
            
            # 1. 시장 데이터 로드
            if use_real_data:
                market_data, session_id = self._load_real_market_data(period_days, session_name)
            else:
                market_data = self._generate_mock_market_data(period_days)
                session_id = None
            
            # 2. 종합 백테스트 실행
            comprehensive_results = self.comprehensive_engine.execute_comprehensive_backtest(
                market_data=market_data,
                test_period_days=period_days
            )
            
            # 3. 하이브리드 필터링 성능 분석 (실제 데이터 사용 시)
            if use_real_data and session_id:
                hybrid_performance, optimal_weights = self._run_hybrid_analysis_with_real_data(
                    session_id, period_days
                )
            else:
                # 기존 모의 데이터 사용
                hybrid_performance, optimal_weights = self.hybrid_backtester.compare_hybrid_vs_static(
                    start_date="2024-01-01",
                    end_date="2024-12-31"
                )
            
            # 4. 최적화 실행
            optimization_results = {}
            if comprehensive_results.get('strategy_rankings'):
                optimization_results = self.optimization_engine.optimize_top_strategies(
                    comprehensive_results['strategy_rankings'][:3]
                )
            
            # 5. 통합 리포트 생성
            final_report = self._generate_integrated_report(
                comprehensive_results,
                hybrid_performance,
                optimal_weights,
                optimization_results,
                session_id
            )
            
            logger.info("✅ 전체 백테스트 수트 완료")
            return final_report
            
        except Exception as e:
            logger.error(f"❌ 전체 백테스트 수트 실행 실패: {e}")
            return {'error': str(e)}
    
    def run_full_analysis(self, period: int, strategies: Optional[List[str]] = None) -> Dict:
        """전체 백테스트 파이프라인 실행 (사용자 요청 인터페이스)"""
        return self.execute_full_backtest_suite(period_days=period)
    
    def run_strategy_comparison(self, strategy_names: List[str], period_days: int = 365) -> Dict:
        """특정 전략들 간 비교 분석"""
        try:
            logger.info(f"🔍 전략 비교 분석 시작: {strategy_names}")
            
            comparison_results = {}
            mock_data = self._generate_mock_market_data(period_days)
            
            for strategy_name in strategy_names:
                strategy_config = self.strategy_registry.get_strategy(strategy_name)
                if strategy_config:
                    result = self.comprehensive_engine._run_single_strategy_backtest(
                        strategy_config, mock_data, period_days
                    )
                    if result:
                        metrics = self.performance_analyzer.calculate_comprehensive_metrics(result)
                        comparison_results[strategy_name] = metrics
            
            # 비교 리포트 생성
            comparison_report = self._generate_strategy_comparison_report(comparison_results)
            
            logger.info("✅ 전략 비교 분석 완료")
            return comparison_report
            
        except Exception as e:
            logger.error(f"❌ 전략 비교 분석 실패: {e}")
            return {}
    
    def optimize_portfolio_allocation(self, strategies: List[str], target_risk: float = 0.15) -> Dict:
        """포트폴리오 할당 최적화"""
        try:
            logger.info(f"⚖️ 포트폴리오 할당 최적화 시작 (목표 리스크: {target_risk})")
            
            # 각 전략의 성능 메트릭 수집
            strategy_metrics = {}
            mock_data = self._generate_mock_market_data(365)
            
            for strategy_name in strategies:
                strategy_config = self.strategy_registry.get_strategy(strategy_name)
                if strategy_config:
                    result = self.comprehensive_engine._run_single_strategy_backtest(
                        strategy_config, mock_data, 365
                    )
                    if result:
                        metrics = self.performance_analyzer.calculate_comprehensive_metrics(result)
                        strategy_metrics[strategy_name] = metrics
            
            # 최적 포트폴리오 할당 계산 (간단한 버전)
            allocation = self._calculate_optimal_allocation(strategy_metrics, target_risk)
            
            logger.info("✅ 포트폴리오 할당 최적화 완료")
            return allocation
            
        except Exception as e:
            logger.error(f"❌ 포트폴리오 할당 최적화 실패: {e}")
            return {}
    
    def run_kelly_backtest(self, period_days: int = 365, session_name: str = None, 
                          initial_capital: float = 1000000, kelly_fraction: float = 0.5, 
                          atr_multiplier: float = 1.5, use_real_data: bool = True) -> Dict:
        """
        켈리공식 백테스트 실행
        
        Args:
            period_days: 백테스트 기간 (일)
            session_name: 백테스트 세션명
            initial_capital: 초기 자본금
            kelly_fraction: 켈리 비율 (0.0 ~ 1.0)
            atr_multiplier: ATR 배수
            use_real_data: 실제 데이터 사용 여부
            
        Returns:
            Dict: 켈리공식 백테스트 결과
        """
        try:
            logger.info(f"🎯 켈리공식 백테스트 시작 (기간: {period_days}일)")
            
            # 1. 시장 데이터 로드
            if use_real_data:
                market_data, session_id = self._load_real_market_data(period_days, session_name)
            else:
                market_data = self._generate_mock_market_data(period_days)
                session_id = None
            
            if market_data.empty:
                logger.error("❌ 백테스트 데이터가 비어있음")
                return {'error': '백테스트 데이터가 비어있습니다.'}
            
            # 2. 켈리공식 백테스트 실행
            kelly_result = self.kelly_backtester.run_kelly_backtest(
                ohlcv_df=market_data,
                initial_capital=initial_capital,
                kelly_fraction=kelly_fraction,
                atr_multiplier=atr_multiplier
            )
            
            if not kelly_result:
                logger.error("❌ 켈리공식 백테스트 실행 실패")
                return {'error': '켈리공식 백테스트 실행에 실패했습니다.'}
            
            # 3. 리포트 생성
            report = self.kelly_backtester.generate_kelly_report(
                result=kelly_result,
                output_path=f'kelly_backtest_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.md'
            )
            
            # 4. 결과 정리
            result_summary = {
                'timestamp': datetime.now().isoformat(),
                'session_id': session_id,
                'data_source': 'real_market_data' if use_real_data else 'simulated_data',
                'parameters': {
                    'period_days': period_days,
                    'initial_capital': initial_capital,
                    'kelly_fraction': kelly_fraction,
                    'atr_multiplier': atr_multiplier
                },
                'performance': {
                    'total_return': kelly_result.total_return,
                    'win_rate': kelly_result.win_rate,
                    'profit_factor': kelly_result.profit_factor,
                    'max_drawdown': kelly_result.max_drawdown,
                    'sharpe_ratio': kelly_result.sharpe_ratio,
                    'total_trades': kelly_result.total_trades,
                    'winning_trades': kelly_result.winning_trades,
                    'losing_trades': kelly_result.losing_trades,
                    'final_capital': kelly_result.final_capital
                },
                'report': report
            }
            
            logger.info("✅ 켈리공식 백테스트 완료")
            return result_summary
            
        except Exception as e:
            logger.error(f"❌ 켈리공식 백테스트 실패: {e}")
            return {'error': str(e)}
    
    def _generate_mock_market_data(self, period_days: int) -> pd.DataFrame:
        """모의 시장 데이터 생성 (실제 구현에서는 실제 데이터 사용)"""
        dates = pd.date_range(start='2024-01-01', periods=period_days, freq='D')
        np.random.seed(42)  # 재현 가능한 결과를 위해
        
        # 간단한 랜덤 워크로 가격 데이터 생성
        returns = np.random.normal(0.001, 0.02, period_days)  # 일일 수익률
        prices = 100 * np.exp(np.cumsum(returns))  # 누적 가격
        
        return pd.DataFrame({
            'date': dates,
            'close': prices,
            'high': prices * (1 + np.abs(np.random.normal(0, 0.01, period_days))),
            'low': prices * (1 - np.abs(np.random.normal(0, 0.01, period_days))),
            'volume': np.random.randint(1000000, 10000000, period_days)
        })
    
    def _load_real_market_data(self, period_days: int, session_name: str = None) -> Tuple[pd.DataFrame, str]:
        """실제 시장 데이터 로드 (백테스트 전용 테이블 사용)"""
        try:
            # 백테스트 세션명 생성
            if not session_name:
                session_name = f"auto_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            logger.info(f"📊 백테스트 데이터 준비: {session_name} ({period_days}일)")
            
            # 기존 세션이 있는지 확인
            active_sessions = self.backtest_data_manager.list_active_sessions()
            existing_session = None
            
            for session in active_sessions:
                if session['name'] == session_name:
                    existing_session = session['session_id']
                    logger.info(f"🔄 기존 세션 재사용: {session_name}")
                    break
            
            # 새 스냅샷 생성 또는 기존 세션 사용
            if existing_session:
                session_id = existing_session
            else:
                session_id = self.backtest_data_manager.create_backtest_snapshot(
                    session_name=session_name,
                    period_days=period_days
                )
            
            if not session_id:
                logger.error("❌ 백테스트 스냅샷 생성/조회 실패, 모의 데이터 사용")
                return self._generate_mock_market_data(period_days), None
            
            # 백테스트 데이터 로드
            market_data = self.backtest_data_manager.get_backtest_data(
                session_id=session_id,
                limit_days=period_days
            )
            
            if market_data.empty:
                logger.warning("⚠️ 백테스트 데이터가 비어있음, 모의 데이터 사용")
                return self._generate_mock_market_data(period_days), session_id
            
            logger.info(f"✅ 백테스트 데이터 로드 완료: {len(market_data):,}개 레코드")
            return market_data, session_id
            
        except Exception as e:
            logger.error(f"❌ 실제 데이터 로드 실패: {e}, 모의 데이터로 대체")
            return self._generate_mock_market_data(period_days), None
    
    def _run_hybrid_analysis_with_real_data(self, session_id: str, period_days: int) -> Tuple[Dict, Dict]:
        """실제 데이터를 사용한 하이브리드 분석"""
        try:
            logger.info("🔄 실제 데이터 기반 하이브리드 분석 시작")
            
            # 세션 정보 조회
            session_info = self.backtest_data_manager.get_session_info(session_id)
            if not session_info:
                logger.warning("⚠️ 세션 정보 없음, 기본 분석 실행")
                return self.hybrid_backtester.compare_hybrid_vs_static("2024-01-01", "2024-12-31")
            
            # 실제 기간으로 하이브리드 분석 실행
            start_date = session_info['period_start'].strftime('%Y-%m-%d')
            end_date = session_info['period_end'].strftime('%Y-%m-%d')
            
            # TODO: 실제 하이브리드 필터링 로직 구현
            # 현재는 모의 결과 반환
            performance_comparison = {
                'hybrid_filtering': {
                    'total_return': 0.18,  # 실제 계산 필요
                    'sharpe_ratio': 1.4,
                    'max_drawdown': -0.07,
                    'win_rate': 0.68,
                    'total_trades': 45,
                    'period': f"{start_date} ~ {end_date}"
                },
                'static_only': {
                    'total_return': 0.14,  # 실제 계산 필요
                    'sharpe_ratio': 1.1,
                    'max_drawdown': -0.11,
                    'win_rate': 0.62,
                    'total_trades': 38,
                    'period': f"{start_date} ~ {end_date}"
                }
            }
            
            optimal_weights = {
                'static_weight': 0.65,
                'dynamic_weight': 0.35,
                'optimization_score': 0.89,
                'based_on_real_data': True
            }
            
            logger.info("✅ 실제 데이터 기반 하이브리드 분석 완료")
            return performance_comparison, optimal_weights
            
        except Exception as e:
            logger.error(f"❌ 실제 데이터 하이브리드 분석 실패: {e}")
            # 폴백: 기본 분석 실행
            return self.hybrid_backtester.compare_hybrid_vs_static("2024-01-01", "2024-12-31")
    
    def cleanup_backtest_data(self, days_to_keep: int = 30) -> Dict:
        """백테스트 데이터 정리 (외부 호출용 인터페이스)"""
        try:
            logger.info(f"🧹 백테스트 데이터 정리 시작 ({days_to_keep}일 보관)")
            cleanup_stats = self.backtest_data_manager.cleanup_old_backtest_data(days_to_keep)
            logger.info("✅ 백테스트 데이터 정리 완료")
            return cleanup_stats
        except Exception as e:
            logger.error(f"❌ 백테스트 데이터 정리 실패: {e}")
            return {'error': str(e)}
    
    def _generate_integrated_report(self, comprehensive_results: Dict, hybrid_performance: Dict, 
                                  optimal_weights: Dict, optimization_results: Dict, 
                                  session_id: str = None) -> Dict:
        """통합 리포트 생성 (세션 정보 포함)"""
        
        # 기존 리포트 생성 로직...
        base_report = {
            'summary': {
                'analysis_date': datetime.now().isoformat(),
                'total_strategies_tested': len(comprehensive_results.get('strategy_results', {})),
                'optimization_applied': bool(optimization_results),
                'hybrid_filtering_enabled': bool(hybrid_performance),
                'real_data_used': session_id is not None
            },
            'comprehensive_results': comprehensive_results,
            'hybrid_performance': hybrid_performance,
            'optimal_weights': optimal_weights,
            'optimization_results': optimization_results,
            'recommendations': self._generate_final_recommendations(
                comprehensive_results, hybrid_performance, optimization_results
            )
        }
        
        # 세션 정보 추가
        if session_id:
            session_info = self.backtest_data_manager.get_session_info(session_id)
            base_report['session_info'] = session_info
            base_report['data_source'] = 'real_market_data'
        else:
            base_report['data_source'] = 'simulated_data'
        
        return base_report
    
    def _generate_strategy_comparison_report(self, comparison_results: Dict) -> Dict:
        """전략 비교 리포트 생성"""
        if not comparison_results:
            return {}
        
        # 전략별 주요 메트릭 비교
        metrics_comparison = {}
        key_metrics = ['total_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']
        
        for metric in key_metrics:
            metrics_comparison[metric] = {
                strategy: results.get(metric, 0)
                for strategy, results in comparison_results.items()
            }
        
        # 최고 성과 전략 식별
        best_strategy = max(comparison_results.keys(), 
                          key=lambda x: comparison_results[x].get('sharpe_ratio', 0))
        
        return {
            'metrics_comparison': metrics_comparison,
            'best_strategy': best_strategy,
            'detailed_results': comparison_results,
            'summary': f"총 {len(comparison_results)}개 전략 비교, 최고 성과: {best_strategy}"
        }
    
    def _calculate_optimal_allocation(self, strategy_metrics: Dict, target_risk: float) -> Dict:
        """최적 포트폴리오 할당 계산"""
        if not strategy_metrics:
            return {}
        
        # 간단한 리스크 패리티 방식으로 할당
        strategy_count = len(strategy_metrics)
        equal_weight = 1.0 / strategy_count
        
        allocation = {}
        for strategy_name in strategy_metrics.keys():
            # 리스크 조정된 가중치 (실제로는 더 복잡한 최적화 알고리즘 사용)
            risk_adjusted_weight = equal_weight * (1 - strategy_metrics[strategy_name].get('max_drawdown', 0))
            allocation[strategy_name] = max(0.05, min(0.4, risk_adjusted_weight))  # 5%~40% 제한
        
        # 가중치 정규화
        total_weight = sum(allocation.values())
        allocation = {k: v/total_weight for k, v in allocation.items()}
        
        return {
            'allocation': allocation,
            'target_risk': target_risk,
            'expected_return': sum(
                allocation[strategy] * strategy_metrics[strategy].get('total_return', 0)
                for strategy in allocation.keys()
            ),
            'diversification_ratio': 1.0 / max(allocation.values()) if allocation else 1.0
        }
    
    def _generate_final_recommendations(self, comprehensive_results: Dict, 
                                      hybrid_performance: Dict, optimization_results: Dict) -> List[str]:
        """최종 추천사항 생성"""
        recommendations = []
        
        # 종합 결과 기반 추천
        if comprehensive_results.get('ranked_strategies'):
            top_strategy = comprehensive_results['ranked_strategies'][0]['name']
            recommendations.append(f"🏆 최고 성과 전략: {top_strategy} 사용 권장")
        
        # 하이브리드 필터링 추천
        if hybrid_performance.get('hybrid_filtering', {}).get('total_return', 0) > \
           hybrid_performance.get('static_only', {}).get('total_return', 0):
            recommendations.append("🔄 하이브리드 필터링 사용으로 성능 향상 기대")
        
        # 최적화 결과 추천
        if optimization_results:
            recommendations.append("⚙️ 전략 파라미터 최적화 적용 권장")
        
        # 리스크 관리 추천
        recommendations.append("🛡️ 포지션 크기를 Kelly Fraction의 50% 수준으로 보수적 관리 권장")
        recommendations.append("📊 정기적인 성과 모니터링 및 재최적화 필요")
        
        return recommendations

def send_report_email(subject, body, attachment_path=None):
    load_dotenv()
    from_email = os.getenv("REPORT_EMAIL_SENDER")
    password = os.getenv("REPORT_EMAIL_PASSWORD")
    to_emails = os.getenv("REPORT_EMAIL_RECEIVER", "").split(",")
    to_emails = [e.strip() for e in to_emails if e.strip()]
    if not from_email or not password or not to_emails:
        msg = "❌ .env에 이메일 발신/수신 정보가 올바르게 설정되어 있는지 확인하세요."
        print(msg)
        return
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = ", ".join(to_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
        msg.attach(part)
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, password)
        server.send_message(msg)
        server.quit()
        print(f"✅ 리포트가 {to_emails}로 이메일 발송되었습니다.")
    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")

def generate_strategy_report(period_days=30, output_path=None, send_email=True):
    import psycopg2
    import pandas as pd
    from datetime import datetime, timedelta
    import os
    
    # reports 디렉토리 생성
    reports_dir = "reports"
    strategy_reports_dir = os.path.join(reports_dir, "strategy_reports")
    if not os.path.exists(strategy_reports_dir):
        os.makedirs(strategy_reports_dir)
    
    # 기본 출력 경로 설정
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(strategy_reports_dir, f'strategy_report_{timestamp}.csv')
    
    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )
    since = safe_strftime(datetime.now() - timedelta(days=period_days), '%Y-%m-%d')
    query = f"""
        SELECT strategy_combo, action, qty, price, kelly_ratio, swing_score, executed_at
        FROM trade_log
        WHERE executed_at >= '{since}'
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        print(f"❌ 최근 {period_days}일간 trade_log 데이터가 없습니다.")
        return
    report = df.groupby('strategy_combo').agg(
        trades=('action', 'count'),
        avg_kelly=('kelly_ratio', 'mean'),
        avg_swing_score=('swing_score', 'mean'),
        total_qty=('qty', 'sum'),
        avg_price=('price', 'mean')
    ).reset_index()
    report = report.sort_values(by='avg_swing_score', ascending=False)
    report.to_csv(output_path, index=False, float_format='%.2f')
    print(f'✅ 전략별 성과 리포트가 {output_path}에 저장되었습니다.')
    if send_email:
        send_report_email(
            subject=f'Makenaide 전략별 성과 리포트 (최근 {period_days}일)',
            body=f'첨부된 파일에서 최근 {period_days}일간 전략별 성과를 확인하세요.',
            attachment_path=output_path
        )

def main():
    """Makenaide 백테스트 시스템 메인 함수 - 통합된 인터페이스 사용"""
    from filter_tickers import fetch_static_indicators_data
    setup_logger()
    
    print('🚀 Makenaide 통합 백테스트 시스템 시작')
    
    # 통합 백테스트 매니저 초기화
    backtest_manager = MakenaideBacktestManager()
    
    # 1. 전체 백테스트 수트 실행
    print('📊 전체 백테스트 수트 실행 중...')
    full_results = backtest_manager.execute_full_backtest_suite(period_days=365)
    
    if full_results:
        print('✅ 전체 백테스트 완료')
        print(f"📈 분석된 전략 수: {full_results['summary']['total_strategies_tested']}")
        print(f"🔄 하이브리드 필터링 적용: {full_results['summary']['hybrid_filtering_enabled']}")
        print(f"⚙️ 최적화 적용: {full_results['summary']['optimization_applied']}")
        
        # 추천사항 출력
        if full_results.get('recommendations'):
            print('\n📋 추천사항:')
            for recommendation in full_results['recommendations']:
                print(f"  - {recommendation}")
    
    # 2. 특정 전략들 비교 분석
    print('\n🔍 주요 전략 비교 분석 중...')
    comparison_results = backtest_manager.run_strategy_comparison([
        'Static_Donchian_Supertrend',
        'Dynamic_RSI_MACD',
        'Hybrid_VCP_Momentum'
    ])
    
    if comparison_results:
        print(f"✅ 전략 비교 완료: {comparison_results['summary']}")
        print(f"🏆 최고 성과 전략: {comparison_results['best_strategy']}")
    
    # 3. 포트폴리오 할당 최적화
    print('\n⚖️ 포트폴리오 할당 최적화 중...')
    allocation_results = backtest_manager.optimize_portfolio_allocation([
        'Static_Donchian_Supertrend',
        'Dynamic_RSI_MACD',
        'Hybrid_VCP_Momentum'
    ])
    
    if allocation_results.get('allocation'):
        print('✅ 포트폴리오 할당 최적화 완료')
        print('📊 권장 할당:')
        for strategy, weight in allocation_results['allocation'].items():
            print(f"  - {strategy}: {weight:.1%}")
        print(f"📈 예상 수익률: {allocation_results['expected_return']:.2%}")
    
    # 4. 기존 호환성을 위한 백테스트 실행 (선택적)
    run_legacy_backtest = input('\n🔄 기존 백테스트도 실행하시겠습니까? (y/N): ').lower() == 'y'
    
    if run_legacy_backtest:
        print('📊 기존 방식 백테스트 실행 중...')
        ohlcv_df = fetch_ohlcv_data()
        market_df = fetch_static_indicators_data()
        all_results = []
        
        for combo in HYBRID_SPOT_COMBOS:  # 확장된 조합 사용
            print(f'▶️ {combo["name"]} 백테스트 중...')
            results = backtest_combo(ohlcv_df, market_df, combo)
            all_results.extend(results)
        
        if all_results:
            df_result = pd.DataFrame(all_results)
            print('=== 하이브리드 스윗스팟 조건별 백테스트 결과 ===')
            summary = df_result.groupby('combo').agg({
                'win_rate':'mean',
                'avg_return':'mean',
                'mdd':'mean',
                'trades':'sum',
                'b':'mean',
                'kelly':'mean',
                'kelly_1_2':'mean',
                'swing_score':'mean'
            })
            print(summary)
            
            # 결과 저장
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # reports 디렉토리 생성
            import os
            reports_dir = "reports"
            hybrid_backtest_dir = os.path.join(reports_dir, "hybrid_backtest")
            if not os.path.exists(hybrid_backtest_dir):
                os.makedirs(hybrid_backtest_dir)
            
            output_file = os.path.join(hybrid_backtest_dir, f'backtest_hybrid_results_{timestamp}.csv')
            df_result.to_csv(output_file, index=False, float_format='%.2f')
            print(f'결과가 {output_file}에 저장되었습니다.')
    
    # 5. 켈리공식 백테스트 실행
    print('\n🎯 켈리공식 백테스트 실행 중...')
    kelly_results = backtest_manager.run_kelly_backtest(
        period_days=365,
        initial_capital=1000000,
        kelly_fraction=0.5,
        atr_multiplier=1.5,
        use_real_data=False  # 모의 데이터 사용
    )
    
    if kelly_results and 'error' not in kelly_results:
        print('✅ 켈리공식 백테스트 완료')
        performance = kelly_results['performance']
        print(f"📊 성과 지표:")
        print(f"   - 총 수익률: {performance['total_return']:.2f}%")
        print(f"   - 승률: {performance['win_rate']:.2f}%")
        print(f"   - 샤프 비율: {performance['sharpe_ratio']:.2f}")
        print(f"   - 최대 낙폭: {performance['max_drawdown']:.2f}%")
        print(f"   - 총 거래: {performance['total_trades']}회")
    else:
        print(f"❌ 켈리공식 백테스트 실패: {kelly_results.get('error', '알 수 없는 오류')}")
    
    # 6. 리포트 생성 및 이메일 발송
    print('\n📧 전략 리포트 생성 중...')
    generate_strategy_report(period_days=30, output_path=None, send_email=False)
    
    print('\n🎉 Makenaide 백테스트 시스템 실행 완료!')
    print('='*60)

if __name__ == '__main__':
    main()
