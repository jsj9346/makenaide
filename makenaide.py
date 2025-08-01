import time
import logging
import os
import sys
import pandas as pd
from datetime import datetime, date
from typing import Any, Dict
import builtins  # builtins 모듈 추가
import gc  # 가비지 컬렉션
import psutil  # 메모리 모니터링
from scanner import update_tickers
from utils import (
    get_db_connection, load_env, setup_logger, get_current_price_safe,
    MIN_KRW_ORDER, MIN_KRW_SELL_ORDER, TAKER_FEE_RATE,
    retry_on_error, handle_api_error, handle_db_error, handle_network_error,
    logger, load_blacklist, safe_strftime, safe_float_convert
)
import psycopg2
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyupbit
import pandas as pd
from data_fetcher import get_ohlcv_d
import json

# ✅ db_manager.py의 함수 사용
from db_manager import get_db_connection_context



# 중요 상수 정의
ONE_HMIL_KRW = 100_000_000  # 1억원 (거래대금 필터링 기준)

# 로거 초기화
logger = setup_logger()

# print 함수 오버라이드
_original_print = builtins.print
def print(*args, **kwargs):
    """
    print 함수를 오버라이드하여 모든 출력을 로그 파일에도 기록합니다.
    """
    timestamp = safe_strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    file_name = os.path.basename(__file__)
    message = ' '.join(str(a) for a in args)
    
    # 로그 파일에 기록
    logger.info(f"[{timestamp}][{file_name}] {message}")
    
    # 원래의 print 함수 호출
    _original_print(f"[{timestamp}][{file_name}] {message}", **kwargs)

# 로깅 시작 메시지
logger.info("="*50)
logger.info("Makenaide 봇 시작")
logger.info("="*50)

# 현재 파일의 절대 경로를 기준으로 import 경로 설정
current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)

# Python 경로 설정 (현재 디렉토리만 추가)
sys.path = [current_dir] + [p for p in sys.path if p != current_dir]

from portfolio_manager import PortfolioManager
from trend_analyzer import analyze_trend_with_gpt, save_trend_analysis_to_db, save_trend_analysis_log, should_reuse_gpt_response, unified_gpt_analysis_engine
from data_fetcher import (
    process_single_ticker as process_ticker_data,
    calculate_technical_indicators,
    generate_chart_image,
    get_ohlcv_d,
    get_ohlcv_4h,
    calculate_technical_indicators_4h,
    save_market_data_4h_to_db,
    enhanced_ohlcv_processor,
    generate_gpt_analysis_json
)
import trade_executor
from trade_executor import TrailingStopManager, check_and_execute_trailing_stop, sell_asset
import pyupbit
import psycopg2
from dotenv import load_dotenv
from auth import generate_jwt_token
import re
import requests  # NEW: for direct REST calls to Upbit

# --- Minimal JWT-based Upbit REST client --- #
class UpbitClient:
    """
    Minimal Upbit REST client that authenticates with a pre‑built JWT token.
    Only the methods actually used elsewhere in this file are implemented.
    """
    BASE_URL = "https://api.upbit.com"

    def __init__(self, jwt_token: str):
        self.jwt_token = jwt_token
        self.headers = {"Authorization": f"Bearer {jwt_token}"}
   

from utils import validate_and_correct_phase
from db_manager import DBManager
from config_loader import load_config
# === 스윗스팟/백테스트 모듈 import ===
from backtester import backtest_combo, SPOT_COMBOS, generate_strategy_report
# from parallel_processor import process_tickers_parallel, process_data_parallel
from filter_tickers import filter_breakout_candidates, filter_by_monthly_data_length, apply_timing_filter_4h

# 디버깅을 위한 import 경로 출력
logger.info(f"[DEBUG] Current working directory: {os.getcwd()}")
logger.info(f"[DEBUG] Current file path: {current_file}")
logger.info(f"[DEBUG] Current directory: {current_dir}")


class MakenaideBot:
    """
    메인 자동매매 로직을 담당하는 클래스. 상태(포트폴리오, DB, 설정 등)와 주요 기능을 멤버로 관리한다.
    """
    def __init__(self):
        """
        MakenaideBot 초기화: 환경변수, DB 연결, API 연결, 설정 로드
        """
        start_time = time.time()
        logger.info("🔧 MakenaideBot 초기화 시작")
        
        # 환경 변수 로드
        load_env()
        
        # DB 매니저 초기화
        self.db_mgr = DBManager()
        
        # 기존 설정 로드
        self.config = load_config("config/strategy.yaml")
        
        # 새로운 트레이딩 설정 로드
        try:
            from config_loader import get_trading_config  # 통합된 버전 사용
            self.trading_config = get_trading_config()
            logger.info("✅ 통합 트레이딩 설정 로드 완료")
        except Exception as e:
            logger.warning(f"⚠️ 통합 트레이딩 설정 로드 실패: {e}")
            self.trading_config = None
        
        # === GPT 분석 방식 설정 ===
        self.use_json_instead_of_chart = True  # True: JSON 방식, False: 차트 이미지 방식
        
        # === DB 저장 기능 설정 ===
        self.save_to_db = True  # True: DB에 저장, False: 저장 건너뜀
        
        # --- Upbit/OpenAI 인증 처리 --- #
        self.access_key = os.getenv("UPBIT_ACCESS_KEY")
        self.secret_key = os.getenv("UPBIT_SECRET_KEY")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")

        # API 키가 없으면 즉시 오류
        if not self.access_key or not self.secret_key:
            logger.error("❌ Upbit API 키가 설정되지 않았습니다 (.env 확인).")
            raise ValueError("Upbit API 키가 필요합니다. .env 파일을 확인하세요.")

        self.upbit = None

        try:
            import pyupbit
            self.upbit = pyupbit.Upbit(self.access_key, self.secret_key)
            balance = self.upbit.get_balance("KRW")
            logger.info(f"💰 Upbit(pyupbit) 인증 성공 (KRW 잔액: {balance:,.0f}원)") #[TODO]다음에는 KRW 잔액을 포함한 현재 포트폴오를 print 하도록
        except Exception as e:
            logger.warning(f"⚠️ pyupbit 인증 실패: {e}")
        
        
        # 포트폴리오 매니저 초기화
        self.pm = PortfolioManager(
            self.upbit,
            risk_pct=self.config['risk']['pct'],
            atr_period=self.config['atr']['period'],
            pyramiding_config=self.config['pyramiding']
        )
        
        # 트레일링 스탑 설정
        self.ts_cfg = self.config['trailing_stop']
        self.trailing_manager = TrailingStopManager(atr_multiplier=self.ts_cfg['atr_multiplier'])
        
        # DB 매니저에 배치 업데이트 메서드 추가 (fallback)
        if not hasattr(self.db_mgr, 'batch_update_trailing_stops'):
            self.db_mgr.batch_update_trailing_stops = self._batch_update_trailing_stops_fallback
        
        # 모듈 속성 초기화 (파이프라인 실행을 위한 모듈 참조)
        self._initialize_modules()
        
        # GPT 분석 결과 라이프사이클 관리자 초기화
        self._initialize_gpt_lifecycle_manager()
        
        # 초기화 완료 상태
        self.initialized = False
        
        logger.info(f"✅ MakenaideBot 초기화 완료 (소요시간: {time.time() - start_time:.2f}초)")
    
    @contextmanager
    def get_db_connection_safe(self):
        """표준화된 안전한 DB 연결 컨텍스트 매니저
        모든 DB 작업에서 사용하도록 표준화
        """
        from utils import get_db_connection
        conn = None
        try:
            conn = get_db_connection()
            if conn is None:
                raise ConnectionError("DB 연결 실패")
            yield conn
        except Exception as e:
            logger.error(f"❌ 안전한 DB 연결 중 오류: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn:
                try:
                    conn.close()
                    logger.debug("안전한 DB 연결 종료")
                except Exception as e:
                    logger.warning(f"⚠️ DB 연결 종료 중 오류: {e}")

    def _initialize_modules(self):
        """모듈 초기화 및 속성 할당"""
        try:
            import scanner
            import data_fetcher
            import filter_tickers
            import trend_analyzer
            
            # 모듈을 속성으로 할당
            self.scanner = scanner
            self.data_fetcher = data_fetcher
            self.filter_tickers = filter_tickers
            self.trend_analyzer = trend_analyzer
            
            logger.info("✅ 모듈 초기화 완료")
            
        except ImportError as e:
            logger.error(f"❌ 모듈 임포트 실패: {e}")
            raise
    
    def _initialize_gpt_lifecycle_manager(self):
        """GPT 분석 결과 라이프사이클 관리자 초기화"""
        try:
            from trend_analyzer import GPTAnalysisLifecycleManager
            
            # 라이프사이클 관리자 초기화
            self.gpt_lifecycle_manager = GPTAnalysisLifecycleManager(
                db_manager=self.db_mgr,
                config=None  # 기본 설정 사용
            )
            
            logger.info("✅ GPT 분석 결과 라이프사이클 관리자 초기화 완료")
            
        except Exception as e:
            logger.warning(f"⚠️ GPT 라이프사이클 관리자 초기화 실패: {e}")
            self.gpt_lifecycle_manager = None
    
    def _check_gpt_analysis_cache(self, ticker: str) -> tuple[bool, dict]:
        """
        GPT 분석 결과 캐시 확인
        
        Args:
            ticker: 확인할 티커
            
        Returns:
            tuple: (건너뛸지 여부, 기존 분석 데이터)
        """
        try:
            from trend_analyzer import should_skip_gpt_analysis
            
            # 캐싱 설정
            cache_config = {
                'max_age_minutes': 720,  # 12시간
                'enable_caching': True,
                'skip_if_fresh': True
            }
            
            # 캐시 확인
            should_skip, existing_analysis = should_skip_gpt_analysis(
                ticker, self.db_mgr, cache_config
            )
            
            return should_skip, existing_analysis
            
        except Exception as e:
            logger.warning(f"⚠️ {ticker} 캐시 확인 실패: {e}")
            return False, None

    def validate_static_indicators_data(self):
        """static_indicators 테이블의 데이터 무결성을 검증 (에러 처리 강화)"""
        try:
            with self.db_mgr.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # 컬럼 존재 여부 먼저 확인
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'static_indicators'
                    ORDER BY ordinal_position
                """)
                
                existing_columns = {row[0]: row[1] for row in cursor.fetchall()}
                
                if not existing_columns:
                    logger.error("❌ static_indicators 테이블이 존재하지 않거나 컬럼이 없습니다.")
                    return False
                
                logger.info(f"📊 static_indicators 테이블 컬럼 {len(existing_columns)}개 확인됨")
                
                # 문제 컬럼들을 데이터 타입별로 분류
                problem_columns = ['nvt_relative', 'volume_change_7_30', 'adx', 'supertrend_signal']
                
                # 실제 존재하는 컬럼만 필터링
                existing_problem_columns = [col for col in problem_columns if col in existing_columns]
                missing_columns = [col for col in problem_columns if col not in existing_columns]
                
                if missing_columns:
                    logger.warning(f"⚠️ 누락된 컬럼들: {missing_columns}")
                
                # 데이터 타입별 분류
                numeric_columns = []
                text_columns = []
                
                for col in existing_problem_columns:
                    data_type = existing_columns[col]
                    if 'text' in data_type.lower() or 'varchar' in data_type.lower() or 'char' in data_type.lower():
                        text_columns.append(col)
                    else:
                        numeric_columns.append(col)
                
                logger.info(f"📊 검증 대상 - 숫자 컬럼: {numeric_columns}, 텍스트 컬럼: {text_columns}")
                
                validation_results = {'success': 0, 'failed': 0, 'warnings': 0}
                
                # 숫자 컬럼 검증
                for column in numeric_columns:
                    try:
                        cursor.execute(f"""
                            SELECT COUNT(DISTINCT {column}) as unique_count,
                                   COUNT(*) as total_count,
                                   AVG({column}) as avg_value,
                                   MIN({column}) as min_value,
                                   MAX({column}) as max_value
                            FROM static_indicators 
                            WHERE {column} IS NOT NULL
                        """)
                        
                        result = cursor.fetchone()
                        if result:
                            unique_count, total_count, avg_val, min_val, max_val = result
                            
                            if unique_count <= 1 and total_count > 10:
                                logger.warning(f"⚠️ {column} 컬럼 데이터 이상: 모든 값이 동일함 (값: {avg_val})")
                                validation_results['warnings'] += 1
                                
                                # 데이터 재계산 시도
                                self._attempt_column_recalculation(column)
                            else:
                                logger.info(f"✅ {column} 컬럼 정상: 고유값 {unique_count}개, 범위 {min_val}~{max_val}")
                                validation_results['success'] += 1
                                
                    except Exception as e:
                        logger.error(f"❌ {column} 컬럼 검증 실패: {e}")
                        validation_results['failed'] += 1
                        
                        # 컬럼 데이터 타입 확인 및 수정 시도
                        self._attempt_column_type_fix(column, e)
                
                # 텍스트 컬럼 검증 (supertrend_signal)
                for column in text_columns:
                    try:
                        cursor.execute(f"""
                            SELECT COUNT(DISTINCT {column}) as unique_count,
                                   COUNT(*) as total_count,
                                   MIN({column}) as min_value,
                                   MAX({column}) as max_value
                            FROM static_indicators 
                            WHERE {column} IS NOT NULL
                        """)
                        
                        result = cursor.fetchone()
                        if result:
                            unique_count, total_count, min_val, max_val = result
                            
                            if unique_count <= 1 and total_count > 10:
                                logger.warning(f"⚠️ {column} 컬럼 데이터 이상: 모든 값이 동일함 (값: {min_val})")
                                validation_results['warnings'] += 1
                                
                                # 텍스트 컬럼 재계산 시도
                                self._attempt_text_column_recalculation(column)
                            else:
                                logger.info(f"✅ {column} 컬럼 정상: 고유값 {unique_count}개, 범위 {min_val}~{max_val}")
                                validation_results['success'] += 1
                                
                    except Exception as e:
                        logger.error(f"❌ {column} 컬럼 검증 실패: {e}")
                        validation_results['failed'] += 1
                        
                        # 텍스트 컬럼 타입 확인
                        self._attempt_text_column_fix(column, e)
                
                # 검증 결과 요약
                total_checked = validation_results['success'] + validation_results['failed'] + validation_results['warnings']
                logger.info(f"📊 static_indicators 검증 완료: 성공 {validation_results['success']}/{total_checked}, "
                          f"경고 {validation_results['warnings']}, 실패 {validation_results['failed']}")
                
                return validation_results['failed'] == 0
                        
        except Exception as e:
            logger.error(f"❌ static_indicators 데이터 검증 실패: {e}")
            return False

    def _attempt_column_recalculation(self, column):
        """컬럼 데이터 재계산 시도"""
        try:
            logger.info(f"🔧 {column} 컬럼 데이터 재계산 시도 중...")
            
            # 재계산 로직은 data_fetcher의 static_indicators 계산 함수 활용
            # 여기서는 로그만 남기고 실제 재계산은 별도 프로세스에서 수행
            logger.warning(f"⚠️ {column} 컬럼 재계산 필요 - data_fetcher.process_all_static_indicators() 실행 권장")
            
        except Exception as e:
            logger.error(f"❌ {column} 컬럼 재계산 실패: {e}")

    def _attempt_column_type_fix(self, column, error):
        """컬럼 데이터 타입 문제 해결 시도"""
        try:
            error_str = str(error).lower()
            
            if 'function avg(text)' in error_str:
                logger.warning(f"🔧 {column} 컬럼이 TEXT 타입으로 저장됨 - 숫자 변환 필요")
                # 실제 타입 변환은 별도 마이그레이션에서 수행
                
            elif 'does not exist' in error_str:
                logger.warning(f"🔧 {column} 컬럼이 존재하지 않음 - 스키마 업데이트 필요")
                
        except Exception as e:
            logger.error(f"❌ {column} 컬럼 타입 수정 실패: {e}")

    def _attempt_text_column_recalculation(self, column):
        """텍스트 컬럼 재계산 시도"""
        try:
            logger.info(f"🔧 {column} 텍스트 컬럼 재계산 시도 중...")
            
            if column == 'supertrend_signal':
                logger.warning(f"⚠️ {column} 컬럼 재계산 필요 - Supertrend 지표 재계산 권장")
                
        except Exception as e:
            logger.error(f"❌ {column} 텍스트 컬럼 재계산 실패: {e}")

    def _attempt_text_column_fix(self, column, error):
        """텍스트 컬럼 문제 해결 시도"""
        try:
            error_str = str(error).lower()
            
            if 'does not exist' in error_str:
                logger.warning(f"🔧 {column} 텍스트 컬럼이 존재하지 않음 - 스키마 업데이트 필요")
                
        except Exception as e:
            logger.error(f"❌ {column} 텍스트 컬럼 수정 실패: {e}")

    def validate_ohlcv_precision(self, sample_size=10):
        """OHLCV 데이터의 소수점 정밀도 검증 (스몰캡 코인 지원)"""
        try:
            with self.db_mgr.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # 가격이 0인 레코드 확인
                cursor.execute("""
                    SELECT ticker, COUNT(*) as zero_count
                    FROM ohlcv 
                    WHERE (open = 0 OR high = 0 OR low = 0 OR close = 0)
                    GROUP BY ticker
                    ORDER BY zero_count DESC
                    LIMIT %s
                """, (sample_size,))
                
                zero_records = cursor.fetchall()
                
                if zero_records:
                    logger.warning(f"⚠️ 가격이 0인 레코드 발견: {len(zero_records)}개 티커")
                    for ticker, count in zero_records:
                        logger.warning(f"   - {ticker}: {count}개 레코드")
                else:
                    logger.info("✅ 가격이 0인 레코드 없음")
                
                # 극소값 가격 확인 (스몰캡 코인)
                cursor.execute("""
                    SELECT ticker, close, volume
                    FROM ohlcv 
                    WHERE close < 0.01 AND close > 0
                    AND date >= CURRENT_DATE - INTERVAL '7 days'
                    ORDER BY close ASC
                    LIMIT %s
                """, (sample_size,))
                
                small_cap_records = cursor.fetchall()
                
                if small_cap_records:
                    logger.info(f"📊 스몰캡 코인 발견: {len(small_cap_records)}개")
                    for ticker, price, volume in small_cap_records:
                        logger.info(f"   - {ticker}: 가격 {price:.8f}, 거래량 {volume}")
                else:
                    logger.info("📊 스몰캡 코인 없음")
                    
        except Exception as e:
            logger.error(f"❌ OHLCV 정밀도 검증 실패: {e}")

    @retry_on_error(max_retries=3, delay=5)
    def init_db(self):
        from init_db_pg import create_tables
        logger.info("🔧 DB 테이블 생성 확인 및 초기화")
        try:
            create_tables()
        except Exception as e:
            handle_db_error(e, "DB 테이블 생성")
            raise

    @retry_on_error(max_retries=3, delay=5)
    def update_tickers(self):
        try:
            logger.info("🔄 티커 목록 업데이트 중")
            update_tickers()
            logger.info("✅ 티커 업데이트 완료")
        except Exception as e:
            handle_network_error(e, "티커 업데이트")
            raise

    def update_trailing_stops_batch(self, assets_data):
        """배치 처리로 트레일링 스탑 업데이트 최적화"""
        if not assets_data:
            logger.info("💼 트레일링 스탑 업데이트할 자산이 없습니다.")
            return
        
        logger.info(f"🔄 {len(assets_data)}개 자산 트레일링 스탑 배치 처리 시작")
        
        try:
            # 모든 티커에 대한 현재가 및 ATR 데이터 일괄 조회
            all_tickers = [asset['ticker'] for asset in assets_data]
            current_prices = {}
            atr_values = {}
            
            # 현재가 일괄 조회
            try:
                import pyupbit
                ticker_chunks = [all_tickers[i:i+20] for i in range(0, len(all_tickers), 20)]
                
                for chunk in ticker_chunks:
                    chunk_prices = pyupbit.get_current_price(chunk)
                    if isinstance(chunk_prices, dict):
                        current_prices.update(chunk_prices)
                    elif len(chunk) == 1 and chunk_prices is not None:
                        current_prices[chunk[0]] = chunk_prices
            except Exception as e:
                logger.error(f"❌ 현재가 일괄 조회 중 오류: {e}")
                return
            
            # ATR 값 일괄 조회 (실제 존재하는 컬럼만 조회)
            try:
                with self.db_mgr.get_connection_context() as conn:
                    cursor = conn.cursor()
                    placeholders = ','.join(['%s'] * len(all_tickers))
                    
                    # static_indicators 테이블에서 실제 존재하는 컬럼만 조회
                    atr_query = f"""
                        SELECT ticker, atr, volume_change_7_30
                        FROM static_indicators 
                        WHERE ticker IN ({placeholders})
                    """
                    cursor.execute(atr_query, all_tickers)
                    
                    for row in cursor.fetchall():
                        ticker, atr, volume_change = row
                        if atr is not None:
                            atr_values[ticker] = {
                                'atr': safe_float_convert(atr, context=f"{ticker} ATR"),
                                'volume_ratio': safe_float_convert(volume_change, context=f"{ticker} Volume Change")
                            }
                    
                    # RSI는 ohlcv 테이블에서 별도 조회 (최신 데이터 기준)
                    rsi_query = f"""
                        SELECT ticker, rsi_14
                        FROM ohlcv 
                        WHERE ticker IN ({placeholders})
                        AND date >= CURRENT_DATE - INTERVAL '7 days'
                        ORDER BY ticker, date DESC
                    """
                    cursor.execute(rsi_query, all_tickers)
                    
                    # 티커별 최신 RSI 값 수집
                    rsi_data = {}
                    for row in cursor.fetchall():
                        ticker, rsi_14 = row
                        if ticker not in rsi_data and rsi_14 is not None:
                            rsi_data[ticker] = safe_float_convert(rsi_14, context=f"{ticker} RSI")
                    
                    # ATR 데이터에 RSI 추가
                    for ticker in atr_values:
                        atr_values[ticker]['rsi'] = rsi_data.get(ticker, 50)  # 기본값 50
                        
            except Exception as e:
                logger.error(f"❌ ATR 데이터 일괄 조회 중 오류: {e}")
                return
            
            # 동적 스탑 로스 계산 및 배치 업데이트 준비
            batch_updates = []
            
            for asset in assets_data:
                ticker = asset['ticker']
                
                if ticker not in current_prices or ticker not in atr_values:
                    continue
                
                current_price = current_prices[ticker]
                atr_data = atr_values[ticker]
                
                # ATR 기반 동적 스탑 로스 계산
                dynamic_stop = self.calculate_dynamic_stop(ticker, current_price, atr_data)
                
                if dynamic_stop:
                    batch_updates.append({
                        'ticker': ticker,
                        'stop_price': dynamic_stop['stop_price'],
                        'activation_price': dynamic_stop['activation_price'],
                        'atr_value': dynamic_stop['atr_value'],
                        'updated_at': datetime.now()
                    })
            
            # 배치 DB 업데이트 실행
            if batch_updates:
                self.db_mgr.batch_update_trailing_stops(batch_updates)
                logger.info(f"✅ {len(batch_updates)}개 트레일링 스탑 배치 업데이트 완료")
            
        except Exception as e:
            logger.error(f"❌ 트레일링 스탑 배치 처리 중 오류: {e}")
    
    def calculate_dynamic_stop(self, ticker, current_price, atr_data):
        """ATR 기반 동적 스탑 로스 계산"""
        try:
            atr_value = atr_data['atr']
            rsi = atr_data.get('rsi', 50)
            volume_ratio = atr_data.get('volume_ratio', 1.0)
            
            # ATR 기반 변동성 계산 (가격 대비 퍼센트)
            atr_pct = (atr_value / current_price) * 100
            
            # 시장 상황을 고려한 동적 배수 계산
            # RSI가 높을수록 (과매수) 더 보수적 스탑
            # 거래량이 많을수록 더 보수적 스탑
            rsi_multiplier = 1.0 + (rsi - 50) * 0.01  # RSI 50 기준으로 조정
            volume_multiplier = min(1.0 + (volume_ratio - 1.0) * 0.2, 1.5)  # 최대 1.5배
            
            # 기본 ATR 배수에 동적 요소 적용
            base_multiplier = self.ts_cfg.get('atr_multiplier', 2.0)
            dynamic_multiplier = base_multiplier * rsi_multiplier * volume_multiplier
            
            # 최종 스탑 가격 계산
            stop_distance = atr_value * dynamic_multiplier
            stop_price = current_price - stop_distance
            
            # 활성화 가격 (현재가 기준 5% 상승)
            activation_price = current_price * 1.05
            
            logger.debug(f"📊 {ticker} 동적 스탑 계산: "
                        f"ATR={atr_value:.2f}({atr_pct:.2f}%), "
                        f"배수={dynamic_multiplier:.2f}, "
                        f"스탑가={stop_price:.2f}")
            
            return {
                'stop_price': stop_price,
                'activation_price': activation_price,
                'atr_value': atr_value,
                'dynamic_multiplier': dynamic_multiplier
            }
            
        except Exception as e:
            logger.error(f"❌ {ticker} 동적 스탑 계산 중 오류: {e}")
            return None

    def update_portfolio(self):
        """
        1. 계정 정보 업데이트
        2. 포트폴리오 자산 변동 기록
        3. 보유종목 TrailingStop 설정/관리 (배치 처리 최적화)
        """
        try:
            logger.info("🔄 포트폴리오 정보 업데이트 중")
            balances = self.upbit.get_balances()

            # 🔧 [수정] balances 응답 형식 검증 및 변환 (portfolio_manager.py와 동일한 로직)
            logger.debug(f"🔍 update_portfolio balances 응답 타입: {type(balances)}")
            
            # None인 경우 처리
            if balances is None:
                logger.warning("⚠️ get_balances가 None을 반환했습니다.")
                return []
            
            # 문자열로 반환된 경우 JSON 파싱 시도
            if isinstance(balances, str):
                try:
                    import json
                    balances = json.loads(balances)
                    logger.info("✅ 문자열 응답을 JSON으로 파싱 완료")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ JSON 파싱 실패: {e}")
                    return []
            
            # 리스트가 아닌 경우 처리
            if not isinstance(balances, list):
                logger.info(f"📊 update_portfolio: balances 반환값이 리스트가 아님 (타입: {type(balances)}) - 변환 시도")
                if isinstance(balances, dict):
                    if 'data' in balances:
                        balances = balances['data']
                        logger.info("✅ 'data' 키에서 리스트 추출 완료")
                    elif 'result' in balances:
                        balances = balances['result']
                        logger.info("✅ 'result' 키에서 리스트 추출 완료")
                    else:
                        # 단일 잔고 정보인 경우 리스트로 변환
                        balances = [balances]
                        logger.info("✅ 단일 잔고 정보를 리스트로 변환 완료")
                else:
                    logger.error(f"❌ 예상치 못한 balances 형식: {type(balances)}")
                    return []

            # 블랙리스트 로드
            blacklist = load_blacklist()

            # 🔧 [수정] 블랙리스트에 포함된 종목 필터링 (안전한 딕셔너리 접근)
            filtered_balances = []
            for balance in balances:
                try:
                    if isinstance(balance, dict) and balance.get('currency'):
                        currency = balance.get('currency')
                        if f"KRW-{currency}" not in blacklist:
                            filtered_balances.append(balance)
                        else:
                            logger.debug(f"⏭️ {currency}는 블랙리스트에 포함되어 제외됩니다.")
                except Exception as e:
                    logger.warning(f"⚠️ balance 필터링 중 오류: {e} - {balance}")
                    continue
            
            balances = filtered_balances

            # DB에 포트폴리오 정보 저장
            self.db_mgr.save_portfolio_history(balances)
            
            # 포트폴리오 요약 정보 출력 (simple_portfolio_summary 사용)
            logger.info("📊 포트폴리오 현황 출력 시작")
            self.pm.simple_portfolio_summary()
            logger.info("📊 포트폴리오 현황 출력 완료")
            
            # 포트폴리오 자산 추적 및 기록
            total = self.pm.get_total_balance()
            logger.info(f"💰 총 자산: {total:,.0f} KRW")

            # 트레일링 스탑 설정 및 관리 (배치 처리 최적화)
            if not balances or len(balances) <= 1:  # KRW만 있는 경우
                logger.info("💼 보유 자산이 없어 트레일링 스탑 설정 건너뜀")
                return balances
                
            # KRW를 제외한 실제 보유 자산 필터링
            assets = [balance for balance in balances if balance.get('currency') != 'KRW']
            if not assets:
                logger.info("💼 KRW 외 보유 자산이 없어 트레일링 스탑 설정 건너뜀")
                return balances
            
            # 자산 데이터 구조화
            assets_data = []
            for asset in assets:
                currency = asset.get('currency')
                ticker = f"KRW-{currency}"
                assets_data.append({
                    'ticker': ticker,
                    'currency': currency,
                    'balance': safe_float_convert(asset.get('balance', 0), context=f"{ticker} balance"),
                    'avg_buy_price': safe_float_convert(asset.get('avg_buy_price', 0), context=f"{ticker} avg_buy_price")
                })
            
            # 배치 처리로 트레일링 스탑 업데이트
            self.update_trailing_stops_batch(assets_data)
                
            logger.info("✅ 포트폴리오 업데이트 완료")
            return balances
            
        except Exception as e:
            logger.error(f"❌ 포트폴리오 업데이트 중 오류 발생: {str(e)}")
            return None

    def save_gpt_analysis_to_db(self, gpt_results: list):
        """
        GPT 분석 결과를 PostgreSQL의 trend_analysis 테이블에 저장합니다.
        
        Args:
            gpt_results (list): GPT 분석 결과 딕셔너리 리스트
        """
        if not self.save_to_db:
            logger.info("💾 DB 저장 설정이 비활성화되어 GPT 분석 결과 저장을 건너뜁니다.")
            return
            
        if not gpt_results:
            logger.warning("⚠️ 저장할 GPT 분석 결과가 없습니다.")
            return
            
        logger.info(f"🔄 GPT 분석 결과 DB 저장 시작: {len(gpt_results)}개 종목")
        
        # 디버깅: 받은 데이터 로깅
        for i, result in enumerate(gpt_results):
            logger.info(f"📝 저장할 데이터 {i+1}: {result}")
            
        try:
            with self.get_db_connection_safe() as conn:
                cursor = conn.cursor()
                
                # 테이블 존재 여부 확인
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'trend_analysis'
                    );
                """)
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    logger.error("❌ trend_analysis 테이블이 존재하지 않습니다!")
                    return
                
                logger.info("✅ trend_analysis 테이블 존재 확인됨")
                
                # ✅ trend_analysis 테이블의 실제 스키마에 맞게 수정
                # 실제 컬럼: ticker, score, confidence, action, market_phase, pattern, reason
                insert_query = """
                INSERT INTO trend_analysis (ticker, score, confidence, action, market_phase, pattern, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET
                score = EXCLUDED.score,
                confidence = EXCLUDED.confidence,
                action = EXCLUDED.action,
                market_phase = EXCLUDED.market_phase,
                pattern = EXCLUDED.pattern,
                reason = EXCLUDED.reason,
                created_at = CURRENT_TIMESTAMP
                """
                
                data_to_insert = []
                for result in gpt_results:
                    try:
                        ticker = result.get("ticker")
                        if not ticker:
                            logger.warning(f"⚠️ 티커가 없는 결과 건너뜀: {result}")
                            continue
                        
                        # 🔧 안전한 타입 변환 적용
                        score = safe_float_convert(result.get("score", 0), context=f"GPT분석 {ticker} score")
                        confidence = safe_float_convert(result.get("confidence", 0), context=f"GPT분석 {ticker} confidence")
                        action = result.get("action", "HOLD")
                        market_phase = result.get("market_phase", "Unknown")
                        pattern = result.get("pattern", "")
                        reason = result.get("reason", "")
                        
                        # ✅ 필드 누락 검증 및 경고 (개선된 데이터 품질 검증)
                        if not action or action == "HOLD":
                            logger.warning(f"⚠️ {ticker} action 필드 누락 또는 기본값: {action}")
                        if not market_phase or market_phase == "Unknown":
                            logger.warning(f"⚠️ {ticker} market_phase 필드 누락 또는 기본값: {market_phase}")
                        if not pattern:
                            logger.warning(f"⚠️ {ticker} pattern 필드 누락")
                        if not reason:
                            logger.warning(f"⚠️ {ticker} reason 필드 누락")
                        
                        # 데이터 유효성 검사
                        if not isinstance(action, str) or action not in ["BUY", "HOLD", "AVOID", "SELL"]:
                            logger.warning(f"⚠️ {ticker} 잘못된 action 값: {action}, HOLD로 변경")
                            action = "HOLD"
                        
                        insert_data = (ticker, score, confidence, action, market_phase, pattern, reason)
                        data_to_insert.append(insert_data)
                        logger.info(f"✅ {ticker} 데이터 준비 완료: score={score}, action={action}, phase={market_phase}")
                        
                    except Exception as e:
                        logger.error(f"❌ GPT 분석 결과 데이터 변환 오류: {result.get('ticker', 'Unknown')} | {str(e)}")
                        import traceback
                        logger.error(f"상세 오류: {traceback.format_exc()}")
                        continue
                
                if data_to_insert:
                    logger.info(f"🔄 {len(data_to_insert)}개 데이터 DB 삽입 시작")
                    
                    # 개별 삽입으로 더 상세한 디버깅
                    success_count = 0
                    for i, data in enumerate(data_to_insert):
                        try:
                            cursor.execute(insert_query, data)
                            success_count += 1
                            logger.info(f"✅ {data[0]} 삽입 성공 ({i+1}/{len(data_to_insert)})")
                        except Exception as insert_error:
                            logger.error(f"❌ {data[0]} 삽입 실패: {str(insert_error)}")
                            logger.error(f"삽입 데이터: {data}")
                    
                    # 커밋
                    conn.commit()
                    logger.info(f"✅ 트랜잭션 커밋 완료: {success_count}/{len(data_to_insert)}개 성공")
                    
                    # 실제 저장 확인
                    cursor.execute("SELECT COUNT(*) FROM trend_analysis WHERE ticker = ANY(%s)", 
                                 ([row[0] for row in data_to_insert],))
                    saved_count = cursor.fetchone()[0]
                    logger.info(f"🔍 저장 검증: DB에서 {saved_count}개 확인됨")
                    
                    if saved_count == len(data_to_insert):
                        logger.info(f"✅ GPT 분석 결과 DB 저장 완료: {len(data_to_insert)}개 종목")
                    else:
                        logger.warning(f"⚠️ 저장 불일치: 시도={len(data_to_insert)}, 실제저장={saved_count}")
                else:
                    logger.warning("⚠️ 저장 가능한 GPT 분석 결과가 없습니다.")
                    
        except Exception as db_error:
            # psycopg2.Error는 psycopg2가 import되지 않았을 수 있으므로 일반 Exception으로 처리
            logger.error(f"❌ PostgreSQL DB 오류: {str(db_error)}")
            if hasattr(db_error, 'pgcode'):
                logger.error(f"오류 코드: {db_error.pgcode}")
            import traceback
            logger.error(f"상세 스택: {traceback.format_exc()}")
        except Exception as e:
            logger.error(f"❌ GPT 분석 결과 DB 저장 중 예상치 못한 오류: {str(e)}")
            import traceback
            logger.error(f"상세 스택: {traceback.format_exc()}")

    def save_trade_log_to_db(self, trade_logs: list):
        """
        매수 이력을 PostgreSQL의 trade_log 테이블에 저장합니다.
        
        Args:
            trade_logs (list): 매수 이력 딕셔너리 리스트
        """
        if not self.save_to_db:
            logger.info("💾 DB 저장 설정이 비활성화되어 매수 이력 저장을 건너뜁니다.")
            return
            
        if not trade_logs:
            logger.info("💾 저장할 매수 이력이 없습니다.")
            return
            
        try:
            with self.get_db_connection_safe() as conn:
                cursor = conn.cursor()
                
                # trade_log 테이블에 데이터 삽입 (qty 컬럼 추가)
                insert_query = """
                INSERT INTO trade_log (ticker, action, buy_price, qty, score, confidence, trade_amount_krw, bought_at, status, error_msg)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
                """
                
                data_to_insert = []
                for log in trade_logs:
                    try:
                        ticker = log.get("ticker")
                        
                        # 🔧 안전한 타입 변환 적용
                        buy_price = safe_float_convert(log.get("buy_price", 0), context=f"매수이력 {ticker} buy_price")
                        score = safe_float_convert(log.get("score", 0), context=f"매수이력 {ticker} score")
                        confidence = safe_float_convert(log.get("confidence", 0), context=f"매수이력 {ticker} confidence")
                        trade_amount_krw = safe_float_convert(log.get("trade_amount_krw", 0), context=f"매수이력 {ticker} trade_amount_krw")
                        
                        status = log.get("status", "UNKNOWN")
                        error_msg = log.get("error_msg", None)
                        
                        # action 컬럼 설정 (매수 시도이므로 'BUY'로 설정)
                        action = "BUY"
                        
                        # qty 계산 (trade_amount_krw / buy_price)
                        qty = 0.0
                        if buy_price > 0:
                            qty = trade_amount_krw / buy_price
                        
                        data_to_insert.append((ticker, action, buy_price, qty, score, confidence, trade_amount_krw, status, error_msg))
                        
                    except Exception as e:
                        logger.warning(f"⚠️ 매수 이력 데이터 변환 오류: {log.get('ticker', 'Unknown')} | {str(e)}")
                        continue
                
                if data_to_insert:
                    cursor.executemany(insert_query, data_to_insert)
                    conn.commit()
                    logger.info(f"✅ 매수 이력 DB 저장 완료: {len(data_to_insert)}개 이력")
                else:
                    logger.warning("⚠️ 저장 가능한 매수 이력이 없습니다.")
                    
        except Exception as e:
            logger.warning(f"⚠️ 매수 이력 DB 저장 실패: {str(e)}")

    @retry_on_error(max_retries=3, delay=5)
    def update_all_tickers_ohlcv(self, ticker):
        """
        Fetch incremental 1d OHLCV data for `ticker`, save to DB, and delete data older than 251 days.
        """
        from datetime import date
        from data_fetcher import get_ohlcv_d, delete_old_ohlcv

        # 1) Determine the last stored date
        try:
            row = self.db_mgr.execute_query(
                "SELECT MAX(date) FROM ohlcv WHERE ticker = %s", (ticker,), fetchone=True
            )
            last_date = row[0] if row and row[0] else None
        except Exception as e:
            logger.error(f"❌ {ticker} 가장 최신 OHLCV 날짜 조회 중 오류: {e}")
            last_date = None

        today = date.today()
        # 2) Fetch data
        if last_date is None:
            # No data in DB, fetch full history
            df = get_ohlcv_d(ticker)
        else:
            days_diff = (today - last_date).days
            if days_diff <= 0:
                logger.info(f"✅ {ticker} OHLCV가 최신 상태입니다 (마지막 저장일: {last_date})")
                return
            df = get_ohlcv_d(ticker, interval='1d', count=days_diff)

        # 기존: df = get_ohlcv_d() 이후
        if df is not None and not df.empty:
            # 통합된 저장 로직 사용 (날짜 복구 + 적응형 소수점 + 원자적 저장)
            save_result = enhanced_ohlcv_processor(ticker, df, data_source="api")
            if save_result:
                logger.info(f"✅ {ticker} OHLCV 업데이트 저장 완료 (통합 파이프라인)")
            else:
                logger.error(f"❌ {ticker} OHLCV 업데이트 저장 실패 (통합 파이프라인)")

        # 3) Delete old records
        delete_old_ohlcv(ticker)
        logger.info(f"✅ {ticker} OHLCV 데이터 업데이트 완료 (추가 {len(df)}개 봉)")

    # 🔧 [제거] 중복 함수 제거 - data_fetcher.calculate_technical_indicators 직접 사용

    def save_chart_image(self, ticker: str, df: pd.DataFrame) -> str:
        """차트 이미지를 생성하고 저장합니다"""
        try:
            from data_fetcher import save_chart_image
            save_chart_image(ticker, df)
            return f"charts/{ticker}.png"
        except Exception as e:
            logger.error(f"❌ {ticker} 차트 이미지 생성 실패: {str(e)}")
            return None

    def generate_ohlcv_json(self, ticker: str) -> str:
        """
        수정 목표: 분리된 테이블에서 안전하게 데이터 조회
        
        수정 내용:
        1. get_combined_ohlcv_and_static_data() 함수 사용
        2. ohlcv 동적 지표와 static 지표를 별도로 처리
        3. JSON 구조를 다음과 같이 변경:
        {
            "ticker": "KRW-BTC",
            "ohlcv": [...],  // 기본 OHLCV + 동적 지표
            "indicators": {
                "static": {...},  // pivot, r1, s1, support, resistance
                "dynamic": {...}  // 시계열 동적 지표
            }
        }
        """
        try:
            from utils import get_combined_ohlcv_and_static_data
            
            # 분리된 테이블에서 안전하게 데이터 조회
            combined_data = get_combined_ohlcv_and_static_data(ticker, limit_days=100)
            
            if not combined_data['ohlcv_data']:
                logger.warning(f"⚠️ {ticker} OHLCV 데이터가 없습니다.")
                return None
            
            # OHLCV 데이터 가져오기
            df = get_ohlcv_d(ticker, interval="day", count=100, force_fetch=False)
            
            if df is None or df.empty:
                logger.warning(f"⚠️ {ticker} 기본 OHLCV 데이터가 없습니다.")
                return None
            
            # 정적 지표 데이터 준비
            static_indicators = {}
            if combined_data['static_data'] and combined_data['static_columns']:
                static_columns = combined_data['static_columns']
                static_values = combined_data['static_data']
                
                # 정적 지표를 딕셔너리로 변환 (ticker 컬럼 제외)
                for i, col in enumerate(static_columns):
                    if col != 'ticker' and i < len(static_values):
                        static_indicators[col] = static_values[i]
                
                logger.info(f"✅ {ticker} 정적 지표 {len(static_indicators)}개 수집: {list(static_indicators.keys())}")
            
            # JSON 변환을 위해 인덱스를 문자열로 변환
            df.index = [safe_strftime(idx, '%Y-%m-%d') for idx in df.index]
            
            # NaN 값을 None으로 변환하여 JSON 호환성 확보
            df = df.where(pd.notnull(df), None)
            
            # 새로운 JSON 구조 생성
            json_structure = {
                "ticker": ticker,
                "ohlcv": df.to_dict(orient='index'),
                "indicators": {
                    "static": static_indicators,
                    "dynamic": {
                        "description": "시계열 동적 지표들은 ohlcv 데이터에 포함되어 있습니다",
                        "available_in_ohlcv": ["ma_50", "ma_200", "rsi_14", "macd_histogram", "ht_trendline"]
                    }
                }
            }
            
            # JSON 문자열로 변환
            import json
            json_data = json.dumps(json_structure, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ {ticker} 분리된 구조의 JSON 데이터 생성 완료")
            logger.info(f"   - OHLCV 데이터: {len(df)}일치")
            logger.info(f"   - 정적 지표: {len(static_indicators)}개")
            
            return json_data
            
        except Exception as e:
            logger.warning(f"⚠️ {ticker} 분리 구조 JSON 생성 실패: {str(e)}")
            # 기존 방식으로 폴백
            try:
                logger.info(f"🔄 {ticker} 기존 방식으로 폴백 시도")
                df = get_ohlcv_d(ticker, interval="day", count=100, force_fetch=False)
                
                if df is not None and not df.empty:
                    df.index = [safe_strftime(idx, '%Y-%m-%d') for idx in df.index]
                    df = df.where(pd.notnull(df), None)
                    json_data = df.to_json(orient='index', indent=2)
                    logger.info(f"✅ {ticker} 폴백 JSON 생성 성공")
                    return json_data
                
            except Exception as fallback_error:
                logger.error(f"❌ {ticker} 폴백도 실패: {fallback_error}")
            
            return None

    def fetch_market_data_internal(self, tickers: list, timeframe: str = '1d') -> pd.DataFrame:
        """
        시장 데이터를 수집하고 저장하는 내부 메서드 - 날짜 복구 우선 실행
        """
        try:
            import pandas as pd
            start_time = time.time()
            logger.info(f"🔄 {timeframe} 봉 시장 데이터 수집 시작")

            # 블랙리스트 로드 및 필터링
            blacklist = load_blacklist() or {}
            filtered_tickers = [ticker for ticker in tickers if ticker not in blacklist]
            if not filtered_tickers:
                logger.warning("⚠️ 처리할 티커가 없습니다.")
                return pd.DataFrame()

            logger.info(f"📊 처리 대상 티커: {len(filtered_tickers)}개 (전체: {len(tickers)}개, 블랙리스트 제외: {len(tickers) - len(filtered_tickers)}개)")

            processed_tickers = []
            failed_tickers = []

            for i, ticker in enumerate(filtered_tickers, 1):
                try:
                    logger.info(f"🔄 [{i}/{len(filtered_tickers)}] {ticker} 처리 시작...")
                    
                    if ticker in load_blacklist():
                        logger.info(f"⏭️ {ticker}는 블랙리스트에 있어 처리 건너뜀")
                        continue

                    # OHLCV 가져오기 - 개선된 통합 방식
                    if timeframe == '1d':
                        # 1단계: DB에서 먼저 조회
                        ohlcv_data = self.db_mgr.fetch_ohlcv(ticker, days=450)
                        
                        # 2단계: DB에 데이터가 없으면 API에서 수집
                        if ohlcv_data is None or ohlcv_data.empty:
                            logger.warning(f"⚠️ {ticker} DB에 OHLCV 데이터 없음 → API 수집 시도")
                            from data_fetcher import get_ohlcv_d
                            ohlcv_data = get_ohlcv_d(ticker, count=450)
                            
                            if ohlcv_data is None or ohlcv_data.empty:
                                logger.error(f"❌ {ticker} API 수집 실패")
                                failed_tickers.append(ticker)
                                continue
                            else:
                                # 수집된 데이터를 DB에 저장
                                self.db_mgr.insert_ohlcv(ticker, ohlcv_data)
                                logger.info(f"✅ {ticker} API 수집 및 DB 저장 완료")
                    else:
                        # 4시간봉은 별도 처리
                        from data_fetcher import get_ohlcv_4h
                        ohlcv_data = get_ohlcv_4h(ticker)

                    # 데이터 검증 강화
                    if ohlcv_data is None or ohlcv_data.empty:
                        logger.error(f"❌ {ticker} OHLCV 데이터 수집/조회 완전 실패")
                        failed_tickers.append(ticker)
                        continue

                    # ✅ 핵심: 지표 계산 전에 통합 파이프라인으로 처리
                    if hasattr(ohlcv_data.index, 'year') and len(ohlcv_data.index) > 0 and ohlcv_data.index[0].year == 1970:
                        logger.warning(f"🚨 {ticker} 1970 날짜 감지 - 지표 계산 전 통합 처리")
                        enhanced_ohlcv_processor(ticker, ohlcv_data, data_source="api")
                        logger.info(f"✅ {ticker} 통합 파이프라인 처리 완료")

                    # 안전한 날짜 출력을 위한 처리
                    try:
                        from utils import safe_strftime
                        if hasattr(ohlcv_data.index, '__len__') and len(ohlcv_data.index) > 0:
                            # 인덱스가 DatetimeIndex인지 확인
                            if isinstance(ohlcv_data.index, pd.DatetimeIndex):
                                start_date = safe_strftime(ohlcv_data.index[0])
                                end_date = safe_strftime(ohlcv_data.index[-1])
                                
                                # 1970-01-01 패턴 감지 및 통합 파이프라인 처리
                                if start_date == "1970-01-01" and end_date == "1970-01-01":
                                    logger.warning(f"🚨 {ticker} 날짜 이상 감지: 기간: {start_date} ~ {end_date}")
                                    ohlcv_data_before = f"{start_date} ~ {end_date}"
                                    
                                    # 통합 파이프라인 실행 (날짜 복구 + 소수점 처리 + 저장)
                                    enhanced_ohlcv_processor(ticker, ohlcv_data, data_source="api")
                                    
                                    logger.info(f"📅 {ticker} 통합 파이프라인 처리 완료: {ohlcv_data_before}")
                                    logger.info(f"✅ {ticker} OHLCV 데이터 확보: {len(ohlcv_data)}개 레코드")
                                else:
                                    logger.info(f"✅ {ticker} OHLCV 데이터 확보: {len(ohlcv_data)}개 레코드 (기간: {start_date} ~ {end_date})")
                            else:
                                # DatetimeIndex가 아닌 경우 변환 시도
                                ohlcv_data.index = pd.to_datetime(ohlcv_data.index)
                                start_date = safe_strftime(ohlcv_data.index[0])
                                end_date = safe_strftime(ohlcv_data.index[-1])
                                
                                # 1970-01-01 패턴 감지 및 통합 파이프라인 처리
                                if start_date == "1970-01-01" and end_date == "1970-01-01":
                                    logger.warning(f"🚨 {ticker} 날짜 이상 감지: 기간: {start_date} ~ {end_date}")
                                    ohlcv_data_before = f"{start_date} ~ {end_date}"
                                    
                                    # 통합 파이프라인 실행 (날짜 복구 + 소수점 처리 + 저장)
                                    enhanced_ohlcv_processor(ticker, ohlcv_data, data_source="api")
                                    
                                    logger.info(f"📅 {ticker} 통합 파이프라인 처리 완료: {ohlcv_data_before}")
                                    logger.info(f"✅ {ticker} OHLCV 데이터 확보: {len(ohlcv_data)}개 레코드")
                                else:
                                    logger.info(f"✅ {ticker} OHLCV 데이터 확보: {len(ohlcv_data)}개 레코드 (기간: {start_date} ~ {end_date})")
                        else:
                            logger.info(f"✅ {ticker} OHLCV 데이터 확보: {len(ohlcv_data)}개 레코드 (날짜 정보 없음)")
                    except Exception as date_err:
                        logger.info(f"✅ {ticker} OHLCV 데이터 확보: {len(ohlcv_data)}개 레코드")
                        logger.debug(f"날짜 출력 오류: {date_err}")

                    # ========== 통합 파이프라인 처리 ==========
                    # 1단계: 통합 OHLCV 처리 (날짜 복구 + 소수점 처리 + 저장)
                    logger.info(f"1단계: {ticker} 통합 OHLCV 처리")
                    from data_fetcher import enhanced_ohlcv_processor
                    save_result = enhanced_ohlcv_processor(ticker, ohlcv_data, data_source="api")
                    if not save_result:
                        logger.error(f"❌ {ticker} 통합 OHLCV 처리 실패 - 지표 계산 중단")
                        failed_tickers.append(ticker)
                        continue
                    
                    # 2단계: 지표 계산
                    logger.info(f"2단계: {ticker} 지표 계산")
                    # ========================================
                    
                    # ✅ 복구된 올바른 날짜로 지표 계산
                    logger.info(f"🔄 {ticker} 기술적 지표 계산 시작...")
                    if timeframe == '1d':
                        # 통합 지표 계산 사용 (static_indicators + ohlcv 동적지표)
                        from data_fetcher import calculate_unified_indicators
                        df_with_indicators = calculate_unified_indicators(ohlcv_data, ticker)
                    else:
                        from data_fetcher import calculate_technical_indicators_4h
                        df_with_indicators = calculate_technical_indicators_4h(ohlcv_data)

                    if df_with_indicators is None or df_with_indicators.empty:
                        logger.warning(f"⚠️ {ticker} {timeframe} 봉 기술적 지표 계산 실패")
                        failed_tickers.append(ticker)
                        continue
                    else:
                        logger.info(f"✅ {ticker} 기술적 지표 계산 완료: {len(df_with_indicators)}개 레코드")

                    # DB 저장
                    logger.info(f"🔄 {ticker} DB 저장 시작...")
                    if timeframe == '1d':
                        # 원자적 통합 저장 (static_indicators + ohlcv 동적지표만)
                        from data_fetcher import save_all_indicators_atomically
                        save_result = save_all_indicators_atomically(ticker, df_with_indicators, timeframe)
                        if save_result:
                            logger.info(f"✅ {ticker} static_indicators + ohlcv 동적지표 저장 완료")
                        else:
                            logger.warning(f"⚠️ {ticker} 지표 저장 실패")
                            failed_tickers.append(ticker)
                            continue
                    else:
                        from data_fetcher import save_market_data_4h_to_db
                        save_market_data_4h_to_db(ticker, df_with_indicators)

                    processed_tickers.append(ticker)
                    logger.info(f"✅ {ticker} {timeframe} 봉 처리 완료")

                except Exception as e:
                    logger.error(f"❌ {ticker} {timeframe} 봉 처리 중 예외: {str(e)}")
                    import traceback
                    logger.error(f"상세 오류: {traceback.format_exc()}")
                    failed_tickers.append(ticker)
                    continue

            # 처리 결과 요약
            logger.info(f"📊 처리 결과 요약:")
            logger.info(f"   - 성공: {len(processed_tickers)}개")
            logger.info(f"   - 실패: {len(failed_tickers)}개")
            if failed_tickers:
                logger.warning(f"   - 실패 티커: {failed_tickers[:10]}" + (f" 외 {len(failed_tickers)-10}개" if len(failed_tickers) > 10 else ""))

            # 결과 조회 - static_indicators 테이블에서 조회 (표준화된 DB 연결 사용)
            if not processed_tickers:
                logger.warning("⚠️ 처리된 티커가 없습니다.")
                return pd.DataFrame()

            with self.get_db_connection_safe() as conn:
                if timeframe == '1d':
                    # 1일봉: static_indicators 테이블에서 조회
                    table_name = "static_indicators"
                    logger.info(f"🔍 {table_name} 테이블에서 데이터 조회 중...")
                    
                    market_data = pd.read_sql_query(
                        f"SELECT * FROM {table_name} WHERE ticker IN %s",
                        conn,
                        params=(tuple(processed_tickers),)
                    )
                else:
                    # 4시간봉: 기존 market_data_4h 테이블 사용
                    table_name = "market_data_4h"
                    logger.info(f"🔍 {table_name} 테이블에서 데이터 조회 중...")
                    
                    market_data = pd.read_sql_query(
                        f"SELECT * FROM {table_name} WHERE ticker IN %s",
                        conn,
                        params=(tuple(processed_tickers),)
                    )

                logger.info(f"🔍 {table_name} 테이블 조회 결과: {len(market_data)}개 레코드")
                
                if not market_data.empty:
                    market_data.set_index('ticker', inplace=True)
                    logger.info(f"✅ {timeframe} 봉 시장 데이터 조회 완료: {len(market_data)}개 티커")
                    
                    # 조회된 티커 목록 로깅
                    retrieved_tickers = market_data.index.tolist()
                    logger.info(f"📋 조회된 티커: {retrieved_tickers[:10]}" + (f" 외 {len(retrieved_tickers)-10}개" if len(retrieved_tickers) > 10 else ""))
                else:
                    logger.error(f"❌ {table_name} 테이블이 비어있음 (전체 레코드: 0개)")
                    
                    # 테이블 전체 상태 확인
                    total_count_query = f"SELECT COUNT(*) as cnt FROM {table_name}"
                    total_count_df = pd.read_sql_query(total_count_query, conn)
                    total_count = total_count_df.iloc[0]['cnt'] if not total_count_df.empty else 0
                    logger.error(f"❌ {table_name} 테이블 전체 레코드 수: {total_count}개")
            
            logger.info(f"✅ {timeframe} 봉 시장 데이터 수집 완료 (총 {len(processed_tickers)}개, 소요시간: {time.time() - start_time:.2f}초)")
            return market_data

        except Exception as e:
            logger.error(f"❌ {timeframe} 봉 시장 데이터 수집 중 오류 발생: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def run_backtest_and_report(self, ohlcv_df, market_df) -> bool:
        """통합된 backtester.py 사용으로 기능 확장"""
        if ohlcv_df is None or ohlcv_df.empty:
            logger.warning("⚠️ OHLCV 데이터 없음")
            return False
        if market_df is None or market_df.empty:
            logger.warning("⚠️ 시장 데이터 없음")
            return False
            
        try:
            # 🔧 [개선] 백테스트 스냅샷 생성 - backtest_ohlcv, backtest_sessions 테이블 업데이트
            logger.info("📊 백테스트 스냅샷 생성 시작...")
            from backtester import BacktestDataManager
            import pandas as pd
            
            # 백테스트 데이터 매니저 초기화
            backtest_manager = BacktestDataManager()
            
            # 현재 시간 기준으로 백테스트 세션 생성
            from datetime import datetime
            session_name = f"makenaide_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 백테스트 스냅샷 생성 (최근 200일 데이터)
            session_id = backtest_manager.create_backtest_snapshot(session_name, period_days=200)
            
            if session_id:
                logger.info(f"✅ 백테스트 스냅샷 생성 완료: {session_id}")
                
                # 생성된 스냅샷 데이터 확인
                snapshot_data = backtest_manager.get_backtest_data(session_id)
                if not snapshot_data.empty:
                    logger.info(f"📊 스냅샷 데이터 확인: {len(snapshot_data)}개 레코드")
                else:
                    logger.warning("⚠️ 스냅샷 데이터가 비어있습니다")
            else:
                logger.warning("⚠️ 백테스트 스냅샷 생성 실패")
            
            # 기존 SPOT_COMBOS 백테스트 + 새로운 하이브리드 전략
            from backtester import (
                backtest_combo, SPOT_COMBOS, HYBRID_SPOT_COMBOS, 
                generate_strategy_report, HybridFilteringBacktester,
                backtest_hybrid_filtering_performance
            )
            
            # 1. 기존 전략 조합 백테스트
            logger.info("🎯 기존 전략 조합 백테스트 시작")
            all_results = []
            
            # 기존 SPOT_COMBOS + 새로운 하이브리드 전략
            all_combos = SPOT_COMBOS + HYBRID_SPOT_COMBOS
            
            for combo in all_combos:
                logger.info(f'▶️ {combo["name"]} 백테스트 중...')
                results = backtest_combo(ohlcv_df, market_df, combo)
                if results:
                    all_results.extend(results)
                    
            if all_results:
                df_result = pd.DataFrame(all_results)
                logger.info('=== 확장된 스윗스팟 조건별 백테스트 결과 ===')
                logger.info(df_result.groupby('combo').agg({
                    'win_rate':'mean','avg_return':'mean','mdd':'mean',
                    'trades':'sum','b':'mean','kelly':'mean',
                    'kelly_1_2':'mean','swing_score':'mean'
                }))
                
                # 🔧 [개선] DB 저장 추가
                try:
                    from backtester import BacktestDataManager
                    backtest_manager = BacktestDataManager()
                    
                    # 백테스트 결과에 기간 정보 추가
                    from datetime import datetime, timedelta
                    period_end = datetime.now().date()
                    period_start = period_end - timedelta(days=200)  # 200일 기간
                    
                    for result in all_results:
                        result['period_start'] = period_start
                        result['period_end'] = period_end
                    
                    # DB에 결과 저장
                    if hasattr(backtest_manager, 'save_backtest_results_to_db'):
                        save_success = backtest_manager.save_backtest_results_to_db(all_results, session_id)
                        if save_success:
                            logger.info("✅ 백테스트 결과 DB 저장 완료")
                            
                            # 저장된 결과 확인
                            saved_results = backtest_manager.get_backtest_results_from_db(session_id)
                            if not saved_results.empty:
                                logger.info(f"📊 DB 저장 확인: {len(saved_results)}개 전략 결과")
                            else:
                                logger.warning("⚠️ DB 저장 확인 실패")
                        else:
                            logger.warning("⚠️ 백테스트 결과 DB 저장 실패")
                    else:
                        logger.warning("⚠️ save_backtest_results_to_db 메서드가 없습니다")
                    
                except Exception as db_error:
                    logger.warning(f"⚠️ 백테스트 결과 DB 저장 중 오류: {db_error}")
                
                # 기존 CSV 저장 유지 (호환성)
                df_result.to_csv('backtest_spot_results_with_hybrid.csv', index=False, float_format='%.2f')
                logger.info('결과가 backtest_spot_results_with_hybrid.csv에 저장되었습니다.')
            
            # 2. 하이브리드 필터링 성능 비교 백테스트
            logger.info("🔄 하이브리드 필터링 성능 비교 시작")
            
            # 최근 30일간 하이브리드 vs 정적전용 비교
            from datetime import datetime, timedelta
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            backtest_period = f"{start_date}:{end_date}"
            
            hybrid_comparison, optimal_weights = backtest_hybrid_filtering_performance(backtest_period)
            
            if hybrid_comparison:
                logger.info("📊 하이브리드 필터링 성능 비교 결과:")
                logger.info(f"   - 하이브리드 모드: {hybrid_comparison.get('hybrid', {})}")
                logger.info(f"   - 정적전용 모드: {hybrid_comparison.get('static_only', {})}")
                logger.info(f"   - 최적 가중치: {optimal_weights}")
                
                # 하이브리드 비교 결과 저장
                with open('hybrid_filtering_comparison.json', 'w', encoding='utf-8') as f:
                    json.dump({
                        'comparison': hybrid_comparison,
                        'optimal_weights': optimal_weights,
                        'period': backtest_period,
                        'generated_at': datetime.now().isoformat()
                    }, f, indent=2, ensure_ascii=False)
            
            # 3. 성능 모니터링 시스템과 연동
            logger.info("📈 성능 모니터링 시스템 연동")
            try:
                from performance_monitor import get_performance_monitor
                monitor = get_performance_monitor()
                
                backtest_metrics = {
                    'strategy_count': len(all_results) if all_results else 0,
                    'hybrid_comparison': hybrid_comparison,
                    'optimal_weights': optimal_weights,
                    'total_combos_tested': len(all_combos),
                    'hybrid_combos_tested': len([c for c in all_combos if c.get('hybrid_filtering')]),
                    'backtest_period': backtest_period
                }
                
                # 백테스트 세션 기록 (새로운 메서드 필요)
                if hasattr(monitor, 'record_backtest_session'):
                    monitor.record_backtest_session(backtest_metrics)
                
            except Exception as monitor_error:
                logger.warning(f"⚠️ 성능 모니터링 연동 중 오류: {monitor_error}")
            
            # 4. 기존 전략 리포트 생성
            logger.info("📄 전략별 성과 리포트 생성")
            generate_strategy_report(period_days=30, output_path='strategy_report.csv', send_email=True)
            
            # 🔧 [개선] 백테스트 분석 리포트 생성 추가
            logger.info("📊 백테스트 분석 리포트 생성")
            try:
                if hasattr(backtest_manager, 'generate_backtest_analysis_report'):
                    # 마크다운 리포트 생성
                    markdown_report = backtest_manager.generate_backtest_analysis_report(
                        session_id=session_id, 
                        output_format="markdown"
                    )
                    
                    if markdown_report and not markdown_report.startswith("⚠️"):
                        # 리포트 파일 저장
                        report_filename = f"backtest_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
                        with open(report_filename, 'w', encoding='utf-8') as f:
                            f.write(markdown_report)
                        logger.info(f"✅ 백테스트 분석 리포트 생성 완료: {report_filename}")
                        
                        # HTML 리포트도 생성
                        html_report = backtest_manager.generate_backtest_analysis_report(
                            session_id=session_id, 
                            output_format="html"
                        )
                        if html_report and not html_report.startswith("⚠️"):
                            html_filename = f"backtest_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                            with open(html_filename, 'w', encoding='utf-8') as f:
                                f.write(html_report)
                            logger.info(f"✅ HTML 리포트 생성 완료: {html_filename}")
                    else:
                        logger.warning("⚠️ 백테스트 분석 리포트 생성 실패 또는 데이터 없음")
                else:
                    logger.warning("⚠️ generate_backtest_analysis_report 메서드가 없습니다")
                    
            except Exception as report_error:
                logger.warning(f"⚠️ 백테스트 분석 리포트 생성 중 오류: {report_error}")
            
            # 5. 리포트 기반 자동 튜닝 실행
            logger.info('🔧 전략별 Kelly fraction 자동 튜닝 실행')
            try:
                from strategy_tuner import auto_tune_strategies
                auto_tune_strategies(report_path='strategy_report.csv', config_path='config/strategy.yaml')
            except ImportError:
                logger.warning("⚠️ strategy_tuner 모듈을 찾을 수 없어 자동 튜닝을 건너뜁니다.")
            
            # 🔧 [개선] 백테스트 결과 검증
            logger.info("🔍 백테스트 결과 검증 중...")
            try:
                # 최신 백테스트 세션 확인
                from backtester import BacktestDataManager
                backtest_manager = BacktestDataManager()
                
                # 최신 세션 정보 조회
                latest_session = backtest_manager._get_latest_session_id()
                if latest_session:
                    session_info = backtest_manager.get_session_info(latest_session)
                    if session_info:
                        logger.info(f"✅ 최신 백테스트 세션 확인: {latest_session}")
                        logger.info(f"   - 세션명: {session_info.get('name', 'N/A')}")
                        logger.info(f"   - 기간: {session_info.get('period_start', 'N/A')} ~ {session_info.get('period_end', 'N/A')}")
                        logger.info(f"   - 생성일: {session_info.get('created_at', 'N/A')}")
                    else:
                        logger.warning("⚠️ 세션 정보 조회 실패")
                else:
                    logger.warning("⚠️ 최신 백테스트 세션을 찾을 수 없습니다")
                    
            except Exception as verify_error:
                logger.warning(f"⚠️ 백테스트 결과 검증 중 오류: {verify_error}")
            
            logger.info("✅ 확장된 백테스트 및 리포트 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f'❌ 확장된 백테스트/리포트 생성 중 오류: {e}')
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return False

    def _auto_sync_blacklist_with_is_active(self):
        """
        일관성 저하 시 자동 동기화 수행
        
        블랙리스트와 is_active 컬럼 간의 불일치를 해결하기 위해
        블랙리스트를 기준으로 is_active 컬럼을 업데이트합니다.
        
        동기화 로직:
        1. 모든 티커를 일단 활성화 (is_active = true)
        2. 블랙리스트에 있는 티커들을 비활성화 (is_active = false)
        3. 동기화 결과 검증 및 로깅
        
        Returns:
            bool: 동기화 성공 여부
        """
        try:
            logger.info("🔄 블랙리스트-is_active 자동 동기화 시작...")
            
            blacklist = load_blacklist()
            if not blacklist:
                logger.warning("⚠️ 블랙리스트가 비어있거나 로드 실패")
                return False
            
            # 동기화 전 상태 확인
            pre_sync_query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN is_active = true THEN 1 END) as active_before,
                    COUNT(CASE WHEN is_active = false THEN 1 END) as inactive_before
                FROM tickers
            """
            pre_sync_result = self.db_mgr.execute_query(pre_sync_query)
            if pre_sync_result:
                total, active_before, inactive_before = pre_sync_result[0]
                logger.info(f"📊 동기화 전 상태: 총 {total}개 (활성 {active_before}개, 비활성 {inactive_before}개)")
            
            # 1단계: 모든 티커를 일단 활성화
            logger.info("1️⃣ 모든 티커 활성화 중...")
            update_query = "UPDATE tickers SET is_active = true"
            update_result = self.db_mgr.execute_query(update_query)
            
            # 2단계: 블랙리스트 티커들을 비활성화
            blacklisted_tickers = list(blacklist.keys())
            logger.info(f"2️⃣ 블랙리스트 티커 비활성화 중: {len(blacklisted_tickers)}개")
            
            if blacklisted_tickers:
                placeholders = ','.join(['%s'] * len(blacklisted_tickers))
                deactivate_query = f"""
                    UPDATE tickers 
                    SET is_active = false 
                    WHERE ticker IN ({placeholders})
                """
                deactivate_result = self.db_mgr.execute_query(deactivate_query, blacklisted_tickers)
                
                # 실제로 업데이트된 티커 확인
                verify_query = f"""
                    SELECT ticker FROM tickers 
                    WHERE ticker IN ({placeholders}) AND is_active = false
                """
                verify_result = self.db_mgr.execute_query(verify_query, blacklisted_tickers)
                updated_tickers = [row[0] for row in verify_result] if verify_result else []
                
                logger.info(f"   - 블랙리스트 대상: {len(blacklisted_tickers)}개")
                logger.info(f"   - 실제 비활성화: {len(updated_tickers)}개")
                
                if len(updated_tickers) != len(blacklisted_tickers):
                    missing_tickers = set(blacklisted_tickers) - set(updated_tickers)
                    logger.warning(f"   ⚠️ 비활성화 실패 티커: {missing_tickers}")
            
            # 동기화 후 상태 확인
            post_sync_result = self.db_mgr.execute_query(pre_sync_query)
            if post_sync_result:
                total, active_after, inactive_after = post_sync_result[0]
                logger.info(f"📊 동기화 후 상태: 총 {total}개 (활성 {active_after}개, 비활성 {inactive_after}개)")
                
                # 변화량 계산
                active_change = active_after - active_before
                inactive_change = inactive_after - inactive_before
                logger.info(f"📈 변화량: 활성 {active_change:+d}개, 비활성 {inactive_change:+d}개")
            
            logger.info(f"✅ 자동 동기화 완료: {len(blacklisted_tickers)}개 티커 비활성화")
            return True
            
        except Exception as e:
            logger.error(f"❌ 자동 동기화 실패: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return False

    def _validate_active_status_only(self, ticker_list):
        """
        이미 필터링된 티커들의 활성 상태만 검증 (역증가 방지)
        + 성능 모니터링 로직 추가
        
        Args:
            ticker_list: 이미 거래대금/기술적 분석으로 필터링된 티커 리스트
            
        Returns:
            list: 활성 상태이면서 블랙리스트에 없는 티커들만 반환
        """
        import time
        start_time = time.time()
        
        if not ticker_list:
            return []
        
        try:
            # is_active 컬럼 활용 검증
            placeholders = ','.join(['%s'] * len(ticker_list))
            active_query = f"""
                SELECT ticker FROM tickers 
                WHERE ticker IN ({placeholders}) AND is_active = true
            """
            
            active_result = self.db_mgr.execute_query(active_query, ticker_list)
            active_tickers = [row[0] for row in active_result] if active_result else []
            
            # 블랙리스트 추가 필터링
            blacklist = load_blacklist()
            final_tickers = [t for t in active_tickers if t not in blacklist]
            
            # 성능 메트릭스 계산
            processing_time = time.time() - start_time
            efficiency = len(final_tickers) / len(ticker_list) if ticker_list else 0
            throughput = len(ticker_list) / processing_time if processing_time > 0 else 0
            
            # 기본 검증 로깅
            logger.info(f"🔍 티커 활성 상태 검증:")
            logger.info(f"   - 입력 티커: {len(ticker_list)}개")
            logger.info(f"   - 활성 티커: {len(active_tickers)}개") 
            logger.info(f"   - 블랙리스트 제외 후: {len(final_tickers)}개")
            
            # 성능 메트릭스 로깅
            logger.info(f"⚡ 성능 메트릭스:")
            logger.info(f"   - 처리 시간: {processing_time:.3f}초")
            logger.info(f"   - 필터링 효율: {efficiency:.2%}")
            logger.info(f"   - 처리 속도: {throughput:.0f} 티커/초")
            
            # 성능 임계치 확인 및 경고
            if processing_time > 0.1:  # 100ms 초과
                logger.warning(f"⚠️ 처리 시간 초과: {processing_time:.3f}초 (임계치: 0.1초)")
            
            if efficiency < 0.8:  # 효율 80% 미만
                logger.warning(f"⚠️ 필터링 효율 저하: {efficiency:.2%} (임계치: 80%)")
            
            return final_tickers
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"❌ 활성 상태 검증 실패 (소요시간: {processing_time:.3f}초): {e}")
            
            # 폴백: 블랙리스트만 적용
            try:
                blacklist = load_blacklist()
                filtered_tickers = [t for t in ticker_list if t not in blacklist]
                fallback_time = time.time() - start_time
                logger.warning(f"🔄 폴백 필터링 (블랙리스트만): {len(filtered_tickers)}개 (소요시간: {fallback_time:.3f}초)")
                return filtered_tickers
            except:
                total_time = time.time() - start_time
                logger.error(f"❌ 폴백 필터링도 실패 (총 소요시간: {total_time:.3f}초)")
                return ticker_list

    def filter_comprehensive_indicators(self, market_df=None, timeframe='1d'):
        """
        수정된 하이브리드 필터링: 이미 필터링된 데이터 기준으로 활성 티커 검증
        
        정적+동적 지표를 조합한 하이브리드 필터링으로 돌파 매매 후보를 선별합니다.
        
        데이터 소스:
        - static_indicators: 정적 지표 (resistance, support, atr, adx, price, high_60 등)
        - ohlcv: 동적 지표 (rsi_14, macd_histogram, bb_upper, bb_lower, volume_20ma 등)
        
        수정된 논리적 순서:
        1. static_indicators에서 이미 필터링된 티커 조회 (거래대금 필터링 완료)
        2. 이 티커들의 활성 상태만 검증 (is_active=true 확인)
        3. 블랙리스트 제외하여 최종 대상 확정
        4. 검증된 티커들로만 하이브리드 쿼리 수행
        
        Args:
            market_df (pd.DataFrame, optional): 기존 시장 데이터 (호환성 유지용)
            timeframe (str): 시간 프레임 ('1d', '4h' 등)

        Returns:
            pd.DataFrame: 하이브리드 필터링을 통과한 종목 데이터 (데이터 소스 정보 포함)
        """
        try:
            import time
            from datetime import datetime
            self._filter_start_time = time.time()  # 성능 측정을 위한 시작 시간 기록
            
            logger.info("🔍 수정된 하이브리드 필터링 (정적+동적 지표) 시작...")

            # 필터링 설정 로드 (YAML 파일에서)
            from filter_tickers import load_filter_config
            config = load_filter_config("config/filter_rules_config.yaml")
            
            # 사용자 정의 설정이 있으면 mode 키를 보존하며 병합
            if hasattr(self, 'config') and self.config.get('filter'):
                user_config = self.config.get('filter', {})
                original_mode = config.get('mode')
                config.update(user_config)
                if 'mode' not in user_config and original_mode is not None:
                    config['mode'] = original_mode

            # 하이브리드 필터링 활성화 확인
            enable_hybrid = config.get('enable_hybrid_filtering', True)
            if not enable_hybrid:
                logger.info("하이브리드 필터링이 비활성화됨, 기존 방식 사용")
                if market_df is None or market_df.empty:
                    logger.warning("⚠️ 시장 데이터가 비어있습니다.")
                    return pd.DataFrame()
                from filter_tickers import filter_breakout_candidates
                return filter_breakout_candidates(market_df, config)

            # === 1단계: 이미 필터링된 시장 데이터 기준 ===
            if market_df is None or market_df.empty:
                # static_indicators에서 필터링된 티커 조회 (거래대금 필터링 완료)
                static_result = self.db_mgr.execute_query("""
                    SELECT ticker FROM static_indicators 
                    WHERE price > 0 AND high_60 > 0
                """)
                if not static_result:
                    logger.warning("⚠️ 필터링된 static 데이터 없음")
                    return pd.DataFrame()
                
                pre_filtered_tickers = [row[0] for row in static_result]
                logger.info(f"📊 사전 필터링 완료된 티커: {len(pre_filtered_tickers)}개")
            else:
                pre_filtered_tickers = market_df.index.tolist()
                logger.info(f"📊 market_df 기준 티커: {len(pre_filtered_tickers)}개")
            
            # === 2단계: 사전 필터링된 티커들의 활성 상태만 검증 ===
            validated_tickers = self._validate_active_status_only(pre_filtered_tickers)
            logger.info(f"📊 활성 상태 검증 후: {len(validated_tickers)}개 티커")
            
            # === 자동 동기화 트리거 조건 검사 ===
            if len(validated_tickers) < len(pre_filtered_tickers) * 0.7:  # 30% 이상 손실시
                loss_rate = (len(pre_filtered_tickers) - len(validated_tickers)) / len(pre_filtered_tickers)
                logger.warning(f"📉 필터링 효율 저하 감지: {loss_rate:.1%} 손실 (임계치: 30%)")
                logger.warning("🔄 자동 동기화 트리거 조건 충족, 동기화 시도...")
                
                sync_result = self._auto_sync_blacklist_with_is_active()
                if sync_result:
                    logger.info("✅ 자동 동기화 성공, 재검증 수행")
                    # 동기화 후 재검증
                    revalidated_tickers = self._validate_active_status_only(pre_filtered_tickers)
                    
                    # 개선 효과 확인
                    improvement = len(revalidated_tickers) - len(validated_tickers)
                    if improvement > 0:
                        logger.info(f"📈 동기화 효과: {improvement}개 티커 추가 확보 ({len(revalidated_tickers)}개)")
                        validated_tickers = revalidated_tickers
                    else:
                        logger.warning(f"⚠️ 동기화 후에도 개선 없음: {len(revalidated_tickers)}개")
                else:
                    logger.error("❌ 자동 동기화 실패, 기존 결과 유지")
            else:
                efficiency_rate = len(validated_tickers) / len(pre_filtered_tickers)
                logger.info(f"✅ 필터링 효율 양호: {efficiency_rate:.1%} (임계치: 70%)")
            
            # === 3단계: 검증된 티커들로만 하이브리드 쿼리 수행 ===
            if not validated_tickers:
                logger.warning("⚠️ 활성 상태 검증 통과 티커 없음")
                return pd.DataFrame()

            # 성능 최적화된 단일 쿼리로 정적+동적 지표 조회 (검증된 티커만 대상)
            hybrid_query = """
                SELECT 
                    s.ticker, s.price, s.high_60, s.low_60, s.resistance, s.support, 
                    s.atr, s.adx, s.updated_at,
                    o.rsi_14, o.macd_histogram, o.bb_upper, o.bb_lower, 
                    o.volume_20ma, o.stoch_k, o.current_close, o.ma_50, o.ma_200
                FROM static_indicators s
                LEFT JOIN (
                    SELECT DISTINCT ON (ticker) 
                           ticker, rsi_14, macd_histogram, bb_upper, bb_lower,
                           volume_20ma, stoch_k, close as current_close, ma_50, ma_200
                    FROM ohlcv 
                    ORDER BY ticker, date DESC
                ) o ON s.ticker = o.ticker
                WHERE s.ticker = ANY(%s)
            """
            
            hybrid_result = self.db_mgr.execute_query(hybrid_query, (validated_tickers,))
            
            if not hybrid_result:
                logger.warning("⚠️ 하이브리드 데이터 조회 실패")
                return pd.DataFrame()
            
            # 하이브리드 DataFrame 생성 (단일 쿼리 결과)
            combined_df = pd.DataFrame(hybrid_result, columns=[
                'ticker', 'price', 'high_60', 'low_60', 'resistance', 'support', 
                'atr', 'adx', 'updated_at',
                'rsi_14', 'macd_histogram', 'bb_upper', 'bb_lower', 
                'volume_20ma', 'stoch_k', 'current_close', 'ma_50', 'ma_200'
            ])
            combined_df.set_index('ticker', inplace=True)
            
            # 데이터 일관성 검증 (정적/동적 분리하여 검증)
            static_columns = ['price', 'high_60', 'low_60', 'resistance', 'support', 'atr', 'adx', 'updated_at']
            dynamic_columns = ['rsi_14', 'macd_histogram', 'bb_upper', 'bb_lower', 'volume_20ma', 'stoch_k', 'current_close', 'ma_50', 'ma_200']
            
            static_df = combined_df[static_columns].copy()
            dynamic_df = combined_df[dynamic_columns].copy()
            
            # 동적 데이터가 있는 티커만 추출 (NULL이 아닌 경우)
            has_dynamic_data = combined_df[dynamic_columns].notna().any(axis=1)
            dynamic_df = dynamic_df[has_dynamic_data]
            
            from filter_tickers import validate_data_consistency
            validation_result = validate_data_consistency(static_df, dynamic_df)
            
            if not validation_result['is_valid']:
                logger.error("❌ 데이터 일관성 검증 실패, 필터링 중단")
                return pd.DataFrame()
            
            logger.info(f"✅ 하이브리드 데이터 로드 완료: {len(combined_df)}개 종목 (검증된 티커로만 쿼리)")
            logger.info(f"📊 동적 데이터 보유: {has_dynamic_data.sum()}개 종목 ({has_dynamic_data.sum()/len(combined_df)*100:.1f}%)")

            # 하이브리드 필터링 적용
            from filter_tickers import filter_comprehensive_candidates
            filtered_df = filter_comprehensive_candidates(combined_df, config)

            # 결과 로깅
            if filtered_df.empty:
                logger.warning("⚠️ 돌파 매매 조건을 만족하는 종목이 없습니다.")
                return pd.DataFrame()

            logger.info(f"✅ {len(filtered_df)} breakout candidates selected out of {len(combined_df)}")
            logger.info(f"📊 선별된 돌파 후보: {', '.join(filtered_df.index.tolist())}")
            
            # 상세 정보 디버그 로깅
            if logger.isEnabledFor(logging.DEBUG):
                try:
                    debug_cols = ['price', 'ma_200', 'high_60', 'optional_score']
                    available_cols = [col for col in debug_cols if col in filtered_df.columns]
                    if available_cols:
                        logger.debug(f"돌파 후보 상세 정보:\n{filtered_df[available_cols]}")
                    
                    # 보조 조건 상세 정보
                    if 'optional_details' in filtered_df.columns:
                        for ticker in filtered_df.index:
                            details = filtered_df.loc[ticker, 'optional_details']
                            score = filtered_df.loc[ticker, 'optional_score']
                            logger.debug(f"✨ {ticker}: 점수 {score}, 조건 [{details}]")
                except Exception as e:
                    logger.debug(f"상세 정보 로깅 중 오류: {e}")

            logger.info(f"🎯 돌파 매매 필터링 완료: {len(filtered_df)}개 종목 선별")
            
            # 성능 모니터링 및 알림 시스템 통합
            try:
                import time
                end_time = time.time()
                processing_time = getattr(self, '_filter_start_time', end_time) and (end_time - self._filter_start_time) or 0
                
                # 성능 메트릭스 수집
                session_metrics = {
                    'total_tickers': len(pre_filtered_tickers),
                    'validated_tickers': len(validated_tickers),
                    'filtered_tickers': len(filtered_df),
                    'processing_time': processing_time,
                    'hybrid_mode_count': has_dynamic_data.sum() if 'has_dynamic_data' in locals() else 0,
                    'static_only_count': len(combined_df) - (has_dynamic_data.sum() if 'has_dynamic_data' in locals() else 0),
                    'data_quality_score': validation_result.get('quality_score', 1.0) if 'validation_result' in locals() else 1.0,
                    'static_weight': config.get('static_weight', 0.6),
                    'dynamic_weight': config.get('dynamic_weight', 0.4),
                    'filter_config': config,
                    'error_count': 0,
                    'session_id': f"filter_{safe_strftime(datetime.now(), '%Y%m%d_%H%M%S')}"
                }
                
                # 성능 모니터링 기록
                from performance_monitor import get_performance_monitor
                monitor = get_performance_monitor()
                performance_result = monitor.record_filtering_session(session_metrics)
                
                # 알림 시스템 검사
                from alert_system import get_alert_system
                alert_system = get_alert_system()
                alerts = alert_system.check_and_send_alerts(session_metrics)
                
                if alerts:
                    logger.info(f"📢 {len(alerts)}개 알림 발송됨")
                    
            except Exception as monitor_error:
                logger.warning(f"⚠️ 성능 모니터링 중 오류: {monitor_error}")
            
            return filtered_df

        except Exception as e:
            logger.error(f"❌ 돌파 매매 필터링 중 오류 발생: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            
            # 오류 발생 시에도 알림 시스템에 오류 상황 전달
            try:
                from alert_system import get_alert_system
                alert_system = get_alert_system()
                error_metrics = {
                    'total_tickers': len(pre_filtered_tickers) if 'pre_filtered_tickers' in locals() else 0,
                    'validated_tickers': 0,
                    'filtered_tickers': 0,
                    'processing_time': 0,
                    'data_quality_score': 0.0,
                    'efficiency': 0.0,
                    'error_count': 1,
                    'session_id': f"error_{safe_strftime(datetime.now(), '%Y%m%d_%H%M%S')}"
                }
                alert_system.check_and_send_alerts(error_metrics)
            except:
                pass  # 알림 시스템 오류는 무시
                
            return pd.DataFrame()

    def process_4h_for_candidates(self, candidates_1d): # 4시간봉 필터링을 통한 마켓타이밍
        """
        1차 필터링 통과 종목들에 대해 4시간봉 데이터를 수집하고 마켓타이밍 필터링을 적용합니다.
        
        Args:
            candidates_1d (list): 일봉 기준 필터링 통과 종목 리스트
            
        Returns:
            list: 4시간봉 필터링까지 통과한 최종 종목 리스트
        """
        try:
            # 4시간봉 데이터 수집 시작
            logger.info("📊 4시간봉 데이터 수집 및 마켓타이밍 필터링 시작")
            
            # 입력 데이터 검증 강화
            if not candidates_1d:
                logger.warning("⚠️ 1차 필터링 통과 종목이 없습니다.")
                return []
            
            # 입력 데이터 타입 검증
            if not isinstance(candidates_1d, list):
                logger.error(f"❌ candidates_1d는 list 타입이어야 합니다. 현재 타입: {type(candidates_1d)}")
                return []
            
            # 티커 형식 검증
            valid_tickers = []
            for ticker in candidates_1d:
                if isinstance(ticker, str) and ticker.startswith('KRW-'):
                    valid_tickers.append(ticker)
                else:
                    logger.warning(f"⚠️ 잘못된 티커 형식: {ticker} (타입: {type(ticker)})")
            
            if not valid_tickers:
                logger.error("❌ 유효한 티커가 없습니다.")
                return []
            
            candidates_1d = valid_tickers
            logger.info(f"📋 유효한 티커 {len(candidates_1d)}개 확인됨")
            
            # 1. 4시간봉 데이터 수집
            try:
                from data_fetcher import get_ohlcv_4h, calculate_technical_indicators_4h, save_market_data_4h_to_db
            except ImportError as e:
                logger.error(f"❌ 필수 모듈 임포트 실패: {e}")
                return []
            
            market_data_4h = {}
            processing_errors = []
            
            logger.info(f"🔍 4시간봉 데이터 수집 대상: {len(candidates_1d)}개 종목")
            
            for i, ticker in enumerate(candidates_1d):
                try:
                    # 티커별 처리 시작
                    logger.debug(f"🔄 [{i+1}/{len(candidates_1d)}] {ticker} 4시간봉 데이터 처리 시작")
                    
                    # 🔧 [핵심 수정] 매수 후보가 선정된 종목은 강제로 데이터 수집
                    df_4h = get_ohlcv_4h(ticker, limit=250, force_fetch=True)
                    
                    if df_4h is None or df_4h.empty:
                        logger.warning(f"⚠️ {ticker} 4시간봉 데이터 수집 실패 (강제 수집 시도 후)")
                        processing_errors.append(f"{ticker}: 강제 수집 실패")
                        continue
                    
                    # 🔧 [핵심 수정] 데이터 품질 검증 강화
                    min_required_data = 50  # MA200 계산을 위해 최소 50개 필요
                    if len(df_4h) < min_required_data:
                        logger.warning(f"⚠️ {ticker} 4시간봉 데이터 부족 ({len(df_4h)}개 < {min_required_data}개)")
                        processing_errors.append(f"{ticker}: 데이터 부족")
                        continue
                    
                    # 2. 4시간봉 기술적 지표 계산
                    df_4h_with_indicators = calculate_technical_indicators_4h(df_4h)
                    
                    if df_4h_with_indicators is None or df_4h_with_indicators.empty:
                        logger.warning(f"⚠️ {ticker} 4시간봉 지표 계산 실패")
                        processing_errors.append(f"{ticker}: 지표 계산 실패")
                        continue
                    
                    # 지표 데이터 품질 검증
                    required_indicators = ['rsi_14', 'macd', 'bb_upper', 'bb_lower']
                    missing_indicators = [ind for ind in required_indicators if ind not in df_4h_with_indicators.columns]
                    
                    if missing_indicators:
                        logger.warning(f"⚠️ {ticker} 필수 지표 누락: {missing_indicators}")
                        # 누락된 지표가 있어도 계속 진행 (경고만)
                    
                    # 3. 4시간봉 OHLCV 데이터를 DB에 저장
                    try:
                        from data_fetcher import save_ohlcv_4h_to_db
                        save_ohlcv_4h_to_db(ticker, df_4h)
                        logger.debug(f"💾 {ticker} 4시간봉 OHLCV DB 저장 완료")
                    except Exception as save_e:
                        logger.error(f"❌ {ticker} 4시간봉 OHLCV DB 저장 실패: {save_e}")
                        # DB 저장 실패해도 메모리에는 저장하여 분석 진행
                    
                    # 4. 4시간봉 마켓타이밍 지표를 DB에 저장
                    try:
                        save_market_data_4h_to_db(ticker, df_4h_with_indicators)
                        logger.debug(f"💾 {ticker} 4시간봉 마켓타이밍 지표 DB 저장 완료")
                    except Exception as save_e:
                        logger.error(f"❌ {ticker} 4시간봉 마켓타이밍 지표 DB 저장 실패: {save_e}")
                        # DB 저장 실패해도 메모리에는 저장하여 분석 진행
                    
                    # 5. 메모리에 저장 (이후 필터링에 사용)
                    # 🔧 [핵심 수정] 중복 저장 방지
                    if ticker not in market_data_4h:
                        market_data_4h[ticker] = df_4h_with_indicators
                        logger.debug(f"💾 {ticker} 4시간봉 메모리 저장 완료")
                    else:
                        logger.warning(f"⚠️ {ticker} 이미 메모리에 저장되어 있음 (중복 방지)")
                    
                    logger.info(f"✅ [{i+1}/{len(candidates_1d)}] {ticker} 4시간봉 데이터 처리 완료 ({len(df_4h_with_indicators)}개 레코드)")
                    
                except Exception as e:
                    error_msg = f"{ticker} 4시간봉 데이터 처리 중 오류: {str(e)}"
                    logger.error(f"❌ {error_msg}")
                    processing_errors.append(error_msg)
                    
                    # 개별 티커 실패 시 상세 디버깅 정보
                    import traceback
                    logger.debug(f"🔍 {ticker} 상세 오류:\n{traceback.format_exc()}")
                    continue
            
            # 5. 4시간봉 데이터 수집 결과 확인
            success_count = len(market_data_4h)
            total_count = len(candidates_1d)
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            
            logger.info(f"📊 4시간봉 데이터 수집 완료: {success_count}/{total_count} 종목 성공 ({success_rate:.1f}%)")
            
            if processing_errors:
                logger.warning(f"⚠️ 처리 중 오류 발생: {len(processing_errors)}건")
                for error in processing_errors[:5]:  # 최대 5개만 표시
                    logger.warning(f"   - {error}")
                if len(processing_errors) > 5:
                    logger.warning(f"   - ... 외 {len(processing_errors) - 5}건 더")
            
            if not market_data_4h:
                logger.warning("⚠️ 4시간봉 데이터가 모두 없거나 처리 실패했습니다.")
                return []
            
            # 6. 4시간봉 타이밍 필터링 적용
            try:
                from filter_tickers import apply_timing_filter_4h
                
                timing_filter_config = self.config.get('timing_filter', {})
                if not timing_filter_config:
                    logger.warning("⚠️ 타이밍 필터 설정이 없습니다. 기본 설정 사용")
                    timing_filter_config = {'enabled': True}
                
                # dict를 DataFrame으로 변환
                if market_data_4h and isinstance(market_data_4h, dict):
                    # 각 티커의 최신 데이터만 추출하여 DataFrame 생성
                    latest_data = {}
                    for ticker, df in market_data_4h.items():
                        if df is not None and not df.empty:
                            latest_data[ticker] = df.iloc[-1]
                    
                    if latest_data:
                        market_df_4h = pd.DataFrame(latest_data).T  # Transpose to get tickers as index
                        final_candidates = apply_timing_filter_4h(market_df_4h, timing_filter_config)
                        
                    else:
                        logger.warning("⚠️ 4시간봉 최신 데이터가 없습니다.")
                        final_candidates = []
                else:
                    logger.warning("⚠️ 4시간봉 데이터가 dict 형태가 아닙니다.")
                    final_candidates = []
                
            except ImportError as e:
                logger.error(f"❌ 타이밍 필터 모듈 임포트 실패: {e}")
                # 필터링 실패 시 모든 데이터 처리 성공 종목 반환
                final_candidates = list(market_data_4h.keys())
                logger.warning(f"⚠️ 타이밍 필터링 우회, 처리 성공 종목 {len(final_candidates)}개 반환")
                
            except Exception as e:
                logger.error(f"❌ 타이밍 필터링 중 오류: {e}")
                # 필터링 실패 시 상위 3개 종목만 반환
                final_candidates = list(market_data_4h.keys())[:3]
                logger.warning(f"⚠️ 타이밍 필터링 실패, 상위 {len(final_candidates)}개 종목 반환")
            
            # 7. 결과 로깅
            logger.info(f"📊 4시간봉 마켓타이밍 필터링 결과:")
            logger.info(f"   - 입력: {len(candidates_1d)}개 종목")
            logger.info(f"   - 데이터 처리 성공: {len(market_data_4h)}개 종목")
            logger.info(f"   - 최종 통과: {len(final_candidates)}개 종목")
            if final_candidates:
                logger.info(f"   - 통과 종목: {final_candidates}")
            else:
                logger.warning("⚠️ 4시간봉 필터링 통과 종목이 없습니다.")
            
            # 8. 4시간봉 데이터 정리 (필터링 완료 후)
            # 🔧 [핵심 수정] 필터링 완료 후에만 데이터 정리
            if final_candidates:
                try:
                    logger.info("🧹 4시간봉 데이터 정리 시작 (필터링 성공)")
                    self._cleanup_4h_data()
                    logger.info("✅ 4시간봉 데이터 정리 완료")
                except Exception as cleanup_error:
                    logger.error(f"❌ 4시간봉 데이터 정리 중 오류 발생: {cleanup_error}")
            else:
                logger.info("📊 필터링 결과가 없어 4시간봉 데이터 정리를 건너뜁니다.")
            
            return final_candidates
            
        except Exception as e:
            logger.error(f"❌ 4시간봉 분석 중 전체 오류 발생: {str(e)}")
            import traceback
            logger.error(f"🔍 상세 오류:\n{traceback.format_exc()}")
            
            # 전체 실패 시 빈 리스트 반환
            return []

    def _cleanup_4h_data(self):
        """
        4시간봉 데이터 처리 완료 후 ohlcv_4h와 market_data_4h 테이블의 모든 레코드를 제거합니다.
        메모리 최적화와 데이터 일관성을 위해 필터링 완료 후 즉시 실행됩니다.
        """
        try:
            logger.info("🧹 4시간봉 데이터 정리 시작")
            
            # DB 연결
            with self.db_mgr.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    # 1. ohlcv_4h 테이블 정리
                    cursor.execute("DELETE FROM ohlcv_4h")
                    ohlcv_deleted_count = cursor.rowcount
                    
                    # 2. market_data_4h 테이블 정리
                    cursor.execute("DELETE FROM market_data_4h")
                    market_data_deleted_count = cursor.rowcount
                    
                    # 트랜잭션 커밋
                    conn.commit()
                    
                    logger.info(f"✅ 4시간봉 데이터 정리 완료:")
                    logger.info(f"   - ohlcv_4h: {ohlcv_deleted_count}개 레코드 삭제")
                    logger.info(f"   - market_data_4h: {market_data_deleted_count}개 레코드 삭제")
                    logger.info(f"   - 총 삭제: {ohlcv_deleted_count + market_data_deleted_count}개 레코드")
                    
        except Exception as e:
            logger.error(f"❌ 4시간봉 데이터 정리 중 오류 발생: {str(e)}")
            import traceback
            logger.error(f"🔍 상세 오류:\n{traceback.format_exc()}")
            raise

    def scan_and_filter_tickers(self) -> list:
        """
        DB에서 티커 로드, 블랙리스트 적용, 월봉 데이터 길이 필터, 거래대금 필터, 월봉 패턴 필터를 순서대로 적용합니다.
        Returns:
            list: 필터링된 티커 목록
        """
        try:
            logger.info("🔍 티커 스캔 및 필터링 시작")
            start_time = time.time()

            # DB에서 모든 티커 조회
            tickers_rows = self.db_mgr.execute_query("SELECT ticker FROM tickers")
            if not tickers_rows:
                logger.warning("⚠️ DB에서 티커 정보를 가져오지 못했습니다.")
                return []

            # 티커 리스트로 변환
            all_tickers = [row[0] for row in tickers_rows]
            logger.info(f"📊 DB에서 {len(all_tickers)}개 티커 로드됨 (소요시간: {time.time() - start_time:.2f}초)")

            # 블랙리스트 적용
            blacklist = load_blacklist()
            filtered_tickers = [ticker for ticker in all_tickers if ticker not in blacklist]
            logger.info(f"🚫 블랙리스트 적용 후 {len(filtered_tickers)}개 티커 남음 (제외: {len(all_tickers) - len(filtered_tickers)}개)")

            # 필터 순서: 1) 월봉 데이터 길이, 2) 거래대금 
            filter_start = time.time()
            from filter_tickers import filter_by_monthly_data_length, filter_by_volume

            # 1. 월봉 데이터 길이 필터 (최소 14개월)
            monthly_length_filtered = filter_by_monthly_data_length(filtered_tickers, min_months=14)
            logger.info(f"📅 월봉 데이터 길이(14개월) 필터 적용 후 {len(monthly_length_filtered)}개 티커 남음 (소요시간: {time.time() - filter_start:.2f}초)")

            # 2. 거래대금 필터
            filter_vol_start = time.time()
            volume_filtered = filter_by_volume(monthly_length_filtered)
            logger.info(f"💰 거래대금 필터 적용 후 {len(volume_filtered)}개 티커 남음 (소요시간: {time.time() - filter_vol_start:.2f}초)")

            # 결과 요약
            total_time = time.time() - start_time
            logger.info(f"✅ 티커 스캔 및 필터링 완료: 총 {len(all_tickers)}개 중 {len(volume_filtered)}개 선택 (총 소요시간: {total_time:.2f}초)")
            if len(volume_filtered) < 5:
                logger.warning(f"⚠️ 필터링 결과가 너무 적습니다 ({len(volume_filtered)}개). 필터 설정을 확인하세요.")
            return volume_filtered
        except Exception as e:
            logger.error(f"❌ 티커 스캔 및 필터링 중 오류 발생: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return []

    def process_daily_ohlcv_and_indicators(self, tickers: list) -> pd.DataFrame:
        """
        티커별 OHLCV 수집, 기술적 지표 계산, 차트 이미지 저장을 수행합니다.
        Args:
            tickers (list): 처리할 티커 목록
        Returns:
            pd.DataFrame: 최신 지표가 포함된 DataFrame
        """
        try:
            import pandas as pd
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import time

            start_time = time.time()

            if not tickers:
                logger.warning("⚠️ 처리할 티커 목록이 비어있습니다.")
                return pd.DataFrame()

            logger.info(f"📈 {len(tickers)}개 티커의 일봉 OHLCV 및 지표 처리 시작")

            # 결과를 저장할 DataFrame과 오류 추적
            result_df = pd.DataFrame()
            errors = []
            successful_tickers = []

            # 집합화하여 빠른 포함 체크 지원
            tickers_set = set(tickers)

            # [수정] 순차 처리에서 통합 처리로 변경
            logger.info("🔧 통합 처리: OHLCV 수집 → 지표 계산 → 저장을 한 번에 처리")

            # 병렬 처리를 위한 함수 정의
            def process_ticker(ticker):
                ticker_start = time.time()
                try:
                    # 필터 통과 티커만 데이터 업데이트
                    if ticker not in tickers_set:
                        logger.info(f"⏭️ {ticker}는 필터 통과 티커 목록에 없음. 처리 건너뜀.")
                        return None, ticker

                    # 1. OHLCV 데이터 수집 (기존 update_all_tickers_ohlcv 로직 통합)
                    from datetime import date
                    from data_fetcher import get_ohlcv_d, delete_old_ohlcv, save_all_indicators_atomically

                    # 최신 날짜 확인
                    try:
                        row = self.db_mgr.execute_query(
                            "SELECT MAX(date) FROM ohlcv WHERE ticker = %s", (ticker,), fetchone=True
                        )
                        last_date = row[0] if row and row[0] else None
                    except Exception as e:
                        logger.error(f"❌ {ticker} 가장 최신 OHLCV 날짜 조회 중 오류: {e}")
                        last_date = None

                    today = date.today()
                    
                    # 🚀 [핵심 수정] 데이터 수집 및 최신 데이터 강제 갱신 로직
                    if last_date is None:
                        # 전체 데이터 수집
                        df = get_ohlcv_d(ticker)
                        logger.info(f"🔄 {ticker} 전체 OHLCV 데이터 수집: {len(df) if df is not None else 0}개")
                    else:
                        days_diff = (today - last_date).days
                        if days_diff <= 0:
                            logger.info(f"✅ {ticker} DB 최신 상태이지만 오늘 데이터 실시간 갱신 확인")
                            
                            # 🚀 핵심 수정: 오늘 데이터는 항상 API에서 최신으로 가져오기
                            try:
                                from data_fetcher import get_ohlcv_d
                                today_df = get_ohlcv_d(ticker, interval='day', count=1, fetch_latest_only=True)
                                
                                if today_df is not None and not today_df.empty:
                                    # 기존 DB 데이터 + 최신 1일 데이터 결합
                                    db_df = self.db_mgr.fetch_ohlcv(ticker, days=449)  # 449일 + 오늘 1일 = 450일
                                    
                                    if not db_df.empty:
                                        # 중복 제거하고 병합
                                        import pandas as pd
                                        combined_df = pd.concat([db_df, today_df])
                                        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                                        df = combined_df.sort_index()
                                        logger.info(f"🔄 {ticker} 최신 데이터 갱신 완료: {len(df)}개 레코드 (DB: {len(db_df)}, 최신: {len(today_df)})")
                                    else:
                                        df = today_df
                                        logger.info(f"🔄 {ticker} 새로운 데이터로 시작: {len(df)}개 레코드")
                                else:
                                    # 최신 데이터 수집 실패 시 기존 방식 사용
                                    df = self.db_mgr.fetch_ohlcv(ticker, days=450)
                                    logger.warning(f"⚠️ {ticker} 최신 데이터 수집 실패, 기존 DB 데이터 사용: {len(df) if df is not None else 0}개")
                            except Exception as api_e:
                                # API 오류 시 기존 DB 데이터 사용
                                df = self.db_mgr.fetch_ohlcv(ticker, days=450)
                                logger.warning(f"⚠️ {ticker} API 오류로 기존 DB 데이터 사용: {str(api_e)}")
                        else:
                            # 증분 데이터 수집 (기존 로직 유지)
                            df = get_ohlcv_d(ticker, interval='1d', count=days_diff)
                            logger.info(f"🔄 {ticker} 증분 OHLCV 데이터 수집: {len(df) if df is not None else 0}개")

                    if df is None or df.empty:
                        logger.warning(f"⚠️ {ticker} OHLCV 데이터 수집 실패")
                        return None, ticker

                    # 2. 기술적 지표 계산
                    from data_fetcher import calculate_unified_indicators
                    indicators_df = calculate_unified_indicators(df, ticker)
                    if indicators_df is None or indicators_df.empty:
                        logger.warning(f"⚠️ {ticker} 지표 계산 실패")
                        return None, ticker

                    # 3. [핵심 수정] 통합 저장: OHLCV + 정적/동적 지표를 원자적으로 저장
                    save_result = save_all_indicators_atomically(ticker, indicators_df, timeframe='1d')
                    
                    if not save_result:
                        logger.error(f"❌ {ticker} 통합 저장 실패")
                        return None, ticker

                    # 4. 오래된 데이터 정리
                    delete_old_ohlcv(ticker)

                    # 5. 최신 데이터만 추출
                    latest_data = indicators_df.iloc[-1:].copy()
                    latest_data['ticker'] = ticker

                    ticker_duration = time.time() - ticker_start
                    logger.info(f"✅ {ticker} 통합 처리 완료 (소요시간: {ticker_duration:.2f}초)")
                    return latest_data, ticker

                except Exception as e:
                    error_msg = f"❌ {ticker} 통합 처리 중 오류: {e}"
                    logger.error(error_msg)
                    return None, ticker

            # 병렬 처리 실행 (최대 동시 실행 스레드 수 제한)
            max_workers = min(10, len(tickers))  # 최대 10개 스레드로 제한

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 모든 티커에 대한 작업 제출
                future_to_ticker = {executor.submit(process_ticker, ticker): ticker for ticker in tickers}

                # 작업 완료 시 결과 처리
                for i, future in enumerate(as_completed(future_to_ticker), 1):
                    ticker = future_to_ticker[future]
                    try:
                        data, processed_ticker = future.result()
                        if data is not None:
                            result_df = pd.concat([result_df, data])
                            successful_tickers.append(processed_ticker)
                        else:
                            errors.append(processed_ticker)
                    except Exception as e:
                        logger.error(f"❌ {ticker} 결과 처리 중 예외 발생: {e}")
                        errors.append(ticker)

                    # 진행상황 로깅 (10% 단위)
                    if i % max(1, len(tickers) // 10) == 0 or i == len(tickers):
                        progress = (i / len(tickers)) * 100
                        elapsed = time.time() - start_time
                        estimated_total = (elapsed / i) * len(tickers)
                        remaining = max(0, estimated_total - elapsed)
                        logger.info(f"⏳ 진행률: {progress:.1f}% ({i}/{len(tickers)}) - "
                                   f"경과: {elapsed:.1f}초, 예상 남은 시간: {remaining:.1f}초")

            if result_df.empty:
                logger.warning("⚠️ 모든 티커 처리 실패")
                return pd.DataFrame()

            # 인덱스 설정
            result_df.set_index('ticker', inplace=True)

            # Sequentially generate chart images for successful tickers (JSON 모드가 아닐 때만)
            if not self.use_json_instead_of_chart:
                for ticker in successful_tickers:
                    df = self.db_mgr.fetch_ohlcv(ticker, days=400)
                    self.generate_chart_image(ticker, df)

            # 처리 결과 요약
            total_duration = time.time() - start_time
            logger.info(f"✅ 통합 OHLCV 및 지표 처리 완료: {len(successful_tickers)}/{len(tickers)} 티커 성공 "
                        f"(소요시간: {total_duration:.1f}초, 평균: {total_duration/max(1, len(tickers)):.2f}초/티커)")

            # 오류 목록 로깅
            if errors:
                logger.warning(f"⚠️ 처리 실패한 티커 ({len(errors)}개): {errors[:20]}" +
                               (f" 외 {len(errors)-20}개" if len(errors) > 20 else ""))

            return result_df

        except Exception as e:
            logger.error(f"❌ 통합 OHLCV 및 지표 처리 중 오류 발생: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return pd.DataFrame()

    def analyze_4h_and_filter(self, candidates_1d: list) -> list:
        """
        4시간봉 지표 계산 및 필터를 적용하여 최종 후보를 선정합니다.
        
        Args:
            candidates_1d (list): 1일봉 필터링을 통과한 후보 티커 목록 [(티커, 점수), ...] 또는 [티커, ...]
        
        Returns:
            list: 4시간봉 필터링까지 통과한 최종 후보 티커 목록
        """
        try:
            if not candidates_1d:
                logger.warning("⚠️ 1일봉 필터링을 통과한 후보가 없습니다.")
                return []
            
            # 입력 형태에 따라 티커만 추출
            if isinstance(candidates_1d[0], tuple):
                # [(티커, 점수), ...] 형태
                tickers_only = [ticker for ticker, _ in candidates_1d]
            else:
                # [티커, ...] 형태
                tickers_only = candidates_1d
            
            logger.info(f"🔍 4시간봉 분석 및 필터링 시작 (대상: {len(tickers_only)}개 티커)")
            
            # 4시간봉 처리 및 필터링 함수 호출
            final_candidates = self.process_4h_for_candidates(tickers_only)
            
            logger.info(f"✅ 4시간봉 분석 및 필터링 완료: {len(final_candidates)}개 티커 최종 선정")
            if final_candidates:
                logger.info(f"📊 최종 선정 티커: {final_candidates}")
            
            return final_candidates
            
        except Exception as e:
            logger.error(f"❌ 4시간봉 분석 및 필터링 중 오류 발생: {e}")
            return []

    def trade_and_report(self, scored_tickers, market_df_updated, market_df_4h, gpt_json_data=None):
        """
        필터링, GPT 분석, 매매 실행, 포트폴리오 관리, 백테스트 실행 단계를 수행합니다.
        
        Args:
            scored_tickers (list): 점수가 매겨진 티커 목록 [(티커, 점수), ...]
            market_df_updated (pd.DataFrame): 업데이트된 마켓 데이터프레임
            market_df_4h (pd.DataFrame): 4시간봉 마켓 데이터프레임 (현재 사용하지 않음)
            
        Returns:
            bool: 실행 성공 여부
        """
        try:
            start_time = time.time()
            logger.info("💹 트레이딩 및 리포트 생성 시작")
            
            # 유효성 검사 - market_df_4h는 현재 사용하지 않으므로 검사에서 제외
            if not scored_tickers or market_df_updated is None or market_df_updated.empty:
                logger.warning("⚠️ 트레이딩 및 리포트 생성을 위한 데이터가 부족합니다.")
                return False
                
            # 진행 상황 추적
            step_results = {
                "돌파_후보_준비": False,
                "4시간봉_필터링": False,
                "GPT_분석_매매": False,
                "포트폴리오_업데이트": False,
                "매도_조건_점검": False,
                "백테스트_리포트": False
            }
            
            # 1. 돌파 후보 준비 (이미 필터링 완료됨)
            step_start = time.time()
            candidates_1d = scored_tickers  # 이미 1차 필터링이 완료된 후보들
            step_time = time.time() - step_start
            
            if not candidates_1d:
                logger.warning("⚠️ 돌파 매매 후보가 없습니다.")
                return False
            
            logger.info(f"✅ 1단계: 돌파 후보 준비 완료 - {len(candidates_1d)}개 후보 (소요시간: {step_time:.2f}초)")
            step_results["돌파_후보_준비"] = True
            
            # ✅ GPT 분석 결과 처리 (이미 분석 완료된 결과 사용)
            step_start_gpt = time.time()
            gpt_results = []  # GPT 분석 결과 수집용 리스트
            trade_logs = []  # 매수 이력 수집용 리스트 (전역 선언)
            
            if gpt_json_data and len(gpt_json_data) > 0:
                logger.info("🔄 기존 GPT 분석 결과 처리 중")
                
                # 이미 GPT 분석이 완료된 결과를 사용 (중복 분석 방지)
                gpt_results = gpt_json_data
            else:
                logger.warning("⚠️ GPT 분석 결과가 없어 기본값으로 진행합니다.")
                # GPT 분석 결과가 없는 경우 기본값 생성
                gpt_results = []
                for ticker, score in candidates_1d:
                    gpt_results.append({
                        "ticker": ticker,
                        "score": score,
                        "confidence": 0.5,
                        "action": "HOLD",
                        "market_phase": "Unknown",
                        "pattern": "",
                        "reason": "기본값",
                        "input_type": "fallback"
                    })
            
            # candidates_1d를 기존 형식으로 변환하여 하위 파이프라인 호환성 유지
            candidates_1d = [(result["ticker"], result["score"]) for result in gpt_results]
            
            # 중복 제거 (GPT 분석 결과 정리)
            gpt_results_df = pd.DataFrame(gpt_results)
            gpt_results_df = gpt_results_df.drop_duplicates(subset='ticker')
            gpt_results = gpt_results_df.to_dict(orient='records')
            logger.info(f"[중복 제거] GPT 분석 결과 중복 제거 후 티커 수: {len(gpt_results)}")
            
            # GPT 분석 결과를 score 기준으로 내림차순 정렬
            sorted_results = sorted(gpt_results, key=lambda x: safe_float_convert(x.get("score", 0), context="GPT분석 정렬"), reverse=True)
            
            # 상위 5개 종목 로그 출력
            logger.info("[GPT 분석 결과 상위 5개]")
            for i, result in enumerate(sorted_results[:5], 1):
                logger.info(f"{i}. {result['ticker']}: {result['score']}점 (confidence: {result['confidence']:.2f})")
            
            logger.info(f"✅ GPT 분석 결과 처리 완료 (소요시간: {time.time() - step_start_gpt:.2f}초)")
            
            # GPT 분석 결과 DB 저장 (라이프사이클 관리 적용)
            try:
                self.save_gpt_analysis_to_db(gpt_results)
                
                # 라이프사이클 관리자 통계 업데이트
                if hasattr(self, 'gpt_lifecycle_manager') and self.gpt_lifecycle_manager:
                    cleanup_stats = self.gpt_lifecycle_manager.get_cleanup_stats()
                    logger.info(f"📊 GPT 라이프사이클 통계: 총 정리 {cleanup_stats.get('total_cleaned', 0)}개")
                    
            except Exception as e:
                logger.warning(f"⚠️ GPT 분석 결과 저장 중 오류: {str(e)}")
            
            # GPT 분석 결과 기반 매수 조건 필터링 (매수 실행 제거)
            logger.info("🔍 매수 조건 필터링 시작")
            buy_candidates = []
            excluded_candidates = []
            
            for result in gpt_results:
                try:
                    # 데이터 유효성 검증 강화
                    ticker = result.get("ticker", "")
                    if not ticker:
                        logger.warning(f"⚠️ 티커 정보 누락: {result}")
                        excluded_candidates.append(result)
                        continue
                        
                    score = safe_float_convert(result.get("score", 0), context=f"GPT분석 {ticker} score")
                    confidence = safe_float_convert(result.get("confidence", 0), context=f"GPT분석 {ticker} confidence")
                    
                    # 점수와 신뢰도 범위 검증
                    if not (0 <= score <= 100):
                        logger.warning(f"⚠️ {ticker} 점수 범위 오류: {score} (0-100 범위 초과)")
                        excluded_candidates.append(result)
                        continue
                        
                    if not (0 <= confidence <= 1):
                        logger.warning(f"⚠️ {ticker} 신뢰도 범위 오류: {confidence} (0-1 범위 초과)")
                        excluded_candidates.append(result)
                        continue
                    
                    # 🔧 [핵심 수정] action 필드 타입 안전성 보장
                    action_raw = result.get("action", "AVOID")
                    
                    # action 필드 타입 검증 및 변환
                    if isinstance(action_raw, (int, float)):
                        logger.warning(f"⚠️ {ticker} action 필드가 숫자 타입입니다: {action_raw} ({type(action_raw)}) → 'HOLD'로 변환")
                        action = "HOLD"
                    elif isinstance(action_raw, str):
                        action = action_raw.upper().strip()
                    else:
                        logger.warning(f"⚠️ {ticker} action 필드가 예상치 못한 타입입니다: {action_raw} ({type(action_raw)}) → 'AVOID'로 변환")
                        action = "AVOID"
                    
                    # action 값 유효성 검증
                    valid_actions = ['BUY', 'STRONG_BUY', 'BUY_WEAK', 'SELL', 'STRONG_SELL', 'SELL_WEAK', 'HOLD', 'AVOID', 'NEUTRAL', 'WAIT']
                    if action not in valid_actions:
                        logger.warning(f"⚠️ {ticker} 유효하지 않은 action 값: {action} → 'HOLD'로 변환")
                        action = "HOLD"
                    
                    # 설정 기반 엄격한 매수 조건 적용
                    try:
                        from config import GPT_FILTERING_CONFIG
                        strict_config = GPT_FILTERING_CONFIG['strict_mode']
                    except ImportError:
                        # fallback 설정
                        strict_config = {
                            'min_score': 80,
                            'min_confidence': 0.9,
                            'allowed_actions': ['BUY', 'STRONG_BUY'],
                            'allowed_market_phases': ['Stage1', 'Stage2']
                        }
                    
                    if (score >= strict_config['min_score'] and 
                        confidence >= strict_config['min_confidence'] and 
                        action in strict_config['allowed_actions'] and 
                        result.get("market_phase", "") in strict_config['allowed_market_phases']):
                        buy_candidates.append(result)
                        logger.info(f"✅ 매수 후보 선정: {ticker} | 점수: {score} | 신뢰도: {confidence:.2f} | 액션: {action}")
                    else:
                        excluded_candidates.append(result)
                        logger.info(f"❌ 제외됨: {ticker} | 점수: {score} | 신뢰도: {confidence:.2f} | 액션: {action}")
                        
                except (ValueError, TypeError) as e:
                    logger.error(f"❌ 데이터 타입 오류: {result.get('ticker', 'Unknown')} | 오류: {str(e)}")
                    excluded_candidates.append(result)
            
            logger.info(f"✅ 매수 후보 {len(buy_candidates)}개, 제외된 종목 {len(excluded_candidates)}개")
            
            # 상세한 필터링 결과 로그
            if buy_candidates:
                logger.info("🎯 최종 매수 후보 목록:")
                for candidate in buy_candidates:
                    logger.info(f"   - {candidate['ticker']}: 점수 {candidate['score']}, 신뢰도 {candidate['confidence']:.2f}, 액션 {candidate.get('action', 'Unknown')}")
            else:
                logger.info("📊 엄격한 필터링으로 인해 매수 후보가 없습니다.")
                
            # 2. 4시간봉 분석 및 필터링 (매수 후보가 있을 때만 실행)
            step_start = time.time()
            
            # 🔧 [핵심 수정] 매수 후보가 없으면 4시간봉 처리 완전 건너뛰기
            if not buy_candidates:
                logger.info("📊 매수 후보가 0개이므로 4시간봉 분석 및 필터링을 건너뜁니다.")
                step_time = time.time() - step_start
                logger.info(f"✅ 2단계: 4시간봉 필터링 건너뜀 (소요시간: {step_time:.2f}초)")
                step_results["4시간봉_필터링"] = True  # 건너뛴 것도 성공으로 간주
                
                # 빈 결과로 후속 단계 진행
                passed_4h = []
                final_candidates = []
                
                # 4시간봉 데이터 정리 (건너뛰더라도 정리)
                try:
                    logger.info("🧹 4시간봉 데이터 정리 시작 (매수 후보 없음)")
                    self._cleanup_4h_data()
                    logger.info("✅ 4시간봉 데이터 정리 완료")
                except Exception as cleanup_error:
                    logger.error(f"❌ 4시간봉 데이터 정리 중 오류 발생: {cleanup_error}")
                
            else:
                # 매수 후보가 있을 때만 4시간봉 처리 실행
                candidates_1d = [(result["ticker"], result["score"]) for result in buy_candidates]
                passed_4h = self.analyze_4h_and_filter(candidates_1d)
                step_time = time.time() - step_start
                
                if not passed_4h:
                    logger.warning("⚠️ 4시간봉 필터링을 통과한 후보가 없습니다.")
                    # 🔧 [핵심 수정] 필터링 실패 시에도 데이터 정리 후 False 반환
                    try:
                        logger.info("🧹 4시간봉 데이터 정리 시작 (필터링 실패)")
                        self._cleanup_4h_data()
                        logger.info("✅ 4시간봉 데이터 정리 완료")
                    except Exception as cleanup_error:
                        logger.error(f"❌ 4시간봉 데이터 정리 중 오류 발생: {cleanup_error}")
                    
                    # 🔧 [핵심 수정] 필터링 실패 시에도 정상 종료로 처리
                    step_results["4시간봉_필터링"] = True  # 실패해도 단계 완료로 간주
                    return True  # False 대신 True 반환하여 파이프라인 계속 진행
                
                # GPT 점수를 유지한 채 교집합 추출
                final_candidates = [(t, s) for (t, s) in candidates_1d if t in passed_4h]
                
                logger.info(f"✅ 2단계: 4시간봉 필터링 완료 - {len(final_candidates)}개 후보 선정 (소요시간: {step_time:.2f}초)")
                step_results["4시간봉_필터링"] = True
                
            # 3. 최종 매수 실행 (4시간봉 필터링 통과한 종목만)
            step_start = time.time()
            trade_logs = []  # 매수 이력 수집용 리스트
            
            if final_candidates:
                logger.info("💰 최종 매수 실행 시작")
                
                # 🔧 [핵심 개선] 현재 보유 종목 조회 및 피라미딩 조건 점검
                current_positions = self.pm.get_current_positions()
                current_tickers = {pos['ticker'] for pos in current_positions}
                
                logger.info(f"📊 현재 보유 종목: {len(current_positions)}개")
                if current_tickers:
                    logger.info(f"   - 보유 종목: {', '.join(current_tickers)}")
                
                # 🔧 [3단계 개선] GPT 분석 결과에서 신뢰도 정보 추출
                gpt_confidence_map = {}
                if gpt_json_data:
                    for result in gpt_json_data:
                        ticker = result.get('ticker')
                        confidence = safe_float_convert(result.get('confidence', 0.5), context=f"GPT분석 {ticker} confidence")
                        gpt_confidence_map[ticker] = confidence
                
                # 🔧 [3단계 개선] 포트폴리오 기반 동적 매수 금액 계산
                total_balance = self.pm.get_total_balance()
                base_amount = min(100000, total_balance * 0.02)  # 최대 10만원 또는 총 자산의 2%
                
                for ticker, score in final_candidates:
                    try:
                        # 🔧 [핵심 개선] 보유 종목 확인 및 피라미딩 조건 점검
                        if ticker in current_tickers:
                            logger.info(f"🔄 {ticker} 이미 보유 중 - 피라미딩 조건 점검 시작")
                            
                            # 피라미딩 조건 점검
                            pyramid_result = self._check_pyramiding_for_existing_position(ticker, score, gpt_confidence_map.get(ticker, 0.5))
                            
                            logger.info(f"🔍 {ticker} 피라미딩 조건 점검 결과: should_pyramid={pyramid_result['should_pyramid']}")
                            logger.info(f"🔍 {ticker} 사유: {pyramid_result['reason']}")
                            
                            if pyramid_result['should_pyramid']:
                                logger.info(f"✅ {ticker} 피라미딩 조건 충족 - 추가 매수 진행")
                                
                                # 🔧 [디버깅 강화] 피라미딩 매수 실행 전 상태 로깅
                                logger.info(f"📊 {ticker} 피라미딩 매수 실행 전 trade_logs 개수: {len(trade_logs)}")
                                
                                # 피라미딩 매수 실행
                                try:
                                    self._execute_pyramiding_buy(ticker, score, gpt_confidence_map.get(ticker, 0.5), trade_logs, total_balance)
                                    logger.info(f"📊 {ticker} 피라미딩 매수 실행 후 trade_logs 개수: {len(trade_logs)}")
                                    
                                    # 실행 결과 확인
                                    if trade_logs:
                                        latest_log = trade_logs[-1]
                                        if latest_log.get('ticker') == ticker:
                                            logger.info(f"✅ {ticker} 피라미딩 매수 로그 추가됨: {latest_log.get('status')}")
                                        else:
                                            logger.warning(f"⚠️ {ticker} 피라미딩 매수 로그 누락")
                                    else:
                                        logger.warning(f"⚠️ {ticker} 피라미딩 매수 후 trade_logs가 비어있음")
                                        
                                except Exception as pyramid_error:
                                    logger.error(f"❌ {ticker} 피라미딩 매수 실행 중 예외: {pyramid_error}")
                                    import traceback
                                    logger.debug(f"❌ {ticker} 피라미딩 매수 상세 에러: {traceback.format_exc()}")
                                    
                                    # 예외 발생 시에도 로그 추가
                                    trade_logs.append({
                                        "ticker": ticker,
                                        "buy_price": 0,
                                        "score": score,
                                        "confidence": gpt_confidence_map.get(ticker, 0.5),
                                        "trade_amount_krw": 0,
                                        "status": "PYRAMIDING_EXCEPTION",
                                        "error_msg": str(pyramid_error)
                                    })
                                    
                            else:
                                logger.info(f"⏭️ {ticker} 피라미딩 조건 미충족 - 매수 건너뜀")
                                logger.info(f"   - 사유: {pyramid_result['reason']}")
                                continue
                        else:
                            # 새로운 종목 매수 (기존 로직)
                            logger.info(f"🆕 {ticker} 신규 종목 - 일반 매수 진행")
                            self._execute_new_position_buy(ticker, score, gpt_confidence_map.get(ticker, 0.5), trade_logs, total_balance)
                            
                    except Exception as e:
                        logger.error(f"❌ 매수 처리 중 오류: {ticker} | 오류: {str(e)}")
                        
                        # 예외 발생 이력 수집
                        trade_logs.append({
                            "ticker": ticker,
                            "buy_price": 0,
                            "score": score,
                            "confidence": gpt_confidence_map.get(ticker, 0.5),
                            "trade_amount_krw": base_amount,
                            "status": "ERROR",
                            "error_msg": str(e)
                        })
                
                # 매수 이력 DB 저장
                try:
                    self.save_trade_log_to_db(trade_logs)
                except Exception as e:
                    logger.warning(f"⚠️ 매수 이력 저장 중 오류: {str(e)}")
            
            else:
                logger.info("📊 최종 매수 대상이 없습니다.")
            
            traded_tickers = [log["ticker"] for log in trade_logs if log["status"] == "SUCCESS"]
            step_time = time.time() - step_start
            
            logger.info(f"✅ 3단계: 최종 매수 실행 완료 - {len(traded_tickers)}개 티커 매수 (소요시간: {step_time:.2f}초)")
            step_results["GPT_분석_매매"] = True
            
            # 4. 포트폴리오 업데이트 및 요약 정보 출력
            step_start = time.time()
            portfolio_data = self.update_portfolio()
            step_time = time.time() - step_start
            
            logger.info(f"✅ 4단계: 포트폴리오 업데이트 완료 (소요시간: {step_time:.2f}초)")
            step_results["포트폴리오_업데이트"] = True
            
            # 5. 매도 조건 점검 (PortfolioManager 활용)
            step_start = time.time()
            try:
                from portfolio_manager import PortfolioManager
                portfolio_manager = PortfolioManager(self.upbit)
                portfolio_manager.check_advanced_sell_conditions(portfolio_data)
                step_time = time.time() - step_start
                logger.info(f"✅ 5단계: 매도 조건 점검 완료 (소요시간: {step_time:.2f}초)")
                step_results["매도_조건_점검"] = True
            except Exception as e:
                step_time = time.time() - step_start
                logger.error(f"❌ 5단계: 매도 조건 점검 실패 (소요시간: {step_time:.2f}초): {e}")
                step_results["매도_조건_점검"] = False
            
            # 6. 백테스트 및 리포트
            step_start = time.time()
            try:
                # 전달받은 OHLCV 데이터 사용 또는 새로 수집
                if market_df_4h is not None and not market_df_4h.empty:
                    logger.info("📊 전달받은 OHLCV 데이터로 백테스팅 실행")
                    ohlcv_df = market_df_4h
                else:
                    # OHLCV 데이터 수집 (백테스팅용)
                    logger.info("📊 백테스팅을 위한 OHLCV 데이터 수집 중...")
                    ohlcv_data = {}
                    # 🔧 [개선] 더 많은 종목 데이터 수집 (상위 50개로 확대)
                    test_tickers = list(market_df_updated.index)[:50]  # 상위 50개 종목으로 확대
                    logger.info(f"📊 백테스팅 대상 종목: {len(test_tickers)}개")
                    
                    for ticker in test_tickers:
                        try:
                            df = self.db_mgr.fetch_ohlcv(ticker, days=200)
                            if df is not None and not df.empty:
                                ohlcv_data[ticker] = df
                        except Exception as e:
                            logger.warning(f"⚠️ {ticker} OHLCV 데이터 수집 실패: {e}")
                            continue
                    
                    if ohlcv_data and len(ohlcv_data) >= 1:  # 🔧 [개선] 최소 1개 종목 데이터로 완화
                        # OHLCV 데이터를 DataFrame으로 변환
                        ohlcv_df = pd.concat(ohlcv_data.values(), keys=ohlcv_data.keys(), names=['ticker', 'date'])
                        ohlcv_df = ohlcv_df.reset_index()
                        logger.info(f"📊 백테스팅 데이터 준비 완료: {len(ohlcv_data)}개 종목, {len(ohlcv_df)}개 레코드")
                        
                        # 🔧 [개선] 데이터 품질 검증 추가
                        total_records = len(ohlcv_df)
                        if total_records < 100:
                            logger.warning(f"⚠️ 백테스팅 데이터가 부족합니다: {total_records}개 레코드 (권장: 100개 이상)")
                        else:
                            logger.info(f"✅ 백테스팅 데이터 품질 확인: {total_records}개 레코드")
                    else:
                        ohlcv_df = None
                        logger.warning("⚠️ 백테스팅을 위한 OHLCV 데이터가 없습니다.")
                        
                        # 🔧 [개선] 대체 로직: DB에서 직접 OHLCV 데이터 조회
                        logger.info("🔄 대체 로직: DB에서 직접 OHLCV 데이터 조회 시도...")
                        try:
                            from utils import get_db_connection
                            conn = get_db_connection()
                            if conn:
                                # 최근 200일간의 OHLCV 데이터 조회
                                query = """
                                    SELECT ticker, date, open, high, low, close, volume
                                    FROM ohlcv 
                                    WHERE date >= CURRENT_DATE - INTERVAL '200 days'
                                    ORDER BY ticker, date
                                """
                                ohlcv_df = pd.read_sql_query(query, conn)
                                conn.close()
                                
                                if not ohlcv_df.empty:
                                    logger.info(f"✅ 대체 데이터 조회 성공: {len(ohlcv_df)}개 레코드")
                                    # 날짜 컬럼을 datetime으로 변환
                                    ohlcv_df['date'] = pd.to_datetime(ohlcv_df['date'])
                                else:
                                    logger.warning("⚠️ 대체 데이터 조회 결과도 비어있습니다")
                            else:
                                logger.warning("⚠️ DB 연결 실패로 대체 데이터 조회 불가")
                        except Exception as e:
                            logger.warning(f"⚠️ 대체 데이터 조회 중 오류: {e}")
                
                # 백테스트 실행
                if ohlcv_df is not None and not ohlcv_df.empty:
                    backtest_success = self.run_backtest_and_report(ohlcv_df, market_df_updated)
                    if backtest_success:
                        logger.info(f"✅ 7단계: 백테스트 및 리포트 생성 완료 (소요시간: {time.time() - step_start:.2f}초)")
                        step_results["백테스트_리포트"] = True
                    else:
                        logger.warning(f"⚠️ 7단계: 백테스트 및 리포트 생성 실패 (소요시간: {time.time() - step_start:.2f}초)")
                else:
                    logger.warning("⚠️ 백테스팅을 위한 OHLCV 데이터가 없습니다.")
                    
            except Exception as e:
                logger.error(f"❌ 백테스팅 중 오류 발생: {e}")
                logger.warning("⚠️ 백테스팅을 건너뛰고 파이프라인을 계속 진행합니다.")
                
            # 실행 요약
            total_time = time.time() - start_time
            success_count = sum(1 for success in step_results.values() if success)
            success_rate = (success_count / len(step_results)) * 100
            
            logger.info(f"✅ 트레이딩 및 리포트 생성 완료 - {success_count}/{len(step_results)} 단계 성공 ({success_rate:.1f}%) "
                      f"(총 소요시간: {total_time:.2f}초)")
            
            # 실패한 단계 로깅
            failed_steps = [step for step, success in step_results.items() if not success]
            if failed_steps:
                logger.warning(f"⚠️ 실패한 단계: {', '.join(failed_steps)}")
            
            # GPT 분석 결과 정렬 및 출력 완료 메시지
            logger.info("✅ GPT 분석 결과 정렬 및 출력 완료")
                
            return success_count >= 4  # 과반수 이상 성공하면 전체 성공으로 간주
            
        except Exception as e:
            logger.error(f"❌ 트레이딩 및 리포트 생성 중 오류 발생: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return False

    def process_gpt_analysis_chunked(self, gpt_json_data, config):
        """메모리 최적화가 강화된 청크 단위 GPT 분석 처리"""
        return self.process_gpt_analysis_chunked_enhanced(gpt_json_data, config)
    
    def process_gpt_chunk_with_retry(self, chunk, config):
        """재시도 로직이 강화된 GPT 청크 처리"""
        max_retries = config.get('max_retries', 3)
        
        for attempt in range(max_retries):
            try:
                # unified_gpt_analysis_engine 호출
                results = unified_gpt_analysis_engine(chunk, config)
                return results
                
            except Exception as e:
                logger.warning(f"⚠️ GPT 청크 처리 시도 {attempt+1}/{max_retries} 실패: {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # 지수 백오프
                else:
                    raise e
    
    def _save_intermediate_gpt_results(self, results):
        """중간 GPT 결과를 저장하여 메모리 사용량 최적화"""
        try:
            if self.save_to_db:
                self.save_gpt_analysis_to_db(results)
                logger.debug(f"📝 중간 GPT 결과 {len(results)}개 저장 완료")
        except Exception as e:
            logger.warning(f"⚠️ 중간 GPT 결과 저장 중 오류: {e}")
    
    def _batch_update_trailing_stops_fallback(self, batch_updates):
        """트레일링 스탑 배치 업데이트 fallback 메서드"""
        try:
            if not batch_updates:
                return
            
            logger.info(f"🔄 트레일링 스탑 배치 업데이트 (fallback): {len(batch_updates)}개")
            
            with self.get_db_connection_safe() as conn:
                cursor = conn.cursor()
                
                for update in batch_updates:
                    try:
                        # UPSERT 쿼리 실행
                        upsert_query = """
                        INSERT INTO trailing_stops 
                        (ticker, initial_price, activation_price, stop_price, atr_value, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (ticker) DO UPDATE SET
                        activation_price = EXCLUDED.activation_price,
                        stop_price = EXCLUDED.stop_price,
                        atr_value = EXCLUDED.atr_value,
                        updated_at = NOW()
                        """
                        
                        cursor.execute(upsert_query, (
                            update['ticker'],
                            update.get('initial_price', update['stop_price']),  # 초기가격 없으면 스탑가격 사용
                            update['activation_price'],
                            update['stop_price'],
                            update['atr_value']
                        ))
                        
                    except Exception as e:
                        logger.error(f"❌ {update['ticker']} 트레일링 스탑 업데이트 중 오류: {e}")
                        continue
                
                conn.commit()
                logger.info(f"✅ 트레일링 스탑 배치 업데이트 완료")
                
        except Exception as e:
            logger.error(f"❌ 트레일링 스탑 배치 업데이트 중 오류: {e}")
            raise

    def _log_gpt_analysis_metrics(self, results: list, config: dict):
        """GPT 분석 성능 메트릭 로깅"""
        total_analyzed = len(results)
        json_count = len([r for r in results if r.get("analysis_method") == "json"])
        chart_count = len([r for r in results if r.get("analysis_method", "").startswith("chart")])
        avg_confidence = sum(r.get("confidence", 0) for r in results) / max(total_analyzed, 1)
        
        logger.info(f"📊 GPT 분석 완료: 총 {total_analyzed}개")
        logger.info(f"   - JSON 분석: {json_count}개")
        logger.info(f"   - 차트 분석: {chart_count}개")
        logger.info(f"   - 평균 신뢰도: {avg_confidence:.2f}")
        logger.info(f"   - 분석 모드: {config.get('mode', 'unknown')}")
        logger.info(f"   - 캐싱 사용: {'Yes' if config.get('enable_caching') else 'No'}")

        # 상위 3개 종목 상세 정보
        if results:
            sorted_results = sorted(results, key=lambda x: x.get('score', 0), reverse=True)[:3]
            logger.info("   📈 상위 3개 종목:")
            for i, result in enumerate(sorted_results, 1):
                ticker = result.get('ticker', 'Unknown')
                score = result.get('score', 0)
                confidence = result.get('confidence', 0)
                method = result.get('analysis_method', 'unknown')
                logger.info(f"      {i}. {ticker}: {score}점 (신뢰도: {confidence:.2f}, 방법: {method})")

    def update_all_tickers(self):
        """
        모든 티커의 데이터를 업데이트하고 기술적 지표를 계산하는 전처리 과정을 수행합니다.
        🔧 [개선] 중복 호출 제거: 필요한 경우에만 티커 업데이트 수행
        
        이 함수는 다음 단계를 순차적으로 수행합니다:
        1. 티커 스캔 및 기본 필터링 (DB에서 기존 티커 사용)
        2. 일봉 OHLCV 및 지표 처리
        
        Returns:
            tuple: (filtered_tickers, market_df, market_df_4h) - 필터링된 티커 목록, 마켓 데이터프레임, 4시간봉 데이터프레임
        """
        try:
            logger.info("🚀 티커 데이터 업데이트 및 전처리 시작")
            
            # 1. 티커 스캔 및 필터링 (DB에서 기존 티커 사용, 중복 업데이트 제거)
            filtered_tickers = self.scan_and_filter_tickers()
            if not filtered_tickers:
                logger.warning("⚠️ 필터링된 티커가 없어 전처리를 중단합니다.")
                return [], pd.DataFrame(), pd.DataFrame()
                
            # 2. 일봉 OHLCV 및 지표 처리
            market_df = self.fetch_market_data_internal(filtered_tickers, timeframe='1d')
            if market_df is None or market_df.empty:
                logger.warning("⚠️ 일봉 데이터 처리 결과가 없어 전처리를 중단합니다.")
                return filtered_tickers, pd.DataFrame(), pd.DataFrame()
                
            logger.info(f"✅ 티커 데이터 업데이트 및 전처리 완료: {len(filtered_tickers)}개 티커")
            return filtered_tickers, market_df, pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ 티커 데이터 업데이트 및 전처리 중 오류 발생: {e}")
            return [], pd.DataFrame(), pd.DataFrame()

    def run(self):
        """메인 파이프라인 실행"""
        try:
            logging.info("🚀 Makenaide 파이프라인 시작")
            
            # 📁 로그 파일 정리 (설정 기반)
            try:
                from utils import cleanup_old_log_files, get_log_file_info
                from config import LOG_MANAGEMENT
                
                # 로그 관리 설정 확인
                if LOG_MANAGEMENT.get('enable_log_cleanup', True) and LOG_MANAGEMENT.get('log_cleanup_on_startup', True):
                    retention_days = LOG_MANAGEMENT.get('retention_days', 7)
                    
                    # 로그 파일 정리 실행
                    cleanup_result = cleanup_old_log_files(retention_days=retention_days)
                    if cleanup_result["status"] == "success":
                        logging.info(f"🗑️ 로그 파일 정리 완료: {cleanup_result['deleted_count']}개 파일 삭제 (보관기간: {retention_days}일)")
                    else:
                        logging.warning(f"⚠️ 로그 파일 정리 중 오류: {cleanup_result.get('message', 'Unknown error')}")
                    
                    # 현재 로그 파일 상태 출력
                    log_info = get_log_file_info()
                    if log_info["status"] == "success":
                        logging.info(f"📊 현재 로그 파일 상태: {log_info['total_files']}개 파일, 총 {log_info['total_size_mb']}MB")
                    else:
                        logging.warning(f"⚠️ 로그 파일 정보 조회 실패: {log_info.get('message', 'Unknown error')}")
                else:
                    logging.info("ℹ️ 로그 파일 정리가 비활성화되어 있습니다.")
                    
            except Exception as e:
                logging.error(f"❌ 로그 파일 정리 중 오류: {e}")
                logging.warning("⚠️ 로그 파일 정리 없이 파이프라인을 계속 진행합니다.")
            
            # 0. DB 초기화 확인 및 테이블 생성 (최우선)
            try:
                logging.info("🔧 DB 초기화 확인 중...")
                self.init_db()
                logging.info("✅ DB 초기화 완료")
            except Exception as e:
                logging.error(f"❌ DB 초기화 중 오류: {e}")
                logging.warning("⚠️ DB 초기화 실패, 파이프라인을 종료합니다.")
                return False
            
            # 🚨 [NEW] Disclaimer 동의 확인 (DB 초기화 후)
            try:
                from disclaimer_manager import DisclaimerManager
                disclaimer_mgr = DisclaimerManager(self.db_mgr)
                
                if not disclaimer_mgr.ensure_agreement():
                    logging.error("❌ Disclaimer 동의가 필요합니다. 프로그램을 종료합니다.")
                    return False
                    
                logging.info("✅ Disclaimer 동의 확인 완료")
                
            except Exception as e:
                logging.error(f"❌ Disclaimer 확인 중 오류: {e}")
                logging.error("❌ 프로그램을 종료합니다.")
                return False
            
            # 1. 시스템 데이터 검증 (추가)
            try:
                validation_success = self._perform_system_validation()
                if not validation_success:
                    logging.warning("⚠️ 시스템 데이터 검증에서 문제가 발견되었지만 파이프라인을 계속 진행합니다.")
            except Exception as e:
                logging.error(f"❌ 시스템 데이터 검증 중 오류: {e}")
                logging.warning("⚠️ 데이터 검증 없이 파이프라인을 계속 진행합니다.")
            
            # 1. 시작 전 매도 조건 점검
            sell_results = self.pm.check_advanced_sell_conditions()
            if sell_results and sell_results.get('sell_targets'):
                logging.info(f"💰 매도 조건 충족: {len(sell_results['sell_targets'])}건")
                
            # 2. 수동 개입 감지 (새로 추가)
            try:
                intervention_results = self.pm.detect_manual_interventions()
                if intervention_results.get('total_interventions', 0) > 0:
                    logging.warning(f"⚠️ 수동 개입 {intervention_results['total_interventions']}건 감지")
                    
                    # 수동 개입 요약 출력
                    for intervention in intervention_results.get('interventions', []):
                        logging.warning(f"   - {intervention['description']}")
                else:
                    logging.info("✅ 수동 개입 감지되지 않음")
                    
            except Exception as e:
                logging.error(f"❌ 수동 개입 감지 중 오류: {e}")
            
            # 3. 포트폴리오 상태 업데이트
            self.update_portfolio()
            
            # 4. 피라미딩 조건 확인
            try:
                self._check_pyramiding_conditions(self.pm)
                logging.info("✅ 피라미딩 조건 점검 완료")
            except Exception as e:
                logging.error(f"❌ 피라미딩 조건 점검 실패: {e}")
            
            # 5. 시장 데이터 수집 및 기술적 지표 업데이트 (통합 처리)
            try:
                logging.info("📊 시장 데이터 수집 및 기술적 지표 업데이트 중...")
                # 중복 호출 제거: scanner.update_tickers() 대신 update_all_tickers()만 사용
                filtered_tickers, market_df, _ = self.update_all_tickers()
                
                if not filtered_tickers:
                    logging.info("✅ 조건을 만족하는 종목이 없습니다.")
                    return
                    
            except Exception as e:
                logging.error(f"❌ 시장 데이터 수집 중 오류: {e}")
                recovery_success = self._handle_pipeline_error("market_data_collection", e)
                if not recovery_success:
                    logging.error("❌ 시장 데이터 수집 복구 실패, 파이프라인 종료")
                    return
                else:
                    # 복구 성공 시 기본 티커로 재시도
                    filtered_tickers, market_df, _ = self.update_all_tickers()
            
            # 🌡️ [이동] 6-1. 시장 체온계 검사 (최신 데이터 기반)
            try:
                logging.info("🌡️ 시장 체온계 검사 중... (최신 데이터 기반)")
                from market_sentiment import get_market_sentiment_snapshot
                
                sentiment_result = get_market_sentiment_snapshot()
                
                if not sentiment_result['should_proceed']:
                    logging.warning("⚠️ 시장 조건 미충족으로 파이프라인 중단")
                    logging.info(f"   - 상승종목: {sentiment_result['pct_up']}%")
                    logging.info(f"   - 거래대금집중도: {sentiment_result['top10_volume_ratio']}%")
                    logging.info(f"   - MA200상회: {sentiment_result['ma200_above_ratio']}%")
                    logging.info(f"   - 종합점수: {sentiment_result['sentiment_score']}")
                    logging.info(f"   - 시장상황: {sentiment_result['market_condition']}")
                    return
                else:
                    logging.info(f"✅ 시장 조건 충족, 파이프라인 진행")
                    logging.info(f"   - 시장상황: {sentiment_result['market_condition']}")
                    logging.info(f"   - 종합점수: {sentiment_result['sentiment_score']}")
                    logging.info(f"   - 상승종목: {sentiment_result['pct_up']}%")
                    logging.info(f"   - 거래대금집중도: {sentiment_result['top10_volume_ratio']}%")
                    logging.info(f"   - MA200상회: {sentiment_result['ma200_above_ratio']}%")
                    
            except Exception as e:
                logging.error(f"❌ 시장 체온계 검사 실패: {e}")
                logging.warning("⚠️ 시장 체온계 검사 없이 파이프라인 진행")
            
            # 7. 필터링 및 종목 선별 (에러 처리 강화)
            try:
                logging.info("🔍 종목 필터링 중...")
                filtered_df = self.filter_comprehensive_indicators(market_df)
                
                if filtered_df is None or filtered_df.empty:
                    logging.info("✅ 조건을 만족하는 종목이 없습니다.")
                    return
                    
            except Exception as e:
                logging.error(f"❌ 종목 필터링 중 오류: {e}")
                recovery_success = self._handle_pipeline_error("filtering", e)
                if not recovery_success:
                    logging.error("❌ 종목 필터링 복구 실패, 파이프라인 종료")
                    return
                else:
                    # 복구 성공 시 기본 필터링으로 진행
                    filtered_df = pd.DataFrame(index=filtered_tickers[:5])  # 상위 5개만 선택
            
            # 7-1. 일봉 데이터 차트 이미지 생성 (필터링 통과 종목)
            try:
                logging.info("📊 일봉 차트 이미지 생성 중...")
                chart_generation_success = True
                
                for ticker in filtered_df.index:
                    try:
                                                # OHLCV 데이터 조회
                        df = self.db_mgr.fetch_ohlcv(ticker, days=400)
                        if df is not None and not df.empty:
                            # 기술적 지표 계산 (차트 생성에 필요한 지표들 포함)
                            from data_fetcher import calculate_technical_indicators
                            df_with_indicators = calculate_technical_indicators(df)
                            
                            if df_with_indicators is not None and not df_with_indicators.empty:
                                # 차트 이미지 생성 (data_fetcher.py의 함수 직접 호출)
                                chart_path = generate_chart_image(ticker, df_with_indicators)
                                if chart_path:
                                    logging.info(f"✅ {ticker} 일봉 차트 이미지 생성 완료: {chart_path}")
                                else:
                                    logging.warning(f"⚠️ {ticker} 일봉 차트 이미지 생성 실패")
                                    chart_generation_success = False
                            else:
                                logging.warning(f"⚠️ {ticker} 기술적 지표 계산 실패")
                                chart_generation_success = False
                        else:
                            logging.warning(f"⚠️ {ticker} 일봉 OHLCV 데이터 없음")
                            chart_generation_success = False
                    except Exception as chart_e:
                        logging.error(f"❌ {ticker} 일봉 차트 생성 중 오류: {chart_e}")
                        chart_generation_success = False
                        continue
                
                if chart_generation_success:
                    logging.info(f"✅ 일봉 차트 이미지 생성 완료: {len(filtered_df.index)}개 종목")
                else:
                    logging.warning("⚠️ 일부 일봉 차트 이미지 생성 실패, 계속 진행")
                
            except Exception as e:
                logging.error(f"❌ 일봉 차트 이미지 생성 중 오류: {e}")
                logging.warning("⚠️ 일봉 차트 생성 실패, 계속 진행")
            
            # 🔄 [수정] 8. GPT 분석을 위한 JSON 생성 및 GPT 분석 실행
            try:
                logging.info("🤖 GPT 분석을 위한 JSON 생성 및 분석 실행 중...")
                
                # GPT 분석 대상 데이터 준비 (JSON 방식) - 캐싱 로직 적용
                analysis_candidates = []
                skipped_count = 0
                
                for ticker in filtered_df.index:
                    # 캐싱 로직 적용: 기존 분석 결과 확인
                    should_skip, existing_analysis = self._check_gpt_analysis_cache(ticker)
                    
                    if should_skip and existing_analysis:
                        # 기존 분석 결과 사용
                        analysis_candidates.append({
                            "ticker": ticker,
                            "base_score": existing_analysis.get('score', 85),
                            "cached_result": existing_analysis,
                            "skip_gpt_call": True
                        })
                        skipped_count += 1
                        logging.info(f"⏭️ {ticker} 캐시된 분석 결과 사용 (score: {existing_analysis.get('score', 85)})")
                    else:
                        # 새로운 JSON 데이터 생성
                        from data_fetcher import generate_gpt_analysis_json
                        json_data = generate_gpt_analysis_json(ticker, days=200)
                        if json_data:
                            analysis_candidates.append({
                                "ticker": ticker,
                                "base_score": 85,
                                "json_data": json_data
                            })
                        else:
                            logging.warning(f"⚠️ {ticker} JSON 데이터 생성 실패, 기본 데이터로 진행")
                            analysis_candidates.append({
                                "ticker": ticker,
                                "base_score": 85
                            })
                
                logging.info(f"📋 GPT 분석 대상: {len(analysis_candidates)}개 종목 (캐시 사용: {skipped_count}개)")
                
                # GPT 분석 설정
                gpt_config = self.get_gpt_config()
                from trend_analyzer import AnalysisConfig, GPTAnalysisOptimizerSingleton
                analysis_config = AnalysisConfig(
                    mode="json",
                    batch_size=gpt_config.get("batch_size", 3),
                    enable_caching=gpt_config.get("enable_caching", True),
                    cache_ttl_minutes=gpt_config.get("cache_ttl_minutes", 720),
                    api_timeout_seconds=gpt_config.get("api_timeout_seconds", 30),
                    max_retries=gpt_config.get("max_retries", 3)
                )
                
                # GPT 분석 최적화기 인스턴스 생성
                optimizer = GPTAnalysisOptimizerSingleton()
                
                # 캐시된 결과와 새로운 GPT 분석 결과 통합
                gpt_results = []
                
                # 1. 캐시된 결과 처리
                cached_results = [candidate for candidate in analysis_candidates if candidate.get('skip_gpt_call')]
                for cached_candidate in cached_results:
                    cached_result = cached_candidate['cached_result']
                    gpt_results.append({
                        'ticker': cached_candidate['ticker'],
                        'score': cached_result.get('score', 85),
                        'action': cached_result.get('action', 'HOLD'),
                        'confidence': cached_result.get('confidence', 0.7),
                        'market_phase': cached_result.get('market_phase', 'Unknown'),
                        'pattern': cached_result.get('pattern', ''),
                        'reason': cached_result.get('reason', ''),
                        'from_cache': True
                    })
                
                # 2. 새로운 GPT 분석 실행 (캐시되지 않은 종목만)
                new_candidates = [candidate for candidate in analysis_candidates if not candidate.get('skip_gpt_call')]
                
                if new_candidates:
                    from trend_analyzer import _call_gpt_json_batch
                    logging.info(f"🧠 새로운 GPT JSON 분석 실행: {len(new_candidates)}개 종목")
                    new_gpt_results = _call_gpt_json_batch(new_candidates, analysis_config, optimizer)
                    gpt_results.extend(new_gpt_results)
                    logging.info(f"✅ 새로운 GPT 분석 완료: {len(new_gpt_results)}개 결과")
                else:
                    logging.info("⏭️ 새로운 GPT 분석 대상이 없습니다 (모두 캐시 사용)")
                
                logging.info(f"✅ 전체 GPT 분석 완료: {len(gpt_results)}개 결과 (캐시: {len(cached_results)}개, 신규: {len(new_candidates)}개)")
                
                # GPT 분석 결과 로깅
                if gpt_results:
                    logging.info("📊 GPT 분석 결과 요약:")
                    cached_count = 0
                    new_count = 0
                    
                    for result in gpt_results:
                        ticker = result.get('ticker', 'Unknown')
                        score = result.get('score', 0)
                        action = result.get('action', 'Unknown')
                        confidence = result.get('confidence', 0)
                        from_cache = result.get('from_cache', False)
                        
                        if from_cache:
                            cached_count += 1
                            logging.info(f"   - {ticker}: {score}점, {action}, 신뢰도: {confidence:.2f} [캐시]")
                        else:
                            new_count += 1
                            logging.info(f"   - {ticker}: {score}점, {action}, 신뢰도: {confidence:.2f} [신규]")
                    
                    logging.info(f"📊 분석 결과 통계: 캐시 {cached_count}개, 신규 {new_count}개")
                
            except Exception as e:
                logging.error(f"❌ GPT 분석 중 오류: {e}")
                import traceback
                logging.error(f"상세 오류: {traceback.format_exc()}")
                recovery_success = self._handle_pipeline_error("gpt_analysis", e)
                if recovery_success:
                    # GPT 분석 없이 기본 거래 로직으로 진행
                    gpt_results = []
                    for ticker in filtered_df.index:
                        gpt_results.append({
                            'ticker': ticker,
                            'action': 'buy',
                            'confidence': 0.7,
                            'score': 75
                        })
                    logging.warning("⚠️ GPT 분석 우회, 기본 거래 로직으로 진행")
                else:
                    logging.error("❌ GPT 분석 복구 실패, 파이프라인 종료")
                    return
            
            # 🔄 [수정] 9. GPT 분석 결과 기반 4시간봉 데이터 처리 (조건 통과 종목만)
            try:
                logging.info("⏰ GPT 분석 결과 기반 4시간봉 데이터 처리 중...")
                
                # GPT 분석 결과에서 조건을 통과한 종목들 선별
                qualified_tickers = []
                if gpt_results:
                    for result in gpt_results:
                        from utils import safe_float_convert
                        score = safe_float_convert(result.get("score", 0), context=f"4시간봉필터 {result.get('ticker', 'Unknown')} score")
                        confidence = safe_float_convert(result.get("confidence", 0), context=f"4시간봉필터 {result.get('ticker', 'Unknown')} confidence")
                        action = result.get("action", "buy")
                        market_phase = result.get("market_phase", "Unknown")

                        # GPT 분석 결과 조건 통과 종목 선별 (설정 기반)
                        action = result.get("action", "AVOID").upper()
                        try:
                            from config import GPT_FILTERING_CONFIG
                            strict_config = GPT_FILTERING_CONFIG['strict_mode']
                        except ImportError:
                            # fallback 설정
                            strict_config = {
                                'min_score': 80,
                                'min_confidence': 0.9,
                                'allowed_actions': ['BUY', 'STRONG_BUY'],
                                'allowed_market_phases': ['Stage1', 'Stage2']
                            }
                        
                        if (score >= strict_config['min_score'] and 
                            confidence >= strict_config['min_confidence'] and 
                            action in strict_config['allowed_actions'] and 
                            market_phase in strict_config['allowed_market_phases']):
                            qualified_tickers.append(result["ticker"])
                
                if qualified_tickers:
                    logging.info(f"🎯 조건 통과 {len(qualified_tickers)}개 티커의 4시간봉 데이터 처리 시작")
                    
                    for ticker in qualified_tickers:
                        try:
                            # 4시간봉 OHLCV 수집
                            from data_fetcher import get_ohlcv_4h, save_ohlcv_4h_to_db
                            df_4h = get_ohlcv_4h(ticker, limit=200, force_fetch=True)
                            
                            if df_4h is not None and not df_4h.empty:
                                # DB 저장
                                save_ohlcv_4h_to_db(ticker, df_4h)
                                
                                # 마켓타이밍 지표 계산
                                from data_fetcher import calculate_technical_indicators_4h
                                df_with_indicators = calculate_technical_indicators_4h(df_4h)
                                
                                if df_with_indicators is not None:
                                    # market_data_4h 테이블에 저장
                                    from data_fetcher import save_market_data_4h_to_db
                                    save_market_data_4h_to_db(ticker, df_with_indicators)
                                    logging.info(f"✅ {ticker} 4시간봉 처리 완료 (OHLCV + 지표)")
                                else:
                                    logging.warning(f"⚠️ {ticker} 4시간봉 지표 계산 실패")
                            else:
                                logging.warning(f"⚠️ {ticker} 4시간봉 데이터 수집 실패")
                                
                        except Exception as e:
                            logging.error(f"❌ {ticker} 4시간봉 처리 실패: {e}")
                else:
                    logging.info("📊 조건 통과 종목이 없어 4시간봉 처리를 건너뜁니다.")
                    
            except Exception as e:
                logging.error(f"❌ 4시간봉 데이터 처리 중 오류: {e}")
                logging.warning("⚠️ 4시간봉 처리 실패, 계속 진행")
            
            # 10. 최종 거래 실행 및 리포트 (에러 처리 강화)
            if not gpt_results:
                logging.warning("⚠️ GPT 분석 결과가 없어 파이프라인을 종료합니다.")
                return
                
            try:
                logging.info("📋 거래 및 리포트 생성 중...")
                
                # 백테스팅을 위한 OHLCV 데이터 준비
                ohlcv_data_for_trading = {}
                trading_tickers = [result.get('ticker', '') for result in gpt_results if result.get('ticker')]
                
                for ticker in trading_tickers[:10]:  # 상위 10개 종목만
                    try:
                        df = self.db_mgr.fetch_ohlcv(ticker, days=200)
                        if df is not None and not df.empty:
                            ohlcv_data_for_trading[ticker] = df
                    except Exception as e:
                        logging.warning(f"⚠️ {ticker} OHLCV 데이터 수집 실패: {e}")
                        continue
                
                # OHLCV 데이터를 DataFrame으로 변환
                if ohlcv_data_for_trading:
                    ohlcv_df_for_trading = pd.concat(ohlcv_data_for_trading.values(), keys=ohlcv_data_for_trading.keys(), names=['ticker', 'date'])
                    ohlcv_df_for_trading = ohlcv_df_for_trading.reset_index()
                    logging.info(f"📊 거래용 OHLCV 데이터 준비 완료: {len(ohlcv_data_for_trading)}개 종목")
                else:
                    ohlcv_df_for_trading = None
                    logging.warning("⚠️ 거래용 OHLCV 데이터 수집 실패")
                
                # GPT 분석 결과를 scored_tickers 형식으로 변환
                scored_tickers = [(result.get('ticker', ''), result.get('score', 0)) for result in gpt_results if result.get('ticker')]
                self.trade_and_report(scored_tickers, market_df, ohlcv_df_for_trading, gpt_results)
                
            except Exception as e:
                logging.error(f"❌ 거래 및 리포트 생성 중 오류: {e}")
                logging.warning("⚠️ 거래 실행은 건너뛰고 파이프라인을 종료합니다.")
            
            logging.info("✅ Makenaide 파이프라인 완료")
            
        except Exception as e:
            logging.error(f"❌ 파이프라인 실행 중 전체 오류: {e}")
            import traceback
            traceback.print_exc()
            # 전체 실패 시에도 복구 시도
            try:
                self._handle_critical_error(e)
            except:
                pass
            raise

    def _check_pyramiding_conditions(self, portfolio_manager):
        """보유 종목에 대한 피라미딩 조건 점검 및 실행 (안전성 강화)"""
        try:
            # 현재 보유 종목 조회
            current_positions = portfolio_manager.get_current_positions()
            
            if not current_positions:
                logging.info("📊 보유 종목이 없어 피라미딩 조건 점검을 건너뜁니다.")
                return
            
            logging.info(f"📊 피라미딩 조건 점검 대상: {len(current_positions)}개 보유 종목")
            
            pyramiding_results = []
            
            for position in current_positions:
                ticker = position.get('ticker', '')
                if not ticker:
                    continue
                
                # 티커 형식 확인 (KRW- 접두사 제거)
                if ticker.startswith('KRW-'):
                    symbol = ticker[4:]  # KRW- 제거
                else:
                    symbol = ticker
                
                # 피라미딩 조건 점검
                try:
                    # 매수 정보를 portfolio_manager.purchase_info에 등록
                    # (실제 보유 종목이므로 기존 매수 정보로 간주)
                    if ticker not in portfolio_manager.purchase_info:
                        # 안전한 데이터 추출
                        avg_price = self._safe_extract_position_data(position, 'avg_price', 'avg_buy_price')
                        timestamp = self._safe_extract_position_data(position, 'timestamp', 'created_at')
                        quantity = self._safe_extract_position_data(position, 'quantity', 'balance')
                        
                        if avg_price and avg_price > 0:
                            portfolio_manager.purchase_info[ticker] = {
                                'price': float(avg_price),
                                'timestamp': str(timestamp) if timestamp else '',
                                'quantity': float(quantity) if quantity else 0
                            }
                            logging.debug(f"✅ {ticker} 피라미딩 정보 등록: 평균가={avg_price}, 수량={quantity}")
                        else:
                            logging.warning(f"⚠️ {ticker} 유효하지 않은 포지션 데이터, 피라미딩 건너뜀")
                            continue
                    
                    # 피라미딩 조건 체크 (안전성 강화)
                    pyramid_executed = self._safe_check_pyramiding(portfolio_manager, ticker)
                    
                    if pyramid_executed is True:
                        pyramiding_results.append({
                            'ticker': ticker,
                            'status': 'executed',
                            'message': f'{ticker} 피라미딩 매수 실행됨'
                        })
                        logging.info(f"✅ {ticker} 피라미딩 매수 실행 완료")
                    elif pyramid_executed is False:
                        pyramiding_results.append({
                            'ticker': ticker,
                            'status': 'no_action',
                            'message': f'{ticker} 피라미딩 조건 미충족'
                        })
                        logging.debug(f"📊 {ticker} 피라미딩 조건 미충족")
                    else:
                        # None 반환 시 (에러 상황)
                        pyramiding_results.append({
                            'ticker': ticker,
                            'status': 'error',
                            'message': f'{ticker} 피라미딩 체크 중 내부 오류'
                        })
                        logging.warning(f"⚠️ {ticker} 피라미딩 체크 결과 None 반환")
                        
                except Exception as e:
                    pyramiding_results.append({
                        'ticker': ticker,
                        'status': 'error',
                        'message': f'{ticker} 피라미딩 체크 중 오류: {str(e)}'
                    })
                    logging.error(f"❌ {ticker} 피라미딩 체크 중 오류: {e}")
                    # 상세 에러 정보 로깅
                    import traceback
                    logging.debug(f"❌ {ticker} 피라미딩 에러 상세: {traceback.format_exc()}")
            
            # 피라미딩 결과 요약
            executed_count = sum(1 for r in pyramiding_results if r['status'] == 'executed')
            no_action_count = sum(1 for r in pyramiding_results if r['status'] == 'no_action')
            error_count = sum(1 for r in pyramiding_results if r['status'] == 'error')
            
            logging.info("🔼 피라미딩 조건 점검 완료:")
            logging.info(f"   - 실행: {executed_count}건")
            logging.info(f"   - 미실행: {no_action_count}건")
            logging.info(f"   - 오류: {error_count}건")
            
            # 피라미딩 실행 결과가 있으면 상세 로그 출력
            if executed_count > 0:
                executed_tickers = [r['ticker'] for r in pyramiding_results if r['status'] == 'executed']
                logging.info(f"🔼 피라미딩 실행 종목: {', '.join(executed_tickers)}")
            
            # 에러가 있으면 상세 로그 출력
            if error_count > 0:
                error_details = [f"{r['ticker']}: {r['message']}" for r in pyramiding_results if r['status'] == 'error']
                logging.warning(f"⚠️ 피라미딩 에러 상세: {'; '.join(error_details)}")
            
        except Exception as e:
            logging.error(f"❌ 피라미딩 조건 점검 중 전체 오류: {e}")
            # 상세 에러 정보 로깅
            import traceback
            logging.debug(f"❌ 피라미딩 조건 점검 에러 상세: {traceback.format_exc()}")
            
    def _safe_extract_position_data(self, position: dict, primary_key: str, fallback_key: str = None):
        """포지션 데이터에서 안전하게 값 추출"""
        try:
            # 1차 키로 조회
            value = position.get(primary_key)
            if value is not None and value != 0:
                return value
                
            # 2차 키로 조회 (fallback)
            if fallback_key:
                value = position.get(fallback_key)
                if value is not None and value != 0:
                    return value
                    
            return None
            
        except Exception as e:
            logging.warning(f"⚠️ 포지션 데이터 추출 실패: {e}")
            return None
            
    def _safe_check_pyramiding(self, portfolio_manager, ticker: str):
        """안전한 피라미딩 조건 체크"""
        try:
            # 피라미딩 조건 체크 시도
            result = portfolio_manager.check_pyramiding(ticker)
            
            # 결과 유효성 검증
            if result is None:
                logging.warning(f"⚠️ {ticker} 피라미딩 체크 결과가 None")
                return None
            elif isinstance(result, bool):
                return result
            else:
                logging.warning(f"⚠️ {ticker} 피라미딩 체크 예상치 못한 결과 타입: {type(result)}")
                return None
                
        except Exception as e:
            logging.error(f"❌ {ticker} 안전한 피라미딩 체크 실패: {e}")
            return None

    def _initialize_system(self):
        """시스템 초기화 단계"""
        try:
            # DB 초기화 (최초 1회)
            if not self.initialized:
                step_start = time.time()
                self.init_db()
                self.initialized = True
                logger.info(f"✅ DB 초기화 완료 (소요시간: {time.time() - step_start:.2f}초)")
            else:
                logger.info("✅ DB 이미 초기화됨 (건너뜀)")
                
            # 🔧 [개선] 티커 정보 업데이트는 update_all_tickers()에서 통합 처리
            # 중복 호출 제거로 성능 향상
            logger.info("✅ 시스템 초기화 완료 (티커 업데이트는 메인 파이프라인에서 처리)")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 시스템 초기화 중 오류: {e}")
            return False

    def _update_market_data(self):
        """시장 데이터 업데이트 및 전처리"""
        try:
            step_start = time.time()
            
            # 티커 데이터 업데이트 및 전처리
            filtered_tickers, market_df, _ = self.update_all_tickers()
            
            if not filtered_tickers or market_df is None or market_df.empty:
                logger.warning("⚠️ 시장 데이터 전처리에 실패했습니다.")
            return None
                
            logger.info(f"✅ 시장 데이터 업데이트 완료 ({len(filtered_tickers)}개 티커, 소요시간: {time.time() - step_start:.2f}초)")
            
            return {
                'filtered_tickers': filtered_tickers,
                'market_df': market_df
            }
            
        except Exception as e:
            logger.error(f"❌ 시장 데이터 업데이트 중 오류: {e}")
            return None
            
    def _execute_trading_pipeline(self, market_data):
        """트레이딩 파이프라인 실행"""
        try:
            step_start = time.time()
            
            # 종합 지표 필터링
            filtered_df = self.filter_comprehensive_indicators(market_data['market_df'])
            
            if filtered_df is None or filtered_df.empty:
                logger.warning("⚠️ 종합 지표 필터링에 실패했습니다.")
                return False

            logger.info(f"✅ 종합 지표 필터링 완료 (선별된 티커: {len(filtered_df)}개, 소요시간: {time.time() - step_start:.2f}초)")

            # 차트 이미지 생성
            self._generate_chart_images(filtered_df)
            
            # GPT 분석 데이터 준비
            gpt_data = self._prepare_gpt_analysis_data(filtered_df)
            
            # trend_analyzer.py의 unified_gpt_analysis_engine 사용
            from trend_analyzer import unified_gpt_analysis_engine
            
            # GPT 분석 설정
            analysis_config = self.get_gpt_config()
            
            # unified_gpt_analysis_engine 호출하여 GPT 분석 실행
            logger.info("🧠 GPT 분석 실행 (trend_analyzer.unified_gpt_analysis_engine 사용)")
            gpt_results = unified_gpt_analysis_engine(gpt_data, analysis_config)
            
            if gpt_results:
                logger.info(f"✅ GPT 분석 완료: {len(gpt_results)}개 결과")
                # 결과를 기존 파이프라인에 전달
                trading_results = self.trade_and_report(gpt_results, market_data['market_df'], None, gpt_results)
                return trading_results
            else:
                logger.warning("⚠️ GPT 분석 결과가 없습니다")
                return False
            
        except Exception as e:
            logger.error(f"❌ 트레이딩 파이프라인 실행 중 오류: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return False

    def _generate_chart_images(self, filtered_df):
        """필터링 통과 종목에 대한 차트 이미지 생성"""
        if filtered_df.empty:
            return
            
        logger.info("📊 필터링 통과 종목 차트 이미지 생성 시작")
        for ticker in filtered_df.index:
            try:
                ohlcv_data = self.get_ohlcv_from_db(ticker, limit=250)
                if ohlcv_data is not None and not ohlcv_data.empty:
                    # 기술적 지표 계산 (차트 생성에 필요한 지표들 포함)
                    from data_fetcher import calculate_technical_indicators
                    ohlcv_with_indicators = calculate_technical_indicators(ohlcv_data)
                    
                    if ohlcv_with_indicators is not None and not ohlcv_with_indicators.empty:
                        # data_fetcher.py의 generate_chart_image 함수 직접 호출
                        chart_path = generate_chart_image(ticker, ohlcv_with_indicators)
                        logger.info(f"✅ {ticker} 차트 생성 완료: {chart_path}")
                    else:
                        logger.warning(f"⚠️ {ticker} 기술적 지표 계산 실패")
                else:
                    logger.warning(f"⚠️ {ticker} OHLCV 데이터 없음, 차트 생성 건너뜀")
            except Exception as e:
                logger.error(f"❌ {ticker} 차트 생성 실패: {str(e)}")

    def _prepare_gpt_analysis_data(self, filtered_df):
        """GPT 분석용 데이터 준비"""
        # 중복 티커 제거
        filtered_df = filtered_df[~filtered_df.index.duplicated(keep='first')]
        logger.info(f"[중복 제거] GPT 분석 대상 티커 수: {len(filtered_df)}")
        
        # 기존의 긴 GPT 데이터 준비 로직을 여기로 이동
        # (현재 코드의 나머지 부분은 동일하게 유지)
        scored_tickers = [(ticker, 85.0) for ticker in filtered_df.index]
        return scored_tickers

    def _generate_reports(self, market_df):
        """리포트 생성 - 향상된 백테스트 연동"""
        try:
            step_start = time.time()
            
            # 1. 실시간 성과 업데이트
            try:
                from strategy_analyzer import get_enhanced_analyzer  # 통합된 버전 사용
                analyzer = get_enhanced_analyzer()
                performance_update = analyzer.update_strategy_performance(days=7)
                
                if performance_update:
                    logger.info(f"📊 실시간 성과 업데이트 완료: 승률 {performance_update.get('win_rate', 0):.1%}")
                    
                    # 성과 기반 추천사항 출력
                    recommendation = performance_update.get('recommendation', '')
                    if recommendation:
                        logger.info(f"💡 추천사항: {recommendation}")
                
            except Exception as e:
                logger.warning(f"⚠️ 실시간 성과 업데이트 실패: {e}")
            
            # 2. 기존 백테스트 실행
            if market_df is not None and not market_df.empty:
                backtest_success = self.run_backtest_and_report(None, market_df)
                logger.info(f"✅ 백테스트 및 리포트 생성 완료 (소요시간: {time.time() - step_start:.2f}초)")
                return backtest_success
            else:
                logger.warning("⚠️ 백테스트를 위한 데이터가 부족합니다.")
                return False
                
        except Exception as e:
            logger.error(f"❌ 리포트 생성 중 오류: {e}")
            return False

    def _handle_critical_error(self, error):
        """중요한 오류 처리"""
        logger.error(f"❌ Makenaide 봇 실행 중 오류 발생: {error}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False

    def get_trading_config_value(self, key_path: str, default: Any = None) -> Any:
        """트레이딩 설정값 조회 (기존 설정 우선, 없으면 새 설정)"""
        if self.trading_config:
            return self.trading_config.get(key_path, default)
        return default

    def update_trading_config_value(self, key_path: str, value: Any) -> bool:
        """트레이딩 설정값 업데이트"""
        if self.trading_config:
            return self.trading_config.set(key_path, value)
        return False

    def get_gpt_config(self) -> Dict[str, Any]:
        """GPT 분석 설정 조회"""
        if self.trading_config:
            return self.trading_config.get_gpt_config()
        return {
            'score_threshold': 85,
            'confidence_threshold': 0.9,
            'batch_size': 5,
            'memory_threshold_mb': 500
        }

    def get_risk_config(self) -> Dict[str, Any]:
        """리스크 관리 설정 조회"""
        if self.trading_config:
            return self.trading_config.get_risk_config()
        return {
            'base_stop_loss': 3.0,
            'base_take_profit': 6.0,
            'max_volatility_multiplier': 3.0,
            'max_position_size': 0.05
        }

    def get_active_tickers_hybrid(self):
        """
        전체 활성 티커 조회: 시스템 전반적인 티커 목록 제공 (레거시 호환성 유지)
        
        주요 용도:
        - 전체 시스템 초기화 시 활성 티커 목록 제공
        - 백테스트나 전체 시장 분석용 티커 리스트 생성
        - 기존 코드와의 호환성 유지
        
        ⚠️ 주의: filter_comprehensive_indicators()에서는 사용하지 않음
        대신 _validate_active_status_only()를 사용하여 이미 필터링된 티커의 활성 상태만 검증
        
        우선순위:
        1. is_active 컬럼이 있으면 우선 활용
        2. 블랙리스트로 추가 필터링
        3. 두 결과의 교집합을 최종 활용
        
        Returns:
            list: 활성 상태이고 블랙리스트에 없는 전체 티커 목록
        """
        try:
            # 검증 함수 실행
            from utils import validate_ticker_filtering_system
            validation = validate_ticker_filtering_system()
            
            logger.info(f"📊 전체 티커 필터링 시스템 검증 결과:")
            logger.info(f"   - is_active 컬럼 사용 가능: {validation['is_active_available']}")
            logger.info(f"   - 블랙리스트 사용 가능: {validation['blacklist_available']}")
            logger.info(f"   - 필터링 일관성: {validation['filtering_consistency']}")
            
            tickers_result = self.db_mgr.execute_query("SELECT ticker FROM tickers")
            if not tickers_result:
                logger.warning("⚠️ 티커 데이터가 없습니다.")
                return []
                
            if validation["is_active_available"]:
                # is_active 컬럼 활용
                active_result = self.db_mgr.execute_query("SELECT ticker FROM tickers WHERE is_active = true")
                active_tickers = {row[0] for row in active_result} if active_result else set()
                logger.info(f"📊 is_active 필터링 결과: {len(active_tickers)}개 티커")
            else:
                # 전체 티커 조회
                active_tickers = {row[0] for row in tickers_result}
                logger.info(f"📊 전체 티커 조회: {len(active_tickers)}개 티커")
            
            # 블랙리스트 추가 필터링
            if validation["blacklist_available"]:
                blacklist = load_blacklist()
                filtered_tickers = [t for t in active_tickers if t not in blacklist]
                logger.info(f"📊 블랙리스트 추가 필터링 후: {len(filtered_tickers)}개 티커")
                
                # 일관성 확인 및 경고
                if validation["is_active_available"] and validation["consistency_rate"] < 0.8:
                    logger.warning(f"⚠️ is_active 컬럼과 블랙리스트 일관성 낮음: {validation['consistency_rate']:.2%}")
                    logger.warning("   블랙리스트 동기화를 권장합니다: python scanner.py --sync-blacklist")
                    
            else:
                filtered_tickers = list(active_tickers)
                logger.warning("⚠️ 블랙리스트 로드 실패, is_active 결과만 사용")
            
            logger.info(f"✅ 전체 하이브리드 필터링 완료: {len(filtered_tickers)}개 티커 선별")
            return filtered_tickers
            
        except Exception as e:
            logger.error(f"❌ 하이브리드 티커 필터링 실패: {e}")
            # 폴백: 기존 방식 사용
            try:
                tickers_result = self.db_mgr.execute_query("SELECT ticker FROM tickers")
                if tickers_result:
                    tickers = [row[0] for row in tickers_result]
                    blacklist = load_blacklist()
                    filtered_tickers = [t for t in tickers if t not in blacklist]
                    logger.info(f"🔄 폴백 필터링 완료: {len(filtered_tickers)}개 티커")
                    return filtered_tickers
                return []
            except:
                return []

    def _activate_emergency_cleanup(self, all_results, streaming_saver):
        """긴급 정리 모드 활성화"""
        logger.warning("🚨 긴급 정리 모드 활성화")
        
        # 1단계: 모든 결과를 즉시 저장
        if all_results:
            streaming_saver.emergency_save(all_results)
            all_results.clear()
            logger.info(f"💾 긴급 저장 완료")
        
        # 2단계: 강화된 가비지 컬렉션
            for generation in range(3):
                collected = gc.collect(generation)
            logger.debug(f"🧹 GC 세대 {generation}: {collected}개 객체 정리")
        
        # 3단계: 시스템 메모리 정리 요청
        try:
            import ctypes
            if hasattr(ctypes, 'windll'):  # Windows
                ctypes.windll.kernel32.SetProcessWorkingSetSize(-1, -1, -1)
            elif hasattr(ctypes, 'CDLL'):  # Unix/Linux
                libc = ctypes.CDLL("libc.so.6")
                libc.malloc_trim(0)
        except:
            pass  # 플랫폼에서 지원하지 않는 경우 무시
        
        logger.info("🧹 긴급 정리 모드 완료")

    def _resize_remaining_chunks(self, chunks, current_index, new_chunk_size):
        """남은 청크들을 새로운 크기로 재분할"""
        if current_index >= len(chunks) - 1:
            return chunks
        
        # 남은 데이터 수집
        remaining_data = []
        for chunk in chunks[current_index + 1:]:
            remaining_data.extend(chunk)
        
        # 새로운 크기로 재분할
        new_chunks = [remaining_data[i:i+new_chunk_size] 
                     for i in range(0, len(remaining_data), new_chunk_size)]
        
        return chunks[:current_index + 1] + new_chunks

    def _process_chunk_results(self, chunk_results):
        """청크 결과 후처리 및 메모리 최적화"""
        processed_results = []
        
        for result in chunk_results:
            # ✅ GPT 분석의 모든 핵심 필드 보존 (DB 스키마와 일치)
            processed_result = {
                "ticker": result.get("ticker", "Unknown"),
                "score": safe_float_convert(result.get("score", 0), context="GPT분석 result score"),
                "confidence": safe_float_convert(result.get("confidence", 0), context="GPT분석 result confidence"),
                # ✅ DB 스키마에 필요한 필드들 추가 (trend_analysis 테이블)
                "action": result.get("action", "HOLD"),
                "market_phase": result.get("market_phase", "Unknown"), 
                "pattern": result.get("pattern", ""),
                "reason": result.get("reason", ""),
                # 기존 필드들 유지
                "input_type": result.get("analysis_method", result.get("input_type", "unknown")),
                "chart_path": result.get("chart_path", f"charts/{result.get('ticker', 'unknown')}.png")
            }
            
            # 불필요한 메모리 사용량이 큰 필드 제거
            for key in ['raw_response', 'debug_info', 'intermediate_data']:
                if key in result:
                    del result[key]
            
            processed_results.append(processed_result)
        
        return processed_results

    def _generate_fallback_results(self, chunk):
        """실패한 청크에 대한 기본값 생성"""
        fallback_results = []
        for ticker_data in chunk:
                    ticker = ticker_data.get("ticker", "Unknown")
                    fallback_results.append({
                        "ticker": ticker,
                        "score": 50.0,
                        "confidence": 0.30,
                # ✅ DB 스키마 필드들 추가 (trend_analysis 테이블)
                "action": "HOLD",
                "market_phase": "Unknown",
                "pattern": "",
                "reason": "분석 실패로 인한 기본값",
                # 기존 필드들 유지
                        "input_type": "chunk_error",
                        "chart_path": f"charts/{ticker}.png"
                    })
        return fallback_results

    def _handle_pipeline_error(self, stage_name, error):
        """파이프라인 단계별 에러 처리"""
        logger.error(f"❌ {stage_name} 단계 실패: {error}")
        
        # 스테이지별 복구 로직
        if stage_name == "market_data_collection":
            logger.info("🔄 대체 데이터 수집 방법 시도 중...")
            try:
                # 기본 티커 리스트로 대체 시도
                basic_tickers = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
                filtered_tickers, market_df, _ = self.update_all_tickers()
                if filtered_tickers:
                    logger.info(f"✅ 대체 데이터 수집 성공: {len(filtered_tickers)}개 티커")
                    return True
            except Exception as fallback_error:
                logger.error(f"❌ 대체 데이터 수집도 실패: {fallback_error}")
                
        elif stage_name == "filtering":
            logger.info("🔄 기본 필터링 조건으로 복구 시도 중...")
            try:
                # 최소한의 필터링 조건으로 복구 시도
                active_tickers = self.get_active_tickers_hybrid()
                if active_tickers:
                    logger.info(f"✅ 기본 필터링 복구 성공: {len(active_tickers)}개 티커")
                    return True
            except Exception as fallback_error:
                logger.error(f"❌ 기본 필터링 복구도 실패: {fallback_error}")
                
        elif stage_name == "gpt_analysis":
            logger.info("🔄 GPT 분석 우회 시도 중...")
            logger.warning("⚠️ GPT 분석 없이 기본 거래 로직으로 진행")
            return True
            
        return False

    def _perform_system_validation(self):
        """시스템 초기화 시 데이터 검증 수행"""
        try:
            logger.info("🔍 시스템 데이터 검증 시작...")
            
            # 1. static_indicators 테이블 검증
            try:
                self.validate_static_indicators_data()
                logger.info("✅ static_indicators 테이블 검증 완료")
            except Exception as e:
                logger.warning(f"⚠️ static_indicators 테이블 검증 실패: {e}")
            
            # 2. OHLCV 정밀도 검증
            try:
                self.validate_ohlcv_precision()
                logger.info("✅ OHLCV 정밀도 검증 완료")
            except Exception as e:
                logger.warning(f"⚠️ OHLCV 정밀도 검증 실패: {e}")
            
            logger.info("✅ 시스템 데이터 검증 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 시스템 데이터 검증 중 오류: {e}")
            return False
    
    # get_total_balance 함수는 portfolio_manager.py로 통합됨
    # 위의 get_total_balance 함수를 사용하세요
    
    def _validate_market_conditions(self, ticker, current_price, score, confidence):
        """
        시장 조건 검증 메서드
        """
        try:
            validation_result = {
                'valid': True,
                'reason': '검증 통과'
            }
            
            # 기본 검증 조건들
            if current_price <= 0:
                validation_result['valid'] = False
                validation_result['reason'] = '현재가가 0 이하입니다'
                return validation_result
            
            if score < 0:
                validation_result['valid'] = False
                validation_result['reason'] = '점수가 음수입니다'
                return validation_result
            
            if confidence < 0 or confidence > 1:
                validation_result['valid'] = False
                validation_result['reason'] = '신뢰도가 유효하지 않습니다 (0~1 범위)'
                return validation_result
            
            # 추가 검증 로직은 필요에 따라 확장 가능
            logger.debug(f"✅ 시장 조건 검증 통과: {ticker} | 가격: {current_price} | 점수: {score} | 신뢰도: {confidence}")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ 시장 조건 검증 중 오류: {e}")
            return {
                'valid': False,
                'reason': f'검증 중 오류 발생: {str(e)}'
            }
    
    def _analyze_buy_error(self, error_msg, ticker, current_price, trade_amount_krw):
        """
        매수 오류 분석 메서드
        """
        try:
            analysis = "알 수 없는 오류"
            error_lower = error_msg.lower()
            
            # 일반적인 오류 패턴 분석
            if "insufficient" in error_lower or "잔액" in error_msg:
                analysis = "잔액 부족"
            elif "minimum" in error_lower or "최소" in error_msg:
                analysis = "최소 주문 금액 미달"
            elif "market" in error_lower or "마켓" in error_msg:
                analysis = "마켓 상태 문제"
            elif "api" in error_lower:
                analysis = "API 호출 오류"
            elif "timeout" in error_lower:
                analysis = "타임아웃 오류"
            elif "rate" in error_lower or "제한" in error_msg:
                analysis = "API 호출 제한"
            else:
                analysis = f"기타 오류: {error_msg[:50]}"
            
            logger.debug(f"🔍 매수 오류 분석: {ticker} | 오류: {error_msg} | 분석: {analysis}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ 매수 오류 분석 중 오류: {e}")
            return "오류 분석 실패"

    def calculate_kelly_position_size(self, ticker: str, score: float, confidence: float, 
                                    current_price: float, total_balance: float) -> dict:
        """
        켈리 공식 기반 포지션 사이징 계산
        
        Args:
            ticker: 티커 심볼
            score: GPT 분석 점수
            confidence: GPT 분석 신뢰도
            current_price: 현재가
            total_balance: 총 자산
            
        Returns:
            dict: 켈리 기반 포지션 사이징 결과
        """
        try:
            # 1. 기술적 지표 데이터 조회 (ATR, 지지/저항선 등)
            market_data = self._get_market_data_for_kelly(ticker)
            if not market_data:
                logger.warning(f"⚠️ {ticker} 켈리 계산을 위한 시장 데이터 없음")
                return self._get_default_kelly_result(total_balance)
            
            # 2. ATR 기반 리스크 계산
            atr = market_data.get('atr', 0)
            if atr <= 0:
                logger.warning(f"⚠️ {ticker} ATR 값이 유효하지 않음: {atr}")
                return self._get_default_kelly_result(total_balance)
            
            # 3. 손절가 및 목표가 계산
            stop_loss = current_price - (atr * 2.5)  # 2.5x ATR 손절
            target_price = current_price + (atr * 4.0)  # 4.0x ATR 목표 (리스크 대비 1.6:1)
            
            # 4. 승률 추정 (점수와 신뢰도 기반)
            # 점수 50점 기준으로 승률 추정 (40-80% 범위)
            base_win_rate = 0.4 + (score / 100.0) * 0.4  # 40-80% 범위
            # 신뢰도로 승률 조정
            estimated_win_rate = base_win_rate * confidence
            estimated_win_rate = max(0.3, min(estimated_win_rate, 0.8))  # 30-80% 범위
            
            # 5. 평균 수익/손실 비율 계산
            avg_win = (target_price - current_price) / current_price
            avg_loss = (current_price - stop_loss) / current_price
            
            # 6. 켈리 공식 적용: f = (bp - q) / b
            # b = 승리시 수익률, p = 승률, q = 패배 확률
            if avg_loss > 0 and avg_win > 0:
                kelly_fraction = (avg_win * estimated_win_rate - (1 - estimated_win_rate)) / avg_win
                # 켈리 비율을 0-25% 범위로 제한 (보수적 접근)
                kelly_fraction = max(0, min(kelly_fraction, 0.25))
            else:
                kelly_fraction = 0.01  # 기본값
            
            # 7. ATR 기반 변동성 조정 (강화된 로직)
            atr_ratio = atr / current_price
            
            # 변동성에 따른 포지션 크기 조정 (더 세밀한 조정)
            if atr_ratio > 0.05:  # 5% 이상 변동성 (고변동성)
                volatility_adjustment = 0.5  # 50% 축소
            elif atr_ratio > 0.03:  # 3-5% 변동성 (중변동성)
                volatility_adjustment = 0.7  # 30% 축소
            elif atr_ratio > 0.02:  # 2-3% 변동성 (저변동성)
                volatility_adjustment = 0.9  # 10% 축소
            elif atr_ratio > 0.01:  # 1-2% 변동성 (매우 낮은 변동성)
                volatility_adjustment = 1.1  # 10% 증가
            else:  # 1% 미만 변동성 (극히 낮은 변동성)
                volatility_adjustment = 1.3  # 30% 증가
            
            # 8. 최종 포지션 크기 계산
            final_position_size = kelly_fraction * volatility_adjustment * confidence
            final_position_size = max(0.005, min(final_position_size, 0.15))  # 0.5-15% 범위
            
            # 9. 실제 매수 금액 계산
            position_amount_krw = total_balance * final_position_size
            
            # 10. 최소/최대 금액 제한
            from utils import MIN_KRW_ORDER, TAKER_FEE_RATE
            min_amount_with_fee = MIN_KRW_ORDER / (1 + TAKER_FEE_RATE)
            max_amount = min(200000, total_balance * 0.05)  # 최대 20만원 또는 총 자산의 5%
            
            position_amount_krw = max(min_amount_with_fee, min(position_amount_krw, max_amount))
            
            # 11. 수수료를 포함한 실제 주문 금액
            actual_order_amount = position_amount_krw * (1 + TAKER_FEE_RATE)
            
            logger.info(f"💰 {ticker} 켈리 공식 계산 완료:")
            logger.info(f"   - 켈리 비율: {kelly_fraction:.3f} ({kelly_fraction*100:.1f}%)")
            logger.info(f"   - ATR 비율: {atr_ratio:.2%} (변동성)")
            logger.info(f"   - 변동성 조정: {volatility_adjustment:.3f}")
            logger.info(f"   - 최종 포지션: {final_position_size:.3f} ({final_position_size*100:.1f}%)")
            logger.info(f"   - 예상 승률: {estimated_win_rate:.1%}")
            logger.info(f"   - 리스크/리워드: 1:{avg_win/avg_loss:.2f}")
            logger.info(f"   - 매수 금액: {position_amount_krw:,.0f}원")
            
            return {
                'position_amount_krw': position_amount_krw,
                'actual_order_amount': actual_order_amount,
                'kelly_fraction': kelly_fraction,
                'volatility_adjustment': volatility_adjustment,
                'final_position_size': final_position_size,
                'estimated_win_rate': estimated_win_rate,
                'risk_reward_ratio': avg_win / avg_loss if avg_loss > 0 else 0,
                'stop_loss': stop_loss,
                'target_price': target_price,
                'atr': atr
            }
            
        except Exception as e:
            logger.error(f"❌ {ticker} 켈리 공식 계산 중 오류: {str(e)}")
            return self._get_default_kelly_result(total_balance)
    
    def _get_market_data_for_kelly(self, ticker: str) -> dict:
        """켈리 계산을 위한 시장 데이터 조회"""
        try:
            # static_indicators에서 ATR 및 기타 지표 조회
            with self.get_db_connection_safe() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT atr, adx, price, high_60, low_60
                    FROM static_indicators 
                    WHERE ticker = %s
                """, (ticker,))
                
                result = cursor.fetchone()
                if result:
                    atr, adx, price, high_60, low_60 = result
                    return {
                        'atr': atr or 0,
                        'adx': adx or 25,
        
                        'price': price or 0,
                        'high_60': high_60 or 0,
                        'low_60': low_60 or 0
                    }
                else:
                    logger.warning(f"⚠️ {ticker} static_indicators 데이터 없음")
                return None
                
        except Exception as e:
            logger.error(f"❌ {ticker} 시장 데이터 조회 실패: {str(e)}")
            return None

    def _get_default_kelly_result(self, total_balance: float) -> dict:
        """기본 켈리 계산 결과 (오류 시 사용)"""
        from utils import MIN_KRW_ORDER, TAKER_FEE_RATE
        
        min_amount_with_fee = MIN_KRW_ORDER / (1 + TAKER_FEE_RATE)
        actual_order_amount = min_amount_with_fee * (1 + TAKER_FEE_RATE)
        
        return {
            'position_amount_krw': min_amount_with_fee,
            'actual_order_amount': actual_order_amount,
            'kelly_fraction': 0.01,
            'volatility_adjustment': 1.0,
            'final_position_size': 0.01,
            'estimated_win_rate': 0.5,
            'risk_reward_ratio': 1.0,
            'stop_loss': 0,
            'target_price': 0,
            'atr': 0
        }
    
    def get_technical_data_batch(self, tickers: list) -> dict:
        """배치로 여러 티커의 기술적 지표를 한 번에 조회 (성능 최적화)"""
        import time
        start_time = time.time()
        logger.info(f"📊 배치 기술적 지표 조회 시작: {len(tickers)}개 티커")
        
        if not tickers:
            return {}
            
        try:
            with self.get_db_connection_safe() as conn:
                cursor = conn.cursor()
                
                # 단일 배치 쿼리로 모든 티커의 데이터 조회
                placeholders = ','.join(['%s'] * len(tickers))
                
                batch_query = f"""
                    SELECT 
                        s.ticker,
                        s.price, s.atr, s.adx, s.volume_change_7_30, s.supertrend_signal,
                        o.close, o.rsi_14, o.ma_50, o.ma_200, o.bb_upper, o.bb_lower
                    FROM static_indicators s
                    LEFT JOIN LATERAL (
                        SELECT close, rsi_14, ma_50, ma_200, bb_upper, bb_lower
                        FROM ohlcv 
                        WHERE ticker = s.ticker 
                        ORDER BY date DESC 
                        LIMIT 1
                    ) o ON true
                    WHERE s.ticker IN ({placeholders})
                """
                
                cursor.execute(batch_query, tickers)
                results = cursor.fetchall()
                
                # 결과를 딕셔너리로 변환
                batch_data = {}
                for row in results:
                    ticker = row[0]
                    s_price, s_atr, s_adx, s_volume_change, s_supertrend = row[1:6]
                    o_close, o_rsi, o_ma50, o_ma200, o_bb_upper, o_bb_lower = row[6:]
                    
                    # 기본값 설정
                    price = float(s_price or 0)
                    if price == 0 and o_close:
                        price = float(o_close)
                    
                    batch_data[ticker] = {
                        'price': price,
                        'rsi_14': float(o_rsi or 50),
                        'ma_50': float(o_ma50 or 0),
                        'ma_200': float(o_ma200 or 0),
                        'bb_upper': float(o_bb_upper or 0),
                        'bb_lower': float(o_bb_lower or 0),
                        'atr': float(s_atr or 0),
                        'adx': float(s_adx or 25),
                        'volume_change_7_30': float(s_volume_change or 0),
                        'supertrend_signal': s_supertrend or 'neutral'
                    }
                
                execution_time = time.time() - start_time
                query_count_saved = len(tickers) * 2 - 1  # 기존: 티커당 2쿼리, 최적화: 1쿼리
                logger.info(f"✅ 배치 조회 완료: {len(batch_data)}개 티커 ({execution_time:.2f}초)")
                logger.info(f"💰 DB 쿼리 최적화: {query_count_saved}개 쿼리 절약 ({query_count_saved/len(tickers)*2*100:.0f}% 감소)")
                return batch_data
                
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ 배치 기술적 지표 조회 실패: {str(e)} (소요시간: {execution_time:.2f}초)")
            return {}
    
    def _get_default_technical_data(self) -> dict:
        """기본 기술적 지표 데이터 (오류 시 사용)"""
        return {
            'rsi_14': 50,
            'macd_signal': 'neutral',
            'ma_alignment': 'neutral',
            'bb_upper': 0,
            'bb_lower': 0,
            'trend_strength': 0.3,
            'volume_momentum': 0.5,
            'adx': 25,
            'price': 0
        }
    
    def _get_technical_data_for_integration(self, ticker: str) -> dict:
        """통합 포지션 사이징을 위한 기술적 지표 데이터 조회 (수정됨)"""
        try:
            # 🔧 [수정] 실제 존재하는 컬럼으로 쿼리 변경
            with self.get_db_connection_safe() as conn:
                cursor = conn.cursor()
                
                # 최적화된 단일 JOIN 쿼리로 모든 지표 한번에 조회
                cursor.execute("""
                    SELECT 
                        s.price, s.atr, s.adx, s.volume_change_7_30, s.supertrend_signal,
                        o.close, o.rsi_14, o.ma_50, o.ma_200, o.bb_upper, o.bb_lower
                    FROM static_indicators s
                    LEFT JOIN (
                        SELECT ticker, close, rsi_14, ma_50, ma_200, bb_upper, bb_lower
                        FROM ohlcv 
                        WHERE ticker = %s 
                        ORDER BY date DESC 
                        LIMIT 1
                    ) o ON s.ticker = o.ticker
                    WHERE s.ticker = %s
                """, (ticker, ticker))
                
                combined_result = cursor.fetchone()
                
                # 기본값 설정
                price = 0
                rsi_14 = 50
                ma_50 = 0
                ma_200 = 0
                bb_upper = 0
                bb_lower = 0
                atr = 0
                adx = 25
                volume_change_7_30 = 0
                supertrend_signal = 'neutral'
                
                # 최적화된 통합 결과 처리 (2개 쿼리 → 1개 쿼리로 50% 감소)
                if combined_result:
                    s_price, s_atr, s_adx, s_volume_change, s_supertrend, o_close, o_rsi, o_ma50, o_ma200, o_bb_upper, o_bb_lower = combined_result
                    
                    # static_indicators 데이터
                    price = float(s_price or 0)
                    atr = float(s_atr or 0)
                    adx = float(s_adx or 25)
                    volume_change_7_30 = float(s_volume_change or 0)
                    supertrend_signal = s_supertrend or 'neutral'
                    
                    # ohlcv 데이터 처리
                    if o_close:
                        if price == 0:  # static_indicators에 price가 없으면 ohlcv의 close 사용
                            price = float(o_close)
                        rsi_14 = float(o_rsi or 50)
                        ma_50 = float(o_ma50 or 0)
                        ma_200 = float(o_ma200 or 0)
                        bb_upper = float(o_bb_upper or 0)
                        bb_lower = float(o_bb_lower or 0)
                
                # MACD 신호 판단 (ohlcv에 macd 데이터가 없으므로 기본값 사용)
                macd_signal_type = 'neutral'
                
                # 이동평균 정렬 판단
                if price and ma_50 and ma_200 and price > 0 and ma_50 > 0 and ma_200 > 0:
                    if price > ma_50 > ma_200:
                        ma_alignment = 'bullish'
                    elif price < ma_50 < ma_200:
                        ma_alignment = 'bearish'
                    else:
                        ma_alignment = 'neutral'
                else:
                    ma_alignment = 'neutral'
                
                # ADX 기반 추세 강도 판단
                if adx and adx > 0:
                    if adx > 25:
                        trend_strength = min(adx / 50.0, 1.0)  # 0-1 범위로 정규화
                    else:
                        trend_strength = 0.3  # 약한 추세
                else:
                    trend_strength = 0.3
                
                # 볼륨 변화 기반 모멘텀 판단
                if volume_change_7_30 and volume_change_7_30 > 0:
                    volume_momentum = min(volume_change_7_30 / 100.0, 1.0)  # 0-1 범위로 정규화
                else:
                    volume_momentum = 0.5
                
                return {
                    'rsi_14': rsi_14 or 50,
                    'macd_signal': macd_signal_type,
                    'ma_alignment': ma_alignment,
                    'bb_upper': bb_upper or 0,
                    'bb_lower': bb_lower or 0,
                    'trend_strength': trend_strength,
                    'volume_momentum': volume_momentum,
                    'adx': adx or 25,
                    'price': price or 0
                }
                    
        except Exception as e:
            logger.error(f"❌ {ticker} 기술적 지표 데이터 조회 실패: {str(e)}")
            return self._get_default_technical_data()
    
    def _get_market_conditions_for_integration(self) -> dict:
        """통합 포지션 사이징을 위한 시장 상황 데이터 조회 (확장)"""
        try:
            # 시장 전체 상황 분석
            with self.get_db_connection_safe() as conn:
                cursor = conn.cursor()
                
                # 1. 전체 시장 변동성 분석 (ATR 기반)
                cursor.execute("""
                    SELECT AVG(atr) as avg_atr, STDDEV(atr) as atr_std
                    FROM static_indicators 
                    WHERE atr IS NOT NULL AND atr > 0
                """)
                atr_result = cursor.fetchone()
                
                if atr_result and atr_result[0]:
                    avg_atr, atr_std = atr_result
                    if atr_std and atr_std > 0:
                        # 현재 ATR이 평균 대비 어느 정도인지 계산
                        atr_z_score = (avg_atr - atr_std) / atr_std if atr_std > 0 else 0
                        if atr_z_score > 1.5:
                            market_volatility = 'high'
                        elif atr_z_score < -1.5:
                            market_volatility = 'low'
                        else:
                            market_volatility = 'normal'
                    else:
                        market_volatility = 'normal'
                else:
                    market_volatility = 'normal'
                
                # 2. 전체 시장 추세 강도 분석 (MA200 기울기 기반)
                cursor.execute("""
                    SELECT 
                           COUNT(*) as total_count,
                           COUNT(CASE WHEN adx > 20 THEN 1 END) as strong_trend_count,
                           COUNT(CASE WHEN adx <= 20 THEN 1 END) as weak_trend_count
                    FROM static_indicators 
                    WHERE adx IS NOT NULL
                """)
                trend_result = cursor.fetchone()
                
                if trend_result and trend_result[0] is not None:
                    avg_slope, positive_count, total_count = trend_result
                    if total_count > 0:
                        # 상승 종목 비율
                        bullish_ratio = positive_count / total_count
                        # 추세 강도 (0-1 범위)
                        trend_strength = min(max(bullish_ratio, 0.3), 0.8)
                    else:
                        trend_strength = 0.5
                else:
                    trend_strength = 0.5
                
                # 3. 시장 센티먼트 분석 (ADX 기반)
                cursor.execute("""
                    SELECT AVG(adx) as avg_adx
                    FROM static_indicators 
                    WHERE adx IS NOT NULL AND adx > 0
                """)
                adx_result = cursor.fetchone()
                
                if adx_result and adx_result[0]:
                    avg_adx = adx_result[0]
                    if avg_adx > 30:
                        market_sentiment = 'strong_trend'
                    elif avg_adx > 20:
                        market_sentiment = 'moderate_trend'
                    else:
                        market_sentiment = 'weak_trend'
                else:
                    market_sentiment = 'neutral'
                
                # 4. 거래량 트렌드 분석
                cursor.execute("""
                    SELECT AVG(volume_change_7_30) as avg_volume_change
                    FROM static_indicators 
                    WHERE volume_change_7_30 IS NOT NULL
                """)
                volume_result = cursor.fetchone()
                
                if volume_result and volume_result[0]:
                    avg_volume_change = volume_result[0]
                    if avg_volume_change > 50:
                        volume_trend = 'high'
                    elif avg_volume_change < -20:
                        volume_trend = 'low'
                    else:
                        volume_trend = 'normal'
                else:
                    volume_trend = 'normal'
                
                return {
                    'market_volatility': market_volatility,
                    'trend_strength': round(trend_strength, 3),
                    'market_sentiment': market_sentiment,
                    'volume_trend': volume_trend,
                    'avg_atr': round(avg_atr, 4) if atr_result and atr_result[0] else 0.02,
                    'bullish_ratio': round(bullish_ratio, 3) if 'bullish_ratio' in locals() else 0.5,
                    'avg_adx': round(avg_adx, 1) if adx_result and adx_result[0] else 25,
                    'avg_volume_change': round(avg_volume_change, 1) if volume_result and volume_result[0] else 0
                }
                
        except Exception as e:
            logger.error(f"❌ 시장 상황 데이터 조회 실패: {str(e)}")
            return {
                'market_volatility': 'normal',
                'trend_strength': 0.5,
                'market_sentiment': 'neutral',
                'volume_trend': 'normal',
                'avg_atr': 0.02,
                'bullish_ratio': 0.5,
                'avg_adx': 25,
                'avg_volume_change': 0
            }

    def get_ohlcv_from_db(self, ticker: str, limit: int = 450) -> pd.DataFrame:
        """DB에서 OHLCV 데이터를 최근 날짜순으로 정확히 조회"""
        try:
            with self.get_db_connection_safe() as conn:
                # 최근 데이터부터 내림차순으로 조회 후 다시 오름차순 정렬
                query = """
                SELECT date, open, high, low, close, volume
                FROM (
                    SELECT date, open, high, low, close, volume
                    FROM ohlcv 
                    WHERE ticker = %s 
                    ORDER BY date DESC 
                    LIMIT %s
                ) subquery
                ORDER BY date ASC
                """
                
                df = pd.read_sql_query(query, conn, params=[ticker, limit])
                
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    
                    # 검증 로그
                    from utils import safe_strftime
                    start_date = safe_strftime(df.index[0])
                    end_date = safe_strftime(df.index[-1])
                    logger.debug(f"🔍 {ticker} DB 조회 완료: {len(df)}개 ({start_date} ~ {end_date})")
                    
                return df
                
        except Exception as e:
            logger.error(f"❌ {ticker} DB 조회 실패: {str(e)}")
            return pd.DataFrame()

    def _check_pyramiding_for_existing_position(self, ticker: str, score: float, confidence: float) -> dict:
        """
        기존 보유 종목에 대한 피라미딩 조건 점검
        
        Args:
            ticker (str): 티커명
            score (float): GPT 분석 점수
            confidence (float): GPT 분석 신뢰도
            
        Returns:
            dict: 피라미딩 조건 점검 결과
        """
        try:
            # 현재가 조회 먼저 수행
            current_price = get_current_price_safe(ticker)
            if not current_price:
                return {
                    'should_pyramid': False,
                    'reason': '현재가 조회 실패',
                    'type': 'price_fetch_failed'
                }
            
            # 🔧 [핵심 수정] 상세 분석을 먼저 수행하여 실제 조건 확인
            pyramid_analysis = self._analyze_pyramiding_conditions(ticker, current_price, score, confidence)
            
            # 분석 결과에서 충족된 조건 개수 확인
            met_conditions_count = pyramid_analysis.get('met_conditions', 0)
            total_conditions = pyramid_analysis.get('total_conditions', 4)
            
            # 🔧 [핵심 수정] 조건 충족 판단을 reason 텍스트로도 확인
            reason_text = pyramid_analysis.get('reason', '')
            conditions_met_by_reason = "피라미딩 조건 충족:" in reason_text
            
            # 🔧 [수정] 최소 2개 조건 충족 시 피라미딩 실행 결정
            should_pyramid_by_analysis = met_conditions_count >= 2 or conditions_met_by_reason
            
            logger.info(f"🔍 {ticker} 조건 분석 상세: 충족={met_conditions_count}, 전체={total_conditions}, reason_check={conditions_met_by_reason}, 실행결정={should_pyramid_by_analysis}")
            logger.info(f"🔍 {ticker} 분석 사유: {pyramid_analysis.get('reason', 'Unknown')}")
            
            if should_pyramid_by_analysis:
                # 조건이 충족되었으면 포트폴리오 매니저를 통해 실제 실행
                logger.info(f"✅ {ticker} 피라미딩 조건 분석 결과: 조건 충족 확인됨")
                
                # 🔧 [수정] 실제 피라미딩 매수 실행 시도 - 자동 실행 먼저 시도
                pyramid_executed = self.pm.check_pyramiding(ticker)
                
                if pyramid_executed:
                    return {
                        'should_pyramid': True,
                        'reason': f'피라미딩 조건 충족 및 자동 실행 완료: {pyramid_analysis["reason"]}',
                        'type': 'pyramiding_executed',
                        'details': pyramid_analysis
                    }
                else:
                    # 🔧 [핵심 수정] 자동 실행 실패 시에도 조건은 충족되었으므로 수동 실행 진행
                    logger.warning(f"⚠️ {ticker} 피라미딩 자동 실행 실패, 하지만 조건 충족으로 수동 실행 진행")
                    return {
                        'should_pyramid': True,
                        'reason': f'피라미딩 조건 충족 (수동 실행): {pyramid_analysis["reason"]}',
                        'type': 'manual_execution_needed',
                        'details': pyramid_analysis
                    }
            else:
                # 조건 미충족
                logger.debug(f"📊 {ticker} 피라미딩 조건 미충족: {met_conditions_count}/{total_conditions}개 조건만 충족")
                return {
                    'should_pyramid': False,
                    'reason': pyramid_analysis['reason'],
                    'type': 'conditions_not_met',
                    'details': pyramid_analysis
                }
                
        except Exception as e:
            logger.error(f"❌ {ticker} 피라미딩 조건 점검 중 오류: {e}")
            return {
                'should_pyramid': False,
                'reason': f'피라미딩 조건 점검 실패: {str(e)}',
                'type': 'error'
            }

    def _analyze_pyramiding_conditions(self, ticker: str, current_price: float, score: float, confidence: float) -> dict:
        """
        피라미딩 조건 상세 분석
        
        Args:
            ticker (str): 티커명
            current_price (float): 현재가
            score (float): GPT 분석 점수
            confidence (float): GPT 분석 신뢰도
            
        Returns:
            dict: 피라미딩 조건 분석 결과
        """
        try:
            # 현재 보유 포지션 정보 조회
            current_positions = self.pm.get_current_positions()
            position_info = next((pos for pos in current_positions if pos['ticker'] == ticker), None)
            
            if not position_info:
                return {
                    'reason': '보유 포지션 정보 조회 실패',
                    'conditions_checked': []
                }
            
            avg_price = position_info.get('avg_buy_price', 0)
            current_value = position_info.get('value', 0)
            
            # 피라미딩 조건들 점검
            conditions_checked = []
            
            # 1. 저항선 돌파 조건
            resistance_breakout = self._check_resistance_breakout(ticker, current_price)
            conditions_checked.append({
                'condition': '저항선 돌파',
                'met': resistance_breakout['met'],
                'details': resistance_breakout['details']
            })
            
            # 2. 전고점 돌파 조건
            high_breakout = self._check_high_breakout(ticker, current_price)
            conditions_checked.append({
                'condition': '전고점 돌파',
                'met': high_breakout['met'],
                'details': high_breakout['details']
            })
            
            # 3. 기술적 지표 조건
            technical_conditions = self._check_technical_pyramiding_conditions(ticker, current_price)
            conditions_checked.append({
                'condition': '기술적 지표',
                'met': technical_conditions['met'],
                'details': technical_conditions['details']
            })
            
            # 4. 수익률 조건
            return_rate = ((current_price - avg_price) / avg_price * 100) if avg_price > 0 else 0
            profit_condition = return_rate >= 5.0  # 5% 이상 수익 시에만 피라미딩 고려
            conditions_checked.append({
                'condition': '수익률 조건',
                'met': profit_condition,
                'details': f'현재 수익률: {return_rate:.1f}% (기준: 5.0%)'
            })
            
            # 조건 충족 여부 판단
            met_conditions = [c for c in conditions_checked if c['met']]
            unmet_conditions = [c for c in conditions_checked if not c['met']]
            
            if len(met_conditions) >= 2:  # 최소 2개 조건 충족 시 피라미딩 고려
                reason = f"피라미딩 조건 충족: {', '.join([c['condition'] for c in met_conditions])}"
            else:
                reason = f"피라미딩 조건 미충족: {', '.join([c['condition'] for c in unmet_conditions])}"
            
            return {
                'reason': reason,
                'conditions_checked': conditions_checked,
                'met_conditions': len(met_conditions),
                'total_conditions': len(conditions_checked)
            }
            
        except Exception as e:
            logger.error(f"❌ {ticker} 피라미딩 조건 분석 중 오류: {e}")
            return {
                'reason': f'피라미딩 조건 분석 실패: {str(e)}',
                'conditions_checked': [],
                'met_conditions': 0,
                'total_conditions': 4
            }

    def _check_resistance_breakout(self, ticker: str, current_price: float) -> dict:
        """
        저항선 돌파 조건 점검
        
        Args:
            ticker (str): 티커명
            current_price (float): 현재가
            
        Returns:
            dict: 저항선 돌파 조건 점검 결과
        """
        try:
            # 최근 OHLCV 데이터 조회
            ohlcv_data = self.get_ohlcv_from_db(ticker, limit=50)
            if ohlcv_data is None or ohlcv_data.empty:
                return {
                    'met': False,
                    'details': 'OHLCV 데이터 없음'
                }
            
            # 최근 고점들 계산 (20일 기준)
            recent_highs = ohlcv_data['high'].rolling(window=20).max()
            current_high = recent_highs.iloc[-1]
            
            # 저항선 돌파 여부 (현재가가 최근 고점을 1% 이상 돌파)
            breakout_threshold = current_high * 1.01
            is_breakout = current_price > breakout_threshold
            
            return {
                'met': is_breakout,
                'details': f'현재가: {current_price:,.0f}원, 저항선: {breakout_threshold:,.0f}원, 돌파: {is_breakout}'
            }
            
        except Exception as e:
            logger.error(f"❌ {ticker} 저항선 돌파 조건 점검 중 오류: {e}")
            return {
                'met': False,
                'details': f'점검 실패: {str(e)}'
            }

    def _check_high_breakout(self, ticker: str, current_price: float) -> dict:
        """
        전고점 돌파 조건 점검
        
        Args:
            ticker (str): 티커명
            current_price (float): 현재가
            
        Returns:
            dict: 전고점 돌파 조건 점검 결과
        """
        try:
            # 최근 OHLCV 데이터 조회 (더 긴 기간)
            ohlcv_data = self.get_ohlcv_from_db(ticker, limit=100)
            if ohlcv_data is None or ohlcv_data.empty:
                return {
                    'met': False,
                    'details': 'OHLCV 데이터 없음'
                }
            
            # 최근 100일 고점
            historical_high = ohlcv_data['high'].max()
            
            # 전고점 돌파 여부 (현재가가 전고점을 0.5% 이상 돌파)
            breakout_threshold = historical_high * 1.005
            is_breakout = current_price > breakout_threshold
            
            return {
                'met': is_breakout,
                'details': f'현재가: {current_price:,.0f}원, 전고점: {historical_high:,.0f}원, 돌파: {is_breakout}'
            }
            
        except Exception as e:
            logger.error(f"❌ {ticker} 전고점 돌파 조건 점검 중 오류: {e}")
            return {
                'met': False,
                'details': f'점검 실패: {str(e)}'
            }

    def _check_technical_pyramiding_conditions(self, ticker: str, current_price: float) -> dict:
        """
        기술적 지표 기반 피라미딩 조건 점검
        
        Args:
            ticker (str): 티커명
            current_price (float): 현재가
            
        Returns:
            dict: 기술적 지표 조건 점검 결과
        """
        try:
            # 시장 데이터 조회
            market_data = self._get_market_data_for_kelly(ticker)
            if not market_data:
                return {
                    'met': False,
                    'details': '시장 데이터 조회 실패'
                }
            
            conditions_met = []
            conditions_failed = []
            
            # 1. Supertrend 매수 신호
            supertrend_signal = market_data.get('supertrend_signal', '')
            if supertrend_signal == 'bull':
                conditions_met.append('Supertrend 매수')
            else:
                conditions_failed.append(f'Supertrend: {supertrend_signal}')
            
            # 2. ADX 강도 (25 이상)
            adx = market_data.get('adx', 0)
            if adx > 25:
                conditions_met.append('ADX 강함')
            else:
                conditions_failed.append(f'ADX: {adx:.1f}')
            
            # 3. RSI 과매수 방지 (75 미만)
            rsi = market_data.get('rsi', 50)
            if rsi < 75:
                conditions_met.append('RSI 정상')
            else:
                conditions_failed.append(f'RSI 과매수: {rsi:.1f}')
            
            # 4. MA20 상승
            ma20 = market_data.get('ma20', current_price)
            if current_price > ma20:
                conditions_met.append('MA20 상승')
            else:
                conditions_failed.append(f'MA20 하락: {current_price:,.0f} < {ma20:,.0f}')
            
            # 최소 3개 조건 충족 시 통과
            is_met = len(conditions_met) >= 3
            
            return {
                'met': is_met,
                'details': f'충족: {conditions_met}, 미충족: {conditions_failed}'
            }
            
        except Exception as e:
            logger.error(f"❌ {ticker} 기술적 지표 조건 점검 중 오류: {e}")
            return {
                'met': False,
                'details': f'점검 실패: {str(e)}'
            }

    def _execute_pyramiding_buy(self, ticker: str, score: float, confidence: float, trade_logs: list, total_balance: float):
        """
        피라미딩 매수 실행
        
        Args:
            ticker (str): 티커명
            score (float): GPT 분석 점수
            confidence (float): GPT 분석 신뢰도
            trade_logs (list): 거래 로그 리스트
            total_balance (float): 총 자산
        """
        try:
            current_price = get_current_price_safe(ticker)
            if not current_price or current_price <= 0:
                logger.warning(f"⚠️ {ticker} 현재가 조회 실패")
                trade_logs.append({
                    "ticker": ticker,
                    "buy_price": 0,
                    "score": score,
                    "confidence": confidence,
                    "trade_amount_krw": 0,
                    "status": "PRICE_FETCH_FAILED",
                    "error_msg": "현재가 조회 실패"
                })
                return
            
            # 피라미딩 전용 포지션 사이징 (기존보다 작은 크기)
            pyramid_position_size = 0.01  # 총 자산의 1% (기본 피라미딩 크기)
            trade_amount_krw = total_balance * pyramid_position_size
            
            # 최소/최대 금액 제한 적용
            from utils import MIN_KRW_ORDER, TAKER_FEE_RATE
            min_amount_with_fee = MIN_KRW_ORDER / (1 + TAKER_FEE_RATE)
            max_amount = min(50000, total_balance * 0.02)  # 피라미딩은 최대 5만원 또는 2%
            trade_amount_krw = max(min_amount_with_fee, min(trade_amount_krw, max_amount))
            
            # 수수료를 포함한 실제 주문 금액
            actual_order_amount = trade_amount_krw * (1 + TAKER_FEE_RATE)
            
            logger.info(f"🔼 피라미딩 매수 시도: {ticker} | 점수: {score} | 신뢰도: {confidence:.2f} | 포지션: {pyramid_position_size:.1%} | 금액: {trade_amount_krw:,.0f}원")
            
            # 매수 실행
            from trade_executor import buy_asset
            buy_result = buy_asset(
                upbit_client=self.upbit,
                ticker=ticker,
                current_price=current_price,
                trade_amount_krw=actual_order_amount,
                gpt_confidence=confidence,
                gpt_reason=f"피라미딩 매수 - GPT 분석 점수: {score}점, 신뢰도: {confidence:.2f}"
            )
            
            if buy_result.get("status") in ["SUCCESS", "SUCCESS_PARTIAL", "SUCCESS_PARTIAL_NO_AVG", "SUCCESS_NO_AVG_PRICE"]:
                buy_price = buy_result.get('price', current_price)
                status_msg = "피라미딩 매수 성공" if buy_result.get("status") == "SUCCESS" else "피라미딩 매수 부분 체결 성공"
                logger.info(f"🔼 {status_msg}: {ticker} | 체결가: {buy_price:.2f} | 금액: {trade_amount_krw:,.0f}원")
                
                trade_logs.append({
                    "ticker": ticker,
                    "buy_price": buy_price,
                    "score": score,
                    "confidence": confidence,
                    "trade_amount_krw": trade_amount_krw,
                    "status": "PYRAMIDING_SUCCESS",
                    "error_msg": None
                })
            else:
                error_msg = buy_result.get('error', 'Unknown')
                logger.warning(f"⚠️ 피라미딩 매수 실패: {ticker} | 오류: {error_msg}")
                
                trade_logs.append({
                    "ticker": ticker,
                    "buy_price": current_price,
                    "score": score,
                    "confidence": confidence,
                    "trade_amount_krw": trade_amount_krw,
                    "status": "PYRAMIDING_FAILED",
                    "error_msg": error_msg
                })
                
        except Exception as e:
            logger.error(f"❌ {ticker} 피라미딩 매수 실행 중 오류: {str(e)}")
            trade_logs.append({
                "ticker": ticker,
                "buy_price": 0,
                "score": score,
                "confidence": confidence,
                "trade_amount_krw": 0,
                "status": "PYRAMIDING_ERROR",
                "error_msg": str(e)
            })

    def _execute_new_position_buy(self, ticker: str, score: float, confidence: float, trade_logs: list, total_balance: float):
        """
        신규 종목 매수 실행 (기존 로직)
        
        Args:
            ticker (str): 티커명
            score (float): GPT 분석 점수
            confidence (float): GPT 분석 신뢰도
            trade_logs (list): 거래 로그 리스트
            total_balance (float): 총 자산
        """
        try:
            current_price = get_current_price_safe(ticker)
            if not current_price or current_price <= 0:
                logger.warning(f"⚠️ {ticker} 현재가 조회 실패")
                trade_logs.append({
                    "ticker": ticker,
                    "buy_price": 0,
                    "score": score,
                    "confidence": confidence,
                    "trade_amount_krw": 0,
                    "status": "PRICE_FETCH_FAILED",
                    "error_msg": "현재가 조회 실패"
                })
                return
            
            # 기존의 통합 포지션 사이징 로직 사용
            kelly_result = self.calculate_kelly_position_size(
                ticker=ticker,
                score=score,
                confidence=confidence,
                current_price=current_price,
                total_balance=total_balance
            )
            
            # 기술적 지표 데이터 수집
            technical_data = self._get_technical_data_for_integration(ticker)
            market_conditions = self._get_market_conditions_for_integration()
            
            # 통합 포지션 사이징 계산
            try:
                from strategy_analyzer import calculate_integrated_position_size
                
                kelly_params = {
                    'kelly_fraction': kelly_result['kelly_fraction'],
                    'estimated_win_rate': kelly_result['estimated_win_rate'],
                    'risk_reward_ratio': kelly_result['risk_reward_ratio']
                }
                
                atr_params = {
                    'atr': kelly_result['atr'],
                    'current_price': current_price
                }
                
                integrated_result = calculate_integrated_position_size(
                    technical_data=technical_data,
                    kelly_params=kelly_params,
                    atr_params=atr_params,
                    market_conditions=market_conditions
                )
                
                integrated_position_size = integrated_result['final_position_size']
                trade_amount_krw = total_balance * integrated_position_size
                
            except Exception as e:
                logger.warning(f"⚠️ {ticker} 통합 포지션 사이징 실패, 기본 켈리 공식 사용: {e}")
                integrated_position_size = kelly_result['final_position_size']
                trade_amount_krw = total_balance * integrated_position_size
            
            # 최소/최대 금액 제한 적용
            from utils import MIN_KRW_ORDER, TAKER_FEE_RATE
            min_amount_with_fee = MIN_KRW_ORDER / (1 + TAKER_FEE_RATE)
            max_amount = min(200000, total_balance * 0.05)
            trade_amount_krw = max(min_amount_with_fee, min(trade_amount_krw, max_amount))
            
            # 수수료를 포함한 실제 주문 금액
            actual_order_amount = trade_amount_krw * (1 + TAKER_FEE_RATE)
            
            # 시장 상황 기반 추가 검증
            market_validation = self._validate_market_conditions(ticker, current_price, score, confidence)
            if not market_validation['valid']:
                logger.warning(f"⚠️ 시장 조건 검증 실패: {ticker} | 사유: {market_validation['reason']}")
                trade_logs.append({
                    "ticker": ticker,
                    "buy_price": current_price,
                    "score": score,
                    "confidence": confidence,
                    "trade_amount_krw": trade_amount_krw,
                    "status": "MARKET_VALIDATION_FAILED",
                    "error_msg": market_validation['reason']
                })
                return
            
            logger.info(f"🆕 신규 매수 시도: {ticker} | 점수: {score} | 신뢰도: {confidence:.2f} | 통합포지션: {integrated_position_size:.1%} | 금액: {trade_amount_krw:,.0f}원")
            
            # 매수 실행
            from trade_executor import buy_asset
            buy_result = buy_asset(
                upbit_client=self.upbit,
                ticker=ticker,
                current_price=current_price,
                trade_amount_krw=actual_order_amount,
                gpt_confidence=confidence,
                gpt_reason=f"신규 매수 - GPT 분석 점수: {score}점, 신뢰도: {confidence:.2f}"
            )
            
            if buy_result.get("status") in ["SUCCESS", "SUCCESS_PARTIAL", "SUCCESS_PARTIAL_NO_AVG", "SUCCESS_NO_AVG_PRICE"]:
                buy_price = buy_result.get('price', current_price)
                status_msg = "신규 매수 성공" if buy_result.get("status") == "SUCCESS" else "신규 매수 부분 체결 성공"
                logger.info(f"💰 {status_msg}: {ticker} | 체결가: {buy_price:.2f} | 금액: {trade_amount_krw:,.0f}원")
                
                trade_logs.append({
                    "ticker": ticker,
                    "buy_price": buy_price,
                    "score": score,
                    "confidence": confidence,
                    "trade_amount_krw": trade_amount_krw,
                    "status": "SUCCESS",
                    "error_msg": None
                })
            else:
                error_msg = buy_result.get('error', 'Unknown')
                logger.warning(f"⚠️ 신규 매수 실패: {ticker} | 오류: {error_msg}")
                
                error_analysis = self._analyze_buy_error(error_msg, ticker, current_price, trade_amount_krw)
                
                trade_logs.append({
                    "ticker": ticker,
                    "buy_price": current_price,
                    "score": score,
                    "confidence": confidence,
                    "trade_amount_krw": trade_amount_krw,
                    "status": "FAILED",
                    "error_msg": f"{error_msg} | 분석: {error_analysis}"
                })
                
        except Exception as e:
            logger.error(f"❌ {ticker} 신규 매수 실행 중 오류: {str(e)}")
            trade_logs.append({
                "ticker": ticker,
                "buy_price": 0,
                "score": score,
                "confidence": confidence,
                "trade_amount_krw": 0,
                "status": "ERROR",
                "error_msg": str(e)
            })

def main():
    """메인 실행 함수"""
    import time
    
    start_time = time.time()
    
    try:
        logger.info("="*50)
        logger.info("🚀 makenaide 시작")
        logger.info("="*50)
        
        # MakenaideBot 인스턴스 생성 및 실행
        try:
            bot = MakenaideBot()
        except Exception as e:
            logger.error(f"❌ MakenaideBot 인스턴스 생성 실패: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return False

        try:
            success = bot.run()
        except Exception as e:
            logger.error(f"❌ MakenaideBot 실행 중 오류 발생: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return False
        
        # 실행 결과 요약
        total_time = time.time() - start_time
        hours, remainder = divmod(total_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        time_str = f"{int(hours)}시간 {int(minutes)}분 {seconds:.1f}초" if hours > 0 else f"{int(minutes)}분 {seconds:.1f}초"
        
        if success:
            logger.info("="*50)
            logger.info(f"✅ makenaide 성공적으로 완료 (총 소요시간: {time_str})")
            logger.info("="*50)
        else:
            logger.warning("="*50)
            logger.warning(f"⚠️ makenaide 실행 중 일부 과정 실패 (총 소요시간: {time_str})")
            logger.warning("="*50)
        
    except Exception as e:
        logger.error("="*50)
        logger.error(f"❌ makenaide 실행 중 예상치 못한 오류 발생: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        logger.error("="*50)
        return False
        
    return True

if __name__ == "__main__":
    main()