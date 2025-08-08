#!/usr/bin/env python3
"""
Lambda DB Initializer - Phase 2 아키텍처 개선
Makenaide 봇의 PostgreSQL 스키마 초기화를 독립적인 Lambda 함수로 분리

주요 기능:
1. DB 연결 상태 확인
2. 스키마 존재 여부 검증
3. 필수 테이블 및 인덱스 생성
4. 데이터 마이그레이션 처리
5. 스키마 버전 관리

Author: Phase 2 Architecture Migration
Version: 1.0.0
"""

import json
import logging
import time
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# AWS Lambda 환경 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DBInitializerConfig:
    """DB 초기화 설정 클래스"""
    
    # DB 연결 설정
    DB_CONFIG = {
        'host': os.environ.get('DB_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com'),
        'port': int(os.environ.get('DB_PORT', '5432')),
        'database': os.environ.get('DB_NAME', 'makenaide'),
        'user': os.environ.get('DB_USER', 'bruce'),
        'password': os.environ.get('DB_PASSWORD', '0asis314.')
    }
    
    # 스키마 버전 관리
    SCHEMA_VERSION = "2.0.0"
    
    # 필수 테이블 목록 (의존성 순서)
    CORE_TABLES = [
        'tickers',
        'ohlcv', 
        'ohlcv_4h',
        'static_indicators',
        'market_data_4h',
        'trade_log',
        'trade_history',
        'portfolio_history',
        'trailing_stops',
        'trend_analysis',
        'trend_analysis_log',
        'performance_summary',
        'strategy_performance',
        'manual_override_log',
        'disclaimer_agreements'
    ]
    
    # 백테스트 테이블 목록
    BACKTEST_TABLES = [
        'backtest_sessions',
        'backtest_ohlcv',
        'backtest_results',
        'backtest_trades'
    ]

class DatabaseManager:
    """Lambda 환경용 DB 매니저"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        
    def get_connection(self):
        """DB 연결 획득"""
        if self.connection is None:
            import psycopg2
            try:
                self.connection = psycopg2.connect(**self.config)
                self.connection.autocommit = False  # 트랜잭션 제어
                logger.info("✅ PostgreSQL 연결 성공")
            except Exception as e:
                logger.error(f"❌ DB 연결 실패: {e}")
                raise
        return self.connection
    
    def execute_query(self, query: str, params: tuple = None, fetchone: bool = False, fetchall: bool = False):
        """쿼리 실행 (트랜잭션 지원)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            
            if fetchone:
                return cursor.fetchone()
            elif fetchall:
                return cursor.fetchall()
            else:
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"❌ 쿼리 실행 실패: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
    
    def commit(self):
        """트랜잭션 커밋"""
        if self.connection:
            self.connection.commit()
    
    def rollback(self):
        """트랜잭션 롤백"""
        if self.connection:
            self.connection.rollback()
    
    def close(self):
        """연결 종료"""
        if self.connection:
            self.connection.close()
            self.connection = None

class SchemaValidator:
    """스키마 검증 및 상태 확인"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def check_table_exists(self, table_name: str) -> bool:
        """테이블 존재 여부 확인"""
        try:
            result = self.db.execute_query(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
                """,
                (table_name,),
                fetchone=True
            )
            return result[0] if result else False
        except Exception as e:
            logger.error(f"❌ 테이블 존재 확인 실패 ({table_name}): {e}")
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """테이블 레코드 수 조회"""
        try:
            result = self.db.execute_query(
                f"SELECT COUNT(*) FROM {table_name}",
                fetchone=True
            )
            return result[0] if result else 0
        except Exception as e:
            logger.warning(f"⚠️ 테이블 레코드 수 조회 실패 ({table_name}): {e}")
            return 0
    
    def validate_schema_integrity(self) -> Dict[str, Any]:
        """스키마 무결성 검증"""
        results = {
            'core_tables': {},
            'backtest_tables': {},
            'missing_tables': [],
            'schema_health': 'healthy'
        }
        
        # 핵심 테이블 검증
        for table in DBInitializerConfig.CORE_TABLES:
            exists = self.check_table_exists(table)
            row_count = self.get_table_row_count(table) if exists else 0
            
            results['core_tables'][table] = {
                'exists': exists,
                'row_count': row_count
            }
            
            if not exists:
                results['missing_tables'].append(table)
        
        # 백테스트 테이블 검증
        for table in DBInitializerConfig.BACKTEST_TABLES:
            exists = self.check_table_exists(table)
            row_count = self.get_table_row_count(table) if exists else 0
            
            results['backtest_tables'][table] = {
                'exists': exists,
                'row_count': row_count
            }
        
        # 스키마 상태 평가
        if results['missing_tables']:
            results['schema_health'] = 'incomplete'
        
        return results

class SchemaInitializer:
    """스키마 초기화 및 생성"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def create_disclaimer_table(self) -> Tuple[str, List[str]]:
        """Disclaimer 동의 테이블 생성"""
        
        table_sql = """
            CREATE TABLE IF NOT EXISTS disclaimer_agreements (
                id SERIAL PRIMARY KEY,
                agreement_version VARCHAR(10) NOT NULL,
                agreed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                agreed_by VARCHAR(100) DEFAULT 'system',
                ip_address INET,
                user_agent TEXT,
                agreement_text_hash VARCHAR(64),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """
        
        indexes = [
            """
            CREATE INDEX IF NOT EXISTS idx_disclaimer_agreements_version 
            ON disclaimer_agreements(agreement_version)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_disclaimer_agreements_active 
            ON disclaimer_agreements(is_active)
            """
        ]
        
        return table_sql, indexes
    
    def create_backtest_tables(self) -> List[str]:
        """백테스트 전용 테이블들 생성"""
        
        return [
            # 백테스트 세션 관리 테이블
            """
            CREATE TABLE IF NOT EXISTS backtest_sessions (
                session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(100) NOT NULL,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                data_snapshot_date TIMESTAMP DEFAULT NOW(),
                description TEXT,
                status VARCHAR(20) DEFAULT 'active',
                created_at TIMESTAMP DEFAULT NOW()
            )
            """,
            
            # 백테스트 전용 OHLCV 테이블
            """
            CREATE TABLE IF NOT EXISTS backtest_ohlcv (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                date DATE NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(ticker, date)
            )
            """,
            
            # 백테스트 결과 저장 테이블
            """
            CREATE TABLE IF NOT EXISTS backtest_results (
                id SERIAL PRIMARY KEY,
                session_id UUID REFERENCES backtest_sessions(session_id),
                strategy_name VARCHAR(100) NOT NULL,
                combo_name VARCHAR(100),
                period_start DATE,
                period_end DATE,
                -- 성과 지표
                win_rate DECIMAL(5,4),
                avg_return DECIMAL(10,6),
                mdd DECIMAL(5,4),  -- Max Drawdown
                total_trades INTEGER,
                winning_trades INTEGER,
                losing_trades INTEGER,
                -- 켈리 공식 관련
                kelly_fraction DECIMAL(5,4),
                kelly_1_2 DECIMAL(5,4),
                -- 추가 지표
                b_value DECIMAL(10,6),
                swing_score DECIMAL(10,6),
                sharpe_ratio DECIMAL(10,6),
                sortino_ratio DECIMAL(10,6),
                profit_factor DECIMAL(10,6),
                -- 메타데이터
                parameters JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """,
            
            # 백테스트 결과 상세 테이블 (개별 거래 기록)
            """
            CREATE TABLE IF NOT EXISTS backtest_trades (
                id SERIAL PRIMARY KEY,
                result_id INTEGER REFERENCES backtest_results(id),
                ticker VARCHAR(20),
                entry_date DATE,
                exit_date DATE,
                entry_price DECIMAL(15,8),
                exit_price DECIMAL(15,8),
                quantity DECIMAL(20,8),
                pnl DECIMAL(15,8),
                return_pct DECIMAL(10,6),
                hold_days INTEGER,
                strategy_signal TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """,
            
            # 백테스트 인덱스들
            """
            CREATE INDEX IF NOT EXISTS idx_backtest_ohlcv_ticker_date 
            ON backtest_ohlcv(ticker, date DESC)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_backtest_sessions_created_at 
            ON backtest_sessions(created_at)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_backtest_sessions_status 
            ON backtest_sessions(status)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_backtest_results_session 
            ON backtest_results(session_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_backtest_results_strategy 
            ON backtest_results(strategy_name)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_backtest_results_created 
            ON backtest_results(created_at)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_backtest_trades_result 
            ON backtest_trades(result_id)
            """
        ]
    
    def create_core_schema(self) -> bool:
        """핵심 스키마 생성"""
        try:
            logger.info("🚀 핵심 스키마 생성 시작")
            
            # 메인 스키마 SQL (기존 init_db_pg.py에서 추출)
            main_schema_sql = """
            CREATE TABLE IF NOT EXISTS performance_summary (
                id SERIAL PRIMARY KEY,
                period_start DATE,
                period_end DATE,
                initial_cash REAL,
                final_valuation REAL,
                net_profit REAL,
                win_rate REAL,
                profit_factor REAL,
                max_drawdown REAL,
                num_trades INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trade_log (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                action TEXT NOT NULL,
                qty REAL,
                price REAL,
                buy_price REAL,
                score REAL,
                confidence REAL,
                trade_amount_krw REAL,
                bought_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                error_msg TEXT,
                kelly_ratio REAL,
                swing_score REAL,
                strategy_combo TEXT,
                executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS strategy_performance (
                id SERIAL PRIMARY KEY,
                strategy_combo TEXT NOT NULL,
                period_start DATE,
                period_end DATE,
                win_rate REAL,
                avg_return REAL,
                mdd REAL,
                num_trades INTEGER,
                kelly_ratio REAL,
                swing_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trend_analysis (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                score INTEGER,
                confidence REAL,
                action TEXT,
                market_phase TEXT,
                pattern TEXT,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_trend_analysis_ticker UNIQUE (ticker)
            );

            CREATE TABLE IF NOT EXISTS trend_analysis_log (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                action TEXT,
                confidence INTEGER,
                time_window TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trade_history (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                trade_datetime TIMESTAMP NOT NULL,
                order_type TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                order_id TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                gpt_confidence REAL,
                gpt_summary TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS market_data_4h (
                ticker text PRIMARY KEY,
                price real,
                ma_10 real,
                ma_20 real,
                ma_50 real,
                ma_200 real,
                rsi_14 real,
                stochastic_k real,
                stochastic_d real,
                macd real,
                macds real,
                macdh real,
                adx real,
                plus_di real,
                minus_di real,
                bb_upper real,
                bb_middle real,
                bb_lower real,
                cci real,
                supertrend real,
                supertrend_signal text,
                pivot real,
                r1 real, r2 real, r3 real,
                s1 real, s2 real, s3 real,
                fibo_236 real, fibo_382 real,
                fibo_500 real, fibo_618 real, fibo_786 real,
                updated_at timestamp without time zone default CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS tickers (
                ticker VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT true,
                PRIMARY KEY (ticker)
            );

            CREATE TABLE IF NOT EXISTS ohlcv (
                ticker TEXT,
                date DATE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                fibo_618 REAL,
                fibo_382 REAL,
                ht_trendline REAL,
                ma_20 REAL,
                ma_50 REAL,
                ma_200 REAL,
                bb_upper REAL,
                bb_lower REAL,
                donchian_high REAL,
                donchian_low REAL,
                macd_histogram REAL,
                rsi_14 REAL,
                volume_20ma REAL,
                volume_ratio REAL,
                stoch_k REAL,
                stoch_d REAL,
                cci REAL,
                PRIMARY KEY (ticker, date)
            );

            CREATE TABLE IF NOT EXISTS ohlcv_4h (
                ticker TEXT,
                date TIMESTAMP,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (ticker, date)
            );

            CREATE TABLE IF NOT EXISTS portfolio_history (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                action TEXT NOT NULL,
                qty REAL,
                price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trailing_stops (
                id SERIAL PRIMARY KEY,
                ticker TEXT UNIQUE NOT NULL,
                initial_price REAL NOT NULL,
                activation_price REAL NOT NULL,
                stop_price REAL NOT NULL,
                atr_value REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS static_indicators (
                ticker TEXT PRIMARY KEY,
                nvt_relative REAL,
                volume_change_7_30 REAL,
                price REAL,
                high_60 REAL,
                low_60 REAL,
                pivot REAL,
                s1 REAL,
                r1 REAL,
                resistance REAL,
                support REAL,
                atr REAL,
                adx REAL,
                supertrend_signal TEXT,
                rsi_14 REAL,
                ma20 REAL,
                volume_ratio REAL,
                volume REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS manual_override_log (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                detection_type VARCHAR(50) NOT NULL,
                expected_quantity DECIMAL(20, 8),
                actual_quantity DECIMAL(20, 8),
                quantity_diff DECIMAL(20, 8),
                description TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved BOOLEAN DEFAULT FALSE,
                resolved_at TIMESTAMP,
                notes TEXT
            );
            """
            
            # SQL 실행
            self.db.execute_query(main_schema_sql)
            logger.info("✅ 핵심 테이블 생성 완료")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 핵심 스키마 생성 실패: {e}")
            self.db.rollback()
            return False
    
    def create_indexes(self) -> bool:
        """인덱스 생성"""
        try:
            logger.info("🔧 인덱스 생성 시작")
            
            index_queries = [
                # market_data_4h 테이블 인덱스
                "CREATE INDEX IF NOT EXISTS idx_market_data_4h_ticker ON market_data_4h(ticker);",
                "CREATE INDEX IF NOT EXISTS idx_market_data_4h_updated_at ON market_data_4h(updated_at);",
                
                # ohlcv 테이블 인덱스
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker ON ohlcv(ticker);",
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_date ON ohlcv(date);",
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_date ON ohlcv(ticker, date DESC);",
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_ma_indicators ON ohlcv(ticker, ma_50, ma_200) WHERE ma_50 IS NOT NULL AND ma_200 IS NOT NULL;",
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_technical ON ohlcv(ticker, rsi_14) WHERE rsi_14 IS NOT NULL;",
                
                # ohlcv_4h 테이블 인덱스
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_4h_ticker ON ohlcv_4h(ticker);",
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_4h_date ON ohlcv_4h(date);",
                
                # trade_log 테이블 인덱스
                "CREATE INDEX IF NOT EXISTS idx_trade_log_ticker ON trade_log(ticker);",
                "CREATE INDEX IF NOT EXISTS idx_trade_log_executed_at ON trade_log(executed_at);",
                "CREATE INDEX IF NOT EXISTS idx_trade_log_strategy_combo ON trade_log(strategy_combo);",
                
                # trend_analysis 테이블 인덱스
                "CREATE INDEX IF NOT EXISTS idx_trend_analysis_ticker ON trend_analysis(ticker);",
                "CREATE INDEX IF NOT EXISTS idx_trend_analysis_created_at ON trend_analysis(created_at);",
                
                # static_indicators 테이블 인덱스
                "CREATE INDEX IF NOT EXISTS idx_static_indicators_ticker ON static_indicators(ticker);",
                "CREATE INDEX IF NOT EXISTS idx_static_indicators_updated_at ON static_indicators(updated_at);",
                
                # manual_override_log 테이블 인덱스
                "CREATE INDEX IF NOT EXISTS idx_manual_override_log_ticker ON manual_override_log(ticker);",
                "CREATE INDEX IF NOT EXISTS idx_manual_override_log_detected_at ON manual_override_log(detected_at);",
                "CREATE INDEX IF NOT EXISTS idx_manual_override_log_resolved ON manual_override_log(resolved);"
            ]
            
            for query in index_queries:
                self.db.execute_query(query)
            
            logger.info("✅ 인덱스 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 인덱스 생성 실패: {e}")
            self.db.rollback()
            return False
    
    def execute_sql_safely(self, sql_query: str, description: str) -> bool:
        """SQL 쿼리를 안전하게 실행"""
        try:
            self.db.execute_query(sql_query)
            logger.info(f"✅ {description} 성공")
            return True
        except Exception as e:
            logger.error(f"❌ {description} 실패: {e}")
            return False

class LambdaDBInitializer:
    """Lambda DB 초기화 메인 클래스"""
    
    def __init__(self):
        self.db = DatabaseManager(DBInitializerConfig.DB_CONFIG)
        self.validator = SchemaValidator(self.db)
        self.initializer = SchemaInitializer(self.db)
        
    def process_db_initialization_request(self, event: dict) -> dict:
        """DB 초기화 요청 처리"""
        try:
            start_time = time.time()
            
            # 요청 파라미터 파싱
            operation_type = event.get('operation_type', 'full_init')
            force_recreate = event.get('force_recreate', False)
            include_backtest = event.get('include_backtest', True)
            
            logger.info(f"🚀 DB 초기화 시작: {operation_type}")
            
            results = {}
            
            if operation_type == 'check_schema':
                results = self._check_schema_status()
            elif operation_type == 'init_core':
                results = self._initialize_core_schema(force_recreate)
            elif operation_type == 'init_backtest':
                results = self._initialize_backtest_schema()
            elif operation_type == 'full_init':
                results = self._full_initialization(force_recreate, include_backtest)
            else:
                raise ValueError(f"지원하지 않는 작업 타입: {operation_type}")
            
            elapsed = time.time() - start_time
            
            response = {
                'statusCode': 200,
                'body': {
                    'success': True,
                    'operation_type': operation_type,
                    'execution_time': round(elapsed, 3),
                    'results': results,
                    'schema_version': DBInitializerConfig.SCHEMA_VERSION,
                    'timestamp': datetime.now().isoformat(),
                    'lambda_version': 'DB_INIT_v1.0'
                }
            }
            
            logger.info(f"✅ DB 초기화 완료: {elapsed:.3f}초")
            return response
            
        except Exception as e:
            logger.error(f"❌ DB 초기화 처리 실패: {e}")
            self.db.rollback()
            return {
                'statusCode': 500,
                'body': {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
            }
        finally:
            self.db.close()
    
    def _check_schema_status(self) -> dict:
        """스키마 상태 확인"""
        try:
            schema_status = self.validator.validate_schema_integrity()
            logger.info(f"📊 스키마 상태: {schema_status['schema_health']}")
            return schema_status
        except Exception as e:
            logger.error(f"❌ 스키마 상태 확인 실패: {e}")
            return {'error': str(e)}
    
    def _initialize_core_schema(self, force_recreate: bool = False) -> dict:
        """핵심 스키마 초기화"""
        try:
            if force_recreate:
                logger.warning("⚠️ 강제 재생성 모드 - 기존 데이터 손실 가능")
            
            # 핵심 스키마 생성
            core_success = self.initializer.create_core_schema()
            
            # 인덱스 생성
            index_success = self.initializer.create_indexes()
            
            # Disclaimer 테이블 생성
            disclaimer_table, disclaimer_indexes = self.initializer.create_disclaimer_table()
            disclaimer_success = self.initializer.execute_sql_safely(disclaimer_table, "Disclaimer 테이블 생성")
            
            for index_sql in disclaimer_indexes:
                self.initializer.execute_sql_safely(index_sql, "Disclaimer 인덱스 생성")
            
            if core_success and index_success and disclaimer_success:
                self.db.commit()
                logger.info("✅ 핵심 스키마 초기화 완료")
                
                # 상태 재확인
                schema_status = self.validator.validate_schema_integrity()
                
                return {
                    'core_schema': 'success',
                    'indexes': 'success',
                    'disclaimer': 'success',
                    'schema_status': schema_status
                }
            else:
                self.db.rollback()
                return {
                    'core_schema': 'failed',
                    'error': '핵심 스키마 생성 실패'
                }
                
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 핵심 스키마 초기화 실패: {e}")
            return {'error': str(e)}
    
    def _initialize_backtest_schema(self) -> dict:
        """백테스트 스키마 초기화"""
        try:
            backtest_tables = self.initializer.create_backtest_tables()
            success_count = 0
            
            for table_sql in backtest_tables:
                if self.initializer.execute_sql_safely(table_sql, "백테스트 테이블 생성"):
                    success_count += 1
            
            if success_count == len(backtest_tables):
                self.db.commit()
                logger.info("✅ 백테스트 스키마 초기화 완료")
                return {
                    'backtest_schema': 'success',
                    'tables_created': success_count
                }
            else:
                self.db.rollback()
                return {
                    'backtest_schema': 'partial',
                    'tables_created': success_count,
                    'total_tables': len(backtest_tables)
                }
                
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ 백테스트 스키마 초기화 실패: {e}")
            return {'error': str(e)}
    
    def _full_initialization(self, force_recreate: bool = False, include_backtest: bool = True) -> dict:
        """전체 초기화"""
        try:
            results = {}
            
            # 1. 핵심 스키마 초기화
            core_result = self._initialize_core_schema(force_recreate)
            results['core'] = core_result
            
            # 2. 백테스트 스키마 초기화 (선택적)
            if include_backtest:
                backtest_result = self._initialize_backtest_schema()
                results['backtest'] = backtest_result
            
            # 3. 최종 상태 확인
            final_status = self.validator.validate_schema_integrity()
            results['final_status'] = final_status
            
            logger.info("🎉 전체 DB 초기화 완료")
            return results
            
        except Exception as e:
            logger.error(f"❌ 전체 초기화 실패: {e}")
            return {'error': str(e)}

def lambda_handler(event, context):
    """Lambda 함수 진입점"""
    try:
        logger.info(f"📥 Lambda DB 초기화 요청 수신: {json.dumps(event, indent=2)}")
        
        # DB 초기화기 초기화
        initializer = LambdaDBInitializer()
        
        # 요청 처리
        result = initializer.process_db_initialization_request(event)
        
        logger.info("📤 Lambda 응답 준비 완료")
        return result
        
    except Exception as e:
        logger.error(f"❌ Lambda 함수 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': f"Lambda 함수 실행 실패: {str(e)}",
                'timestamp': datetime.now().isoformat()
            }
        }

# 로컬 테스트용
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        'operation_type': 'check_schema',
        'force_recreate': False,
        'include_backtest': True
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))