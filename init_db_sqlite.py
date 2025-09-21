#!/usr/bin/env python3
"""
SQLite 데이터베이스 초기화 스크립트
Makenaide 로컬 운영을 위한 완전한 데이터베이스 스키마 초기화

🔧 주요 기능:
- 모든 필수 테이블 생성 (19개 테이블)
- 인덱스 생성으로 성능 최적화
- 외래키 제약 조건 설정
- 기본 데이터 초기화
- 스키마 무결성 검증

📊 테이블 구조:
1. Core Tables: tickers, ohlcv_data
2. Analysis Tables: technical_analysis (통합), gpt_analysis
3. Trading Tables: trades, trade_history, portfolio_history, kelly_analysis
4. System Tables: failure_*, recovery_*, prediction_*
5. Meta Tables: disclaimer_agreements, manual_override_log

🚀 EC2 Local Architecture 호환:
- Amazon Linux 2023 호환
- SQLite 3.x WAL 모드 지원
- 메모리 효율적 인덱스 설계
- 트랜잭션 기반 안전한 초기화
"""

import sqlite3
import os
import sys
import logging
from datetime import datetime
from typing import Optional, List, Tuple
from pathlib import Path

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQLiteDatabaseInitializer:
    """SQLite 데이터베이스 초기화 클래스"""

    def __init__(self, db_path: str = "makenaide_local.db"):
        """
        초기화 클래스 생성

        Args:
            db_path: 데이터베이스 파일 경로
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self):
        """컨텍스트 매니저 진입"""
        self.conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            check_same_thread=False,
            isolation_level=None  # 자동 커밋 모드
        )

        # SQLite 최적화 설정
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("PRAGMA cache_size = -64000")  # 64MB 캐시
        self.conn.execute("PRAGMA temp_store = memory")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA auto_vacuum = INCREMENTAL")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.close()

    def create_core_tables(self) -> None:
        """핵심 테이블 생성 (tickers, ohlcv_data)"""
        logger.info("📋 핵심 테이블 생성 중...")

        # 1. tickers 테이블 - Phase 0에서 사용
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                ticker TEXT PRIMARY KEY,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                is_active INTEGER DEFAULT 1
                -- Phase 0: 순수한 종목 목록 관리 전용
                -- 기술적 분석은 Phase 2에서 별도 테이블 사용
            )
        """)

        # 2. ohlcv_data 테이블 - Phase 1에서 사용
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_data (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                ma5 REAL,
                ma20 REAL,
                ma60 REAL,
                ma120 REAL,
                ma200 REAL,
                rsi REAL,
                volume_ratio REAL,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (ticker, date)
            )
        """)

        # OHLCV 인덱스 생성
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_data_ticker ON ohlcv_data(ticker)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_data_date ON ohlcv_data(date)")

        logger.info("✅ 핵심 테이블 생성 완료")

    def create_analysis_tables(self) -> None:
        """분석 관련 테이블 생성"""
        logger.info("🔍 분석 테이블 생성 중...")

        # 1. 통합 technical_analysis 테이블 - HybridTechnicalFilter + LayeredScoring 통합
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS technical_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                analysis_date TEXT NOT NULL,

                -- Weinstein Stage 분석 (HybridTechnicalFilter 호환)
                current_stage INTEGER,
                stage_confidence REAL,
                ma200_trend TEXT,
                ma200_slope REAL,
                price_vs_ma200 REAL,
                breakout_strength REAL,
                volume_surge REAL,
                days_in_stage INTEGER,

                -- 4-Gate 필터링 결과
                gate1_stage2 INTEGER,
                gate2_volume INTEGER,
                gate3_momentum INTEGER,
                gate4_quality INTEGER,
                total_gates_passed INTEGER,
                quality_score REAL,
                recommendation TEXT,

                -- 핵심 기술적 지표 (trading_engine.py 호환, 타입 최적화)
                atr REAL,                    -- TEXT → REAL 변경
                supertrend REAL,             -- TEXT → REAL 변경
                adx REAL,                    -- 기존 유지
                macd_histogram REAL,         -- TEXT → REAL 변경
                support_level REAL,          -- TEXT → REAL 변경

                -- LayeredScoringEngine 확장 (IntegratedScoringSystem 호환)
                macro_score REAL,
                structural_score REAL,
                micro_score REAL,
                total_score REAL,
                quality_gates_passed BOOLEAN,
                analysis_details TEXT,

                -- 메타데이터
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),

                UNIQUE(ticker, analysis_date)
            )
        """)

        # 2. [DEPRECATED] makenaide_technical_analysis 테이블 제거됨
        # → technical_analysis 테이블로 통합됨 (Phase 1 통합 작업)

        # 3. gpt_analysis 테이블 - Phase 3 GPT 분석에서 사용
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS gpt_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                analysis_date TEXT NOT NULL,

                -- VCP 패턴 분석
                vcp_detected BOOLEAN DEFAULT 0,
                vcp_confidence REAL DEFAULT 0.0,
                vcp_stage INTEGER DEFAULT 0,
                vcp_volatility_ratio REAL DEFAULT 0.0,

                -- Cup & Handle 패턴 분석
                cup_handle_detected BOOLEAN DEFAULT 0,
                cup_handle_confidence REAL DEFAULT 0.0,
                cup_depth_ratio REAL DEFAULT 0.0,
                handle_duration_days INTEGER DEFAULT 0,

                -- GPT 종합 분석
                gpt_recommendation TEXT DEFAULT 'HOLD',
                gpt_confidence REAL DEFAULT 0.0,
                gpt_reasoning TEXT DEFAULT '',
                api_cost_usd REAL DEFAULT 0.0,
                processing_time_ms INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                analysis_result TEXT,

                UNIQUE(ticker, analysis_date)
            )
        """)

        # 4. kelly_analysis 테이블 - Kelly Calculator에서 사용
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS kelly_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                analysis_date TEXT NOT NULL,

                -- Technical Filter 단계
                detected_pattern TEXT NOT NULL,
                quality_score REAL NOT NULL,
                base_position_pct REAL NOT NULL,
                quality_multiplier REAL NOT NULL,
                technical_position_pct REAL NOT NULL,

                -- GPT 조정 단계 (선택적)
                gpt_confidence REAL DEFAULT NULL,
                gpt_recommendation TEXT DEFAULT NULL,
                gpt_adjustment REAL DEFAULT 1.0,
                final_position_pct REAL NOT NULL,

                -- 메타 정보
                risk_level TEXT DEFAULT 'moderate',
                max_portfolio_allocation REAL DEFAULT 25.0,
                reasoning TEXT DEFAULT '',

                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(ticker, analysis_date)
            )
        """)

        # 5. static_indicators 테이블 - 정적 지표 저장
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS static_indicators (
                ticker TEXT PRIMARY KEY,
                -- 확정된 정적 지표들
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
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # 분석 테이블 인덱스 생성
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_technical_analysis_ticker ON technical_analysis(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_technical_analysis_date ON technical_analysis(analysis_date)",
            "CREATE INDEX IF NOT EXISTS idx_technical_analysis_recommendation ON technical_analysis(recommendation)",
            "CREATE INDEX IF NOT EXISTS idx_gpt_analysis_ticker ON gpt_analysis(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_gpt_analysis_date ON gpt_analysis(analysis_date)",
            "CREATE INDEX IF NOT EXISTS idx_kelly_ticker ON kelly_analysis(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_kelly_date ON kelly_analysis(analysis_date)",
            "CREATE INDEX IF NOT EXISTS idx_static_indicators_ticker ON static_indicators(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_static_indicators_updated_at ON static_indicators(updated_at)"
        ]

        for index in indexes:
            self.conn.execute(index)

        logger.info("✅ 분석 테이블 생성 완료")

    def create_trading_tables(self) -> None:
        """거래 관련 테이블 생성"""
        logger.info("💰 거래 테이블 생성 중...")

        # 1. trades 테이블 - 실시간 거래 기록
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                order_type TEXT NOT NULL,
                status TEXT NOT NULL,
                order_id TEXT,
                quantity REAL,
                price REAL,
                amount_krw REAL,
                fee REAL,
                error_message TEXT,
                timestamp TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. trade_history 테이블 - 거래 이력 저장
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                trade_datetime TEXT NOT NULL,
                order_type TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                order_id TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                gpt_confidence REAL,
                gpt_summary TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # 3. portfolio_history 테이블 - 포트폴리오 변경 이력
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                action TEXT NOT NULL,
                qty REAL,
                price REAL,
                timestamp TEXT DEFAULT (datetime('now')),
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # 4. trailing_stops 테이블 - 트레일링 스탑 관리
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trailing_stops (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT UNIQUE NOT NULL,
                initial_price REAL NOT NULL,
                activation_price REAL NOT NULL,
                stop_price REAL NOT NULL,
                atr_value REAL,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)

        logger.info("✅ 거래 테이블 생성 완료")

    def create_system_monitoring_tables(self) -> None:
        """시스템 모니터링 관련 테이블 생성"""
        logger.info("🔍 시스템 모니터링 테이블 생성 중...")

        # 1. failure_records 테이블 - 실패 기록
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS failure_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                failure_type TEXT NOT NULL,
                sub_type TEXT,
                severity TEXT NOT NULL,
                phase TEXT,
                error_message TEXT NOT NULL,
                metadata TEXT,
                recovery_attempted INTEGER DEFAULT 0,
                recovery_successful INTEGER DEFAULT 0,
                resolution_time INTEGER,
                similar_failure_count INTEGER DEFAULT 0,
                failure_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. failure_patterns 테이블 - 실패 패턴 분석
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS failure_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_id TEXT UNIQUE NOT NULL,
                failure_type TEXT NOT NULL,
                sub_type TEXT,
                frequency INTEGER DEFAULT 1,
                first_occurrence TEXT NOT NULL,
                last_occurrence TEXT NOT NULL,
                avg_resolution_time REAL DEFAULT 0,
                success_rate REAL DEFAULT 0,
                risk_score REAL DEFAULT 0,
                trend TEXT DEFAULT 'stable',
                recommendations TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 3. system_health_metrics 테이블 - 시스템 건강성 지표
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS system_health_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                total_failures_24h INTEGER DEFAULT 0,
                critical_failures_24h INTEGER DEFAULT 0,
                avg_resolution_time REAL DEFAULT 0,
                failure_rate_trend TEXT DEFAULT 'stable',
                most_common_failure TEXT,
                risk_level TEXT DEFAULT 'LOW',
                health_score REAL DEFAULT 100,
                metrics_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 4. recovery_attempts 테이블 - 복구 시도 기록
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS recovery_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                failure_record_id INTEGER,
                recovery_method TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                successful INTEGER DEFAULT 0,
                execution_time INTEGER,  -- 실행 시간(초)
                error_message TEXT,
                metadata TEXT,  -- JSON
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (failure_record_id) REFERENCES failure_records (id)
            )
        """)

        # 시스템 모니터링 인덱스 생성
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_failure_records_timestamp ON failure_records(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_failure_records_type ON failure_records(failure_type)",
            "CREATE INDEX IF NOT EXISTS idx_failure_records_subtype ON failure_records(sub_type)",
            "CREATE INDEX IF NOT EXISTS idx_failure_records_severity ON failure_records(severity)",
            "CREATE INDEX IF NOT EXISTS idx_failure_records_hash ON failure_records(failure_hash)",
            "CREATE INDEX IF NOT EXISTS idx_failure_patterns_id ON failure_patterns(pattern_id)",
            "CREATE INDEX IF NOT EXISTS idx_failure_patterns_type ON failure_patterns(failure_type)",
            "CREATE INDEX IF NOT EXISTS idx_failure_patterns_risk ON failure_patterns(risk_score)",
            "CREATE INDEX IF NOT EXISTS idx_system_health_timestamp ON system_health_metrics(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_system_health_risk ON system_health_metrics(risk_level)"
        ]

        for index in indexes:
            self.conn.execute(index)

        logger.info("✅ 시스템 모니터링 테이블 생성 완료")

    def create_recovery_system_tables(self) -> None:
        """복구 시스템 관련 테이블 생성"""
        logger.info("🔧 복구 시스템 테이블 생성 중...")

        # 1. recovery_plans 테이블 - 복구 계획
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS recovery_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id TEXT UNIQUE NOT NULL,
                failure_type TEXT NOT NULL,
                failure_sub_type TEXT,
                severity TEXT NOT NULL,
                actions TEXT NOT NULL,
                execution_order TEXT NOT NULL,
                estimated_total_duration INTEGER NOT NULL,
                success_probability REAL NOT NULL,
                approval_required BOOLEAN NOT NULL,
                approved_by TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
        """)

        # 2. recovery_executions 테이블 - 복구 실행 기록
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS recovery_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT UNIQUE NOT NULL,
                plan_id TEXT NOT NULL,
                failure_record_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                executed_actions TEXT,
                success_rate REAL,
                error_message TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (failure_record_id) REFERENCES failure_records (id),
                FOREIGN KEY (plan_id) REFERENCES recovery_plans (plan_id)
            )
        """)

        # 3. recovery_action_stats 테이블 - 복구 액션 통계
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS recovery_action_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT NOT NULL,
                failure_type TEXT NOT NULL,
                execution_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                avg_duration REAL DEFAULT 0,
                last_success_rate REAL DEFAULT 0,
                last_updated TEXT NOT NULL
            )
        """)

        logger.info("✅ 복구 시스템 테이블 생성 완료")

    def create_prediction_tables(self) -> None:
        """예측 시스템 관련 테이블 생성"""
        logger.info("🔮 예측 시스템 테이블 생성 중...")

        # 1. prediction_results 테이블 - 예측 결과
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prediction_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prediction_id TEXT UNIQUE NOT NULL,
                prediction_type TEXT NOT NULL,
                risk_level TEXT NOT NULL,
                confidence TEXT NOT NULL,
                predicted_failure_time TEXT,
                failure_probability REAL NOT NULL,
                affected_components TEXT,
                recommended_actions TEXT,
                evidence TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL,
                validated_at TEXT,
                actual_outcome TEXT,
                accuracy_score REAL
            )
        """)

        # 2. prediction_accuracy 테이블 - 예측 정확도 추적
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prediction_accuracy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prediction_id TEXT NOT NULL,
                prediction_type TEXT NOT NULL,
                predicted_time TEXT,
                actual_time TEXT,
                accuracy_score REAL NOT NULL,
                confidence_level TEXT NOT NULL,
                notes TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (prediction_id) REFERENCES prediction_results (prediction_id)
            )
        """)

        # 3. trend_analysis 테이블 - 트렌드 분석
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trend_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                trend_direction TEXT NOT NULL,
                trend_strength REAL NOT NULL,
                slope REAL NOT NULL,
                r_squared REAL NOT NULL,
                prediction_7d REAL,
                prediction_30d REAL,
                anomaly_score REAL NOT NULL,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
        """)

        logger.info("✅ 예측 시스템 테이블 생성 완료")

    def create_meta_tables(self) -> None:
        """메타 데이터 테이블 생성"""
        logger.info("📝 메타 데이터 테이블 생성 중...")

        # 1. disclaimer_agreements 테이블 - 면책 조항 동의
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS disclaimer_agreements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agreement_version TEXT NOT NULL,
                agreed_at TEXT DEFAULT (datetime('now')),
                agreed_by TEXT DEFAULT 'system',
                ip_address TEXT,
                user_agent TEXT,
                agreement_text_hash TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # 2. manual_override_log 테이블 - 수동 개입 로그
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_override_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                detection_type TEXT NOT NULL,
                expected_quantity REAL,
                actual_quantity REAL,
                quantity_diff REAL,
                description TEXT,
                detected_at TEXT DEFAULT (datetime('now')),
                resolved INTEGER DEFAULT 0,
                resolved_at TEXT,
                notes TEXT
            )
        """)

        # 메타 테이블 인덱스 생성
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_disclaimer_agreements_version ON disclaimer_agreements(agreement_version)",
            "CREATE INDEX IF NOT EXISTS idx_disclaimer_agreements_active ON disclaimer_agreements(is_active)",
            "CREATE INDEX IF NOT EXISTS idx_manual_override_log_ticker ON manual_override_log(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_manual_override_log_detected_at ON manual_override_log(detected_at)",
            "CREATE INDEX IF NOT EXISTS idx_manual_override_log_resolved ON manual_override_log(resolved)"
        ]

        for index in indexes:
            self.conn.execute(index)

        logger.info("✅ 메타 데이터 테이블 생성 완료")

    def insert_initial_data(self) -> None:
        """초기 데이터 삽입"""
        logger.info("📊 초기 데이터 삽입 중...")

        # 기본 면책 조항 동의 삽입
        self.conn.execute("""
            INSERT OR IGNORE INTO disclaimer_agreements
            (agreement_version, agreed_by, agreement_text_hash, is_active)
            VALUES ('v1.0', 'system_init', 'init_hash', 1)
        """)

        logger.info("✅ 초기 데이터 삽입 완료")

    def verify_schema(self) -> Tuple[bool, List[str]]:
        """스키마 무결성 검증"""
        logger.info("🔍 스키마 무결성 검증 중...")

        issues = []

        # 필수 테이블 목록
        required_tables = [
            'tickers', 'ohlcv_data', 'technical_analysis',
            'gpt_analysis', 'kelly_analysis', 'static_indicators', 'trades', 'trade_history',
            'portfolio_history', 'trailing_stops', 'failure_records', 'failure_patterns',
            'system_health_metrics', 'recovery_attempts', 'recovery_plans', 'recovery_executions',
            'recovery_action_stats', 'prediction_results', 'prediction_accuracy', 'trend_analysis',
            'disclaimer_agreements', 'manual_override_log'
        ]

        # 테이블 존재 여부 확인
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {row[0] for row in cursor.fetchall()}

        for table in required_tables:
            if table not in existing_tables:
                issues.append(f"필수 테이블 누락: {table}")

        # 인덱스 존재 여부 확인 (주요 인덱스만)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        existing_indexes = {row[0] for row in cursor.fetchall()}

        required_indexes = [
            'idx_ohlcv_data_ticker', 'idx_ohlcv_data_date',
            'idx_technical_analysis_ticker', 'idx_gpt_analysis_ticker',
            'idx_kelly_ticker', 'idx_failure_records_timestamp'
        ]

        for index in required_indexes:
            if index not in existing_indexes:
                issues.append(f"주요 인덱스 누락: {index}")

        success = len(issues) == 0
        if success:
            logger.info("✅ 스키마 무결성 검증 통과")
        else:
            logger.warning(f"⚠️ 스키마 검증에서 {len(issues)}개 이슈 발견")
            for issue in issues:
                logger.warning(f"   - {issue}")

        return success, issues

    def initialize_database(self) -> bool:
        """전체 데이터베이스 초기화 실행"""
        try:
            logger.info("🚀 Makenaide SQLite 데이터베이스 초기화 시작")
            logger.info(f"📁 데이터베이스 파일: {os.path.abspath(self.db_path)}")

            # 트랜잭션 시작
            self.conn.execute("BEGIN TRANSACTION")

            # 단계별 테이블 생성
            self.create_core_tables()
            self.create_analysis_tables()
            self.create_trading_tables()
            self.create_system_monitoring_tables()
            self.create_recovery_system_tables()
            self.create_prediction_tables()
            self.create_meta_tables()

            # 초기 데이터 삽입
            self.insert_initial_data()

            # 트랜잭션 커밋
            self.conn.execute("COMMIT")

            # 스키마 검증
            success, issues = self.verify_schema()

            if success:
                logger.info("🎉 Makenaide 데이터베이스 초기화 완료!")
                logger.info("✅ 모든 테이블과 인덱스가 정상적으로 생성되었습니다.")

                # 데이터베이스 크기 정보
                file_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                logger.info(f"📊 데이터베이스 파일 크기: {file_size:,} bytes")

                return True
            else:
                logger.error("❌ 스키마 검증 실패")
                return False

        except Exception as e:
            logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
            self.conn.execute("ROLLBACK")
            return False

def main():
    """메인 실행 함수"""
    print("=" * 80)
    print("🔧 Makenaide SQLite 데이터베이스 초기화")
    print("=" * 80)

    db_path = os.getenv('SQLITE_DATABASE', './makenaide_local.db')

    # 기존 파일 존재 여부 확인
    if os.path.exists(db_path):
        print(f"⚠️ 기존 데이터베이스 파일이 존재합니다: {db_path}")
        response = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
        if response != 'y':
            print("작업이 취소되었습니다.")
            sys.exit(0)
        print()

    # 데이터베이스 초기화 실행
    try:
        with SQLiteDatabaseInitializer(db_path) as db_init:
            success = db_init.initialize_database()

        if success:
            print("\n🎉 데이터베이스 초기화가 성공적으로 완료되었습니다!")
            print(f"📁 파일 위치: {os.path.abspath(db_path)}")
            print("\n🚀 이제 makenaide.py를 실행할 수 있습니다.")
            sys.exit(0)
        else:
            print("\n❌ 데이터베이스 초기화에 실패했습니다.")
            print("로그를 확인하여 문제를 해결해 주세요.")
            sys.exit(1)

    except Exception as e:
        print(f"\n💥 예상치 못한 오류가 발생했습니다: {e}")
        print("상세한 오류 정보는 로그를 확인해 주세요.")
        sys.exit(1)

if __name__ == "__main__":
    main()