#!/usr/bin/env python3
"""
기술적 분석 테이블 통합 마이그레이션 스크립트
technical_analysis + makenaide_technical_analysis → 통합 technical_analysis

🎯 마이그레이션 전략:
1. 새로운 통합 스키마로 technical_analysis_unified 테이블 생성
2. 두 소스 테이블에서 데이터 병합 (ticker 기준 LEFT JOIN)
3. 컬럼 매핑 및 데이터 타입 변환
4. 기존 테이블 백업 후 새 테이블로 교체

📊 데이터 소스:
- technical_analysis: Weinstein Stage 분석 (205개, 2025-09-17~19)
- makenaide_technical_analysis: LayeredScoring (201개, 2025-09-21)
"""

import sqlite3
import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import json

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TechnicalAnalysisMigrator:
    """기술적 분석 테이블 통합 마이그레이션 클래스"""

    def __init__(self, db_path: str = "makenaide_local.db"):
        """
        마이그레이션 클래스 초기화

        Args:
            db_path: SQLite 데이터베이스 경로
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.migration_log = {
            "start_time": None,
            "end_time": None,
            "source_counts": {},
            "target_count": 0,
            "errors": [],
            "success": False
        }

    def __enter__(self):
        """컨텍스트 매니저 진입"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close()

    def connect(self) -> None:
        """데이터베이스 연결"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # 딕셔너리 스타일 접근
            logger.info(f"📱 데이터베이스 연결: {self.db_path}")
        except Exception as e:
            logger.error(f"❌ 데이터베이스 연결 실패: {e}")
            raise

    def close(self) -> None:
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            logger.info("🔒 데이터베이스 연결 종료")

    def analyze_source_tables(self) -> Dict[str, Any]:
        """소스 테이블 분석"""
        logger.info("🔍 소스 테이블 분석 중...")

        analysis = {
            "technical_analysis": {},
            "makenaide_technical_analysis": {}
        }

        # technical_analysis 테이블 분석
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_count,
                COUNT(DISTINCT ticker) as unique_tickers,
                MIN(analysis_date) as oldest_date,
                MAX(analysis_date) as newest_date,
                COUNT(CASE WHEN current_stage IS NOT NULL THEN 1 END) as stage_data_count,
                COUNT(CASE WHEN supertrend IS NOT NULL AND supertrend != '' THEN 1 END) as supertrend_data_count,
                COUNT(CASE WHEN atr IS NOT NULL THEN 1 END) as atr_data_count
            FROM technical_analysis
        """)

        row = cursor.fetchone()
        analysis["technical_analysis"] = {
            "total_count": row["total_count"],
            "unique_tickers": row["unique_tickers"],
            "date_range": f"{row['oldest_date']} ~ {row['newest_date']}",
            "stage_data_coverage": f"{row['stage_data_count']}/{row['total_count']} ({row['stage_data_count']/row['total_count']*100:.1f}%)",
            "supertrend_coverage": f"{row['supertrend_data_count']}/{row['total_count']} ({row['supertrend_data_count']/row['total_count']*100:.1f}%)",
            "atr_coverage": f"{row['atr_data_count']}/{row['total_count']} ({row['atr_data_count']/row['total_count']*100:.1f}%)"
        }

        # makenaide_technical_analysis 테이블 분석
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_count,
                COUNT(DISTINCT ticker) as unique_tickers,
                MIN(analysis_timestamp) as oldest_timestamp,
                MAX(analysis_timestamp) as newest_timestamp,
                COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as score_data_count,
                COUNT(CASE WHEN macro_score IS NOT NULL THEN 1 END) as macro_score_count
            FROM makenaide_technical_analysis
        """)

        row = cursor.fetchone()
        analysis["makenaide_technical_analysis"] = {
            "total_count": row["total_count"],
            "unique_tickers": row["unique_tickers"],
            "timestamp_range": f"{row['oldest_timestamp']} ~ {row['newest_timestamp']}",
            "score_coverage": f"{row['score_data_count']}/{row['total_count']} ({row['score_data_count']/row['total_count']*100:.1f}%)",
            "macro_score_coverage": f"{row['macro_score_count']}/{row['total_count']} ({row['macro_score_count']/row['total_count']*100:.1f}%)"
        }

        # 공통 ticker 분석
        cursor = self.conn.execute("""
            SELECT COUNT(DISTINCT t1.ticker) as common_tickers
            FROM technical_analysis t1
            INNER JOIN makenaide_technical_analysis t2 ON t1.ticker = t2.ticker
        """)
        analysis["common_tickers"] = cursor.fetchone()["common_tickers"]

        self.migration_log["source_counts"] = analysis
        return analysis

    def create_unified_table(self) -> None:
        """통합 technical_analysis 테이블 생성"""
        logger.info("🏗️ 통합 테이블 생성 중...")

        # 기존 unified 테이블이 있다면 삭제
        self.conn.execute("DROP TABLE IF EXISTS technical_analysis_unified")

        # 통합 스키마로 새 테이블 생성
        unified_schema = """
        CREATE TABLE technical_analysis_unified (
            -- 기본 정보
            ticker TEXT NOT NULL,
            analysis_date TEXT NOT NULL,

            -- Weinstein Stage analysis (HybridTechnicalFilter 호환)
            current_stage INTEGER,
            stage_confidence REAL,
            stage_trend TEXT,
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

            -- 기술적 지표 (trading_engine.py 호환, 타입 최적화)
            close_price REAL,
            ma5 REAL,
            ma20 REAL,
            ma60 REAL,
            ma120 REAL,
            ma200 REAL,
            rsi REAL,
            atr REAL,                    -- TEXT → REAL 변경
            supertrend REAL,             -- TEXT → REAL 변경
            macd_histogram REAL,         -- TEXT → REAL 변경
            adx REAL,
            support_level REAL,          -- TEXT → REAL 변경
            resistance_level REAL,
            volume_surge_factor REAL,

            -- LayeredScoringEngine 확장 (IntegratedScoringSystem 호환)
            macro_score REAL,
            structural_score REAL,
            micro_score REAL,
            total_score REAL,
            quality_gates_passed BOOLEAN,
            analysis_details TEXT,

            -- 메타데이터
            source_table TEXT,           -- 데이터 출처 추적
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- 복합 UNIQUE 제약조건
            UNIQUE(ticker, analysis_date)
        )
        """

        self.conn.execute(unified_schema)

        # 인덱스 생성
        indexes = [
            "CREATE INDEX idx_unified_ticker ON technical_analysis_unified(ticker)",
            "CREATE INDEX idx_unified_date ON technical_analysis_unified(analysis_date)",
            "CREATE INDEX idx_unified_stage ON technical_analysis_unified(current_stage)",
            "CREATE INDEX idx_unified_recommendation ON technical_analysis_unified(recommendation)",
            "CREATE INDEX idx_unified_total_score ON technical_analysis_unified(total_score)"
        ]

        for index_sql in indexes:
            self.conn.execute(index_sql)

        logger.info("✅ 통합 테이블 생성 완료")

    def migrate_data(self) -> None:
        """데이터 마이그레이션 실행"""
        logger.info("🔄 데이터 마이그레이션 시작...")

        # 최신 데이터 우선 정책으로 두 테이블 데이터 병합
        # Step 1: technical_analysis에서 각 ticker의 최신 데이터 선택
        latest_technical_query = """
        CREATE TEMPORARY TABLE latest_technical AS
        SELECT t1.*
        FROM technical_analysis t1
        WHERE t1.analysis_date = (
            SELECT MAX(t2.analysis_date)
            FROM technical_analysis t2
            WHERE t2.ticker = t1.ticker
        )
        """
        self.conn.execute(latest_technical_query)

        # Step 2: 통합 마이그레이션
        migration_query = """
        INSERT INTO technical_analysis_unified (
            ticker, analysis_date,
            -- Weinstein Stage 데이터 (technical_analysis에서)
            current_stage, stage_confidence, ma200_trend, ma200_slope,
            price_vs_ma200, breakout_strength, volume_surge, days_in_stage,
            gate1_stage2, gate2_volume, gate3_momentum, gate4_quality,
            total_gates_passed, quality_score, recommendation,
            -- 기술적 지표 (technical_analysis에서, 타입 변환)
            atr, supertrend, macd_histogram, adx, support_level,
            -- LayeredScoring 데이터 (makenaide_technical_analysis에서)
            macro_score, structural_score, micro_score, total_score,
            quality_gates_passed, analysis_details,
            -- 메타데이터
            source_table
        )
        SELECT
            all_tickers.ticker as ticker,
            COALESCE(t1.analysis_date, DATE(t2.analysis_timestamp)) as analysis_date,
            -- Weinstein Stage 데이터
            t1.current_stage,
            t1.stage_confidence,
            t1.ma200_trend,
            t1.ma200_slope,
            t1.price_vs_ma200,
            t1.breakout_strength,
            t1.volume_surge,
            t1.days_in_stage,
            t1.gate1_stage2,
            t1.gate2_volume,
            t1.gate3_momentum,
            t1.gate4_quality,
            t1.total_gates_passed,
            COALESCE(t1.quality_score, t2.quality_score) as quality_score,
            COALESCE(t1.recommendation, t2.recommendation) as recommendation,
            -- 기술적 지표 (타입 변환)
            CASE
                WHEN t1.atr IS NOT NULL AND t1.atr != ''
                THEN CAST(t1.atr AS REAL)
                ELSE NULL
            END as atr,
            CASE
                WHEN t1.supertrend IS NOT NULL AND t1.supertrend != ''
                THEN CAST(t1.supertrend AS REAL)
                ELSE NULL
            END as supertrend,
            CASE
                WHEN t1.macd_histogram IS NOT NULL AND t1.macd_histogram != ''
                THEN CAST(t1.macd_histogram AS REAL)
                ELSE NULL
            END as macd_histogram,
            t1.adx,
            CASE
                WHEN t1.support_level IS NOT NULL AND t1.support_level != ''
                THEN CAST(t1.support_level AS REAL)
                ELSE NULL
            END as support_level,
            -- LayeredScoring 데이터
            t2.macro_score,
            t2.structural_score,
            t2.micro_score,
            t2.total_score,
            t2.quality_gates_passed,
            t2.analysis_details,
            -- 소스 추적
            CASE
                WHEN t1.ticker IS NOT NULL AND t2.ticker IS NOT NULL THEN 'both'
                WHEN t1.ticker IS NOT NULL THEN 'technical_analysis'
                WHEN t2.ticker IS NOT NULL THEN 'makenaide_technical_analysis'
                ELSE 'unknown'
            END as source_table
        FROM (
            SELECT DISTINCT ticker FROM latest_technical
            UNION
            SELECT DISTINCT ticker FROM makenaide_technical_analysis
        ) all_tickers
        LEFT JOIN latest_technical t1 ON all_tickers.ticker = t1.ticker
        LEFT JOIN makenaide_technical_analysis t2 ON all_tickers.ticker = t2.ticker
        """

        cursor = self.conn.execute(migration_query)
        migrated_count = cursor.rowcount

        # Step 3: 임시 테이블 정리
        self.conn.execute("DROP TABLE latest_technical")

        self.conn.commit()

        # 최종 카운트 확인
        final_count = self.conn.execute("SELECT COUNT(*) FROM technical_analysis_unified").fetchone()[0]
        self.migration_log["target_count"] = final_count

        logger.info(f"✅ 데이터 마이그레이션 완료: {final_count}개 레코드")

    def validate_migration(self) -> Dict[str, Any]:
        """마이그레이션 결과 검증"""
        logger.info("🔍 마이그레이션 검증 중...")

        validation = {}

        # 기본 통계
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_count,
                COUNT(DISTINCT ticker) as unique_tickers,
                COUNT(CASE WHEN source_table = 'both' THEN 1 END) as both_sources,
                COUNT(CASE WHEN source_table = 'technical_analysis' THEN 1 END) as technical_only,
                COUNT(CASE WHEN source_table = 'makenaide_technical_analysis' THEN 1 END) as makenaide_only,
                COUNT(CASE WHEN current_stage IS NOT NULL THEN 1 END) as stage_data,
                COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as score_data
            FROM technical_analysis_unified
        """)

        row = cursor.fetchone()
        validation["summary"] = {
            "total_records": row["total_count"],
            "unique_tickers": row["unique_tickers"],
            "both_sources": row["both_sources"],
            "technical_only": row["technical_only"],
            "makenaide_only": row["makenaide_only"],
            "stage_data_coverage": f"{row['stage_data']}/{row['total_count']} ({row['stage_data']/row['total_count']*100:.1f}%)",
            "score_data_coverage": f"{row['score_data']}/{row['total_count']} ({row['score_data']/row['total_count']*100:.1f}%)"
        }

        # 샘플 데이터 확인
        cursor = self.conn.execute("""
            SELECT ticker, current_stage, total_score, source_table
            FROM technical_analysis_unified
            ORDER BY total_score DESC NULLS LAST
            LIMIT 5
        """)
        validation["sample_data"] = [dict(row) for row in cursor.fetchall()]

        return validation

    def backup_existing_tables(self) -> None:
        """기존 테이블 백업"""
        logger.info("💾 기존 테이블 백업 중...")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # technical_analysis 백업
        self.conn.execute(f"""
            CREATE TABLE technical_analysis_backup_{timestamp} AS
            SELECT * FROM technical_analysis
        """)

        # makenaide_technical_analysis 백업
        self.conn.execute(f"""
            CREATE TABLE makenaide_technical_analysis_backup_{timestamp} AS
            SELECT * FROM makenaide_technical_analysis
        """)

        self.conn.commit()
        logger.info(f"✅ 백업 완료: *_backup_{timestamp}")

    def replace_tables(self) -> None:
        """테이블 교체"""
        logger.info("🔄 테이블 교체 중...")

        # 기존 테이블 삭제
        self.conn.execute("DROP TABLE technical_analysis")
        self.conn.execute("DROP TABLE makenaide_technical_analysis")

        # 통합 테이블을 technical_analysis로 리네임
        self.conn.execute("ALTER TABLE technical_analysis_unified RENAME TO technical_analysis")

        self.conn.commit()
        logger.info("✅ 테이블 교체 완료")

    def run_migration(self) -> Dict[str, Any]:
        """전체 마이그레이션 실행"""
        self.migration_log["start_time"] = datetime.now().isoformat()

        try:
            logger.info("🚀 기술적 분석 테이블 통합 마이그레이션 시작")

            # 1. 소스 테이블 분석
            source_analysis = self.analyze_source_tables()
            logger.info(f"📊 소스 분석: {source_analysis}")

            # 2. 통합 테이블 생성
            self.create_unified_table()

            # 3. 데이터 마이그레이션
            self.migrate_data()

            # 4. 검증
            validation = self.validate_migration()
            logger.info(f"✅ 검증 결과: {validation}")

            # 5. 백업 및 교체
            self.backup_existing_tables()
            self.replace_tables()

            self.migration_log["success"] = True
            self.migration_log["validation"] = validation

            logger.info("🎉 마이그레이션 성공적 완료!")

        except Exception as e:
            error_msg = f"마이그레이션 실패: {e}"
            logger.error(f"❌ {error_msg}")
            self.migration_log["errors"].append(error_msg)
            raise

        finally:
            self.migration_log["end_time"] = datetime.now().isoformat()

        return self.migration_log

    def save_migration_log(self) -> None:
        """마이그레이션 로그 저장"""
        log_filename = f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(log_filename, 'w', encoding='utf-8') as f:
            json.dump(self.migration_log, f, indent=2, ensure_ascii=False)

        logger.info(f"📝 마이그레이션 로그 저장: {log_filename}")

def main():
    """메인 실행 함수"""
    print("🔄 기술적 분석 테이블 통합 마이그레이션")
    print("=" * 50)

    try:
        with TechnicalAnalysisMigrator() as migrator:
            # 마이그레이션 실행
            result = migrator.run_migration()

            # 로그 저장
            migrator.save_migration_log()

            # 결과 출력
            print("\n📊 마이그레이션 완료!")
            print(f"⏰ 실행 시간: {result['start_time']} ~ {result['end_time']}")
            print(f"📈 성공 여부: {'✅ 성공' if result['success'] else '❌ 실패'}")
            print(f"📊 마이그레이션된 레코드: {result['target_count']}개")

            if result.get('validation'):
                validation = result['validation']['summary']
                print(f"🎯 검증 결과:")
                print(f"  - 총 레코드: {validation['total_records']}")
                print(f"  - 고유 종목: {validation['unique_tickers']}")
                print(f"  - 양쪽 소스: {validation['both_sources']}")
                print(f"  - Stage 데이터: {validation['stage_data_coverage']}")
                print(f"  - Score 데이터: {validation['score_data_coverage']}")

    except Exception as e:
        print(f"❌ 마이그레이션 실패: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())