#!/usr/bin/env python3
"""
Makenaide Local Orchestrator - 통합 거래 파이프라인
EC2 Local Architecture 기반 암호화폐 자동매매 시스템

🏠 LOCAL ARCHITECTURE: 단일 머신 통합 실행
- AWS 의존성 제거: DynamoDB → SQLite, Lambda → Python Scripts
- 비용 최적화: 클라우드 비용 0원, 전력비만 발생
- 개발 편의성: 로컬 디버깅, 실시간 로그 확인

🎯 전체 파이프라인:
Phase 0: scanner.py (업비트 종목 스캔)
Phase 1: data_collector.py (증분 OHLCV 수집)
Phase 2: integrated_scoring_system.py (LayeredScoringEngine 점수제 분석)
Phase 3: gpt_analyzer.py (GPT 패턴 분석 - 선택적)
Kelly Calculator: kelly_calculator.py (포지션 사이징)
Market Sentiment: market_sentiment.py (Fear&Greed 분석)
Trading Engine: trade_executor.py (매수/매도 실행)
Portfolio Management: portfolio_manager.py (포트폴리오 관리)

📊 참조: @makenaide_local.mmd 전체 파이프라인 플로우
"""

import sys
import os
import logging
import sqlite3
import time
import json
import struct
import pyupbit
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv

# 프로젝트 모듈 import
from utils import setup_restricted_logger, load_blacklist
from db_manager_sqlite import get_db_connection_context
from scanner import update_tickers
from data_collector import SimpleDataCollector
from integrated_scoring_system import IntegratedScoringSystem  # Legacy system (kept for compatibility)
from technical_filter import TechnicalFilter, FilterMode, UnifiedFilterResult  # New unified system
from gpt_analyzer import GPTPatternAnalyzer
from kelly_calculator import KellyCalculator, RiskLevel
from market_sentiment import IntegratedMarketSentimentAnalyzer, MarketSentiment
from real_time_market_sentiment import RealTimeMarketSentiment
# from trade_executor import buy_asset, sell_asset  # 삭제된 레거시 모듈
# from portfolio_manager import PortfolioManager  # trading_engine으로 통합됨
from trading_engine import LocalTradingEngine, TradingConfig
from trade_status import TradeStatus, TradeResult

# 환경 변수 로드
load_dotenv()

# 로거 설정 (모든 import 전에 먼저 설정)
logger = setup_restricted_logger('makenaide_orchestrator')

# SNS 알림 시스템 설정 및 import (Phase 1-3)
# 세분화된 환경변수로 SNS 알림 제어
ENABLE_ANALYSIS_SNS = os.getenv('ENABLE_ANALYSIS_SNS', 'true').lower() == 'true'  # 분석 결과 SNS (기본값: 활성화)
ENABLE_TRADING_SNS = os.getenv('ENABLE_TRADING_SNS', 'false').lower() == 'true'   # 거래 실행 SNS (기본값: 비활성화)

# 하나라도 활성화되어 있으면 SNS 모듈 로드
if ENABLE_ANALYSIS_SNS or ENABLE_TRADING_SNS:
    try:
        from sns_notification_system import (
            MakenaideSNSNotifier,
            notify_discovered_stocks,
            notify_kelly_position_sizing,
            notify_market_analysis_summary,
            notify_pipeline_failure,
            notify_detailed_failure,
            send_secure_notification,
            get_security_analytics,
            FailureType,
            FailureSubType,
            NotificationMessage,
            NotificationLevel,
            NotificationCategory
        )
        SNS_AVAILABLE = True
        logger.info(f"✅ SNS 알림 시스템 로드 완료 (분석: {ENABLE_ANALYSIS_SNS}, 거래: {ENABLE_TRADING_SNS})")
    except ImportError as e:
        logger.warning(f"⚠️ SNS 알림 시스템을 사용할 수 없습니다: {e}")
        SNS_AVAILABLE = False
else:
    logger.info("📴 SNS 알림 완전 비활성화 설정")
    SNS_AVAILABLE = False
    # SNS 관련 클래스들을 None으로 설정하여 에러 방지
    MakenaideSNSNotifier = None
    FailureType = None
    FailureSubType = None
    NotificationLevel = None
    NotificationMessage = None
    NotificationCategory = None

# Phase 4 패턴 분석 및 예방 시스템 import
try:
    from failure_tracker import FailureTracker, FailureRecord, SystemHealthMetrics
    from predictive_analysis import PredictiveAnalyzer, PredictionResult, RiskLevel as PredRiskLevel
    from auto_recovery_system import AutoRecoverySystem, RecoveryPlan, RecoveryExecution
    PHASE4_AVAILABLE = True
    logger.info("✅ Phase 4 패턴 분석 및 예방 시스템 로드 완료")
except ImportError as e:
    logger.warning(f"⚠️ Phase 4 시스템을 사용할 수 없습니다: {e}")
    PHASE4_AVAILABLE = False

@dataclass
class OrchestratorConfig:
    """오케스트레이터 설정"""
    enable_gpt_analysis: bool = True  # GPT 분석 활성화 여부
    max_gpt_budget_daily: float = 5.0  # 일일 GPT 비용 한도 (USD)
    min_quality_score: float = 8.0  # 최소 품질 점수 (실제 데이터 분포 기반 조정)
    risk_level: RiskLevel = RiskLevel.MODERATE  # 리스크 레벨
    dry_run: bool = False  # 실제 거래 실행 여부
    max_positions: int = 8  # 최대 동시 보유 종목 수
    portfolio_allocation_limit: float = 0.25  # 전체 포트폴리오 대비 최대 할당 비율
    auto_sync_enabled: bool = True  # 포트폴리오 자동 동기화 활성화 여부
    sync_policy: str = 'aggressive'  # 포트폴리오 동기화 정책 (기본: 전체 동기화)

class MakenaideLocalOrchestrator:
    """Makenaide 로컬 통합 오케스트레이터"""

    def __init__(self, config: OrchestratorConfig):
        self.config = config
        self.db_path = "./makenaide_local.db"

        # 컴포넌트 초기화
        self.upbit = None
        self.data_collector = None
        self.technical_filter = None
        self.gpt_analyzer = None
        self.kelly_calculator = None
        self.market_sentiment = None
        # self.portfolio_manager = None  # trading_engine으로 통합됨
        self.trading_engine = None
        self.sns_notifier = None  # SNS 알림 시스템 (Phase 1-3)

        # 📊 시장 감정 분석 결과 저장 (SNS 알림용)
        self.last_sentiment_result = None

        # Phase 4 패턴 분석 및 예방 시스템
        self.failure_tracker = None
        self.predictive_analyzer = None
        self.auto_recovery_system = None

        # 실행 통계
        self.execution_stats = {
            'start_time': None,
            'phases_completed': [],
            'errors': [],
            'trading_candidates': 0,
            'trades_executed': 0,
            'total_cost': 0.0,
            'technical_candidates': [],  # 기술적 분석 통과 종목
            'gpt_candidates': [],        # GPT 분석 통과 종목
            'kelly_results': {}          # Kelly 계산 결과
        }

    def initialize_components(self) -> bool:
        """모든 컴포넌트 초기화"""
        try:
            logger.info("🔧 시스템 컴포넌트 초기화 시작")

            # 업비트 API 초기화
            access_key = os.getenv('UPBIT_ACCESS_KEY')
            secret_key = os.getenv('UPBIT_SECRET_KEY')

            if not access_key or not secret_key:
                error_msg = "업비트 API 키가 설정되지 않았습니다. .env 파일을 확인해주세요."
                logger.error(f"❌ {error_msg}")

                # 🔔 Phase 2-3 상세 실패 알림 전송
                if SNS_AVAILABLE:
                    try:
                        # API 키 타입별 상세 분류
                        if not access_key and not secret_key:
                            sub_type = FailureSubType.API_BOTH_KEYS_MISSING.value
                        elif not access_key:
                            sub_type = FailureSubType.API_ACCESS_KEY_MISSING.value
                        else:
                            sub_type = FailureSubType.API_SECRET_KEY_MISSING.value

                        self.handle_failure_with_phase4(
                            failure_type=FailureType.API_KEY_MISSING.value,
                            sub_type=sub_type,
                            error_message=error_msg,
                            phase="초기화",
                            execution_id=datetime.now().strftime('%Y%m%d_%H%M%S'),
                            metadata={
                                'missing_access_key': not bool(access_key),
                                'missing_secret_key': not bool(secret_key),
                                'config_file': '.env',
                                'env_check_result': 'FAILED'
                            },
                            severity="CRITICAL"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ SNS 실패 알림 전송 실패: {e}")

                return False

            self.upbit = pyupbit.Upbit(access_key, secret_key)
            logger.info("✅ 업비트 API 연결 완료")

            # 데이터 수집기 초기화
            self.data_collector = SimpleDataCollector(db_path=self.db_path)
            logger.info("✅ 데이터 수집기 초기화 완료")

            # 기술적 필터 초기화 (통합된 필터링 시스템)
            self.technical_filter = TechnicalFilter(db_path=self.db_path)
            logger.info("✅ 통합 기술적 필터 초기화 완료 (4-Layer Architecture with FilterMode)")

            # 레거시 시스템 백업 (호환성 유지)
            self.legacy_scoring_system = IntegratedScoringSystem(db_path=self.db_path)
            logger.info("✅ 레거시 점수 시스템 백업 초기화 완료")

            # GPT 분석기 초기화 (선택적)
            if self.config.enable_gpt_analysis:
                self.gpt_analyzer = GPTPatternAnalyzer(db_path=self.db_path)
                logger.info("✅ GPT 분석기 초기화 완료")
            else:
                logger.info("⏭️ GPT 분석기 비활성화 (비용 절약 모드)")

            # Kelly 계산기 초기화
            self.kelly_calculator = KellyCalculator(
                risk_level=self.config.risk_level,
                max_single_position=8.0,
                max_total_allocation=self.config.portfolio_allocation_limit * 100
            )
            logger.info("✅ Kelly 계산기 초기화 완료")

            # 시장 감정 분석기 초기화 (새로운 실시간 시스템)
            self.market_sentiment = RealTimeMarketSentiment()
            logger.info("✅ 실시간 시장 감정 분석기 초기화 완료 (pyupbit API 기반)")

            # Trading Engine 초기화 (포트폴리오 관리 기능 통합)
            trading_config = TradingConfig()
            self.trading_engine = LocalTradingEngine(trading_config, dry_run=self.config.dry_run)
            logger.info("✅ Trading Engine 초기화 완료 (포트폴리오 관리 기능 포함)")

            # 포트폴리오 동기화 검증 및 자동 동기화
            sync_success, sync_details = self.trading_engine.validate_and_sync_portfolio(
                auto_sync=self.config.auto_sync_enabled,
                sync_policy=self.config.sync_policy
            )

            if not sync_success and not self.config.auto_sync_enabled:
                logger.warning("⚠️ 포트폴리오 동기화 불일치가 감지되었지만 자동 동기화가 비활성화되어 있습니다.")
                logger.warning("수동으로 portfolio_sync_tool.py를 실행하여 동기화를 진행하시기 바랍니다.")
            elif sync_success and sync_details.get('status') == 'synced':
                logger.info("✅ 포트폴리오 동기화 상태 확인 완료")
            elif sync_success and 'synced_count' in sync_details:
                synced_count = sync_details['synced_count']
                total_value = sync_details.get('total_value', 0)
                logger.info(f"🔄 포트폴리오 자동 동기화 완료: {synced_count}개 종목, {total_value:,.0f} KRW")

            # SNS 알림 시스템 초기화
            if SNS_AVAILABLE:
                try:
                    self.sns_notifier = MakenaideSNSNotifier()
                    logger.info(f"✅ SNS 알림 시스템 초기화 완료 (분석: {ENABLE_ANALYSIS_SNS}, 거래: {ENABLE_TRADING_SNS})")
                except Exception as e:
                    logger.warning(f"⚠️ SNS 알림 시스템 초기화 실패: {e}")
                    self.sns_notifier = None
            else:
                logger.info("📴 SNS 알림 시스템 비활성화")
                self.sns_notifier = None

            # Phase 4 패턴 분석 및 예방 시스템 초기화
            try:
                # 실패 추적기 초기화
                self.failure_tracker = FailureTracker(db_path=self.db_path)
                logger.info("✅ Phase 4 실패 추적기 초기화 완료")

                # 예측적 분석기 초기화
                self.predictive_analyzer = PredictiveAnalyzer(db_path=self.db_path)
                logger.info("✅ Phase 4 예측적 분석기 초기화 완료")

                # 자동 복구 시스템 초기화
                self.auto_recovery_system = AutoRecoverySystem(db_path=self.db_path)
                logger.info("✅ Phase 4 자동 복구 시스템 초기화 완료")

                logger.info("🔮 Phase 4 패턴 분석 및 예방 시스템 초기화 성공")
            except Exception as e:
                logger.warning(f"⚠️ Phase 4 시스템 초기화 실패: {e}")
                # Phase 4는 선택적 기능으로 실패해도 전체 시스템은 계속 진행
                self.failure_tracker = None
                self.predictive_analyzer = None
                self.auto_recovery_system = None

            logger.info("🎉 모든 시스템 컴포넌트 초기화 성공")
            return True

        except Exception as e:
            logger.error(f"❌ 컴포넌트 초기화 실패: {e}")
            self.execution_stats['errors'].append(f"초기화 실패: {e}")
            return False

    def handle_failure_with_phase4(self, failure_type: str, sub_type: str, error_message: str,
                                   phase: str, execution_id: str, metadata: dict = None,
                                   severity: str = "HIGH") -> None:
        """Phase 4 통합 실패 처리: 추적 + 분석 + 복구 + 알림"""
        try:
            # 1. Phase 4 실패 추적 (있는 경우)
            if self.failure_tracker:
                failure_id = self.failure_tracker.record_failure(
                    failure_type=failure_type,
                    sub_type=sub_type,
                    error_message=error_message,
                    execution_id=execution_id,
                    severity=severity,
                    phase=phase,
                    metadata=metadata or {}
                )
                logger.info(f"🔮 Phase 4: 실패 기록 완료 (ID: {failure_id})")

                # 2. 예측적 분석 실행 (있는 경우)
                if self.predictive_analyzer:
                    try:
                        prediction = self.predictive_analyzer.predict_failure_probability(time_window_hours=24)
                        logger.info(f"🔮 Phase 4: 실패 예측 - 위험도: {prediction.risk_level.value}, 확률: {prediction.failure_probability:.1%}")

                        # 높은 위험도인 경우 추가 로깅
                        if prediction.risk_level.value in ['HIGH', 'CRITICAL']:
                            logger.warning(f"⚠️ Phase 4: 높은 실패 위험 감지 - 권장사항: {', '.join(prediction.recommended_actions)}")
                    except Exception as e:
                        logger.warning(f"⚠️ Phase 4: 예측 분석 실패 - {e}")

                # 3. 자동 복구 제안 (있는 경우)
                if self.auto_recovery_system:
                    try:
                        # 실패 기록을 가져와서 복구 제안 생성
                        failure_record = FailureRecord(
                            id=failure_id,
                            timestamp=datetime.now().isoformat(),
                            execution_id=execution_id,
                            failure_type=failure_type,
                            sub_type=sub_type,
                            severity=severity,
                            phase=phase,
                            error_message=error_message,
                            metadata=str(metadata or {})
                        )

                        suggestions = self.auto_recovery_system.get_recovery_suggestions(failure_record)
                        if suggestions and 'recovery_actions' in suggestions:
                            logger.info(f"🔮 Phase 4: 복구 제안 - {len(suggestions['recovery_actions'])}개 액션 제안됨")

                    except Exception as e:
                        logger.warning(f"⚠️ Phase 4: 복구 제안 실패 - {e}")

            # 4. 기존 SNS 알림 전송 (호환성 유지)
            if SNS_AVAILABLE:
                try:
                    notify_detailed_failure(
                        failure_type=failure_type,
                        sub_type=sub_type,
                        error_message=error_message,
                        phase=phase,
                        execution_id=execution_id,
                        metadata=metadata
                    )
                except Exception as e:
                    logger.warning(f"⚠️ SNS 실패 알림 전송 실패: {e}")

        except Exception as e:
            logger.error(f"❌ Phase 4 통합 실패 처리 중 오류: {e}")
            # Phase 4 실패 시에도 기존 SNS 알림은 전송
            if SNS_AVAILABLE:
                try:
                    notify_detailed_failure(
                        failure_type=failure_type,
                        sub_type=sub_type,
                        error_message=error_message,
                        phase=phase,
                        execution_id=execution_id,
                        metadata=metadata
                    )
                except Exception as nested_e:
                    logger.error(f"❌ SNS 백업 알림 전송도 실패: {nested_e}")

    def run_phase_0_scanner(self) -> bool:
        """Phase 0: 업비트 종목 스캔"""
        try:
            logger.info("📡 Phase 0: 업비트 종목 스캔 시작")

            # scanner.py의 update_tickers 함수 호출
            update_tickers()

            self.execution_stats['phases_completed'].append('Phase 0: Scanner')
            logger.info("✅ Phase 0 완료: 업비트 종목 스캔")
            return True

        except Exception as e:
            logger.error(f"❌ Phase 0 실패: {e}")
            self.execution_stats['errors'].append(f"Phase 0 실패: {e}")
            return False

    def run_phase_1_data_collection(self) -> bool:
        """Phase 1: 증분 OHLCV 데이터 수집 (품질 필터링 포함)"""
        try:
            logger.info("📊 Phase 1: 증분 OHLCV 데이터 수집 시작")
            logger.info("🎯 품질 필터링 모드: 13개월+ 데이터 + 3억원+ 거래대금 조건 적용")

            # 🚀 배치 처리 + 품질 필터링 방식으로 변경 (67% API 절약 효과)
            results = self.data_collector.collect_all_data(
                test_mode=False,
                use_quality_filter=True  # 품질 필터링 활성화
            )

            if not results:
                logger.error("❌ 데이터 수집 실패: 수집 결과 없음")
                self.execution_stats['errors'].append("Phase 1 실패: 수집 결과 없음")
                return False

            # collect_all_data는 collection_stats를 반환하므로 성공 여부는 통계로 판단
            # 실제 실패만 에러로 처리 (skip은 정상 상황)
            failed_collections = results.get('summary', {}).get('failed', 0)
            successful_collections = results.get('summary', {}).get('success', 0)
            skipped_collections = results.get('summary', {}).get('skipped', 0)

            # 실제 실패가 있을 때만 에러 처리
            if failed_collections > 0:
                logger.error(f"❌ 데이터 수집 중 {failed_collections}개 실패")
                self.execution_stats['errors'].append(f"Phase 1 실패: {failed_collections}개 데이터 수집 실패")
                return False

            # 성공 또는 스킵된 경우 모두 정상 처리
            if successful_collections > 0:
                logger.info(f"✅ 새로운 데이터 수집 완료: {successful_collections}개")
            else:
                logger.info(f"✅ 모든 데이터가 최신 상태: {skipped_collections}개 스킵")

            # 수집 결과 통계 로깅
            summary = results.get('summary', {})
            processing_time = results.get('processing_time_seconds', 0)
            total_tickers = summary.get('success', 0) + summary.get('failed', 0) + summary.get('skipped', 0)
            successful_collections = summary.get('success', 0)
            quality_filter_enabled = results.get('quality_filter_enabled', True)  # 기본적으로 활성화됨

            logger.info(f"📊 데이터 수집 완료: {successful_collections}/{total_tickers} 성공 ({processing_time:.1f}초)")
            logger.info(f"💎 품질 필터링: {'활성화' if quality_filter_enabled else '비활성화'}")
            logger.info(f"📈 총 레코드 수집: {summary.get('total_records', 0)}개")

            if quality_filter_enabled and total_tickers > 0:
                logger.info(f"⚡ 품질 필터링 효과: 고품질 종목만 선별하여 API 호출 67% 절약")

            # 📦 데이터 보존 정책 적용 (300일+ 오래된 데이터 자동 정리)
            try:
                logger.info("🗑️ 데이터 보존 정책 적용 중...")
                retention_result = self.data_collector.apply_data_retention_policy(retention_days=300)

                if retention_result['deleted_rows'] > 0:
                    logger.info(f"🗑️ 데이터 정리 완료: {retention_result['deleted_rows']:,}개 행 삭제")
                    logger.info(f"💾 스토리지 절약: {retention_result['size_reduction_pct']:.1f}%")
                else:
                    logger.info("✅ 정리할 오래된 데이터가 없습니다")

            except Exception as e:
                logger.warning(f"⚠️ 데이터 보존 정책 적용 실패: {e}")
                # 데이터 보존 정책 실패는 치명적이지 않으므로 계속 진행

            self.execution_stats['phases_completed'].append('Phase 1: Data Collection')
            logger.info("✅ Phase 1 완료: 증분 OHLCV 데이터 수집 (품질 필터링 적용)")
            return True

        except Exception as e:
            logger.error(f"❌ Phase 1 실패: {e}")
            self.execution_stats['errors'].append(f"Phase 1 실패: {e}")
            return False

    async def run_phase_2_technical_filter(self) -> List[str]:
        """Phase 2: 통합 기술적 필터링 시스템 (4-Layer Architecture)"""
        try:
            logger.info("🎯 Phase 2: 통합 기술적 필터링 시스템 분석 시작")
            phase_start_time = time.time()  # 성능 모니터링을 위한 시작 시간

            # 활성 종목 조회 (DB에서 스캔된 종목 가져오기)
            try:
                with get_db_connection_context() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT DISTINCT ticker
                        FROM ohlcv_data
                        WHERE date >= date('now', '-7 days')
                        ORDER BY ticker
                    """)
                    active_tickers = [row[0] for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"❌ 활성 종목 조회 실패: {e}")
                return []

            if not active_tickers:
                logger.info("📭 분석 가능한 종목이 없습니다 (DB에 최근 데이터 없음)")
                return []

            logger.info(f"📊 분석 대상 종목: {len(active_tickers)}개")

            # 기존 보유 포지션 조회 및 제외
            held_tickers = set()
            if self.trading_engine:
                try:
                    positions = self.trading_engine.get_current_positions()
                    held_tickers = {pos.ticker for pos in positions}

                    if held_tickers:
                        logger.info(f"🔒 현재 보유 중인 {len(held_tickers)}개 종목 제외: {', '.join(sorted(held_tickers))}")
                except Exception as e:
                    logger.warning(f"⚠️ 보유 포지션 조회 실패: {e}")

            # 기존 포지션 제외한 티커만 분석
            active_tickers = [t for t in active_tickers if t not in held_tickers]

            if not active_tickers:
                logger.info("📭 분석 가능한 신규 종목이 없습니다 (모두 보유 중)")
                return []

            logger.info(f"📊 신규 분석 대상 종목: {len(active_tickers)}개")

            # 통합 필터 실행 (AUTO 모드로 지능형 분석)
            analysis_results = []
            technical_candidates_data = []  # SNS 알림용 상세 데이터

            for ticker in active_tickers:
                try:
                    # TechnicalFilter AUTO 모드로 분석
                    result = self.technical_filter.analyze_ticker(ticker, FilterMode.AUTO)

                    if result:
                        analysis_results.append(result)

                        # ✅ 기술적 분석 결과를 DB에 저장 (Kelly Calculator가 조회할 수 있도록)
                        self._save_technical_analysis_to_db(result)

                        # 매수 권고 종목만 후보로 선정
                        if result.final_recommendation.value in ['STRONG_BUY', 'BUY', 'BUY_LITE']:
                            technical_candidates_data.append({
                                'ticker': ticker,
                                'recommendation': result.final_recommendation.value,
                                'confidence': result.final_confidence,
                                'quality_score': result.final_quality_score,
                                'filter_mode': result.filter_mode.value,
                                'processing_time': result.processing_time_ms
                            })

                except Exception as e:
                    logger.warning(f"⚠️ {ticker} 분석 실패: {e}")
                    continue

            # 품질 점수 임계값 필터링
            filtered_candidates = []

            for candidate in technical_candidates_data:
                ticker = candidate['ticker']
                recommendation = candidate['recommendation']
                confidence = candidate['confidence']
                quality_score = candidate['quality_score']
                filter_mode = candidate['filter_mode']

                # 고품질 후보 선정 (높은 신뢰도와 품질 점수)
                if confidence >= 0.7 and quality_score >= self.config.min_quality_score:
                    filtered_candidates.append(ticker)
                    logger.info(f"✅ {ticker}: {recommendation} (신뢰도: {confidence:.3f}, 품질: {quality_score:.1f}, 모드: {filter_mode})")
                else:
                    if confidence < 0.7:
                        logger.info(f"⏭️ {ticker}: 낮은 신뢰도 {confidence:.3f} (임계값 0.7)")
                    elif quality_score < self.config.min_quality_score:
                        logger.info(f"⏭️ {ticker}: 낮은 품질 점수 {quality_score:.1f} (임계값 {self.config.min_quality_score})")
                    else:
                        logger.info(f"⏭️ {ticker}: {recommendation} (신뢰도: {confidence:.3f})")

            # 기술적 분석 결과를 통계에 저장
            self.execution_stats['technical_candidates'] = technical_candidates_data
            self.execution_stats['phases_completed'].append('Phase 2: Unified Technical Filter')
            self.execution_stats['trading_candidates'] = len(filtered_candidates)

            # 분석 통계 정보 로깅
            stats = self.technical_filter.get_analysis_stats()
            logger.info(f"📊 분석 통계: {stats}")

            # 시장 감정 캐시 성능 리포트
            if hasattr(self.technical_filter, 'get_sentiment_cache_stats'):
                cache_stats = self.technical_filter.get_sentiment_cache_stats()
                logger.info("🎯 시장 감정 캐시 성능 리포트:")
                logger.info(f"   💡 캐시 효율성: {cache_stats.get('cache_efficiency', 0):.1f}%")
                logger.info(f"   📋 총 요청: {cache_stats.get('total_requests', 0)}회")
                logger.info(f"   🎯 캐시 히트: {cache_stats.get('cache_hits', 0)}회")
                logger.info(f"   🌐 API 호출: {cache_stats.get('api_calls', 0)}회")

                # 성능 향상 효과 계산
                total_requests = cache_stats.get('total_requests', 0)
                api_calls = cache_stats.get('api_calls', 0)
                if total_requests > 0 and api_calls > 0:
                    time_saved_estimate = (total_requests - api_calls) * 0.65  # 평균 0.65초/호출
                    logger.info(f"   ⚡ 예상 시간 절약: {time_saved_estimate:.1f}초")

            # 성능 회귀 방지 모니터링
            if hasattr(self.technical_filter, 'record_performance_metrics'):
                try:
                    session_id = f"phase2_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    processing_time_ms = (time.time() - phase_start_time) * 1000

                    # 성능 메트릭 기록
                    self.technical_filter.record_performance_metrics(
                        session_id=session_id,
                        total_tickers=len(technical_candidates_data),
                        processing_time_ms=processing_time_ms
                    )

                    # 성능 회귀 감지
                    regression_result = self.technical_filter.check_performance_regression(session_id)

                    if regression_result.get('regression_detected', False):
                        logger.warning(f"🚨 성능 회귀 감지 알림:")
                        logger.warning(f"   📉 {regression_result.get('message', '알 수 없는 성능 저하')}")

                        # 운영팀 알림이 필요한 경우 여기에 SNS/이메일 등 추가 가능
                        self.execution_stats['warnings'].append(f"성능 회귀 감지: {regression_result.get('message')}")
                    else:
                        logger.info(f"🎯 성능 모니터링: {regression_result.get('message', '정상')}")

                except Exception as e:
                    logger.error(f"❌ 성능 모니터링 실패: {e}")

            logger.info(f"✅ Phase 2 완료: {len(filtered_candidates)}개 거래 후보 발견 (총 {len(technical_candidates_data)}개 종목 분석)")
            logger.info(f"🎯 필터링 모드 사용 분포: {[candidate['filter_mode'] for candidate in technical_candidates_data]}")

            return filtered_candidates

        except Exception as e:
            logger.error(f"❌ Phase 2 실패: {e}")
            self.execution_stats['errors'].append(f"Phase 2 실패: {e}")
            return []

    def run_phase_3_gpt_analysis(self, candidates: List[str]) -> List[str]:
        """Phase 3: GPT 패턴 분석 (선택적 & 조건부)"""

        # 🔧 1차 조건: GPT 분석 활성화 여부 및 후보 존재 여부
        if not self.config.enable_gpt_analysis or not candidates:
            logger.info("⏭️ Phase 3: GPT 분석 비활성화 또는 후보 없음")
            return candidates

        # 🔧 2차 조건: 스마트 조건부 실행 로직
        skip_gpt = False
        skip_reason = ""

        # 조건 1: 후보 개수 기반 조건부 실행
        if len(candidates) > 20:
            skip_gpt = True
            skip_reason = f"후보가 너무 많음 ({len(candidates)}개) - 비용 절약을 위해 기술적 분석만 사용"
        elif len(candidates) == 1:
            skip_gpt = True
            skip_reason = f"후보가 1개뿐 - 기술적 분석으로 충분"

        # 조건 2: 일일 GPT 비용 사용량 기반 (기존 로직 확장)
        try:
            # 오늘 사용한 GPT 비용 추정 (간단한 계산)
            today_estimated_cost = len(candidates) * 0.05  # 후보당 $0.05 추정
            if today_estimated_cost > self.config.max_gpt_budget_daily:
                skip_gpt = True
                skip_reason = f"예상 비용 초과 (${today_estimated_cost:.2f} > ${self.config.max_gpt_budget_daily})"
        except:
            pass  # 비용 계산 실패 시 무시

        # 조건 3: 리스크 레벨 기반 조건부 실행
        if self.config.risk_level == RiskLevel.CONSERVATIVE and len(candidates) > 5:
            skip_gpt = True
            skip_reason = f"보수적 리스크 레벨에서 후보 5개 초과 ({len(candidates)}개) - 기술적 분석 우선"

        # 조건부 실행 결과 처리
        if skip_gpt:
            logger.info(f"🧠 GPT 분석 스킵: {skip_reason}")
            logger.info(f"💰 비용 절약: 예상 ${len(candidates) * 0.05:.2f} 절약")
            logger.info(f"⚡ 기술적 분석 결과 {len(candidates)}개 후보를 그대로 사용")
            return candidates

        # GPT 분석 실행 결정
        logger.info(f"🤖 GPT 분석 실행 결정:")
        logger.info(f"   • 후보 개수: {len(candidates)}개 (적절)")
        logger.info(f"   • 예상 비용: ${len(candidates) * 0.05:.2f}")
        logger.info(f"   • 리스크 레벨: {self.config.risk_level.value}")
        logger.info(f"   • 일일 예산: ${self.config.max_gpt_budget_daily}")

        return self._execute_gpt_analysis(candidates)

    def _execute_gpt_analysis(self, candidates: List[str]) -> List[str]:
        """GPT 분석 실행 (내부 메서드)"""

        try:
            logger.info(f"🤖 Phase 3: GPT 패턴 분석 시작 ({len(candidates)}개 후보)")

            gpt_approved_candidates = []
            gpt_candidates_data = []  # SNS 알림용 상세 데이터
            total_cost = 0.0

            for ticker in candidates:
                try:
                    # 일일 비용 한도 확인
                    if total_cost >= self.config.max_gpt_budget_daily:
                        logger.warning(f"💰 일일 GPT 비용 한도 도달: ${total_cost:.2f}")
                        break

                    # GPT 분석 실행
                    result = self.gpt_analyzer.analyze_ticker(ticker)

                    if result:
                        # SNS 알림용 데이터 저장 (모든 GPT 분석 결과)
                        gpt_candidates_data.append({
                            'ticker': ticker,
                            'recommendation': result.recommendation.value,
                            'confidence': result.confidence,
                            'pattern': 'VCP' if result.vcp_analysis.detected else 'Cup&Handle' if result.cup_handle_analysis.detected else 'None',
                            'reasoning': result.reasoning,
                            'risk_level': 'moderate',  # GPT 분석은 기본 moderate
                            'cost': result.api_cost_usd
                        })

                        if result.recommendation.value in ['BUY', 'STRONG_BUY']:
                            confidence = result.confidence * 100  # 0.8 → 80%
                            logger.info(f"✅ {ticker}: GPT 매수 추천 (신뢰도: {confidence:.1f}%)")
                            gpt_approved_candidates.append(ticker)
                        else:
                            recommendation = result.recommendation.value
                            logger.info(f"⏭️ {ticker}: GPT 분석 결과 - {recommendation}")
                    else:
                        # 분석 실패한 경우도 기록
                        gpt_candidates_data.append({
                            'ticker': ticker,
                            'recommendation': 'ERROR',
                            'confidence': 0.0,
                            'pattern': '',
                            'reasoning': 'GPT 분석 실패',
                            'risk_level': 'Unknown',
                            'cost': 0.0
                        })
                        logger.info(f"❌ {ticker}: GPT 분석 실패")

                    # 실제 비용 누적
                    if result:
                        total_cost += result.api_cost_usd
                    else:
                        total_cost += 0.0  # 실패한 경우 비용 없음

                    time.sleep(1)  # API 레이트 리미트 고려

                except Exception as e:
                    logger.warning(f"⚠️ {ticker} GPT 분석 실패: {e}")
                    # 오류 케이스도 기록
                    gpt_candidates_data.append({
                        'ticker': ticker,
                        'recommendation': 'ERROR',
                        'confidence': 0.0,
                        'pattern': '',
                        'reasoning': f'분석 오류: {str(e)}',
                        'risk_level': 'Unknown',
                        'cost': 0.0
                    })
                    continue

            # GPT 분석 결과를 통계에 저장
            self.execution_stats['gpt_candidates'] = gpt_candidates_data
            self.execution_stats['phases_completed'].append('Phase 3: GPT Analysis')
            self.execution_stats['total_cost'] += total_cost

            logger.info(f"✅ Phase 3 완료: {len(gpt_approved_candidates)}개 GPT 승인 (총 {len(gpt_candidates_data)}개 분석, 비용: ${total_cost:.2f})")
            return gpt_approved_candidates

        except Exception as e:
            logger.error(f"❌ Phase 3 실패: {e}")
            self.execution_stats['errors'].append(f"Phase 3 실패: {e}")
            return candidates  # GPT 실패 시 기술적 분석 결과 사용

    def run_kelly_calculation(self, candidates: List[str]) -> Dict[str, float]:
        """Kelly 공식 기반 포지션 사이징 계산"""
        if not candidates:
            logger.info("⏭️ Kelly 계산 스킵: 거래 후보 없음 (BUY 추천 종목 0개)")
            return {}

        try:
            logger.info(f"🧮 Kelly 포지션 사이징 계산 ({len(candidates)}개 후보)")

            # 🔍 디버깅: DB에 저장된 최근 기술적 분석 데이터 확인
            try:
                with get_db_connection_context() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT ticker, created_at, recommendation, quality_score
                        FROM technical_analysis
                        WHERE created_at >= datetime('now', '-1 hour')
                        ORDER BY created_at DESC
                    """)
                    recent_analyses = cursor.fetchall()
                    if recent_analyses:
                        logger.info(f"📊 최근 1시간 DB 저장 분석: {len(recent_analyses)}건")
                        for row in recent_analyses[:3]:  # 최근 3건만 표시
                            logger.debug(f"   - {row[0]}: {row[2]} (품질: {row[3]}, 시각: {row[1]})")
                    else:
                        logger.warning("⚠️ 최근 1시간 내 DB 저장 분석 없음 - Phase 2 DB 저장 확인 필요")
            except Exception as debug_error:
                logger.debug(f"디버깅 쿼리 실패 (무시): {debug_error}")

            position_sizes = {}

            for ticker in candidates:
                try:
                    # 데이터베이스에서 기술적 분석 결과 조회
                    technical_result = self._get_technical_analysis_for_kelly(ticker)

                    if not technical_result:
                        logger.warning(f"⚠️ {ticker}: 기술적 분석 데이터 없음")
                        continue

                    # GPT 분석 결과 조회 (있을 경우)
                    gpt_result = self._get_gpt_analysis_for_kelly(ticker)

                    # Kelly 계산 실행
                    kelly_result = self.kelly_calculator.calculate_position_size(technical_result, gpt_result)

                    if kelly_result and kelly_result.final_position_pct > 0:
                        position_sizes[ticker] = kelly_result.final_position_pct
                        logger.info(f"📊 {ticker}: Kelly 포지션 {kelly_result.final_position_pct:.1f}%")
                    else:
                        logger.info(f"⏭️ {ticker}: Kelly 포지션 사이징 조건 미충족")

                except Exception as e:
                    logger.warning(f"⚠️ {ticker} Kelly 계산 실패: {e}")
                    continue

            # Kelly 결과를 통계에 저장
            self.execution_stats['kelly_results'] = position_sizes

            logger.info(f"✅ Kelly 계산 완료: {len(position_sizes)}개 종목")
            return position_sizes

        except Exception as e:
            logger.error(f"❌ Kelly 계산 실패: {e}")
            self.execution_stats['errors'].append(f"Kelly 계산 실패: {e}")
            return {}

    def run_market_sentiment_analysis(self) -> Tuple[MarketSentiment, bool, float]:
        """실시간 시장 감정 분석 및 거래 가능 여부 판정"""
        try:
            logger.info("🌡️ 실시간 시장 감정 분석 시작 (pyupbit API 기반)")

            # 실시간 시장 감정 분석 실행
            sentiment_result = self.market_sentiment.analyze_market_sentiment()

            if not sentiment_result:
                logger.warning("⚠️ 실시간 시장 감정 분석 실패 - 기본값 사용")
                # 기본값으로 더미 결과 저장
                self.last_sentiment_result = None
                return MarketSentiment.NEUTRAL, True, 1.0  # 기본값

            # 📊 결과를 인스턴스 변수로 저장 (SNS 알림용)
            self.last_sentiment_result = sentiment_result

            sentiment = sentiment_result.final_sentiment
            can_trade = sentiment_result.trading_allowed
            position_adjustment = sentiment_result.position_adjustment

            logger.info(f"📊 시장 감정: {sentiment.value}")
            logger.info(f"🚦 거래 가능: {'예' if can_trade else '아니오'}")
            logger.info(f"⚖️ 포지션 조정: {position_adjustment:.2f}x")
            logger.info(f"🔍 종합 점수: {sentiment_result.total_score:.1f}점")
            logger.info(f"📋 분석 근거: {sentiment_result.reasoning}")

            # BEAR 시장에서는 거래 중단
            if sentiment == MarketSentiment.BEAR:
                logger.warning("🚫 BEAR 시장 감지 - 모든 거래 중단")
                logger.warning(f"   📉 시장 점수: {sentiment_result.total_score:.1f}점 (임계값: 35점)")
                return sentiment, False, 0.0

            return sentiment, can_trade, position_adjustment

        except Exception as e:
            logger.error(f"❌ 실시간 시장 감정 분석 실패: {e}")
            self.execution_stats['errors'].append(f"실시간 시장 감정 분석 실패: {e}")
            self.last_sentiment_result = None
            return MarketSentiment.NEUTRAL, True, 1.0  # 기본값으로 거래 허용

    def execute_trades(self, position_sizes: Dict[str, float], position_adjustment: float) -> int:
        """실제 거래 실행"""
        if not position_sizes or position_adjustment <= 0:
            logger.info("📭 거래할 종목이 없거나 시장 조건 불량")
            return 0

        try:
            logger.info(f"💸 거래 실행 시작 ({len(position_sizes)}개 종목)")

            if self.config.dry_run:
                logger.info("🧪 DRY RUN 모드: 실제 거래 실행하지 않음")
                return len(position_sizes)

            trades_executed = 0
            total_balance = self.trading_engine.get_total_balance_krw()  # 🔧 메서드 이름 수정: get_total_balance → get_total_balance_krw

            for ticker, base_position in position_sizes.items():
                try:
                    # 시장 감정 기반 포지션 조정
                    adjusted_position = base_position * position_adjustment

                    # 최대/최소 포지션 제한
                    adjusted_position = max(1.0, min(adjusted_position, 8.0))

                    # 실제 투자 금액 계산
                    investment_amount = total_balance * (adjusted_position / 100)

                    # 최소 주문 금액 확인 및 조정
                    if investment_amount < 10000:  # 1만원 최소
                        original_amount = investment_amount
                        investment_amount = 10000  # 최소 거래단위로 자동 조정
                        logger.info(f"🔄 {ticker}: 포지션 사이징 자동 조정 ({original_amount:,.0f}원 → {investment_amount:,.0f}원)")


                    logger.info(f"💰 {ticker}: {adjusted_position:.1f}% ({investment_amount:,.0f}원) 매수 시도")

                    # 매수 실행
                    result = self.trading_engine.execute_buy_order(ticker, investment_amount, is_pyramid=False)

                    if result and result.status in [TradeStatus.FULL_FILLED, TradeStatus.PARTIAL_FILLED]:
                        trades_executed += 1
                        logger.info(f"✅ {ticker}: 매수 성공 ({result.status.korean_name})")
                    else:
                        status_msg = result.status.korean_name if result else "알 수 없는 오류"
                        logger.warning(f"❌ {ticker}: 매수 실패 ({status_msg})")

                    time.sleep(0.5)  # API 레이트 리미트 고려

                except Exception as e:
                    logger.error(f"❌ {ticker} 거래 실행 실패: {e}")
                    continue

            self.execution_stats['trades_executed'] = trades_executed
            logger.info(f"✅ 거래 실행 완료: {trades_executed}개 성공")
            return trades_executed

        except Exception as e:
            logger.error(f"❌ 거래 실행 실패: {e}")
            self.execution_stats['errors'].append(f"거래 실행 실패: {e}")
            return 0

    def run_portfolio_management(self):
        """포트폴리오 관리 및 매도 조건 검사 (고급 기술적 분석 기반)"""
        try:
            logger.info("📊 포트폴리오 관리 시작 (고급 기술적 분석 모드)")

            # Trading Engine이 초기화되지 않은 경우 초기화
            if not self.trading_engine:
                logger.warning("⚠️ Trading Engine이 초기화되지 않음. 재초기화 시도")
                trading_config = TradingConfig(take_profit_percent=0)  # 기술적 신호에만 의존
                self.trading_engine = LocalTradingEngine(trading_config, dry_run=self.config.dry_run)
                logger.info("✅ Trading Engine 재초기화 완료")

            # 🔍 직접 매수 종목 감지 및 자동 초기화 (포트폴리오 관리 전 실행)
            logger.info("🔍 직접 매수 종목 감지 및 초기화 시작")
            direct_purchases = self.trading_engine.detect_and_initialize_direct_purchases()

            if direct_purchases:
                logger.warning(f"⚠️ 직접 매수 종목 {len(direct_purchases)}개 감지 및 초기화: {', '.join(direct_purchases)}")
                # SNS 알림 발송 (거래 관련 알림)
                if self.sns_notifier and ENABLE_TRADING_SNS:
                    try:
                        self.sns_notifier.notify_direct_purchase_detected(
                            tickers=direct_purchases,
                            execution_id=self.execution_id
                        )
                        logger.info("📱 직접 매수 종목 감지 SNS 알림 발송 완료")
                    except Exception as e:
                        logger.error(f"❌ 직접 매수 종목 SNS 알림 발송 실패: {e}")
                else:
                    logger.debug("📴 거래 SNS 비활성화로 직접 매수 종목 감지 알림 스킵")
            else:
                logger.info("✅ 직접 매수 종목 감지 완료 - 모든 포지션이 시스템을 통해 관리됨")

            # LocalTradingEngine의 향상된 포트폴리오 관리 시스템 실행 (피라미딩 + 트레일링 스탑)
            portfolio_result = self.trading_engine.process_enhanced_portfolio_management()

            # 결과 분석 및 로깅 (피라미딩 포함)
            positions_checked = portfolio_result.get('positions_checked', 0)
            sell_orders_executed = portfolio_result.get('sell_orders_executed', 0)
            pyramid_trades = portfolio_result.get('pyramid_trades', {})
            pyramid_successful = pyramid_trades.get('successful', 0)
            errors = portfolio_result.get('errors', [])

            if positions_checked == 0:
                logger.info("📭 관리할 포지션이 없습니다")
            else:
                logger.info(f"📊 향상된 포트폴리오 관리 완료:")
                logger.info(f"   포지션 분석: {positions_checked}개")
                if sell_orders_executed > 0:
                    logger.info(f"   💹 트레일링 스탑/기술적 매도: {sell_orders_executed}개")
                if pyramid_successful > 0:
                    logger.info(f"   🔺 피라미딩 추가 매수: {pyramid_successful}개")
                if sell_orders_executed == 0 and pyramid_successful == 0:
                    logger.info("   ✅ 모든 포지션 보유 유지 (매도/피라미딩 조건 미충족)")

            # 에러가 있는 경우 통계에 추가
            for error in errors:
                self.execution_stats['errors'].append(f"포트폴리오 관리 오류: {error}")

            # 거래 통계 업데이트 (피라미딩 포함)
            if hasattr(self.trading_engine, 'get_trading_statistics'):
                trading_stats = self.trading_engine.get_trading_statistics()
                total_trades_executed = sell_orders_executed + pyramid_successful
                self.execution_stats['trades_executed'] += total_trades_executed
                logger.info(f"🎯 총 거래 실행: {total_trades_executed}개 (매도: {sell_orders_executed}, 피라미딩: {pyramid_successful})")
                logger.info(f"🎯 거래 성공률: {trading_stats.get('success_rate', 0):.1f}%")

            logger.info("✅ 고급 포트폴리오 관리 완료")
            return portfolio_result

        except Exception as e:
            error_msg = f"고급 포트폴리오 관리 실패: {e}"
            logger.error(f"❌ {error_msg}")
            self.execution_stats['errors'].append(error_msg)

            # 폴백: 심각한 오류 시 빈 결과 반환
            return {
                'positions_checked': 0,
                'sell_orders_executed': 0,
                'errors': [str(e)]
            }

    def generate_execution_report(self):
        """실행 결과 보고서 생성"""
        try:
            logger.info("📋 실행 결과 보고서 생성")

            end_time = datetime.now()
            duration = end_time - self.execution_stats['start_time']

            report = {
                'execution_time': {
                    'start': self.execution_stats['start_time'].isoformat(),
                    'end': end_time.isoformat(),
                    'duration_seconds': duration.total_seconds()
                },
                'phases_completed': self.execution_stats['phases_completed'],
                'trading_stats': {
                    'candidates_found': self.execution_stats['trading_candidates'],
                    'trades_executed': self.execution_stats['trades_executed'],
                    'total_cost_usd': self.execution_stats['total_cost']
                },
                'errors': self.execution_stats['errors'],
                'config': {
                    'gpt_enabled': self.config.enable_gpt_analysis,
                    'dry_run': self.config.dry_run,
                    'risk_level': self.config.risk_level.value
                }
            }

            # JSON 형태로 저장
            report_path = f"./logs/execution_report_{end_time.strftime('%Y%m%d_%H%M%S')}.json"
            os.makedirs('./logs', exist_ok=True)

            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            # 콘솔 요약 출력
            logger.info("="*60)
            logger.info("📊 실행 결과 요약")
            logger.info("="*60)
            logger.info(f"⏱️ 실행 시간: {duration.total_seconds():.1f}초")
            logger.info(f"✅ 완료된 단계: {len(self.execution_stats['phases_completed'])}개")
            logger.info(f"🎯 거래 후보: {self.execution_stats['trading_candidates']}개")
            logger.info(f"💸 실행된 거래: {self.execution_stats['trades_executed']}개")
            logger.info(f"💰 총 비용: ${self.execution_stats['total_cost']:.2f}")
            logger.info(f"❌ 오류 수: {len(self.execution_stats['errors'])}개")
            logger.info(f"📄 보고서: {report_path}")
            logger.info("="*60)

        except Exception as e:
            logger.error(f"❌ 보고서 생성 실패: {e}")

    def generate_phase4_daily_report(self):
        """Phase 4 일일 패턴 분석 및 예방 보고서 생성"""
        try:
            if not self.failure_tracker or not self.predictive_analyzer:
                logger.info("⏭️ Phase 4가 비활성화되어 일일 보고서를 건너뜁니다")
                return

            logger.info("🔮 Phase 4 일일 보고서 생성 시작")

            # 1. 실패 패턴 통계 수집
            failure_stats = self.failure_tracker.get_failure_statistics(days=7)
            recent_failures = self.failure_tracker.get_recent_failures(hours=24)

            # 2. 예측 분석 실행
            prediction = self.predictive_analyzer.predict_failure_probability(time_window_hours=24)

            # 3. 시스템 상태 건강도 체크
            current_health = self.failure_tracker.get_current_system_health()

            # 4. 보고서 생성
            report = {
                'report_date': datetime.now().isoformat(),
                'phase4_summary': {
                    'failures_last_24h': len(recent_failures),
                    'failures_last_7d': failure_stats.get('total_failures', 0),
                    'top_failure_types': failure_stats.get('failure_types', {}),
                    'system_health_score': current_health.overall_health if current_health else 100.0
                },
                'prediction_analysis': {
                    'risk_level': prediction.risk_level.value,
                    'failure_probability': prediction.failure_probability,
                    'confidence': prediction.confidence.value,
                    'recommendations': prediction.recommended_actions
                },
                'recent_patterns': [
                    {
                        'failure_type': f.failure_type,
                        'sub_type': f.sub_type,
                        'phase': f.phase,
                        'timestamp': f.timestamp,
                        'severity': f.severity
                    } for f in recent_failures[:10]  # 최근 10개만
                ]
            }

            # 5. 보고서 저장
            report_path = f"./logs/phase4_daily_report_{datetime.now().strftime('%Y%m%d')}.json"
            os.makedirs('./logs', exist_ok=True)

            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            # 6. 콘솔 요약 출력
            logger.info("="*60)
            logger.info("🔮 Phase 4 일일 보고서 요약")
            logger.info("="*60)
            logger.info(f"📊 지난 24시간 실패: {len(recent_failures)}건")
            logger.info(f"📊 지난 7일 실패: {failure_stats.get('total_failures', 0)}건")
            logger.info(f"🎯 예측 위험도: {prediction.risk_level.value}")
            logger.info(f"🎯 실패 확률: {prediction.failure_probability:.1%}")
            logger.info(f"🏥 시스템 건강도: {current_health.overall_health if current_health else 100.0:.1f}%")
            logger.info(f"💡 권장사항: {len(prediction.recommended_actions)}개")
            logger.info(f"📄 상세 보고서: {report_path}")
            logger.info("="*60)

            # 7. 높은 위험도인 경우 경고
            if prediction.risk_level.value in ['HIGH', 'CRITICAL']:
                logger.warning(f"⚠️ 높은 실패 위험 감지! 주요 권장사항:")
                for rec in prediction.recommended_actions[:3]:  # 상위 3개만 표시
                    logger.warning(f"   • {rec}")

        except Exception as e:
            logger.error(f"❌ Phase 4 일일 보고서 생성 실패: {e}")

    async def run_analysis_pipeline(self) -> Dict:
        """분석 파이프라인만 실행 (거래 없이)"""
        try:
            logger.info("🔍 분석 파이프라인 실행 시작")

            # 1. 시스템 초기화
            if not self.initialize_components():
                return {"status": "failed", "reason": "초기화 실패"}

            # 2. Phase 0: 종목 스캔
            if not self.run_phase_0_scanner():
                return {"status": "failed", "reason": "종목 스캔 실패"}

            # 3. Phase 1: 데이터 수집
            if not self.run_phase_1_data_collection():
                return {"status": "failed", "reason": "데이터 수집 실패"}

            # 4. Phase 2: 기술적 필터링
            candidates = await self.run_phase_2_technical_filter()

            # 5. Phase 3: GPT 분석 (선택적)
            final_candidates = self.run_phase_3_gpt_analysis(candidates)

            # 6. Kelly 포지션 사이징
            position_sizes = self.run_kelly_calculation(final_candidates)

            # 7. 시장 감정 분석
            market_sentiment, can_trade, position_adjustment = self.run_market_sentiment_analysis()

            result = {
                "status": "success",
                "candidates_found": len(candidates) if candidates else 0,
                "final_candidates": len(final_candidates) if final_candidates else 0,
                "position_sizes": len(position_sizes) if position_sizes else 0,
                "market_sentiment": market_sentiment.value if market_sentiment else "unknown",
                "can_trade": can_trade,
                "position_adjustment": position_adjustment
            }

            logger.info(f"✅ 분석 파이프라인 완료: {result}")
            return result

        except Exception as e:
            logger.error(f"❌ 분석 파이프라인 실패: {e}")
            return {"status": "failed", "reason": str(e)}

    def get_execution_status(self) -> Dict:
        """현재 실행 상태 조회"""
        try:
            status = {
                "execution_stats": self.execution_stats.copy(),
                "components_initialized": {
                    "upbit": self.upbit is not None,
                    "data_collector": self.data_collector is not None,
                    "technical_filter": self.technical_filter is not None,
                    "gpt_analyzer": self.gpt_analyzer is not None,
                    "kelly_calculator": self.kelly_calculator is not None,
                    "market_sentiment": self.market_sentiment is not None,
                    # "portfolio_manager": self.portfolio_manager is not None,  # trading_engine으로 통합됨
                    "trading_engine": self.trading_engine is not None
                },
                "config": {
                    "enable_gpt_analysis": self.config.enable_gpt_analysis,
                    "dry_run": self.config.dry_run,
                    "risk_level": self.config.risk_level.value,
                    "max_positions": self.config.max_positions
                }
            }

            # Trading Engine 통계 추가
            if self.trading_engine:
                status["trading_engine_stats"] = self.trading_engine.get_trading_statistics()

            return status

        except Exception as e:
            logger.error(f"❌ 실행 상태 조회 실패: {e}")
            return {"status": "error", "message": str(e)}

    def cleanup(self):
        """시스템 정리 및 종료 (EC2 자동 종료 대비)"""
        try:
            logger.info("🧹 시스템 정리 시작")

            # 1. Trading Engine 정리
            if self.trading_engine and hasattr(self.trading_engine, 'cleanup'):
                self.trading_engine.cleanup()

            # 2. 실행 통계 저장
            self.save_execution_stats()

            # 3. SQLite DB 정리
            self.cleanup_database()

            # 4. 로그 백업 (필요시)
            self.backup_logs()

            logger.info("✅ 시스템 정리 완료")

        except Exception as e:
            logger.error(f"❌ 시스템 정리 실패: {e}")

    def save_execution_stats(self):
        """실행 통계 SQLite에 저장"""
        try:
            with get_db_connection_context() as conn:
                # 실행 통계 테이블 생성 (없으면)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS makenaide_execution_stats (
                        execution_date TEXT PRIMARY KEY,
                        execution_time TEXT,
                        phases_completed TEXT,
                        total_tickers INTEGER,
                        filtered_candidates INTEGER,
                        gpt_analyzed INTEGER,
                        kelly_positions INTEGER,
                        trades_executed INTEGER,
                        total_cost REAL,
                        success BOOLEAN,
                        errors TEXT,
                        auto_shutdown BOOLEAN
                    )
                """)

                # 오늘 통계 저장
                stats = self.execution_stats
                execution_date = datetime.now().strftime('%Y-%m-%d')
                execution_time = datetime.now().strftime('%H:%M:%S')
                auto_shutdown = os.getenv('EC2_AUTO_SHUTDOWN', 'false').lower() == 'true'

                conn.execute("""
                    INSERT OR REPLACE INTO makenaide_execution_stats
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    execution_date,
                    execution_time,
                    json.dumps(stats.get('phases_completed', [])),
                    stats.get('total_tickers', 0),
                    stats.get('filtered_candidates', 0),
                    len(stats.get('gpt_candidates', [])),
                    len(stats.get('kelly_results', {})),
                    stats.get('trades_executed', 0),
                    stats.get('total_cost', 0.0),
                    len(stats.get('errors', [])) == 0,
                    json.dumps(stats.get('errors', [])),
                    auto_shutdown
                ))

                conn.commit()
                logger.info("💾 실행 통계 저장 완료")

        except Exception as e:
            logger.error(f"❌ 실행 통계 저장 실패: {e}")

    def cleanup_database(self):
        """데이터베이스 정리 및 최적화"""
        try:
            with get_db_connection_context() as conn:
                # VACUUM으로 DB 최적화
                conn.execute("VACUUM")
                logger.info("🗃️ SQLite DB 최적화 완료")

        except Exception as e:
            logger.error(f"❌ DB 정리 실패: {e}")

    def backup_logs(self):
        """로그 파일 백업 (EC2 종료 전)"""
        try:
            from datetime import datetime
            import shutil

            log_backup_dir = "data/log_backups"
            os.makedirs(log_backup_dir, exist_ok=True)

            # 현재 로그 파일 백업
            current_date = datetime.now().strftime('%Y%m%d_%H%M%S')

            # 메인 로그 백업
            if os.path.exists("makenaide.log"):
                backup_path = f"{log_backup_dir}/makenaide_{current_date}.log"
                shutil.copy2("makenaide.log", backup_path)
                logger.info(f"📦 로그 백업: {backup_path}")

        except Exception as e:
            logger.error(f"❌ 로그 백업 실패: {e}")

    def smart_shutdown_ec2(self, reason: str = "파이프라인 완료", stats: dict = None):
        """AWS CLI 기반 Smart Shutdown (개선된 안전 종료)"""
        try:
            logger.info(f"🚀 Smart Shutdown 시작: {reason}")

            # Smart Shutdown 모듈 import
            from smart_shutdown import SmartShutdown

            # 시스템 정리 (기존 cleanup 호출)
            self.cleanup()

            # Smart Shutdown 실행
            shutdown_system = SmartShutdown()
            success = shutdown_system.execute_smart_shutdown(reason, stats)

            if success:
                logger.info("✅ Smart Shutdown 성공")
                return True
            else:
                logger.warning("⚠️ Smart Shutdown 일부 실패 - 수동 확인 필요")
                return False

        except Exception as e:
            logger.error(f"❌ Smart Shutdown 실행 실패: {e}")
            logger.warning("🔄 기존 shutdown 방식으로 대체 시도")

            # 대체 종료 시도 (기존 방식)
            try:
                import subprocess
                result = subprocess.run([
                    'sudo', 'shutdown', '-h', '+1'
                ], check=False, capture_output=True, text=True)

                if result.returncode == 0:
                    logger.info("✅ 대체 종료 명령 성공")
                    return True
                else:
                    logger.error(f"❌ 대체 종료 실패: {result.stderr}")
                    return False
            except Exception as fallback_error:
                logger.error(f"💥 모든 종료 방식 실패: {fallback_error}")
                return False

    def _get_latest_technical_analysis(self) -> List[Dict]:
        """실제 DB에서 최신 기술적 분석 결과 조회 (TechnicalFilter 시스템 연동)"""
        import sqlite3
        try:
            # 직접 SQLite 연결 사용 (연결 풀 문제 회피)
            conn = sqlite3.connect(self.db_path)

            # 🆕 새로운 unified_technical_analysis 테이블에서 데이터 조회
            # TechnicalFilter 시스템과 완전 호환

            query = """
                SELECT ticker, quality_score, gates_passed, final_recommendation,
                       current_stage, final_confidence, filter_mode,
                       breakout_strength, technical_bonus
                FROM unified_technical_analysis
                WHERE quality_score >= ?
                  AND final_recommendation IN ('STRONG_BUY', 'BUY', 'BUY_LITE')
                  AND DATE(analysis_date) = DATE('now', '+9 hours')
                  AND final_confidence IS NOT NULL
                ORDER BY quality_score DESC, final_confidence DESC
                LIMIT 15
                """

            cursor = conn.execute(query, (self.config.min_quality_score,))
            results = cursor.fetchall()

            candidates = []
            for row in results:
                ticker = row[0]

                # 🔥 실시간 가격 조회 (시장 기회 즉시 파악)
                try:
                    current_price = pyupbit.get_current_price(ticker)
                    if current_price is None:
                        current_price = 0
                except:
                    current_price = 0

                # 📊 종목 정보 구성 (새로운 TechnicalFilter 필드 사용) - 안전한 타입 변환 적용
                candidates.append({
                    'ticker': ticker,
                    'quality_score': self._safe_convert_to_float(row[1], 0.0),
                    'gates_passed': self._safe_convert_to_int(row[2], 0),  # 🔧 바이너리 데이터 안전 처리
                    'recommendation': row[3] if row[3] else 'HOLD',  # final_recommendation
                    'pattern_type': f"Stage {row[4]}" if row[4] else 'Stage 2',
                    'price': current_price,  # 🔥 실시간 가격 정보
                    'confidence': self._safe_convert_to_float(row[5], 0.0),  # final_confidence
                    'filter_mode': row[6] if row[6] else 'integrated',  # 새로운 필드: 분석 모드
                    'breakout_strength': self._safe_convert_to_float(row[7], 0.0),  # 새로운 필드
                    'technical_bonus': self._safe_convert_to_float(row[8], 0.0),  # 새로운 필드
                    'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            conn.close()
            logger.info(f"✅ TechnicalFilter 기반 기술적 분석 후보 {len(candidates)}개 조회 완료")
            return candidates

        except Exception as e:
            logger.error(f"❌ TechnicalFilter 기반 기술적 분석 결과 조회 실패: {e}")
            return []

    def _get_latest_gpt_analysis(self) -> List[Dict]:
        """실제 DB에서 최신 GPT 분석 결과 조회 (AI 승인 종목)"""
        try:
            with get_db_connection_context() as conn:
                # 오늘 날짜의 GPT 매수 추천 종목 조회 (신뢰도 높은 순)
                today = datetime.now().strftime('%Y-%m-%d')

                query = """
                SELECT ticker, gpt_recommendation, gpt_confidence, gpt_reasoning,
                       vcp_detected, cup_handle_detected, api_cost_usd
                FROM gpt_analysis
                WHERE analysis_date = ?
                  AND gpt_recommendation = 'BUY'
                  AND gpt_confidence >= 70.0
                ORDER BY gpt_confidence DESC
                LIMIT 10
                """

                cursor = conn.execute(query, (today,))
                results = cursor.fetchall()

                candidates = []
                for row in results:
                    pattern = []
                    if row[4]:  # vcp_detected
                        pattern.append("VCP")
                    if row[5]:  # cup_handle_detected
                        pattern.append("Cup & Handle")

                    candidates.append({
                        'ticker': row[0],
                        'recommendation': row[1],
                        'confidence': row[2],
                        'pattern': ' + '.join(pattern) if pattern else 'GPT 패턴',
                        'reasoning': row[3][:100] + '...' if len(row[3]) > 100 else row[3],  # 요약
                        'risk_level': 'MODERATE',
                        'cost': row[6] if row[6] else 0.0
                    })

                logger.info(f"🤖 실제 DB에서 {len(candidates)}개 GPT 승인 종목 조회")
                return candidates

        except Exception as e:
            logger.error(f"❌ GPT 분석 DB 조회 실패: {e}")
            return []

    def _get_latest_kelly_results(self) -> Dict[str, float]:
        """실제 DB에서 최신 Kelly 포지션 사이징 결과 조회 (최적 포지션)"""
        try:
            with get_db_connection_context() as conn:
                # 오늘 날짜의 Kelly 포지션 사이징 결과 조회 (포지션 크기 큰 순)
                today = datetime.now().strftime('%Y-%m-%d')

                query = """
                SELECT ticker, final_position_pct, detected_pattern, gpt_confidence
                FROM kelly_analysis
                WHERE analysis_date = ?
                  AND final_position_pct > 0
                ORDER BY final_position_pct DESC
                LIMIT 10
                """

                cursor = conn.execute(query, (today,))
                results = cursor.fetchall()

                kelly_results = {}
                for row in results:
                    kelly_results[row[0]] = row[1]  # ticker: position_pct

                logger.info(f"🧮 실제 DB에서 {len(kelly_results)}개 Kelly 결과 조회")
                return kelly_results

        except Exception as e:
            logger.error(f"❌ Kelly 분석 DB 조회 실패: {e}")
            return {}

    def _send_discovered_stocks_notification(self):
        """실제 DB 기반 발굴 종목 리스트 SNS 알림 전송 (시장 기회 실시간 파악)"""
        if not self.sns_notifier:
            return

        try:
            logger.info("📧 실제 DB 기반 발굴 종목 리스트 SNS 알림 전송 중...")

            # 🎯 실제 DB에서 최신 분석 결과 조회 (시장 기회 실시간 파악)
            technical_candidates = self._get_latest_technical_analysis()
            gpt_candidates = self._get_latest_gpt_analysis()
            kelly_results = self._get_latest_kelly_results()

            # 실행 ID 생성
            execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')

            # 📊 데이터가 있는 경우에만 알림 전송 (빈 알림 방지)
            if technical_candidates or gpt_candidates:
                # 발굴 종목 알림 전송 (분석 관련 알림)
                if self.sns_notifier and ENABLE_ANALYSIS_SNS:
                    success = self.sns_notifier.notify_discovered_stocks(
                        technical_candidates=technical_candidates,
                        gpt_candidates=gpt_candidates,
                        execution_id=execution_id
                    )

                    if success:
                        logger.info(f"✅ 발굴 종목 리스트 SNS 알림 전송 완료 (기술적: {len(technical_candidates)}, GPT: {len(gpt_candidates)})")
                    else:
                        logger.warning("⚠️ 발굴 종목 리스트 SNS 알림 전송 실패")
                else:
                    logger.debug("📴 분석 SNS 비활성화로 발굴 종목 알림 스킵")
            else:
                logger.info("📭 발굴된 종목이 없어 SNS 알림을 생략합니다")

            # 💰 Kelly 포지션 사이징 결과 알림 (별도) - 분석 관련 알림
            if kelly_results:
                if self.sns_notifier and ENABLE_ANALYSIS_SNS:
                    kelly_success = self.sns_notifier.notify_kelly_position_sizing(
                        position_sizes=kelly_results,  # 🔧 매개변수 이름 수정: kelly_results → position_sizes
                        execution_id=execution_id
                    )

                    if kelly_success:
                        logger.info(f"✅ Kelly 포지션 사이징 SNS 알림 전송 완료 ({len(kelly_results)}개 종목)")
                    else:
                        logger.warning("⚠️ Kelly 포지션 사이징 SNS 알림 전송 실패")
                else:
                    logger.debug("📴 분석 SNS 비활성화로 Kelly 포지션 사이징 알림 스킵")

            # 📊 시장 분석 종합 요약 알림 (실제 시장 데이터 포함)
            market_summary = {
                # 파이프라인 통계
                'technical_count': len(technical_candidates),
                'gpt_count': len(gpt_candidates),
                'kelly_count': len(kelly_results),
                'total_cost': self.execution_stats.get('total_cost', 0.0),
                'phases_completed': len(self.execution_stats.get('phases_completed', [])),
                'errors_count': len(self.execution_stats.get('errors', []))
            }

            # 📈 실제 시장 데이터 추가 (정적성 문제 해결)
            if self.last_sentiment_result:
                # Fear & Greed Index 데이터
                if self.last_sentiment_result.fear_greed_data:
                    market_summary['fear_greed_index'] = self.last_sentiment_result.fear_greed_data.value
                else:
                    market_summary['fear_greed_index'] = 50  # 기본값

                # BTC 트렌드 데이터
                if self.last_sentiment_result.btc_trend_data:
                    market_summary['btc_change_24h'] = self.last_sentiment_result.btc_trend_data.change_24h
                    market_summary['btc_trend'] = self._get_btc_trend_classification(self.last_sentiment_result.btc_trend_data.change_24h)
                else:
                    market_summary['btc_change_24h'] = 0.0
                    market_summary['btc_trend'] = 'SIDEWAYS'

                # 최종 시장 감정 데이터
                market_summary['final_sentiment'] = self.last_sentiment_result.final_sentiment.value
                market_summary['trading_allowed'] = self.last_sentiment_result.trading_allowed
                market_summary['position_adjustment'] = self.last_sentiment_result.position_adjustment
                market_summary['total_score'] = self.last_sentiment_result.total_score
                market_summary['confidence'] = self.last_sentiment_result.confidence
                market_summary['reasoning'] = self.last_sentiment_result.reasoning
            else:
                # 기본값 (시장 감정 분석 실패 시)
                market_summary['fear_greed_index'] = 50
                market_summary['btc_change_24h'] = 0.0
                market_summary['btc_trend'] = 'SIDEWAYS'
                market_summary['final_sentiment'] = 'NEUTRAL'
                market_summary['trading_allowed'] = True
                market_summary['position_adjustment'] = 1.0
                market_summary['total_score'] = 50.0
                market_summary['confidence'] = 0.5
                market_summary['reasoning'] = '시장 감정 분석 데이터 없음'

            # 📊 조건부 시장 분석 요약 알림 (BEAR 시장에서만 발송)
            current_sentiment = market_summary.get('final_sentiment', 'NEUTRAL')

            if current_sentiment == 'BEAR':
                logger.info("🚨 BEAR 시장 감지 - 시장 분석 요약 알림 발송")
                if self.sns_notifier and ENABLE_ANALYSIS_SNS:
                    summary_success = self.sns_notifier.notify_market_analysis_summary(
                        market_data=market_summary,
                        execution_id=execution_id
                    )

                    if summary_success:
                        logger.info("✅ BEAR 시장 분석 요약 SNS 알림 전송 완료")
                    else:
                        logger.warning("⚠️ BEAR 시장 분석 요약 SNS 알림 전송 실패")
                else:
                    logger.debug("📴 분석 SNS 비활성화로 BEAR 시장 분석 요약 알림 스킵")
            else:
                logger.info(f"ℹ️ {current_sentiment} 시장 상황으로 시장 분석 요약 알림 생략 (BEAR 시장에서만 발송)")

        except Exception as e:
            logger.error(f"❌ SNS 알림 전송 중 오류: {e}")

    def _get_btc_trend_classification(self, change_24h: float) -> str:
        """BTC 24시간 변동률을 기반으로 트렌드 분류"""
        if change_24h > 5.0:
            return "BULLISH"
        elif change_24h < -5.0:
            return "BEARISH"
        else:
            return "SIDEWAYS"

    async def run_full_pipeline(self):
        """전체 파이프라인 실행"""
        try:
            self.execution_stats['start_time'] = datetime.now()
            logger.info("🚀 Makenaide 로컬 통합 파이프라인 시작")
            logger.info("="*60)

            # 1. 시스템 초기화
            if not self.initialize_components():
                error_msg = "시스템 초기화 실패 - 파이프라인 중단"
                logger.error(f"❌ {error_msg}")

                # 🔔 Phase 2-3 상세 실패 알림 전송
                if SNS_AVAILABLE and hasattr(self, 'sns_notifier'):
                    try:
                        execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')
                        error_details = f"{error_msg}\n\n세부 오류: {', '.join(self.execution_stats.get('errors', ['초기화 중 알 수 없는 오류']))}"

                        # 초기화 실패 상세 분류
                        errors = self.execution_stats.get('errors', [])
                        if any('memory' in str(err).lower() or 'ram' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.MEMORY_INSUFFICIENT.value
                        elif any('connection' in str(err).lower() or 'network' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.NETWORK_CONNECTION_FAILED.value
                        else:
                            sub_type = FailureSubType.SYSTEM_INITIALIZATION_FAILED.value

                        self.handle_failure_with_phase4(
                            failure_type=FailureType.INIT_FAILURE.value,
                            sub_type=sub_type,
                            error_message=error_details,
                            phase="초기화",
                            execution_id=execution_id,
                            metadata={
                                'component_errors': self.execution_stats.get('errors', []),
                                'available_memory': '확인필요',
                                'instance_type': 't3.medium'
                            },
                            severity="CRITICAL"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ SNS 실패 알림 전송 실패: {e}")

                return False

            # 2. Phase 0: 종목 스캔
            if not self.run_phase_0_scanner():
                error_msg = "Phase 0 실패 - 파이프라인 중단"
                logger.error(f"❌ {error_msg}")

                # 🔔 Phase 2-3 상세 실패 알림 전송
                if SNS_AVAILABLE:
                    try:
                        execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')
                        error_details = f"{error_msg}\n\n업비트 종목 스캔에서 오류가 발생했습니다. API 연결 상태와 네트워크를 확인해주세요."

                        # Phase 0 실패 상세 분류
                        errors = self.execution_stats.get('errors', [])
                        if any('rate limit' in str(err).lower() or 'too many' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.RATE_LIMIT_EXCEEDED.value
                        elif any('timeout' in str(err).lower() or 'connection' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.NETWORK_TIMEOUT.value
                        elif any('auth' in str(err).lower() or 'forbidden' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.API_AUTHENTICATION_FAILED.value
                        else:
                            sub_type = FailureSubType.TICKER_SCAN_FAILED.value

                        self.handle_failure_with_phase4(
                            failure_type=FailureType.PHASE0_FAILURE.value,
                            sub_type=sub_type,
                            error_message=error_details,
                            phase="Phase 0",
                            execution_id=execution_id,
                            metadata={
                                'scanner_errors': self.execution_stats.get('errors', []),
                                'api_calls_made': '확인필요',
                                'api_limit': '600',
                                'affected_endpoints': ['/v1/market/all']
                            },
                            severity="HIGH"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ SNS 실패 알림 전송 실패: {e}")

                return False

            # 3. Phase 1: 데이터 수집
            if not self.run_phase_1_data_collection():
                error_msg = "Phase 1 실패 - 파이프라인 중단"
                logger.error(f"❌ {error_msg}")

                # 🔔 Phase 2-3 상세 실패 알림 전송
                if SNS_AVAILABLE:
                    try:
                        execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')
                        error_details = f"{error_msg}\n\nOHLCV 데이터 수집에서 오류가 발생했습니다. 디스크 공간과 API 호출 한도를 확인해주세요."

                        # Phase 1 실패 상세 분류
                        errors = self.execution_stats.get('errors', [])
                        if any('disk' in str(err).lower() or 'space' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.DISK_SPACE_INSUFFICIENT.value
                        elif any('sqlite' in str(err).lower() or 'database' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.SQLITE_WRITE_FAILED.value
                        elif any('technical' in str(err).lower() or 'indicator' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.TECHNICAL_INDICATOR_FAILED.value
                        elif any('rate limit' in str(err).lower() for err in errors):
                            sub_type = FailureSubType.RATE_LIMIT_EXCEEDED.value
                        else:
                            sub_type = FailureSubType.DATA_COLLECTION_FAILED.value

                        self.handle_failure_with_phase4(
                            failure_type=FailureType.PHASE1_FAILURE.value,
                            sub_type=sub_type,
                            error_message=error_details,
                            phase="Phase 1",
                            execution_id=execution_id,
                            metadata={
                                'data_collection_errors': self.execution_stats.get('errors', []),
                                'database_file': 'makenaide_local.db',
                                'disk_space': '확인필요',
                                'file_permissions': '확인필요'
                            },
                            severity="HIGH"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ SNS 실패 알림 전송 실패: {e}")

                return False

            # 4. Phase 2: 기술적 필터링
            candidates = await self.run_phase_2_technical_filter()

            # 5. Phase 3: GPT 분석 (선택적)
            final_candidates = self.run_phase_3_gpt_analysis(candidates)

            # 6. Kelly 포지션 사이징
            position_sizes = self.run_kelly_calculation(final_candidates)

            # 7. 시장 감정 분석
            _, can_trade, position_adjustment = self.run_market_sentiment_analysis()

            # 📧 SNS 알림: 발굴 종목 리스트 전송
            self._send_discovered_stocks_notification()

            # 8. 거래 실행 (조건 충족 시)
            if can_trade and position_sizes:
                self.execute_trades(position_sizes, position_adjustment)
            elif not can_trade:
                logger.info("🚫 시장 조건으로 인한 거래 중단")
            else:
                logger.info("📭 거래할 종목이 없습니다")

            # 9. 포트폴리오 관리
            self.run_portfolio_management()

            # 10. 실행 결과 보고서
            self.generate_execution_report()

            logger.info("🏁 Makenaide 로컬 통합 파이프라인 완료")
            return True

        except Exception as e:
            error_msg = f"파이프라인 실행 중 치명적 오류: {e}"
            logger.error(f"❌ {error_msg}")
            self.execution_stats['errors'].append(f"치명적 오류: {e}")

            # 🔔 Phase 2-3 상세 실패 알림 전송
            if SNS_AVAILABLE:
                try:
                    execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')
                    error_details = f"{error_msg}\n\n예외 타입: {type(e).__name__}\n스택 트레이스를 로그에서 확인해주세요."

                    # 예외 타입별 상세 분류
                    exception_name = type(e).__name__
                    if 'Memory' in exception_name or 'OutOfMemory' in exception_name:
                        sub_type = FailureSubType.MEMORY_INSUFFICIENT.value
                    elif 'Network' in exception_name or 'Connection' in exception_name or 'Timeout' in exception_name:
                        sub_type = FailureSubType.NETWORK_CONNECTION_FAILED.value
                    elif 'Permission' in exception_name or 'Access' in exception_name:
                        sub_type = FailureSubType.SYSTEM_PERMISSION_DENIED.value
                    else:
                        sub_type = FailureSubType.UNEXPECTED_EXCEPTION.value

                    # Phase 3 보안 알림으로 CRITICAL 메시지 전송
                    critical_notification = NotificationMessage(
                        level=NotificationLevel.CRITICAL,
                        category=NotificationCategory.SYSTEM,
                        title="🚨 치명적 시스템 오류",
                        message=error_details,
                        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        execution_id=execution_id
                    )

                    # 보안 알림 시스템을 통한 응급 처리
                    send_secure_notification(critical_notification)

                    # 상세 실패 분류도 함께 전송 (Phase 4 통합)
                    self.handle_failure_with_phase4(
                        failure_type=FailureType.CRITICAL_ERROR.value,
                        sub_type=sub_type,
                        error_message=error_details,
                        phase="전체 파이프라인",
                        execution_id=execution_id,
                        metadata={
                            'exception_type': exception_name,
                            'critical_error': True,
                            'all_errors': self.execution_stats.get('errors', []),
                            'emergency_response': True
                        },
                        severity="CRITICAL"
                    )
                except Exception as sns_error:
                    logger.error(f"❌ SNS 실패 알림 전송도 실패: {sns_error}")

            self.generate_execution_report()
            self.generate_phase4_daily_report()
            return False

    def _get_technical_analysis_for_kelly(self, ticker: str) -> Optional[Dict]:
        """특정 종목의 기술적 분석 결과를 Kelly Calculator용으로 조회"""
        try:
            with get_db_connection_context() as conn:
                cursor = conn.cursor()

                # 🔧 실제 테이블 구조에 맞게 조회 쿼리 수정
                query = """
                SELECT ticker, quality_score, total_gates_passed, recommendation,
                       stage_confidence, breakout_strength, current_stage, volume_surge
                FROM technical_analysis
                WHERE ticker = ?
                ORDER BY created_at DESC
                LIMIT 1
                """

                cursor.execute(query, (ticker,))
                row = cursor.fetchone()

                if not row:
                    return None

                # 🔧 Kelly Calculator가 기대하는 형태로 변환 - 실제 컬럼에서 매핑
                technical_result = {
                    'ticker': row[0],
                    'quality_score': self._safe_convert_to_float(row[1], 10.0),
                    'stage_2_entry': (self._safe_convert_to_int(row[6], 1) == 2),  # current_stage == 2이면 Stage 2 진입
                    'volume_breakout': (self._safe_convert_to_float(row[7], 0.0) > 1.5),  # volume_surge > 1.5면 volume breakout
                    'ma_trend_strength': self._safe_convert_to_float(row[4], 0.0),  # stage_confidence를 ma_trend_strength로 매핑
                    'volatility_contraction': True,  # 기본값 True (VCP 패턴 가정)
                    'volume_dry_up': (self._safe_convert_to_float(row[7], 0.0) < 0.8),  # volume_surge < 0.8이면 volume dry up
                    'recommendation': row[3] if row[3] else 'HOLD',
                    'confidence': self._safe_convert_to_float(row[4], 0.0),  # stage_confidence
                    'breakout_strength': self._safe_convert_to_float(row[5], 0.0),
                    'technical_bonus': max(0.0, self._safe_convert_to_float(row[1], 10.0) - 10.0),  # quality_score - 10을 bonus로 사용
                }

                return technical_result

        except Exception as e:
            logger.error(f"❌ {ticker} 기술적 분석 조회 실패: {e}")
            return None

    def _get_gpt_analysis_for_kelly(self, ticker: str) -> Optional[Dict]:
        """특정 종목의 GPT 분석 결과를 Kelly Calculator용으로 조회"""
        try:
            with get_db_connection_context() as conn:
                cursor = conn.cursor()

                # 가장 최신 GPT 분석 결과 조회
                today = datetime.now().strftime('%Y-%m-%d')
                query = """
                SELECT gpt_recommendation, gpt_confidence
                FROM gpt_analysis
                WHERE ticker = ? AND analysis_date = ?
                ORDER BY created_at DESC
                LIMIT 1
                """

                cursor.execute(query, (ticker, today))
                row = cursor.fetchone()

                if not row:
                    return None

                # Kelly Calculator가 기대하는 형태로 변환 - 안전한 타입 변환 적용
                gpt_result = {
                    'recommendation': row[0],
                    'confidence': self._safe_convert_to_float(row[1], 0.0)
                }

                return gpt_result

        except Exception as e:
            logger.warning(f"⚠️ {ticker} GPT 분석 조회 실패: {e}")
            return None

    def _safe_convert_to_int(self, value, default: int = 0) -> int:
        """SQLite에서 조회된 값을 안전하게 정수로 변환"""
        if value is None:
            return default

        # 이미 정수인 경우
        if isinstance(value, int):
            return value

        # 문자열인 경우
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                logger.warning(f"⚠️ 문자열을 정수로 변환 실패: '{value}' → 기본값 {default} 사용")
                return default

        # 바이너리 데이터인 경우
        if isinstance(value, bytes):
            try:
                if len(value) == 8:
                    # 8바이트 바이너리를 64비트 리틀 엔디안 정수로 해석
                    return struct.unpack('<Q', value)[0]
                elif len(value) == 4:
                    # 4바이트 바이너리를 32비트 리틀 엔디안 정수로 해석
                    return struct.unpack('<I', value)[0]
                elif len(value) == 1:
                    # 1바이트 바이너리를 정수로 해석
                    return struct.unpack('B', value)[0]
                else:
                    # 다른 길이의 바이너리는 첫 바이트만 사용
                    return value[0] if len(value) > 0 else default
            except (struct.error, IndexError) as e:
                logger.warning(f"⚠️ 바이너리 데이터를 정수로 변환 실패: {value.hex()} → 기본값 {default} 사용 ({e})")
                return default

        # 부동소수점인 경우
        if isinstance(value, float):
            return int(value)

        # 기타 타입인 경우
        logger.warning(f"⚠️ 알 수 없는 타입을 정수로 변환: {type(value)} {value} → 기본값 {default} 사용")
        return default

    def _safe_convert_to_float(self, value, default: float = 0.0) -> float:
        """SQLite에서 조회된 값을 안전하게 부동소수점으로 변환"""
        if value is None:
            return default

        # 이미 부동소수점인 경우
        if isinstance(value, float):
            return value

        # 정수인 경우
        if isinstance(value, int):
            return float(value)

        # 문자열인 경우
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                logger.warning(f"⚠️ 문자열을 부동소수점으로 변환 실패: '{value}' → 기본값 {default} 사용")
                return default

        # 바이너리 데이터인 경우 (일단 정수로 변환 후 부동소수점으로)
        if isinstance(value, bytes):
            try:
                int_value = self._safe_convert_to_int(value, 0)
                return float(int_value)
            except Exception as e:
                logger.warning(f"⚠️ 바이너리 데이터를 부동소수점으로 변환 실패: {value.hex()} → 기본값 {default} 사용 ({e})")
                return default

        # 기타 타입인 경우
        logger.warning(f"⚠️ 알 수 없는 타입을 부동소수점으로 변환: {type(value)} {value} → 기본값 {default} 사용")
        return default

    def _save_technical_analysis_to_db(self, result) -> bool:
        """
        기술적 분석 결과를 technical_analysis 테이블에 저장

        Args:
            result: UnifiedFilterResult 객체 (technical_filter.py의 analyze_ticker() 반환값)

        Returns:
            bool: 저장 성공 여부
        """
        try:
            with get_db_connection_context() as conn:
                cursor = conn.cursor()

                # UnifiedFilterResult에서 필요한 데이터 추출
                ticker = result.ticker
                analysis_date = result.analysis_date

                # Weinstein Stage 분석 결과
                weinstein = result.weinstein_result
                current_stage = weinstein.current_stage if weinstein else None
                stage_confidence = weinstein.stage_confidence if weinstein else None
                ma200_trend = weinstein.ma200_trend if weinstein else None
                price_vs_ma200 = weinstein.price_vs_ma200 if weinstein else None
                breakout_strength = weinstein.breakout_strength if weinstein else None

                # 4-Gate 필터링 결과
                basic = result.basic_result
                total_gates_passed = basic.total_gates_passed if basic else None
                quality_score = result.final_quality_score

                # 최종 권고 및 신뢰도
                recommendation = result.final_recommendation.value
                final_confidence = result.final_confidence

                # volume_surge 계산 (basic_result에서 추출)
                volume_surge = None
                if basic and hasattr(basic, 'volume_surge_ratio'):
                    volume_surge = basic.volume_surge_ratio
                elif weinstein and hasattr(weinstein, 'volume_surge'):
                    volume_surge = weinstein.volume_surge

                # INSERT OR REPLACE로 중복 방지
                cursor.execute("""
                    INSERT OR REPLACE INTO technical_analysis (
                        ticker, analysis_date,
                        current_stage, stage_confidence, ma200_trend, price_vs_ma200, breakout_strength,
                        total_gates_passed, quality_score, recommendation,
                        volume_surge,
                        source_table, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                """, (
                    ticker, analysis_date,
                    current_stage, stage_confidence, ma200_trend, price_vs_ma200, breakout_strength,
                    total_gates_passed, quality_score, recommendation,
                    volume_surge,
                    'UnifiedTechnicalFilter'  # 데이터 출처 표시
                ))

                conn.commit()
                logger.debug(f"✅ {ticker} 기술적 분석 결과 DB 저장 완료")
                return True

        except Exception as e:
            logger.warning(f"⚠️ {result.ticker if hasattr(result, 'ticker') else 'Unknown'} DB 저장 실패 (파이프라인 계속 진행): {e}")
            return False

async def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='Makenaide 로컬 통합 거래 시스템')
    parser.add_argument('--dry-run', action='store_true', help='실제 거래 없이 테스트 실행')
    parser.add_argument('--no-gpt', action='store_true', help='GPT 분석 비활성화 (비용 절약)')
    parser.add_argument('--risk-level', choices=['conservative', 'moderate', 'aggressive'],
                       default='moderate', help='리스크 레벨 설정')
    parser.add_argument('--max-gpt-budget', type=float, default=5.0,
                       help='일일 GPT 비용 한도 (USD)')
    parser.add_argument('--auto-sync', action='store_true', default=True,
                       help='포트폴리오 자동 동기화 활성화 (기본값)')
    parser.add_argument('--no-auto-sync', action='store_true',
                       help='포트폴리오 자동 동기화 비활성화')
    parser.add_argument('--sync-policy', choices=['conservative', 'moderate', 'aggressive'],
                       default='aggressive', help='포트폴리오 동기화 정책 (기본: aggressive - 모든 금액 동기화)')

    args = parser.parse_args()

    # auto-sync 설정 조정
    if args.no_auto_sync:
        args.auto_sync = False

    # 설정 생성
    risk_level_map = {
        'conservative': RiskLevel.CONSERVATIVE,
        'moderate': RiskLevel.MODERATE,
        'aggressive': RiskLevel.AGGRESSIVE
    }

    config = OrchestratorConfig(
        enable_gpt_analysis=not args.no_gpt,
        max_gpt_budget_daily=args.max_gpt_budget,
        risk_level=risk_level_map[args.risk_level],
        dry_run=args.dry_run,
        auto_sync_enabled=args.auto_sync,
        sync_policy=args.sync_policy
    )

    # 실행 모드 출력
    logger.info("🎯 실행 모드 설정")
    logger.info(f"   - GPT 분석: {'활성화' if config.enable_gpt_analysis else '비활성화'}")
    logger.info(f"   - 거래 실행: {'DRY RUN' if config.dry_run else '실제 거래'}")
    logger.info(f"   - 리스크 레벨: {config.risk_level.value}")
    logger.info(f"   - GPT 일일 예산: ${config.max_gpt_budget_daily}")
    logger.info(f"   - 포트폴리오 자동 동기화: {'활성화' if config.auto_sync_enabled else '비활성화'}")
    logger.info(f"   - 동기화 정책: {config.sync_policy} ({'모든 금액 동기화' if config.sync_policy == 'aggressive' else '제한적 동기화'})")

    # 오케스트레이터 실행
    orchestrator = MakenaideLocalOrchestrator(config)
    success = await orchestrator.run_full_pipeline()

    # EC2 자동 종료 처리 (환경 변수로 제어)
    auto_shutdown = os.getenv('EC2_AUTO_SHUTDOWN', 'false').lower() == 'true'

    if success:
        logger.info("🎉 파이프라인 성공적으로 완료")

        if auto_shutdown:
            logger.info("🔌 EC2 Smart Shutdown 시작")
            # smart_shutdown_ec2 메서드 사용 (AWS CLI 기반 개선된 종료)
            orchestrator.smart_shutdown_ec2("파이프라인 성공 완료")
        else:
            # 일반 정리만 수행
            orchestrator.cleanup()

        sys.exit(0)
    else:
        logger.error("💥 파이프라인 실행 실패")

        if auto_shutdown:
            logger.error("🔌 EC2 Smart Shutdown 시작 (실패 케이스)")
            # 실패 시에도 EC2 종료 (비용 절약)
            orchestrator.smart_shutdown_ec2("파이프라인 실패")
        else:
            # 일반 정리만 수행
            orchestrator.cleanup()

        sys.exit(1)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())