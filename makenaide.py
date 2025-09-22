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
from integrated_scoring_system import IntegratedScoringSystem
from gpt_analyzer import GPTPatternAnalyzer
from kelly_calculator import KellyCalculator, RiskLevel
from market_sentiment import IntegratedMarketSentimentAnalyzer, MarketSentiment
from real_time_market_sentiment import RealTimeMarketSentiment
# from trade_executor import buy_asset, sell_asset  # 삭제된 레거시 모듈
# from portfolio_manager import PortfolioManager  # trading_engine으로 통합됨
from trading_engine import LocalTradingEngine, TradingConfig, OrderStatus, TradeResult

# 환경 변수 로드
load_dotenv()

# 로거 설정 (모든 import 전에 먼저 설정)
logger = setup_restricted_logger('makenaide_orchestrator')

# SNS 알림 시스템 import (Phase 1-3)
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
    logger.info("✅ SNS 알림 시스템 로드 완료 (Phase 1-3 기능 포함)")
except ImportError as e:
    logger.warning(f"⚠️ SNS 알림 시스템을 사용할 수 없습니다: {e}")
    SNS_AVAILABLE = False

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
    min_quality_score: float = 12.0  # 최소 품질 점수
    risk_level: RiskLevel = RiskLevel.MODERATE  # 리스크 레벨
    dry_run: bool = False  # 실제 거래 실행 여부
    max_positions: int = 8  # 최대 동시 보유 종목 수
    portfolio_allocation_limit: float = 0.25  # 전체 포트폴리오 대비 최대 할당 비율

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

            # 기술적 필터 초기화 (새로운 점수제 시스템)
            self.technical_filter = IntegratedScoringSystem(db_path=self.db_path)
            logger.info("✅ 기술적 필터 초기화 완료 (LayeredScoringEngine 점수제 시스템)")

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

            # SNS 알림 시스템 초기화
            if SNS_AVAILABLE:
                try:
                    self.sns_notifier = MakenaideSNSNotifier()
                    logger.info("✅ SNS 알림 시스템 초기화 완료")
                except Exception as e:
                    logger.warning(f"⚠️ SNS 알림 시스템 초기화 실패: {e}")
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
            successful_collections = results.get('summary', {}).get('success', 0)
            if successful_collections == 0:
                logger.error("❌ 데이터 수집 실패: 성공한 수집이 없음")
                self.execution_stats['errors'].append("Phase 1 실패: 성공한 데이터 수집 없음")
                return False

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
        """Phase 2: LayeredScoringEngine 점수제 기술적 필터링"""
        try:
            logger.info("🎯 Phase 2: LayeredScoringEngine 점수제 분석 시작")

            # 점수제 필터 실행 (새로운 시스템)
            analysis_results = await self.technical_filter.run_full_analysis()

            if not analysis_results:
                logger.info("📭 분석 가능한 종목이 없습니다")
                return []

            # 품질 점수 임계값 필터링
            filtered_candidates = []
            technical_candidates_data = []  # SNS 알림용 상세 데이터

            for result in analysis_results:
                ticker = result.ticker
                total_score = result.total_score
                quality_gates_passed = result.quality_gates_passed
                recommendation = result.recommendation

                # SNS 알림용 데이터 저장 (모든 후보)
                technical_candidates_data.append({
                    'ticker': ticker,
                    'quality_score': total_score,  # 총점을 품질 점수로 사용
                    'gates_passed': '통과' if quality_gates_passed else '미통과',
                    'recommendation': recommendation,
                    'pattern_type': f'Stage {result.stage}',
                    'price': 0,  # 필요시 추가 구현
                    'volume_ratio': 0,  # 필요시 추가 구현
                    'macro_score': result.macro_score,
                    'structural_score': result.structural_score,
                    'micro_score': result.micro_score,
                    'confidence': result.confidence
                })

                # 매수 추천이고 Quality Gate를 통과한 종목만 필터링
                if recommendation == "BUY" and quality_gates_passed and total_score >= self.config.min_quality_score:
                    filtered_candidates.append(ticker)
                    logger.info(f"✅ {ticker}: 총점 {total_score:.1f}, Quality Gate 통과, 권고: {recommendation}")
                    logger.info(f"   └ Macro: {result.macro_score:.1f}, Structural: {result.structural_score:.1f}, Micro: {result.micro_score:.1f}")
                else:
                    if total_score < self.config.min_quality_score:
                        logger.info(f"⏭️ {ticker}: 총점 {total_score:.1f} (임계값 {self.config.min_quality_score} 미달)")
                    elif not quality_gates_passed:
                        logger.info(f"⏭️ {ticker}: Quality Gate 미통과 (총점 {total_score:.1f})")
                    else:
                        logger.info(f"⏭️ {ticker}: {recommendation} 권고 (총점 {total_score:.1f})")

            # 기술적 분석 결과를 통계에 저장
            self.execution_stats['technical_candidates'] = technical_candidates_data
            self.execution_stats['phases_completed'].append('Phase 2: LayeredScoringEngine Filter')
            self.execution_stats['trading_candidates'] = len(filtered_candidates)

            logger.info(f"✅ Phase 2 완료: {len(filtered_candidates)}개 거래 후보 발견 (총 {len(technical_candidates_data)}개 종목 분석)")
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
                            'recommendation': result.get('recommendation', 'Unknown'),
                            'confidence': result.get('confidence', 0.0),
                            'pattern': result.get('pattern', ''),
                            'reasoning': result.get('reasoning', ''),
                            'risk_level': result.get('risk_level', 'Unknown'),
                            'cost': 0.05  # 추정 비용
                        })

                        if result.get('recommendation') == 'BUY':
                            confidence = result.get('confidence', 0.0)
                            logger.info(f"✅ {ticker}: GPT 매수 추천 (신뢰도: {confidence:.1f}%)")
                            gpt_approved_candidates.append(ticker)
                        else:
                            recommendation = result.get('recommendation', 'Unknown')
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

                    # 비용 추정 (GPT-5-mini 기준)
                    estimated_cost = 0.05  # 대략적인 비용 추정
                    total_cost += estimated_cost

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
            return {}

        try:
            logger.info(f"🧮 Kelly 포지션 사이징 계산 ({len(candidates)}개 후보)")

            position_sizes = {}

            for ticker in candidates:
                try:
                    # 패턴 분석 및 포지션 사이징
                    position_size = self.kelly_calculator.calculate_position_size(ticker)

                    if position_size > 0:
                        position_sizes[ticker] = position_size
                        logger.info(f"📊 {ticker}: Kelly 포지션 {position_size:.1f}%")
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
            total_balance = self.trading_engine.get_total_balance()

            for ticker, base_position in position_sizes.items():
                try:
                    # 시장 감정 기반 포지션 조정
                    adjusted_position = base_position * position_adjustment

                    # 최대/최소 포지션 제한
                    adjusted_position = max(1.0, min(adjusted_position, 8.0))

                    # 실제 투자 금액 계산
                    investment_amount = total_balance * (adjusted_position / 100)

                    # 최소 주문 금액 확인
                    if investment_amount < 10000:  # 1만원 최소
                        logger.info(f"⏭️ {ticker}: 투자 금액 부족 ({investment_amount:,.0f}원)")
                        continue

                    logger.info(f"💰 {ticker}: {adjusted_position:.1f}% ({investment_amount:,.0f}원) 매수 시도")

                    # 매수 실행
                    result = self.trading_engine.execute_buy_order(ticker, investment_amount)

                    if result and result.status == OrderStatus.SUCCESS:
                        trades_executed += 1
                        logger.info(f"✅ {ticker}: 매수 성공")
                    else:
                        logger.warning(f"❌ {ticker}: 매수 실패")

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
        """실제 DB에서 최신 기술적 분석 결과 조회 (시장 기회 실시간 파악)"""
        try:
            with get_db_connection_context() as conn:
                # 🆕 오늘 날짜의 최신 기술적 분석 결과만 조회 (과거 데이터 반복 발송 방지)
                # GPT Analysis와 동일한 날짜 필터링 로직 적용

                # 통합 technical_analysis 테이블에서 LayeredScoring 데이터 조회
                query = """
                SELECT ticker, quality_score, quality_gates_passed, recommendation,
                       current_stage as stage, stage_confidence as confidence,
                       macro_score, structural_score
                FROM technical_analysis
                WHERE quality_score >= ?
                  AND recommendation IN ('STRONG_BUY', 'BUY', 'WATCH')
                  AND DATE(updated_at) = DATE('now', '+9 hours')
                  AND total_score IS NOT NULL
                ORDER BY quality_score DESC, stage_confidence DESC
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

                    candidates.append({
                        'ticker': ticker,
                        'quality_score': row[1],
                        'gates_passed': row[2],  # quality_gates_passed (Boolean)
                        'recommendation': row[3],
                        'pattern_type': f"Stage {row[4]}" if row[4] else 'Stage 2',  # stage
                        'price': current_price,  # 🔥 실시간 가격 정보
                        'confidence': row[5],  # confidence
                        'macro_score': row[6],  # macro_score
                        'structural_score': row[7]  # structural_score
                    })

                logger.info(f"🎯 실제 DB에서 {len(candidates)}개 기술적 분석 종목 조회")
                return candidates

        except Exception as e:
            logger.error(f"❌ 기술적 분석 DB 조회 실패: {e}")
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
                # 발굴 종목 알림 전송
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
                logger.info("📭 발굴된 종목이 없어 SNS 알림을 생략합니다")

            # 💰 Kelly 포지션 사이징 결과 알림 (별도)
            if kelly_results:
                kelly_success = self.sns_notifier.notify_kelly_position_sizing(
                    kelly_results=kelly_results,
                    execution_id=execution_id
                )

                if kelly_success:
                    logger.info(f"✅ Kelly 포지션 사이징 SNS 알림 전송 완료 ({len(kelly_results)}개 종목)")
                else:
                    logger.warning("⚠️ Kelly 포지션 사이징 SNS 알림 전송 실패")

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
                summary_success = self.sns_notifier.notify_market_analysis_summary(
                    market_data=market_summary,
                    execution_id=execution_id
                )

                if summary_success:
                    logger.info("✅ BEAR 시장 분석 요약 SNS 알림 전송 완료")
                else:
                    logger.warning("⚠️ BEAR 시장 분석 요약 SNS 알림 전송 실패")
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

    args = parser.parse_args()

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
        dry_run=args.dry_run
    )

    # 실행 모드 출력
    logger.info("🎯 실행 모드 설정")
    logger.info(f"   - GPT 분석: {'활성화' if config.enable_gpt_analysis else '비활성화'}")
    logger.info(f"   - 거래 실행: {'DRY RUN' if config.dry_run else '실제 거래'}")
    logger.info(f"   - 리스크 레벨: {config.risk_level.value}")
    logger.info(f"   - GPT 일일 예산: ${config.max_gpt_budget_daily}")

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