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
Phase 2: hybrid_technical_filter.py (Weinstein Stage 2 분석)
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
from utils import logger, setup_logger, setup_restricted_logger, load_blacklist
from db_manager_sqlite import get_db_connection_context
from scanner import update_tickers
from data_collector import SimpleDataCollector
from hybrid_technical_filter import HybridTechnicalFilter
from gpt_analyzer import GPTPatternAnalyzer
from kelly_calculator import KellyCalculator, RiskLevel
from market_sentiment import IntegratedMarketSentimentAnalyzer, MarketSentiment
# from trade_executor import buy_asset, sell_asset  # 삭제된 레거시 모듈
# from portfolio_manager import PortfolioManager  # trading_engine으로 통합됨
from trading_engine import LocalTradingEngine, TradingConfig, OrderStatus, TradeResult

# SNS 알림 시스템 import
try:
    from sns_notification_system import (
        MakenaideSNSNotifier,
        notify_discovered_stocks,
        notify_kelly_position_sizing,
        notify_market_analysis_summary
    )
    SNS_AVAILABLE = True
    logger.info("✅ SNS 알림 시스템 로드 완료")
except ImportError as e:
    logger.warning(f"⚠️ SNS 알림 시스템을 사용할 수 없습니다: {e}")
    SNS_AVAILABLE = False

# 환경 변수 로드
load_dotenv()

# 로거 설정
logger = setup_restricted_logger('makenaide_orchestrator')

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
        self.sns_notifier = None  # SNS 알림 시스템

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
                logger.error("❌ 업비트 API 키가 설정되지 않았습니다")
                return False

            self.upbit = pyupbit.Upbit(access_key, secret_key)
            logger.info("✅ 업비트 API 연결 완료")

            # 데이터 수집기 초기화
            self.data_collector = SimpleDataCollector(db_path=self.db_path)
            logger.info("✅ 데이터 수집기 초기화 완료")

            # 기술적 필터 초기화
            self.technical_filter = HybridTechnicalFilter(db_path=self.db_path)
            logger.info("✅ 기술적 필터 초기화 완료")

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

            # 시장 감정 분석기 초기화
            self.market_sentiment = IntegratedMarketSentimentAnalyzer()
            logger.info("✅ 시장 감정 분석기 초기화 완료")

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

            logger.info("🎉 모든 시스템 컴포넌트 초기화 성공")
            return True

        except Exception as e:
            logger.error(f"❌ 컴포넌트 초기화 실패: {e}")
            self.execution_stats['errors'].append(f"초기화 실패: {e}")
            return False

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
        """Phase 1: 증분 OHLCV 데이터 수집"""
        try:
            logger.info("📊 Phase 1: 증분 OHLCV 데이터 수집 시작")

            # 활성 티커 목록 조회
            with get_db_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT ticker FROM tickers
                    WHERE is_active = 1 OR is_active IS NULL
                    ORDER BY ticker
                """)
                active_tickers = [row[0] for row in cursor.fetchall()]

            if not active_tickers:
                logger.warning("⚠️ 활성 티커가 없습니다")
                return False

            logger.info(f"📋 활성 티커 {len(active_tickers)}개 대상 데이터 수집")

            # 배치별로 데이터 수집 (메모리 효율성)
            batch_size = 10
            for i in range(0, len(active_tickers), batch_size):
                batch_tickers = active_tickers[i:i+batch_size]
                logger.info(f"🔄 배치 {i//batch_size + 1}: {len(batch_tickers)}개 티커 처리")

                for ticker in batch_tickers:
                    try:
                        # 갭 분석 및 증분 수집
                        self.data_collector.collect_ticker_data(ticker)
                        time.sleep(0.1)  # API 레이트 리미트 고려
                    except Exception as e:
                        logger.warning(f"⚠️ {ticker} 데이터 수집 실패: {e}")
                        continue

            self.execution_stats['phases_completed'].append('Phase 1: Data Collection')
            logger.info("✅ Phase 1 완료: 증분 OHLCV 데이터 수집")
            return True

        except Exception as e:
            logger.error(f"❌ Phase 1 실패: {e}")
            self.execution_stats['errors'].append(f"Phase 1 실패: {e}")
            return False

    def run_phase_2_technical_filter(self) -> List[str]:
        """Phase 2: Weinstein Stage 2 기술적 필터링"""
        try:
            logger.info("🎯 Phase 2: Weinstein Stage 2 기술적 필터링 시작")

            # 기술적 필터 실행 (올바른 메서드 사용)
            analysis_results = self.technical_filter.run_full_analysis()

            # Stage 2 후보 추출
            stage2_candidates = analysis_results.get('stage2_candidates', [])

            if not stage2_candidates:
                logger.info("📭 Stage 2 진입 종목이 없습니다")
                return []

            # 품질 점수 임계값 필터링
            filtered_candidates = []
            technical_candidates_data = []  # SNS 알림용 상세 데이터

            for candidate in stage2_candidates:
                ticker = candidate['ticker']
                quality_score = candidate['quality_score']
                gates_passed = candidate['gates_passed']
                recommendation = candidate['recommendation']

                # SNS 알림용 데이터 저장 (모든 후보)
                technical_candidates_data.append({
                    'ticker': ticker,
                    'quality_score': quality_score,
                    'gates_passed': gates_passed,
                    'recommendation': recommendation,
                    'pattern_type': candidate.get('pattern_type', 'Stage 2'),
                    'price': candidate.get('current_price', 0),
                    'volume_ratio': candidate.get('volume_ratio', 0)
                })

                if quality_score >= self.config.min_quality_score:
                    filtered_candidates.append(ticker)
                    logger.info(f"✅ {ticker}: 품질 점수 {quality_score:.1f}, 게이트 {gates_passed}/4 통과, 권고: {recommendation}")
                else:
                    logger.info(f"⏭️ {ticker}: 품질 점수 {quality_score:.1f} (임계값 {self.config.min_quality_score} 미달)")

            # 기술적 분석 결과를 통계에 저장
            self.execution_stats['technical_candidates'] = technical_candidates_data
            self.execution_stats['phases_completed'].append('Phase 2: Technical Filter')
            self.execution_stats['trading_candidates'] = len(filtered_candidates)

            logger.info(f"✅ Phase 2 완료: {len(filtered_candidates)}개 거래 후보 발견 (총 {len(technical_candidates_data)}개 종목 분석)")
            return filtered_candidates

        except Exception as e:
            logger.error(f"❌ Phase 2 실패: {e}")
            self.execution_stats['errors'].append(f"Phase 2 실패: {e}")
            return []

    def run_phase_3_gpt_analysis(self, candidates: List[str]) -> List[str]:
        """Phase 3: GPT 패턴 분석 (선택적)"""
        if not self.config.enable_gpt_analysis or not candidates:
            logger.info("⏭️ Phase 3: GPT 분석 비활성화 또는 후보 없음")
            return candidates

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
        """시장 감정 분석 및 거래 가능 여부 판정"""
        try:
            logger.info("🌡️ 시장 감정 분석 시작")

            # 통합 시장 감정 분석
            sentiment_result = self.market_sentiment.analyze_comprehensive_market_sentiment()

            if not sentiment_result:
                logger.warning("⚠️ 시장 감정 분석 실패")
                return MarketSentiment.NEUTRAL, True, 1.0  # 기본값

            sentiment = sentiment_result.final_sentiment
            can_trade = sentiment_result.trading_allowed
            position_adjustment = sentiment_result.position_adjustment

            logger.info(f"📊 시장 감정: {sentiment.value}")
            logger.info(f"🚦 거래 가능: {'예' if can_trade else '아니오'}")
            logger.info(f"⚖️ 포지션 조정: {position_adjustment:.2f}x")

            # BEAR 시장에서는 거래 중단
            if sentiment == MarketSentiment.BEAR:
                logger.warning("🚫 BEAR 시장 감지 - 모든 거래 중단")
                return sentiment, False, 0.0

            return sentiment, can_trade, position_adjustment

        except Exception as e:
            logger.error(f"❌ 시장 감정 분석 실패: {e}")
            self.execution_stats['errors'].append(f"시장 감정 분석 실패: {e}")
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
                    result = buy_asset(self.upbit, ticker, investment_amount)

                    if result:
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
        """포트폴리오 관리 및 매도 조건 검사"""
        try:
            logger.info("📊 포트폴리오 관리 시작")

            # 현재 보유 종목 조회
            balances = self.upbit.get_balances()

            if not balances:
                logger.info("📭 보유 종목이 없습니다")
                return

            for balance in balances:
                ticker = f"KRW-{balance['currency']}"

                # KRW는 제외
                if balance['currency'] == 'KRW':
                    continue

                balance_amount = float(balance['balance'])
                avg_buy_price = float(balance['avg_buy_price'])

                if balance_amount == 0:
                    continue

                try:
                    # 현재가 조회
                    current_price = pyupbit.get_current_price(ticker)

                    if not current_price:
                        continue

                    # 손익률 계산
                    pnl_ratio = (current_price - avg_buy_price) / avg_buy_price

                    logger.info(f"📈 {ticker}: 손익률 {pnl_ratio*100:.1f}%")

                    # 매도 조건 검사
                    should_sell = False
                    sell_reason = ""

                    # 7-8% 손절 조건
                    if pnl_ratio <= -0.08:
                        should_sell = True
                        sell_reason = "손절 조건"

                    # 20-25% 수익 실현 조건
                    elif pnl_ratio >= 0.25:
                        should_sell = True
                        sell_reason = "수익 실현 조건"

                    if should_sell and not self.config.dry_run:
                        logger.info(f"💹 {ticker}: {sell_reason}으로 매도 실행")

                        # 매도 실행
                        sell_result = sell_asset(self.upbit, ticker, balance_amount)

                        if sell_result:
                            logger.info(f"✅ {ticker}: 매도 성공")
                        else:
                            logger.warning(f"❌ {ticker}: 매도 실패")

                    elif should_sell and self.config.dry_run:
                        logger.info(f"🧪 {ticker}: {sell_reason} (DRY RUN)")

                except Exception as e:
                    logger.warning(f"⚠️ {ticker} 포트폴리오 관리 실패: {e}")
                    continue

            logger.info("✅ 포트폴리오 관리 완료")

        except Exception as e:
            logger.error(f"❌ 포트폴리오 관리 실패: {e}")
            self.execution_stats['errors'].append(f"포트폴리오 관리 실패: {e}")

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

    def run_analysis_pipeline(self) -> Dict:
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
            candidates = self.run_phase_2_technical_filter()

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
        """시스템 정리 및 종료"""
        try:
            logger.info("🧹 시스템 정리 시작")

            # Trading Engine 정리
            if self.trading_engine and hasattr(self.trading_engine, 'cleanup'):
                self.trading_engine.cleanup()

            # 기타 컴포넌트 정리
            # TODO: 각 컴포넌트별 cleanup 메서드 호출

            logger.info("✅ 시스템 정리 완료")

        except Exception as e:
            logger.error(f"❌ 시스템 정리 실패: {e}")

    def _get_latest_technical_analysis(self) -> List[Dict]:
        """실제 DB에서 최신 기술적 분석 결과 조회 (시장 기회 실시간 파악)"""
        try:
            with get_db_connection_context(self.db_path) as conn:
                # 오늘 날짜의 최신 기술적 분석 결과 조회 (추천 등급 높은 순)
                today = datetime.now().strftime('%Y-%m-%d')

                query = """
                SELECT ticker, quality_score, total_gates_passed, recommendation,
                       current_stage, ma200_trend, volume_surge, breakout_strength
                FROM technical_analysis
                WHERE analysis_date = ?
                  AND quality_score >= ?
                  AND recommendation IN ('STRONG_BUY', 'BUY')
                ORDER BY quality_score DESC, total_gates_passed DESC
                LIMIT 15
                """

                cursor = conn.execute(query, (today, self.config.min_quality_score))
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
                        'gates_passed': row[2],
                        'recommendation': row[3],
                        'pattern_type': f"Stage {row[4]}" if row[4] else 'Stage 2',
                        'price': current_price,  # 🔥 실시간 가격 정보
                        'volume_ratio': row[6] if row[6] else 0,
                        'ma200_trend': row[5],
                        'breakout_strength': row[7] if row[7] else 0
                    })

                logger.info(f"🎯 실제 DB에서 {len(candidates)}개 기술적 분석 종목 조회")
                return candidates

        except Exception as e:
            logger.error(f"❌ 기술적 분석 DB 조회 실패: {e}")
            return []

    def _get_latest_gpt_analysis(self) -> List[Dict]:
        """실제 DB에서 최신 GPT 분석 결과 조회 (AI 승인 종목)"""
        try:
            with get_db_connection_context(self.db_path) as conn:
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
            with get_db_connection_context(self.db_path) as conn:
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

            # 📊 시장 분석 종합 요약 알림 (실제 통계)
            market_summary = {
                'technical_count': len(technical_candidates),
                'gpt_count': len(gpt_candidates),
                'kelly_count': len(kelly_results),
                'total_cost': self.execution_stats.get('total_cost', 0.0),
                'phases_completed': len(self.execution_stats.get('phases_completed', [])),
                'errors_count': len(self.execution_stats.get('errors', []))
            }

            summary_success = self.sns_notifier.notify_market_analysis_summary(
                market_summary=market_summary,
                execution_id=execution_id
            )

            if summary_success:
                logger.info("✅ 시장 분석 요약 SNS 알림 전송 완료")
            else:
                logger.warning("⚠️ 시장 분석 요약 SNS 알림 전송 실패")

        except Exception as e:
            logger.error(f"❌ SNS 알림 전송 중 오류: {e}")

    def run_full_pipeline(self):
        """전체 파이프라인 실행"""
        try:
            self.execution_stats['start_time'] = datetime.now()
            logger.info("🚀 Makenaide 로컬 통합 파이프라인 시작")
            logger.info("="*60)

            # 1. 시스템 초기화
            if not self.initialize_components():
                logger.error("❌ 시스템 초기화 실패 - 파이프라인 중단")
                return False

            # 2. Phase 0: 종목 스캔
            if not self.run_phase_0_scanner():
                logger.error("❌ Phase 0 실패 - 파이프라인 중단")
                return False

            # 3. Phase 1: 데이터 수집
            if not self.run_phase_1_data_collection():
                logger.error("❌ Phase 1 실패 - 파이프라인 중단")
                return False

            # 4. Phase 2: 기술적 필터링
            candidates = self.run_phase_2_technical_filter()

            # 5. Phase 3: GPT 분석 (선택적)
            final_candidates = self.run_phase_3_gpt_analysis(candidates)

            # 6. Kelly 포지션 사이징
            position_sizes = self.run_kelly_calculation(final_candidates)

            # 7. 시장 감정 분석
            market_sentiment, can_trade, position_adjustment = self.run_market_sentiment_analysis()

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
            logger.error(f"❌ 파이프라인 실행 중 치명적 오류: {e}")
            self.execution_stats['errors'].append(f"치명적 오류: {e}")
            self.generate_execution_report()
            return False

def main():
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
    success = orchestrator.run_full_pipeline()

    # EC2 자동 종료 처리 (환경 변수로 제어)
    auto_shutdown = os.getenv('EC2_AUTO_SHUTDOWN', 'false').lower() == 'true'

    if success:
        logger.info("🎉 파이프라인 성공적으로 완료")

        if auto_shutdown:
            logger.info("🔌 EC2 자동 종료 시작 (30초 후)")
            orchestrator.cleanup()  # 시스템 정리

            # 30초 후 안전한 종료 (로그 기록 시간 확보)
            import subprocess
            subprocess.run(['sudo', 'shutdown', '-h', '+1'], check=False)
            logger.info("✅ EC2 종료 명령 실행됨")

        sys.exit(0)
    else:
        logger.error("💥 파이프라인 실행 실패")

        if auto_shutdown:
            logger.error("🔌 EC2 자동 종료 시작 (실패 케이스, 30초 후)")
            orchestrator.cleanup()  # 시스템 정리

            # 실패 시에도 EC2 종료 (비용 절약)
            import subprocess
            subprocess.run(['sudo', 'shutdown', '-h', '+1'], check=False)
            logger.error("❌ EC2 종료 명령 실행됨 (실패 케이스)")

        sys.exit(1)

if __name__ == "__main__":
    main()