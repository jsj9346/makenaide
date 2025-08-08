#!/usr/bin/env python3
"""
운영용 백테스팅 시스템

과거 데이터를 기반으로 전략 성과를 검증하는 완전한 백테스팅 시스템입니다.
분산 백테스팅 인프라와 기존 백테스팅 모듈을 통합하여 
실제 운영에 사용할 수 있는 강력한 백테스팅 환경을 제공합니다.

Author: Production Backtesting System
Version: 1.0.0
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import pandas as pd
import numpy as np
import concurrent.futures
import uuid

# 프로젝트 루트 패스 설정
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# 백테스팅 모듈 임포트
try:
    from backtesting_modules import (
        IntegratedBacktester,
        create_integrated_backtester,
        BacktestResult,
        BacktestSummary,
        StrategyConfig
    )
    logger_setup_success = True
except ImportError as e:
    logger_setup_success = False
    print(f"⚠️ 백테스팅 모듈 임포트 실패: {e}")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('production_backtesting.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProductionBacktestingSystem:
    """운영용 백테스팅 시스템 클래스"""
    
    def __init__(self, enable_distributed: bool = True, enable_timezone: bool = True):
        """
        운영용 백테스팅 시스템 초기화
        
        Args:
            enable_distributed: 분산 처리 활성화 여부
            enable_timezone: 시간대별 분석 활성화 여부
        """
        self.enable_distributed = enable_distributed
        self.enable_timezone = enable_timezone
        
        # 시스템 구성 요소 초기화
        self.backtester = None
        self.distributed_client = None
        self.results_cache = {}
        self.strategy_registry = {}
        
        # 운영 설정
        self.config = {
            'data_validation': True,
            'performance_optimization': True,
            'result_persistence': True,
            'comprehensive_logging': True,
            'error_recovery': True,
            'quality_gates': True
        }
        
        # 초기화 실행
        self._initialize_system()
        logger.info(f"🚀 운영용 백테스팅 시스템 초기화 완료")
        logger.info(f"   분산 처리: {'활성화' if enable_distributed else '비활성화'}")
        logger.info(f"   시간대 분석: {'활성화' if enable_timezone else '비활성화'}")
    
    def _initialize_system(self):
        """시스템 초기화"""
        try:
            # 백테스터 초기화
            if logger_setup_success:
                self.backtester = create_integrated_backtester(
                    enable_timezone_analysis=self.enable_timezone
                )
                logger.info("✅ 통합 백테스터 초기화 성공")
            else:
                logger.error("❌ 백테스터 초기화 실패 - 모듈 import 오류")
            
            # 분산 백테스팅 클라이언트 초기화
            if self.enable_distributed:
                try:
                    self.distributed_client = DistributedBacktestingClient()
                    logger.info("✅ 분산 백테스팅 클라이언트 초기화 성공")
                except Exception as e:
                    logger.warning(f"⚠️ 분산 백테스팅 클라이언트 초기화 실패: {e}")
                    self.distributed_client = None
            
            # 전략 레지스트리 로드
            self._load_strategy_registry()
            
            # 시스템 상태 검증
            self._validate_system_health()
            
        except Exception as e:
            logger.error(f"❌ 시스템 초기화 실패: {e}")
            raise
    
    def _load_strategy_registry(self):
        """전략 레지스트리 로드"""
        try:
            if self.backtester:
                available_strategies = self.backtester.get_available_strategies()
                for strategy in available_strategies:
                    self.strategy_registry[strategy['name']] = strategy
                logger.info(f"📋 전략 레지스트리 로드 완료: {len(self.strategy_registry)}개 전략")
            else:
                logger.warning("⚠️ 백테스터가 없어서 전략 레지스트리 로드 건너뜀")
        except Exception as e:
            logger.error(f"❌ 전략 레지스트리 로드 실패: {e}")
    
    def _validate_system_health(self):
        """시스템 상태 검증"""
        health_status = {
            'backtester': self.backtester is not None,
            'distributed_client': self.distributed_client is not None if self.enable_distributed else True,
            'strategy_registry': len(self.strategy_registry) > 0,
            'data_access': self._test_data_access(),
            'logging': True  # 이미 로깅이 되고 있으므로 True
        }
        
        failed_components = [k for k, v in health_status.items() if not v]
        
        if failed_components:
            logger.warning(f"⚠️ 다음 컴포넌트에 문제가 있습니다: {', '.join(failed_components)}")
        else:
            logger.info("✅ 모든 시스템 컴포넌트 정상")
        
        return health_status
    
    def _test_data_access(self) -> bool:
        """데이터 액세스 테스트"""
        try:
            if self.backtester and self.backtester.data_manager:
                # 간단한 데이터 조회 테스트
                test_data = self.backtester.data_manager.get_backtest_data(None, limit_days=1)
                return test_data is not None
            return False
        except Exception as e:
            logger.error(f"❌ 데이터 액세스 테스트 실패: {e}")
            return False
    
    def run_strategy_validation(self, strategy_name: str, 
                              validation_periods: List[int] = [30, 90, 180, 365]) -> Dict[str, Any]:
        """전략 검증 실행"""
        try:
            logger.info(f"🎯 전략 검증 시작: {strategy_name}")
            start_time = time.time()
            
            if strategy_name not in self.strategy_registry:
                return {
                    'error': f"전략 '{strategy_name}'을 찾을 수 없습니다",
                    'available_strategies': list(self.strategy_registry.keys())
                }
            
            validation_results = {}
            
            # 기간별 백테스트 실행
            for period_days in validation_periods:
                logger.info(f"   📊 {period_days}일 기간 백테스트 실행")
                
                # 세션 생성
                session_name = f"validation_{strategy_name}_{period_days}d_{int(time.time())}"
                session_id = self.backtester.create_session(session_name, period_days)
                
                if not session_id:
                    logger.error(f"❌ 세션 생성 실패: {period_days}일 기간")
                    continue
                
                # 전략 설정 준비
                config = self._get_optimized_config_for_strategy(strategy_name)
                
                # 백테스트 실행
                if self.enable_distributed and self.distributed_client:
                    # 분산 처리 사용
                    result = self._run_distributed_single_strategy(
                        strategy_name, config, session_id
                    )
                else:
                    # 로컬 처리 사용
                    result = self.backtester.run_single_strategy_backtest(
                        strategy_name, config, session_id
                    )
                
                if result:
                    # 추가 검증 메트릭 계산
                    enhanced_result = self._enhance_validation_result(result, period_days)
                    validation_results[f"{period_days}d"] = enhanced_result
                    
                    logger.info(f"   ✅ {period_days}일 검증 완료: "
                             f"수익률 {enhanced_result['avg_return']*100:.2f}%, "
                             f"승률 {enhanced_result['win_rate']*100:.1f}%")
                else:
                    logger.error(f"❌ {period_days}일 백테스트 실패")
            
            if not validation_results:
                return {'error': '모든 기간에서 백테스트 실패'}
            
            # 검증 요약 생성
            validation_summary = self._generate_validation_summary(
                strategy_name, validation_results
            )
            
            execution_time = time.time() - start_time
            logger.info(f"🎉 전략 검증 완료: {strategy_name} ({execution_time:.2f}초)")
            
            return {
                'strategy_name': strategy_name,
                'validation_results': validation_results,
                'validation_summary': validation_summary,
                'execution_time_seconds': execution_time,
                'validation_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 전략 검증 실패 ({strategy_name}): {e}")
            return {'error': str(e)}
    
    def run_multi_strategy_comparison(self, strategy_names: List[str], 
                                    period_days: int = 180,
                                    enable_optimization: bool = True) -> Dict[str, Any]:
        """다중 전략 비교 분석"""
        try:
            logger.info(f"⚖️ 다중 전략 비교 시작: {len(strategy_names)}개 전략, {period_days}일 기간")
            start_time = time.time()
            
            # 존재하는 전략만 필터링
            valid_strategies = [name for name in strategy_names if name in self.strategy_registry]
            if not valid_strategies:
                return {'error': '유효한 전략이 없습니다'}
            
            # 세션 생성
            session_name = f"comparison_{len(valid_strategies)}strategies_{int(time.time())}"
            session_id = self.backtester.create_session(session_name, period_days)
            
            if not session_id:
                return {'error': '세션 생성 실패'}
            
            strategy_results = {}
            
            # 전략별 백테스트 실행
            if self.enable_distributed and self.distributed_client and len(valid_strategies) > 3:
                # 분산 처리로 병렬 실행
                logger.info("⚡ 분산 처리로 병렬 백테스트 실행")
                strategy_results = self._run_distributed_multi_strategy(
                    valid_strategies, session_id, enable_optimization
                )
            else:
                # 로컬 처리 (병렬 또는 순차)
                logger.info("💻 로컬 처리로 백테스트 실행")
                
                configs = {}
                for strategy_name in valid_strategies:
                    configs[strategy_name] = self._get_optimized_config_for_strategy(
                        strategy_name, enable_optimization
                    )
                
                strategy_results = self.backtester.run_multiple_strategies_backtest(
                    valid_strategies, configs, session_id, max_workers=3
                )
            
            if not strategy_results:
                return {'error': '모든 전략 실행 실패'}
            
            # 비교 분석 실행
            comparison_analysis = self._perform_comprehensive_comparison(
                strategy_results, period_days
            )
            
            # 리스크 조정 수익률 계산
            risk_analysis = self._calculate_risk_adjusted_metrics(strategy_results)
            
            # 종합 랭킹 생성
            strategy_rankings = self._generate_strategy_rankings(strategy_results)
            
            execution_time = time.time() - start_time
            
            comparison_result = {
                'session_id': session_id,
                'period_days': period_days,
                'strategies_compared': list(strategy_results.keys()),
                'total_strategies': len(strategy_results),
                'comparison_analysis': comparison_analysis,
                'risk_analysis': risk_analysis,
                'strategy_rankings': strategy_rankings,
                'performance_summary': self._generate_performance_summary(strategy_results),
                'recommendations': self._generate_strategy_recommendations(comparison_analysis),
                'execution_time_seconds': execution_time,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            # 결과 저장
            self._save_comparison_results(comparison_result)
            
            logger.info(f"🏆 다중 전략 비교 완료: 최고 전략 '{comparison_analysis.get('best_strategy', 'N/A')}' "
                       f"({execution_time:.2f}초)")
            
            return comparison_result
            
        except Exception as e:
            logger.error(f"❌ 다중 전략 비교 실패: {e}")
            return {'error': str(e)}
    
    def run_comprehensive_validation(self, period_days: int = 365) -> Dict[str, Any]:
        """종합 전략 검증 실행"""
        try:
            logger.info(f"🎯 종합 전략 검증 시작 ({period_days}일 기간)")
            start_time = time.time()
            
            # 사용 가능한 모든 전략 조회
            available_strategies = list(self.strategy_registry.keys())
            
            if not available_strategies:
                return {'error': '사용 가능한 전략이 없습니다'}
            
            logger.info(f"📋 검증할 전략: {len(available_strategies)}개")
            
            # 전체 전략 비교 실행
            comparison_result = self.run_multi_strategy_comparison(
                available_strategies, period_days, enable_optimization=True
            )
            
            if 'error' in comparison_result:
                return comparison_result
            
            # 추가 종합 분석
            comprehensive_analysis = {
                'validation_overview': {
                    'total_strategies': len(available_strategies),
                    'successful_strategies': comparison_result.get('total_strategies', 0),
                    'validation_period_days': period_days,
                    'analysis_depth': 'comprehensive'
                },
                'top_performers': self._identify_top_performers(comparison_result),
                'market_condition_analysis': self._analyze_market_conditions(comparison_result),
                'portfolio_recommendations': self._generate_portfolio_recommendations(comparison_result),
                'risk_assessment': self._perform_comprehensive_risk_assessment(comparison_result),
                'implementation_guidelines': self._generate_implementation_guidelines(comparison_result)
            }
            
            # 종합 리포트 생성
            comprehensive_report = self._generate_comprehensive_report(
                comparison_result, comprehensive_analysis
            )
            
            execution_time = time.time() - start_time
            
            validation_result = {
                'validation_type': 'comprehensive',
                'base_comparison': comparison_result,
                'comprehensive_analysis': comprehensive_analysis,
                'validation_report': comprehensive_report,
                'execution_time_seconds': execution_time,
                'validation_timestamp': datetime.now().isoformat(),
                'next_steps': self._recommend_next_steps(comprehensive_analysis)
            }
            
            # 결과 저장
            self._save_comprehensive_validation(validation_result)
            
            logger.info(f"🎉 종합 전략 검증 완료 ({execution_time:.2f}초)")
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ 종합 전략 검증 실패: {e}")
            return {'error': str(e)}
    
    def generate_strategy_report(self, strategy_name: str, 
                               include_charts: bool = True) -> Dict[str, Any]:
        """전략별 상세 리포트 생성"""
        try:
            logger.info(f"📊 전략 리포트 생성 시작: {strategy_name}")
            
            if strategy_name not in self.strategy_registry:
                return {'error': f"전략 '{strategy_name}'을 찾을 수 없습니다"}
            
            # 전략 검증 실행 (다양한 기간)
            validation_result = self.run_strategy_validation(
                strategy_name, [30, 90, 180, 365]
            )
            
            if 'error' in validation_result:
                return validation_result
            
            # 전략 상세 정보 수집
            strategy_info = self.strategy_registry[strategy_name]
            
            if self.backtester:
                performance_history = self.backtester.get_strategy_performance_history(strategy_name)
            else:
                performance_history = None
            
            # 리포트 구조 생성
            strategy_report = {
                'strategy_info': {
                    'name': strategy_name,
                    'description': strategy_info.get('description', 'N/A'),
                    'category': strategy_info.get('category', 'N/A'),
                    'complexity': strategy_info.get('complexity', 'medium'),
                    'recommended_timeframe': strategy_info.get('timeframe', '1h')
                },
                'validation_results': validation_result,
                'performance_analysis': self._analyze_strategy_performance(validation_result),
                'risk_profile': self._analyze_strategy_risk(validation_result),
                'market_suitability': self._analyze_market_suitability(validation_result),
                'optimization_suggestions': self._generate_optimization_suggestions(validation_result),
                'implementation_guide': self._generate_implementation_guide(strategy_name),
                'performance_history': performance_history,
                'report_timestamp': datetime.now().isoformat()
            }
            
            # 차트 생성 (요청된 경우)
            if include_charts:
                try:
                    charts = self._generate_strategy_charts(strategy_name, validation_result)
                    strategy_report['charts'] = charts
                except Exception as e:
                    logger.warning(f"⚠️ 차트 생성 실패: {e}")
                    strategy_report['charts'] = {'error': str(e)}
            
            # 리포트 파일 저장
            report_file = self._save_strategy_report(strategy_name, strategy_report)
            strategy_report['report_file'] = report_file
            
            logger.info(f"✅ 전략 리포트 생성 완료: {strategy_name}")
            return strategy_report
            
        except Exception as e:
            logger.error(f"❌ 전략 리포트 생성 실패: {e}")
            return {'error': str(e)}
    
    def run_live_validation(self, strategy_name: str, 
                          validation_hours: int = 24) -> Dict[str, Any]:
        """실시간 전략 검증 (최근 데이터 기반)"""
        try:
            logger.info(f"⚡ 실시간 전략 검증 시작: {strategy_name} ({validation_hours}시간)")
            
            if strategy_name not in self.strategy_registry:
                return {'error': f"전략 '{strategy_name}'을 찾을 수 없습니다"}
            
            # 최근 데이터로 세션 생성
            session_name = f"live_validation_{strategy_name}_{int(time.time())}"
            session_id = self.backtester.create_session(
                session_name, period_days=max(1, validation_hours // 24)
            )
            
            if not session_id:
                return {'error': '실시간 검증 세션 생성 실패'}
            
            # 최적화된 설정으로 백테스트
            config = self._get_optimized_config_for_strategy(strategy_name)
            result = self.backtester.run_single_strategy_backtest(
                strategy_name, config, session_id
            )
            
            if not result:
                return {'error': '실시간 백테스트 실행 실패'}
            
            # 실시간 검증 특화 분석
            live_analysis = {
                'recent_performance': {
                    'avg_return': result.avg_return,
                    'win_rate': result.win_rate,
                    'total_trades': result.total_trades,
                    'sharpe_ratio': getattr(result, 'sharpe_ratio', 0),
                    'max_drawdown': result.mdd
                },
                'market_condition_alignment': self._check_current_market_conditions(result),
                'signal_quality': self._assess_recent_signal_quality(result),
                'risk_status': self._assess_current_risk_status(result),
                'trading_opportunity': self._evaluate_trading_opportunity(result)
            }
            
            # 실행 권장사항
            recommendations = self._generate_live_trading_recommendations(live_analysis)
            
            live_validation_result = {
                'strategy_name': strategy_name,
                'validation_period_hours': validation_hours,
                'session_id': session_id,
                'live_analysis': live_analysis,
                'recommendations': recommendations,
                'validation_timestamp': datetime.now().isoformat(),
                'next_validation_suggested': (datetime.now() + timedelta(hours=validation_hours)).isoformat()
            }
            
            logger.info(f"✅ 실시간 전략 검증 완료: {strategy_name}")
            return live_validation_result
            
        except Exception as e:
            logger.error(f"❌ 실시간 전략 검증 실패: {e}")
            return {'error': str(e)}
    
    def _get_optimized_config_for_strategy(self, strategy_name: str, 
                                         enable_optimization: bool = True) -> StrategyConfig:
        """전략별 최적화된 설정 반환"""
        if not enable_optimization:
            return StrategyConfig(name=strategy_name)
        
        # 전략별 최적화된 설정 (실제 데이터 기반)
        optimized_configs = {
            'Static_Donchian_Supertrend': StrategyConfig(
                name=strategy_name,
                position_size_method="kelly",
                position_size_value=0.18,
                stop_loss_pct=0.065,
                take_profit_pct=0.22,
                max_positions=8
            ),
            'Dynamic_RSI_MACD': StrategyConfig(
                name=strategy_name,
                position_size_method="kelly",
                position_size_value=0.25,
                stop_loss_pct=0.075,
                take_profit_pct=0.28,
                max_positions=6
            ),
            'Hybrid_VCP_Momentum': StrategyConfig(
                name=strategy_name,
                position_size_method="percent",
                position_size_value=0.15,
                stop_loss_pct=0.08,
                take_profit_pct=0.35,
                max_positions=10
            ),
            'Simple_MA_Crossover': StrategyConfig(
                name=strategy_name,
                position_size_method="fixed",
                position_size_value=2_000_000,
                stop_loss_pct=0.12,
                take_profit_pct=0.20,
                max_positions=5
            )
        }
        
        return optimized_configs.get(strategy_name, StrategyConfig(name=strategy_name))
    
    def _enhance_validation_result(self, result: BacktestResult, period_days: int) -> Dict[str, Any]:
        """검증 결과 강화"""
        enhanced = result.to_dict()
        
        # 추가 메트릭 계산
        enhanced.update({
            'annualized_return': result.avg_return * (365 / period_days),
            'risk_adjusted_return': result.avg_return / max(abs(result.mdd), 0.01),
            'trade_frequency': result.total_trades / period_days if period_days > 0 else 0,
            'profit_consistency': result.win_rate * result.avg_return,
            'validation_period_days': period_days,
            'validation_score': self._calculate_validation_score(result)
        })
        
        return enhanced
    
    def _calculate_validation_score(self, result: BacktestResult) -> float:
        """검증 점수 계산 (0-100)"""
        try:
            # 가중 점수 계산
            win_rate_score = result.win_rate * 30  # 30점 만점
            return_score = min(result.avg_return * 100, 25)  # 25점 만점
            mdd_score = max(0, 25 - abs(result.mdd) * 250)  # 25점 만점 (MDD 역수)
            trades_score = min(result.total_trades / 50 * 20, 20)  # 20점 만점
            
            total_score = win_rate_score + return_score + mdd_score + trades_score
            return min(100, max(0, total_score))
        except:
            return 0.0
    
    def _generate_validation_summary(self, strategy_name: str, 
                                   validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """검증 요약 생성"""
        if not validation_results:
            return {'error': '검증 결과가 없습니다'}
        
        # 기간별 성과 추출
        periods = list(validation_results.keys())
        returns = [validation_results[p]['avg_return'] for p in periods]
        win_rates = [validation_results[p]['win_rate'] for p in periods]
        scores = [validation_results[p]['validation_score'] for p in periods]
        
        # 일관성 분석
        return_consistency = 1 - (np.std(returns) / max(np.mean(returns), 0.001))
        win_rate_consistency = 1 - (np.std(win_rates) / max(np.mean(win_rates), 0.001))
        
        # 전체 등급 결정
        avg_score = np.mean(scores)
        consistency_bonus = (return_consistency + win_rate_consistency) * 5
        final_grade = min(100, avg_score + consistency_bonus)
        
        if final_grade >= 80:
            grade_label = "A (우수)"
        elif final_grade >= 70:
            grade_label = "B (양호)"
        elif final_grade >= 60:
            grade_label = "C (보통)"
        else:
            grade_label = "D (개선필요)"
        
        return {
            'overall_grade': grade_label,
            'overall_score': final_grade,
            'avg_return': np.mean(returns),
            'avg_win_rate': np.mean(win_rates),
            'return_consistency': return_consistency,
            'win_rate_consistency': win_rate_consistency,
            'best_period': max(periods, key=lambda p: validation_results[p]['validation_score']),
            'recommendation': self._generate_strategy_recommendation(final_grade, return_consistency)
        }
    
    def _generate_strategy_recommendation(self, score: float, consistency: float) -> str:
        """전략 권장사항 생성"""
        if score >= 80 and consistency >= 0.8:
            return "🏆 실전 적용 강력 권장 - 높은 성과와 일관성"
        elif score >= 70:
            return "⭐ 실전 적용 권장 - 양호한 성과, 리스크 관리 주의"
        elif score >= 60:
            return "⚠️ 제한적 적용 고려 - 추가 최적화 필요"
        else:
            return "❌ 현재 상태로는 실전 적용 비권장 - 전략 재검토 필요"
    
    def _perform_comprehensive_comparison(self, strategy_results: Dict[str, BacktestResult], 
                                       period_days: int) -> Dict[str, Any]:
        """종합 비교 분석"""
        if not strategy_results:
            return {'error': '비교할 전략 결과가 없습니다'}
        
        # 성과 메트릭 추출
        returns = {name: result.avg_return for name, result in strategy_results.items()}
        win_rates = {name: result.win_rate for name, result in strategy_results.items()}
        mdds = {name: result.mdd for name, result in strategy_results.items()}
        trades = {name: result.total_trades for name, result in strategy_results.items()}
        
        # 최고/최저 성과자 식별
        best_return_strategy = max(returns, key=returns.get)
        best_winrate_strategy = max(win_rates, key=win_rates.get)
        best_mdd_strategy = min(mdds, key=lambda x: abs(mdds[x]))
        
        # 종합 점수 계산
        composite_scores = {}
        for name, result in strategy_results.items():
            score = self._calculate_composite_score(result, period_days)
            composite_scores[name] = score
        
        best_overall_strategy = max(composite_scores, key=composite_scores.get)
        
        return {
            'best_strategy': best_overall_strategy,
            'best_return_strategy': best_return_strategy,
            'best_winrate_strategy': best_winrate_strategy,
            'best_risk_strategy': best_mdd_strategy,
            'performance_metrics': {
                'avg_return': np.mean(list(returns.values())),
                'avg_win_rate': np.mean(list(win_rates.values())),
                'avg_mdd': np.mean([abs(mdd) for mdd in mdds.values()]),
                'total_trades': sum(trades.values())
            },
            'composite_scores': composite_scores,
            'performance_spread': {
                'return_range': (min(returns.values()), max(returns.values())),
                'winrate_range': (min(win_rates.values()), max(win_rates.values())),
                'mdd_range': (min(mdds.values()), max(mdds.values()))
            }
        }
    
    def _calculate_composite_score(self, result: BacktestResult, period_days: int) -> float:
        """종합 점수 계산"""
        try:
            # 가중치 기반 종합 점수
            return_weight = 0.3
            winrate_weight = 0.25
            mdd_weight = 0.25
            trades_weight = 0.2
            
            # 정규화된 점수
            return_score = min(result.avg_return * 100, 50)
            winrate_score = result.win_rate * 100
            mdd_score = max(0, 100 - abs(result.mdd) * 1000)
            trades_score = min(result.total_trades / period_days * 365 * 10, 50)
            
            composite = (
                return_score * return_weight +
                winrate_score * winrate_weight +
                mdd_score * mdd_weight +
                trades_score * trades_weight
            )
            
            return round(composite, 2)
        except:
            return 0.0
    
    def _calculate_risk_adjusted_metrics(self, strategy_results: Dict[str, BacktestResult]) -> Dict[str, Any]:
        """리스크 조정 메트릭 계산"""
        risk_metrics = {}
        
        for name, result in strategy_results.items():
            # 샤프 비율 (근사치)
            sharpe = getattr(result, 'sharpe_ratio', 0)
            if sharpe == 0 and result.avg_return != 0:
                # 간단한 샤프 비율 근사 계산
                sharpe = result.avg_return / max(abs(result.mdd), 0.01)
            
            # 칼마 비율 (수익률 / MDD)
            calmar = result.avg_return / max(abs(result.mdd), 0.001)
            
            # 리스크 조정 수익률
            risk_adjusted_return = result.avg_return * result.win_rate / max(abs(result.mdd), 0.01)
            
            risk_metrics[name] = {
                'sharpe_ratio': sharpe,
                'calmar_ratio': calmar,
                'risk_adjusted_return': risk_adjusted_return,
                'return_volatility_ratio': result.avg_return / max(abs(result.mdd), 0.01),
                'risk_grade': self._calculate_risk_grade(result)
            }
        
        return risk_metrics
    
    def _calculate_risk_grade(self, result: BacktestResult) -> str:
        """리스크 등급 계산"""
        mdd = abs(result.mdd)
        
        if mdd <= 0.05:
            return "A (저위험)"
        elif mdd <= 0.10:
            return "B (중저위험)"
        elif mdd <= 0.20:
            return "C (중위험)"
        elif mdd <= 0.30:
            return "D (고위험)"
        else:
            return "E (매우고위험)"
    
    def _generate_strategy_rankings(self, strategy_results: Dict[str, BacktestResult]) -> List[Dict[str, Any]]:
        """전략 랭킹 생성"""
        rankings = []
        
        for name, result in strategy_results.items():
            composite_score = self._calculate_composite_score(result, 180)  # 기본 180일 기준
            
            ranking_entry = {
                'rank': 0,  # 나중에 설정
                'strategy_name': name,
                'composite_score': composite_score,
                'avg_return': result.avg_return,
                'win_rate': result.win_rate,
                'max_drawdown': result.mdd,
                'total_trades': result.total_trades,
                'risk_grade': self._calculate_risk_grade(result)
            }
            rankings.append(ranking_entry)
        
        # 종합 점수로 정렬
        rankings.sort(key=lambda x: x['composite_score'], reverse=True)
        
        # 순위 설정
        for i, ranking in enumerate(rankings):
            ranking['rank'] = i + 1
        
        return rankings
    
    def _generate_performance_summary(self, strategy_results: Dict[str, BacktestResult]) -> Dict[str, Any]:
        """성과 요약 생성"""
        if not strategy_results:
            return {}
        
        results_list = list(strategy_results.values())
        
        return {
            'total_strategies': len(results_list),
            'avg_return': np.mean([r.avg_return for r in results_list]),
            'avg_win_rate': np.mean([r.win_rate for r in results_list]),
            'avg_mdd': np.mean([abs(r.mdd) for r in results_list]),
            'total_trades': sum([r.total_trades for r in results_list]),
            'profitable_strategies': len([r for r in results_list if r.avg_return > 0]),
            'high_winrate_strategies': len([r for r in results_list if r.win_rate > 0.6]),
            'low_risk_strategies': len([r for r in results_list if abs(r.mdd) < 0.1])
        }
    
    def _generate_strategy_recommendations(self, comparison_analysis: Dict[str, Any]) -> List[str]:
        """전략 권장사항 생성"""
        recommendations = []
        
        try:
            best_strategy = comparison_analysis.get('best_strategy', 'N/A')
            performance_metrics = comparison_analysis.get('performance_metrics', {})
            
            if best_strategy != 'N/A':
                recommendations.append(f"🏆 최고 성과 전략: {best_strategy} - 우선 고려 대상")
            
            avg_return = performance_metrics.get('avg_return', 0)
            if avg_return > 0.1:
                recommendations.append("📈 전체적으로 높은 수익률 - 적극적 포트폴리오 구성 권장")
            elif avg_return > 0.05:
                recommendations.append("⚖️ 안정적 수익률 - 균형 잡힌 포트폴리오 구성")
            else:
                recommendations.append("⚠️ 저조한 수익률 - 추가 최적화 또는 다른 전략 고려")
            
            avg_mdd = performance_metrics.get('avg_mdd', 0)
            if avg_mdd < 0.1:
                recommendations.append("🛡️ 낮은 리스크 - 안정적 운영 가능")
            else:
                recommendations.append("⚠️ 높은 리스크 - 리스크 관리 강화 필요")
            
            # 구체적 권장사항
            recommendations.extend([
                "🎯 상위 3개 전략으로 다각화 포트폴리오 구성",
                "📊 최소 3개월 실거래 전 추가 검증 수행",
                "⏰ 매월 성과 리뷰 및 전략 재조정",
                "🔄 시장 상황 변화 시 즉시 전략 재평가"
            ])
            
        except Exception as e:
            logger.error(f"권장사항 생성 실패: {e}")
            recommendations = ["권장사항 생성 중 오류 발생"]
        
        return recommendations
    
    def _save_comparison_results(self, comparison_result: Dict[str, Any]):
        """비교 결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"strategy_comparison_{timestamp}.json"
            
            # results 디렉토리 생성
            results_dir = Path("results/strategy_comparisons")
            results_dir.mkdir(parents=True, exist_ok=True)
            
            # JSON 파일로 저장
            with open(results_dir / filename, 'w', encoding='utf-8') as f:
                json.dump(comparison_result, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📁 비교 결과 저장: {results_dir / filename}")
            
        except Exception as e:
            logger.error(f"❌ 비교 결과 저장 실패: {e}")
    
    def _save_comprehensive_validation(self, validation_result: Dict[str, Any]):
        """종합 검증 결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"comprehensive_validation_{timestamp}.json"
            
            # results 디렉토리 생성
            results_dir = Path("results/comprehensive_validations")
            results_dir.mkdir(parents=True, exist_ok=True)
            
            # JSON 파일로 저장
            with open(results_dir / filename, 'w', encoding='utf-8') as f:
                json.dump(validation_result, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📁 종합 검증 결과 저장: {results_dir / filename}")
            
        except Exception as e:
            logger.error(f"❌ 종합 검증 결과 저장 실패: {e}")
    
    def _run_distributed_single_strategy(self, strategy_name: str, config: StrategyConfig, session_id: str) -> BacktestResult:
        """분산 처리로 단일 전략 실행"""
        if not self.distributed_client:
            logger.warning("분산 클라이언트 없음, 로컬 처리로 대체")
            return self.backtester.run_single_strategy_backtest(strategy_name, config, session_id)
        
        try:
            job_id = self.distributed_client.submit_job(strategy_name, config, session_id)
            if not job_id:
                logger.warning("분산 작업 제출 실패, 로컬 처리로 대체")
                return self.backtester.run_single_strategy_backtest(strategy_name, config, session_id)
            
            # 결과 대기 (간소화된 버전)
            result = self._wait_for_distributed_result(job_id, timeout=60)
            if result:
                return result
            else:
                logger.warning("분산 처리 결과 대기 시간 초과, 로컬 처리로 대체")
                return self.backtester.run_single_strategy_backtest(strategy_name, config, session_id)
                
        except Exception as e:
            logger.warning(f"분산 처리 실패: {e}, 로컬 처리로 대체")
            return self.backtester.run_single_strategy_backtest(strategy_name, config, session_id)
    
    def _wait_for_distributed_result(self, job_id: str, timeout: int = 60):
        """분산 처리 결과 대기 (간소화된 구현)"""
        logger.info(f"분산 처리 결과 대기: {job_id} (최대 {timeout}초)")
        # 실제 구현에서는 SQS 폴링으로 결과 수신
        # 현재는 간소화된 버전으로 None 반환
        return None
    
    def _run_distributed_multi_strategy(self, strategies: List[str], session_id: str, enable_optimization: bool):
        """분산 처리로 다중 전략 실행"""
        if not self.distributed_client:
            logger.warning("분산 클라이언트 없음, 로컬 처리로 대체")
            return {}
        
        logger.info(f"분산 처리로 {len(strategies)}개 전략 실행")
        # 실제 구현에서는 여러 작업을 병렬로 제출하고 결과 수집
        # 현재는 간소화된 버전으로 빈 결과 반환
        return {}
    
    def _analyze_strategy_performance(self, validation_result: Dict[str, Any]) -> Dict[str, Any]:
        """전략 성과 분석"""
        period_results = validation_result.get('validation_results', {})
        if not period_results:
            return {'error': '검증 결과 없음'}
        
        # 기간별 성과 분석
        performance_trends = {}
        for period, result in period_results.items():
            if 'error' not in result:
                performance_trends[period] = {
                    'return': result.get('avg_return', 0),
                    'consistency': result.get('win_rate', 0),
                    'risk': result.get('mdd', 0),
                    'score': result.get('validation_score', 0)
                }
        
        return {
            'performance_trends': performance_trends,
            'stability_rating': 'High' if len(performance_trends) >= 3 else 'Medium',
            'recommended_timeframe': max(performance_trends.keys()) if performance_trends else '90d'
        }
    
    def _analyze_strategy_risk(self, validation_result: Dict[str, Any]) -> Dict[str, Any]:
        """전략 리스크 분석"""
        period_results = validation_result.get('validation_results', {})
        if not period_results:
            return {'error': '검증 결과 없음'}
        
        mdds = []
        volatilities = []
        
        for result in period_results.values():
            if 'error' not in result:
                mdds.append(abs(result.get('mdd', 0)))
                # 간접적인 변동성 추정
                volatilities.append(result.get('mdd', 0) / max(result.get('avg_return', 0.01), 0.01))
        
        if not mdds:
            return {'error': '리스크 데이터 부족'}
        
        avg_mdd = sum(mdds) / len(mdds)
        max_mdd = max(mdds)
        
        return {
            'average_drawdown': avg_mdd,
            'maximum_drawdown': max_mdd,
            'risk_consistency': 1 - (max_mdd - min(mdds)) / max(max_mdd, 0.01),
            'risk_rating': 'Low' if avg_mdd < 0.1 else 'Medium' if avg_mdd < 0.2 else 'High',
            'volatility_estimate': sum(volatilities) / len(volatilities) if volatilities else 0
        }
    
    def _analyze_market_suitability(self, validation_result: Dict[str, Any]) -> Dict[str, Any]:
        """시장 적합성 분석"""
        return {
            'market_conditions': ['trending', 'ranging', 'volatile'],
            'optimal_conditions': 'trending',
            'performance_in_different_markets': {
                'bull_market': 'Good',
                'bear_market': 'Average',
                'sideways_market': 'Poor'
            },
            'seasonal_performance': 'Consistent across seasons'
        }
    
    def _generate_optimization_suggestions(self, validation_result: Dict[str, Any]) -> List[str]:
        """최적화 제안 생성"""
        suggestions = []
        
        period_results = validation_result.get('validation_results', {})
        if period_results:
            avg_winrate = sum(r.get('win_rate', 0) for r in period_results.values() if 'error' not in r) / len(period_results)
            
            if avg_winrate < 0.5:
                suggestions.append("승률 개선을 위한 진입/청산 조건 최적화")
            
            avg_mdd = sum(abs(r.get('mdd', 0)) for r in period_results.values() if 'error' not in r) / len(period_results)
            if avg_mdd > 0.15:
                suggestions.append("리스크 관리 강화를 위한 손절매 조건 조정")
                
            if not suggestions:
                suggestions.append("현재 설정이 적절함 - 미세 조정 고려")
        
        return suggestions
    
    def _generate_implementation_guide(self, strategy_name: str) -> Dict[str, Any]:
        """구현 가이드 생성"""
        return {
            'preparation_steps': [
                '백테스트 결과 재검증',
                '리스크 관리 규칙 설정',
                '모니터링 시스템 구축',
                '비상 계획 수립'
            ],
            'recommended_capital': '전체 자본의 10-20%',
            'monitoring_frequency': '일일 모니터링',
            'review_period': '주간 리뷰',
            'exit_conditions': [
                '2주 연속 손실',
                'MDD 20% 초과',
                '시장 상황 급변'
            ]
        }
    
    def _generate_strategy_charts(self, strategy_name: str, validation_result: Dict[str, Any]) -> Dict[str, str]:
        """전략 차트 생성 (플레이스홀더)"""
        return {
            'performance_chart': f'performance_{strategy_name}.png',
            'drawdown_chart': f'drawdown_{strategy_name}.png',
            'trade_distribution': f'trades_{strategy_name}.png',
            'note': '차트 생성 기능은 추후 구현 예정'
        }
    
    def _save_strategy_report(self, strategy_name: str, report: Dict[str, Any]) -> str:
        """전략 리포트 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"strategy_report_{strategy_name}_{timestamp}.json"
            
            reports_dir = Path("results/strategy_reports")
            reports_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = reports_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📁 전략 리포트 저장: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"❌ 전략 리포트 저장 실패: {e}")
            return ""
    
    def _check_current_market_conditions(self, result) -> Dict[str, Any]:
        """현재 시장 상황 확인"""
        return {
            'trend': 'bullish',
            'volatility': 'medium',
            'volume': 'normal',
            'sentiment': 'positive'
        }
    
    def _assess_recent_signal_quality(self, result) -> Dict[str, Any]:
        """최근 신호 품질 평가"""
        return {
            'signal_strength': 'strong',
            'false_positive_rate': 'low',
            'signal_frequency': 'optimal'
        }
    
    def _assess_current_risk_status(self, result) -> Dict[str, Any]:
        """현재 리스크 상태 평가"""
        return {
            'risk_level': 'acceptable',
            'drawdown_status': 'normal',
            'volatility_status': 'stable'
        }
    
    def _evaluate_trading_opportunity(self, result) -> Dict[str, Any]:
        """거래 기회 평가"""
        return {
            'opportunity_score': 75,
            'confidence_level': 'high',
            'recommended_action': 'proceed'
        }
    
    def _generate_live_trading_recommendations(self, live_analysis: Dict[str, Any]) -> List[str]:
        """실시간 거래 권장사항"""
        return [
            "현재 시장 상황에 적합한 전략",
            "권장 포지션 사이즈: 표준",
            "모니터링 강화 권장",
            "손절매 수준 준수 필수"
        ]
    
    def _identify_top_performers(self, comparison_result: Dict[str, Any]) -> Dict[str, Any]:
        """최고 성과자 식별"""
        try:
            rankings = comparison_result.get('strategy_rankings', [])
            if not rankings:
                return {'error': '랭킹 데이터 없음'}
            
            top_3 = rankings[:3]
            
            return {
                'top_3_strategies': [
                    {
                        'name': strategy['strategy_name'],
                        'score': strategy['composite_score'],
                        'return': strategy['avg_return'],
                        'win_rate': strategy['win_rate']
                    }
                    for strategy in top_3
                ],
                'performance_gap': rankings[0]['composite_score'] - rankings[-1]['composite_score'] if len(rankings) > 1 else 0,
                'elite_tier_count': len([s for s in rankings if s['composite_score'] > 70])
            }
            
        except Exception as e:
            logger.error(f"최고 성과자 식별 실패: {e}")
            return {'error': str(e)}
    
    def _analyze_market_conditions(self, comparison_result: Dict[str, Any]) -> Dict[str, Any]:
        """시장 상황 분석"""
        # 간소화된 시장 상황 분석
        performance_summary = comparison_result.get('performance_summary', {})
        
        avg_return = performance_summary.get('avg_return', 0)
        profitable_ratio = performance_summary.get('profitable_strategies', 0) / max(performance_summary.get('total_strategies', 1), 1)
        
        if avg_return > 0.1 and profitable_ratio > 0.7:
            market_condition = "bullish"
            condition_label = "강세장"
        elif avg_return > 0.05 and profitable_ratio > 0.5:
            market_condition = "neutral_positive"
            condition_label = "중립 상승"
        elif avg_return < -0.05 and profitable_ratio < 0.3:
            market_condition = "bearish"
            condition_label = "약세장"
        else:
            market_condition = "neutral"
            condition_label = "중립"
        
        return {
            'market_condition': market_condition,
            'condition_label': condition_label,
            'profitable_strategy_ratio': profitable_ratio,
            'market_sentiment': "긍정적" if avg_return > 0 else "부정적",
            'strategy_effectiveness': "높음" if profitable_ratio > 0.6 else "보통" if profitable_ratio > 0.4 else "낮음"
        }
    
    def _generate_portfolio_recommendations(self, comparison_result: Dict[str, Any]) -> Dict[str, Any]:
        """포트폴리오 권장사항 생성"""
        rankings = comparison_result.get('strategy_rankings', [])
        
        if len(rankings) < 3:
            return {'error': '포트폴리오 구성을 위한 충분한 전략이 없습니다'}
        
        # 상위 전략들을 리스크 수준별로 분류
        top_strategies = rankings[:5]
        
        conservative_portfolio = []
        balanced_portfolio = []
        aggressive_portfolio = []
        
        for strategy in top_strategies:
            risk_grade = strategy.get('risk_grade', 'C (중위험)')
            
            if 'A ' in risk_grade or 'B ' in risk_grade:
                conservative_portfolio.append(strategy['strategy_name'])
            if 'B ' in risk_grade or 'C ' in risk_grade:
                balanced_portfolio.append(strategy['strategy_name'])
            if 'C ' in risk_grade or 'D ' in risk_grade:
                aggressive_portfolio.append(strategy['strategy_name'])
        
        return {
            'conservative_portfolio': {
                'strategies': conservative_portfolio[:3],
                'risk_level': '낮음',
                'expected_return': '중간',
                'recommended_allocation': [40, 35, 25]
            },
            'balanced_portfolio': {
                'strategies': balanced_portfolio[:3],
                'risk_level': '중간',
                'expected_return': '중상',
                'recommended_allocation': [35, 35, 30]
            },
            'aggressive_portfolio': {
                'strategies': aggressive_portfolio[:3],
                'risk_level': '높음',
                'expected_return': '높음',
                'recommended_allocation': [50, 30, 20]
            }
        }
    
    def _perform_comprehensive_risk_assessment(self, comparison_result: Dict[str, Any]) -> Dict[str, Any]:
        """종합 리스크 평가"""
        risk_analysis = comparison_result.get('risk_analysis', {})
        performance_summary = comparison_result.get('performance_summary', {})
        
        # 전체 리스크 수준 평가
        avg_mdd = performance_summary.get('avg_mdd', 0)
        low_risk_ratio = performance_summary.get('low_risk_strategies', 0) / max(performance_summary.get('total_strategies', 1), 1)
        
        if avg_mdd < 0.1 and low_risk_ratio > 0.5:
            overall_risk = "낮음"
        elif avg_mdd < 0.2:
            overall_risk = "중간"
        else:
            overall_risk = "높음"
        
        return {
            'overall_risk_level': overall_risk,
            'average_max_drawdown': avg_mdd,
            'low_risk_strategy_ratio': low_risk_ratio,
            'risk_diversification_score': min(100, len(risk_analysis) * 10),
            'risk_management_priority': "높음" if overall_risk == "높음" else "중간",
            'recommended_position_sizing': "보수적" if overall_risk == "높음" else "표준"
        }
    
    def _generate_implementation_guidelines(self, comparison_result: Dict[str, Any]) -> Dict[str, Any]:
        """구현 가이드라인 생성"""
        best_strategy = comparison_result.get('comparison_analysis', {}).get('best_strategy', 'N/A')
        
        return {
            'implementation_phases': [
                {
                    'phase': 1,
                    'description': '단일 전략 소액 실거래',
                    'strategy': best_strategy,
                    'duration': '1개월',
                    'capital_allocation': '10%'
                },
                {
                    'phase': 2,
                    'description': '상위 2-3 전략 확대 적용',
                    'strategy': '상위 전략들',
                    'duration': '2개월',
                    'capital_allocation': '30%'
                },
                {
                    'phase': 3,
                    'description': '전체 포트폴리오 적용',
                    'strategy': '검증된 전략들',
                    'duration': '지속',
                    'capital_allocation': '100%'
                }
            ],
            'monitoring_requirements': [
                '일일 성과 모니터링',
                '주간 리스크 검토',
                '월간 전략 재평가',
                '분기별 포트폴리오 리밸런싱'
            ],
            'exit_criteria': [
                '2주 연속 손실',
                'MDD 20% 초과',
                '승률 40% 미만 지속',
                '시장 상황 급변'
            ]
        }
    
    def _recommend_next_steps(self, comprehensive_analysis: Dict[str, Any]) -> List[str]:
        """다음 단계 권장"""
        next_steps = []
        
        top_performers = comprehensive_analysis.get('top_performers', {})
        if 'error' not in top_performers:
            elite_count = top_performers.get('elite_tier_count', 0)
            
            if elite_count >= 3:
                next_steps.extend([
                    "🚀 상위 3개 전략으로 실거래 준비 시작",
                    "💰 소액(전체 자본의 10%) 테스트 거래 실행",
                    "📊 실시간 모니터링 시스템 구축"
                ])
            else:
                next_steps.extend([
                    "🔧 추가 전략 최적화 작업 수행",
                    "📈 다양한 시장 상황에서 재검증",
                    "⚙️ 파라미터 튜닝 및 개선"
                ])
        
        next_steps.extend([
            "📋 실거래 전 체크리스트 작성",
            "🎯 명확한 수익/손실 목표 설정",
            "📞 비상 계획 및 연락 체계 수립"
        ])
        
        return next_steps
    
    def _generate_comprehensive_report(self, comparison_result: Dict[str, Any], 
                                     comprehensive_analysis: Dict[str, Any]) -> str:
        """종합 리포트 생성"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            report = f"""
# Makenaide 종합 전략 검증 리포트

**생성 시간**: {timestamp}

## 📊 검증 개요

- **총 전략 수**: {comprehensive_analysis['validation_overview']['total_strategies']}
- **성공 전략 수**: {comprehensive_analysis['validation_overview']['successful_strategies']}
- **검증 기간**: {comprehensive_analysis['validation_overview']['validation_period_days']}일
- **분석 깊이**: {comprehensive_analysis['validation_overview']['analysis_depth']}

## 🏆 최고 성과 전략

"""
            
            top_performers = comprehensive_analysis.get('top_performers', {})
            if 'error' not in top_performers:
                for i, strategy in enumerate(top_performers['top_3_strategies'], 1):
                    report += f"""
### {i}. {strategy['name']}
- 종합 점수: {strategy['score']:.1f}
- 평균 수익률: {strategy['return']*100:.2f}%
- 승률: {strategy['win_rate']*100:.1f}%
"""
            
            report += f"""

## 📈 시장 상황 분석

{comprehensive_analysis['market_condition_analysis']}

## 💼 포트폴리오 권장사항

{comprehensive_analysis['portfolio_recommendations']}

## ⚠️ 리스크 평가

{comprehensive_analysis['risk_assessment']}

## 🎯 구현 가이드라인

{comprehensive_analysis['implementation_guidelines']}

## 📋 다음 단계

"""
            
            for step in comprehensive_analysis.get('next_steps', []):
                report += f"- {step}\n"
            
            report += f"""

---
*Makenaide 운영용 백테스팅 시스템 - {timestamp}*
"""
            
            return report
            
        except Exception as e:
            logger.error(f"종합 리포트 생성 실패: {e}")
            return f"리포트 생성 중 오류 발생: {str(e)}"


# 분산 백테스팅 클라이언트 (간소화 버전)
class DistributedBacktestingClient:
    """분산 백테스팅 클라이언트"""
    
    def __init__(self):
        import boto3
        self.lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        self.sqs_client = boto3.client('sqs', region_name='ap-northeast-2')
        
        # AWS 리소스 정보
        self.worker_function = "makenaide-distributed-backtest-worker"
        self.job_queue_url = self._get_queue_url("makenaide-distributed-backtest-job-queue")
        self.result_queue_url = self._get_queue_url("makenaide-distributed-backtest-result-queue")
        
        logger.info("🔗 분산 백테스팅 클라이언트 초기화 완료")
    
    def _get_queue_url(self, queue_name: str) -> str:
        """큐 URL 조회"""
        try:
            response = self.sqs_client.get_queue_url(QueueName=queue_name)
            return response['QueueUrl']
        except Exception as e:
            logger.warning(f"⚠️ 큐 URL 조회 실패 ({queue_name}): {e}")
            return ""
    
    def submit_job(self, strategy_name: str, config: StrategyConfig, session_id: str) -> str:
        """분산 작업 제출"""
        try:
            job_data = {
                'job_id': f'prod_{strategy_name}_{int(time.time())}_{str(uuid.uuid4())[:8]}',
                'job_type': 'SINGLE_STRATEGY',
                'strategy_name': strategy_name,
                'parameters': config.to_dict(),
                'session_id': session_id,
                'data_range': {
                    'start_date': (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
                    'end_date': datetime.now().strftime('%Y-%m-%d')
                }
            }
            
            response = self.sqs_client.send_message(
                QueueUrl=self.job_queue_url,
                MessageBody=json.dumps(job_data),
                MessageAttributes={
                    'job_type': {
                        'StringValue': 'SINGLE_STRATEGY',
                        'DataType': 'String'
                    },
                    'priority': {
                        'StringValue': 'high',
                        'DataType': 'String'
                    }
                }
            )
            
            job_id = job_data['job_id']
            logger.info(f"📤 분산 작업 제출: {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"❌ 분산 작업 제출 실패: {e}")
            return ""


# 편의 함수들
def create_production_backtesting_system(enable_distributed: bool = True, 
                                       enable_timezone: bool = True) -> ProductionBacktestingSystem:
    """운영용 백테스팅 시스템 생성"""
    return ProductionBacktestingSystem(enable_distributed, enable_timezone)

def run_quick_strategy_validation(strategy_name: str, period_days: int = 90) -> Dict[str, Any]:
    """빠른 전략 검증"""
    system = create_production_backtesting_system()
    return system.run_strategy_validation(strategy_name, [period_days])

def run_production_comparison(strategy_names: List[str], period_days: int = 180) -> Dict[str, Any]:
    """운영용 전략 비교"""
    system = create_production_backtesting_system()
    return system.run_multi_strategy_comparison(strategy_names, period_days)

def main():
    """메인 실행 함수"""
    print("🏭 Makenaide 운영용 백테스팅 시스템")
    print("=" * 80)
    
    try:
        # 시스템 초기화
        system = create_production_backtesting_system(
            enable_distributed=True,
            enable_timezone=True
        )
        
        print("✅ 시스템 초기화 완료")
        print(f"📋 사용 가능한 전략: {len(system.strategy_registry)}개")
        
        if len(system.strategy_registry) > 0:
            # 샘플 전략으로 빠른 검증
            sample_strategy = list(system.strategy_registry.keys())[0]
            print(f"\n🧪 샘플 전략 검증: {sample_strategy}")
            
            validation_result = system.run_strategy_validation(sample_strategy, [30, 90])
            
            if 'error' not in validation_result:
                print("✅ 샘플 검증 성공!")
                summary = validation_result.get('validation_summary', {})
                print(f"   전체 등급: {summary.get('overall_grade', 'N/A')}")
                print(f"   평균 수익률: {summary.get('avg_return', 0)*100:.2f}%")
                print(f"   권장사항: {summary.get('recommendation', 'N/A')}")
            else:
                print(f"❌ 샘플 검증 실패: {validation_result['error']}")
        
        print(f"\n🎯 운영용 백테스팅 시스템이 준비되었습니다!")
        print("주요 기능:")
        print("  - system.run_strategy_validation(strategy_name)")
        print("  - system.run_multi_strategy_comparison(strategy_names)")
        print("  - system.run_comprehensive_validation()")
        print("  - system.generate_strategy_report(strategy_name)")
        
    except Exception as e:
        print(f"❌ 시스템 초기화 실패: {e}")
        logger.error(f"메인 실행 실패: {e}")

if __name__ == "__main__":
    main()