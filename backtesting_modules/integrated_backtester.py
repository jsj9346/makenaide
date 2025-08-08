#!/usr/bin/env python3
"""
통합 백테스터 모듈

모든 백테스팅 기능을 통합한 메인 백테스터 클래스입니다.
기존 backtester.py의 ComprehensiveBacktestEngine과 MakenaideBacktestManager를 분리했습니다.

Author: Backtesting Refactoring
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple, Callable
from pathlib import Path
import time
import concurrent.futures
from threading import Lock

from .data_manager import BacktestDataManager
from .backtest_types import BacktestResult, BacktestSummary, StrategyConfig, create_default_strategy_configs
from .kelly_backtester import KellyBacktester
from .strategy_registry import StrategyRegistry
from .performance_analyzer import PerformanceAnalyzer
from .timezone_backtester import TimezoneBacktester, create_timezone_backtester

logger = logging.getLogger(__name__)

class IntegratedBacktester:
    """통합 백테스팅 엔진"""
    
    def __init__(self, enable_timezone_analysis: bool = True):
        self.data_manager = BacktestDataManager()
        self.strategy_registry = StrategyRegistry()
        self.performance_analyzer = PerformanceAnalyzer()
        
        # 시간대별 분석 설정
        self.enable_timezone_analysis = enable_timezone_analysis
        
        # 실행 상태 관리
        self._execution_lock = Lock()
        self._current_session = None
        self._results_cache = {}
        
        # 기본 설정
        self.default_config = {
            'initial_capital': 10_000_000,
            'max_concurrent_strategies': 5,
            'enable_optimization': True,
            'optimization_trials': 50,
            'risk_free_rate': 0.025,
            'timezone_analysis_enabled': enable_timezone_analysis
        }
        
        timezone_status = "활성화됨" if enable_timezone_analysis else "비활성화됨"
        logger.info(f"✅ 통합 백테스터 초기화 완료 (시간대 분석: {timezone_status})")
    
    def create_session(self, session_name: str, period_days: int = 365) -> Optional[str]:
        """새 백테스트 세션 생성"""
        try:
            with self._execution_lock:
                session_id = self.data_manager.create_backtest_snapshot(session_name, period_days)
                if session_id:
                    self._current_session = session_id
                    logger.info(f"📊 백테스트 세션 생성: {session_name} (ID: {session_id})")
                return session_id
                
        except Exception as e:
            logger.error(f"❌ 세션 생성 실패: {e}")
            return None
    
    def run_single_strategy_backtest(self, strategy_name: str, config: Optional[StrategyConfig] = None,
                                   session_id: Optional[str] = None) -> Optional[BacktestResult]:
        """단일 전략 백테스트 실행"""
        try:
            logger.info(f"🎯 단일 전략 백테스트 시작: {strategy_name}")
            
            # 전략 함수 가져오기
            strategy_func = self.strategy_registry.get_strategy(strategy_name)
            if not strategy_func:
                logger.error(f"❌ 전략 '{strategy_name}'을 찾을 수 없습니다")
                return None
            
            # 설정 준비
            if config is None:
                config = self._get_default_config_for_strategy(strategy_name)
            
            # 데이터 준비
            backtest_data = self.data_manager.get_backtest_data(session_id, limit_days=365)
            if backtest_data.empty:
                logger.error("❌ 백테스트 데이터가 없습니다")
                return None
            
            # 백테스트 실행
            start_time = time.time()
            
            # 시간대 분석 활성화 여부에 따른 백테스터 선택
            if self.enable_timezone_analysis:
                # 시간대별 백테스터 사용
                timezone_backtester = TimezoneBacktester(config, enable_timezone_analysis=True)
                timezone_result = timezone_backtester.backtest_with_timezone_analysis(
                    backtest_data, strategy_func, self.default_config['initial_capital']
                )
                
                # 시간대별 결과를 일반 결과 형태로 변환
                result = self._convert_timezone_result_to_standard(timezone_result, config)
                
            else:
                # 기본 켈리 백테스터 사용
                kelly_backtester = KellyBacktester(config)
                kelly_result = kelly_backtester.backtest_with_kelly(backtest_data, strategy_func)
                
                # 일반 백테스트 결과로 변환
                result = self._convert_kelly_result_to_standard(kelly_result, config)
            
            # 추가 성과 지표 계산
            if result.trades:
                additional_metrics = self.performance_analyzer.calculate_performance_metrics(result.trades)
                result.sharpe_ratio = additional_metrics.get('sharpe_ratio', 0)
                result.sortino_ratio = additional_metrics.get('sortino_ratio', 0)
                result.profit_factor = additional_metrics.get('profit_factor', 0)
                result.swing_score = additional_metrics.get('swing_score', 0)
            
            # 실행 시간 기록
            execution_time = time.time() - start_time
            
            # 결과 저장
            self.data_manager.save_backtest_results(result.to_dict(), session_id)
            
            # 전략 성과 이력 업데이트
            self.strategy_registry.update_performance_history(strategy_name, result)
            
            logger.info(f"✅ 단일 전략 백테스트 완료: {strategy_name} ({execution_time:.2f}초)")
            return result
            
        except Exception as e:
            logger.error(f"❌ 단일 전략 백테스트 실패 ({strategy_name}): {e}")
            return None
    
    def run_multiple_strategies_backtest(self, strategy_names: List[str], 
                                       configs: Optional[Dict[str, StrategyConfig]] = None,
                                       session_id: Optional[str] = None,
                                       max_workers: int = 3) -> Dict[str, BacktestResult]:
        """다중 전략 백테스트 실행 (병렬 처리)"""
        try:
            logger.info(f"🚀 다중 전략 백테스트 시작: {len(strategy_names)}개 전략")
            start_time = time.time()
            
            results = {}
            
            # 병렬 실행 또는 순차 실행 선택
            if max_workers > 1 and len(strategy_names) > 1:
                # 병렬 실행
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_strategy = {}
                    
                    for strategy_name in strategy_names:
                        config = configs.get(strategy_name) if configs else None
                        future = executor.submit(
                            self.run_single_strategy_backtest,
                            strategy_name, config, session_id
                        )
                        future_to_strategy[future] = strategy_name
                    
                    # 결과 수집
                    for future in concurrent.futures.as_completed(future_to_strategy):
                        strategy_name = future_to_strategy[future]
                        try:
                            result = future.result()
                            if result:
                                results[strategy_name] = result
                        except Exception as e:
                            logger.error(f"❌ 병렬 백테스트 오류 ({strategy_name}): {e}")
            else:
                # 순차 실행
                for strategy_name in strategy_names:
                    config = configs.get(strategy_name) if configs else None
                    result = self.run_single_strategy_backtest(strategy_name, config, session_id)
                    if result:
                        results[strategy_name] = result
            
            execution_time = time.time() - start_time
            logger.info(f"✅ 다중 전략 백테스트 완료: {len(results)}/{len(strategy_names)} 성공 ({execution_time:.2f}초)")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 다중 전략 백테스트 실패: {e}")
            return {}
    
    def run_comprehensive_backtest(self, period_days: int = 365, 
                                 strategy_filter: Optional[str] = None) -> BacktestSummary:
        """종합 백테스트 실행"""
        try:
            logger.info(f"🎯 종합 백테스트 시작 (기간: {period_days}일)")
            start_time = time.time()
            
            # 세션 생성
            session_name = f"comprehensive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            session_id = self.create_session(session_name, period_days)
            
            if not session_id:
                raise Exception("세션 생성 실패")
            
            # 전략 목록 준비
            available_strategies = self.strategy_registry.list_strategies()
            strategy_names = [s['name'] for s in available_strategies]
            
            if strategy_filter:
                strategy_names = [name for name in strategy_names if strategy_filter in name]
            
            # 기본 설정들 준비
            default_configs = create_default_strategy_configs()
            configs = {config.name: config for config in default_configs}
            
            # 다중 전략 백테스트 실행
            results = self.run_multiple_strategies_backtest(
                strategy_names, configs, session_id, max_workers=3
            )
            
            if not results:
                raise Exception("백테스트 결과가 없습니다")
            
            # 성과 분석
            result_list = list(results.values())
            comparison = self.performance_analyzer.compare_strategies(result_list)
            
            # 최고 성과 전략
            best_strategy_info = comparison.get('best_strategy')
            best_strategy_name = best_strategy_info if isinstance(best_strategy_info, str) else "N/A"
            
            best_result = None
            if best_strategy_name != "N/A" and best_strategy_name in results:
                best_result = results[best_strategy_name]
            
            # 평균 성과
            avg_return = np.mean([r.avg_return for r in result_list])
            avg_win_rate = np.mean([r.win_rate for r in result_list])
            avg_mdd = np.mean([r.mdd for r in result_list])
            avg_trades = np.mean([r.total_trades for r in result_list])
            
            # 전략별 순위 생성
            strategy_rankings = []
            if 'rankings' in comparison:
                for i, (strategy_name, data) in enumerate(comparison['rankings'], 1):
                    ranking_info = {
                        'rank': i,
                        'strategy_name': strategy_name,
                        'composite_score': data.get('composite_score', 0),
                        'metrics': data.get('result', {}).to_dict() if data.get('result') else {}
                    }
                    strategy_rankings.append(ranking_info)
            
            # 기간 정보
            end_date = date.today()
            start_date = end_date - timedelta(days=period_days)
            
            # 요약 결과 생성
            summary = BacktestSummary(
                session_name=session_name,
                period=(start_date, end_date),
                total_strategies=len(results),
                best_strategy=best_strategy_name,
                best_return=best_result.avg_return if best_result else 0,
                best_sharpe=best_result.sharpe_ratio if best_result else 0,
                best_kelly=best_result.kelly_fraction if best_result else 0,
                avg_return=avg_return,
                avg_win_rate=avg_win_rate,
                avg_mdd=avg_mdd,
                avg_trades=avg_trades,
                strategy_rankings=strategy_rankings,
                execution_time=time.time() - start_time,
                strategies_tested=len(results),
                optimization_applied=False,
                hybrid_filtering_enabled=False
            )
            
            logger.info(f"🎉 종합 백테스트 완료: 최고 전략 '{best_strategy_name}' ({summary.execution_time:.2f}초)")
            return summary
            
        except Exception as e:
            logger.error(f"❌ 종합 백테스트 실패: {e}")
            # 빈 요약 반환
            return BacktestSummary(
                session_name="failed_session",
                period=(date.today() - timedelta(days=30), date.today()),
                total_strategies=0,
                best_strategy="N/A",
                best_return=0,
                best_sharpe=0,
                best_kelly=0,
                avg_return=0,
                avg_win_rate=0,
                avg_mdd=0,
                avg_trades=0,
                strategy_rankings=[],
                execution_time=0,
                strategies_tested=0
            )
    
    def run_strategy_comparison(self, strategy_names: List[str], 
                              period_days: int = 180) -> Dict[str, Any]:
        """전략 비교 분석"""
        try:
            logger.info(f"📊 전략 비교 시작: {len(strategy_names)}개 전략")
            
            # 세션 생성
            session_name = f"comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            session_id = self.create_session(session_name, period_days)
            
            # 전략들 실행
            results = self.run_multiple_strategies_backtest(strategy_names, session_id=session_id)
            
            if not results:
                return {'error': '실행 가능한 전략이 없습니다'}
            
            # 비교 분석
            result_list = list(results.values())
            comparison = self.performance_analyzer.compare_strategies(result_list)
            risk_analysis = self.performance_analyzer.calculate_risk_adjusted_returns(result_list)
            
            # 결과 통합
            comparison_result = {
                'session_id': session_id,
                'strategies_compared': list(results.keys()),
                'comparison_analysis': comparison,
                'risk_analysis': risk_analysis,
                'best_strategy': comparison.get('best_strategy', 'N/A'),
                'execution_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"✅ 전략 비교 완료: 최고 전략 '{comparison.get('best_strategy', 'N/A')}'")
            return comparison_result
            
        except Exception as e:
            logger.error(f"❌ 전략 비교 실패: {e}")
            return {'error': str(e)}
    
    def optimize_strategy_parameters(self, strategy_name: str, 
                                   parameter_ranges: Dict[str, Tuple[float, float]],
                                   session_id: Optional[str] = None) -> Dict[str, Any]:
        """전략 파라미터 최적화"""
        try:
            logger.info(f"🔧 파라미터 최적화 시작: {strategy_name}")
            
            # 전략 함수 가져오기
            strategy_func = self.strategy_registry.get_strategy(strategy_name)
            if not strategy_func:
                return {'error': f"전략 '{strategy_name}'을 찾을 수 없습니다"}
            
            # 기본 설정 준비
            config = self._get_default_config_for_strategy(strategy_name)
            
            # 데이터 준비
            backtest_data = self.data_manager.get_backtest_data(session_id, limit_days=365)
            if backtest_data.empty:
                return {'error': '백테스트 데이터가 없습니다'}
            
            # 켈리 백테스터로 최적화 실행
            kelly_backtester = KellyBacktester(config)
            optimization_result = kelly_backtester.optimize_kelly_parameters(
                backtest_data, strategy_func, parameter_ranges
            )
            
            logger.info(f"✅ 파라미터 최적화 완료: {strategy_name}")
            return optimization_result
            
        except Exception as e:
            logger.error(f"❌ 파라미터 최적화 실패: {e}")
            return {'error': str(e)}
    
    def generate_report(self, session_id: Optional[str] = None, 
                       output_format: str = "markdown") -> str:
        """백테스트 리포트 생성"""
        try:
            # 결과 조회
            results_df = self.data_manager.get_backtest_results_from_db(session_id)
            
            if results_df.empty:
                return "⚠️ 생성할 리포트 데이터가 없습니다."
            
            # BacktestResult 객체들로 변환
            backtest_results = []
            for _, row in results_df.iterrows():
                result = BacktestResult(
                    strategy_name=row['strategy_name'],
                    win_rate=row.get('win_rate', 0),
                    avg_return=row.get('avg_return', 0),
                    mdd=row.get('mdd', 0),
                    total_trades=row.get('total_trades', 0),
                    sharpe_ratio=row.get('sharpe_ratio', 0),
                    kelly_fraction=row.get('kelly_fraction', 0)
                )
                backtest_results.append(result)
            
            # 성과 분석기로 리포트 생성
            return self.performance_analyzer.generate_performance_report(
                backtest_results, output_format
            )
            
        except Exception as e:
            logger.error(f"❌ 리포트 생성 실패: {e}")
            return f"❌ 리포트 생성 중 오류 발생: {str(e)}"
    
    def _get_default_config_for_strategy(self, strategy_name: str) -> StrategyConfig:
        """전략별 기본 설정 반환"""
        # 전략별 맞춤형 설정
        default_configs = {
            'Static_Donchian_Supertrend': StrategyConfig(
                name=strategy_name,
                position_size_method="percent",
                position_size_value=0.15,
                stop_loss_pct=0.07
            ),
            'Dynamic_RSI_MACD': StrategyConfig(
                name=strategy_name,
                position_size_method="kelly",
                position_size_value=0.25,
                stop_loss_pct=0.08,
                take_profit_pct=0.25
            ),
            'Hybrid_VCP_Momentum': StrategyConfig(
                name=strategy_name,
                position_size_method="percent",
                position_size_value=0.12,
                stop_loss_pct=0.075,
                take_profit_pct=0.30,
                max_positions=8
            ),
            'Simple_MA_Crossover': StrategyConfig(
                name=strategy_name,
                position_size_method="fixed",
                position_size_value=1_000_000,
                stop_loss_pct=0.10
            )
        }
        
        return default_configs.get(strategy_name, StrategyConfig(name=strategy_name))
    
    def cleanup_old_results(self, days_to_keep: int = 30) -> Dict[str, Any]:
        """오래된 백테스트 결과 정리"""
        try:
            cleanup_stats = self.data_manager.cleanup_old_backtest_results(days_to_keep)
            logger.info(f"🧹 백테스트 데이터 정리 완료")
            return cleanup_stats
            
        except Exception as e:
            logger.error(f"❌ 데이터 정리 실패: {e}")
            return {'error': str(e)}
    
    def get_session_summary(self, session_id: str) -> Optional[Dict[str, Any]]:
        """세션 요약 정보 조회"""
        try:
            session_info = self.data_manager.get_session_info(session_id)
            results_df = self.data_manager.get_backtest_results_from_db(session_id)
            
            if not session_info:
                return None
            
            summary = {
                'session_info': session_info,
                'total_strategies': len(results_df),
                'execution_summary': {}
            }
            
            if not results_df.empty:
                summary['execution_summary'] = {
                    'avg_return': results_df['avg_return'].mean(),
                    'avg_win_rate': results_df['win_rate'].mean(),
                    'best_strategy': results_df.loc[results_df['avg_return'].idxmax(), 'strategy_name'],
                    'best_return': results_df['avg_return'].max()
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 세션 요약 조회 실패: {e}")
            return None
    
    def get_available_strategies(self) -> List[Dict[str, Any]]:
        """사용 가능한 전략 목록 조회"""
        return self.strategy_registry.list_strategies()
    
    def get_strategy_performance_history(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        """전략 성과 이력 조회"""
        return self.strategy_registry.get_strategy_performance_summary(strategy_name)
    
    def run_timezone_enhanced_backtest(self, strategy_names: List[str], 
                                     period_days: int = 365) -> Dict[str, Any]:
        """시간대 강화 백테스트 실행"""
        try:
            logger.info(f"🌏 시간대 강화 백테스트 시작: {len(strategy_names)}개 전략")
            
            if not self.enable_timezone_analysis:
                logger.warning("⚠️ 시간대 분석이 비활성화되어 있습니다")
                return {'error': '시간대 분석이 비활성화됨'}
            
            # 세션 생성
            session_name = f"timezone_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            session_id = self.create_session(session_name, period_days)
            
            if not session_id:
                return {'error': '세션 생성 실패'}
            
            # 시간대별 백테스트 실행
            timezone_results = {}
            comparison_results = {}
            
            for strategy_name in strategy_names:
                strategy_func = self.strategy_registry.get_strategy(strategy_name)
                if not strategy_func:
                    continue
                
                config = self._get_default_config_for_strategy(strategy_name)
                backtest_data = self.data_manager.get_backtest_data(session_id, limit_days=period_days)
                
                if backtest_data.empty:
                    continue
                
                # 시간대별 백테스터 생성 및 실행
                timezone_backtester = TimezoneBacktester(config, enable_timezone_analysis=True)
                
                # 시간대별 vs 표준 백테스트 비교
                comparison = timezone_backtester.compare_timezone_vs_standard(backtest_data, strategy_func)
                comparison_results[strategy_name] = comparison
                
                # 시간대별 백테스트 결과
                timezone_result = timezone_backtester.backtest_with_timezone_analysis(
                    backtest_data, strategy_func, self.default_config['initial_capital']
                )
                timezone_results[strategy_name] = timezone_result
            
            # 전체 분석 결과 통합
            integrated_analysis = {
                'session_id': session_id,
                'strategy_count': len(timezone_results),
                'timezone_results': timezone_results,
                'comparison_analysis': comparison_results,
                'global_insights': self._generate_global_timezone_insights(timezone_results),
                'recommendations': self._generate_timezone_recommendations(comparison_results),
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"✅ 시간대 강화 백테스트 완료: {len(timezone_results)}개 전략")
            return integrated_analysis
            
        except Exception as e:
            logger.error(f"❌ 시간대 강화 백테스트 실패: {e}") 
            return {'error': str(e)}
    
    def generate_timezone_reports(self, timezone_results: Dict[str, Any], 
                                output_dir: str = "timezone_reports") -> Dict[str, str]:
        """시간대별 백테스트 결과 리포트 생성"""
        try:
            from timezone_report_generator import TimezoneReportGenerator
            import os
            
            # 출력 디렉토리 생성
            os.makedirs(output_dir, exist_ok=True)
            
            report_generator = TimezoneReportGenerator()
            generated_reports = {}
            
            # 전략별 리포트 생성
            for strategy_name, result in timezone_results.get('timezone_results', {}).items():
                if 'error' in result:
                    logger.warning(f"⚠️ {strategy_name} 결과에 오류 있음, 리포트 생략")
                    continue
                
                # 리포트 파일명
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                report_filename = f"{output_dir}/{strategy_name}_timezone_report_{timestamp}.html"
                
                # 리포트 생성
                report_path = report_generator.generate_comprehensive_report(
                    result, report_filename
                )
                
                if report_path:
                    generated_reports[strategy_name] = report_path
                    logger.info(f"📊 {strategy_name} 시간대 리포트 생성: {report_path}")
                else:
                    logger.warning(f"⚠️ {strategy_name} 리포트 생성 실패")
            
            # 통합 요약 리포트 생성
            if len(generated_reports) > 0:
                summary_report = self._generate_timezone_summary_report(
                    timezone_results, f"{output_dir}/timezone_summary_{timestamp}.html"
                )
                if summary_report:
                    generated_reports['summary'] = summary_report
            
            logger.info(f"✅ 시간대별 리포트 생성 완료: {len(generated_reports)}개 파일")
            return generated_reports
            
        except ImportError:
            logger.error("❌ timezone_report_generator 모듈을 찾을 수 없습니다")
            return {}
        except Exception as e:
            logger.error(f"❌ 시간대별 리포트 생성 실패: {e}")
            return {}
    
    def _generate_timezone_summary_report(self, timezone_results: Dict[str, Any], 
                                        output_path: str) -> Optional[str]:
        """시간대별 백테스트 통합 요약 리포트 생성"""
        try:
            # 글로벌 인사이트 추출
            global_insights = timezone_results.get('global_insights', {})
            comparison_analysis = timezone_results.get('comparison_analysis', {})
            recommendations = timezone_results.get('recommendations', [])
            
            # HTML 리포트 생성
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>시간대별 백테스트 통합 요약 리포트</title>
                <style>
                    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #f5f5f5; }}
                    .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                    .header {{ text-align: center; margin-bottom: 30px; color: #2c3e50; }}
                    .section {{ margin: 20px 0; padding: 15px; border-left: 4px solid #3498db; background: #f8f9fa; }}
                    .metric {{ display: inline-block; margin: 10px; padding: 15px; background: #fff; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }}
                    .metric-label {{ font-size: 12px; color: #7f8c8d; margin-bottom: 5px; }}
                    .metric-value {{ font-size: 24px; font-weight: bold; color: #2c3e50; }}
                    .recommendations {{ background: #e8f5e8; border-left-color: #27ae60; }}
                    .insights {{ background: #fff3cd; border-left-color: #ffc107; }}
                    ul {{ list-style-type: none; padding: 0; }}
                    li {{ padding: 8px 0; border-bottom: 1px solid #eee; }}
                    .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #95a5a6; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>🌏 시간대별 백테스트 통합 요약</h1>
                        <p>생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    </div>
            
                    <div class="section">
                        <h2>📊 전체 분석 개요</h2>
                        <div class="metric">
                            <div class="metric-label">분석 전략 수</div>
                            <div class="metric-value">{timezone_results.get('strategy_count', 0)}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">개선된 전략</div>
                            <div class="metric-value">{sum(1 for c in comparison_analysis.values() if c.get('improvement_pct', 0) > 5)}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">평균 개선율</div>
                            <div class="metric-value">{np.mean([c.get('improvement_pct', 0) for c in comparison_analysis.values()]) if comparison_analysis else 0:.1f}%</div>
                        </div>
                    </div>
            
                    <div class="section insights">
                        <h2>💡 글로벌 인사이트</h2>
                        <h3>최고 성과 시간대 (KST)</h3>
                        <ul>
            """
            
            # 최고 성과 시간대 추가
            best_hours = global_insights.get('best_global_hours', [])
            for hour_info in best_hours[:5]:
                html_content += f"<li>{hour_info.get('hour', 'N/A')}: 평균 {hour_info.get('avg_return', 0)*100:.2f}%</li>"
            
            html_content += """
                        </ul>
                        <h3>최고 성과 지역</h3>
                        <ul>
            """
            
            # 최고 성과 지역 추가
            best_regions = global_insights.get('best_regions', [])
            for region_info in best_regions[:3]:
                html_content += f"<li>{region_info.get('region', 'N/A')}: 평균 {region_info.get('avg_return', 0)*100:.2f}%</li>"
            
            html_content += f"""
                        </ul>
                    </div>
            
                    <div class="section recommendations">
                        <h2>🎯 권장사항</h2>
                        <ul>
            """
            
            # 권장사항 추가
            for rec in recommendations:
                html_content += f"<li>{rec}</li>"
            
            html_content += f"""
                        </ul>
                    </div>
            
                    <div class="footer">
                        <p>Makenaide 시간대별 백테스팅 시스템 | 세션 ID: {timezone_results.get('session_id', 'N/A')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            # 파일 저장
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"📊 통합 요약 리포트 생성: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"❌ 통합 요약 리포트 생성 실패: {e}")
            return None
    
    def _convert_timezone_result_to_standard(self, timezone_result: Dict[str, Any], 
                                           config: StrategyConfig) -> BacktestResult:
        """시간대별 백테스트 결과를 표준 BacktestResult로 변환"""
        if 'error' in timezone_result:
            # 오류 발생시 빈 결과 반환
            return BacktestResult(
                strategy_name=config.name,
                win_rate=0,
                avg_return=0,
                mdd=0,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                kelly_fraction=0,
                parameters=config.to_dict(),
                trades=[]
            )
        
        trades = timezone_result.get('trades', [])
        basic_metrics = timezone_result.get('basic_metrics', {})
        
        if trades:
            returns = [t['return_pct'] for t in trades]
            win_trades = [r for r in returns if r > 0]
            
            win_rate = len(win_trades) / len(trades) if trades else 0
            avg_return = np.mean(returns) if returns else 0
            
            # 최대 낙폭 계산 (간소화)
            cumulative_returns = np.cumsum(returns)
            running_max = np.maximum.accumulate(cumulative_returns)
            drawdowns = running_max - cumulative_returns
            max_drawdown = np.max(drawdowns) if len(drawdowns) > 0 else 0
            
            # 켈리 지표 추출
            timezone_kelly = timezone_result.get('timezone_kelly_analysis', {})
            overall_kelly = timezone_kelly.get('overall_kelly', {})
            kelly_fraction = overall_kelly.get('kelly_fraction', 0) if isinstance(overall_kelly, dict) else 0
            
        else:
            win_rate = avg_return = max_drawdown = kelly_fraction = 0
            win_trades = []
        
        return BacktestResult(
            strategy_name=config.name,
            win_rate=win_rate,
            avg_return=avg_return,
            mdd=max_drawdown,
            total_trades=len(trades),
            winning_trades=len(win_trades),
            losing_trades=len(trades) - len(win_trades),
            kelly_fraction=kelly_fraction,
            parameters=config.to_dict(),
            trades=trades,
            # 시간대별 추가 정보
            timezone_enhanced=True,
            timezone_analysis=timezone_result
        )
    
    def _convert_kelly_result_to_standard(self, kelly_result, config: StrategyConfig) -> BacktestResult:
        """켈리 백테스트 결과를 표준 BacktestResult로 변환"""
        return BacktestResult(
            strategy_name=config.name,
            period_start=kelly_result.period[0],
            period_end=kelly_result.period[1],
            win_rate=kelly_result.win_rate,
            avg_return=kelly_result.avg_win - abs(kelly_result.avg_loss),  # 근사치
            mdd=kelly_result.max_drawdown,
            total_trades=kelly_result.total_trades,
            winning_trades=kelly_result.winning_trades,
            losing_trades=kelly_result.losing_trades,
            kelly_fraction=kelly_result.kelly_fraction,
            kelly_1_2=kelly_result.kelly_1_2,
            b_value=kelly_result.b_value,
            parameters=config.to_dict(),
            trades=kelly_result.trades_detail,
            timezone_enhanced=False
        )
    
    def _generate_global_timezone_insights(self, timezone_results: Dict[str, Dict]) -> Dict[str, Any]:
        """글로벌 시간대 인사이트 생성"""
        insights = {
            'best_global_hours': {},
            'best_regions': {},
            'market_condition_analysis': {},
            'activity_correlation_summary': {}
        }
        
        try:
            all_hourly_data = {}
            all_region_data = {}
            
            # 모든 전략의 시간대별 성과 집계
            for strategy_name, result in timezone_results.items():
                if 'timezone_performance' not in result:
                    continue
                
                # 시간별 성과 집계
                hourly_perf = result['timezone_performance'].get('hourly_performance', {})
                for hour, metrics in hourly_perf.items():
                    if hour not in all_hourly_data:
                        all_hourly_data[hour] = []
                    all_hourly_data[hour].append(metrics.get('avg_return', 0))
                
                # 지역별 성과 집계
                region_perf = result['timezone_performance'].get('region_performance', {})
                for region, metrics in region_perf.items():
                    if region not in all_region_data:
                        all_region_data[region] = []
                    all_region_data[region].append(metrics.get('avg_return', 0))
            
            # 최고 성과 시간대 (평균)
            if all_hourly_data:
                hour_averages = {h: np.mean(returns) for h, returns in all_hourly_data.items()}
                best_hours = sorted(hour_averages.items(), key=lambda x: x[1], reverse=True)[:5]
                insights['best_global_hours'] = [
                    {'hour': f"{h}:00", 'avg_return': round(ret, 4)}
                    for h, ret in best_hours
                ]
            
            # 최고 성과 지역 (평균)
            if all_region_data:
                region_averages = {r: np.mean(returns) for r, returns in all_region_data.items()}
                best_regions = sorted(region_averages.items(), key=lambda x: x[1], reverse=True)
                insights['best_regions'] = [
                    {'region': r, 'avg_return': round(ret, 4)}
                    for r, ret in best_regions
                ]
            
        except Exception as e:
            logger.error(f"❌ 글로벌 인사이트 생성 실패: {e}")
            
        return insights
    
    def _generate_timezone_recommendations(self, comparison_results: Dict[str, Dict]) -> List[str]:
        """시간대별 백테스트 권장사항 생성"""
        recommendations = []
        
        try:
            improvement_count = 0
            total_strategies = len(comparison_results)
            
            for strategy_name, comparison in comparison_results.items():
                if 'improvement_pct' in comparison and comparison['improvement_pct'] > 5:
                    improvement_count += 1
            
            improvement_ratio = improvement_count / total_strategies if total_strategies > 0 else 0
            
            if improvement_ratio >= 0.7:
                recommendations.append("🏆 대부분의 전략에서 시간대 분석이 성과를 향상시켰습니다 (70% 이상)")
                recommendations.append("📊 시간대 분석을 모든 전략에 적용하는 것을 강력히 권장합니다")
            elif improvement_ratio >= 0.5:
                recommendations.append("⚡ 절반 이상의 전략에서 시간대 분석이 효과적입니다")
                recommendations.append("🎯 개선된 전략들에만 선택적으로 적용을 고려하세요")
            else:
                recommendations.append("⚠️ 시간대 분석의 효과가 제한적입니다")
                recommendations.append("🔍 다른 최적화 방법을 우선 고려하는 것을 권장합니다")
            
            # 구체적인 활용 권장사항
            recommendations.append("🕐 아시아 시간대(09:00-21:00 KST) 집중 거래 고려")
            recommendations.append("🌏 글로벌 활성도 60% 이상 시간대에서 포지션 크기 증대")
            recommendations.append("📈 시장 상황별 차별화된 손절/익절 전략 적용")
            
        except Exception as e:
            logger.error(f"❌ 권장사항 생성 실패: {e}")
            recommendations = ["시간대 분석 권장사항 생성 중 오류가 발생했습니다"]
        
        return recommendations


# 편의 함수들
def create_integrated_backtester(enable_timezone_analysis: bool = True) -> IntegratedBacktester:
    """통합 백테스터 인스턴스 생성"""
    return IntegratedBacktester(enable_timezone_analysis)

def create_timezone_enhanced_backtester() -> IntegratedBacktester:
    """시간대 강화 백테스터 인스턴스 생성"""
    return IntegratedBacktester(enable_timezone_analysis=True)

def create_standard_backtester() -> IntegratedBacktester:
    """표준 백테스터 인스턴스 생성 (시간대 분석 비활성화)"""
    return IntegratedBacktester(enable_timezone_analysis=False)

def run_quick_backtest(strategy_names: Optional[List[str]] = None, 
                      period_days: int = 90) -> BacktestSummary:
    """빠른 백테스트 실행 (편의 함수)"""
    backtester = create_integrated_backtester()
    
    if strategy_names:
        # 지정된 전략들만 실행
        results = backtester.run_multiple_strategies_backtest(strategy_names)
        # 간단한 요약 생성
        if results:
            result_list = list(results.values())
            best_result = max(result_list, key=lambda x: x.get_composite_score())
            
            return BacktestSummary(
                session_name=f"quick_{datetime.now().strftime('%H%M%S')}",
                period=(date.today() - timedelta(days=period_days), date.today()),
                total_strategies=len(results),
                best_strategy=best_result.strategy_name,
                best_return=best_result.avg_return,
                best_sharpe=best_result.sharpe_ratio,
                best_kelly=best_result.kelly_fraction,
                avg_return=np.mean([r.avg_return for r in result_list]),
                avg_win_rate=np.mean([r.win_rate for r in result_list]),
                avg_mdd=np.mean([r.mdd for r in result_list]),
                avg_trades=np.mean([r.total_trades for r in result_list]),
                strategy_rankings=[],
                execution_time=0,
                strategies_tested=len(results)
            )
    else:
        # 전체 종합 백테스트 실행
        return backtester.run_comprehensive_backtest(period_days)