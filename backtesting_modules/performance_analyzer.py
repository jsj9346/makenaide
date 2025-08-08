#!/usr/bin/env python3
"""
성과 분석 모듈

백테스트 결과의 성과 지표 계산, 분석, 시각화 기능을 제공합니다.
기존 backtester.py의 PerformanceAnalyzer 클래스를 분리했습니다.

Author: Backtesting Refactoring
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import scipy.stats as stats
from scipy.optimize import minimize
import json

from .backtest_types import BacktestResult, BacktestSummary, StrategyConfig

logger = logging.getLogger(__name__)

class PerformanceAnalyzer:
    """백테스트 성과 분석 클래스"""
    
    def __init__(self):
        self.risk_free_rate = 0.025  # 연간 무위험 수익률 2.5%
        
    def calculate_performance_metrics(self, trades: List[Dict], 
                                    initial_capital: float = 10_000_000) -> Dict[str, float]:
        """
        거래 기록으로부터 성과 지표 계산
        
        Args:
            trades: 거래 기록 리스트
            initial_capital: 초기 자본금
            
        Returns:
            Dict: 성과 지표들
        """
        if not trades:
            return self._get_empty_metrics()
        
        try:
            # 기본 통계
            total_trades = len(trades)
            returns = [trade.get('return_pct', 0.0) for trade in trades]
            winning_returns = [r for r in returns if r > 0]
            losing_returns = [r for r in returns if r < 0]
            
            winning_trades = len(winning_returns)
            losing_trades = len(losing_returns)
            win_rate = winning_trades / total_trades if total_trades > 0 else 0.0
            
            # 수익률 통계
            avg_return = np.mean(returns) if returns else 0.0
            total_return = np.prod([1 + r for r in returns]) - 1 if returns else 0.0
            
            avg_win = np.mean(winning_returns) if winning_returns else 0.0
            avg_loss = np.mean(losing_returns) if losing_returns else 0.0
            
            # 리스크 지표
            volatility = np.std(returns) if len(returns) > 1 else 0.0
            max_drawdown = self._calculate_max_drawdown(returns)
            
            # 샤프 비율
            excess_returns = [r - (self.risk_free_rate / 252) for r in returns]  # 일일 무위험 수익률
            sharpe_ratio = (np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)) if np.std(excess_returns) > 0 else 0.0
            
            # 소르티노 비율
            downside_returns = [r for r in returns if r < 0]
            downside_deviation = np.std(downside_returns) if downside_returns else 0.0001
            sortino_ratio = (avg_return - self.risk_free_rate / 252) / downside_deviation * np.sqrt(252) if downside_deviation > 0 else 0.0
            
            # 수익 팩터
            total_gains = sum(winning_returns) if winning_returns else 0.0
            total_losses = abs(sum(losing_returns)) if losing_returns else 0.0001
            profit_factor = total_gains / total_losses if total_losses > 0 else 0.0
            
            # 켈리 공식 관련
            kelly_metrics = self._calculate_kelly_metrics(returns)
            
            # 스윙 스코어 (수익률 대비 변동성)
            swing_score = abs(avg_return) / (volatility + 0.0001) if volatility > 0 else 0.0
            
            # VaR (Value at Risk)
            var_95 = np.percentile(returns, 5) if returns else 0.0
            
            # 거래 지속성 지표
            hold_days = [trade.get('hold_days', 0) for trade in trades if trade.get('hold_days')]
            avg_hold_days = np.mean(hold_days) if hold_days else 0.0
            
            metrics = {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'avg_return': avg_return,
                'total_return': total_return,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'volatility': volatility,
                'max_drawdown': max_drawdown,
                'sharpe_ratio': sharpe_ratio,
                'sortino_ratio': sortino_ratio,
                'profit_factor': profit_factor,
                'kelly_fraction': kelly_metrics['kelly_fraction'],
                'kelly_1_2': kelly_metrics['kelly_1_2'],
                'b_value': kelly_metrics['b_value'],
                'swing_score': swing_score,
                'var_95': var_95,
                'avg_hold_days': avg_hold_days,
                'calmar_ratio': abs(total_return / max_drawdown) if max_drawdown != 0 else 0.0,
                'information_ratio': sharpe_ratio,  # 간소화
                'recovery_factor': abs(total_return / max_drawdown) if max_drawdown != 0 else 0.0
            }
            
            logger.info(f"📊 성과 지표 계산 완료: 총 {len(metrics)}개 지표")
            return metrics
            
        except Exception as e:
            logger.error(f"❌ 성과 지표 계산 실패: {e}")
            return self._get_empty_metrics()
    
    def _get_empty_metrics(self) -> Dict[str, float]:
        """빈 성과 지표 반환"""
        return {
            'total_trades': 0, 'winning_trades': 0, 'losing_trades': 0,
            'win_rate': 0.0, 'avg_return': 0.0, 'total_return': 0.0,
            'avg_win': 0.0, 'avg_loss': 0.0, 'volatility': 0.0,
            'max_drawdown': 0.0, 'sharpe_ratio': 0.0, 'sortino_ratio': 0.0,
            'profit_factor': 0.0, 'kelly_fraction': 0.0, 'kelly_1_2': 0.0,
            'b_value': 0.0, 'swing_score': 0.0, 'var_95': 0.0,
            'avg_hold_days': 0.0, 'calmar_ratio': 0.0, 'information_ratio': 0.0,
            'recovery_factor': 0.0
        }
    
    def _calculate_max_drawdown(self, returns: List[float]) -> float:
        """최대 낙폭(MDD) 계산"""
        if not returns:
            return 0.0
        
        cumulative = np.cumprod([1 + r for r in returns])
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = (cumulative - running_max) / running_max
        
        return np.min(drawdowns) if len(drawdowns) > 0 else 0.0
    
    def _calculate_kelly_metrics(self, returns: List[float]) -> Dict[str, float]:
        """켈리 공식 관련 지표 계산"""
        if len(returns) < 5:
            return {'kelly_fraction': 0.0, 'kelly_1_2': 0.0, 'b_value': 1.0}
        
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]
        
        if not wins or not losses:
            return {'kelly_fraction': 0.0, 'kelly_1_2': 0.0, 'b_value': 1.0}
        
        p = len(wins) / len(returns)  # 승률
        q = 1 - p  # 패률
        avg_win = np.mean(wins)
        avg_loss = abs(np.mean(losses))
        b = avg_win / avg_loss if avg_loss > 0 else 1.0
        
        # 켈리 공식: f* = (bp - q) / b
        kelly_fraction = (b * p - q) / b if b > 0 else 0.0
        kelly_fraction = max(-0.1, min(0.5, kelly_fraction))  # 안전 범위
        
        return {
            'kelly_fraction': kelly_fraction,
            'kelly_1_2': kelly_fraction * 0.5,
            'b_value': b
        }
    
    def compare_strategies(self, results: List[BacktestResult]) -> Dict[str, Any]:
        """
        전략들 성과 비교 분석
        
        Args:
            results: 백테스트 결과 리스트
            
        Returns:
            Dict: 비교 분석 결과
        """
        if not results:
            return {'error': '비교할 결과가 없습니다'}
        
        try:
            # 성과 지표별 순위 계산
            metrics_comparison = {}
            strategy_names = [r.strategy_name for r in results]
            
            # 주요 지표들
            key_metrics = [
                'avg_return', 'win_rate', 'sharpe_ratio', 'kelly_fraction', 
                'max_drawdown', 'profit_factor', 'total_trades'
            ]
            
            for metric in key_metrics:
                values = [getattr(r, metric, 0) for r in results]
                
                # MDD는 낮을수록 좋음 (절댓값)
                if metric == 'max_drawdown':
                    sorted_indices = np.argsort([abs(v) for v in values])
                else:
                    sorted_indices = np.argsort(values)[::-1]  # 내림차순
                
                rankings = {}
                for rank, idx in enumerate(sorted_indices, 1):
                    rankings[strategy_names[idx]] = {
                        'rank': rank,
                        'value': values[idx]
                    }
                
                metrics_comparison[metric] = rankings
            
            # 종합 점수 계산
            composite_scores = {}
            for i, result in enumerate(results):
                score = result.get_composite_score()
                composite_scores[result.strategy_name] = {
                    'composite_score': score,
                    'result': result
                }
            
            # 종합 순위
            sorted_strategies = sorted(
                composite_scores.items(), 
                key=lambda x: x[1]['composite_score'], 
                reverse=True
            )
            
            # 최고 성과 전략
            best_strategy = sorted_strategies[0] if sorted_strategies else None
            
            # 통계 요약
            avg_metrics = {}
            for metric in key_metrics:
                values = [getattr(r, metric, 0) for r in results]
                avg_metrics[metric] = {
                    'mean': np.mean(values),
                    'std': np.std(values),
                    'min': np.min(values),
                    'max': np.max(values)
                }
            
            comparison_result = {
                'total_strategies': len(results),
                'best_strategy': best_strategy[0] if best_strategy else None,
                'best_composite_score': best_strategy[1]['composite_score'] if best_strategy else 0,
                'rankings': sorted_strategies,
                'metrics_comparison': metrics_comparison,
                'average_metrics': avg_metrics,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"📊 전략 비교 완료: {len(results)}개 전략")
            return comparison_result
            
        except Exception as e:
            logger.error(f"❌ 전략 비교 분석 실패: {e}")
            return {'error': str(e)}
    
    def analyze_strategy_consistency(self, strategy_results: List[BacktestResult]) -> Dict[str, Any]:
        """
        특정 전략의 일관성 분석 (여러 기간 결과)
        
        Args:
            strategy_results: 동일 전략의 여러 백테스트 결과
            
        Returns:
            Dict: 일관성 분석 결과
        """
        if len(strategy_results) < 3:
            return {'error': '일관성 분석을 위해서는 최소 3개의 결과가 필요합니다'}
        
        try:
            strategy_name = strategy_results[0].strategy_name
            
            # 지표별 시계열 데이터 추출
            metrics_series = {
                'avg_return': [r.avg_return for r in strategy_results],
                'win_rate': [r.win_rate for r in strategy_results], 
                'sharpe_ratio': [r.sharpe_ratio for r in strategy_results],
                'max_drawdown': [r.mdd for r in strategy_results],
                'total_trades': [r.total_trades for r in strategy_results]
            }
            
            # 일관성 지표 계산
            consistency_metrics = {}
            for metric, values in metrics_series.items():
                if len(values) > 1:
                    mean_val = np.mean(values)
                    std_val = np.std(values)
                    cv = abs(std_val / (mean_val + 0.0001))  # 변동계수
                    
                    consistency_metrics[metric] = {
                        'mean': mean_val,
                        'std': std_val,
                        'coefficient_of_variation': cv,
                        'min': np.min(values),
                        'max': np.max(values),
                        'consistency_score': max(0, 1 - cv)  # 0-1, 높을수록 일관적
                    }
            
            # 종합 일관성 점수
            consistency_scores = [m['consistency_score'] for m in consistency_metrics.values()]
            overall_consistency = np.mean(consistency_scores) if consistency_scores else 0
            
            # 트렌드 분석
            trend_analysis = {}
            for metric, values in metrics_series.items():
                if len(values) >= 3:
                    # 간단한 선형 추세
                    x = np.arange(len(values))
                    trend_slope = np.polyfit(x, values, 1)[0]
                    
                    trend_analysis[metric] = {
                        'trend_slope': trend_slope,
                        'trend_direction': 'improving' if trend_slope > 0.01 else 
                                         'declining' if trend_slope < -0.01 else 'stable'
                    }
            
            # 성과 구간 분석
            returns = [r.avg_return for r in strategy_results]
            positive_periods = len([r for r in returns if r > 0])
            negative_periods = len([r for r in returns if r < 0])
            
            result = {
                'strategy_name': strategy_name,
                'analysis_periods': len(strategy_results),
                'overall_consistency_score': overall_consistency,
                'consistency_metrics': consistency_metrics,
                'trend_analysis': trend_analysis,
                'performance_stability': {
                    'positive_periods': positive_periods,
                    'negative_periods': negative_periods,
                    'positive_ratio': positive_periods / len(strategy_results)
                },
                'recommendation': self._generate_consistency_recommendation(overall_consistency, trend_analysis)
            }
            
            logger.info(f"📈 일관성 분석 완료: {strategy_name} (점수: {overall_consistency:.3f})")
            return result
            
        except Exception as e:
            logger.error(f"❌ 일관성 분석 실패: {e}")
            return {'error': str(e)}
    
    def _generate_consistency_recommendation(self, consistency_score: float, 
                                           trend_analysis: Dict) -> str:
        """일관성 분석 기반 추천"""
        recommendations = []
        
        if consistency_score > 0.7:
            recommendations.append("✅ 높은 일관성을 보이는 안정적인 전략입니다")
        elif consistency_score > 0.5:
            recommendations.append("⚠️ 보통 수준의 일관성을 보입니다")
        else:
            recommendations.append("❌ 일관성이 부족하여 추가 검토가 필요합니다")
        
        # 트렌드 기반 추천
        improving_trends = sum(1 for t in trend_analysis.values() 
                             if t.get('trend_direction') == 'improving')
        declining_trends = sum(1 for t in trend_analysis.values() 
                             if t.get('trend_direction') == 'declining')
        
        if improving_trends > declining_trends:
            recommendations.append("📈 성과가 개선되는 추세입니다")
        elif declining_trends > improving_trends:
            recommendations.append("📉 성과가 하락하는 추세로 주의가 필요합니다")
        
        return " | ".join(recommendations)
    
    def calculate_risk_adjusted_returns(self, results: List[BacktestResult]) -> Dict[str, Any]:
        """위험 조정 수익률 계산"""
        try:
            risk_adjusted_analysis = {}
            
            for result in results:
                # 위험 조정 지표들
                sharpe = result.sharpe_ratio
                sortino = result.sortino_ratio if hasattr(result, 'sortino_ratio') else 0
                calmar = abs(result.avg_return / result.mdd) if result.mdd != 0 else 0
                
                # 종합 위험 조정 점수
                risk_score = (sharpe * 0.4 + sortino * 0.3 + calmar * 0.3)
                
                risk_adjusted_analysis[result.strategy_name] = {
                    'sharpe_ratio': sharpe,
                    'sortino_ratio': sortino,
                    'calmar_ratio': calmar,
                    'risk_adjusted_score': risk_score,
                    'volatility': getattr(result, 'volatility', 0),
                    'max_drawdown': result.mdd
                }
            
            # 위험 조정 수익률 순위
            sorted_by_risk_adjusted = sorted(
                risk_adjusted_analysis.items(),
                key=lambda x: x[1]['risk_adjusted_score'],
                reverse=True
            )
            
            return {
                'risk_adjusted_rankings': sorted_by_risk_adjusted,
                'best_risk_adjusted': sorted_by_risk_adjusted[0] if sorted_by_risk_adjusted else None,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 위험 조정 수익률 계산 실패: {e}")
            return {'error': str(e)}
    
    def generate_performance_report(self, results: List[BacktestResult], 
                                  output_format: str = "markdown") -> str:
        """성과 분석 리포트 생성"""
        try:
            if not results:
                return "⚠️ 분석할 백테스트 결과가 없습니다."
            
            # 비교 분석 실행
            comparison = self.compare_strategies(results)
            risk_analysis = self.calculate_risk_adjusted_returns(results)
            
            if output_format == "markdown":
                return self._generate_markdown_report(comparison, risk_analysis)
            elif output_format == "json":
                return self._generate_json_report(comparison, risk_analysis)
            else:
                return "지원하지 않는 형식입니다"
                
        except Exception as e:
            logger.error(f"❌ 성과 리포트 생성 실패: {e}")
            return f"❌ 리포트 생성 중 오류 발생: {str(e)}"
    
    def _generate_markdown_report(self, comparison: Dict, risk_analysis: Dict) -> str:
        """마크다운 형식 리포트 생성"""
        report = f"""# 백테스트 성과 분석 리포트

## 📊 전체 요약
- **분석 전략 수**: {comparison.get('total_strategies', 0)}개
- **최고 성과 전략**: {comparison.get('best_strategy', 'N/A')}
- **최고 종합 점수**: {comparison.get('best_composite_score', 0):.3f}

## 🏆 전략 순위

| 순위 | 전략명 | 종합점수 | 수익률 | 승률 | 샤프비율 | 최대낙폭 |
|------|--------|----------|--------|------|----------|----------|
"""
        
        # 순위 테이블 생성
        rankings = comparison.get('rankings', [])
        for i, (strategy_name, data) in enumerate(rankings[:10], 1):  # 상위 10개
            result = data.get('result')
            if result:
                report += f"| {i} | {strategy_name} | {data['composite_score']:.3f} | {result.avg_return:.2%} | {result.win_rate:.2%} | {result.sharpe_ratio:.2f} | {result.mdd:.2%} |\n"
        
        # 위험 조정 수익률 분석
        if 'risk_adjusted_rankings' in risk_analysis:
            report += "\n## ⚖️ 위험 조정 수익률 순위\n\n"
            report += "| 순위 | 전략명 | 위험조정점수 | 샤프비율 | 소르티노비율 | 칼마비율 |\n"
            report += "|------|--------|--------------|----------|--------------|----------|\n"
            
            for i, (strategy_name, data) in enumerate(risk_analysis['risk_adjusted_rankings'][:5], 1):
                report += f"| {i} | {strategy_name} | {data['risk_adjusted_score']:.3f} | {data['sharpe_ratio']:.2f} | {data['sortino_ratio']:.2f} | {data['calmar_ratio']:.2f} |\n"
        
        # 평균 지표
        avg_metrics = comparison.get('average_metrics', {})
        if avg_metrics:
            report += "\n## 📈 평균 성과 지표\n\n"
            for metric, stats in avg_metrics.items():
                report += f"- **{metric}**: 평균 {stats['mean']:.3f}, 표준편차 {stats['std']:.3f}\n"
        
        report += f"\n---\n생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        return report
    
    def _generate_json_report(self, comparison: Dict, risk_analysis: Dict) -> str:
        """JSON 형식 리포트 생성"""
        report_data = {
            'summary': {
                'total_strategies': comparison.get('total_strategies', 0),
                'best_strategy': comparison.get('best_strategy'),
                'best_composite_score': comparison.get('best_composite_score', 0),
                'generated_at': datetime.now().isoformat()
            },
            'strategy_comparison': comparison,
            'risk_analysis': risk_analysis
        }
        
        return json.dumps(report_data, indent=2, ensure_ascii=False, default=str)