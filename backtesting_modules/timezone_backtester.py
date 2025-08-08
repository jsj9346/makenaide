#!/usr/bin/env python3
"""
시간대별 백테스터 모듈

시간대별 시장 분석을 백테스팅에 통합한 고도화된 백테스터입니다.
TimezoneMarketAnalyzer를 활용하여 시간대별 전략 조정을 적용합니다.

Author: Timezone Backtesting Integration
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Callable
import pytz
from pathlib import Path
import json

from .backtest_types import BacktestResult, StrategyConfig, BacktestSummary
from .kelly_backtester import KellyBacktester
from .performance_analyzer import PerformanceAnalyzer

# TimezoneMarketAnalyzer import (기존 파일 사용)
import sys
sys.path.append('.')
from timezone_market_analyzer import TimezoneMarketAnalyzer

logger = logging.getLogger(__name__)

class TimezoneBacktester(KellyBacktester):
    """시간대별 시장 분석 기반 백테스터"""
    
    def __init__(self, config: StrategyConfig, enable_timezone_analysis: bool = True):
        super().__init__(config)
        
        self.timezone_analyzer = TimezoneMarketAnalyzer() if enable_timezone_analysis else None
        self.timezone_results = {}
        self.hourly_performance = {}
        
        logger.info(f"🌏 시간대별 백테스터 초기화 완료 (분석기 {'활성화' if enable_timezone_analysis else '비활성화'})")
    
    def backtest_with_timezone_analysis(self, ohlcv_data: pd.DataFrame, strategy_func: Callable,
                                      initial_capital: float = 10_000_000) -> Dict[str, Any]:
        """
        시간대별 분석을 적용한 백테스트 실행
        
        Args:
            ohlcv_data: OHLCV 데이터 (datetime 포함)
            strategy_func: 전략 함수
            initial_capital: 초기 자본
            
        Returns:
            Dict: 시간대별 분석 결과가 포함된 백테스트 결과
        """
        try:
            logger.info(f"🎯 시간대별 백테스트 시작: {self.config.name}")
            
            if not self.timezone_analyzer:
                logger.warning("⚠️ 시간대 분석기가 비활성화됨, 일반 백테스트로 실행")
                return self._run_standard_backtest(ohlcv_data, strategy_func, initial_capital)
            
            # 데이터 전처리 및 시간 정보 추가
            enhanced_data = self._prepare_timezone_data(ohlcv_data)
            
            # 시간대별 거래 실행
            timezone_trades = self._execute_timezone_strategy(enhanced_data, strategy_func)
            
            if not timezone_trades:
                logger.warning("⚠️ 시간대별 거래가 생성되지 않았습니다")
                return self._create_empty_timezone_result()
            
            # 기본 성과 지표 계산
            basic_metrics = self._calculate_basic_metrics(timezone_trades)
            
            # 시간대별 성과 분석
            timezone_performance = self._analyze_timezone_performance(timezone_trades)
            
            # 시간대별 켈리 분석
            timezone_kelly_analysis = self._calculate_timezone_kelly_metrics(timezone_trades)
            
            # 글로벌 활성도와 성과 상관관계 분석
            activity_correlation = self._analyze_activity_correlation(timezone_trades)
            
            # 통합 결과 생성
            result = {
                'strategy_name': self.config.name,
                'timezone_analysis_enabled': True,
                'backtest_period': {
                    'start': enhanced_data['date'].min().isoformat(),
                    'end': enhanced_data['date'].max().isoformat(),
                    'total_days': (enhanced_data['date'].max() - enhanced_data['date'].min()).days
                },
                'basic_metrics': basic_metrics,
                'timezone_performance': timezone_performance,
                'timezone_kelly_analysis': timezone_kelly_analysis,
                'activity_correlation': activity_correlation,
                'trades': timezone_trades,
                'hourly_breakdown': self._generate_hourly_breakdown(timezone_trades),
                'recommendations': self._generate_timezone_recommendations(
                    timezone_performance, activity_correlation
                ),
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"✅ 시간대별 백테스트 완료: {len(timezone_trades)}개 거래")
            return result
            
        except Exception as e:
            logger.error(f"❌ 시간대별 백테스트 실패: {e}")
            return self._create_empty_timezone_result()
    
    def _prepare_timezone_data(self, ohlcv_data: pd.DataFrame) -> pd.DataFrame:
        """시간대 분석을 위한 데이터 전처리"""
        enhanced_data = ohlcv_data.copy()
        
        # 날짜 컬럼이 datetime이 아닌 경우 변환
        if not pd.api.types.is_datetime64_any_dtype(enhanced_data['date']):
            enhanced_data['date'] = pd.to_datetime(enhanced_data['date'])
        
        # KST 시간대 설정
        kst = pytz.timezone('Asia/Seoul')
        if enhanced_data['date'].dt.tz is None:
            enhanced_data['date'] = enhanced_data['date'].dt.tz_localize(kst)
        else:
            enhanced_data['date'] = enhanced_data['date'].dt.tz_convert(kst)
        
        # 시간 정보 추출
        enhanced_data['hour'] = enhanced_data['date'].dt.hour
        enhanced_data['day_of_week'] = enhanced_data['date'].dt.dayofweek
        enhanced_data['is_weekend'] = enhanced_data['day_of_week'].isin([5, 6])
        
        # 각 시간에 대한 시장 분석 추가
        timezone_analysis = []
        for _, row in enhanced_data.iterrows():
            kst_hour = row['hour']
            analysis = self.timezone_analyzer.generate_comprehensive_analysis(kst_hour)
            
            timezone_analysis.append({
                'global_activity_score': analysis['global_activity_score'],
                'market_condition': analysis['market_condition'],
                'dominant_region': analysis['trading_style']['dominant_region'],
                'trading_style': analysis['trading_style']['primary_style'],
                'position_size_modifier': analysis['strategy_adjustments']['position_size_modifier'],
                'stop_loss_pct': analysis['strategy_adjustments']['risk_parameters']['stop_loss_pct'],
                'take_profit_levels': analysis['strategy_adjustments']['risk_parameters']['take_profit_levels']
            })
        
        # 시간대 분석 결과를 DataFrame에 추가
        timezone_df = pd.DataFrame(timezone_analysis)
        enhanced_data = pd.concat([enhanced_data.reset_index(drop=True), timezone_df], axis=1)
        
        logger.info(f"📊 시간대 데이터 전처리 완료: {len(enhanced_data)}개 레코드")
        return enhanced_data
    
    def _execute_timezone_strategy(self, enhanced_data: pd.DataFrame, strategy_func: Callable) -> List[Dict]:
        """시간대별 조정이 적용된 전략 실행"""
        trades = []
        positions = {}  # {ticker: position_info}
        
        try:
            # 데이터 정렬
            data = enhanced_data.sort_values(['ticker', 'date'])
            
            # 티커별 처리
            for ticker in data['ticker'].unique():
                ticker_data = data[data['ticker'] == ticker].copy()
                ticker_data = ticker_data.sort_values('date')
                
                for idx, row in ticker_data.iterrows():
                    current_time = row['date']
                    
                    # 전략 시그널 생성 (기본 OHLCV 데이터만 사용)
                    ohlcv_subset = ticker_data.loc[:idx, ['date', 'open', 'high', 'low', 'close', 'volume']]
                    signal = strategy_func(ohlcv_subset)
                    
                    # 시간대별 조정 적용
                    adjusted_signal, trade_params = self._apply_timezone_adjustments(signal, row)
                    
                    if adjusted_signal == 'BUY' and ticker not in positions:
                        # 매수 실행
                        entry_price = row['close']
                        base_position_size = self._calculate_base_position_size(entry_price)
                        
                        # 시간대별 포지션 크기 조정
                        adjusted_position_size = base_position_size * trade_params['position_size_modifier']
                        
                        positions[ticker] = {
                            'entry_price': entry_price,
                            'quantity': adjusted_position_size,
                            'entry_date': current_time,
                            'entry_hour': row['hour'],
                            'strategy_signal': signal,
                            'timezone_params': trade_params,
                            'global_activity': row['global_activity_score'],
                            'market_condition': row['market_condition'],
                            'dominant_region': row['dominant_region']
                        }
                        
                    elif adjusted_signal == 'SELL' and ticker in positions:
                        # 매도 실행
                        position = positions[ticker]
                        exit_price = row['close']
                        
                        # 거래 기록 생성
                        trade = self._create_timezone_trade_record(
                            ticker, position, exit_price, current_time, row
                        )
                        trades.append(trade)
                        del positions[ticker]
                    
                    # 시간 기반 강제 매도 체크 (보유 시간 제한)
                    elif ticker in positions:
                        position = positions[ticker]
                        hold_hours = (current_time - position['entry_date']).total_seconds() / 3600
                        max_hold_hours = trade_params.get('max_holding_hours', 72)
                        
                        if hold_hours >= max_hold_hours:
                            exit_price = row['close']
                            trade = self._create_timezone_trade_record(
                                ticker, position, exit_price, current_time, row, 
                                exit_reason='TIME_LIMIT'
                            )
                            trades.append(trade)
                            del positions[ticker]
            
            # 미체결 포지션 정리
            for ticker, position in positions.items():
                final_row = data[data['ticker'] == ticker].iloc[-1]
                exit_price = final_row['close']
                trade = self._create_timezone_trade_record(
                    ticker, position, exit_price, final_row['date'], final_row, 
                    exit_reason='END_OF_DATA'
                )
                trades.append(trade)
            
            logger.info(f"📈 시간대별 전략 실행 완료: {len(trades)}개 거래")
            return trades
            
        except Exception as e:
            logger.error(f"❌ 시간대별 전략 실행 실패: {e}")
            return []
    
    def _apply_timezone_adjustments(self, base_signal: str, row: pd.Series) -> Tuple[str, Dict]:
        """시간대별 조정 적용"""
        # 기본 거래 파라미터
        trade_params = {
            'position_size_modifier': row['position_size_modifier'],
            'stop_loss_pct': row['stop_loss_pct'],
            'take_profit_levels': row['take_profit_levels'],
            'max_holding_hours': 24 if row['trading_style'] == 'momentum_driven' else 72
        }
        
        # 시장 상태에 따른 시그널 조정
        market_condition = row['market_condition']
        
        # 매우 조용한 시장에서는 보수적 접근
        if market_condition in ['VERY_QUIET', 'QUIET']:
            # 매수 신호를 보수적으로 조정
            if base_signal == 'BUY' and row['global_activity_score'] < 30:
                base_signal = 'HOLD'  # 활성도가 너무 낮으면 거래 중단
            
            # 포지션 크기 추가 감소
            trade_params['position_size_modifier'] *= 0.7
        
        # 매우 활성화된 시장에서는 적극적 접근
        elif market_condition == 'VERY_ACTIVE':
            # 포지션 크기 소폭 증대 (안전 범위 내)
            trade_params['position_size_modifier'] = min(1.0, trade_params['position_size_modifier'] * 1.1)
        
        return base_signal, trade_params
    
    def _create_timezone_trade_record(self, ticker: str, position: Dict, exit_price: float,
                                    exit_time: datetime, exit_row: pd.Series,
                                    exit_reason: str = 'STRATEGY_SIGNAL') -> Dict:
        """시간대 정보가 포함된 거래 기록 생성"""
        entry_price = position['entry_price']
        quantity = position['quantity']
        
        # 기본 계산
        pnl = (exit_price - entry_price) * quantity
        return_pct = (exit_price - entry_price) / entry_price
        hold_hours = (exit_time - position['entry_date']).total_seconds() / 3600
        
        # 시간대별 분석 정보 포함
        trade_record = {
            'ticker': ticker,
            'entry_date': position['entry_date'],
            'exit_date': exit_time,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'quantity': quantity,
            'pnl': pnl,
            'return_pct': return_pct,
            'hold_hours': hold_hours,
            'exit_reason': exit_reason,
            
            # 시간대별 정보
            'entry_hour': position['entry_hour'],
            'exit_hour': exit_row['hour'],
            'entry_global_activity': position['global_activity'],
            'exit_global_activity': exit_row['global_activity_score'],
            'entry_market_condition': position['market_condition'],
            'exit_market_condition': exit_row['market_condition'],
            'dominant_region_entry': position['dominant_region'],
            'dominant_region_exit': exit_row['dominant_region'],
            
            # 전략 정보
            'strategy_signal': position['strategy_signal'],
            'timezone_params': position['timezone_params']
        }
        
        return trade_record
    
    def _analyze_timezone_performance(self, trades: List[Dict]) -> Dict[str, Any]:
        """시간대별 성과 분석"""
        if not trades:
            return {'error': '분석할 거래가 없습니다'}
        
        try:
            # 시간대별 성과 집계
            hourly_performance = {}
            for hour in range(24):
                hour_trades = [t for t in trades if t['entry_hour'] == hour]
                if hour_trades:
                    returns = [t['return_pct'] for t in hour_trades]
                    hourly_performance[hour] = {
                        'trade_count': len(hour_trades),
                        'avg_return': np.mean(returns),
                        'win_rate': len([r for r in returns if r > 0]) / len(returns),
                        'total_return': np.sum(returns),
                        'std_return': np.std(returns),
                        'best_return': np.max(returns),
                        'worst_return': np.min(returns)
                    }
            
            # 시장 상태별 성과
            condition_performance = {}
            for condition in ['VERY_ACTIVE', 'ACTIVE', 'MODERATE', 'QUIET', 'VERY_QUIET']:
                condition_trades = [t for t in trades if t['entry_market_condition'] == condition]
                if condition_trades:
                    returns = [t['return_pct'] for t in condition_trades]
                    condition_performance[condition] = {
                        'trade_count': len(condition_trades),
                        'avg_return': np.mean(returns),
                        'win_rate': len([r for r in returns if r > 0]) / len(returns),
                        'total_return': np.sum(returns)
                    }
            
            # 지역별 성과
            region_performance = {}
            all_regions = set([t['dominant_region_entry'] for t in trades])
            for region in all_regions:
                region_trades = [t for t in trades if t['dominant_region_entry'] == region]
                if region_trades:
                    returns = [t['return_pct'] for t in region_trades]
                    region_performance[region] = {
                        'trade_count': len(region_trades),
                        'avg_return': np.mean(returns),
                        'win_rate': len([r for r in returns if r > 0]) / len(returns),
                        'total_return': np.sum(returns)
                    }
            
            # 최고/최저 성과 시간대
            best_hours = sorted(hourly_performance.items(), 
                              key=lambda x: x[1]['avg_return'], reverse=True)[:3]
            worst_hours = sorted(hourly_performance.items(), 
                               key=lambda x: x[1]['avg_return'])[:3]
            
            return {
                'hourly_performance': hourly_performance,
                'condition_performance': condition_performance,
                'region_performance': region_performance,
                'best_hours': [{'hour': h, 'avg_return': p['avg_return']} for h, p in best_hours],
                'worst_hours': [{'hour': h, 'avg_return': p['avg_return']} for h, p in worst_hours],
                'total_timezone_trades': len(trades)
            }
            
        except Exception as e:
            logger.error(f"❌ 시간대별 성과 분석 실패: {e}")
            return {'error': str(e)}
    
    def _calculate_timezone_kelly_metrics(self, trades: List[Dict]) -> Dict[str, Any]:
        """시간대별 켈리 지표 계산"""
        if not trades:
            return {'error': '계산할 거래가 없습니다'}
        
        try:
            # 전체 켈리 지표
            all_returns = [t['return_pct'] for t in trades]
            overall_kelly = self.calculate_kelly_fraction(all_returns)
            
            # 시간대별 켈리 지표
            hourly_kelly = {}
            for hour in range(24):
                hour_trades = [t for t in trades if t['entry_hour'] == hour]
                if len(hour_trades) >= 5:  # 최소 5개 거래 필요
                    hour_returns = [t['return_pct'] for t in hour_trades]
                    kelly_metrics = self.calculate_kelly_fraction(hour_returns)
                    hourly_kelly[hour] = kelly_metrics
            
            # 시장 상태별 켈리 지표
            condition_kelly = {}
            for condition in ['VERY_ACTIVE', 'ACTIVE', 'MODERATE', 'QUIET', 'VERY_QUIET']:
                condition_trades = [t for t in trades if t['entry_market_condition'] == condition]
                if len(condition_trades) >= 5:
                    condition_returns = [t['return_pct'] for t in condition_trades]
                    kelly_metrics = self.calculate_kelly_fraction(condition_returns)
                    condition_kelly[condition] = kelly_metrics
            
            return {
                'overall_kelly': overall_kelly,
                'hourly_kelly': hourly_kelly,
                'condition_kelly': condition_kelly,
                'best_kelly_hour': max(hourly_kelly.items(), 
                                     key=lambda x: x[1]['kelly_fraction'])[0] if hourly_kelly else None,
                'best_kelly_condition': max(condition_kelly.items(), 
                                          key=lambda x: x[1]['kelly_fraction'])[0] if condition_kelly else None
            }
            
        except Exception as e:
            logger.error(f"❌ 시간대별 켈리 지표 계산 실패: {e}")
            return {'error': str(e)}
    
    def _analyze_activity_correlation(self, trades: List[Dict]) -> Dict[str, Any]:
        """글로벌 활성도와 성과 상관관계 분석"""
        if not trades:
            return {'error': '분석할 거래가 없습니다'}
        
        try:
            # 활성도별 구간 분석
            activity_bins = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100)]
            activity_analysis = {}
            
            for low, high in activity_bins:
                bin_trades = [
                    t for t in trades 
                    if low <= t['entry_global_activity'] < high
                ]
                if bin_trades:
                    returns = [t['return_pct'] for t in bin_trades]
                    activity_analysis[f"{low}-{high}"] = {
                        'trade_count': len(bin_trades),
                        'avg_return': np.mean(returns),
                        'win_rate': len([r for r in returns if r > 0]) / len(returns),
                        'std_return': np.std(returns)
                    }
            
            # 상관계수 계산
            activity_scores = [t['entry_global_activity'] for t in trades]
            returns = [t['return_pct'] for t in trades]
            
            correlation = np.corrcoef(activity_scores, returns)[0, 1] if len(trades) > 1 else 0
            
            # 최적 활성도 구간 찾기
            best_activity_range = max(activity_analysis.items(), 
                                    key=lambda x: x[1]['avg_return'])[0] if activity_analysis else None
            
            return {
                'activity_correlation': correlation,
                'activity_analysis': activity_analysis,
                'best_activity_range': best_activity_range,
                'correlation_strength': self._interpret_correlation(correlation),
                'recommendation': self._generate_activity_recommendation(correlation, activity_analysis)
            }
            
        except Exception as e:
            logger.error(f"❌ 활성도 상관관계 분석 실패: {e}")
            return {'error': str(e)}
    
    def _interpret_correlation(self, correlation: float) -> str:
        """상관계수 해석"""
        abs_corr = abs(correlation)
        if abs_corr >= 0.7:
            return "강한 상관관계"
        elif abs_corr >= 0.5:
            return "중간 상관관계"
        elif abs_corr >= 0.3:
            return "약한 상관관계"
        else:
            return "상관관계 없음"
    
    def _generate_activity_recommendation(self, correlation: float, activity_analysis: Dict) -> str:
        """활성도 기반 권장사항 생성"""
        if correlation > 0.3:
            return "높은 글로벌 활성도 시간대에 집중 거래를 권장합니다"
        elif correlation < -0.3:
            return "낮은 글로벌 활성도 시간대에 집중 거래를 권장합니다"
        else:
            return "글로벌 활성도와 성과 간 명확한 관계가 없으므로 다른 요인을 고려하세요"
    
    def _generate_hourly_breakdown(self, trades: List[Dict]) -> Dict[str, Any]:
        """시간별 상세 분석"""
        breakdown = {}
        
        for hour in range(24):
            hour_trades = [t for t in trades if t['entry_hour'] == hour]
            if hour_trades:
                breakdown[f"{hour:02d}:00"] = {
                    'trade_count': len(hour_trades),
                    'avg_return': np.mean([t['return_pct'] for t in hour_trades]),
                    'win_rate': len([t for t in hour_trades if t['return_pct'] > 0]) / len(hour_trades),
                    'avg_hold_hours': np.mean([t['hold_hours'] for t in hour_trades]),
                    'dominant_regions': list(set([t['dominant_region_entry'] for t in hour_trades])),
                    'market_conditions': list(set([t['entry_market_condition'] for t in hour_trades]))
                }
        
        return breakdown
    
    def _generate_timezone_recommendations(self, timezone_performance: Dict, 
                                         activity_correlation: Dict) -> List[str]:
        """시간대별 거래 권장사항 생성"""
        recommendations = []
        
        # 최고 성과 시간대 추천
        if 'best_hours' in timezone_performance and timezone_performance['best_hours']:
            best_hours = timezone_performance['best_hours'][:3]
            hour_list = ", ".join([f"{h['hour']:02d}:00" for h in best_hours])
            recommendations.append(f"🏆 최고 성과 시간대: {hour_list}")
        
        # 활성도 기반 추천
        if 'best_activity_range' in activity_correlation and activity_correlation['best_activity_range']:
            range_str = activity_correlation['best_activity_range']
            recommendations.append(f"📊 최적 글로벌 활성도 구간: {range_str}%")
        
        # 상관관계 기반 추천
        correlation = activity_correlation.get('correlation_strength', '')
        if correlation:
            recommendations.append(f"📈 활성도-성과 상관관계: {correlation}")
        
        # 지역별 추천
        if 'region_performance' in timezone_performance:
            region_perf = timezone_performance['region_performance']
            if region_perf:
                best_region = max(region_perf.items(), key=lambda x: x[1]['avg_return'])[0]
                recommendations.append(f"🌏 최고 성과 지역: {best_region}")
        
        return recommendations
    
    def _run_standard_backtest(self, ohlcv_data: pd.DataFrame, strategy_func: Callable,
                             initial_capital: float) -> Dict[str, Any]:
        """시간대 분석 없는 표준 백테스트"""
        kelly_result = self.backtest_with_kelly(ohlcv_data, strategy_func, initial_capital)
        
        return {
            'strategy_name': self.config.name,
            'timezone_analysis_enabled': False,
            'kelly_result': kelly_result,
            'note': '시간대 분석이 비활성화된 상태에서 실행됨'
        }
    
    def _calculate_basic_metrics(self, trades: List[Dict]) -> Dict[str, Any]:
        """기본 성과 지표 계산"""
        if not trades:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'avg_return': 0,
                'total_return': 0,
                'avg_hold_time': 0,
                'max_drawdown': 0,
                'volatility': 0
            }
        
        try:
            returns = [t['return_pct'] for t in trades]
            win_trades = [r for r in returns if r > 0]
            lose_trades = [r for r in returns if r <= 0]
            
            # 기본 지표
            total_trades = len(trades)
            winning_trades = len(win_trades)
            losing_trades = len(lose_trades)
            win_rate = winning_trades / total_trades if total_trades > 0 else 0
            
            # 수익률 지표
            avg_return = np.mean(returns) if returns else 0
            total_return = sum(returns)
            volatility = np.std(returns) if len(returns) > 1 else 0
            
            # 보유 시간
            hold_times = [t['hold_hours'] for t in trades]
            avg_hold_time = np.mean(hold_times) if hold_times else 0
            
            # 최대 낙폭 계산
            cumulative_returns = np.cumsum(returns)
            running_max = np.maximum.accumulate(cumulative_returns)
            drawdowns = running_max - cumulative_returns
            max_drawdown = np.max(drawdowns) if len(drawdowns) > 0 else 0
            
            return {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'avg_return': avg_return,
                'total_return': total_return,
                'avg_hold_time': avg_hold_time,
                'max_drawdown': max_drawdown,
                'volatility': volatility,
                'avg_win': np.mean(win_trades) if win_trades else 0,
                'avg_loss': np.mean(lose_trades) if lose_trades else 0,
                'profit_factor': sum(win_trades) / abs(sum(lose_trades)) if lose_trades else 0
            }
            
        except Exception as e:
            logger.error(f"❌ 기본 성과 지표 계산 실패: {e}")
            return {
                'total_trades': len(trades),
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'avg_return': 0,
                'total_return': 0,
                'avg_hold_time': 0,
                'max_drawdown': 0,
                'volatility': 0
            }

    def _calculate_base_position_size(self, entry_price: float) -> float:
        """기본 포지션 크기 계산"""
        try:
            if self.config.position_size_method == "fixed":
                return self.config.position_size_value / entry_price
            elif self.config.position_size_method == "percent":
                # 포트폴리오의 일정 비율
                portfolio_value = 10_000_000  # 기본 포트폴리오 가치
                position_value = portfolio_value * self.config.position_size_value
                return position_value / entry_price
            elif self.config.position_size_method == "kelly":
                # 켈리 공식 기반 (기본값 사용)
                portfolio_value = 10_000_000
                kelly_fraction = self.config.position_size_value
                position_value = portfolio_value * kelly_fraction
                return position_value / entry_price
            else:
                # 기본값: 포트폴리오의 10%
                portfolio_value = 10_000_000
                position_value = portfolio_value * 0.1
                return position_value / entry_price
                
        except Exception as e:
            logger.error(f"❌ 포지션 크기 계산 실패: {e}")
            # 안전한 기본값 반환
            return 100_000 / entry_price
    
    def _create_empty_timezone_result(self) -> Dict[str, Any]:
        """빈 시간대 백테스트 결과 생성"""
        return {
            'strategy_name': self.config.name,
            'timezone_analysis_enabled': True,
            'error': '시간대별 백테스트 실행 실패',
            'basic_metrics': {},
            'timezone_performance': {},
            'timezone_kelly_analysis': {},
            'activity_correlation': {},
            'trades': [],
            'hourly_breakdown': {},
            'recommendations': [],
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    def compare_timezone_vs_standard(self, ohlcv_data: pd.DataFrame, strategy_func: Callable) -> Dict[str, Any]:
        """시간대별 vs 표준 백테스트 비교"""
        try:
            logger.info("🔄 시간대별 vs 표준 백테스트 비교 시작")
            
            # 시간대별 백테스트
            timezone_result = self.backtest_with_timezone_analysis(ohlcv_data, strategy_func)
            
            # 표준 백테스트 (시간대 분석 비활성화)
            standard_backtester = TimezoneBacktester(self.config, enable_timezone_analysis=False)
            standard_result = standard_backtester._run_standard_backtest(ohlcv_data, strategy_func, 10_000_000)
            
            # 비교 분석
            comparison = {
                'timezone_enhanced': {
                    'total_trades': len(timezone_result.get('trades', [])),
                    'avg_return': np.mean([t['return_pct'] for t in timezone_result.get('trades', [])]) if timezone_result.get('trades') else 0,
                    'win_rate': len([t for t in timezone_result.get('trades', []) if t['return_pct'] > 0]) / len(timezone_result.get('trades', [])) if timezone_result.get('trades') else 0
                },
                'standard': {
                    'total_trades': standard_result.get('kelly_result', {}).total_trades if hasattr(standard_result.get('kelly_result', {}), 'total_trades') else 0,
                    'avg_return': 0,  # 간소화
                    'win_rate': standard_result.get('kelly_result', {}).win_rate if hasattr(standard_result.get('kelly_result', {}), 'win_rate') else 0
                }
            }
            
            # 개선 효과 계산
            timezone_return = comparison['timezone_enhanced']['avg_return']
            standard_return = comparison['standard']['avg_return']
            improvement = (timezone_return - standard_return) / (abs(standard_return) + 0.0001) * 100
            
            return {
                'comparison': comparison,
                'improvement_pct': improvement,
                'recommendation': "시간대 분석 적용을 권장합니다" if improvement > 5 else "표준 백테스트로 충분합니다",
                'analysis_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 비교 분석 실패: {e}")
            return {'error': str(e)}


def create_timezone_backtester(config: StrategyConfig, enable_timezone: bool = True) -> TimezoneBacktester:
    """시간대별 백테스터 생성 함수"""
    return TimezoneBacktester(config, enable_timezone_analysis=enable_timezone)