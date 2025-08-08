#!/usr/bin/env python3
"""
켈리 공식 백테스터 모듈

켈리 공식을 활용한 포지션 크기 최적화 백테스팅을 제공합니다.
기존 backtester.py의 KellyBacktester 클래스를 분리했습니다.

Author: Backtesting Refactoring  
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Any
import scipy.stats as stats
from scipy.optimize import minimize

from .backtest_types import KellyBacktestResult, StrategyConfig, BacktestResult

logger = logging.getLogger(__name__)

class KellyBacktester:
    """켈리 공식 기반 백테스터"""
    
    def __init__(self, config: StrategyConfig):
        self.config = config
        self.results_cache = {}
        
    def calculate_kelly_fraction(self, returns: List[float]) -> Dict[str, float]:
        """
        켈리 공식으로 최적 포지션 크기 계산
        
        Args:
            returns: 거래 수익률 리스트
            
        Returns:
            Dict: 켈리 공식 관련 지표들
        """
        if len(returns) < 10:
            logger.warning("⚠️ 거래 기록이 부족합니다 (최소 10개 필요)")
            return self._get_default_kelly_metrics()
        
        # 승리/패배 구분
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]
        
        if not wins or not losses:
            logger.warning("⚠️ 승리 또는 패배 기록이 없습니다")
            return self._get_default_kelly_metrics()
        
        # 기본 통계
        total_trades = len(returns)
        win_rate = len(wins) / total_trades  # p
        loss_rate = 1 - win_rate  # q
        
        avg_win = np.mean(wins)
        avg_loss = abs(np.mean(losses))
        b_value = avg_win / avg_loss if avg_loss > 0 else 0  # 평균 수익/손실 비율
        
        # 켈리 공식: f* = (bp - q) / b
        kelly_fraction = (b_value * win_rate - loss_rate) / b_value if b_value > 0 else 0
        
        # 안전 범위로 제한 (-10% ~ 50%)
        kelly_fraction = max(-0.1, min(0.5, kelly_fraction))
        
        # 변형 버전들
        kelly_half = kelly_fraction * 0.5
        kelly_quarter = kelly_fraction * 0.25
        
        # 추가 리스크 지표
        returns_array = np.array(returns)
        volatility = np.std(returns_array) if len(returns_array) > 1 else 0
        max_drawdown = self._calculate_max_drawdown(returns)
        var_95 = np.percentile(returns_array, 5) if len(returns_array) > 0 else 0
        
        return {
            'kelly_fraction': kelly_fraction,
            'kelly_1_2': kelly_half,
            'kelly_1_4': kelly_quarter,
            'win_rate': win_rate,
            'loss_rate': loss_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'b_value': b_value,
            'total_trades': total_trades,
            'winning_trades': len(wins),
            'losing_trades': len(losses),
            'volatility': volatility,
            'max_drawdown': max_drawdown,
            'var_95': var_95
        }
    
    def _get_default_kelly_metrics(self) -> Dict[str, float]:
        """기본 켈리 지표 반환 (데이터 부족 시)"""
        return {
            'kelly_fraction': 0.0,
            'kelly_1_2': 0.0, 
            'kelly_1_4': 0.0,
            'win_rate': 0.5,
            'loss_rate': 0.5,
            'avg_win': 0.0,
            'avg_loss': 0.0,
            'b_value': 1.0,
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'volatility': 0.0,
            'max_drawdown': 0.0,
            'var_95': 0.0
        }
    
    def _calculate_max_drawdown(self, returns: List[float]) -> float:
        """최대 낙폭 계산"""
        if not returns:
            return 0.0
        
        cumulative = np.cumprod(1 + np.array(returns))
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        
        return np.min(drawdown) if len(drawdown) > 0 else 0.0
    
    def backtest_with_kelly(self, ohlcv_data: pd.DataFrame, strategy_func, 
                           initial_capital: float = 10_000_000) -> KellyBacktestResult:
        """
        켈리 공식을 적용한 백테스트 실행
        
        Args:
            ohlcv_data: OHLCV 데이터
            strategy_func: 전략 함수
            initial_capital: 초기 자본
            
        Returns:
            KellyBacktestResult: 켈리 백테스트 결과
        """
        try:
            logger.info(f"🎯 켈리 백테스트 시작: {self.config.name}")
            
            # 기본 백테스트 실행
            trades = self._execute_strategy(ohlcv_data, strategy_func)
            
            if not trades:
                logger.warning("⚠️ 생성된 거래가 없습니다")
                return self._create_empty_kelly_result()
            
            # 수익률 계산
            returns = [trade.get('return_pct', 0.0) for trade in trades]
            
            # 켈리 지표 계산
            kelly_metrics = self.calculate_kelly_fraction(returns)
            
            # 포트폴리오 성과 시뮬레이션
            portfolio_performance = self._simulate_kelly_portfolio(
                trades, kelly_metrics, initial_capital
            )
            
            # 결과 생성
            period = (
                ohlcv_data['date'].min().date(),
                ohlcv_data['date'].max().date()
            )
            
            result = KellyBacktestResult(
                strategy_name=self.config.name,
                period=period,
                kelly_fraction=kelly_metrics['kelly_fraction'],
                kelly_1_2=kelly_metrics['kelly_1_2'],
                kelly_1_4=kelly_metrics['kelly_1_4'],
                win_rate=kelly_metrics['win_rate'],
                avg_win=kelly_metrics['avg_win'],
                avg_loss=kelly_metrics['avg_loss'],
                b_value=kelly_metrics['b_value'],
                max_drawdown=portfolio_performance['max_drawdown'],
                volatility=kelly_metrics['volatility'],
                var_95=kelly_metrics['var_95'],
                total_trades=kelly_metrics['total_trades'],
                winning_trades=kelly_metrics['winning_trades'],
                losing_trades=kelly_metrics['losing_trades'],
                parameters=self.config.to_dict(),
                trades_detail=trades
            )
            
            logger.info(f"✅ 켈리 백테스트 완료: 켈리비율 {kelly_metrics['kelly_fraction']:.3f}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 켈리 백테스트 실패: {e}")
            return self._create_empty_kelly_result()
    
    def _execute_strategy(self, data: pd.DataFrame, strategy_func) -> List[Dict]:
        """전략 실행 및 거래 기록 생성"""
        trades = []
        positions = {}  # {ticker: {'entry_price': float, 'quantity': float, 'entry_date': date}}
        
        try:
            # 데이터 정렬
            data = data.sort_values(['ticker', 'date'])
            
            # 티커별 처리
            for ticker in data['ticker'].unique():
                ticker_data = data[data['ticker'] == ticker].copy()
                ticker_data = ticker_data.sort_values('date')
                
                for idx, row in ticker_data.iterrows():
                    # 전략 시그널 생성
                    signal = strategy_func(ticker_data.loc[:idx])
                    
                    if signal == 'BUY' and ticker not in positions:
                        # 매수 실행
                        entry_price = row['close']
                        quantity = self._calculate_position_size(entry_price)
                        
                        positions[ticker] = {
                            'entry_price': entry_price,
                            'quantity': quantity,
                            'entry_date': row['date'],
                            'strategy_signal': signal
                        }
                        
                    elif signal == 'SELL' and ticker in positions:
                        # 매도 실행
                        position = positions[ticker]
                        exit_price = row['close']
                        
                        # 거래 기록 생성
                        pnl = (exit_price - position['entry_price']) * position['quantity']
                        return_pct = (exit_price - position['entry_price']) / position['entry_price']
                        hold_days = (row['date'] - position['entry_date']).days
                        
                        trade = {
                            'ticker': ticker,
                            'entry_date': position['entry_date'],
                            'exit_date': row['date'],
                            'entry_price': position['entry_price'],
                            'exit_price': exit_price,
                            'quantity': position['quantity'],
                            'pnl': pnl,
                            'return_pct': return_pct,
                            'hold_days': hold_days,
                            'strategy_signal': position['strategy_signal']
                        }
                        
                        trades.append(trade)
                        del positions[ticker]
            
            logger.info(f"📊 전략 실행 완료: {len(trades)}개 거래 생성")
            return trades
            
        except Exception as e:
            logger.error(f"❌ 전략 실행 실패: {e}")
            return []
    
    def _calculate_position_size(self, price: float) -> float:
        """포지션 크기 계산"""
        if self.config.position_size_method == "fixed":
            return self.config.position_size_value / price
        elif self.config.position_size_method == "percent":
            total_value = self.config.initial_cash
            position_value = total_value * self.config.position_size_value
            return position_value / price
        else:  # kelly
            # 기본 포지션 크기 (켈리 공식은 거래 후 적용)
            return self.config.initial_cash * 0.1 / price
    
    def _simulate_kelly_portfolio(self, trades: List[Dict], kelly_metrics: Dict, 
                                initial_capital: float) -> Dict[str, float]:
        """켈리 공식을 적용한 포트폴리오 성과 시뮬레이션"""
        if not trades:
            return {'max_drawdown': 0.0, 'final_value': initial_capital}
        
        # 포트폴리오 가치 시뮬레이션
        portfolio_values = [initial_capital]
        current_value = initial_capital
        
        kelly_fraction = kelly_metrics['kelly_fraction']
        kelly_safe = max(0.05, min(0.25, kelly_fraction))  # 5% ~ 25% 제한
        
        for trade in trades:
            # 켈리 공식 적용된 포지션 크기로 재계산
            trade_return = trade['return_pct']
            position_fraction = kelly_safe
            
            # 포트폴리오 가치 업데이트
            portfolio_change = current_value * position_fraction * trade_return
            current_value += portfolio_change
            portfolio_values.append(current_value)
        
        # 최대 낙폭 계산
        portfolio_array = np.array(portfolio_values)
        running_max = np.maximum.accumulate(portfolio_array)
        drawdowns = (portfolio_array - running_max) / running_max
        max_drawdown = np.min(drawdowns) if len(drawdowns) > 0 else 0.0
        
        return {
            'max_drawdown': max_drawdown,
            'final_value': current_value,
            'portfolio_values': portfolio_values
        }
    
    def _create_empty_kelly_result(self) -> KellyBacktestResult:
        """빈 켈리 백테스트 결과 생성"""
        return KellyBacktestResult(
            strategy_name=self.config.name,
            period=(date.today() - timedelta(days=30), date.today()),
            parameters=self.config.to_dict()
        )
    
    def optimize_kelly_parameters(self, ohlcv_data: pd.DataFrame, strategy_func,
                                 param_ranges: Dict[str, Tuple[float, float]]) -> Dict[str, Any]:
        """
        켈리 공식을 목표로 하는 파라미터 최적화
        
        Args:
            ohlcv_data: OHLCV 데이터
            strategy_func: 전략 함수
            param_ranges: 최적화할 파라미터 범위
            
        Returns:
            Dict: 최적화 결과
        """
        try:
            logger.info(f"🔧 켈리 최적화 시작: {list(param_ranges.keys())}")
            
            def objective_function(params):
                """최적화 목적 함수 (켈리 비율 최대화)"""
                # 파라미터 적용
                temp_config = StrategyConfig.from_dict(self.config.to_dict())
                
                for i, (param_name, _) in enumerate(param_ranges.items()):
                    if '.' in param_name:
                        # 중첩된 파라미터 (예: technical_indicators.rsi_period)
                        parts = param_name.split('.')
                        if parts[0] == 'technical_indicators':
                            temp_config.technical_indicators[parts[1]] = params[i]
                    else:
                        setattr(temp_config, param_name, params[i])
                
                # 임시 백테스터로 테스트
                temp_backtester = KellyBacktester(temp_config)
                result = temp_backtester.backtest_with_kelly(ohlcv_data, strategy_func)
                
                # 켈리 비율을 음수로 반환 (minimize 함수용)
                return -result.kelly_fraction if result.kelly_fraction > 0 else 1.0
            
            # 초기값과 범위 설정
            param_names = list(param_ranges.keys())
            bounds = list(param_ranges.values())
            initial_guess = [(b[0] + b[1]) / 2 for b in bounds]
            
            # 최적화 실행
            optimization_result = minimize(
                objective_function,
                initial_guess,
                bounds=bounds,
                method='L-BFGS-B'
            )
            
            if optimization_result.success:
                # 최적 파라미터로 최종 백테스트
                optimal_params = optimization_result.x
                optimal_config = StrategyConfig.from_dict(self.config.to_dict())
                
                for i, param_name in enumerate(param_names):
                    if '.' in param_name:
                        parts = param_name.split('.')
                        if parts[0] == 'technical_indicators':
                            optimal_config.technical_indicators[parts[1]] = optimal_params[i]
                    else:
                        setattr(optimal_config, param_name, optimal_params[i])
                
                optimal_backtester = KellyBacktester(optimal_config)
                final_result = optimal_backtester.backtest_with_kelly(ohlcv_data, strategy_func)
                
                logger.info(f"✅ 켈리 최적화 완료: 켈리비율 {final_result.kelly_fraction:.3f}")
                
                return {
                    'optimization_success': True,
                    'optimal_parameters': dict(zip(param_names, optimal_params)),
                    'optimal_kelly_fraction': final_result.kelly_fraction,
                    'backtest_result': final_result,
                    'optimization_details': optimization_result
                }
            else:
                logger.warning("⚠️ 켈리 최적화 실패")
                return {
                    'optimization_success': False,
                    'error': optimization_result.message
                }
                
        except Exception as e:
            logger.error(f"❌ 켈리 최적화 중 오류: {e}")
            return {
                'optimization_success': False,
                'error': str(e)
            }
    
    def compare_kelly_vs_fixed(self, ohlcv_data: pd.DataFrame, strategy_func) -> Dict[str, Any]:
        """켈리 공식 vs 고정 포지션 크기 비교"""
        try:
            # 1. 켈리 공식 백테스트
            kelly_result = self.backtest_with_kelly(ohlcv_data, strategy_func)
            
            # 2. 고정 크기 백테스트
            fixed_config = StrategyConfig.from_dict(self.config.to_dict())
            fixed_config.position_size_method = "fixed"
            fixed_config.position_size_value = 1_000_000  # 100만원 고정
            
            fixed_backtester = KellyBacktester(fixed_config)
            fixed_result = fixed_backtester.backtest_with_kelly(ohlcv_data, strategy_func)
            
            # 3. 퍼센트 백테스트 (10%)
            percent_config = StrategyConfig.from_dict(self.config.to_dict())
            percent_config.position_size_method = "percent"
            percent_config.position_size_value = 0.1
            
            percent_backtester = KellyBacktester(percent_config)
            percent_result = percent_backtester.backtest_with_kelly(ohlcv_data, strategy_func)
            
            # 결과 비교
            comparison = {
                'kelly': {
                    'kelly_fraction': kelly_result.kelly_fraction,
                    'win_rate': kelly_result.win_rate,
                    'max_drawdown': kelly_result.max_drawdown,
                    'total_trades': kelly_result.total_trades
                },
                'fixed': {
                    'kelly_fraction': fixed_result.kelly_fraction,
                    'win_rate': fixed_result.win_rate,
                    'max_drawdown': fixed_result.max_drawdown,
                    'total_trades': fixed_result.total_trades
                },
                'percent': {
                    'kelly_fraction': percent_result.kelly_fraction,
                    'win_rate': percent_result.win_rate,
                    'max_drawdown': percent_result.max_drawdown,
                    'total_trades': percent_result.total_trades
                }
            }
            
            # 최고 성과 방식 선택
            best_method = max(comparison.keys(), 
                            key=lambda x: comparison[x]['kelly_fraction'])
            
            logger.info(f"📊 포지션 크기 비교 완료: 최고 방식은 '{best_method}'")
            
            return {
                'comparison': comparison,
                'best_method': best_method,
                'recommendation': f"'{best_method}' 방식을 권장합니다"
            }
            
        except Exception as e:
            logger.error(f"❌ 포지션 크기 비교 실패: {e}")
            return {'error': str(e)}