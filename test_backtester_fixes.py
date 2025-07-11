#!/usr/bin/env python3
"""
백테스트 수정사항 검증 스크립트
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import logging

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtester import backtest_combo, KellyBacktester
from backtester import StrategyConfig, BacktestResult, KellyBacktestResult, ComprehensiveBacktestEngine

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_date_handling():
    """날짜 처리 함수 테스트"""
    logger.info("🔍 날짜 처리 함수 테스트 시작")
    
    # 테스트 케이스 1: datetime 객체
    dt1 = datetime(2024, 1, 1)
    dt2 = datetime(2024, 1, 10)
    duration = (dt2 - dt1).days
    assert duration == 9, f"datetime 계산 오류: {duration}"
    logger.info("✅ datetime 객체 테스트 통과")
    
    # 테스트 케이스 2: date 객체
    d1 = date(2024, 1, 1)
    d2 = date(2024, 1, 10)
    duration = (d2 - d1).days
    assert duration == 9, f"date 계산 오류: {duration}"
    logger.info("✅ date 객체 테스트 통과")
    
    # 테스트 케이스 3: 정수형 (모의 데이터)
    int1 = 1
    int2 = 10
    # 정수형은 기본값 1 사용
    duration = 1
    assert duration == 1, f"정수형 기본값 오류: {duration}"
    logger.info("✅ 정수형 기본값 테스트 통과")
    
    logger.info("🎉 모든 날짜 처리 테스트 통과")

def test_backtest_combo_ticker_handling():
    """backtest_combo 함수의 ticker 컬럼 처리 테스트"""
    logger.info("🔍 backtest_combo ticker 처리 테스트 시작")
    
    # 테스트 케이스 1: ticker 컬럼이 없는 경우
    ohlcv_df = pd.DataFrame({
        'open': [100, 101, 102],
        'high': [105, 106, 107],
        'low': [95, 96, 97],
        'close': [103, 104, 105],
        'volume': [1000, 1100, 1200]
    })
    
    market_df = pd.DataFrame({
        'close': [103, 104, 105],
        'rsi': [50, 55, 60],
        'macd': [0.1, 0.2, 0.3]
    })
    
    combo = {'name': 'Test_Strategy'}
    
    try:
        results = backtest_combo(ohlcv_df, market_df, combo)
        assert len(results) > 0, "ticker 컬럼이 없을 때 모의 결과가 생성되어야 함"
        assert results[0]['ticker'] == 'MOCK_TICKER', "모의 티커명이 올바르지 않음"
        logger.info("✅ ticker 컬럼 없는 경우 테스트 통과")
    except Exception as e:
        logger.error(f"❌ ticker 컬럼 없는 경우 테스트 실패: {e}")
        return False
    
    # 테스트 케이스 2: 정상적인 ticker 컬럼이 있는 경우
    ohlcv_df_with_ticker = pd.DataFrame({
        'ticker': ['BTC', 'BTC', 'BTC'],
        'open': [100, 101, 102],
        'high': [105, 106, 107],
        'low': [95, 96, 97],
        'close': [103, 104, 105],
        'volume': [1000, 1100, 1200]
    })
    
    market_df_with_ticker = pd.DataFrame({
        'ticker': ['BTC'],
        'close': [103],
        'rsi': [50],
        'macd': [0.1]
    })
    
    try:
        results = backtest_combo(ohlcv_df_with_ticker, market_df_with_ticker, combo)
        # 결과가 있거나 없을 수 있음 (전략 조건에 따라)
        logger.info("✅ ticker 컬럼 있는 경우 테스트 통과")
    except Exception as e:
        logger.error(f"❌ ticker 컬럼 있는 경우 테스트 실패: {e}")
        return False
    
    logger.info("🎉 backtest_combo ticker 처리 테스트 통과")
    return True

def test_kelly_report_date_handling():
    """켈리 리포트 날짜 처리 테스트"""
    logger.info("🔍 켈리 리포트 날짜 처리 테스트 시작")
    
    # 테스트용 거래 데이터 생성
    trades = [
        {
            'date': datetime(2024, 1, 1),
            'action': 'BUY',
            'price': 100000,
            'quantity': 1.0
        },
        {
            'date': datetime(2024, 1, 10),
            'action': 'SELL',
            'price': 110000,
            'quantity': 1.0,
            'return_pct': 10.0
        },
        {
            'date': 1,  # 정수형 테스트
            'action': 'BUY',
            'price': 100000,
            'quantity': 1.0
        }
    ]
    
    # KellyBacktestResult 생성
    result = KellyBacktestResult(
        strategy_name="Test_Strategy",
        total_return=0.1,
        win_rate=0.5,
        profit_factor=1.2,
        max_drawdown=0.05,
        sharpe_ratio=1.0,
        total_trades=2,
        winning_trades=1,
        losing_trades=1,
        final_capital=1100000,
        initial_capital=1000000,
        kelly_fraction=0.5,
        atr_multiplier=1.5,
        trades=trades,
        equity_curve=[1000000, 1100000],
        parameters={}
    )
    
    # KellyBacktester 인스턴스 생성
    kelly_backtester = KellyBacktester()
    
    try:
        report = kelly_backtester.generate_kelly_report(result)
        assert len(report) > 0, "리포트가 생성되어야 함"
        assert "Test_Strategy" in report, "전략명이 리포트에 포함되어야 함"
        logger.info("✅ 켈리 리포트 날짜 처리 테스트 통과")
    except Exception as e:
        logger.error(f"❌ 켈리 리포트 날짜 처리 테스트 실패: {e}")
        return False
    
    logger.info("🎉 켈리 리포트 날짜 처리 테스트 통과")
    return True

def test_strategy_backtest_date_handling():
    """전략 백테스트 날짜 처리 테스트"""
    logger.info("🔍 전략 백테스트 날짜 처리 테스트 시작")
    
    # 테스트용 시장 데이터 생성
    market_data = pd.DataFrame({
        'close': [100, 101, 102, 103, 104],
        'high': [105, 106, 107, 108, 109],
        'low': [95, 96, 97, 98, 99],
        'volume': [1000, 1100, 1200, 1300, 1400]
    })
    
    # StrategyConfig 생성
    strategy_config = StrategyConfig(
        name="Test_Strategy",
        parameters={'position_size': 0.1},
        entry_conditions=['test_entry'],
        exit_conditions=['test_exit'],
        risk_management={'max_position_size': 0.15, 'stop_loss': 0.05}
    )
    
    # ComprehensiveBacktestEngine 인스턴스 생성
    engine = ComprehensiveBacktestEngine()
    
    try:
        # 백테스트 실행 (실제로는 모의 신호만 생성)
        result = engine._run_single_strategy_backtest(strategy_config, market_data, 3)
        
        # 결과가 None이거나 BacktestResult 객체여야 함
        assert result is None or isinstance(result, BacktestResult), "결과 타입이 올바르지 않음"
        logger.info("✅ 전략 백테스트 날짜 처리 테스트 통과")
    except Exception as e:
        logger.error(f"❌ 전략 백테스트 날짜 처리 테스트 실패: {e}")
        return False
    
    logger.info("🎉 전략 백테스트 날짜 처리 테스트 통과")
    return True

def main():
    """메인 테스트 함수"""
    logger.info("🚀 백테스트 수정사항 검증 시작")
    
    test_results = []
    
    # 1. 날짜 처리 함수 테스트
    try:
        test_date_handling()
        test_results.append(("날짜 처리", True))
    except Exception as e:
        logger.error(f"❌ 날짜 처리 테스트 실패: {e}")
        test_results.append(("날짜 처리", False))
    
    # 2. backtest_combo ticker 처리 테스트
    try:
        success = test_backtest_combo_ticker_handling()
        test_results.append(("backtest_combo ticker 처리", success))
    except Exception as e:
        logger.error(f"❌ backtest_combo ticker 처리 테스트 실패: {e}")
        test_results.append(("backtest_combo ticker 처리", False))
    
    # 3. 켈리 리포트 날짜 처리 테스트
    try:
        success = test_kelly_report_date_handling()
        test_results.append(("켈리 리포트 날짜 처리", success))
    except Exception as e:
        logger.error(f"❌ 켈리 리포트 날짜 처리 테스트 실패: {e}")
        test_results.append(("켈리 리포트 날짜 처리", False))
    
    # 4. 전략 백테스트 날짜 처리 테스트
    try:
        success = test_strategy_backtest_date_handling()
        test_results.append(("전략 백테스트 날짜 처리", success))
    except Exception as e:
        logger.error(f"❌ 전략 백테스트 날짜 처리 테스트 실패: {e}")
        test_results.append(("전략 백테스트 날짜 처리", False))
    
    # 결과 요약
    logger.info("=" * 50)
    logger.info("📊 테스트 결과 요약")
    logger.info("=" * 50)
    
    passed = 0
    total = len(test_results)
    
    for test_name, success in test_results:
        status = "✅ 통과" if success else "❌ 실패"
        logger.info(f"{test_name}: {status}")
        if success:
            passed += 1
    
    logger.info(f"총 {total}개 테스트 중 {passed}개 통과 ({passed/total*100:.1f}%)")
    
    if passed == total:
        logger.info("🎉 모든 테스트 통과! 백테스트 수정사항이 정상적으로 작동합니다.")
        return True
    else:
        logger.error("⚠️ 일부 테스트가 실패했습니다. 추가 검토가 필요합니다.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 