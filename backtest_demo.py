#!/usr/bin/env python3
"""
백테스트 시스템 통합 데모

새로운 MakenaideBacktestManager 통합 인터페이스를 활용한 데모입니다.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
from pathlib import Path
import logging

# 새로운 통합 백테스터 import
try:
    from backtester import MakenaideBacktestManager, StrategyConfig, BacktestResult
    INTEGRATED_AVAILABLE = True
except ImportError:
    print("⚠️ 통합 백테스터를 import할 수 없습니다. 간단한 데모 모드로 실행합니다.")
    INTEGRATED_AVAILABLE = False

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

class MakenaideDemo:
    """Makenaide 백테스트 시스템 데모 클래스"""
    
    def __init__(self):
        if INTEGRATED_AVAILABLE:
            self.backtest_manager = MakenaideBacktestManager()
            logger.info("🚀 통합 백테스트 매니저 초기화 완료")
        else:
            self.backtest_manager = None
            logger.warning("⚠️ 간단한 데모 모드로 실행")
    
    def run_full_demo(self) -> Dict:
        """전체 데모 실행"""
        logger.info("🎯 Makenaide 백테스트 데모 시작")
        
        if not INTEGRATED_AVAILABLE:
            return self._run_simple_demo()
        
        results = {}
        
        try:
            # 1. 전체 백테스트 수트 실행
            logger.info("📊 1단계: 전체 백테스트 수트 실행")
            full_results = self.backtest_manager.execute_full_backtest_suite(period_days=365)
            results['full_backtest'] = full_results
            
            # 2. 전략 비교 분석
            logger.info("🔍 2단계: 전략 비교 분석")
            strategy_comparison = self.backtest_manager.run_strategy_comparison([
                'Static_Donchian_Supertrend',
                'Dynamic_RSI_MACD',
                'Hybrid_VCP_Momentum'
            ], period_days=180)
            results['strategy_comparison'] = strategy_comparison
            
            # 3. 포트폴리오 최적화
            logger.info("⚖️ 3단계: 포트폴리오 할당 최적화")
            portfolio_optimization = self.backtest_manager.optimize_portfolio_allocation([
                'Static_Donchian_Supertrend',
                'Dynamic_RSI_MACD',
                'Hybrid_VCP_Momentum'
            ], target_risk=0.15)
            results['portfolio_optimization'] = portfolio_optimization
            
            # 4. 하이브리드 필터링 성능 분석
            logger.info("🔄 4단계: 하이브리드 필터링 성능 분석")
            hybrid_performance, optimal_weights = self.backtest_manager.hybrid_backtester.compare_hybrid_vs_static(
                start_date="2024-01-01",
                end_date="2024-12-31"
            )
            results['hybrid_analysis'] = {
                'performance': hybrid_performance,
                'optimal_weights': optimal_weights
            }
            
            # 5. 데모 리포트 생성
            demo_report = self._generate_demo_report(results)
            results['demo_report'] = demo_report
            
            logger.info("✅ 전체 데모 완료")
            
        except Exception as e:
            logger.error(f"❌ 데모 실행 중 오류: {e}")
            results['error'] = str(e)
        
        return results
    
    def run_quick_demo(self) -> Dict:
        """빠른 데모 실행 (30일)"""
        logger.info("⚡ 빠른 데모 실행 (30일)")
        
        if not INTEGRATED_AVAILABLE:
            return self._run_simple_demo()
        
        results = {}
        
        try:
            # 짧은 기간 백테스트
            quick_results = self.backtest_manager.execute_full_backtest_suite(period_days=30)
            results['quick_backtest'] = quick_results
            
            # 주요 전략만 비교
            strategy_comparison = self.backtest_manager.run_strategy_comparison([
                'Static_Donchian_Supertrend',
                'Hybrid_VCP_Momentum'
            ], period_days=30)
            results['strategy_comparison'] = strategy_comparison
            
            logger.info("✅ 빠른 데모 완료")
            
        except Exception as e:
            logger.error(f"❌ 빠른 데모 실행 중 오류: {e}")
            results['error'] = str(e)
        
        return results
    
    def _run_simple_demo(self) -> Dict:
        """간단한 데모 (통합 시스템을 사용할 수 없을 때)"""
        logger.info("🔧 간단한 데모 모드 실행")
        
        # 모의 데이터 생성
        mock_data = self._generate_mock_data(90)
        
        # 간단한 전략 결과 시뮬레이션
        strategies = ['Moving Average', 'Mean Reversion', 'Momentum']
        results = {}
        
        for strategy in strategies:
            # 랜덤한 성과 데이터 생성 (실제 계산 대신)
            np.random.seed(hash(strategy) % 2**32)
            performance = {
                'total_return': np.random.normal(0.08, 0.15),
                'sharpe_ratio': np.random.normal(0.8, 0.4),
                'max_drawdown': -abs(np.random.normal(0.12, 0.08)),
                'win_rate': np.random.uniform(0.45, 0.75),
                'total_trades': np.random.randint(20, 100)
            }
            results[strategy] = performance
        
        return {
            'simple_demo': True,
            'strategies': results,
            'note': '통합 시스템을 사용할 수 없어 간단한 모의 결과를 표시합니다.'
        }
    
    def _generate_mock_data(self, days: int) -> pd.DataFrame:
        """모의 시장 데이터 생성"""
        dates = pd.date_range(start='2024-01-01', periods=days, freq='D')
        np.random.seed(42)
        
        # 랜덤 워크로 가격 생성
        returns = np.random.normal(0.001, 0.02, days)
        prices = 100 * np.exp(np.cumsum(returns))
        
        return pd.DataFrame({
            'date': dates,
            'close': prices,
            'high': prices * (1 + np.abs(np.random.normal(0, 0.01, days))),
            'low': prices * (1 - np.abs(np.random.normal(0, 0.01, days))),
            'volume': np.random.randint(1000000, 10000000, days)
        })
    
    def _generate_demo_report(self, results: Dict) -> Dict:
        """데모 리포트 생성"""
        report = {
            'demo_date': datetime.now().isoformat(),
            'summary': {},
            'key_findings': [],
            'recommendations': []
        }
        
        # 전체 백테스트 요약
        if 'full_backtest' in results and results['full_backtest']:
            full_summary = results['full_backtest'].get('summary', {})
            report['summary']['strategies_tested'] = full_summary.get('total_strategies_tested', 0)
            report['summary']['optimization_applied'] = full_summary.get('optimization_applied', False)
            report['summary']['hybrid_enabled'] = full_summary.get('hybrid_filtering_enabled', False)
        
        # 전략 비교 결과
        if 'strategy_comparison' in results and results['strategy_comparison']:
            comparison = results['strategy_comparison']
            if 'best_strategy' in comparison:
                report['key_findings'].append(f"최고 성과 전략: {comparison['best_strategy']}")
        
        # 포트폴리오 최적화 결과
        if 'portfolio_optimization' in results and results['portfolio_optimization']:
            portfolio = results['portfolio_optimization']
            if 'expected_return' in portfolio:
                report['key_findings'].append(f"포트폴리오 예상 수익률: {portfolio['expected_return']:.2%}")
        
        # 하이브리드 분석 결과
        if 'hybrid_analysis' in results:
            hybrid = results['hybrid_analysis']
            if 'performance' in hybrid:
                hybrid_perf = hybrid['performance']
                if 'hybrid_filtering' in hybrid_perf and 'static_only' in hybrid_perf:
                    hybrid_return = hybrid_perf['hybrid_filtering'].get('total_return', 0)
                    static_return = hybrid_perf['static_only'].get('total_return', 0)
                    if hybrid_return > static_return:
                        report['key_findings'].append("하이브리드 필터링이 정적 전략보다 우수한 성과")
        
        # 기본 추천사항
        report['recommendations'] = [
            "🏆 최고 성과 전략을 기반으로 포트폴리오 구성 권장",
            "🔄 하이브리드 필터링 적용으로 성능 향상 기대",
            "⚖️ 리스크 관리를 위한 포트폴리오 분산 필요",
            "📊 정기적인 성과 모니터링 및 재최적화 권장"
        ]
        
        return report
    
    def save_demo_results(self, results: Dict, output_dir: str = "demo_results") -> str:
        """데모 결과 저장"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # JSON 결과 저장
        json_file = output_path / f"demo_backtest_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        # 텍스트 요약 저장
        if 'demo_report' in results:
            summary_file = output_path / f"demo_summary_{timestamp}.txt"
            with open(summary_file, 'w', encoding='utf-8') as f:
                report = results['demo_report']
                f.write("=== Makenaide 백테스트 데모 결과 ===\n\n")
                f.write(f"실행 일시: {report.get('demo_date', 'N/A')}\n\n")
                
                # 요약 정보
                if 'summary' in report:
                    f.write("📊 요약:\n")
                    for key, value in report['summary'].items():
                        f.write(f"  - {key}: {value}\n")
                    f.write("\n")
                
                # 주요 발견사항
                if 'key_findings' in report:
                    f.write("🔍 주요 발견사항:\n")
                    for finding in report['key_findings']:
                        f.write(f"  - {finding}\n")
                    f.write("\n")
                
                # 추천사항
                if 'recommendations' in report:
                    f.write("💡 추천사항:\n")
                    for rec in report['recommendations']:
                        f.write(f"  - {rec}\n")
        
        logger.info(f"📁 데모 결과 저장 완료: {json_file}")
        return str(json_file)

def run_demo():
    """데모 실행"""
    print("🎯 Makenaide 백테스트 시스템 통합 데모 시작\n")
    
    # 데모 실행
    demo = MakenaideDemo()
    report = demo.run_full_demo()
    
    # 결과 출력
    print("\n" + "="*60)
    print("📊 Makenaide 백테스트 데모 결과")
    print("="*60)
    
    summary = report.get('summary', {})
    print(f"📈 테스트 기간: {report.get('test_period', 'N/A')}")
    print(f"🎯 테스트된 전략 수: {summary.get('total_strategies', 0)}")
    print(f"🏆 최고 성능 전략: {summary.get('best_strategy', 'N/A')}")
    print(f"📊 최고 점수: {summary.get('best_score', 0):.3f}")
    print(f"💰 최고 수익률: {summary.get('best_return', 0):.2%}")
    print(f"📈 최고 샤프 비율: {summary.get('best_sharpe', 0):.2f}")
    
    print("\n🏆 전략 순위:")
    rankings = report.get('rankings', [])
    for i, strategy in enumerate(rankings, 1):
        metrics = strategy['metrics']
        print(
            f"{i}. {strategy['strategy_name']}: "
            f"점수 {strategy['composite_score']:.3f}, "
            f"수익률 {metrics.get('total_return', 0):.2%}, "
            f"샤프 {metrics.get('sharpe_ratio', 0):.2f}, "
            f"거래 수 {metrics.get('total_trades', 0)}"
        )
    
    print("\n💡 추천사항:")
    for rec in report.get('recommendations', []):
        print(f"- {rec}")
    
    # 결과 저장
    saved_file = demo.save_demo_results(report)
    print(f"\n💾 상세 결과 저장됨: {saved_file}")
    
    return report

if __name__ == "__main__":
    run_demo()