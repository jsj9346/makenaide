#!/usr/bin/env python3
"""
백테스트 결과 분석 및 조회 도구
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import argparse
import logging

# 현재 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtester import BacktestDataManager
from utils import setup_logger

# 로거 설정
logger = setup_logger()

class BacktestAnalyzer:
    """백테스트 결과 분석 도구"""
    
    def __init__(self):
        self.backtest_manager = BacktestDataManager()
    
    def list_sessions(self, limit: int = 10):
        """백테스트 세션 목록 조회"""
        sessions = self.backtest_manager.list_active_sessions()
        
        if not sessions:
            print("⚠️ 활성 백테스트 세션이 없습니다.")
            return
        
        print(f"📊 활성 백테스트 세션 ({len(sessions)}개):")
        print("-" * 80)
        
        for i, session in enumerate(sessions[:limit], 1):
            print(f"{i:2d}. {session['name']}")
            print(f"    세션 ID: {session['session_id']}")
            print(f"    기간: {session['period_start']} ~ {session['period_end']}")
            print(f"    생성일: {session['created_at']}")
            print()
    
    def show_session_results(self, session_id: str = None):
        """특정 세션의 백테스트 결과 조회"""
        results = self.backtest_manager.get_backtest_results_from_db(session_id)
        
        if results.empty:
            print("⚠️ 해당 세션의 백테스트 결과가 없습니다.")
            return
        
        print(f"📈 백테스트 결과 ({len(results)}개 전략):")
        print("-" * 80)
        
        # 기본 통계
        avg_return = results['avg_return'].mean()
        avg_win_rate = results['win_rate'].mean()
        avg_mdd = results['mdd'].mean()
        
        print(f"📊 전체 통계:")
        print(f"   - 평균 수익률: {avg_return:.2%}")
        print(f"   - 평균 승률: {avg_win_rate:.2%}")
        print(f"   - 평균 최대 낙폭: {avg_mdd:.2%}")
        print()
        
        # 전략별 상세 결과
        print("📋 전략별 상세 결과:")
        print(f"{'전략명':<25} {'수익률':<8} {'승률':<8} {'최대낙폭':<10} {'거래수':<6} {'켈리비율':<8}")
        print("-" * 80)
        
        for _, result in results.iterrows():
            print(f"{result['strategy_name']:<25} {result['avg_return']:>7.2%} {result['win_rate']:>7.2%} "
                  f"{result['mdd']:>9.2%} {result['total_trades']:>6} {result['kelly_fraction']:>7.3f}")
        
        print()
        
        # Top 3 전략
        top_strategies = results.nlargest(3, 'avg_return')
        print("🏆 Top 3 전략:")
        for i, (_, strategy) in enumerate(top_strategies.iterrows(), 1):
            print(f"   {i}위: {strategy['strategy_name']} (수익률: {strategy['avg_return']:.2%})")
    
    def compare_sessions(self, session_ids: list):
        """여러 세션의 성과 비교"""
        if len(session_ids) < 2:
            print("⚠️ 비교할 세션이 2개 이상 필요합니다.")
            return
        
        print("📊 세션별 성과 비교:")
        print("-" * 80)
        
        comparison_data = []
        
        for session_id in session_ids:
            results = self.backtest_manager.get_backtest_results_from_db(session_id)
            if not results.empty:
                session_info = self.backtest_manager.get_session_info(session_id)
                session_name = session_info['name'] if session_info else session_id
                
                avg_return = results['avg_return'].mean()
                avg_win_rate = results['win_rate'].mean()
                avg_mdd = results['mdd'].mean()
                strategy_count = len(results)
                
                comparison_data.append({
                    'session_name': session_name,
                    'avg_return': avg_return,
                    'avg_win_rate': avg_win_rate,
                    'avg_mdd': avg_mdd,
                    'strategy_count': strategy_count
                })
        
        if not comparison_data:
            print("⚠️ 비교할 데이터가 없습니다.")
            return
        
        # 비교 테이블 출력
        print(f"{'세션명':<30} {'평균수익률':<10} {'평균승률':<10} {'평균낙폭':<10} {'전략수':<6}")
        print("-" * 80)
        
        for data in comparison_data:
            print(f"{data['session_name']:<30} {data['avg_return']:>9.2%} {data['avg_win_rate']:>9.2%} "
                  f"{data['avg_mdd']:>9.2%} {data['strategy_count']:>6}")
        
        print()
        
        # 최고 성과 세션
        best_session = max(comparison_data, key=lambda x: x['avg_return'])
        print(f"🏆 최고 성과 세션: {best_session['session_name']} (수익률: {best_session['avg_return']:.2%})")
    
    def generate_report(self, session_id: str = None, output_format: str = "markdown", output_file: str = None):
        """백테스트 분석 리포트 생성"""
        report = self.backtest_manager.generate_backtest_analysis_report(
            session_id=session_id, 
            output_format=output_format
        )
        
        if report.startswith("⚠️"):
            print(report)
            return
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"✅ 리포트가 {output_file}에 저장되었습니다.")
        else:
            print(report)
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """오래된 백테스트 데이터 정리"""
        print(f"🧹 {days_to_keep}일 이전 백테스트 데이터 정리 중...")
        
        cleanup_stats = self.backtest_manager.cleanup_old_backtest_results(days_to_keep)
        
        if 'error' not in cleanup_stats:
            print("✅ 정리 완료:")
            print(f"   - 삭제된 결과: {cleanup_stats.get('deleted_results', 0)}개")
            print(f"   - 삭제된 거래 기록: {cleanup_stats.get('deleted_trades', 0)}개")
            print(f"   - 삭제된 세션: {cleanup_stats.get('deleted_sessions', 0)}개")
        else:
            print(f"❌ 정리 실패: {cleanup_stats['error']}")
    
    def show_strategy_performance(self, strategy_name: str, limit_days: int = None):
        """특정 전략의 성과 분석"""
        results = self.backtest_manager.get_backtest_results_from_db(
            strategy_name=strategy_name, 
            limit_days=limit_days
        )
        
        if results.empty:
            print(f"⚠️ '{strategy_name}' 전략의 백테스트 결과가 없습니다.")
            return
        
        print(f"📈 '{strategy_name}' 전략 성과 분석:")
        print("-" * 80)
        
        # 기본 통계
        avg_return = results['avg_return'].mean()
        avg_win_rate = results['win_rate'].mean()
        avg_mdd = results['mdd'].mean()
        total_tests = len(results)
        
        print(f"📊 통계 요약:")
        print(f"   - 총 테스트 횟수: {total_tests}회")
        print(f"   - 평균 수익률: {avg_return:.2%}")
        print(f"   - 평균 승률: {avg_win_rate:.2%}")
        print(f"   - 평균 최대 낙폭: {avg_mdd:.2%}")
        print()
        
        # 최고/최저 성과
        best_result = results.loc[results['avg_return'].idxmax()]
        worst_result = results.loc[results['avg_return'].idxmin()]
        
        print(f"🏆 최고 성과:")
        print(f"   - 수익률: {best_result['avg_return']:.2%}")
        print(f"   - 승률: {best_result['win_rate']:.2%}")
        print(f"   - 최대 낙폭: {best_result['mdd']:.2%}")
        print(f"   - 테스트일: {best_result['created_at']}")
        print()
        
        print(f"📉 최저 성과:")
        print(f"   - 수익률: {worst_result['avg_return']:.2%}")
        print(f"   - 승률: {worst_result['win_rate']:.2%}")
        print(f"   - 최대 낙폭: {worst_result['mdd']:.2%}")
        print(f"   - 테스트일: {worst_result['created_at']}")

def main():
    parser = argparse.ArgumentParser(description='백테스트 결과 분석 도구')
    parser.add_argument('command', choices=['list', 'show', 'compare', 'report', 'cleanup', 'strategy'],
                       help='실행할 명령')
    parser.add_argument('--session-id', help='세션 ID')
    parser.add_argument('--strategy-name', help='전략명')
    parser.add_argument('--limit', type=int, default=10, help='조회 제한 수')
    parser.add_argument('--days', type=int, default=30, help='일수 제한')
    parser.add_argument('--format', choices=['markdown', 'html', 'json'], default='markdown',
                       help='리포트 형식')
    parser.add_argument('--output', help='출력 파일명')
    parser.add_argument('--session-ids', nargs='+', help='비교할 세션 ID 목록')
    
    args = parser.parse_args()
    
    analyzer = BacktestAnalyzer()
    
    try:
        if args.command == 'list':
            analyzer.list_sessions(args.limit)
        
        elif args.command == 'show':
            analyzer.show_session_results(args.session_id)
        
        elif args.command == 'compare':
            if args.session_ids:
                analyzer.compare_sessions(args.session_ids)
            else:
                print("⚠️ --session-ids 인수가 필요합니다.")
        
        elif args.command == 'report':
            output_file = args.output or f"backtest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{args.format}"
            analyzer.generate_report(args.session_id, args.format, output_file)
        
        elif args.command == 'cleanup':
            analyzer.cleanup_old_data(args.days)
        
        elif args.command == 'strategy':
            if args.strategy_name:
                analyzer.show_strategy_performance(args.strategy_name, args.days)
            else:
                print("⚠️ --strategy-name 인수가 필요합니다.")
        
    except Exception as e:
        logger.error(f"❌ 명령 실행 중 오류: {e}")
        print(f"❌ 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main() 