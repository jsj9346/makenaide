#!/usr/bin/env python3
"""
종합 백테스트 시스템 실행 스크립트

사용법:
python run_comprehensive_backtest.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--mode MODE]

모드:
- demo: 데모 모드 (기본값)
- full: 전체 시스템 분석 (DB 연결 필요)
- simple: 간단한 백테스트만 실행
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

def run_demo_mode(start_date: str = None, end_date: str = None):
    """데모 모드 실행"""
    try:
        print("🎯 데모 모드 백테스트 시작")
        
        # 데모 백테스트 실행
        from backtest_demo import run_demo
        result = run_demo()
        
        return result
        
    except Exception as e:
        print(f"❌ 데모 모드 실행 실패: {e}")
        return None

def run_full_mode(start_date: str, end_date: str):
    """전체 시스템 모드 실행"""
    try:
        print("🎯 전체 시스템 백테스트 시작")
        
        # 통합 백테스트 시스템 실행
        from backtest_integration import run_integrated_backtest_demo
        result = run_integrated_backtest_demo()
        
        return result
        
    except ImportError as e:
        print(f"❌ 모듈 import 실패: {e}")
        print("💡 데모 모드로 전환합니다.")
        return run_demo_mode(start_date, end_date)
    except Exception as e:
        print(f"❌ 전체 시스템 실행 실패: {e}")
        return None

def run_simple_mode():
    """간단 백테스트 모드"""
    try:
        print("🎯 간단 백테스트 시작")
        
        # 종합 백테스터만 실행
        from comprehensive_backtester import run_comprehensive_backtest_demo
        result = run_comprehensive_backtest_demo()
        
        return result
        
    except ImportError as e:
        print(f"❌ 모듈 import 실패: {e}")
        print("💡 데모 모드로 전환합니다.")
        return run_demo_mode()
    except Exception as e:
        print(f"❌ 간단 백테스트 실행 실패: {e}")
        return None

def validate_date(date_string: str) -> bool:
    """날짜 형식 검증"""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='종합 백테스트 시스템',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python run_comprehensive_backtest.py                           # 데모 모드
  python run_comprehensive_backtest.py --mode full               # 전체 시스템
  python run_comprehensive_backtest.py --mode simple             # 간단 백테스트
  python run_comprehensive_backtest.py --start-date 2023-01-01 --end-date 2023-12-31
        """
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        help='백테스트 시작 날짜 (YYYY-MM-DD 형식)',
        default=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='백테스트 종료 날짜 (YYYY-MM-DD 형식)',
        default=datetime.now().strftime('%Y-%m-%d')
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['demo', 'full', 'simple'],
        default='demo',
        help='실행 모드 선택'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='backtest_results',
        help='결과 저장 디렉토리'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='최소한의 출력만 표시'
    )
    
    args = parser.parse_args()
    
    # 날짜 검증
    if not validate_date(args.start_date):
        print(f"❌ 잘못된 시작 날짜 형식: {args.start_date}")
        sys.exit(1)
    
    if not validate_date(args.end_date):
        print(f"❌ 잘못된 종료 날짜 형식: {args.end_date}")
        sys.exit(1)
    
    # 날짜 순서 검증
    start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    if start_dt >= end_dt:
        print(f"❌ 시작 날짜가 종료 날짜보다 늦습니다: {args.start_date} >= {args.end_date}")
        sys.exit(1)
    
    # 출력 디렉토리 생성
    Path(args.output_dir).mkdir(exist_ok=True)
    
    if not args.quiet:
        print("="*60)
        print("📊 종합 백테스트 시스템")
        print("="*60)
        print(f"🗓️ 테스트 기간: {args.start_date} ~ {args.end_date}")
        print(f"🔧 실행 모드: {args.mode}")
        print(f"📁 결과 저장: {args.output_dir}")
        print()
    
    # 모드별 실행
    try:
        if args.mode == 'demo':
            result = run_demo_mode(args.start_date, args.end_date)
        elif args.mode == 'full':
            result = run_full_mode(args.start_date, args.end_date)
        elif args.mode == 'simple':
            result = run_simple_mode()
        else:
            print(f"❌ 알 수 없는 모드: {args.mode}")
            sys.exit(1)
        
        if result is None:
            print("❌ 백테스트 실행 실패")
            sys.exit(1)
        
        if not args.quiet:
            print("\n✅ 백테스트 완료!")
            
            # 간단한 결과 요약 출력
            if 'summary' in result:
                summary = result['summary']
                print(f"🏆 최고 전략: {summary.get('best_strategy', 'N/A')}")
                print(f"📊 최고 점수: {summary.get('best_score', 0):.3f}")
                
                if 'best_return' in summary:
                    print(f"💰 최고 수익률: {summary.get('best_return', 0):.2%}")
    
    except KeyboardInterrupt:
        print("\n❌ 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 예상치 못한 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 