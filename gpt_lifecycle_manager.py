#!/usr/bin/env python3
"""
GPT 분석 결과 라이프사이클 관리 스크립트

이 스크립트는 GPT 분석 결과의 보관, 캐싱, 삭제를 체계적으로 관리합니다.
품질 기반 보관 정책과 시장 상황 기반 보관 정책을 적용합니다.

사용법:
    python gpt_lifecycle_manager.py --help
    python gpt_lifecycle_manager.py --stats
    python gpt_lifecycle_manager.py --cleanup
    python gpt_lifecycle_manager.py --dry-run
    python gpt_lifecycle_manager.py --config
"""

import argparse
import sys
import os
import logging
from datetime import datetime, timedelta

# 프로젝트 루트 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_manager import DBManager
from trend_analyzer import GPTAnalysisLifecycleManager, get_gpt_analysis_stats

def setup_logging():
    """로깅 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('gpt_lifecycle.log')
        ]
    )

def print_banner():
    """배너 출력"""
    print("=" * 60)
    print("🤖 GPT 분석 결과 라이프사이클 관리 시스템")
    print("=" * 60)

def show_statistics():
    """통계 정보 표시"""
    try:
        db_manager = DBManager()
        lifecycle_manager = GPTAnalysisLifecycleManager(db_manager)
        
        # 라이프사이클 통계
        lifecycle_stats = lifecycle_manager.get_cleanup_stats()
        print("\n📊 GPT 분석 결과 라이프사이클 통계:")
        print(f"  - 총 정리된 레코드: {lifecycle_stats.get('total_cleaned', 0):,}개")
        print(f"  - 품질 기반 정리: {lifecycle_stats.get('quality_based_cleaned', 0):,}개")
        print(f"  - 만료 기반 정리: {lifecycle_stats.get('expired_cleaned', 0):,}개")
        print(f"  - 오류 기반 정리: {lifecycle_stats.get('error_cleaned', 0):,}개")
        print(f"  - 마지막 정리: {lifecycle_stats.get('last_cleanup', 'N/A')}")
        
        # 전체 통계
        overall_stats = get_gpt_analysis_stats(db_manager)
        print("\n📈 전체 GPT 분석 통계:")
        print(f"  - 총 레코드: {overall_stats.get('total_records', 0):,}개")
        print(f"  - 최근 24시간: {overall_stats.get('recent_records_24h', 0):,}개")
        print(f"  - 최근 12시간: {overall_stats.get('fresh_records_12h', 0):,}개")
        print(f"  - 오래된 레코드: {overall_stats.get('old_records', 0):,}개")
        print(f"  - 정리 권장: {'예' if overall_stats.get('cleanup_recommended', False) else '아니오'}")
        
        # 상세 분석
        show_detailed_analysis(db_manager)
        
    except Exception as e:
        print(f"❌ 통계 조회 실패: {e}")
        return False
    
    return True

def show_detailed_analysis(db_manager: DBManager):
    """상세 분석 정보 표시"""
    try:
        # 품질별 분포
        quality_query = """
            SELECT 
                CASE 
                    WHEN score >= 80 THEN '고품질 (80+)'
                    WHEN score >= 60 THEN '중품질 (60-79)'
                    WHEN score >= 40 THEN '저품질 (40-59)'
                    ELSE '매우 낮음 (<40)'
                END as quality_level,
                COUNT(*) as count
            FROM trend_analysis 
            GROUP BY quality_level
            ORDER BY count DESC
        """
        
        result = db_manager.execute_query(quality_query)
        print("\n🎯 품질별 분포:")
        for row in result:
            print(f"  - {row[0]}: {row[1]:,}개")
        
        # 시장 단계별 분포
        phase_query = """
            SELECT 
                market_phase,
                COUNT(*) as count
            FROM trend_analysis 
            WHERE market_phase IS NOT NULL
            GROUP BY market_phase
            ORDER BY count DESC
        """
        
        result = db_manager.execute_query(phase_query)
        print("\n📊 시장 단계별 분포:")
        for row in result:
            print(f"  - {row[0]}: {row[1]:,}개")
        
        # 액션별 분포
        action_query = """
            SELECT 
                action,
                COUNT(*) as count
            FROM trend_analysis 
            GROUP BY action
            ORDER BY count DESC
        """
        
        result = db_manager.execute_query(action_query)
        print("\n⚡ 액션별 분포:")
        for row in result:
            print(f"  - {row[0]}: {row[1]:,}개")
        
    except Exception as e:
        print(f"⚠️ 상세 분석 실패: {e}")

def perform_cleanup(dry_run: bool = False):
    """정리 작업 수행"""
    try:
        db_manager = DBManager()
        
        if dry_run:
            # 시뮬레이션 모드
            config = {
                'cleanup_policy': {'dry_run_mode': True}
            }
            lifecycle_manager = GPTAnalysisLifecycleManager(db_manager, config)
            result = lifecycle_manager.force_cleanup()
            
            print("🔍 GPT 분석 결과 정리 시뮬레이션 완료:")
            print(f"  - 삭제될 레코드: {result.get('total_cleaned', 0):,}개")
            print(f"  - 품질 기반 삭제: {result.get('quality_based_cleaned', 0):,}개")
            print(f"  - 만료 기반 삭제: {result.get('expired_cleaned', 0):,}개")
            print(f"  - 오류 기반 삭제: {result.get('error_cleaned', 0):,}개")
            print("💡 실제 삭제를 원하면 --cleanup 명령어를 사용하세요.")
            
        else:
            # 실제 정리
            lifecycle_manager = GPTAnalysisLifecycleManager(db_manager)
            result = lifecycle_manager.force_cleanup()
            
            print("🧹 GPT 분석 결과 강제 정리 완료:")
            print(f"  - 총 정리된 레코드: {result.get('total_cleaned', 0):,}개")
            print(f"  - 품질 기반 정리: {result.get('quality_based_cleaned', 0):,}개")
            print(f"  - 만료 기반 정리: {result.get('expired_cleaned', 0):,}개")
            print(f"  - 오류 기반 정리: {result.get('error_cleaned', 0):,}개")
        
        return True
        
    except Exception as e:
        print(f"❌ 정리 작업 실패: {e}")
        return False

def show_config():
    """설정 정보 표시"""
    try:
        from config import GPT_ANALYSIS_LIFECYCLE
        
        print("\n⚙️ GPT 분석 결과 라이프사이클 설정:")
        print(f"  - 기본 보관 시간: {GPT_ANALYSIS_LIFECYCLE['retention_policy']['default_retention_hours']}시간")
        print(f"  - 고신뢰도 보관 시간: {GPT_ANALYSIS_LIFECYCLE['retention_policy']['high_confidence_retention_hours']}시간")
        print(f"  - 저신뢰도 보관 시간: {GPT_ANALYSIS_LIFECYCLE['retention_policy']['low_confidence_retention_hours']}시간")
        print(f"  - 오류 결과 보관 시간: {GPT_ANALYSIS_LIFECYCLE['retention_policy']['error_result_retention_hours']}시간")
        
        print(f"\n🔄 자동 정리 설정:")
        print(f"  - 자동 정리 활성화: {'예' if GPT_ANALYSIS_LIFECYCLE['cleanup_policy']['enable_auto_cleanup'] else '아니오'}")
        print(f"  - 정리 주기: {GPT_ANALYSIS_LIFECYCLE['cleanup_policy']['cleanup_interval_hours']}시간")
        print(f"  - 배치 정리 크기: {GPT_ANALYSIS_LIFECYCLE['cleanup_policy']['batch_cleanup_size']}개")
        
        print(f"\n📊 품질 기반 보관:")
        print(f"  - 활성화: {'예' if GPT_ANALYSIS_LIFECYCLE['quality_based_retention']['enabled'] else '아니오'}")
        print(f"  - 고품질 기준: {GPT_ANALYSIS_LIFECYCLE['quality_based_retention']['score_thresholds']['high_quality']}점")
        print(f"  - 중간품질 기준: {GPT_ANALYSIS_LIFECYCLE['quality_based_retention']['score_thresholds']['medium_quality']}점")
        print(f"  - 저품질 기준: {GPT_ANALYSIS_LIFECYCLE['quality_based_retention']['score_thresholds']['low_quality']}점")
        
        print(f"\n📈 시장 상황 기반 보관:")
        print(f"  - 활성화: {'예' if GPT_ANALYSIS_LIFECYCLE['market_condition_retention']['enabled'] else '아니오'}")
        print(f"  - Stage1 배수: {GPT_ANALYSIS_LIFECYCLE['market_condition_retention']['market_phase_multipliers']['Stage1']}")
        print(f"  - Stage2 배수: {GPT_ANALYSIS_LIFECYCLE['market_condition_retention']['market_phase_multipliers']['Stage2']}")
        print(f"  - BUY 액션 배수: {GPT_ANALYSIS_LIFECYCLE['market_condition_retention']['action_based_retention']['BUY']}")
        print(f"  - STRONG_BUY 액션 배수: {GPT_ANALYSIS_LIFECYCLE['market_condition_retention']['action_based_retention']['STRONG_BUY']}")
        
    except ImportError:
        print("⚠️ GPT_ANALYSIS_LIFECYCLE 설정을 찾을 수 없습니다.")
        print("💡 config.py 파일에 GPT_ANALYSIS_LIFECYCLE 설정이 추가되었는지 확인하세요.")
        
        # 기본 설정 정보 표시
        print("\n📋 현재 사용 중인 기본 설정:")
        print("  - 기본 보관 시간: 24시간")
        print("  - 자동 정리 활성화: 예")
        print("  - 정리 주기: 6시간")
        print("  - 품질 기반 보관: 예")
        print("  - 시장 상황 기반 보관: 예")
        
    except Exception as e:
        print(f"❌ 설정 조회 실패: {e}")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description="GPT 분석 결과 라이프사이클 관리 시스템",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python gpt_lifecycle_manager.py --stats          # 통계 정보 표시
  python gpt_lifecycle_manager.py --cleanup       # 실제 정리 실행
  python gpt_lifecycle_manager.py --dry-run       # 시뮬레이션 모드
  python gpt_lifecycle_manager.py --config        # 설정 정보 표시
        """
    )
    
    parser.add_argument('--stats', action='store_true', 
                       help='GPT 분석 결과 통계 정보 표시')
    parser.add_argument('--cleanup', action='store_true', 
                       help='GPT 분석 결과 실제 정리 실행')
    parser.add_argument('--dry-run', action='store_true', 
                       help='GPT 분석 결과 정리 시뮬레이션 실행')
    parser.add_argument('--config', action='store_true', 
                       help='라이프사이클 관리 설정 정보 표시')
    
    args = parser.parse_args()
    
    # 로깅 설정
    setup_logging()
    
    # 배너 출력
    print_banner()
    
    # 명령어 처리
    if args.stats:
        success = show_statistics()
        sys.exit(0 if success else 1)
    
    elif args.cleanup:
        print("\n⚠️ 실제 정리 작업을 시작합니다. 계속하시겠습니까? (y/N): ", end="")
        response = input().strip().lower()
        if response in ['y', 'yes']:
            success = perform_cleanup(dry_run=False)
            sys.exit(0 if success else 1)
        else:
            print("❌ 정리 작업이 취소되었습니다.")
            sys.exit(0)
    
    elif args.dry_run:
        success = perform_cleanup(dry_run=True)
        sys.exit(0 if success else 1)
    
    elif args.config:
        show_config()
        sys.exit(0)
    
    else:
        # 기본 동작: 통계 표시
        print("\n📊 기본 동작: 통계 정보 표시")
        success = show_statistics()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 