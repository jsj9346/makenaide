#!/usr/bin/env python3
"""
🧹 Makenaide 레거시 Lambda 함수 안전 삭제 스크립트
미사용 Lambda 함수들을 안전하게 제거하여 비용 최적화
"""

import boto3
import json
import logging
from datetime import datetime
from typing import List, Dict
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LambdaCleanupManager:
    """Lambda 함수 정리 관리자"""
    
    def __init__(self):
        self.region = 'ap-northeast-2'
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        self.events_client = boto3.client('events', region_name=self.region)
        
        # 활성 핵심 함수들 (삭제 금지)
        self.critical_functions = {
            'makenaide-scanner',  # Phase 0
            'makenaide-data-collector',  # Phase 1
            'makenaide-comprehensive-filter-phase2',  # Phase 2
            'makenaide-gpt-analysis-phase3',  # Phase 3
            'makenaide-4h-analysis-phase4',  # Phase 4
            'makenaide-condition-check-phase5',  # Phase 5
            'makenaide-trade-execution-phase6',  # Phase 6
            'makenaide-integrated-orchestrator-v2',  # 통합 orchestrator
            'makenaide-market-sentiment-check',  # 시장 분석
            'makenaide-basic-RDB-controller',  # RDS 제어
            'makenaide-db-initializer',  # DB 초기화
        }
        
        # 안전 삭제 대상 (로그 사용량 0바이트인 미사용 함수들)
        self.safe_deletion_targets = [
            'makenaide-RDB-shutdown',
            'makenaide-advanced-orchestrator',
            'makenaide-api-gateway', 
            'makenaide-backtest-result-collector',
            'makenaide-basic-controller',
            'makenaide-basic-orchestrator',
            'makenaide-basic-shutdown',
            'makenaide-batch-processor',
            'makenaide-controller',
            'makenaide-data-buffer',
            'makenaide-distributed-backtest-worker',
            'makenaide-ec2-controller',
            'makenaide-ec2-shutdown',
            'makenaide-notification-tester',
            'makenaide-ohlcv-collector',
            'makenaide-orchestrator',
            'makenaide-phase2-comprehensive-filter',
            'makenaide-phase3-gpt-analysis',
            'makenaide-phase4-4h-analysis',
            'makenaide-phase5-condition-check',
            'makenaide-phase6-trade-execution',
            'makenaide-rds-controller',
            'makenaide-shutdown',
            'makenaide-slack-notifier',
            'makenaide-ticker-scanner'
        ]
        
        logger.info("🧹 Lambda 정리 관리자 초기화 완료")
    
    def verify_function_usage(self, function_name: str) -> Dict:
        """함수 사용량 검증"""
        try:
            # 함수 정보 가져오기
            response = self.lambda_client.get_function(FunctionName=function_name)
            
            # EventBridge 타겟으로 사용되는지 확인
            is_eventbridge_target = self._check_eventbridge_usage(function_name)
            
            # 최근 수정일 확인
            last_modified = response['Configuration']['LastModified']
            
            return {
                'exists': True,
                'last_modified': last_modified,
                'runtime': response['Configuration']['Runtime'],
                'memory_size': response['Configuration']['MemorySize'],
                'is_eventbridge_target': is_eventbridge_target,
                'safe_to_delete': not is_eventbridge_target and function_name not in self.critical_functions
            }
            
        except Exception as e:
            logger.error(f"함수 {function_name} 검증 실패: {str(e)}")
            return {'exists': False, 'error': str(e)}
    
    def _check_eventbridge_usage(self, function_name: str) -> bool:
        """EventBridge에서 함수가 사용되는지 확인"""
        try:
            # 모든 규칙에서 타겟 확인
            rules = self.events_client.list_rules(NamePrefix='makenaide-')['Rules']
            
            for rule in rules:
                targets = self.events_client.list_targets_by_rule(Rule=rule['Name'])['Targets']
                for target in targets:
                    if function_name in target.get('Arn', ''):
                        return True
            return False
            
        except Exception as e:
            logger.warning(f"EventBridge 확인 실패 ({function_name}): {str(e)}")
            return True  # 안전을 위해 True 반환
    
    def create_backup_info(self, functions_to_delete: List[str]) -> str:
        """삭제 전 백업 정보 생성"""
        try:
            backup_data = {
                'backup_timestamp': datetime.utcnow().isoformat(),
                'deleted_functions': {}
            }
            
            for func_name in functions_to_delete:
                try:
                    response = self.lambda_client.get_function(FunctionName=func_name)
                    backup_data['deleted_functions'][func_name] = {
                        'runtime': response['Configuration']['Runtime'],
                        'memory_size': response['Configuration']['MemorySize'],
                        'timeout': response['Configuration']['Timeout'],
                        'last_modified': response['Configuration']['LastModified'],
                        'description': response['Configuration'].get('Description', ''),
                        'handler': response['Configuration']['Handler']
                    }
                except Exception as e:
                    logger.warning(f"백업 정보 수집 실패 ({func_name}): {str(e)}")
            
            # 백업 파일 저장
            backup_filename = f'/Users/13ruce/makenaide/lambda_backup_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.json'
            with open(backup_filename, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ 백업 정보 저장: {backup_filename}")
            return backup_filename
            
        except Exception as e:
            logger.error(f"❌ 백업 생성 실패: {str(e)}")
            return None
    
    def safe_delete_function(self, function_name: str) -> bool:
        """안전한 함수 삭제"""
        try:
            # 최종 안전성 검증
            if function_name in self.critical_functions:
                logger.error(f"🚫 중요 함수 삭제 시도 차단: {function_name}")
                return False
            
            # 삭제 실행
            logger.info(f"🗑️  함수 삭제 중: {function_name}")
            
            self.lambda_client.delete_function(FunctionName=function_name)
            
            logger.info(f"✅ 함수 삭제 완료: {function_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 함수 삭제 실패 ({function_name}): {str(e)}")
            return False
    
    def execute_cleanup(self, dry_run: bool = True) -> Dict:
        """정리 작업 실행"""
        try:
            logger.info(f"🚀 Lambda 정리 작업 시작 (DRY_RUN: {dry_run})")
            
            # 삭제 대상 검증
            verified_targets = []
            protected_functions = []
            
            for func_name in self.safe_deletion_targets:
                verification = self.verify_function_usage(func_name)
                
                if verification.get('exists'):
                    if verification.get('safe_to_delete'):
                        verified_targets.append(func_name)
                        logger.info(f"🎯 삭제 대상 확인: {func_name}")
                    else:
                        protected_functions.append(func_name)
                        logger.warning(f"🛡️  보호된 함수: {func_name}")
                else:
                    logger.info(f"ℹ️  이미 존재하지 않음: {func_name}")
            
            if not dry_run and verified_targets:
                # 백업 정보 생성
                backup_file = self.create_backup_info(verified_targets)
                
                # 실제 삭제 실행
                deleted_count = 0
                failed_deletions = []
                
                for func_name in verified_targets:
                    if self.safe_delete_function(func_name):
                        deleted_count += 1
                        time.sleep(1)  # API 제한 방지
                    else:
                        failed_deletions.append(func_name)
                
                result = {
                    'action': 'EXECUTION',
                    'deleted_count': deleted_count,
                    'failed_deletions': failed_deletions,
                    'protected_functions': protected_functions,
                    'backup_file': backup_file,
                    'estimated_monthly_savings': len(verified_targets) * 0.20  # 함수당 월 $0.20 절약 추정
                }
                
                logger.info(f"🎉 정리 완료: {deleted_count}개 함수 삭제, 월 ${result['estimated_monthly_savings']:.2f} 절약 예상")
                
            else:
                # DRY RUN 결과
                result = {
                    'action': 'DRY_RUN',
                    'deletion_targets': verified_targets,
                    'protected_functions': protected_functions,
                    'estimated_monthly_savings': len(verified_targets) * 0.20
                }
                
                logger.info(f"📋 DRY RUN 결과: {len(verified_targets)}개 함수 삭제 가능, 월 ${result['estimated_monthly_savings']:.2f} 절약 예상")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 정리 작업 실패: {str(e)}")
            return {'error': str(e)}
    
    def generate_cleanup_report(self) -> str:
        """정리 리포트 생성"""
        try:
            logger.info("📋 정리 리포트 생성 중...")
            
            report = f"""
🧹 Makenaide Lambda 함수 정리 리포트
생성일: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 현재 상태:
• 전체 Lambda 함수: 29개
• 활성 핵심 함수: {len(self.critical_functions)}개 (유지)
• 삭제 대상: {len(self.safe_deletion_targets)}개 (미사용)

🎯 삭제 대상 함수들:
{chr(10).join(f"   🔴 {func}" for func in self.safe_deletion_targets)}

✅ 유지할 핵심 함수들:
{chr(10).join(f"   🟢 {func}" for func in sorted(self.critical_functions))}

💰 예상 비용 절약:
• 함수 삭제: {len(self.safe_deletion_targets)}개 × $0.20/월 = ${len(self.safe_deletion_targets) * 0.20:.2f}/월
• CloudWatch 로그: 약 $1-2/월 추가 절약
• 총 예상 절약: ${len(self.safe_deletion_targets) * 0.20 + 1.5:.2f}/월

🚀 다음 단계:
1. DRY RUN 실행: python cleanup_legacy_lambda_functions.py --dry-run
2. 실제 삭제: python cleanup_legacy_lambda_functions.py --execute
3. CloudWatch 로그 그룹 정리
4. 성과 모니터링

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            """
            
            return report.strip()
            
        except Exception as e:
            logger.error(f"❌ 리포트 생성 실패: {str(e)}")
            return f"리포트 생성 실패: {str(e)}"

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Makenaide Lambda 함수 정리 도구')
    parser.add_argument('--dry-run', action='store_true', help='실제 삭제 없이 계획만 표시')
    parser.add_argument('--execute', action='store_true', help='실제 삭제 실행')
    parser.add_argument('--report', action='store_true', help='정리 리포트만 생성')
    
    args = parser.parse_args()
    
    cleanup_manager = LambdaCleanupManager()
    
    if args.report:
        print(cleanup_manager.generate_cleanup_report())
        return
    
    # 기본값은 DRY RUN
    dry_run = not args.execute
    
    if args.execute:
        print("⚠️  실제 삭제를 진행합니다. 계속하시겠습니까? (y/N): ", end="")
        confirmation = input().strip().lower()
        if confirmation != 'y':
            print("❌ 작업이 취소되었습니다.")
            return
    
    # 정리 작업 실행
    result = cleanup_manager.execute_cleanup(dry_run=dry_run)
    
    print(f"\n📋 정리 작업 결과:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    if dry_run:
        print(f"\n🔍 실제 삭제를 원하면: python {__file__} --execute")
    else:
        print(f"\n🎉 Lambda 함수 정리 완료!")

if __name__ == "__main__":
    main()