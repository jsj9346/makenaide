#!/usr/bin/env python3
"""
📊 Makenaide CloudWatch 모니터링 최적화 스크립트
불필요한 알람과 로그 그룹을 정리하여 비용 최적화
"""

import boto3
import json
import logging
from datetime import datetime
from typing import List, Dict, Set
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CloudWatchOptimizer:
    """CloudWatch 최적화 관리자"""
    
    def __init__(self):
        self.region = 'ap-northeast-2'
        self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
        self.logs_client = boto3.client('logs', region_name=self.region)
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        
        # 삭제된 Lambda 함수들 (orphaned log groups 대상)
        self.deleted_functions = {
            'makenaide-RDB-shutdown',
            'makenaide-backtest-result-collector', 
            'makenaide-basic-controller',
            'makenaide-basic-shutdown',
            'makenaide-data-buffer',
            'makenaide-distributed-backtest-worker',
            'makenaide-notification-tester',
            'makenaide-phase2-comprehensive-filter',
            'makenaide-phase3-gpt-analysis', 
            'makenaide-phase4-4h-analysis',
            'makenaide-phase5-condition-check',
            'makenaide-phase6-trade-execution',
            'makenaide-slack-notifier'
        }
        
        # 필수 유지할 알람 패턴
        self.critical_alarm_patterns = {
            'daily-cost-alert',
            'lambda-high-error-rate', 
            'ec2-high-cpu',
            'ec2-high-memory',
            'rds-high-connections',
            'ec2-disk-space'  # 디스크 공간은 중요
        }
        
        # 현재 활성 Lambda 함수들
        self.active_functions = set()
        
        logger.info("📊 CloudWatch 최적화 관리자 초기화 완료")
    
    def get_active_lambda_functions(self) -> Set[str]:
        """현재 활성 Lambda 함수 목록 가져오기"""
        try:
            response = self.lambda_client.list_functions()
            functions = {func['FunctionName'] for func in response['Functions'] 
                        if 'makenaide' in func['FunctionName']}
            
            logger.info(f"📋 현재 활성 Lambda 함수: {len(functions)}개")
            return functions
            
        except Exception as e:
            logger.error(f"❌ 활성 함수 목록 가져오기 실패: {str(e)}")
            return set()
    
    def identify_orphaned_log_groups(self) -> List[Dict]:
        """고아 로그 그룹 식별"""
        try:
            logger.info("🔍 고아 로그 그룹 식별 중...")
            
            # 현재 활성 함수 목록 업데이트
            self.active_functions = self.get_active_lambda_functions()
            
            response = self.logs_client.describe_log_groups(
                logGroupNamePrefix='/aws/lambda/makenaide'
            )
            
            orphaned_logs = []
            active_logs = []
            
            for log_group in response['logGroups']:
                log_name = log_group['logGroupName']
                function_name = log_name.replace('/aws/lambda/', '')
                
                # 삭제된 함수의 로그거나 존재하지 않는 함수의 로그인 경우
                is_orphaned = (
                    any(deleted in function_name for deleted in self.deleted_functions) or
                    function_name not in self.active_functions
                )
                
                log_info = {
                    'logGroupName': log_name,
                    'functionName': function_name,
                    'retentionInDays': log_group.get('retentionInDays'),
                    'storedBytes': log_group.get('storedBytes', 0),
                    'creationTime': log_group.get('creationTime')
                }
                
                if is_orphaned:
                    orphaned_logs.append(log_info)
                    logger.info(f"🔴 고아 로그: {function_name}")
                else:
                    active_logs.append(log_info)
            
            logger.info(f"📊 고아 로그 그룹: {len(orphaned_logs)}개, 활성 로그 그룹: {len(active_logs)}개")
            
            return orphaned_logs
            
        except Exception as e:
            logger.error(f"❌ 고아 로그 그룹 식별 실패: {str(e)}")
            return []
    
    def identify_unnecessary_alarms(self) -> List[Dict]:
        """불필요한 알람 식별"""
        try:
            logger.info("⚠️  불필요한 알람 식별 중...")
            
            response = self.cloudwatch.describe_alarms(
                AlarmNamePrefix='makenaide-'
            )
            
            unnecessary_alarms = []
            critical_alarms = []
            
            for alarm in response['MetricAlarms']:
                alarm_name = alarm['AlarmName']
                
                # 중요한 알람인지 확인
                is_critical = any(pattern in alarm_name for pattern in self.critical_alarm_patterns)
                
                # INSUFFICIENT_DATA 상태가 오랫동안 지속된 알람 확인
                is_inactive = alarm['StateValue'] == 'INSUFFICIENT_DATA'
                
                alarm_info = {
                    'AlarmName': alarm_name,
                    'StateValue': alarm['StateValue'],
                    'MetricName': alarm['MetricName'],
                    'Namespace': alarm['Namespace'],
                    'StateReason': alarm.get('StateReason', ''),
                    'is_critical': is_critical,
                    'is_inactive': is_inactive
                }
                
                if not is_critical and is_inactive:
                    # 삭제된 함수와 관련된 알람인지 확인
                    is_orphaned_alarm = any(deleted in alarm_name for deleted in self.deleted_functions)
                    
                    if is_orphaned_alarm or 'stepfunctions' in alarm_name or 'ohlcv-collector' in alarm_name:
                        unnecessary_alarms.append(alarm_info)
                        logger.info(f"🔴 불필요한 알람: {alarm_name}")
                    else:
                        logger.info(f"🟡 검토 대상: {alarm_name}")
                else:
                    critical_alarms.append(alarm_info)
                    logger.info(f"🟢 유지: {alarm_name}")
            
            logger.info(f"📊 불필요한 알람: {len(unnecessary_alarms)}개, 필수 알람: {len(critical_alarms)}개")
            
            return unnecessary_alarms
            
        except Exception as e:
            logger.error(f"❌ 불필요한 알람 식별 실패: {str(e)}")
            return []
    
    def delete_orphaned_log_groups(self, orphaned_logs: List[Dict], dry_run: bool = True) -> Dict:
        """고아 로그 그룹 삭제"""
        try:
            logger.info(f"🗑️  고아 로그 그룹 삭제 (DRY_RUN: {dry_run})")
            
            deleted_count = 0
            failed_deletions = []
            estimated_savings = 0
            
            for log_info in orphaned_logs:
                log_name = log_info['logGroupName']
                stored_bytes = log_info['storedBytes']
                
                # 저장 비용 계산 (GB당 $0.50/월)
                storage_gb = stored_bytes / (1024**3)
                monthly_cost = storage_gb * 0.50
                estimated_savings += monthly_cost
                
                if not dry_run:
                    try:
                        logger.info(f"🗑️  로그 그룹 삭제: {log_name}")
                        
                        self.logs_client.delete_log_group(
                            logGroupName=log_name
                        )
                        
                        deleted_count += 1
                        logger.info(f"✅ 삭제 완료: {log_name}")
                        
                        time.sleep(0.5)  # API 제한 방지
                        
                    except Exception as e:
                        logger.error(f"❌ 로그 그룹 삭제 실패 ({log_name}): {str(e)}")
                        failed_deletions.append(log_name)
                else:
                    logger.info(f"📋 삭제 예정: {log_name} (${monthly_cost:.4f}/월 절약)")
            
            result = {
                'action': 'EXECUTION' if not dry_run else 'DRY_RUN',
                'deleted_count': deleted_count if not dry_run else len(orphaned_logs),
                'failed_deletions': failed_deletions,
                'estimated_monthly_savings': estimated_savings
            }
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 고아 로그 그룹 삭제 실패: {str(e)}")
            return {'error': str(e)}
    
    def delete_unnecessary_alarms(self, unnecessary_alarms: List[Dict], dry_run: bool = True) -> Dict:
        """불필요한 알람 삭제"""
        try:
            logger.info(f"⚠️  불필요한 알람 삭제 (DRY_RUN: {dry_run})")
            
            deleted_count = 0
            failed_deletions = []
            estimated_savings = len(unnecessary_alarms) * 0.10  # 알람당 $0.10/월
            
            for alarm_info in unnecessary_alarms:
                alarm_name = alarm_info['AlarmName']
                
                if not dry_run:
                    try:
                        logger.info(f"🗑️  알람 삭제: {alarm_name}")
                        
                        self.cloudwatch.delete_alarms(
                            AlarmNames=[alarm_name]
                        )
                        
                        deleted_count += 1
                        logger.info(f"✅ 삭제 완료: {alarm_name}")
                        
                        time.sleep(0.3)  # API 제한 방지
                        
                    except Exception as e:
                        logger.error(f"❌ 알람 삭제 실패 ({alarm_name}): {str(e)}")
                        failed_deletions.append(alarm_name)
                else:
                    logger.info(f"📋 삭제 예정: {alarm_name}")
            
            result = {
                'action': 'EXECUTION' if not dry_run else 'DRY_RUN',
                'deleted_count': deleted_count if not dry_run else len(unnecessary_alarms),
                'failed_deletions': failed_deletions,
                'estimated_monthly_savings': estimated_savings
            }
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 불필요한 알람 삭제 실패: {str(e)}")
            return {'error': str(e)}
    
    def execute_optimization(self, dry_run: bool = True) -> Dict:
        """CloudWatch 최적화 실행"""
        try:
            logger.info(f"🚀 CloudWatch 최적화 시작 (DRY_RUN: {dry_run})")
            
            # 1. 고아 로그 그룹 처리
            orphaned_logs = self.identify_orphaned_log_groups()
            log_result = self.delete_orphaned_log_groups(orphaned_logs, dry_run)
            
            # 2. 불필요한 알람 처리  
            unnecessary_alarms = self.identify_unnecessary_alarms()
            alarm_result = self.delete_unnecessary_alarms(unnecessary_alarms, dry_run)
            
            # 결과 통합
            total_savings = (
                log_result.get('estimated_monthly_savings', 0) + 
                alarm_result.get('estimated_monthly_savings', 0)
            )
            
            result = {
                'action': 'EXECUTION' if not dry_run else 'DRY_RUN',
                'log_groups': log_result,
                'alarms': alarm_result,
                'total_monthly_savings': total_savings,
                'optimization_summary': {
                    'orphaned_logs_removed': log_result.get('deleted_count', 0),
                    'unnecessary_alarms_removed': alarm_result.get('deleted_count', 0),
                    'total_items_optimized': (
                        log_result.get('deleted_count', 0) + 
                        alarm_result.get('deleted_count', 0)
                    )
                }
            }
            
            logger.info(f"🎉 CloudWatch 최적화 완료: 월 ${total_savings:.2f} 절약 예상")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ CloudWatch 최적화 실패: {str(e)}")
            return {'error': str(e)}
    
    def generate_optimization_report(self) -> str:
        """최적화 리포트 생성"""
        try:
            logger.info("📋 최적화 리포트 생성 중...")
            
            # 현재 상태 분석
            orphaned_logs = self.identify_orphaned_log_groups()
            unnecessary_alarms = self.identify_unnecessary_alarms()
            
            log_savings = sum((log['storedBytes'] / (1024**3)) * 0.50 for log in orphaned_logs)
            alarm_savings = len(unnecessary_alarms) * 0.10
            total_savings = log_savings + alarm_savings
            
            report = f"""
📊 Makenaide CloudWatch 최적화 리포트
생성일: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 현재 상태:
• 전체 로그 그룹: {len(orphaned_logs) + len(self.active_functions)}개
• 고아 로그 그룹: {len(orphaned_logs)}개 (삭제 대상)
• 전체 알람: {len(unnecessary_alarms) + len(self.critical_alarm_patterns)}개 (추정)
• 불필요한 알람: {len(unnecessary_alarms)}개 (삭제 대상)

🗑️  삭제 대상 로그 그룹:
{chr(10).join(f"   🔴 {log['logGroupName']} ({log['storedBytes']:,} bytes)" for log in orphaned_logs)}

🗑️  삭제 대상 알람:
{chr(10).join(f"   🔴 {alarm['AlarmName']} ({alarm['StateValue']})" for alarm in unnecessary_alarms)}

💰 예상 비용 절약:
• 로그 저장소: ${log_savings:.2f}/월
• 불필요한 알람: ${alarm_savings:.2f}/월
• 총 절약: ${total_savings:.2f}/월

✅ 유지될 핵심 모니터링:
• 일일 비용 알림
• Lambda 오류율 모니터링  
• EC2 CPU/메모리 모니터링
• RDS 연결 상태 모니터링
• EC2 디스크 공간 모니터링
• 활성 함수 로그 (16개 함수)

🚀 실행 단계:
1. DRY RUN: python optimize_cloudwatch_monitoring.py --dry-run
2. 실제 최적화: python optimize_cloudwatch_monitoring.py --execute
3. 결과 검증 및 모니터링

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            """
            
            return report.strip()
            
        except Exception as e:
            logger.error(f"❌ 리포트 생성 실패: {str(e)}")
            return f"리포트 생성 실패: {str(e)}"

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Makenaide CloudWatch 최적화 도구')
    parser.add_argument('--dry-run', action='store_true', help='실제 삭제 없이 계획만 표시')
    parser.add_argument('--execute', action='store_true', help='실제 최적화 실행')
    parser.add_argument('--report', action='store_true', help='최적화 리포트만 생성')
    
    args = parser.parse_args()
    
    optimizer = CloudWatchOptimizer()
    
    if args.report:
        print(optimizer.generate_optimization_report())
        return
    
    # 기본값은 DRY RUN
    dry_run = not args.execute
    
    if args.execute:
        print("⚠️  실제 CloudWatch 최적화를 진행합니다. 계속하시겠습니까? (y/N): ", end="")
        confirmation = input().strip().lower()
        if confirmation != 'y':
            print("❌ 작업이 취소되었습니다.")
            return
    
    # 최적화 실행
    result = optimizer.execute_optimization(dry_run=dry_run)
    
    print(f"\n📋 CloudWatch 최적화 결과:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    if dry_run:
        print(f"\n🔍 실제 최적화를 원하면: python {__file__} --execute")
    else:
        print(f"\n🎉 CloudWatch 최적화 완료!")

if __name__ == "__main__":
    main()