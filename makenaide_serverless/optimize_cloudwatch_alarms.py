#!/usr/bin/env python3
"""
CloudWatch 알람 최적화 스크립트
중요한 알람만 유지하고 중복/불필요한 알람 제거
"""

import boto3
import logging
from datetime import datetime
from typing import List, Dict

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CloudWatchAlarmOptimizer:
    def __init__(self, dry_run=True):
        self.cloudwatch = boto3.client('cloudwatch', region_name='ap-northeast-2')
        self.dry_run = dry_run
        self.mode = "DRY RUN" if dry_run else "LIVE"
        logger.info(f"🎯 CloudWatch 알람 최적화 시작 - {self.mode} 모드")
    
    def get_makenaide_alarms(self):
        """Makenaide 관련 알람 조회"""
        try:
            response = self.cloudwatch.describe_alarms()
            alarms = []
            
            for alarm in response['MetricAlarms']:
                if 'makenaide' in alarm['AlarmName'].lower():
                    alarms.append({
                        'name': alarm['AlarmName'],
                        'metric': alarm['MetricName'],
                        'state': alarm['StateValue'],
                        'namespace': alarm.get('Namespace', ''),
                        'dimensions': alarm.get('Dimensions', [])
                    })
            
            logger.info(f"📊 총 {len(alarms)}개의 Makenaide 알람 발견")
            return alarms
        except Exception as e:
            logger.error(f"❌ 알람 조회 실패: {e}")
            return []
    
    def identify_critical_alarms(self, alarms: List[Dict]) -> List[str]:
        """핵심 알람 식별 (10개만 유지)"""
        critical_alarms = []
        
        # 우선순위 1: Phase 관련 Error 알람 (6개)
        phase_error_alarms = [
            'Makenaide-makenaide-ticker-scanner-phase0-Errors',
            'Makenaide-makenaide-selective-data-collection-phase1-Errors',
            'Makenaide-makenaide-comprehensive-filtering-phase2-Errors',
            'Makenaide-makenaide-gpt-analysis-phase3-Errors',
            'Makenaide-makenaide-condition-check-phase5-Errors',
            'Makenaide-makenaide-trade-execution-phase6-Errors'
        ]
        
        # 우선순위 2: 핵심 서비스 알람 (4개)
        critical_service_alarms = [
            'Makenaide-makenaide-market-sentiment-check-Errors',
            'makenaide-orchestrator-error-rate',
            'makenaide-rds-health-check',
            'makenaide-daily-cost-alert'
        ]
        
        critical_alarms.extend(phase_error_alarms)
        critical_alarms.extend(critical_service_alarms)
        
        logger.info(f"✅ 유지할 핵심 알람 10개 선정 완료")
        return critical_alarms[:10]  # 최대 10개만 유지
    
    def optimize_alarms(self):
        """알람 최적화 실행"""
        alarms = self.get_makenaide_alarms()
        critical_alarms = self.identify_critical_alarms(alarms)
        
        alarms_to_delete = []
        alarms_to_keep = []
        
        for alarm in alarms:
            if alarm['name'] in critical_alarms:
                alarms_to_keep.append(alarm['name'])
                logger.info(f"✅ 유지: {alarm['name']}")
            else:
                alarms_to_delete.append(alarm['name'])
                logger.info(f"🗑️ 삭제 예정: {alarm['name']}")
        
        # 삭제 실행
        deleted_count = 0
        failed_count = 0
        
        if alarms_to_delete:
            logger.info(f"\n🎯 {len(alarms_to_delete)}개 알람 삭제 시작")
            
            if not self.dry_run:
                # 개별 삭제로 의존성 문제 해결
                for alarm_name in alarms_to_delete:
                    try:
                        self.cloudwatch.delete_alarms(AlarmNames=[alarm_name])
                        logger.info(f"✅ 삭제 완료: {alarm_name}")
                        deleted_count += 1
                    except Exception as e:
                        if 'composite alarm' in str(e):
                            logger.warning(f"⚠️ 복합 알람 의존성: {alarm_name}")
                        else:
                            logger.error(f"❌ 삭제 실패: {alarm_name} - {e}")
                        failed_count += 1
                
                logger.info(f"\n✅ 성공적으로 삭제: {deleted_count}개")
                if failed_count > 0:
                    logger.info(f"⚠️ 삭제 실패: {failed_count}개 (복합 알람 의존성)")
            else:
                logger.info(f"🔍 DRY RUN: {len(alarms_to_delete)}개 알람이 삭제될 예정")
        
        # 결과 요약
        logger.info("\n" + "="*50)
        logger.info("📊 CloudWatch 알람 최적화 결과:")
        logger.info(f"- 기존 알람: {len(alarms)}개")
        logger.info(f"- 유지 알람: {len(alarms_to_keep)}개")
        logger.info(f"- 삭제 알람: {len(alarms_to_delete)}개")
        logger.info(f"- 예상 절약액: ${len(alarms_to_delete) * 0.10:.2f}/월")
        logger.info("="*50)
        
        return {
            'total': len(alarms),
            'kept': len(alarms_to_keep),
            'deleted': len(alarms_to_delete),
            'savings': len(alarms_to_delete) * 0.10
        }

if __name__ == "__main__":
    import sys
    
    # DRY RUN 모드 체크
    dry_run = '--execute' not in sys.argv
    
    if dry_run:
        print("\n⚠️ DRY RUN 모드로 실행됩니다.")
        print("실제 삭제를 원하시면 '--execute' 옵션을 추가하세요.\n")
    else:
        print("\n⚠️ 실제 삭제 모드로 실행합니다.")
    
    optimizer = CloudWatchAlarmOptimizer(dry_run=dry_run)
    result = optimizer.optimize_alarms()
    
    if dry_run:
        print(f"\n💡 실제 실행: python {sys.argv[0]} --execute")