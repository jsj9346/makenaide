#!/usr/bin/env python3
"""
RDS 스케줄 최적화 스크립트
30분 → 20분/일로 단축하여 비용 절약
"""

import boto3
import json
from datetime import datetime
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RDSScheduleOptimizer:
    def __init__(self, dry_run=True):
        self.events_client = boto3.client('events', region_name='ap-northeast-2')
        self.lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        self.rds_client = boto3.client('rds', region_name='ap-northeast-2')
        self.dry_run = dry_run
        self.mode = "DRY RUN" if dry_run else "LIVE"
        logger.info(f"🕒 RDS 스케줄 최적화 시작 - {self.mode} 모드")

    def analyze_current_schedule(self):
        """현재 RDS 스케줄 분석"""
        logger.info("📊 현재 RDS 스케줄 분석 중...")
        
        # 현재 스케줄 조회
        startup_rule = self.events_client.describe_rule(Name='makenaide-rds-startup')
        batch_rule = self.events_client.describe_rule(Name='makenaide-batch-processing-schedule')
        
        logger.info(f"현재 RDS 시작: {startup_rule['ScheduleExpression']}")
        logger.info(f"현재 배치 처리: {batch_rule['ScheduleExpression']}")
        
        # RDS 운영 시간 계산 (현재: 30분/일)
        current_daily_minutes = 30
        logger.info(f"현재 일일 운영 시간: {current_daily_minutes}분")
        
        return current_daily_minutes

    def create_optimized_schedule(self):
        """최적화된 스케줄 생성 (20분/일)"""
        logger.info("🎯 최적화된 스케줄 생성 중...")
        
        # 새로운 스케줄: RDS를 파이프라인 시작 10분 전에 시작
        # 배치 처리를 3시간 → 2시간으로 단축
        optimized_schedule = {
            'rds_startup': 'cron(50 23 * * ? *)',  # 08:50 UTC (09:00 10분 전)
            'batch_processing': 'rate(2 hours)',   # 4시간 → 2시간으로 단축
            'description': '20분/일로 RDS 운영 시간 단축'
        }
        
        logger.info(f"새로운 RDS 시작: {optimized_schedule['rds_startup']}")
        logger.info(f"새로운 배치 주기: {optimized_schedule['batch_processing']}")
        
        return optimized_schedule

    def update_rds_controller_logic(self):
        """RDS 컨트롤러 로직 업데이트"""
        logger.info("⚙️ RDS 컨트롤러 최적화 중...")
        
        if not self.dry_run:
            try:
                # RDS 컨트롤러 함수 환경 변수 업데이트
                response = self.lambda_client.update_function_configuration(
                    FunctionName='makenaide-rds-controller',
                    Environment={
                        'Variables': {
                            'RDS_OPERATION_MODE': 'OPTIMIZED',
                            'MAX_OPERATION_MINUTES': '20',
                            'AUTO_SHUTDOWN_DELAY': '600'  # 10분 후 자동 종료
                        }
                    }
                )
                logger.info("✅ RDS 컨트롤러 환경 변수 업데이트 완료")
            except Exception as e:
                logger.error(f"❌ RDS 컨트롤러 업데이트 실패: {e}")
        else:
            logger.info("🔍 DRY RUN: RDS 컨트롤러 환경 변수가 업데이트될 예정")

    def update_event_schedules(self, optimized_schedule):
        """EventBridge 스케줄 업데이트"""
        logger.info("📅 EventBridge 스케줄 업데이트 중...")
        
        if not self.dry_run:
            try:
                # RDS 시작 스케줄 업데이트
                self.events_client.put_rule(
                    Name='makenaide-rds-startup',
                    ScheduleExpression=optimized_schedule['rds_startup'],
                    Description='Optimized RDS startup - 10 minutes before pipeline',
                    State='ENABLED'
                )
                
                # 배치 처리 스케줄 업데이트
                self.events_client.put_rule(
                    Name='makenaide-batch-processing-schedule',
                    ScheduleExpression=optimized_schedule['batch_processing'],
                    Description='Optimized batch processing - 2-hour intervals',
                    State='ENABLED'
                )
                
                logger.info("✅ EventBridge 스케줄 업데이트 완료")
                
            except Exception as e:
                logger.error(f"❌ EventBridge 스케줄 업데이트 실패: {e}")
        else:
            logger.info("🔍 DRY RUN: EventBridge 스케줄이 업데이트될 예정")

    def calculate_savings(self):
        """비용 절약 계산"""
        # 현재: db.t3.micro 30분/일 = 15시간/월
        # 최적화 후: 20분/일 = 10시간/월
        current_hours = 15
        optimized_hours = 10
        hourly_rate = 0.018  # db.t3.micro 시간당 요금
        
        current_cost = current_hours * hourly_rate
        optimized_cost = optimized_hours * hourly_rate
        monthly_savings = current_cost - optimized_cost
        
        logger.info("\n" + "="*50)
        logger.info("💰 RDS 비용 절약 계산:")
        logger.info(f"- 현재 운영: {current_hours}시간/월 = ${current_cost:.2f}")
        logger.info(f"- 최적화 후: {optimized_hours}시간/월 = ${optimized_cost:.2f}")
        logger.info(f"- 월간 절약액: ${monthly_savings:.2f}")
        logger.info(f"- 연간 절약액: ${monthly_savings * 12:.2f}")
        logger.info("="*50)
        
        return monthly_savings

    def optimize_schedule(self):
        """RDS 스케줄 최적화 실행"""
        current_minutes = self.analyze_current_schedule()
        optimized_schedule = self.create_optimized_schedule()
        
        # 컨트롤러 로직 업데이트
        self.update_rds_controller_logic()
        
        # EventBridge 스케줄 업데이트
        self.update_event_schedules(optimized_schedule)
        
        # 비용 절약 계산
        savings = self.calculate_savings()
        
        return {
            'current_minutes': current_minutes,
            'optimized_minutes': 20,
            'monthly_savings': savings
        }

if __name__ == "__main__":
    import sys
    
    # DRY RUN 모드 체크
    dry_run = '--execute' not in sys.argv
    
    if dry_run:
        print("\n⚠️ DRY RUN 모드로 실행됩니다.")
        print("실제 변경을 원하시면 '--execute' 옵션을 추가하세요.\n")
    else:
        print("\n⚠️ 실제 스케줄 변경 모드로 실행합니다.")
    
    optimizer = RDSScheduleOptimizer(dry_run=dry_run)
    result = optimizer.optimize_schedule()
    
    if dry_run:
        print(f"\n💡 실제 실행: python {sys.argv[0]} --execute")