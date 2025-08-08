#!/usr/bin/env python3
"""
EventBridge 스케줄러 설정으로 makenaide-orchestrator 자동 실행
"""

import boto3
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventBridgeScheduler:
    """EventBridge 스케줄러 설정"""
    
    def __init__(self):
        self.events_client = boto3.client('events')
        self.lambda_client = boto3.client('lambda')
        self.region = 'ap-northeast-2'
        self.account_id = '901361833359'
        
    def create_scheduled_rule(self):
        """스케줄된 규칙 생성"""
        logger.info("📅 EventBridge 스케줄 규칙 생성")
        
        rule_name = 'makenaide-orchestrator-schedule'
        
        try:
            # 기존 규칙 삭제 (있다면)
            try:
                self.events_client.delete_rule(Name=rule_name, Force=True)
                logger.info("🗑️ 기존 규칙 삭제 완료")
            except:
                pass
            
            # 새 규칙 생성 - 매일 오전 9시와 오후 6시 실행
            response = self.events_client.put_rule(
                Name=rule_name,
                ScheduleExpression='cron(0 0,9 * * ? *)',  # UTC 기준 00:00, 09:00 (한국시간 09:00, 18:00)
                Description='Makenaide Orchestrator 자동 실행 스케줄 (매일 오전 9시, 오후 6시)',
                State='ENABLED'
            )
            
            logger.info(f"✅ 스케줄 규칙 생성 성공: {response['RuleArn']}")
            return response['RuleArn']
            
        except Exception as e:
            logger.error(f"❌ 스케줄 규칙 생성 실패: {e}")
            return None
    
    def add_lambda_permission(self):
        """Lambda 함수에 EventBridge 호출 권한 추가"""
        logger.info("🔐 Lambda 함수 권한 설정")
        
        try:
            # 기존 권한 제거 (있다면)
            try:
                self.lambda_client.remove_permission(
                    FunctionName='makenaide-orchestrator',
                    StatementId='AllowEventBridgeInvoke'
                )
            except:
                pass
            
            # 새 권한 추가
            self.lambda_client.add_permission(
                FunctionName='makenaide-orchestrator',
                StatementId='AllowEventBridgeInvoke',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=f'arn:aws:events:{self.region}:{self.account_id}:rule/makenaide-orchestrator-schedule'
            )
            
            logger.info("✅ Lambda 권한 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lambda 권한 설정 실패: {e}")
            return False
    
    def add_lambda_target(self):
        """Lambda 함수를 타겟으로 추가"""
        logger.info("🎯 Lambda 타겟 설정")
        
        try:
            response = self.events_client.put_targets(
                Rule='makenaide-orchestrator-schedule',
                Targets=[
                    {
                        'Id': '1',
                        'Arn': f'arn:aws:lambda:{self.region}:{self.account_id}:function:makenaide-orchestrator',
                        'Input': json.dumps({
                            'scheduled': True,
                            'trigger': 'eventbridge',
                            'timestamp': datetime.now().isoformat()
                        })
                    }
                ]
            )
            
            if response['FailedEntryCount'] == 0:
                logger.info("✅ Lambda 타겟 설정 완료")
                return True
            else:
                logger.error(f"❌ 타겟 설정 실패: {response['FailedEntries']}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Lambda 타겟 설정 실패: {e}")
            return False
    
    def create_manual_trigger_rule(self):
        """수동 트리거용 규칙 생성"""
        logger.info("🔧 수동 트리거 규칙 생성")
        
        manual_rule_name = 'makenaide-orchestrator-manual'
        
        try:
            # 수동 실행용 규칙 (비활성화 상태로 생성)
            response = self.events_client.put_rule(
                Name=manual_rule_name,
                EventPattern=json.dumps({
                    "source": ["makenaide.manual"],
                    "detail-type": ["Manual Trigger"]
                }),
                Description='Makenaide Orchestrator 수동 실행용',
                State='ENABLED'
            )
            
            # 타겟 추가
            self.events_client.put_targets(
                Rule=manual_rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': f'arn:aws:lambda:{self.region}:{self.account_id}:function:makenaide-orchestrator',
                        'Input': json.dumps({
                            'scheduled': False,
                            'trigger': 'manual',
                            'timestamp': datetime.now().isoformat()
                        })
                    }
                ]
            )
            
            logger.info("✅ 수동 트리거 규칙 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 수동 트리거 규칙 생성 실패: {e}")
            return False
    
    def test_schedule_setup(self):
        """스케줄 설정 테스트"""
        logger.info("🧪 스케줄 설정 테스트")
        
        try:
            # 규칙 조회
            rules = self.events_client.list_rules(
                NamePrefix='makenaide-orchestrator'
            )
            
            logger.info(f"📋 생성된 규칙 수: {len(rules['Rules'])}")
            
            for rule in rules['Rules']:
                logger.info(f"  - {rule['Name']}: {rule['State']}")
                logger.info(f"    스케줄: {rule.get('ScheduleExpression', 'N/A')}")
                
                # 타겟 확인
                targets = self.events_client.list_targets_by_rule(
                    Rule=rule['Name']
                )
                logger.info(f"    타겟 수: {len(targets['Targets'])}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 스케줄 테스트 실패: {e}")
            return False
    
    def setup_complete_scheduler(self):
        """완전한 스케줄러 설정"""
        logger.info("🚀 완전한 EventBridge 스케줄러 설정 시작")
        
        success_steps = 0
        total_steps = 5
        
        # 1단계: 스케줄 규칙 생성
        if self.create_scheduled_rule():
            success_steps += 1
        
        # 2단계: Lambda 권한 추가  
        if self.add_lambda_permission():
            success_steps += 1
        
        # 3단계: Lambda 타겟 추가
        if self.add_lambda_target():
            success_steps += 1
        
        # 4단계: 수동 트리거 규칙 생성
        if self.create_manual_trigger_rule():
            success_steps += 1
        
        # 5단계: 설정 테스트
        if self.test_schedule_setup():
            success_steps += 1
        
        # 결과 요약
        logger.info(f"📊 설정 완료: {success_steps}/{total_steps} 단계 성공")
        
        if success_steps == total_steps:
            logger.info("🎉 EventBridge 스케줄러 설정 완료!")
            self.print_schedule_summary()
            return True
        else:
            logger.warning(f"⚠️ 일부 단계 실패: {total_steps - success_steps}개 문제")
            return False
    
    def print_schedule_summary(self):
        """스케줄 요약 출력"""
        print("\n" + "="*60)
        print("📅 EventBridge 스케줄러 설정 완료!")
        print("="*60)
        print("🕘 자동 실행 스케줄:")
        print("  - 매일 오전 9시 (한국시간)")
        print("  - 매일 오후 6시 (한국시간)")
        print()
        print("🔧 수동 실행 방법:")
        print("  aws events put-events --entries \\")
        print("    'Source=makenaide.manual,DetailType=\"Manual Trigger\",Detail=\"{}\"'")
        print()
        print("📊 모니터링:")
        print("  - CloudWatch Logs에서 실행 로그 확인")
        print("  - EventBridge 콘솔에서 규칙 상태 모니터링")
        print("="*60)

def trigger_manual_execution():
    """수동 실행 트리거"""
    events_client = boto3.client('events')
    
    try:
        response = events_client.put_events(
            Entries=[
                {
                    'Source': 'makenaide.manual',
                    'DetailType': 'Manual Trigger',
                    'Detail': json.dumps({
                        'triggered_by': 'setup_script',
                        'timestamp': datetime.now().isoformat()
                    })
                }
            ]
        )
        
        logger.info("✅ 수동 실행 트리거 성공")
        return True
        
    except Exception as e:
        logger.error(f"❌ 수동 실행 트리거 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("📅 EventBridge 스케줄러 설정")
    print("="*60)
    
    scheduler = EventBridgeScheduler()
    
    # 완전한 스케줄러 설정
    if scheduler.setup_complete_scheduler():
        # 설정 완료 후 테스트 실행
        logger.info("🧪 설정 완료 후 수동 테스트 실행")
        if trigger_manual_execution():
            logger.info("🎉 모든 설정 완료 및 테스트 성공!")
        else:
            logger.warning("⚠️ 테스트 실행 실패")
    else:
        logger.error("❌ 스케줄러 설정 실패")

if __name__ == "__main__":
    main() 