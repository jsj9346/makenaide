#!/usr/bin/env python3
"""
⏰ EventBridge 오케스트레이션 시스템 구축
- Phase 0-1 Lambda 함수들 간의 연동 테스트
- 스케줄링 및 이벤트 기반 워크플로우 구성
- RDS 자동 시작/종료 통합
"""

import boto3
import json
import time
import os
from datetime import datetime, timezone, timedelta

class EventBridgeOrchestrator:
    """EventBridge 기반 서버리스 파이프라인 오케스트레이터"""
    
    def __init__(self):
        self.events_client = boto3.client('events')
        self.lambda_client = boto3.client('lambda')
        self.rds_client = boto3.client('rds')
        self.region = 'ap-northeast-2'
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        
    def create_rds_control_lambda(self):
        """RDS 시작/종료를 위한 제어 Lambda 생성"""
        function_name = 'makenaide-rds-controller'
        
        lambda_code = '''
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """RDS 인스턴스 시작/종료 제어"""
    
    try:
        rds_client = boto3.client('rds')
        
        # 이벤트에서 액션과 DB 식별자 추출
        action = event.get('action', 'start')  # start 또는 stop
        db_instance_id = event.get('db_instance_id', 'makenaide')
        
        logger.info(f"RDS {action} 요청: {db_instance_id}")
        
        if action == 'start':
            # RDS 시작
            try:
                response = rds_client.start_db_instance(DBInstanceIdentifier=db_instance_id)
                logger.info(f"RDS 시작 명령 전송: {db_instance_id}")
                
                return {
                    'statusCode': 200,
                    'action': 'start',
                    'db_instance_id': db_instance_id,
                    'status': 'starting',
                    'message': 'RDS 시작 명령 전송 완료'
                }
                
            except rds_client.exceptions.InvalidDBInstanceStateFault as e:
                if 'already running' in str(e).lower():
                    logger.info(f"RDS 이미 실행 중: {db_instance_id}")
                    return {
                        'statusCode': 200,
                        'action': 'start',
                        'status': 'already_running',
                        'message': 'RDS가 이미 실행 중입니다'
                    }
                else:
                    raise
                    
        elif action == 'stop':
            # RDS 종료
            try:
                response = rds_client.stop_db_instance(DBInstanceIdentifier=db_instance_id)
                logger.info(f"RDS 종료 명령 전송: {db_instance_id}")
                
                return {
                    'statusCode': 200,
                    'action': 'stop',
                    'db_instance_id': db_instance_id,
                    'status': 'stopping',
                    'message': 'RDS 종료 명령 전송 완료'
                }
                
            except rds_client.exceptions.InvalidDBInstanceStateFault as e:
                if 'already stopped' in str(e).lower():
                    logger.info(f"RDS 이미 중지됨: {db_instance_id}")
                    return {
                        'statusCode': 200,
                        'action': 'stop',
                        'status': 'already_stopped',
                        'message': 'RDS가 이미 중지되어 있습니다'
                    }
                else:
                    raise
        else:
            raise ValueError(f"지원하지 않는 액션: {action}")
            
    except Exception as e:
        logger.error(f"RDS 제어 실패: {e}")
        return {
            'statusCode': 500,
            'error': str(e),
            'message': 'RDS 제어 실패'
        }
'''
        
        try:
            # 기존 함수 확인
            self.lambda_client.get_function(FunctionName=function_name)
            print(f"✅ RDS 제어 Lambda 이미 존재: {function_name}")
            
        except self.lambda_client.exceptions.ResourceNotFoundException:
            print(f"🔧 RDS 제어 Lambda 생성: {function_name}")
            
            # IAM 역할 ARN (기존 역할 재사용)  
            role_arn = f"arn:aws:iam::{self.account_id}:role/makenaide-phase0-lambda-role"
            
            # Lambda 함수 생성
            self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.9',
                Role=role_arn,
                Handler='index.lambda_handler',
                Code={'ZipFile': lambda_code.encode('utf-8')},
                Description='Makenaide RDS start/stop controller',
                Timeout=60,
                MemorySize=128,
                Tags={
                    'Project': 'Makenaide',
                    'Component': 'RDS-Controller'
                }
            )
            
            print(f"✅ RDS 제어 Lambda 생성 완료: {function_name}")
    
    def create_pipeline_rules(self):
        """파이프라인 EventBridge 규칙들 생성"""
        
        rules_config = [
            {
                'name': 'makenaide-rds-start-schedule',
                'description': 'RDS 자동 시작 (매일 08:40 KST)',
                'schedule': 'cron(40 23 * * ? *)',  # UTC 23:40 = KST 08:40
                'target_function': 'makenaide-rds-controller',
                'input': {'action': 'start', 'db_instance_id': 'makenaide'}
            },
            {
                'name': 'makenaide-phase0-schedule', 
                'description': 'Phase 0 티커 스캐너 시작 (매일 09:05 KST)',
                'schedule': 'cron(5 0 * * ? *)',   # UTC 00:05 = KST 09:05
                'target_function': 'makenaide-ticker-scanner-phase0',
                'input': {'trigger': 'scheduled', 'source': 'eventbridge'}
            },
            {
                'name': 'makenaide-rds-stop-schedule',
                'description': 'RDS 자동 종료 (매일 10:30 KST)', 
                'schedule': 'cron(30 1 * * ? *)',  # UTC 01:30 = KST 10:30
                'target_function': 'makenaide-rds-controller',
                'input': {'action': 'stop', 'db_instance_id': 'makenaide'}
            }
        ]
        
        for rule_config in rules_config:
            self.create_scheduled_rule(rule_config)
    
    def create_scheduled_rule(self, config):
        """개별 스케줄 규칙 생성"""
        rule_name = config['name']
        
        try:
            # EventBridge 규칙 생성
            self.events_client.put_rule(
                Name=rule_name,
                ScheduleExpression=config['schedule'],
                Description=config['description'],
                State='ENABLED'
            )
            
            # Lambda 타겟 추가
            self.events_client.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': f"arn:aws:lambda:{self.region}:{self.account_id}:function:{config['target_function']}",
                        'Input': json.dumps(config['input'])
                    }
                ]
            )
            
            # Lambda 호출 권한 부여
            try:
                self.lambda_client.add_permission(
                    FunctionName=config['target_function'],
                    StatementId=f'{rule_name}-permission',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=f"arn:aws:events:{self.region}:{self.account_id}:rule/{rule_name}"
                )
            except self.lambda_client.exceptions.ResourceConflictException:
                pass  # 권한이 이미 존재
            
            print(f"✅ EventBridge 규칙 생성: {rule_name}")
            
        except Exception as e:
            print(f"❌ EventBridge 규칙 생성 실패 {rule_name}: {e}")
    
    def create_event_driven_rules(self):
        """이벤트 기반 규칙들 생성 (Phase 간 연동)"""
        
        event_rules = [
            {
                'name': 'makenaide-phase0-to-phase1',
                'description': 'Phase 0 완료 시 Phase 1 트리거',
                'event_pattern': {
                    "source": ["makenaide.ticker_scanner"],
                    "detail-type": ["Ticker Scan Completed"],
                    "detail": {
                        "status": ["completed"]
                    }
                },
                'target_function': 'makenaide-selective-data-collector-phase1'
            },
            {
                'name': 'makenaide-phase1-to-phase2',
                'description': 'Phase 1 완료 시 Phase 2 트리거',
                'event_pattern': {
                    "source": ["makenaide.selective_data_collection"],
                    "detail-type": ["Selective Data Collection Completed"],
                    "detail": {
                        "status": ["completed"]
                    }
                },
                'target_function': 'makenaide-comprehensive-filter-phase2'
            }
        ]
        
        for rule_config in event_rules:
            self.create_event_rule(rule_config)
    
    def create_event_rule(self, config):
        """개별 이벤트 규칙 생성"""
        rule_name = config['name']
        
        try:
            # EventBridge 이벤트 패턴 규칙 생성
            self.events_client.put_rule(
                Name=rule_name,
                EventPattern=json.dumps(config['event_pattern']),
                Description=config['description'],
                State='ENABLED'
            )
            
            # Lambda 타겟 추가
            self.events_client.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': f"arn:aws:lambda:{self.region}:{self.account_id}:function:{config['target_function']}"
                    }
                ]
            )
            
            # Lambda 호출 권한 부여
            try:
                self.lambda_client.add_permission(
                    FunctionName=config['target_function'],
                    StatementId=f'{rule_name}-permission',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=f"arn:aws:events:{self.region}:{self.account_id}:rule/{rule_name}"
                )
            except self.lambda_client.exceptions.ResourceConflictException:
                pass  # 권한이 이미 존재
            except self.lambda_client.exceptions.ResourceNotFoundException:
                print(f"⚠️ Lambda 함수 미존재: {config['target_function']} (추후 배포시 권한 설정 필요)")
            
            print(f"✅ 이벤트 규칙 생성: {rule_name}")
            
        except Exception as e:
            print(f"❌ 이벤트 규칙 생성 실패 {rule_name}: {e}")
    
    def test_phase0_execution(self):
        """Phase 0 Lambda 수동 테스트 실행"""
        try:
            print("🧪 Phase 0 Lambda 테스트 실행...")
            
            test_payload = {
                'source': 'manual_test',
                'trigger': 'test_execution',
                'timestamp': datetime.now().isoformat()
            }
            
            response = self.lambda_client.invoke(
                FunctionName='makenaide-ticker-scanner-phase0',
                InvocationType='RequestResponse',  # 동기 실행
                Payload=json.dumps(test_payload)
            )
            
            # 응답 파싱
            response_payload = json.loads(response['Payload'].read())
            
            if response_payload.get('statusCode') == 200:
                print("✅ Phase 0 테스트 성공!")
                print(f"   - 처리된 티커: {response_payload.get('filtered_tickers', 0)}개")
                print(f"   - 실행 시간: {response_payload.get('execution_time', 'N/A')}")
                return True
            else:
                print(f"❌ Phase 0 테스트 실패: {response_payload}")
                return False
                
        except Exception as e:
            print(f"❌ Phase 0 테스트 실행 실패: {e}")
            return False
    
    def test_rds_controller(self):
        """RDS 컨트롤러 테스트"""
        try:
            print("🧪 RDS 컨트롤러 테스트...")
            
            # RDS 상태 확인 (시작 테스트 전)
            test_payload = {
                'action': 'start',
                'db_instance_id': 'makenaide'
            }
            
            response = self.lambda_client.invoke(
                FunctionName='makenaide-rds-controller',
                InvocationType='RequestResponse',
                Payload=json.dumps(test_payload)
            )
            
            response_payload = json.loads(response['Payload'].read())
            
            if response_payload.get('statusCode') == 200:
                print("✅ RDS 컨트롤러 테스트 성공!")
                print(f"   - 액션: {response_payload.get('action')}")
                print(f"   - 상태: {response_payload.get('status')}")
                print(f"   - 메시지: {response_payload.get('message')}")
                return True
            else:
                print(f"❌ RDS 컨트롤러 테스트 실패: {response_payload}")
                return False
                
        except Exception as e:
            print(f"❌ RDS 컨트롤러 테스트 실행 실패: {e}")
            return False
    
    def validate_pipeline_setup(self):
        """파이프라인 설정 검증"""
        print("🔍 파이프라인 설정 검증...")
        
        validation_results = {
            'lambda_functions': [],
            'eventbridge_rules': [],
            'missing_components': []
        }
        
        # Lambda 함수 존재 확인
        required_functions = [
            'makenaide-ticker-scanner-phase0',
            'makenaide-rds-controller'
        ]
        
        for function_name in required_functions:
            try:
                self.lambda_client.get_function(FunctionName=function_name)
                validation_results['lambda_functions'].append(f"✅ {function_name}")
            except self.lambda_client.exceptions.ResourceNotFoundException:
                validation_results['missing_components'].append(f"❌ Lambda 함수 없음: {function_name}")
        
        # EventBridge 규칙 확인
        required_rules = [
            'makenaide-rds-start-schedule',
            'makenaide-phase0-schedule',
            'makenaide-rds-stop-schedule',
            'makenaide-phase0-to-phase1'
        ]
        
        for rule_name in required_rules:
            try:
                self.events_client.describe_rule(Name=rule_name)
                validation_results['eventbridge_rules'].append(f"✅ {rule_name}")
            except self.events_client.exceptions.ResourceNotFoundException:
                validation_results['missing_components'].append(f"❌ EventBridge 규칙 없음: {rule_name}")
        
        # 결과 출력
        print("\n📋 검증 결과:")
        print("Lambda 함수:")
        for result in validation_results['lambda_functions']:
            print(f"  {result}")
            
        print("EventBridge 규칙:")
        for result in validation_results['eventbridge_rules']:
            print(f"  {result}")
            
        if validation_results['missing_components']:
            print("누락된 구성 요소:")
            for missing in validation_results['missing_components']:
                print(f"  {missing}")
        
        return len(validation_results['missing_components']) == 0
    
    def setup_complete_orchestration(self):
        """전체 오케스트레이션 설정"""
        try:
            print("🚀 EventBridge 오케스트레이션 설정 시작")
            print("="*60)
            
            # 1. RDS 제어 Lambda 생성
            print("🔧 1단계: RDS 제어 Lambda 생성")
            self.create_rds_control_lambda()
            
            # 2. 스케줄 기반 규칙 생성
            print("⏰ 2단계: 스케줄 규칙 생성")
            self.create_pipeline_rules()
            
            # 3. 이벤트 기반 규칙 생성
            print("🔗 3단계: 이벤트 기반 규칙 생성")
            self.create_event_driven_rules()
            
            # 4. 파이프라인 검증
            print("🔍 4단계: 파이프라인 검증")
            validation_success = self.validate_pipeline_setup()
            
            # 5. 테스트 실행
            if validation_success:
                print("🧪 5단계: 컴포넌트 테스트")
                
                rds_test = self.test_rds_controller()
                phase0_test = self.test_phase0_execution()
                
                if rds_test and phase0_test:
                    print("="*60)
                    print("✅ EventBridge 오케스트레이션 설정 완료!")
                    print("📅 스케줄:")
                    print("   - 08:40 KST: RDS 시작")
                    print("   - 09:05 KST: Phase 0 티커 스캔")
                    print("   - 10:30 KST: RDS 종료")
                    print("🔗 이벤트 체인:")
                    print("   - Phase 0 → Phase 1 자동 트리거")
                    print("="*60)
                    return True
                else:
                    print("⚠️ 일부 테스트 실패, 설정은 완료되었습니다")
                    return True
            else:
                print("❌ 파이프라인 검증 실패")
                return False
                
        except Exception as e:
            print(f"❌ 오케스트레이션 설정 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    orchestrator = EventBridgeOrchestrator()
    success = orchestrator.setup_complete_orchestration()
    return success

if __name__ == "__main__": 
    success = main()
    exit(0 if success else 1)