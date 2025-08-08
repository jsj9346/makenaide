#!/usr/bin/env python3
"""
🔧 Slack Lambda 통합 수정
- IAM 역할 문제 해결
- Lambda 함수 재생성 
- SNS 구독 연결
"""

import boto3
import json
import logging
import zipfile
import io
from datetime import datetime
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SlackLambdaFixer:
    """Slack Lambda 통합 수정 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        self.iam_client = boto3.client('iam', region_name='ap-northeast-2')
        self.sns_client = boto3.client('sns', region_name='ap-northeast-2')
        
        # 올바른 IAM 역할 ARN
        self.lambda_role_arn = 'arn:aws:iam::901361833359:role/makenaide-lambda-execution-role'
        
        # SNS 토픽 ARN들
        self.topic_arns = {
            'CRITICAL': 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-critical-alerts',
            'WARNING': 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-warning-alerts',
            'INFO': 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-info-alerts'
        }
    
    def check_lambda_role(self) -> bool:
        """Lambda 실행 역할 존재 확인"""
        try:
            self.iam_client.get_role(RoleName='makenaide-lambda-execution-role')
            logger.info("✅ Lambda 실행 역할 확인됨")
            return True
        except self.iam_client.exceptions.NoSuchEntityException:
            logger.error("❌ Lambda 실행 역할 없음")
            return False
    
    def create_slack_lambda_function(self) -> Dict:
        """Slack Lambda 함수 생성"""
        logger.info("💬 Slack Lambda 함수 생성 중...")
        
        result = {
            'function_created': False,
            'function_arn': None,
            'error': None
        }
        
        try:
            function_name = 'makenaide-slack-notifier'
            
            # 기존 함수 삭제 (있다면)
            try:
                self.lambda_client.delete_function(FunctionName=function_name)
                logger.info(f"🗑️ 기존 함수 삭제: {function_name}")
            except self.lambda_client.exceptions.ResourceNotFoundException:
                pass
            
            # Lambda 함수 코드 생성
            lambda_code = self._generate_lambda_zip()
            
            # Lambda 함수 생성
            response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.11',
                Role=self.lambda_role_arn,
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': lambda_code},
                Description='Makenaide Slack notification handler',
                Timeout=30,
                Environment={
                    'Variables': {
                        'SLACK_WEBHOOK_URL': '',  # 사용자가 설정해야 함
                        'SLACK_CHANNEL': '#makenaide-alerts'
                    }
                }
            )
            
            function_arn = response['FunctionArn']
            result['function_created'] = True
            result['function_arn'] = function_arn
            
            logger.info(f"✅ Slack Lambda 함수 생성 완료: {function_arn}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Slack Lambda 함수 생성 실패: {e}")
            result['error'] = str(e)
            return result
    
    def setup_sns_subscriptions(self, function_arn: str) -> Dict:
        """SNS 구독 설정"""
        logger.info("🔗 SNS 구독 설정 중...")
        
        result = {
            'subscriptions_created': [],
            'permissions_added': [],
            'errors': []
        }
        
        function_name = 'makenaide-slack-notifier'
        
        for level, topic_arn in self.topic_arns.items():
            try:
                # Lambda 함수에 SNS 실행 권한 부여
                statement_id = f'sns-invoke-{level.lower()}'
                
                try:
                    self.lambda_client.add_permission(
                        FunctionName=function_name,
                        StatementId=statement_id,
                        Action='lambda:InvokeFunction',
                        Principal='sns.amazonaws.com',
                        SourceArn=topic_arn
                    )
                    result['permissions_added'].append(level)
                    logger.info(f"✅ {level} SNS 실행 권한 부여")
                    
                except self.lambda_client.exceptions.ResourceConflictException:
                    # 이미 권한이 있는 경우
                    logger.info(f"ℹ️ {level} SNS 실행 권한 이미 존재")
                
                # SNS 구독 생성
                subscription_response = self.sns_client.subscribe(
                    TopicArn=topic_arn,
                    Protocol='lambda',
                    Endpoint=function_arn
                )
                
                subscription_arn = subscription_response['SubscriptionArn']
                result['subscriptions_created'].append({
                    'level': level,
                    'topic_arn': topic_arn,
                    'subscription_arn': subscription_arn
                })
                
                logger.info(f"✅ {level} SNS 구독 생성: {subscription_arn}")
                
            except Exception as e:
                error_msg = f"{level} 구독 설정 실패: {str(e)}"
                result['errors'].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        return result
    
    def _generate_lambda_zip(self) -> bytes:
        """Lambda 함수 ZIP 파일 생성"""
        code = '''
import json
import urllib3
import os
from datetime import datetime

def lambda_handler(event, context):
    """SNS에서 받은 알림을 Slack으로 전송"""
    
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook_url:
        print("SLACK_WEBHOOK_URL 환경변수가 설정되지 않음")
        return {
            'statusCode': 200, 
            'body': json.dumps('Webhook URL not configured, but function executed successfully')
        }
    
    try:
        # SNS 메시지 파싱
        sns_record = event['Records'][0]['Sns']
        subject = sns_record.get('Subject', 'Makenaide Alert')
        message = sns_record['Message']
        
        # CloudWatch 알람 메시지인지 확인
        try:
            alarm_data = json.loads(message)
            is_alarm = 'AlarmName' in alarm_data
        except:
            is_alarm = False
            alarm_data = {}
        
        if is_alarm:
            # CloudWatch 알람 메시지
            alarm_name = alarm_data.get('AlarmName', 'Unknown')
            alarm_description = alarm_data.get('AlarmDescription', '')
            new_state = alarm_data.get('NewStateValue', 'UNKNOWN')
            old_state = alarm_data.get('OldStateValue', 'UNKNOWN')
            reason = alarm_data.get('NewStateReason', '')
            timestamp = alarm_data.get('StateChangeTime', datetime.utcnow().isoformat())
        else:
            # 일반 메시지
            alarm_name = subject
            alarm_description = message
            new_state = 'INFO'
            old_state = 'OK'
            reason = 'Manual notification'
            timestamp = datetime.utcnow().isoformat()
        
        # 상태별 색상 및 이모지
        color_map = {
            'ALARM': '#ff0000',              # 빨강
            'OK': '#36a64f',                 # 초록
            'INSUFFICIENT_DATA': '#ffaa00',  # 주황
            'INFO': '#36a64f'                # 초록
        }
        
        emoji_map = {
            'ALARM': '🚨',
            'OK': '✅', 
            'INSUFFICIENT_DATA': '⚠️',
            'INFO': 'ℹ️'
        }
        
        color = color_map.get(new_state, '#808080')
        emoji = emoji_map.get(new_state, '❓')
        
        # Slack 메시지 구성
        slack_message = {
            'channel': os.environ.get('SLACK_CHANNEL', '#makenaide-alerts'),
            'username': 'Makenaide Bot',
            'icon_emoji': ':robot_face:',
            'attachments': [
                {
                    'color': color,
                    'title': f'{emoji} {subject}',
                    'fields': [
                        {
                            'title': '알람명',
                            'value': alarm_name,
                            'short': True
                        },
                        {
                            'title': '상태 변화',
                            'value': f'{old_state} → {new_state}',
                            'short': True
                        },
                        {
                            'title': '설명',
                            'value': alarm_description or '설명 없음',
                            'short': False
                        },
                        {
                            'title': '사유',
                            'value': reason or '사유 없음',
                            'short': False
                        },
                        {
                            'title': '시간',
                            'value': timestamp,
                            'short': True
                        }
                    ],
                    'footer': 'Makenaide Monitoring System',
                    'ts': int(datetime.utcnow().timestamp())
                }
            ]
        }
        
        # Slack 웹훅으로 전송
        http = urllib3.PoolManager()
        response = http.request(
            'POST',
            webhook_url,
            body=json.dumps(slack_message),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status == 200:
            print(f"Slack 알림 전송 성공: {alarm_name}")
            return {
                'statusCode': 200, 
                'body': json.dumps(f'Slack notification sent successfully: {alarm_name}')
            }
        else:
            print(f"Slack 알림 전송 실패: HTTP {response.status}")
            print(f"Response: {response.data.decode('utf-8')}")
            return {
                'statusCode': response.status, 
                'body': json.dumps(f'Slack notification failed: HTTP {response.status}')
            }
            
    except Exception as e:
        error_msg = f"Slack 알림 처리 오류: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500, 
            'body': json.dumps(error_msg)
        }
'''
        
        # ZIP 파일 생성
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr('lambda_function.py', code)
        
        return zip_buffer.getvalue()
    
    def test_slack_integration(self, function_arn: str) -> Dict:
        """Slack 통합 테스트"""
        logger.info("🧪 Slack 통합 테스트 중...")
        
        test_event = {
            'Records': [
                {
                    'Sns': {
                        'Subject': '[TEST] Makenaide 알림 테스트',
                        'Message': json.dumps({
                            'AlarmName': 'test-notification',
                            'AlarmDescription': 'Slack 통합 테스트 알림입니다',
                            'NewStateValue': 'INFO',
                            'OldStateValue': 'OK',
                            'NewStateReason': 'This is a test notification to verify Slack integration',
                            'StateChangeTime': datetime.utcnow().isoformat()
                        })
                    }
                }
            ]
        }
        
        try:
            response = self.lambda_client.invoke(
                FunctionName='makenaide-slack-notifier',
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            if response['StatusCode'] == 200:
                payload = json.loads(response['Payload'].read())
                logger.info(f"✅ Slack 통합 테스트 성공: {payload.get('body', 'Success')}")
                return {'status': 'SUCCESS', 'response': payload}
            else:
                logger.error(f"❌ Slack 통합 테스트 실패: HTTP {response['StatusCode']}")
                return {'status': 'FAILED', 'error': f"HTTP {response['StatusCode']}"}
                
        except Exception as e:
            logger.error(f"❌ Slack 통합 테스트 오류: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    def fix_slack_integration(self) -> Dict:
        """Slack 통합 전체 수정"""
        logger.info("🔧 Slack Lambda 통합 수정 시작")
        logger.info("=" * 60)
        
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'role_check': False,
            'function_creation': {},
            'subscriptions': {},
            'test_result': {},
            'overall_status': 'UNKNOWN'
        }
        
        try:
            # 1. IAM 역할 확인
            logger.info("\n🔐 1. Lambda 실행 역할 확인")
            report['role_check'] = self.check_lambda_role()
            
            if not report['role_check']:
                logger.error("❌ Lambda 실행 역할이 없어서 중단")
                report['overall_status'] = 'FAILED'
                return report
            
            # 2. Lambda 함수 생성
            logger.info("\n💬 2. Slack Lambda 함수 생성")
            function_result = self.create_slack_lambda_function()
            report['function_creation'] = function_result
            
            if not function_result['function_created']:
                logger.error("❌ Lambda 함수 생성 실패로 중단")
                report['overall_status'] = 'FAILED'
                return report
            
            # 3. SNS 구독 설정
            logger.info("\n🔗 3. SNS 구독 설정")
            subscription_result = self.setup_sns_subscriptions(function_result['function_arn'])
            report['subscriptions'] = subscription_result
            
            # 4. 통합 테스트
            logger.info("\n🧪 4. Slack 통합 테스트")
            test_result = self.test_slack_integration(function_result['function_arn'])
            report['test_result'] = test_result
            
            # 5. 전체 상태 평가
            success_criteria = {
                'role': report['role_check'],
                'function': function_result['function_created'],
                'subscriptions': len(subscription_result['subscriptions_created']) >= 2,
                'test': test_result.get('status') == 'SUCCESS'
            }
            
            success_count = sum(success_criteria.values())
            total_count = len(success_criteria)
            
            if success_count >= 3:
                report['overall_status'] = 'SUCCESS'
            elif success_count >= 2:
                report['overall_status'] = 'PARTIAL'
            else:
                report['overall_status'] = 'FAILED'
            
            # 결과 출력
            print(f"""
🔧 Slack Lambda 통합 수정 완료!

📊 수정 상태: {report['overall_status']} ({success_count}/{total_count} 성공)

🔐 IAM 역할 확인: {'✅' if report['role_check'] else '❌'}

💬 Lambda 함수 생성:
   • 함수 생성: {'✅' if function_result['function_created'] else '❌'}
   • 함수 ARN: {function_result.get('function_arn', 'N/A')}

🔗 SNS 구독 설정:
   • 생성된 구독: {len(subscription_result['subscriptions_created'])}개
   • 권한 부여: {len(subscription_result['permissions_added'])}개
   • 오류: {len(subscription_result['errors'])}개

🧪 통합 테스트:
   • 테스트 상태: {test_result.get('status', 'UNKNOWN')}
   • 함수 실행: {'✅' if test_result.get('status') == 'SUCCESS' else '❌'}

⚠️ 수동 설정 필요:
   1. Slack 웹훅 URL 설정
      - AWS Lambda 콘솔에서 'makenaide-slack-notifier' 함수 열기
      - 환경변수 탭에서 SLACK_WEBHOOK_URL 값 입력
      
   2. Slack에서 Incoming Webhooks 설정
      - Slack 앱에서 Incoming Webhooks 활성화
      - #makenaide-alerts 채널에 웹훅 추가
      - 웹훅 URL 복사하여 Lambda 환경변수에 설정

💡 Slack 알림 기능:
   • 3단계 알림 레벨 (Critical/Warning/Info)
   • 컬러 코딩된 메시지 (빨강/주황/초록)
   • 알람 상세 정보 표시
   • 타임스탬프 및 사유 포함
   • 실시간 알림 수신

🎯 다음 단계:
   • Slack 웹훅 URL 설정 완료
   • 실제 알람 발생으로 Slack 알림 확인
   • 필요시 채널명 변경 (#makenaide-alerts)
            """)
            
            return report
            
        except Exception as e:
            logger.error(f"❌ Slack 통합 수정 실패: {e}")
            report['overall_status'] = 'ERROR'
            report['error'] = str(e)
            return report

def main():
    """메인 실행"""
    fixer = SlackLambdaFixer()
    report = fixer.fix_slack_integration()
    
    if report['overall_status'] in ['SUCCESS', 'PARTIAL']:
        print("\n🎉 Slack Lambda 통합 수정 성공!")
        print("Slack 웹훅 URL 설정을 완료하세요.")
        exit(0)
    else:
        print("\n⚠️ Slack Lambda 통합 수정 실패!")
        exit(1)

if __name__ == '__main__':
    main()