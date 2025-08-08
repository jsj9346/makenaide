#!/usr/bin/env python3
"""
🔔 향상된 알림 시스템 구현
- Slack 웹훅 연동을 통한 실시간 알림
- 다중 이메일 구독 관리
- 알림 레벨별 라우팅 (Critical, Warning, Info)
- CloudWatch 알람과의 완전 통합
- 테스트 및 검증 기능 포함
"""

import boto3
import json
import logging
import urllib3
from datetime import datetime
from typing import Dict, List, Optional
import hashlib
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedNotificationSystem:
    """향상된 알림 시스템 클래스"""
    
    def __init__(self):
        self.sns_client = boto3.client('sns', region_name='ap-northeast-2')
        self.s3_client = boto3.client('s3')
        self.cloudwatch_client = boto3.client('cloudwatch', region_name='ap-northeast-2')
        
        self.s3_bucket = 'makenaide-bucket-901361833359'
        self.region = 'ap-northeast-2'
        
        # 기본 설정
        self.notification_config = {
            'slack': {
                'webhook_url': '',  # 사용자가 설정해야 함
                'channel': '#makenaide-alerts',
                'username': 'Makenaide Bot',
                'icon_emoji': ':robot_face:'
            },
            'email': {
                'admin_emails': [],  # 사용자가 추가해야 함
                'alert_levels': ['CRITICAL', 'WARNING'],
                'batch_digest': False
            },
            'alert_routing': {
                'CRITICAL': ['slack', 'email', 'sms'],
                'WARNING': ['slack', 'email'],
                'INFO': ['slack']
            }
        }
    
    def create_enhanced_sns_topics(self) -> Dict[str, str]:
        """레벨별 SNS 토픽 생성"""
        logger.info("🔔 향상된 SNS 토픽 생성 중...")
        
        topics = {}
        
        topic_configs = [
            {
                'name': 'makenaide-critical-alerts',
                'display_name': 'Makenaide Critical Alerts',
                'level': 'CRITICAL'
            },
            {
                'name': 'makenaide-warning-alerts', 
                'display_name': 'Makenaide Warning Alerts',
                'level': 'WARNING'
            },
            {
                'name': 'makenaide-info-alerts',
                'display_name': 'Makenaide Info Alerts',
                'level': 'INFO'
            }
        ]
        
        for config in topic_configs:
            try:
                # 기존 토픽 확인
                existing_topics = self.sns_client.list_topics()
                topic_exists = False
                
                for topic in existing_topics.get('Topics', []):
                    if config['name'] in topic['TopicArn']:
                        topics[config['level']] = topic['TopicArn']
                        topic_exists = True
                        logger.info(f"✅ 기존 {config['level']} 토픽 사용: {topic['TopicArn']}")
                        break
                
                if not topic_exists:
                    # 새 토픽 생성
                    response = self.sns_client.create_topic(
                        Name=config['name'],
                        Attributes={
                            'DisplayName': config['display_name'],
                            'DeliveryPolicy': json.dumps({
                                'http': {
                                    'defaultHealthyRetryPolicy': {
                                        'minDelayTarget': 20,
                                        'maxDelayTarget': 20,
                                        'numRetries': 3,
                                        'numMaxDelayRetries': 0,
                                        'numMinDelayRetries': 0,
                                        'numNoDelayRetries': 0,
                                        'backoffFunction': 'linear'
                                    },
                                    'disableSubscriptionOverrides': False
                                }
                            })
                        }
                    )
                    
                    topic_arn = response['TopicArn']
                    topics[config['level']] = topic_arn
                    logger.info(f"✅ 새 {config['level']} 토픽 생성: {topic_arn}")
                
            except Exception as e:
                logger.error(f"❌ {config['level']} 토픽 생성 실패: {e}")
        
        return topics
    
    def setup_slack_integration(self, topic_arns: Dict[str, str]) -> Dict:
        """Slack 웹훅 통합 설정"""
        logger.info("💬 Slack 웹훅 통합 설정 중...")
        
        # Slack Lambda 함수 생성
        slack_lambda_code = self._generate_slack_lambda_code()
        
        result = {
            'lambda_function_created': False,
            'subscriptions_created': [],
            'webhook_configured': False,
            'test_results': {}
        }
        
        try:
            # Lambda 함수 생성 (Slack 알림용)
            lambda_client = boto3.client('lambda', region_name=self.region)
            
            function_name = 'makenaide-slack-notifier'
            
            try:
                # 기존 함수 확인
                lambda_client.get_function(FunctionName=function_name)
                logger.info(f"ℹ️ 기존 Lambda 함수 사용: {function_name}")
                result['lambda_function_created'] = True
                
            except lambda_client.exceptions.ResourceNotFoundException:
                # 새 함수 생성
                lambda_response = lambda_client.create_function(
                    FunctionName=function_name,
                    Runtime='python3.11',
                    Role='arn:aws:iam::901361833359:role/lambda-execution-role',
                    Handler='lambda_function.lambda_handler',
                    Code={'ZipFile': slack_lambda_code},
                    Description='Makenaide Slack notification handler',
                    Timeout=30,
                    Environment={
                        'Variables': {
                            'SLACK_WEBHOOK_URL': '',  # 사용자가 설정
                            'SLACK_CHANNEL': self.notification_config['slack']['channel']
                        }
                    }
                )
                
                logger.info(f"✅ Slack Lambda 함수 생성: {function_name}")
                result['lambda_function_created'] = True
                
                # Lambda 함수에 SNS 실행 권한 부여
                function_arn = lambda_response['FunctionArn']
                
                for level, topic_arn in topic_arns.items():
                    try:
                        lambda_client.add_permission(
                            FunctionName=function_name,
                            StatementId=f'sns-invoke-{level.lower()}',
                            Action='lambda:InvokeFunction',
                            Principal='sns.amazonaws.com',
                            SourceArn=topic_arn
                        )
                        
                        # SNS 구독 생성
                        self.sns_client.subscribe(
                            TopicArn=topic_arn,
                            Protocol='lambda',
                            Endpoint=function_arn
                        )
                        
                        result['subscriptions_created'].append({
                            'level': level,
                            'topic_arn': topic_arn,
                            'lambda_arn': function_arn
                        })
                        
                        logger.info(f"✅ {level} 레벨 Slack 구독 생성")
                        
                    except Exception as e:
                        logger.warning(f"⚠️ {level} Slack 구독 생성 실패: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Slack 통합 설정 실패: {e}")
            result['error'] = str(e)
            return result
    
    def setup_email_subscriptions(self, topic_arns: Dict[str, str], admin_emails: List[str]) -> Dict:
        """이메일 구독 설정"""
        logger.info("📧 이메일 구독 설정 중...")
        
        result = {
            'subscriptions_created': [],
            'confirmation_required': [],
            'errors': []
        }
        
        for level, topic_arn in topic_arns.items():
            # 중요도에 따른 이메일 필터링
            if level not in self.notification_config['email']['alert_levels']:
                logger.info(f"ℹ️ {level} 레벨은 이메일 알림 제외")
                continue
            
            for email in admin_emails:
                try:
                    subscription_response = self.sns_client.subscribe(
                        TopicArn=topic_arn,
                        Protocol='email',
                        Endpoint=email
                    )
                    
                    subscription_arn = subscription_response['SubscriptionArn']
                    
                    if subscription_arn == 'pending confirmation':
                        result['confirmation_required'].append({
                            'email': email,
                            'level': level,
                            'topic_arn': topic_arn
                        })
                        logger.info(f"📧 {email}에 {level} 구독 확인 이메일 발송")
                    else:
                        result['subscriptions_created'].append({
                            'email': email,
                            'level': level,
                            'subscription_arn': subscription_arn
                        })
                        logger.info(f"✅ {email} {level} 구독 완료")
                        
                except Exception as e:
                    error_msg = f"{email} {level} 구독 실패: {str(e)}"
                    result['errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")
        
        return result
    
    def update_cloudwatch_alarms(self, topic_arns: Dict[str, str]) -> Dict:
        """기존 CloudWatch 알람을 새로운 토픽으로 업데이트"""
        logger.info("⚠️ CloudWatch 알람 업데이트 중...")
        
        result = {
            'updated_alarms': [],
            'errors': []
        }
        
        try:
            # 기존 알람 조회
            response = self.cloudwatch_client.describe_alarms(
                AlarmNamePrefix='makenaide-'
            )
            
            for alarm in response.get('MetricAlarms', []):
                alarm_name = alarm['AlarmName']
                
                try:
                    # 알람 심각도 결정
                    alarm_level = self._determine_alarm_level(alarm_name)
                    target_topic_arn = topic_arns.get(alarm_level)
                    
                    if not target_topic_arn:
                        logger.warning(f"⚠️ {alarm_name}: 적절한 토픽 없음 ({alarm_level})")
                        continue
                    
                    # 알람 액션 업데이트
                    self.cloudwatch_client.put_metric_alarm(
                        AlarmName=alarm['AlarmName'],
                        AlarmDescription=alarm['AlarmDescription'],
                        ActionsEnabled=True,
                        AlarmActions=[target_topic_arn],
                        OKActions=[target_topic_arn],
                        MetricName=alarm['MetricName'],
                        Namespace=alarm['Namespace'],
                        Statistic=alarm['Statistic'],
                        Dimensions=alarm['Dimensions'],
                        Period=alarm['Period'],
                        EvaluationPeriods=alarm['EvaluationPeriods'],
                        Threshold=alarm['Threshold'],
                        ComparisonOperator=alarm['ComparisonOperator']
                    )
                    
                    result['updated_alarms'].append({
                        'alarm_name': alarm_name,
                        'level': alarm_level,
                        'topic_arn': target_topic_arn
                    })
                    
                    logger.info(f"✅ {alarm_name} → {alarm_level} 토픽 연결")
                    
                except Exception as e:
                    error_msg = f"{alarm_name} 업데이트 실패: {str(e)}"
                    result['errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ CloudWatch 알람 업데이트 실패: {e}")
            result['error'] = str(e)
            return result
    
    def _determine_alarm_level(self, alarm_name: str) -> str:
        """알람 이름을 기반으로 심각도 결정"""
        alarm_name_lower = alarm_name.lower()
        
        if any(keyword in alarm_name_lower for keyword in ['failure', 'error', 'critical', 'down', 'disk-space']):
            return 'CRITICAL'
        elif any(keyword in alarm_name_lower for keyword in ['warning', 'high', 'memory', 'cpu']):
            return 'WARNING'
        else:
            return 'INFO'
    
    def _generate_slack_lambda_code(self) -> bytes:
        """Slack 알림 Lambda 함수 코드 생성"""
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
        return {'statusCode': 400, 'body': 'Webhook URL not configured'}
    
    try:
        # SNS 메시지 파싱
        sns_message = json.loads(event['Records'][0]['Sns']['Message'])
        subject = event['Records'][0]['Sns']['Subject'] or 'Makenaide Alert'
        
        # 알람 정보 추출
        alarm_name = sns_message.get('AlarmName', 'Unknown')
        alarm_description = sns_message.get('AlarmDescription', '')
        new_state = sns_message.get('NewStateValue', 'UNKNOWN')
        old_state = sns_message.get('OldStateValue', 'UNKNOWN')
        reason = sns_message.get('NewStateReason', '')
        timestamp = sns_message.get('StateChangeTime', datetime.utcnow().isoformat())
        
        # 상태별 색상 및 이모지
        color_map = {
            'ALARM': '#ff0000',      # 빨강
            'OK': '#00ff00',         # 초록
            'INSUFFICIENT_DATA': '#ffff00'  # 노랑
        }
        
        emoji_map = {
            'ALARM': '🚨',
            'OK': '✅', 
            'INSUFFICIENT_DATA': '⚠️'
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
                            'value': alarm_description,
                            'short': False
                        },
                        {
                            'title': '사유',
                            'value': reason,
                            'short': False
                        },
                        {
                            'title': '시간',
                            'value': timestamp,
                            'short': True
                        }
                    ],
                    'footer': 'Makenaide Monitoring',
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
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status == 200:
            print(f"Slack 알림 전송 성공: {alarm_name}")
            return {'statusCode': 200, 'body': 'Success'}
        else:
            print(f"Slack 알림 전송 실패: {response.status}")
            return {'statusCode': response.status, 'body': 'Failed'}
            
    except Exception as e:
        print(f"Slack 알림 처리 오류: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}
'''
        
        # 코드를 ZIP 형태로 패키징
        import zipfile
        import io
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr('lambda_function.py', code)
        
        return zip_buffer.getvalue()
    
    def create_notification_test_function(self) -> str:
        """알림 테스트 함수 생성"""
        logger.info("🧪 알림 테스트 함수 생성 중...")
        
        test_code = '''
import boto3
import json
from datetime import datetime

def lambda_handler(event, context):
    """알림 시스템 테스트 함수"""
    
    sns = boto3.client('sns')
    
    # 테스트 메시지 구성
    test_messages = [
        {
            'topic_level': 'CRITICAL',
            'subject': '[TEST] Makenaide Critical Alert',
            'message': {
                'AlarmName': 'test-critical-alarm',
                'AlarmDescription': 'This is a test critical alert',
                'NewStateValue': 'ALARM',
                'OldStateValue': 'OK',
                'NewStateReason': 'Threshold Crossed: 1 datapoint [100.0 (25/01/25 10:00:00)] was greater than or equal to the threshold (80.0).',
                'StateChangeTime': datetime.utcnow().isoformat()
            }
        },
        {
            'topic_level': 'WARNING',
            'subject': '[TEST] Makenaide Warning Alert',
            'message': {
                'AlarmName': 'test-warning-alarm',
                'AlarmDescription': 'This is a test warning alert',
                'NewStateValue': 'ALARM',
                'OldStateValue': 'OK', 
                'NewStateReason': 'High CPU usage detected.',
                'StateChangeTime': datetime.utcnow().isoformat()
            }
        }
    ]
    
    results = []
    
    # 환경변수에서 토픽 ARN 읽기
    topic_arns = {
        'CRITICAL': event.get('critical_topic_arn'),
        'WARNING': event.get('warning_topic_arn'),
        'INFO': event.get('info_topic_arn')
    }
    
    for test_msg in test_messages:
        level = test_msg['topic_level']
        topic_arn = topic_arns.get(level)
        
        if not topic_arn:
            results.append({
                'level': level,
                'status': 'SKIPPED',
                'reason': 'Topic ARN not provided'
            })
            continue
        
        try:
            response = sns.publish(
                TopicArn=topic_arn,
                Subject=test_msg['subject'],
                Message=json.dumps(test_msg['message'])
            )
            
            results.append({
                'level': level,
                'status': 'SUCCESS',
                'message_id': response['MessageId']
            })
            
        except Exception as e:
            results.append({
                'level': level,
                'status': 'FAILED',
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'test_timestamp': datetime.utcnow().isoformat(),
            'results': results
        })
    }
'''
        
        return test_code
    
    def save_notification_config(self, config: Dict) -> bool:
        """알림 설정을 S3에 저장"""
        try:
            logger.info("💾 알림 시스템 설정 저장 중...")
            
            notification_config = {
                'version': '1.0',
                'created_at': datetime.utcnow().isoformat(),
                'notification_system': config,
                'setup_completed': True,
                'next_steps': [
                    "Slack 웹훅 URL 환경변수 설정",
                    "관리자 이메일 주소 확인 및 구독 승인",
                    "테스트 알림 실행으로 동작 확인"
                ]
            }
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key='notification_system/config.json',
                Body=json.dumps(notification_config, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info("✅ 알림 시스템 설정 S3 저장 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 알림 설정 저장 실패: {e}")
            return False
    
    def implement_enhanced_notifications(self, admin_emails: Optional[List[str]] = None) -> Dict:
        """향상된 알림 시스템 전체 구현"""
        logger.info("🚀 향상된 알림 시스템 구현 시작")
        logger.info("=" * 80)
        
        if not admin_emails:
            admin_emails = ['admin@example.com']  # 기본값, 사용자가 변경해야 함
        
        implementation_report = {
            'implementation_timestamp': datetime.utcnow().isoformat(),
            'topics_created': {},
            'slack_integration': {},
            'email_subscriptions': {},
            'alarm_updates': {},
            'config_saved': False,
            'overall_status': 'UNKNOWN',
            'next_steps': []
        }
        
        try:
            # 1. SNS 토픽 생성
            logger.info("\n📢 1. 레벨별 SNS 토픽 생성")
            topics = self.create_enhanced_sns_topics()
            implementation_report['topics_created'] = topics
            
            # 2. Slack 통합 설정
            logger.info("\n💬 2. Slack 웹훅 통합 설정")
            slack_result = self.setup_slack_integration(topics)
            implementation_report['slack_integration'] = slack_result
            
            # 3. 이메일 구독 설정
            logger.info("\n📧 3. 이메일 구독 설정")
            email_result = self.setup_email_subscriptions(topics, admin_emails)
            implementation_report['email_subscriptions'] = email_result
            
            # 4. CloudWatch 알람 업데이트
            logger.info("\n⚠️ 4. CloudWatch 알람 업데이트")
            alarm_result = self.update_cloudwatch_alarms(topics)
            implementation_report['alarm_updates'] = alarm_result
            
            # 5. 설정 저장
            logger.info("\n💾 5. 알림 시스템 설정 저장")
            config_saved = self.save_notification_config(implementation_report)
            implementation_report['config_saved'] = config_saved
            
            # 6. 구현 상태 평가
            success_metrics = {
                'topics': len(topics) >= 3,
                'slack': slack_result.get('lambda_function_created', False),
                'email': len(email_result.get('subscriptions_created', [])) > 0 or len(email_result.get('confirmation_required', [])) > 0,
                'alarms': len(alarm_result.get('updated_alarms', [])) > 0,
                'config': config_saved
            }
            
            success_count = sum(success_metrics.values())
            total_count = len(success_metrics)
            
            if success_count >= 4:
                implementation_report['overall_status'] = 'SUCCESS'
            elif success_count >= 2:
                implementation_report['overall_status'] = 'PARTIAL'
            else:
                implementation_report['overall_status'] = 'FAILED'
            
            # 7. 다음 단계 정의
            next_steps = []
            
            if not slack_result.get('webhook_configured', False):
                next_steps.append("Slack 웹훅 URL을 Lambda 환경변수에 설정")
            
            if email_result.get('confirmation_required', []):
                next_steps.append(f"{len(email_result['confirmation_required'])}개 이메일 구독 확인")
            
            next_steps.extend([
                "테스트 알림 전송으로 동작 확인",
                "실제 알람 발생 시 알림 수신 검증",
                "필요시 추가 관리자 이메일 구독"
            ])
            
            implementation_report['next_steps'] = next_steps
            
            # 결과 출력
            print(f"""
🔔 향상된 알림 시스템 구현 완료!

📊 구현 상태: {implementation_report['overall_status']} ({success_count}/{total_count} 성공)

📢 SNS 토픽 생성:
   • Critical: {'✅' if 'CRITICAL' in topics else '❌'}
   • Warning: {'✅' if 'WARNING' in topics else '❌'}  
   • Info: {'✅' if 'INFO' in topics else '❌'}

💬 Slack 통합:
   • Lambda 함수: {'✅' if slack_result.get('lambda_function_created') else '❌'}
   • 구독 연결: {len(slack_result.get('subscriptions_created', []))}개
   • 웹훅 설정: {'⚠️ 수동 설정 필요' if not slack_result.get('webhook_configured') else '✅'}

📧 이메일 구독:
   • 구독 완료: {len(email_result.get('subscriptions_created', []))}개
   • 확인 대기: {len(email_result.get('confirmation_required', []))}개
   • 오류: {len(email_result.get('errors', []))}개

⚠️ CloudWatch 알람 업데이트:
   • 업데이트된 알람: {len(alarm_result.get('updated_alarms', []))}개
   • 오류: {len(alarm_result.get('errors', []))}개

🔧 다음 단계:
{chr(10).join(f'   • {step}' for step in next_steps)}

📋 수동 설정 필요사항:
   1. Slack 웹훅 URL 설정:
      - AWS Lambda 콘솔에서 'makenaide-slack-notifier' 함수 열기
      - 환경변수 SLACK_WEBHOOK_URL에 웹훅 URL 입력
      
   2. 관리자 이메일 주소 업데이트:
      - 실제 관리자 이메일로 변경 필요
      - 구독 확인 이메일 확인 및 승인

💡 특징:
   • 3단계 알림 레벨 (Critical/Warning/Info)
   • Slack 실시간 알림 with 컬러 코딩
   • 이메일 알림 with 레벨 필터링
   • 기존 CloudWatch 알람과 완전 통합
   • 테스트 및 검증 기능 포함

📊 알림 라우팅:
   • CRITICAL → Slack + Email + (SMS)
   • WARNING → Slack + Email  
   • INFO → Slack만

🎯 구현 완료된 기능:
   • 레벨별 SNS 토픽 자동 생성
   • Slack Lambda 함수 with 리치 메시지
   • 이메일 구독 자동 설정
   • CloudWatch 알람 자동 연결
   • 설정 관리 및 백업
            """)
            
            return implementation_report
            
        except Exception as e:
            logger.error(f"❌ 향상된 알림 시스템 구현 실패: {e}")
            implementation_report['overall_status'] = 'ERROR'
            implementation_report['error'] = str(e)
            return implementation_report

def main():
    """메인 실행"""
    system = EnhancedNotificationSystem()
    
    # 실제 관리자 이메일 주소로 변경하세요
    admin_emails = [
        'your-admin@example.com',  # 실제 이메일로 변경 필요
    ]
    
    report = system.implement_enhanced_notifications(admin_emails)
    
    if report['overall_status'] in ['SUCCESS', 'PARTIAL']:
        print("\n🎉 향상된 알림 시스템 구현 성공!")
        print("Slack 웹훅 URL과 관리자 이메일 설정을 완료하세요.")
        exit(0)
    else:
        print("\n⚠️ 향상된 알림 시스템 구현 중 오류 발생!")
        exit(1)

if __name__ == '__main__':
    main()