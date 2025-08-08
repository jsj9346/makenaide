#!/usr/bin/env python3
"""
🧪 알림 시스템 테스트 함수 생성
- Slack 및 이메일 알림 테스트
- 다양한 알림 레벨 테스트
- 실제 SNS 발송 테스트
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

class NotificationTester:
    """알림 시스템 테스터 클래스"""
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        self.sns_client = boto3.client('sns', region_name='ap-northeast-2')
        
        self.topic_arns = {
            'CRITICAL': 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-critical-alerts',
            'WARNING': 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-warning-alerts',
            'INFO': 'arn:aws:sns:ap-northeast-2:901361833359:makenaide-info-alerts'
        }
        
        self.lambda_role_arn = 'arn:aws:iam::901361833359:role/makenaide-lambda-execution-role'
    
    def create_test_lambda_function(self) -> Dict:
        """알림 테스트 Lambda 함수 생성"""
        logger.info("🧪 알림 테스트 Lambda 함수 생성 중...")
        
        result = {
            'function_created': False,
            'function_arn': None,
            'error': None
        }
        
        try:
            function_name = 'makenaide-notification-tester'
            
            # 기존 함수 삭제 (있다면)
            try:
                self.lambda_client.delete_function(FunctionName=function_name)
                logger.info(f"🗑️ 기존 테스트 함수 삭제: {function_name}")
            except self.lambda_client.exceptions.ResourceNotFoundException:
                pass
            
            # Lambda 함수 코드 생성
            lambda_code = self._generate_test_lambda_zip()
            
            # Lambda 함수 생성
            response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.11',
                Role=self.lambda_role_arn,
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': lambda_code},
                Description='Makenaide notification system tester',
                Timeout=60,
                Environment={
                    'Variables': {
                        'CRITICAL_TOPIC_ARN': self.topic_arns['CRITICAL'],
                        'WARNING_TOPIC_ARN': self.topic_arns['WARNING'],
                        'INFO_TOPIC_ARN': self.topic_arns['INFO']
                    }
                }
            )
            
            function_arn = response['FunctionArn']
            result['function_created'] = True
            result['function_arn'] = function_arn
            
            logger.info(f"✅ 알림 테스트 Lambda 함수 생성 완료: {function_arn}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 알림 테스트 Lambda 함수 생성 실패: {e}")
            result['error'] = str(e)
            return result
    
    def _generate_test_lambda_zip(self) -> bytes:
        """테스트 Lambda 함수 ZIP 파일 생성"""
        code = '''
import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """알림 시스템 테스트 함수"""
    
    sns = boto3.client('sns')
    
    # 환경변수에서 토픽 ARN 읽기
    topic_arns = {
        'CRITICAL': os.environ.get('CRITICAL_TOPIC_ARN'),
        'WARNING': os.environ.get('WARNING_TOPIC_ARN'),
        'INFO': os.environ.get('INFO_TOPIC_ARN')
    }
    
    # 테스트 타입 결정
    test_type = event.get('test_type', 'all')
    
    # 테스트 메시지 구성
    test_messages = {
        'CRITICAL': {
            'subject': '[TEST CRITICAL] Makenaide 중요 알림 테스트',
            'message': {
                'AlarmName': 'test-critical-alarm',
                'AlarmDescription': '이것은 중요 알림 테스트입니다. Slack과 이메일로 전송됩니다.',
                'NewStateValue': 'ALARM',
                'OldStateValue': 'OK',
                'NewStateReason': 'Critical threshold exceeded: CPU usage > 90%',
                'StateChangeTime': datetime.utcnow().isoformat(),
                'Region': 'ap-northeast-2',
                'AccountId': '901361833359'
            }
        },
        'WARNING': {
            'subject': '[TEST WARNING] Makenaide 경고 알림 테스트',
            'message': {
                'AlarmName': 'test-warning-alarm',
                'AlarmDescription': '이것은 경고 알림 테스트입니다. Slack과 이메일로 전송됩니다.',
                'NewStateValue': 'ALARM',
                'OldStateValue': 'OK',
                'NewStateReason': 'Warning threshold exceeded: Memory usage > 80%',
                'StateChangeTime': datetime.utcnow().isoformat(),
                'Region': 'ap-northeast-2',
                'AccountId': '901361833359'
            }
        },
        'INFO': {
            'subject': '[TEST INFO] Makenaide 정보 알림 테스트',
            'message': {
                'AlarmName': 'test-info-notification',
                'AlarmDescription': '이것은 정보 알림 테스트입니다. Slack으로만 전송됩니다.',
                'NewStateValue': 'OK',
                'OldStateValue': 'ALARM',
                'NewStateReason': 'System returned to normal state',
                'StateChangeTime': datetime.utcnow().isoformat(),
                'Region': 'ap-northeast-2',
                'AccountId': '901361833359'
            }
        }
    }
    
    results = []
    
    # 테스트할 레벨 결정
    if test_type == 'all':
        test_levels = ['CRITICAL', 'WARNING', 'INFO']
    else:
        test_levels = [test_type.upper()]
    
    for level in test_levels:
        if level not in topic_arns or not topic_arns[level]:
            results.append({
                'level': level,
                'status': 'SKIPPED',
                'reason': f'{level} topic ARN not found'
            })
            continue
        
        try:
            test_msg = test_messages[level]
            
            response = sns.publish(
                TopicArn=topic_arns[level],
                Subject=test_msg['subject'],
                Message=json.dumps(test_msg['message'], ensure_ascii=False, indent=2)
            )
            
            results.append({
                'level': level,
                'status': 'SUCCESS',
                'message_id': response['MessageId'],
                'topic_arn': topic_arns[level],
                'timestamp': datetime.utcnow().isoformat()
            })
            
            print(f"✅ {level} 테스트 알림 전송 성공: {response['MessageId']}")
            
        except Exception as e:
            results.append({
                'level': level,
                'status': 'FAILED',
                'error': str(e),
                'topic_arn': topic_arns[level]
            })
            
            print(f"❌ {level} 테스트 알림 전송 실패: {str(e)}")
    
    # 결과 요약
    success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
    total_count = len(results)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'test_timestamp': datetime.utcnow().isoformat(),
            'test_type': test_type,
            'success_count': success_count,
            'total_count': total_count,
            'success_rate': f"{(success_count/total_count*100):.1f}%" if total_count > 0 else "0%",
            'results': results,
            'summary': f"{success_count}/{total_count} 알림 테스트 성공"
        }, ensure_ascii=False, indent=2)
    }
'''
        
        # ZIP 파일 생성
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr('lambda_function.py', code)
        
        return zip_buffer.getvalue()
    
    def run_notification_tests(self) -> Dict:
        """알림 테스트 실행"""
        logger.info("🚀 알림 시스템 테스트 실행")
        logger.info("=" * 60)
        
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'function_creation': {},
            'test_executions': {},
            'overall_status': 'UNKNOWN'
        }
        
        try:
            # 1. 테스트 함수 생성
            logger.info("\n🧪 1. 알림 테스트 함수 생성")
            function_result = self.create_test_lambda_function()
            report['function_creation'] = function_result
            
            if not function_result['function_created']:
                logger.error("❌ 테스트 함수 생성 실패로 중단")
                report['overall_status'] = 'FAILED'
                return report
            
            # 함수 준비 대기 (잠시 대기)
            import time
            time.sleep(5)
            
            # 2. 알림 테스트 실행
            logger.info("\n📢 2. 다양한 알림 레벨 테스트 실행")
            
            test_cases = [
                {'test_type': 'CRITICAL', 'description': 'Critical 알림 테스트'},
                {'test_type': 'WARNING', 'description': 'Warning 알림 테스트'},
                {'test_type': 'INFO', 'description': 'Info 알림 테스트'}
            ]
            
            test_results = {}
            
            for test_case in test_cases:
                logger.info(f"\n📤 {test_case['description']} 실행 중...")
                
                try:
                    response = self.lambda_client.invoke(
                        FunctionName='makenaide-notification-tester',
                        InvocationType='RequestResponse',
                        Payload=json.dumps(test_case)
                    )
                    
                    if response['StatusCode'] == 200:
                        payload = json.loads(response['Payload'].read())
                        result_body = json.loads(payload['body'])
                        
                        test_results[test_case['test_type']] = {
                            'status': 'SUCCESS',
                            'result': result_body
                        }
                        
                        logger.info(f"✅ {test_case['test_type']} 테스트 성공: {result_body['summary']}")
                        
                    else:
                        test_results[test_case['test_type']] = {
                            'status': 'FAILED',
                            'error': f"HTTP {response['StatusCode']}"
                        }
                        
                        logger.error(f"❌ {test_case['test_type']} 테스트 실패: HTTP {response['StatusCode']}")
                        
                except Exception as e:
                    test_results[test_case['test_type']] = {
                        'status': 'ERROR',
                        'error': str(e)
                    }
                    
                    logger.error(f"❌ {test_case['test_type']} 테스트 오류: {e}")
            
            report['test_executions'] = test_results
            
            # 3. 전체 상태 평가
            success_count = sum(1 for result in test_results.values() if result['status'] == 'SUCCESS')
            total_count = len(test_results)
            
            if success_count == total_count:
                report['overall_status'] = 'SUCCESS'
            elif success_count > 0:
                report['overall_status'] = 'PARTIAL'
            else:
                report['overall_status'] = 'FAILED'
            
            # 결과 출력
            print(f"""
🧪 알림 시스템 테스트 완료!

📊 테스트 상태: {report['overall_status']} ({success_count}/{total_count} 성공)

🧪 테스트 함수 생성:
   • 함수 생성: {'✅' if function_result['function_created'] else '❌'}
   • 함수 ARN: {function_result.get('function_arn', 'N/A')}

📢 알림 테스트 결과:
{chr(10).join(f"   • {level}: {'✅' if result['status'] == 'SUCCESS' else '❌'} {result.get('result', {}).get('summary', result.get('error', 'Unknown'))}" for level, result in test_results.items())}

💡 테스트된 기능:
   • CRITICAL 알림 → Slack + Email
   • WARNING 알림 → Slack + Email
   • INFO 알림 → Slack만

📋 확인 사항:
   • Slack 채널에서 테스트 메시지 확인
   • 관리자 이메일에서 구독 확인 후 테스트 메시지 확인
   • 각 레벨별 색상 코딩 확인

🎯 다음 단계:
   • Slack 웹훅 URL 설정 완료 후 재테스트
   • 이메일 구독 확인 후 재테스트
   • 실제 CloudWatch 알람으로 최종 검증
            """)
            
            return report
            
        except Exception as e:
            logger.error(f"❌ 알림 시스템 테스트 실패: {e}")
            report['overall_status'] = 'ERROR'
            report['error'] = str(e)
            return report

def main():
    """메인 실행"""
    tester = NotificationTester()
    report = tester.run_notification_tests()
    
    if report['overall_status'] in ['SUCCESS', 'PARTIAL']:
        print("\n🎉 알림 시스템 테스트 완료!")
        print("Slack과 이메일에서 테스트 알림을 확인하세요.")
        exit(0)
    else:
        print("\n⚠️ 알림 시스템 테스트 실패!")
        exit(1)

if __name__ == '__main__':
    main()