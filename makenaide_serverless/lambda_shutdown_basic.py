#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide Basic Shutdown
기능: EC2 인스턴스 종료 (Basic 파이프라인용)

🎯 주요 기능:
1. EC2 인스턴스 종료
2. 상태 확인
3. 종료 완료 응답
"""

import json
import boto3
import logging
from datetime import datetime

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
ec2_client = boto3.client('ec2', region_name='ap-northeast-2')

# EC2 설정
INSTANCE_ID = 'i-082bf343089af62d3'
REGION = 'ap-northeast-2'

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🔌 Makenaide Basic Shutdown 시작")
        logger.info(f"📅 시작 시간: {datetime.now().isoformat()}")
        logger.info(f"📍 대상 인스턴스: {INSTANCE_ID}")
        
        # 현재 인스턴스 상태 확인
        try:
            response = ec2_client.describe_instances(InstanceIds=[INSTANCE_ID])
            current_state = response['Reservations'][0]['Instances'][0]['State']['Name']
            logger.info(f"📊 현재 인스턴스 상태: {current_state}")
            
            if current_state in ['stopped', 'stopping']:
                logger.info("✅ 인스턴스가 이미 종료되었거나 종료 중입니다")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'EC2 인스턴스가 이미 종료되었거나 종료 중입니다',
                        'instance_id': INSTANCE_ID,
                        'state': current_state,
                        'timestamp': datetime.now().isoformat(),
                        'version': 'basic_shutdown_v1.0'
                    })
                }
            
            elif current_state == 'running':
                logger.info("🔄 EC2 인스턴스 종료 중...")
                
                # 인스턴스 종료
                stop_response = ec2_client.stop_instances(InstanceIds=[INSTANCE_ID])
                logger.info(f"✅ 종료 명령 전송 성공: {stop_response}")
                
                # 종료 대기 (최대 5분)
                logger.info("⏳ 인스턴스 종료 완료까지 대기 중...")
                waiter = ec2_client.get_waiter('instance_stopped')
                waiter.wait(
                    InstanceIds=[INSTANCE_ID],
                    WaiterConfig={
                        'Delay': 15,  # 15초마다 확인
                        'MaxAttempts': 20  # 최대 20번 시도 (5분)
                    }
                )
                
                # 최종 상태 확인
                response = ec2_client.describe_instances(InstanceIds=[INSTANCE_ID])
                final_state = response['Reservations'][0]['Instances'][0]['State']['Name']
                
                logger.info(f"🎉 EC2 인스턴스 종료 완료! 상태: {final_state}")
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'EC2 인스턴스 종료 완료',
                        'instance_id': INSTANCE_ID,
                        'state': final_state,
                        'timestamp': datetime.now().isoformat(),
                        'version': 'basic_shutdown_v1.0'
                    })
                }
                
            else:
                logger.warning(f"⚠️ 예상치 못한 인스턴스 상태: {current_state}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'인스턴스 상태가 종료할 수 없는 상태입니다: {current_state}',
                        'instance_id': INSTANCE_ID,
                        'state': current_state,
                        'timestamp': datetime.now().isoformat()
                    })
                }
        
        except Exception as e:
            logger.error(f"❌ EC2 작업 중 오류: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'EC2 작업 실패: {str(e)}',
                    'instance_id': INSTANCE_ID,
                    'timestamp': datetime.now().isoformat()
                })
            }
            
    except Exception as e:
        logger.error(f"❌ Shutdown 치명적 오류: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Basic Shutdown 실행 실패',
                'timestamp': datetime.now().isoformat(),
                'version': 'basic_shutdown_v1.0'
            })
        } 