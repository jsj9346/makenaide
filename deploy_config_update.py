#!/usr/bin/env python3
"""
Makenaide 설정 파일 배포 스크립트
EC2 네트워크 문제 시 대안 배포 방법
"""

import boto3
import zipfile
import io
import json
from datetime import datetime

def create_deployment_lambda():
    """설정 파일 배포용 Lambda 함수 생성"""

    # Lambda 함수 코드
    lambda_code = '''
import json
import boto3
import os

def lambda_handler(event, context):
    """EC2 인스턴스에 설정 파일 배포"""

    # 설정 파일 내용
    config_content = """# Makenaide Market Sentiment 임계값 설정 파일
# 파이프라인 실행 조건을 제어하는 핵심 설정

market_thermometer:
  description: "시장 체온계 임계값 설정 - 파이프라인 실행 여부 결정"

  # 임계값 설정 (완화된 값으로 조정)
  thresholds:
    # 상승종목 비율 임계값 (기존 40% → 30%로 완화)
    min_pct_up: 30.0

    # 거래대금 집중도 허용 한계 (기존 75% → 85%로 완화)
    max_top10_volume: 85.0

    # MA200 상회 종목 비율 (기존 20% → 10%로 완화)
    min_ma200_above: 10.0

    # 종합 시장 점수 임계값 (기존 40점 → 25점으로 완화)
    min_sentiment_score: 25.0

  # 설정 변경 이력
  change_history:
    - date: "2025-09-18"
      reason: "파이프라인 비활성화 문제 해결"
      changes:
        - "min_pct_up: 40.0 → 30.0 (상승종목 비율 완화)"
        - "max_top10_volume: 75.0 → 85.0 (집중도 완화)"
        - "min_ma200_above: 20.0 → 10.0 (MA200 상회 완화)"
        - "min_sentiment_score: 40.0 → 25.0 (종합점수 완화)"
"""

    try:
        # S3에 설정 파일 업로드
        s3 = boto3.client('s3')
        bucket_name = 'makenaide-config-deploy'

        s3.put_object(
            Bucket=bucket_name,
            Key='config/filter_rules_config.yaml',
            Body=config_content,
            ContentType='text/yaml'
        )

        # SNS 알림 발송
        sns = boto3.client('sns')
        message = f"""
🔧 Makenaide 설정 파일 업데이트 완료

📅 배포 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

🎯 주요 변경사항:
• 상승종목 비율: 40% → 30%
• MA200 상회 비율: 20% → 10%
• 거래대금 집중도: 75% → 85%
• 종합점수: 40점 → 25점

📋 다음 단계:
1. EC2 실행 시 S3에서 자동 다운로드
2. 파이프라인 재시작 테스트
3. 임계값 통과 여부 확인

🚀 다음 EventBridge 실행: 18:00 KST
        """

        sns.publish(
            TopicArn='arn:aws:sns:ap-northeast-2:901361833359:makenaide-system-alerts',
            Subject='[Makenaide] 설정 파일 업데이트 완료',
            Message=message
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': '설정 파일 S3 업로드 완료',
                'timestamp': datetime.now().isoformat()
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }
'''

    # Lambda 함수 생성
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

    # ZIP 파일 생성
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('lambda_function.py', lambda_code)

    zip_buffer.seek(0)

    try:
        response = lambda_client.create_function(
            FunctionName='makenaide-config-deployer',
            Runtime='python3.11',
            Role='arn:aws:iam::901361833359:role/makenaide-lambda-execution-role',
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_buffer.read()},
            Description='Makenaide 설정 파일 배포 Lambda',
            Timeout=60,
            MemorySize=128
        )

        print("✅ Lambda 함수 생성 완료:", response['FunctionArn'])
        return response['FunctionArn']

    except lambda_client.exceptions.ResourceConflictException:
        # 이미 존재하는 경우 업데이트
        response = lambda_client.update_function_code(
            FunctionName='makenaide-config-deployer',
            ZipFile=zip_buffer.read()
        )
        print("✅ Lambda 함수 업데이트 완료:", response['FunctionArn'])
        return response['FunctionArn']

def deploy_config():
    """설정 파일 배포 실행"""

    # 1. Lambda 함수 생성/업데이트
    function_arn = create_deployment_lambda()

    # 2. Lambda 함수 실행
    lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

    response = lambda_client.invoke(
        FunctionName='makenaide-config-deployer',
        InvocationType='RequestResponse'
    )

    result = json.loads(response['Payload'].read())
    print("📋 배포 결과:", result)

    if response['StatusCode'] == 200:
        print("🎉 설정 파일 S3 업로드 완료!")
        print("📌 다음 EC2 실행 시 자동으로 설정 파일이 적용됩니다.")
    else:
        print("❌ 배포 실패:", result)

if __name__ == "__main__":
    deploy_config()