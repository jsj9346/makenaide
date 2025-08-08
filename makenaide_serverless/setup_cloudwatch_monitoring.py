#!/usr/bin/env python3
"""
CloudWatch 모니터링 및 경보 설정 스크립트
EC2 인스턴스와 Makenaide 프로세스 모니터링
"""

import boto3
import json
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS 클라이언트
cloudwatch = boto3.client('cloudwatch', region_name='ap-northeast-2')
sns = boto3.client('sns', region_name='ap-northeast-2')
logs = boto3.client('logs', region_name='ap-northeast-2')

# EC2 인스턴스 정보
EC2_INSTANCE_ID = 'i-082bf343089af62d3'
EC2_IP = '52.78.186.226'

def create_sns_topic():
    """SNS 토픽 생성 또는 기존 토픽 사용"""
    try:
        # 기존 토픽 확인
        response = sns.list_topics()
        for topic in response.get('Topics', []):
            if 'makenaide-alerts' in topic['TopicArn']:
                logger.info(f"✅ 기존 SNS 토픽 사용: {topic['TopicArn']}")
                return topic['TopicArn']
        
        # 새 토픽 생성
        response = sns.create_topic(
            Name='makenaide-alerts',
            Attributes={
                'DisplayName': 'Makenaide System Alerts'
            }
        )
        topic_arn = response['TopicArn']
        logger.info(f"✅ 새 SNS 토픽 생성: {topic_arn}")
        return topic_arn
        
    except Exception as e:
        logger.error(f"❌ SNS 토픽 설정 실패: {e}")
        return None

def create_cloudwatch_alarms(topic_arn):
    """CloudWatch 경보 생성"""
    
    alarms = [
        {
            'AlarmName': 'makenaide-ec2-high-cpu',
            'AlarmDescription': 'Makenaide EC2 인스턴스 높은 CPU 사용률',
            'MetricName': 'CPUUtilization',
            'Namespace': 'AWS/EC2',
            'Statistic': 'Average',
            'Dimensions': [{'Name': 'InstanceId', 'Value': EC2_INSTANCE_ID}],
            'Period': 300,
            'EvaluationPeriods': 2,
            'Threshold': 80.0,
            'ComparisonOperator': 'GreaterThanThreshold'
        },
        {
            'AlarmName': 'makenaide-ec2-high-memory',
            'AlarmDescription': 'Makenaide EC2 인스턴스 높은 메모리 사용률',
            'MetricName': 'MemoryUtilization',
            'Namespace': 'CWAgent',
            'Statistic': 'Average',
            'Dimensions': [{'Name': 'InstanceId', 'Value': EC2_INSTANCE_ID}],
            'Period': 300,
            'EvaluationPeriods': 2,
            'Threshold': 85.0,
            'ComparisonOperator': 'GreaterThanThreshold'
        },
        {
            'AlarmName': 'makenaide-ec2-disk-space',
            'AlarmDescription': 'Makenaide EC2 인스턴스 디스크 공간 부족',
            'MetricName': 'DiskSpaceUtilization',
            'Namespace': 'CWAgent',
            'Statistic': 'Average',
            'Dimensions': [
                {'Name': 'InstanceId', 'Value': EC2_INSTANCE_ID},
                {'Name': 'Filesystem', 'Value': '/dev/nvme0n1p1'},
                {'Name': 'MountPath', 'Value': '/'}
            ],
            'Period': 300,
            'EvaluationPeriods': 1,
            'Threshold': 90.0,
            'ComparisonOperator': 'GreaterThanThreshold'
        },
        {
            'AlarmName': 'makenaide-process-failure',
            'AlarmDescription': 'Makenaide 프로세스 실행 실패',
            'MetricName': 'ProcessFailures',
            'Namespace': 'Makenaide/Custom',
            'Statistic': 'Sum',
            'Dimensions': [{'Name': 'InstanceId', 'Value': EC2_INSTANCE_ID}],
            'Period': 3600,  # 1시간
            'EvaluationPeriods': 1,
            'Threshold': 2.0,  # 1시간에 2번 이상 실패
            'ComparisonOperator': 'GreaterThanOrEqualToThreshold'
        }
    ]
    
    created_alarms = []
    for alarm in alarms:
        try:
            alarm_params = {
                'AlarmName': alarm['AlarmName'],
                'AlarmDescription': alarm['AlarmDescription'],
                'ActionsEnabled': True,
                'MetricName': alarm['MetricName'],
                'Namespace': alarm['Namespace'],
                'Statistic': alarm['Statistic'],
                'Dimensions': alarm['Dimensions'],
                'Period': alarm['Period'],
                'EvaluationPeriods': alarm['EvaluationPeriods'],
                'Threshold': alarm['Threshold'],
                'ComparisonOperator': alarm['ComparisonOperator']
            }
            
            if topic_arn:
                alarm_params['AlarmActions'] = [topic_arn]
                alarm_params['OKActions'] = [topic_arn]
            
            cloudwatch.put_metric_alarm(**alarm_params)
            logger.info(f"✅ 경보 생성: {alarm['AlarmName']}")
            created_alarms.append(alarm['AlarmName'])
            
        except Exception as e:
            logger.error(f"❌ 경보 생성 실패 ({alarm['AlarmName']}): {e}")
    
    return created_alarms

def create_custom_log_group():
    """사용자 정의 로그 그룹 생성"""
    try:
        log_group_name = '/makenaide/execution'
        
        try:
            logs.create_log_group(logGroupName=log_group_name)
            logger.info(f"✅ 로그 그룹 생성: {log_group_name}")
        except logs.exceptions.ResourceAlreadyExistsException:
            logger.info(f"ℹ️ 로그 그룹 이미 존재: {log_group_name}")
        
        # 로그 보존 기간 설정 (7일)
        logs.put_retention_policy(
            logGroupName=log_group_name,
            retentionInDays=7
        )
        
        return log_group_name
        
    except Exception as e:
        logger.error(f"❌ 로그 그룹 생성 실패: {e}")
        return None

def setup_custom_metrics():
    """사용자 정의 메트릭 설정"""
    
    # 메트릭 데이터 전송 스크립트 생성
    script_content = f'''#!/bin/bash
# CloudWatch 사용자 정의 메트릭 전송 스크립트

INSTANCE_ID="{EC2_INSTANCE_ID}"
NAMESPACE="Makenaide/Custom"

# 1. 프로세스 실행 상태 확인
if pgrep -f "python.*makenaide" > /dev/null; then
    PROCESS_RUNNING=1
else
    PROCESS_RUNNING=0
fi

# 2. 최근 실행 실패 횟수 확인 (1시간 이내)
RECENT_FAILURES=$(grep "Exit Code: 1" /home/ec2-user/makenaide/logs/execution_history.log | grep "$(date '+%Y-%m-%d %H')" | wc -l)

# 3. 로그 파일 크기 확인
LOG_SIZE=$(du -sm /home/ec2-user/makenaide/logs/ | cut -f1)

# 4. DB 연결 상태 확인
if timeout 5 python3 -c "import psycopg2; from dotenv import load_dotenv; import os; load_dotenv(); psycopg2.connect(host=os.getenv('PG_HOST'), port=os.getenv('PG_PORT'), dbname=os.getenv('PG_DATABASE'), user=os.getenv('PG_USER'), password=os.getenv('PG_PASSWORD')).close()" 2>/dev/null; then
    DB_CONNECTION=1
else
    DB_CONNECTION=0
fi

# CloudWatch에 메트릭 전송
aws cloudwatch put-metric-data --region ap-northeast-2 --namespace "$NAMESPACE" --metric-data \\
    MetricName=ProcessRunning,Value=$PROCESS_RUNNING,Unit=Count,Dimensions=InstanceId=$INSTANCE_ID \\
    MetricName=ProcessFailures,Value=$RECENT_FAILURES,Unit=Count,Dimensions=InstanceId=$INSTANCE_ID \\
    MetricName=LogFileSize,Value=$LOG_SIZE,Unit=Megabytes,Dimensions=InstanceId=$INSTANCE_ID \\
    MetricName=DBConnection,Value=$DB_CONNECTION,Unit=Count,Dimensions=InstanceId=$INSTANCE_ID

echo "$(date): 메트릭 전송 완료 - Process:$PROCESS_RUNNING, Failures:$RECENT_FAILURES, LogSize:$LOG_SIZE MB, DB:$DB_CONNECTION"
'''
    
    return script_content

def main():
    """메인 실행 함수"""
    logger.info("🚀 CloudWatch 모니터링 설정 시작...")
    
    try:
        # 1. SNS 토픽 생성
        topic_arn = create_sns_topic()
        
        # 2. CloudWatch 경보 생성
        created_alarms = create_cloudwatch_alarms(topic_arn)
        
        # 3. 사용자 정의 로그 그룹 생성
        log_group = create_custom_log_group()
        
        # 4. 사용자 정의 메트릭 스크립트 생성
        metrics_script = setup_custom_metrics()
        
        print("\n" + "="*60)
        print("🎉 CloudWatch 모니터링 설정 완료!")
        print("="*60)
        
        if topic_arn:
            print(f"📱 SNS 토픽: {topic_arn}")
        
        if created_alarms:
            print(f"⚠️ 생성된 경보: {len(created_alarms)}개")
            for alarm in created_alarms:
                print(f"   - {alarm}")
        
        if log_group:
            print(f"📋 로그 그룹: {log_group}")
        
        print(f"📊 모니터링 대상: EC2 인스턴스 {EC2_INSTANCE_ID}")
        print(f"🔧 사용자 정의 메트릭 스크립트가 생성됩니다.")
        
        # 메트릭 스크립트 반환
        return metrics_script
        
    except Exception as e:
        logger.error(f"❌ CloudWatch 설정 실패: {e}")
        raise

if __name__ == "__main__":
    metrics_script = main()
    
    # 메트릭 스크립트 파일 저장
    with open('/tmp/cloudwatch_metrics.sh', 'w') as f:
        f.write(metrics_script)
    
    print(f"\n📝 메트릭 스크립트 생성: /tmp/cloudwatch_metrics.sh")
    print("🔧 이 스크립트를 EC2에 복사하여 cron으로 실행하세요.") 