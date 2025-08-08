#!/usr/bin/env python3
"""
AWS CloudWatch 모니터링 시스템

🎯 목적:
- Lambda 함수 성능 모니터링
- EC2 인스턴스 리소스 모니터링  
- RDS 데이터베이스 성능 추적
- 커스텀 메트릭 수집 및 알람 설정

🔧 사용법:
python aws_cloudwatch_monitor.py [--setup-alarms] [--collect-metrics]
"""

import boto3
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import os
from dotenv import load_dotenv

class CloudWatchMonitor:
    """AWS CloudWatch 모니터링 클래스"""
    
    def __init__(self):
        load_dotenv('env.aws')
        
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # AWS 클라이언트 초기화
        try:
            self.cloudwatch = boto3.client('cloudwatch')
            self.ec2 = boto3.client('ec2')
            self.rds = boto3.client('rds')
            self.lambda_client = boto3.client('lambda')
            self.logs_client = boto3.client('logs')
            
            self.logger.info("✅ AWS 클라이언트 초기화 완료")
        except Exception as e:
            self.logger.error(f"❌ AWS 클라이언트 초기화 실패: {e}")
            raise
        
        # 설정값
        self.function_name = os.getenv('LAMBDA_FUNCTION_NAME', 'makenaide-controller')
        self.db_instance_id = 'makenaide'
        self.sns_topic_arn = os.getenv('SNS_TOPIC_ARN')  # 선택적

    def setup_custom_alarms(self) -> bool:
        """커스텀 CloudWatch 알람 설정"""
        self.logger.info("🚨 CloudWatch 알람 설정 시작...")
        
        alarms = [
            # Lambda 함수 에러율 알람
            {
                'AlarmName': 'makenaide-lambda-error-rate',
                'ComparisonOperator': 'GreaterThanThreshold',
                'EvaluationPeriods': 2,
                'MetricName': 'Errors',
                'Namespace': 'AWS/Lambda',
                'Period': 300,
                'Statistic': 'Sum',
                'Threshold': 5.0,
                'ActionsEnabled': True,
                'AlarmDescription': 'Makenaide Lambda 함수 에러 발생',
                'Dimensions': [
                    {
                        'Name': 'FunctionName',
                        'Value': self.function_name
                    }
                ],
                'Unit': 'Count'
            },
            
            # Lambda 함수 실행 시간 알람
            {
                'AlarmName': 'makenaide-lambda-duration',
                'ComparisonOperator': 'GreaterThanThreshold',
                'EvaluationPeriods': 2,
                'MetricName': 'Duration',
                'Namespace': 'AWS/Lambda',
                'Period': 300,
                'Statistic': 'Average',
                'Threshold': 600000.0,  # 10분 (밀리초)
                'ActionsEnabled': True,
                'AlarmDescription': 'Makenaide Lambda 함수 실행시간 초과',
                'Dimensions': [
                    {
                        'Name': 'FunctionName',
                        'Value': self.function_name
                    }
                ],
                'Unit': 'Milliseconds'
            },
            
            # RDS CPU 사용률 알람
            {
                'AlarmName': 'makenaide-rds-cpu-high',
                'ComparisonOperator': 'GreaterThanThreshold',
                'EvaluationPeriods': 3,
                'MetricName': 'CPUUtilization',
                'Namespace': 'AWS/RDS',
                'Period': 300,
                'Statistic': 'Average',
                'Threshold': 80.0,
                'ActionsEnabled': True,
                'AlarmDescription': 'Makenaide RDS CPU 사용률 높음',
                'Dimensions': [
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': self.db_instance_id
                    }
                ],
                'Unit': 'Percent'
            },
            
            # RDS 연결 수 알람
            {
                'AlarmName': 'makenaide-rds-connections-high',
                'ComparisonOperator': 'GreaterThanThreshold',
                'EvaluationPeriods': 2,
                'MetricName': 'DatabaseConnections',
                'Namespace': 'AWS/RDS',
                'Period': 300,
                'Statistic': 'Average',
                'Threshold': 15.0,
                'ActionsEnabled': True,
                'AlarmDescription': 'Makenaide RDS 연결 수 높음',
                'Dimensions': [
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': self.db_instance_id
                    }
                ],
                'Unit': 'Count'
            }
        ]
        
        # SNS 토픽이 있으면 알람 액션 추가
        if self.sns_topic_arn:
            for alarm in alarms:
                alarm['AlarmActions'] = [self.sns_topic_arn]
                alarm['OKActions'] = [self.sns_topic_arn]
        
        # 알람 생성
        created_count = 0
        for alarm in alarms:
            try:
                self.cloudwatch.put_metric_alarm(**alarm)
                self.logger.info(f"✅ 알람 생성: {alarm['AlarmName']}")
                created_count += 1
            except Exception as e:
                self.logger.error(f"❌ 알람 생성 실패 {alarm['AlarmName']}: {e}")
        
        self.logger.info(f"🎯 총 {created_count}개 알람 설정 완료")
        return created_count > 0

    def collect_lambda_metrics(self, hours: int = 24) -> Dict[str, Any]:
        """Lambda 함수 메트릭 수집"""
        self.logger.info(f"📊 Lambda 메트릭 수집 시작 (최근 {hours}시간)")
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        metrics = {}
        
        metric_queries = [
            ('Invocations', 'Sum', '호출 횟수'),
            ('Duration', 'Average', '평균 실행시간'),
            ('Errors', 'Sum', '에러 횟수'),
            ('Throttles', 'Sum', '스로틀 횟수')
        ]
        
        for metric_name, statistic, description in metric_queries:
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName=metric_name,
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': self.function_name
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,  # 1시간 단위
                    Statistics=[statistic]
                )
                
                datapoints = response.get('Datapoints', [])
                if datapoints:
                    # 시간순 정렬
                    datapoints.sort(key=lambda x: x['Timestamp'])
                    latest_value = datapoints[-1][statistic]
                    total_value = sum(dp[statistic] for dp in datapoints)
                    
                    metrics[metric_name] = {
                        'description': description,
                        'latest_value': latest_value,
                        'total_value': total_value,
                        'datapoints_count': len(datapoints),
                        'unit': datapoints[-1].get('Unit', 'None')
                    }
                    
                    self.logger.info(f"✅ {description}: 최근값={latest_value}, 총합={total_value}")
                else:
                    self.logger.warning(f"⚠️ {description}: 데이터 없음")
                    metrics[metric_name] = {'description': description, 'no_data': True}
                    
            except Exception as e:
                self.logger.error(f"❌ {metric_name} 메트릭 수집 실패: {e}")
                metrics[metric_name] = {'description': description, 'error': str(e)}
        
        return metrics

    def collect_rds_metrics(self, hours: int = 24) -> Dict[str, Any]:
        """RDS 메트릭 수집"""
        self.logger.info(f"📊 RDS 메트릭 수집 시작 (최근 {hours}시간)")
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        metrics = {}
        
        metric_queries = [
            ('CPUUtilization', 'Average', 'CPU 사용률', 'Percent'),
            ('DatabaseConnections', 'Average', '데이터베이스 연결 수', 'Count'),
            ('FreeableMemory', 'Average', '여유 메모리', 'Bytes'),
            ('ReadLatency', 'Average', '읽기 지연시간', 'Seconds'),
            ('WriteLatency', 'Average', '쓰기 지연시간', 'Seconds')
        ]
        
        for metric_name, statistic, description, unit in metric_queries:
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName=metric_name,
                    Dimensions=[
                        {
                            'Name': 'DBInstanceIdentifier',
                            'Value': self.db_instance_id
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,  # 1시간 단위
                    Statistics=[statistic]
                )
                
                datapoints = response.get('Datapoints', [])
                if datapoints:
                    datapoints.sort(key=lambda x: x['Timestamp'])
                    latest_value = datapoints[-1][statistic]
                    avg_value = sum(dp[statistic] for dp in datapoints) / len(datapoints)
                    
                    metrics[metric_name] = {
                        'description': description,
                        'latest_value': latest_value,
                        'average_value': avg_value,
                        'datapoints_count': len(datapoints),
                        'unit': unit
                    }
                    
                    if unit == 'Bytes':
                        # 메모리는 MB 단위로 표시
                        latest_mb = latest_value / (1024 * 1024)
                        avg_mb = avg_value / (1024 * 1024)
                        self.logger.info(f"✅ {description}: 최근값={latest_mb:.1f}MB, 평균={avg_mb:.1f}MB")
                    else:
                        self.logger.info(f"✅ {description}: 최근값={latest_value:.2f}, 평균={avg_value:.2f}")
                else:
                    self.logger.warning(f"⚠️ {description}: 데이터 없음")
                    metrics[metric_name] = {'description': description, 'no_data': True}
                    
            except Exception as e:
                self.logger.error(f"❌ {metric_name} 메트릭 수집 실패: {e}")
                metrics[metric_name] = {'description': description, 'error': str(e)}
        
        return metrics

    def send_custom_metric(self, metric_name: str, value: float, unit: str = 'Count', dimensions: Optional[List] = None) -> bool:
        """커스텀 메트릭 전송"""
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            
            if dimensions:
                metric_data['Dimensions'] = dimensions
            
            self.cloudwatch.put_metric_data(
                Namespace='Makenaide/Trading',
                MetricData=[metric_data]
            )
            
            self.logger.info(f"📈 커스텀 메트릭 전송: {metric_name}={value} {unit}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 커스텀 메트릭 전송 실패: {e}")
            return False

    def get_recent_logs(self, log_group_name: str, hours: int = 1) -> List[Dict]:
        """최근 로그 이벤트 조회"""
        self.logger.info(f"📋 로그 조회: {log_group_name} (최근 {hours}시간)")
        
        try:
            end_time = int(datetime.utcnow().timestamp() * 1000)
            start_time = int((datetime.utcnow() - timedelta(hours=hours)).timestamp() * 1000)
            
            response = self.logs_client.filter_log_events(
                logGroupName=log_group_name,
                startTime=start_time,
                endTime=end_time,
                limit=100
            )
            
            events = response.get('events', [])
            self.logger.info(f"✅ {len(events)}개 로그 이벤트 조회")
            
            return events
            
        except Exception as e:
            self.logger.error(f"❌ 로그 조회 실패: {e}")
            return []

    def generate_monitoring_report(self, hours: int = 24) -> Dict[str, Any]:
        """종합 모니터링 보고서 생성"""
        self.logger.info(f"📊 종합 모니터링 보고서 생성 (최근 {hours}시간)")
        
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'period_hours': hours,
            'lambda_metrics': self.collect_lambda_metrics(hours),
            'rds_metrics': self.collect_rds_metrics(hours),
            'health_status': 'unknown'
        }
        
        # 헬스 상태 판정
        try:
            lambda_errors = report['lambda_metrics'].get('Errors', {}).get('total_value', 0)
            rds_cpu = report['rds_metrics'].get('CPUUtilization', {}).get('latest_value', 0)
            
            if lambda_errors > 10 or rds_cpu > 90:
                report['health_status'] = 'critical'
            elif lambda_errors > 5 or rds_cpu > 80:
                report['health_status'] = 'warning'
            else:
                report['health_status'] = 'healthy'
        except:
            pass
        
        # 보고서 저장
        report_file = f"aws_monitoring_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"📄 모니터링 보고서 저장: {report_file}")
        self.logger.info(f"🎯 시스템 상태: {report['health_status'].upper()}")
        
        return report

    def check_alarm_status(self) -> Dict[str, str]:
        """알람 상태 확인"""
        self.logger.info("🚨 알람 상태 확인...")
        
        try:
            alarm_names = [
                'makenaide-lambda-error-rate',
                'makenaide-lambda-duration', 
                'makenaide-rds-cpu-high',
                'makenaide-rds-connections-high'
            ]
            
            response = self.cloudwatch.describe_alarms(AlarmNames=alarm_names)
            alarms = response.get('MetricAlarms', [])
            
            alarm_status = {}
            for alarm in alarms:
                name = alarm['AlarmName']
                state = alarm['StateValue']
                alarm_status[name] = state
                
                if state == 'ALARM':
                    self.logger.warning(f"🚨 알람 발생: {name}")
                elif state == 'OK':
                    self.logger.info(f"✅ 정상: {name}")
                else:
                    self.logger.info(f"❓ 상태 불분명: {name} ({state})")
            
            return alarm_status
            
        except Exception as e:
            self.logger.error(f"❌ 알람 상태 확인 실패: {e}")
            return {}


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='AWS CloudWatch 모니터링')
    parser.add_argument('--setup-alarms', action='store_true', help='알람 설정')
    parser.add_argument('--collect-metrics', action='store_true', help='메트릭 수집')
    parser.add_argument('--hours', type=int, default=24, help='수집 기간 (시간)')
    parser.add_argument('--check-alarms', action='store_true', help='알람 상태 확인')
    
    args = parser.parse_args()
    
    monitor = CloudWatchMonitor()
    
    if args.setup_alarms:
        monitor.setup_custom_alarms()
    
    if args.collect_metrics:
        monitor.generate_monitoring_report(args.hours)
    
    if args.check_alarms:
        monitor.check_alarm_status()
    
    if not any([args.setup_alarms, args.collect_metrics, args.check_alarms]):
        # 기본 동작: 종합 모니터링
        monitor.generate_monitoring_report(args.hours)
        monitor.check_alarm_status()


if __name__ == '__main__':
    main() 