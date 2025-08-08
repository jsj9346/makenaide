#!/usr/bin/env python3
"""
Lambda 비용 모니터링 시스템

🎯 목적:
- Lambda 함수들의 실행 비용 추적
- 비용 절감 효과 측정
- 예산 초과 알림 시스템

💰 추적 대상:
- 실행 시간 기반 비용
- 메모리 사용량 기반 비용
- API 호출 비용 (SQS, RDS 등)
"""

import boto3
import json
from datetime import datetime, timedelta
from typing import Dict, List
import os

class LambdaCostMonitor:
    """Lambda 비용 모니터링 클래스"""
    
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch', region_name='ap-northeast-2')
        self.lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        self.pricing = boto3.client('pricing', region_name='us-east-1')  # Pricing은 us-east-1만 지원
        
        # Lambda 함수 목록
        self.functions = [
            'makenaide-ticker-scanner',
            'makenaide-ohlcv-collector', 
            'makenaide-orchestrator',
            'makenaide-controller'  # 기존 EC2 제어 함수
        ]
        
        # Lambda 요금 정보 (ap-northeast-2 기준, USD)
        self.lambda_pricing = {
            'requests': 0.0000002,  # 100만 요청당 $0.20
            'gb_second': 0.0000166667,  # GB-초당 $0.0000166667
            'provisioned_concurrency': 0.0000041667  # GB-초당 $0.0000041667
        }

    def get_function_metrics(self, function_name: str, hours: int = 24) -> Dict:
        """Lambda 함수의 메트릭 수집"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)
            
            metrics = {}
            
            # 1. 실행 횟수
            invocations_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName='Invocations',
                Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1시간 단위
                Statistics=['Sum']
            )
            
            total_invocations = sum(dp['Sum'] for dp in invocations_response['Datapoints'])
            metrics['invocations'] = total_invocations
            
            # 2. 실행 시간 (Duration)
            duration_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName='Duration',
                Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average', 'Sum']
            )
            
            if duration_response['Datapoints']:
                avg_duration = sum(dp['Average'] for dp in duration_response['Datapoints']) / len(duration_response['Datapoints'])
                total_duration = sum(dp['Sum'] for dp in duration_response['Datapoints'])
                metrics['avg_duration_ms'] = avg_duration
                metrics['total_duration_ms'] = total_duration
            else:
                metrics['avg_duration_ms'] = 0
                metrics['total_duration_ms'] = 0
            
            # 3. 에러 수
            errors_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName='Errors',
                Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum']
            )
            
            total_errors = sum(dp['Sum'] for dp in errors_response['Datapoints'])
            metrics['errors'] = total_errors
            
            return metrics
            
        except Exception as e:
            print(f"❌ {function_name} 메트릭 수집 실패: {e}")
            return {}

    def calculate_function_cost(self, function_name: str, metrics: Dict) -> Dict:
        """Lambda 함수의 비용 계산"""
        try:
            # 함수 설정 정보 가져오기
            function_config = self.lambda_client.get_function(FunctionName=function_name)
            memory_mb = function_config['Configuration']['MemorySize']
            memory_gb = memory_mb / 1024
            
            # 1. 요청 비용 계산
            request_cost = metrics.get('invocations', 0) * self.lambda_pricing['requests']
            
            # 2. 컴퓨팅 비용 계산 (GB-초)
            total_duration_seconds = metrics.get('total_duration_ms', 0) / 1000
            gb_seconds = memory_gb * total_duration_seconds
            compute_cost = gb_seconds * self.lambda_pricing['gb_second']
            
            # 3. 총 비용
            total_cost = request_cost + compute_cost
            
            return {
                'function_name': function_name,
                'memory_mb': memory_mb,
                'invocations': metrics.get('invocations', 0),
                'avg_duration_ms': metrics.get('avg_duration_ms', 0),
                'total_duration_ms': metrics.get('total_duration_ms', 0),
                'errors': metrics.get('errors', 0),
                'gb_seconds': gb_seconds,
                'request_cost_usd': request_cost,
                'compute_cost_usd': compute_cost,
                'total_cost_usd': total_cost,
                'success_rate': (1 - metrics.get('errors', 0) / max(metrics.get('invocations', 1), 1)) * 100
            }
            
        except Exception as e:
            print(f"❌ {function_name} 비용 계산 실패: {e}")
            return {}

    def generate_cost_report(self, hours: int = 24) -> Dict:
        """비용 리포트 생성"""
        print(f"📊 Lambda 비용 분석 리포트 생성 (최근 {hours}시간)")
        print("=" * 60)
        
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'period_hours': hours,
            'functions': [],
            'summary': {}
        }
        
        total_cost = 0
        total_invocations = 0
        total_errors = 0
        
        for function_name in self.functions:
            print(f"🔍 {function_name} 분석 중...")
            
            # 메트릭 수집
            metrics = self.get_function_metrics(function_name, hours)
            
            if metrics:
                # 비용 계산
                cost_data = self.calculate_function_cost(function_name, metrics)
                
                if cost_data:
                    report['functions'].append(cost_data)
                    total_cost += cost_data['total_cost_usd']
                    total_invocations += cost_data['invocations']
                    total_errors += cost_data['errors']
                    
                    print(f"  💰 비용: ${cost_data['total_cost_usd']:.6f}")
                    print(f"  📞 실행: {cost_data['invocations']}회")
                    print(f"  ⏱️  평균 실행시간: {cost_data['avg_duration_ms']:.0f}ms")
                    print(f"  ✅ 성공률: {cost_data['success_rate']:.1f}%")
                else:
                    print(f"  ⚠️ 비용 계산 실패")
            else:
                print(f"  ⚠️ 메트릭 수집 실패")
            
            print()
        
        # 요약 정보
        report['summary'] = {
            'total_cost_usd': total_cost,
            'total_invocations': total_invocations,
            'total_errors': total_errors,
            'overall_success_rate': (1 - total_errors / max(total_invocations, 1)) * 100,
            'avg_cost_per_invocation': total_cost / max(total_invocations, 1),
            'estimated_monthly_cost': total_cost * (30 * 24 / hours)  # 월 예상 비용
        }
        
        # 결과 출력
        print("📋 비용 요약")
        print("=" * 60)
        print(f"💰 총 비용: ${total_cost:.6f}")
        print(f"📞 총 실행 횟수: {total_invocations:,}")
        print(f"❌ 총 에러 수: {total_errors}")
        print(f"✅ 전체 성공률: {report['summary']['overall_success_rate']:.1f}%")
        print(f"💵 실행당 평균 비용: ${report['summary']['avg_cost_per_invocation']:.8f}")
        print(f"📅 월 예상 비용: ${report['summary']['estimated_monthly_cost']:.4f}")
        
        return report

    def compare_with_ec2_cost(self, ec2_hours_per_day: float = 6) -> Dict:
        """EC2 vs Lambda 비용 비교"""
        print("\n💰 EC2 vs Lambda 비용 비교")
        print("=" * 60)
        
        # EC2 t3.medium 비용 (ap-northeast-2, 온디맨드)
        ec2_hourly_cost = 0.0416  # USD per hour
        daily_ec2_cost = ec2_hours_per_day * ec2_hourly_cost
        monthly_ec2_cost = daily_ec2_cost * 30
        
        # 현재 Lambda 비용 (24시간 기준)
        lambda_report = self.generate_cost_report(24)
        daily_lambda_cost = lambda_report['summary']['total_cost_usd']
        monthly_lambda_cost = lambda_report['summary']['estimated_monthly_cost']
        
        # 절감 효과 계산
        daily_savings = daily_ec2_cost - daily_lambda_cost
        monthly_savings = monthly_ec2_cost - monthly_lambda_cost
        savings_percentage = (daily_savings / daily_ec2_cost) * 100
        
        comparison = {
            'ec2_daily_cost': daily_ec2_cost,
            'ec2_monthly_cost': monthly_ec2_cost,
            'lambda_daily_cost': daily_lambda_cost,
            'lambda_monthly_cost': monthly_lambda_cost,
            'daily_savings': daily_savings,
            'monthly_savings': monthly_savings,
            'savings_percentage': savings_percentage
        }
        
        print(f"🖥️  EC2 일일 비용 ({ec2_hours_per_day}시간): ${daily_ec2_cost:.4f}")
        print(f"🖥️  EC2 월 비용: ${monthly_ec2_cost:.2f}")
        print(f"⚡ Lambda 일일 비용: ${daily_lambda_cost:.6f}")
        print(f"⚡ Lambda 월 예상 비용: ${monthly_lambda_cost:.4f}")
        print(f"💾 일일 절감액: ${daily_savings:.4f}")
        print(f"💾 월 절감액: ${monthly_savings:.2f}")
        print(f"📊 절감률: {savings_percentage:.1f}%")
        
        if savings_percentage > 0:
            print(f"🎉 Lambda 전환으로 {savings_percentage:.1f}% 비용 절감!")
        else:
            print(f"⚠️ Lambda 비용이 {abs(savings_percentage):.1f}% 더 높음")
        
        return comparison

    def save_report(self, report: Dict, filename: str = None):
        """리포트를 파일로 저장"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'lambda_cost_report_{timestamp}.json'
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            print(f"📄 리포트 저장 완료: {filename}")
        except Exception as e:
            print(f"❌ 리포트 저장 실패: {e}")

def main():
    """메인 실행 함수"""
    print("🚀 Makenaide Lambda 비용 모니터링 시작")
    print("=" * 60)
    
    monitor = LambdaCostMonitor()
    
    try:
        # 비용 리포트 생성
        report = monitor.generate_cost_report(24)
        
        # EC2와 비용 비교
        comparison = monitor.compare_with_ec2_cost(6)  # 하루 6시간 EC2 실행 가정
        
        # 리포트 저장
        full_report = {
            'cost_analysis': report,
            'cost_comparison': comparison,
            'recommendations': [
                "티커 스캔 주기 최적화로 추가 비용 절감 가능",
                "OHLCV 수집 배치 크기 조정으로 효율성 향상",
                "에러율 개선으로 불필요한 재실행 비용 절약"
            ]
        }
        
        monitor.save_report(full_report)
        
        print(f"\n✅ Lambda 비용 모니터링 완료!")
        
    except Exception as e:
        print(f"❌ 모니터링 중 오류 발생: {e}")

if __name__ == '__main__':
    main() 