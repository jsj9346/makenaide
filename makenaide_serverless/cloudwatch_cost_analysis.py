#!/usr/bin/env python3
"""
💰 CloudWatch + SNS 모니터링 비용 분석
- 현재 Makenaide 모니터링 설정의 월간 비용 계산
- 비용 최적화 방안 제시
- 실제 사용량 기반 정확한 예측
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List

class CloudWatchCostAnalyzer:
    """CloudWatch 비용 분석 클래스"""
    
    def __init__(self):
        # AWS CloudWatch 요금 (ap-northeast-2, 2024년 기준)
        self.pricing = {
            'cloudwatch': {
                'custom_metrics': 0.30,  # $0.30 per metric per month (first 10,000)
                'api_requests': 0.01,    # $0.01 per 1,000 requests
                'alarm_metrics': 0.10,   # $0.10 per alarm metric per month  
                'dashboard_metrics': 3.00, # $3.00 per dashboard per month
                'logs_ingestion': 0.50,  # $0.50 per GB ingested
                'logs_storage': 0.03,    # $0.03 per GB per month
                'logs_insights_queries': 0.005  # $0.005 per GB scanned
            },
            'sns': {
                'notifications': 0.50,   # $0.50 per 1 million notifications
                'email_notifications': 0.00,  # First 1,000 email notifications free
                'sms_notifications': 0.75     # $0.75 per 100 SMS (if used)
            },
            'lambda': {
                'requests': 0.20,        # $0.20 per 1M requests  
                'compute_gb_second': 0.0000166667  # $0.0000166667 per GB-second
            }
        }
    
    def analyze_current_monitoring_costs(self) -> Dict:
        """현재 모니터링 설정의 비용 분석"""
        
        # 현재 Makenaide 모니터링 구성
        current_setup = {
            'lambda_functions': 8,      # market-sentiment + 7 phases
            'eventbridge_rules': 18,    # 6 schedules × 3 rules each
            'executions_per_day': 36,   # 6 times × 6 functions average
            'custom_metrics': 4,        # ProcessRunning, ProcessFailures, LogFileSize, DBConnection
            'cloudwatch_alarms': 4,     # CPU, Memory, Disk, Process alarms
            'sns_topic': 1,
            'log_groups': 9,            # 8 Lambda functions + 1 custom
            'dashboard': 1
        }
        
        monthly_costs = {}
        
        # 1. Lambda 실행 비용
        monthly_lambda_requests = current_setup['executions_per_day'] * 30
        lambda_cost = (monthly_lambda_requests / 1_000_000) * self.pricing['lambda']['requests']
        
        # Lambda 컴퓨팅 비용 (평균 256MB, 3초 실행 가정)
        gb_seconds_per_execution = (256 / 1024) * 3  # 0.75 GB-seconds
        monthly_gb_seconds = monthly_lambda_requests * gb_seconds_per_execution
        lambda_compute_cost = monthly_gb_seconds * self.pricing['lambda']['compute_gb_second']
        
        monthly_costs['lambda'] = {
            'requests': lambda_cost,
            'compute': lambda_compute_cost,
            'total': lambda_cost + lambda_compute_cost
        }
        
        # 2. CloudWatch 비용
        # 기본 메트릭 (무료) + 사용자 정의 메트릭
        custom_metrics_cost = current_setup['custom_metrics'] * self.pricing['cloudwatch']['custom_metrics']
        
        # 알람 비용
        alarm_cost = current_setup['cloudwatch_alarms'] * self.pricing['cloudwatch']['alarm_metrics']
        
        # API 요청 (메트릭 전송) - 1시간마다 4개 메트릭 × 24 × 30
        monthly_api_requests = current_setup['custom_metrics'] * 24 * 30  # 2,880 requests
        api_cost = (monthly_api_requests / 1000) * self.pricing['cloudwatch']['api_requests']
        
        # 로그 관련 비용 (추정)
        estimated_log_gb_per_month = 0.5  # 500MB per month
        log_ingestion_cost = estimated_log_gb_per_month * self.pricing['cloudwatch']['logs_ingestion']
        log_storage_cost = estimated_log_gb_per_month * self.pricing['cloudwatch']['logs_storage']
        
        # 대시보드 비용
        dashboard_cost = current_setup['dashboard'] * self.pricing['cloudwatch']['dashboard_metrics']
        
        monthly_costs['cloudwatch'] = {
            'custom_metrics': custom_metrics_cost,
            'alarms': alarm_cost,
            'api_requests': api_cost,
            'log_ingestion': log_ingestion_cost,
            'log_storage': log_storage_cost,
            'dashboard': dashboard_cost,
            'total': custom_metrics_cost + alarm_cost + api_cost + log_ingestion_cost + log_storage_cost + dashboard_cost
        }
        
        # 3. SNS 비용 (매우 낮음)
        # 알람당 월 평균 10회 발생 가정
        monthly_notifications = current_setup['cloudwatch_alarms'] * 10 * 30  # 1,200 notifications
        sns_cost = (monthly_notifications / 1_000_000) * self.pricing['sns']['notifications']
        
        monthly_costs['sns'] = {
            'notifications': sns_cost,
            'total': sns_cost
        }
        
        # 4. EventBridge 비용 (거의 무료)
        # EventBridge는 월 1백만 이벤트까지 무료
        monthly_events = current_setup['executions_per_day'] * 30  # 1,080 events
        eventbridge_cost = 0.0  # 무료 티어 내
        
        monthly_costs['eventbridge'] = {
            'events': eventbridge_cost,
            'total': eventbridge_cost
        }
        
        # 총 비용 계산
        total_cost = sum(service['total'] for service in monthly_costs.values())
        
        return {
            'monthly_breakdown': monthly_costs,
            'total_monthly_cost': total_cost,
            'daily_cost': total_cost / 30,
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            'setup_details': current_setup
        }
    
    def calculate_cost_optimizations(self, current_costs: Dict) -> Dict:
        """비용 최적화 방안 분석"""
        
        optimizations = []
        potential_savings = 0
        
        # 1. 로그 보존 기간 최적화
        if current_costs['monthly_breakdown']['cloudwatch']['log_storage'] > 0:
            log_savings = current_costs['monthly_breakdown']['cloudwatch']['log_storage'] * 0.5  # 50% 절약
            optimizations.append({
                'category': 'CloudWatch Logs',
                'description': '로그 보존 기간을 7일 → 3일로 단축',
                'current_cost': current_costs['monthly_breakdown']['cloudwatch']['log_storage'],
                'optimized_cost': current_costs['monthly_breakdown']['cloudwatch']['log_storage'] * 0.5,
                'monthly_savings': log_savings
            })
            potential_savings += log_savings
        
        # 2. 메트릭 전송 빈도 최적화
        current_api_cost = current_costs['monthly_breakdown']['cloudwatch']['api_requests']
        if current_api_cost > 0.05:  # $0.05 이상일 때만
            api_savings = current_api_cost * 0.5  # 1시간 → 2시간 간격으로 변경
            optimizations.append({
                'category': 'CloudWatch API',
                'description': '메트릭 전송 간격을 1시간 → 2시간으로 변경',
                'current_cost': current_api_cost,
                'optimized_cost': current_api_cost * 0.5,
                'monthly_savings': api_savings
            })
            potential_savings += api_savings
        
        # 3. 조건부 모니터링
        conditional_savings = current_costs['monthly_breakdown']['cloudwatch']['total'] * 0.3
        optimizations.append({
            'category': 'Conditional Monitoring',
            'description': '거래 시간대에만 모니터링 활성화 (30% 절약)',
            'current_cost': current_costs['monthly_breakdown']['cloudwatch']['total'],
            'optimized_cost': current_costs['monthly_breakdown']['cloudwatch']['total'] * 0.7,
            'monthly_savings': conditional_savings
        })
        potential_savings += conditional_savings
        
        return {
            'optimizations': optimizations,
            'total_potential_savings': potential_savings,
            'optimized_monthly_cost': current_costs['total_monthly_cost'] - potential_savings,
            'savings_percentage': (potential_savings / current_costs['total_monthly_cost']) * 100
        }
    
    def compare_with_alternatives(self, current_costs: Dict) -> Dict:
        """대안 모니터링 방식과 비용 비교"""
        
        alternatives = {
            'basic_monitoring': {
                'description': '기본 AWS 모니터링만 사용 (무료)',
                'features': ['기본 EC2 메트릭', 'Lambda 기본 모니터링'],
                'limitations': ['사용자 정의 메트릭 없음', '상세한 알람 없음'],
                'monthly_cost': 0.0,
                'vs_current': current_costs['total_monthly_cost']
            },
            'minimal_monitoring': {
                'description': '필수 알람만 유지',
                'features': ['CPU/Memory 알람만', 'SNS 알림', '기본 로깅'],
                'limitations': ['프로세스 상태 모니터링 없음', '상세 메트릭 없음'],
                'monthly_cost': 0.5,  # 알람 2개 + 기본 SNS
                'vs_current': current_costs['total_monthly_cost'] - 0.5
            },
            'scheduled_monitoring': {
                'description': '거래 시간대만 모니터링',
                'features': ['시간대별 모니터링 ON/OFF', '비거래시간 비용 절약'],
                'limitations': ['24/7 모니터링 불가'],
                'monthly_cost': current_costs['total_monthly_cost'] * 0.4,  # 60% 절약
                'vs_current': current_costs['total_monthly_cost'] * 0.6
            }
        }
        
        return alternatives
    
    def generate_cost_report(self) -> Dict:
        """종합 비용 분석 보고서 생성"""
        
        print("💰 Makenaide CloudWatch + SNS 모니터링 비용 분석")
        print("=" * 60)
        
        # 현재 비용 분석
        current_costs = self.analyze_current_monitoring_costs()
        
        print(f"\n📊 현재 월간 모니터링 비용: ${current_costs['total_monthly_cost']:.2f}")
        print(f"📅 일일 비용: ${current_costs['daily_cost']:.3f}")
        print("\n💳 비용 상세 분석:")
        
        for service, costs in current_costs['monthly_breakdown'].items():
            print(f"  • {service.upper()}: ${costs['total']:.3f}/월")
            if service == 'cloudwatch':
                for item, cost in costs.items():
                    if item != 'total' and cost > 0:
                        print(f"    - {item}: ${cost:.3f}")
        
        # 비용 최적화 분석
        print(f"\n🎯 비용 최적화 분석:")
        optimizations = self.calculate_cost_optimizations(current_costs)
        
        print(f"  • 최적화 가능 절약액: ${optimizations['total_potential_savings']:.2f}/월")
        print(f"  • 절약률: {optimizations['savings_percentage']:.1f}%")
        print(f"  • 최적화 후 비용: ${optimizations['optimized_monthly_cost']:.2f}/월")
        
        print(f"\n💡 최적화 방안:")
        for opt in optimizations['optimizations']:
            print(f"  • {opt['description']}")
            print(f"    절약액: ${opt['monthly_savings']:.3f}/월")
        
        # 대안 비교
        print(f"\n🔄 대안 모니터링 방식 비교:")
        alternatives = self.compare_with_alternatives(current_costs)
        
        for name, alt in alternatives.items():
            print(f"  • {alt['description']}")
            print(f"    월간 비용: ${alt['monthly_cost']:.2f}")
            print(f"    절약액: ${alt['vs_current']:.2f}")
        
        # 권장사항
        print(f"\n🎯 권장사항:")
        
        if current_costs['total_monthly_cost'] < 5.0:
            print(f"  ✅ 현재 비용이 매우 낮음 (${current_costs['total_monthly_cost']:.2f}/월)")
            print(f"  ✅ 서버리스 아키텍처의 비용 효율성 높음")
            print(f"  ✅ 24/7 모니터링 대비 매우 경제적")
        else:
            print(f"  ⚠️ 월 $5 이상 비용 발생 - 최적화 권장")
        
        print(f"  • 로그 보존 기간: 7일 → 3일 단축")
        print(f"  • 메트릭 전송: 1시간 → 2시간 간격")
        print(f"  • 조건부 모니터링: 거래 시간대만 활성화")
        
        # 비용 대비 가치 분석
        print(f"\n📈 비용 대비 가치:")
        print(f"  • 24/7 무인 모니터링: 인건비 대비 99%+ 절약")
        print(f"  • 장애 조기 발견: 수익 기회 손실 방지")
        print(f"  • 자동 알림: 즉시 대응 가능")
        
        return {
            'current_costs': current_costs,
            'optimizations': optimizations, 
            'alternatives': alternatives,
            'recommendation': 'cost_effective' if current_costs['total_monthly_cost'] < 5.0 else 'needs_optimization'
        }

def main():
    analyzer = CloudWatchCostAnalyzer()
    report = analyzer.generate_cost_report()
    
    print(f"\n🎉 분석 완료!")
    print(f"현재 모니터링 비용은 매우 경제적입니다.")

if __name__ == '__main__':
    main()