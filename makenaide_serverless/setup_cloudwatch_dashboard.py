#!/usr/bin/env python3
"""
📊 CloudWatch Dashboard 구축 스크립트
- Makenaide 자동매매 시스템 종합 모니터링 대시보드
- Lambda 성능, 오류율, 비용, Phase별 데이터 플로우 모니터링
"""

import boto3
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MakenaideDashboardBuilder:
    """CloudWatch 대시보드 구축 클래스"""
    
    def __init__(self):
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.region = 'ap-northeast-2'
        self.account_id = '901361833359'
        
        # Lambda 함수 목록
        self.lambda_functions = [
            'makenaide-ticker-scanner-phase0',
            'makenaide-selective-data-collection-phase1', 
            'makenaide-comprehensive-filtering-phase2',
            'makenaide-gpt-analysis-phase3',
            'makenaide-4h-analysis-phase4',
            'makenaide-condition-check-phase5',
            'makenaide-trade-execution-phase6',
            'makenaide-market-sentiment-check'
        ]
        
        self.dashboard_name = 'Makenaide-Trading-System-Dashboard'
        
    def create_lambda_performance_widgets(self) -> list:
        """Lambda 성능 모니터링 위젯 생성"""
        widgets = []
        
        # 1. Lambda 실행 시간 모니터링
        duration_widget = {
            "type": "metric",
            "x": 0, "y": 0,
            "width": 12, "height": 6,
            "properties": {
                "metrics": [
                    ["AWS/Lambda", "Duration", "FunctionName", func_name]
                    for func_name in self.lambda_functions
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": self.region,
                "title": "⚡ Lambda 실행 시간 (ms)",
                "period": 300,
                "stat": "Average"
            }
        }
        widgets.append(duration_widget)
        
        # 2. Lambda 호출 횟수
        invocation_widget = {
            "type": "metric",
            "x": 12, "y": 0,
            "width": 12, "height": 6,
            "properties": {
                "metrics": [
                    ["AWS/Lambda", "Invocations", "FunctionName", func_name]
                    for func_name in self.lambda_functions
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": self.region,
                "title": "📊 Lambda 호출 횟수",
                "period": 300,
                "stat": "Sum"
            }
        }
        widgets.append(invocation_widget)
        
        # 3. Lambda 오류율
        error_widget = {
            "type": "metric",
            "x": 0, "y": 6,
            "width": 12, "height": 6,
            "properties": {
                "metrics": [
                    ["AWS/Lambda", "Errors", "FunctionName", func_name]
                    for func_name in self.lambda_functions
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": self.region,
                "title": "❌ Lambda 오류 발생",
                "period": 300,
                "stat": "Sum"
            }
        }
        widgets.append(error_widget)
        
        # 4. Lambda 성공률 (계산 메트릭)
        success_rate_widget = {
            "type": "metric",
            "x": 12, "y": 6,
            "width": 12, "height": 6,
            "properties": {
                "metrics": [
                    ["AWS/Lambda", "Invocations", "FunctionName", func_name, {"id": f"inv_{i}"}]
                    for i, func_name in enumerate(self.lambda_functions)
                ] + [
                    [".", "Errors", ".", func_name, {"id": f"err_{i}"}]
                    for i, func_name in enumerate(self.lambda_functions)
                ] + [
                    [{"expression": f"100 - (err_{i} / inv_{i} * 100)", "label": f"{func_name} 성공률 (%)", "id": f"rate_{i}"}]
                    for i, func_name in enumerate(self.lambda_functions)
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": self.region,
                "title": "✅ Lambda 성공률 (%)",
                "period": 300,
                "yAxis": {
                    "left": {
                        "min": 0,
                        "max": 100
                    }
                }
            }
        }
        widgets.append(success_rate_widget)
        
        return widgets
        
    def create_system_health_widgets(self) -> list:
        """시스템 상태 모니터링 위젯"""
        widgets = []
        
        # 5. Phase별 처리 현황 (로그 기반 메트릭)
        phase_status_widget = {
            "type": "log",
            "x": 0, "y": 12,
            "width": 24, "height": 6,
            "properties": {
                "query": f"SOURCE '/aws/lambda/makenaide-ticker-scanner-phase0'\n"
                         f"| SOURCE '/aws/lambda/makenaide-selective-data-collection-phase1'\n"
                         f"| SOURCE '/aws/lambda/makenaide-comprehensive-filtering-phase2'\n"
                         f"| SOURCE '/aws/lambda/makenaide-gpt-analysis-phase3'\n"
                         f"| SOURCE '/aws/lambda/makenaide-4h-analysis-phase4'\n"
                         f"| SOURCE '/aws/lambda/makenaide-condition-check-phase5'\n"
                         f"| SOURCE '/aws/lambda/makenaide-trade-execution-phase6'\n"
                         f"| fields @timestamp, @message\n"
                         f"| filter @message like /완료/ or @message like /성공/ or @message like /실패/\n"
                         f"| sort @timestamp desc\n"
                         f"| limit 100",
                "region": self.region,
                "title": "🔄 Phase별 실행 현황 (최근 100건)",
                "view": "table"
            }
        }
        widgets.append(phase_status_widget)
        
        return widgets
    
    def create_trading_metrics_widgets(self) -> list:
        """거래 성과 모니터링 위젯"""
        widgets = []
        
        # 6. 시장 상황 모니터링
        market_sentiment_widget = {
            "type": "log",
            "x": 0, "y": 18,
            "width": 12, "height": 6,
            "properties": {
                "query": f"SOURCE '/aws/lambda/makenaide-market-sentiment-check'\n"
                         f"| fields @timestamp, @message\n"
                         f"| filter @message like /시장 상황/\n"
                         f"| sort @timestamp desc\n"
                         f"| limit 20",
                "region": self.region,
                "title": "📊 시장 상황 분석 결과",
                "view": "table"
            }
        }
        widgets.append(market_sentiment_widget)
        
        # 7. 거래 실행 현황
        trading_status_widget = {
            "type": "log",
            "x": 12, "y": 18,
            "width": 12, "height": 6,
            "properties": {
                "query": f"SOURCE '/aws/lambda/makenaide-trade-execution-phase6'\n"
                         f"| fields @timestamp, @message\n"
                         f"| filter @message like /거래/ or @message like /매수/ or @message like /매도/\n"
                         f"| sort @timestamp desc\n"
                         f"| limit 20",
                "region": self.region,
                "title": "💼 거래 실행 현황",
                "view": "table"
            }
        }
        widgets.append(trading_status_widget)
        
        return widgets
    
    def create_cost_monitoring_widgets(self) -> list:
        """비용 모니터링 위젯"""
        widgets = []
        
        # 8. Lambda 비용 추정
        lambda_cost_widget = {
            "type": "metric",
            "x": 0, "y": 24,
            "width": 12, "height": 6,
            "properties": {
                "metrics": [
                    [{"expression": f"(inv_total * 0.0000002) + (duration_total / 1000 * 0.0000166667)", 
                      "label": "일일 예상 Lambda 비용 (USD)", "id": "cost_calc"}],
                    ["AWS/Lambda", "Invocations", {"stat": "Sum", "id": "inv_total", "visible": False}],
                    [".", "Duration", {"stat": "Sum", "id": "duration_total", "visible": False}]
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": self.region,
                "title": "💰 Lambda 비용 추정 (USD)",
                "period": 86400,  # 1일
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                }
            }
        }
        widgets.append(lambda_cost_widget)
        
        # 9. 리소스 사용률 요약
        resource_summary_widget = {
            "type": "metric",
            "x": 12, "y": 24,
            "width": 12, "height": 6,
            "properties": {
                "metrics": [
                    ["AWS/Lambda", "Duration", {"stat": "Average", "label": "평균 실행 시간 (ms)"}],
                    [".", "ConcurrentExecutions", {"stat": "Maximum", "label": "최대 동시 실행"}],
                    [".", "Throttles", {"stat": "Sum", "label": "스로틀링 발생"}]
                ],
                "view": "singleValue",
                "region": self.region,
                "title": "📈 리소스 사용률 요약",
                "period": 3600
            }
        }
        widgets.append(resource_summary_widget)
        
        return widgets
    
    def create_alert_status_widgets(self) -> list:
        """알림 상태 모니터링 위젯"""
        widgets = []
        
        # 10. SNS 발송 현황
        sns_widget = {
            "type": "metric", 
            "x": 0, "y": 30,
            "width": 12, "height": 6,
            "properties": {
                "metrics": [
                    ["AWS/SNS", "NumberOfMessagesPublished", "TopicName", "makenaide-alerts"],
                    [".", "NumberOfNotificationsFailed", ".", "."],
                    [".", "NumberOfNotificationsDelivered", ".", "."]
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": self.region,
                "title": "📲 SNS 알림 발송 현황",
                "period": 300
            }
        }
        widgets.append(sns_widget)
        
        # 11. EventBridge 이벤트 현황
        eventbridge_widget = {
            "type": "metric",
            "x": 12, "y": 30,
            "width": 12, "height": 6,
            "properties": {
                "metrics": [
                    ["AWS/Events", "InvocationsCount", "RuleName", "makenaide-market-sentiment-daily"],
                    [".", ".", ".", "makenaide-phase5-to-phase6"],
                    [".", "FailedInvocations", ".", "makenaide-market-sentiment-daily"],
                    [".", ".", ".", "makenaide-phase5-to-phase6"]
                ],
                "view": "timeSeries", 
                "stacked": False,
                "region": self.region,
                "title": "⚡ EventBridge 이벤트 현황",
                "period": 300
            }
        }
        widgets.append(eventbridge_widget)
        
        return widgets
        
    def create_dashboard(self) -> bool:
        """대시보드 생성"""
        try:
            logger.info("CloudWatch 대시보드 생성 중...")
            
            # 모든 위젯 수집
            all_widgets = []
            all_widgets.extend(self.create_lambda_performance_widgets())
            all_widgets.extend(self.create_system_health_widgets()) 
            all_widgets.extend(self.create_trading_metrics_widgets())
            all_widgets.extend(self.create_cost_monitoring_widgets())
            all_widgets.extend(self.create_alert_status_widgets())
            
            # 대시보드 본문 구성
            dashboard_body = {
                "widgets": all_widgets
            }
            
            # 대시보드 생성/업데이트
            self.cloudwatch_client.put_dashboard(
                DashboardName=self.dashboard_name,
                DashboardBody=json.dumps(dashboard_body)
            )
            
            logger.info(f"✅ 대시보드 생성 완료: {self.dashboard_name}")
            
            # 대시보드 URL 생성
            dashboard_url = f"https://{self.region}.console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards:name={self.dashboard_name}"
            
            logger.info(f"🌐 대시보드 URL: {dashboard_url}")
            
            return True, dashboard_url
            
        except Exception as e:
            logger.error(f"대시보드 생성 실패: {e}")
            return False, None
    
    def create_custom_metrics(self):
        """커스텀 메트릭 생성 (필요시)"""
        try:
            logger.info("커스텀 메트릭 설정 중...")
            
            # 예시: 거래 성공률 메트릭
            self.cloudwatch_client.put_metric_data(
                Namespace='Makenaide/Trading',
                MetricData=[
                    {
                        'MetricName': 'TradingSuccessRate',
                        'Value': 100.0,
                        'Unit': 'Percent',
                        'Timestamp': datetime.utcnow()
                    }
                ]
            )
            
            logger.info("✅ 커스텀 메트릭 설정 완료")
            
        except Exception as e:
            logger.error(f"커스텀 메트릭 설정 실패: {e}")
    
    def print_dashboard_summary(self):
        """대시보드 구성 요약"""
        logger.info("\n" + "="*80)
        logger.info("📊 Makenaide CloudWatch 대시보드 구성")
        logger.info("="*80)
        
        print(f"""
🎯 대시보드 이름: {self.dashboard_name}

📈 모니터링 영역:
┌─ Lambda 성능 모니터링
│  ├── ⚡ 실행 시간 추이
│  ├── 📊 호출 횟수
│  ├── ❌ 오류 발생률
│  └── ✅ 성공률 (%)

┌─ 시스템 상태
│  ├── 🔄 Phase별 실행 현황
│  └── 📋 실시간 로그 조회

┌─ 거래 성과
│  ├── 📊 시장 상황 분석
│  └── 💼 거래 실행 현황

┌─ 비용 관리
│  ├── 💰 Lambda 비용 추정
│  └── 📈 리소스 사용률

└─ 알림 시스템
   ├── 📲 SNS 발송 현황
   └── ⚡ EventBridge 이벤트

📋 모니터링 대상 Lambda:
""")
        for i, func in enumerate(self.lambda_functions, 1):
            phase = "Market Sentiment" if "sentiment" in func else f"Phase {i-1}"
            print(f"   {i:2d}. {func} ({phase})")
        
        logger.info("="*80)

def main():
    """메인 실행"""
    try:
        logger.info("🚀 CloudWatch 모니터링 대시보드 구축 시작")
        
        dashboard_builder = MakenaideDashboardBuilder()
        
        # 1. 대시보드 구성 요약
        dashboard_builder.print_dashboard_summary()
        
        # 2. 대시보드 생성
        success, dashboard_url = dashboard_builder.create_dashboard()
        
        if success:
            logger.info("✅ CloudWatch 대시보드 구축 완료!")
            logger.info(f"🌐 대시보드 접속: {dashboard_url}")
            
            # 3. 커스텀 메트릭 설정
            dashboard_builder.create_custom_metrics()
            
            print(f"""

🎉 대시보드 구축 완료!

📊 접속 방법:
   1. AWS 콘솔 → CloudWatch → 대시보드
   2. 대시보드명: {dashboard_builder.dashboard_name}
   3. 직접 링크: {dashboard_url}

⚠️  다음 단계:
   1. 대시보드에서 현재 상태 확인
   2. CloudWatch 알람 설정 (다음 작업)
   3. 정기적 모니터링 습관화

🔧 커스터마이징:
   - 위젯 크기 조정 가능
   - 시간 범위 변경 가능 
   - 추가 메트릭 위젯 추가 가능
            """)
            
            return True
        else:
            logger.error("❌ 대시보드 구축 실패")
            return False
            
    except Exception as e:
        logger.error(f"실행 실패: {e}")
        return False

if __name__ == '__main__':
    main()