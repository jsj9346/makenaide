#!/usr/bin/env python3
"""
📊 Makenaide SNS 리포트 양식 디자인
투자자를 위한 종합적이고 전문적인 리포트 템플릿
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MakenaideSNSReportDesigner:
    """Makenaide SNS 리포트 양식 디자이너"""
    
    def __init__(self):
        self.report_templates = {}
        self.sample_data = self._generate_sample_data()
        logger.info("📊 Makenaide SNS 리포트 디자이너 초기화 완료")
    
    def _generate_sample_data(self) -> Dict:
        """샘플 데이터 생성"""
        return {
            'discovered_assets': [
                {
                    'ticker': 'BTC',
                    'korean_name': '비트코인',
                    'current_price': 67800000,
                    'signal_strength': 'STRONG',
                    'gpt_analysis': '볼링거 밴드 하한선 접촉 후 반등 신호. RSI 30 이하 과매도 구간에서 상승 전환 패턴 확인.',
                    'technical_reasons': [
                        'MA200 지지선 확인 (65,500,000원)',
                        '거래량 급증 (평소 대비 180%)',
                        'MACD 골든크로스 형성',
                        '일봉 해머형 반전 캔들 출현'
                    ],
                    'entry_price': 66500000,
                    'target_price': 73000000,
                    'stop_loss': 63200000,
                    'position_size': '5%',
                    'risk_reward_ratio': '1:2.1',
                    'chart_url': 'https://s3.amazonaws.com/makenaide-charts/BTC_20250808.png'
                },
                {
                    'ticker': 'ETH',
                    'korean_name': '이더리움',
                    'current_price': 3250000,
                    'signal_strength': 'MEDIUM',
                    'gpt_analysis': '하락 추세 중 지지선 테스트. 단기 반등 가능성 있으나 신중한 접근 필요.',
                    'technical_reasons': [
                        '피보나치 61.8% 되돌림 지점 도달',
                        'RSI 다이버전스 발생',
                        '거래량 프로파일 지지 구간'
                    ],
                    'entry_price': 3180000,
                    'target_price': 3480000,
                    'stop_loss': 3050000,
                    'position_size': '3%',
                    'risk_reward_ratio': '1:2.3'
                }
            ],
            'sold_assets': [
                {
                    'ticker': 'ADA',
                    'korean_name': '에이다',
                    'sell_price': 1250,
                    'buy_price': 1180,
                    'sell_reason': '목표가 도달 (익절)',
                    'rule_triggered': '20% 수익 실현 규칙',
                    'holding_period': '3일',
                    'profit_loss': '+5.93%',
                    'profit_amount': 47500
                }
            ],
            'performance': {
                'daily': {
                    'win_rate': 75.0,
                    'total_return': 2.3,
                    'gross_profit': 145000,
                    'fees_paid': 12500,
                    'net_profit': 132500,
                    'trades_executed': 4,
                    'winning_trades': 3
                },
                'weekly': {
                    'win_rate': 68.2,
                    'total_return': 8.7,
                    'gross_profit': 520000,
                    'fees_paid': 48000,
                    'net_profit': 472000,
                    'trades_executed': 22,
                    'winning_trades': 15
                },
                'monthly': {
                    'win_rate': 71.4,
                    'total_return': 15.8,
                    'gross_profit': 1580000,
                    'fees_paid': 145000,
                    'net_profit': 1435000,
                    'trades_executed': 84,
                    'winning_trades': 60
                }
            },
            'market_analysis': {
                'overall_sentiment': 'NEUTRAL',
                'fear_greed_index': 42,
                'btc_dominance': 54.2,
                'market_cap_change': -1.8,
                'key_events': [
                    'Fed 금리 동결 발표 예정 (오늘 밤 3시)',
                    'BTC ETF 순유입 지속 ($120M)',
                    '알트코인 시즌 초기 신호 감지'
                ]
            },
            'risk_management': {
                'portfolio_risk_score': 6.2,
                'max_drawdown': 4.2,
                'var_95': 3.8,
                'position_concentration': 'BALANCED',
                'leverage_usage': 0.0,
                'cash_ratio': 25.0
            },
            'system_health': {
                'pipeline_success_rate': 98.5,
                'api_response_time': 145,
                'last_error': None,
                'uptime': '99.8%',
                'cost_efficiency': 93.2
            }
        }
    
    def design_daily_report_template(self) -> str:
        """일일 리포트 템플릿 디자인"""
        template = '''
🤖 Makenaide 일일 투자 리포트
📅 {date} ({weekday})

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 TODAY'S DISCOVERIES | 오늘의 발굴 종목

{discovered_assets}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 EXECUTED TRADES | 체결된 거래

{executed_trades}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 DAILY PERFORMANCE | 일일 성과

📊 수익률: {daily_return}% | 순수익: {net_profit:,}원
🏆 승률: {win_rate}% ({winning_trades}/{total_trades})
💸 거래비용: {fees:,}원
📋 실행된 거래: {total_trades}건

{performance_chart}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🌍 MARKET OVERVIEW | 시장 현황

🎭 시장 심리: {market_sentiment} | 공포탐욕지수: {fear_greed}
₿ BTC 도미넌스: {btc_dominance}% | 시총변화: {market_cap_change}%

📢 주요 이슈:
{key_events}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🛡️ RISK MANAGEMENT | 리스크 관리

⚖️ 포트폴리오 리스크: {risk_score}/10
📉 최대 손실폭: {max_drawdown}%
💼 현금 비중: {cash_ratio}%
🎚️ 포지션 분산도: {position_concentration}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔧 SYSTEM STATUS | 시스템 상태

✅ 파이프라인 성공률: {pipeline_success}%
⚡ API 응답시간: {api_time}ms
💰 비용 효율성: {cost_efficiency}% 절약
⏱️ 가동률: {uptime}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔮 TOMORROW'S PLAN | 내일 계획

{tomorrow_plan}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ IMPORTANT NOTICES | 중요 공지

{important_notices}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚀 "지지말아요" - Makenaide가 함께합니다
📊 실시간 모니터링: CloudWatch Dashboard
💡 문의사항: 이 이메일에 답장하세요

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        '''
        
        return template
    
    def design_weekly_report_template(self) -> str:
        """주간 리포트 템플릿 디자인"""
        template = '''
📊 Makenaide 주간 투자 리포트
📅 {week_start} ~ {week_end}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 WEEKLY SUMMARY | 주간 요약

🏆 총 수익률: {weekly_return}%
💰 순수익: {net_profit:,}원
📊 승률: {win_rate}% ({winning_trades}/{total_trades})
📋 총 거래: {total_trades}건

{weekly_performance_chart}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 TOP PERFORMERS | 이번 주 베스트

{top_performers}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📉 PORTFOLIO CHANGES | 포트폴리오 변화

{portfolio_changes}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔍 STRATEGY ANALYSIS | 전략 분석

{strategy_analysis}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 MARKET INSIGHTS | 시장 인사이트

{market_insights}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 NEXT WEEK OUTLOOK | 다음 주 전망

{next_week_outlook}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚀 Makenaide - 한 주간 수고하셨습니다!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        '''
        
        return template
    
    def generate_discovered_assets_section(self, assets: List[Dict]) -> str:
        """발굴 종목 섹션 생성"""
        if not assets:
            return "🔍 오늘은 새로운 매수 기회를 발견하지 못했습니다.\n   시장 상황을 지속 모니터링 중입니다."
        
        section = ""
        for i, asset in enumerate(assets, 1):
            section += f"""
🎯 {i}. {asset['ticker']} ({asset['korean_name']})
💰 현재가: {asset['current_price']:,}원 | 신호강도: {asset['signal_strength']}

🤖 GPT 분석:
   {asset['gpt_analysis']}

📊 기술적 근거:
{chr(10).join(f"   • {reason}" for reason in asset['technical_reasons'])}

💡 거래 계획:
   • 진입가: {asset['entry_price']:,}원
   • 목표가: {asset['target_price']:,}원  
   • 손절가: {asset['stop_loss']:,}원
   • 포지션: {asset['position_size']} | 위험보상비: {asset['risk_reward_ratio']}

📈 차트 분석: {asset.get('chart_url', '차트 링크 준비 중')}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        
        return section.strip()
    
    def generate_executed_trades_section(self, trades: List[Dict]) -> str:
        """체결 거래 섹션 생성"""
        if not trades:
            return "📋 오늘 체결된 거래가 없습니다."
        
        section = ""
        for i, trade in enumerate(trades, 1):
            profit_emoji = "📈" if trade['profit_loss'].startswith('+') else "📉"
            
            section += f"""
{profit_emoji} {i}. {trade['ticker']} ({trade['korean_name']}) - {trade['sell_reason']}

💰 매수가: {trade['buy_price']:,}원 → 매도가: {trade['sell_price']:,}원
📊 수익률: {trade['profit_loss']} | 수익금: {trade['profit_amount']:,}원
⏰ 보유기간: {trade['holding_period']}
🎯 적용규칙: {trade['rule_triggered']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        
        return section.strip()
    
    def generate_performance_chart_ascii(self, performance: Dict) -> str:
        """ASCII 성과 차트 생성"""
        return_rate = performance['total_return']
        win_rate = performance['win_rate']
        
        # 간단한 ASCII 차트
        return_bars = "█" * max(1, int(abs(return_rate) / 2))
        win_bars = "█" * max(1, int(win_rate / 10))
        
        chart = f"""
📊 성과 차트:
   수익률 {return_rate:+.1f}%  |{return_bars}|
   승  률 {win_rate:.1f}%    |{win_bars}|
        """
        
        return chart
    
    def generate_sample_daily_report(self) -> str:
        """샘플 일일 리포트 생성"""
        data = self.sample_data
        template = self.design_daily_report_template()
        
        # 발굴 종목 섹션
        discovered_section = self.generate_discovered_assets_section(
            data['discovered_assets']
        )
        
        # 체결 거래 섹션  
        executed_section = self.generate_executed_trades_section(
            data['sold_assets']
        )
        
        # 성과 차트
        performance_chart = self.generate_performance_chart_ascii(
            data['performance']['daily']
        )
        
        # 주요 이벤트
        key_events = "\n".join(
            f"   • {event}" for event in data['market_analysis']['key_events']
        )
        
        # 내일 계획
        tomorrow_plan = """
🎯 모니터링 대상: BTC 67,000,000원 지지선 테스트
📊 경제지표: 미국 CPI 발표 (한국시간 밤 10:30)
⚡ 시스템: Phase 2-6 정상 스케줄 실행
💰 현금 비중: 안전 마진 25% 유지
        """
        
        # 중요 공지
        important_notices = """
⚠️ Fed 금리 결정 발표로 인한 변동성 예상
💡 손절선 준수: 어떤 상황에서도 -8% 손절 규칙 적용
📱 긴급상황 시 SMS 알림 활성화됨
        """
        
        # 템플릿 포맷팅
        formatted_report = template.format(
            date=datetime.now().strftime('%Y-%m-%d'),
            weekday=datetime.now().strftime('%A'),
            discovered_assets=discovered_section,
            executed_trades=executed_section,
            daily_return=data['performance']['daily']['total_return'],
            net_profit=data['performance']['daily']['net_profit'],
            win_rate=data['performance']['daily']['win_rate'],
            winning_trades=data['performance']['daily']['winning_trades'],
            total_trades=data['performance']['daily']['trades_executed'],
            fees=data['performance']['daily']['fees_paid'],
            performance_chart=performance_chart,
            market_sentiment=data['market_analysis']['overall_sentiment'],
            fear_greed=data['market_analysis']['fear_greed_index'],
            btc_dominance=data['market_analysis']['btc_dominance'],
            market_cap_change=data['market_analysis']['market_cap_change'],
            key_events=key_events,
            risk_score=data['risk_management']['portfolio_risk_score'],
            max_drawdown=data['risk_management']['max_drawdown'],
            cash_ratio=data['risk_management']['cash_ratio'],
            position_concentration=data['risk_management']['position_concentration'],
            pipeline_success=data['system_health']['pipeline_success_rate'],
            api_time=data['system_health']['api_response_time'],
            cost_efficiency=data['system_health']['cost_efficiency'],
            uptime=data['system_health']['uptime'],
            tomorrow_plan=tomorrow_plan.strip(),
            important_notices=important_notices.strip()
        )
        
        return formatted_report
    
    def save_report_templates(self):
        """리포트 템플릿들을 파일로 저장"""
        templates = {
            'daily_template': self.design_daily_report_template(),
            'weekly_template': self.design_weekly_report_template(),
            'sample_daily_report': self.generate_sample_daily_report()
        }
        
        for template_name, template_content in templates.items():
            filename = f'/Users/13ruce/makenaide/sns_report_{template_name}.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(template_content)
            logger.info(f"✅ {template_name} 저장: {filename}")
    
    def get_recommended_additional_content(self) -> List[str]:
        """추가 권장 리포트 내용"""
        return [
            "🌡️ 시장 온도계 (Fear & Greed Index, VIX 등)",
            "📊 포트폴리오 히트맵 (섹터별/자산별 성과)",
            "⏰ 다음 주요 이벤트 일정 (경제지표, 기업공시 등)",
            "🎯 월간/분기 목표 대비 진척도",
            "📈 백테스팅 결과 vs 실제 성과 비교",
            "🔄 자동 리밸런싱 실행 내역",
            "💡 AI 추천 포트폴리오 조정 제안",
            "🌍 글로벌 시장 상관관계 분석",
            "📱 모바일 알림 설정 상태",
            "🔒 보안 및 API 키 상태 점검",
            "💰 세금 최적화 거래 제안",
            "📊 거래 패턴 분석 (시간대별, 요일별)",
            "🎲 몬테카르로 시뮬레이션 결과",
            "📰 뉴스 심리 분석 (뉴스 임팩트 스코어)",
            "🏆 동일 전략 사용자 대비 성과 순위"
        ]

def main():
    """SNS 리포트 디자인 메인 함수"""
    print("📊 Makenaide SNS 리포트 양식 디자인")
    print("=" * 60)
    
    designer = MakenaideSNSReportDesigner()
    
    print("\n🎯 리포트 템플릿 디자인 중...")
    
    # 샘플 일일 리포트 생성 및 출력
    print("\n📋 샘플 일일 리포트 미리보기:")
    print("-" * 60)
    sample_report = designer.generate_sample_daily_report()
    print(sample_report)
    
    # 리포트 템플릿 저장
    print(f"\n💾 리포트 템플릿 저장 중...")
    designer.save_report_templates()
    
    # 추가 권장 내용 출력
    print(f"\n💡 추가 권장 리포트 내용:")
    additional_content = designer.get_recommended_additional_content()
    for i, content in enumerate(additional_content, 1):
        print(f"   {i:2d}. {content}")
    
    print(f"\n🎉 SNS 리포트 양식 디자인 완료!")
    
    print(f"\n📁 생성된 파일들:")
    print(f"   • sns_report_daily_template.txt - 일일 리포트 템플릿")
    print(f"   • sns_report_weekly_template.txt - 주간 리포트 템플릿")
    print(f"   • sns_report_sample_daily_report.txt - 샘플 리포트")
    
    print(f"\n📊 리포트 특징:")
    print(f"   • 이모지를 활용한 가독성 향상")
    print(f"   • 구조화된 정보 전달")
    print(f"   • ASCII 차트로 시각화")
    print(f"   • 투자자 관점의 핵심 정보 포함")
    print(f"   • SMS/이메일 양쪽에서 최적 표시")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)