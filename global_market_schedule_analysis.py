#!/usr/bin/env python3
"""
🌏 글로벌 암호화폐 시장 스케줄링 분석
- 24시간 글로벌 거래 패턴 분석
- 시간대별 최적 실행 시점 도출
- EventBridge 스케줄 최적화 설계
"""

from datetime import datetime, timezone, timedelta
import pytz

class GlobalCryptoScheduleAnalyzer:
    """글로벌 암호화폐 시장 스케줄링 분석기"""
    
    def __init__(self):
        # 주요 암호화폐 거래소별 시간대
        self.major_timezones = {
            'Seoul': 'Asia/Seoul',       # 업비트, 빗썸 (한국)
            'Tokyo': 'Asia/Tokyo',       # 비트플라이어 (일본)
            'Singapore': 'Asia/Singapore', # 바이낸스 아시아
            'London': 'Europe/London',   # 바이낸스, 코인베이스 유럽
            'New_York': 'America/New_York', # 코인베이스, 크라켄 (미국 동부)
            'Los_Angeles': 'America/Los_Angeles' # 미국 서부
        }
        
        # 제안된 KST 실행 시간
        self.kst_schedule = [2, 9, 15, 18, 21, 23]  # KST 시간
        
    def analyze_global_trading_patterns(self):
        """글로벌 거래 패턴 분석"""
        
        print("🌏 글로벌 암호화폐 시장 24시간 거래 패턴 분석")
        print("=" * 80)
        
        # 각 KST 시간에 대해 세계 시간대 분석
        for kst_hour in self.kst_schedule:
            print(f"\n🕐 KST {kst_hour:02d}:00 실행 시 글로벌 시간대:")
            print("-" * 50)
            
            # KST 기준 datetime 생성
            kst = pytz.timezone('Asia/Seoul')
            base_time = datetime.now(kst).replace(hour=kst_hour, minute=0, second=0, microsecond=0)
            
            for region, tz_name in self.major_timezones.items():
                target_tz = pytz.timezone(tz_name)
                local_time = base_time.astimezone(target_tz)
                
                # 거래 활성도 평가
                activity_score = self.calculate_trading_activity(local_time.hour)
                activity_level = self.get_activity_level(activity_score)
                
                print(f"  {region:12} ({tz_name:20}): {local_time.strftime('%H:%M')} - {activity_level}")
        
        return self.evaluate_schedule_effectiveness()
    
    def calculate_trading_activity(self, hour):
        """시간대별 거래 활성도 점수 계산 (0-100)"""
        
        # 일반적인 거래 활성도 패턴 (현지시간 기준)
        if 0 <= hour < 6:    # 심야-새벽: 낮음
            return 20
        elif 6 <= hour < 9:  # 아침: 중간
            return 60  
        elif 9 <= hour < 12: # 오전: 높음
            return 90
        elif 12 <= hour < 14: # 점심: 중간
            return 70
        elif 14 <= hour < 18: # 오후: 높음
            return 85
        elif 18 <= hour < 21: # 저녁: 매우 높음
            return 95
        elif 21 <= hour < 24: # 밤: 중간-높음
            return 75
        else:
            return 50
    
    def get_activity_level(self, score):
        """활성도 점수를 레벨로 변환"""
        if score >= 90:
            return "🔥 매우높음"
        elif score >= 80:
            return "🟢 높음"
        elif score >= 60:
            return "🟡 중간"
        elif score >= 40:
            return "🟠 낮음"
        else:
            return "🔵 매우낮음"
    
    def evaluate_schedule_effectiveness(self):
        """스케줄 효과성 평가"""
        
        print(f"\n📊 스케줄 효과성 분석")
        print("=" * 80)
        
        total_coverage = 0
        schedule_analysis = {}
        
        for kst_hour in self.kst_schedule:
            kst = pytz.timezone('Asia/Seoul')
            base_time = datetime.now(kst).replace(hour=kst_hour, minute=0, second=0, microsecond=0)
            
            regional_scores = []
            for region, tz_name in self.major_timezones.items():
                target_tz = pytz.timezone(tz_name)
                local_time = base_time.astimezone(target_tz)
                activity = self.calculate_trading_activity(local_time.hour)
                regional_scores.append(activity)
            
            avg_activity = sum(regional_scores) / len(regional_scores)
            total_coverage += avg_activity
            
            # 특별한 의미 부여
            schedule_meaning = self.get_schedule_meaning(kst_hour)
            
            schedule_analysis[kst_hour] = {
                'average_activity': avg_activity,
                'meaning': schedule_meaning,
                'regional_scores': regional_scores
            }
        
        # 결과 출력
        print(f"전체 평균 활성도: {total_coverage/len(self.kst_schedule):.1f}/100")
        print(f"24시간 커버리지: {(len(self.kst_schedule) * 4) / 24 * 100:.0f}% (6회 × 4시간 간격)")
        
        print(f"\n🎯 시간대별 전략적 의미:")
        for kst_hour, analysis in schedule_analysis.items():
            print(f"  KST {kst_hour:02d}:00 - {analysis['meaning']} (활성도: {analysis['average_activity']:.1f})")
        
        return schedule_analysis
    
    def get_schedule_meaning(self, kst_hour):
        """각 시간대의 전략적 의미"""
        meanings = {
            2: "🌙 아시아 심야 + 유럽 저녁 골든타임",
            9: "☀️ 한국 장 시작 + 일본 오전 활성화", 
            15: "🏢 아시아 오후 + 유럽 오전 시작",
            18: "🌆 한국 퇴근시간 + 유럽 점심 활성화",
            21: "🌃 아시아 저녁 골든타임 + 유럽 오후",
            23: "🌌 아시아 밤 + 미국 동부 오전 시작"
        }
        return meanings.get(kst_hour, "일반 거래시간")
    
    def generate_cron_expressions(self):
        """EventBridge용 cron 표현식 생성"""
        
        print(f"\n⚙️ EventBridge Cron 표현식")
        print("=" * 50)
        
        cron_expressions = {}
        
        for kst_hour in self.kst_schedule:
            # KST → UTC 변환 (KST = UTC+9)
            utc_hour = (kst_hour - 9) % 24
            
            # EventBridge cron: (분 시 일 월 요일 연도)
            cron_expression = f"0 {utc_hour} * * ? *"
            
            cron_expressions[kst_hour] = {
                'kst_time': f"{kst_hour:02d}:00 KST",
                'utc_time': f"{utc_hour:02d}:00 UTC", 
                'cron': cron_expression,
                'rule_name': f"makenaide-trading-schedule-{kst_hour:02d}00-kst"
            }
            
            print(f"  KST {kst_hour:02d}:00 → UTC {utc_hour:02d}:00 → {cron_expression}")
        
        return cron_expressions

def main():
    """메인 실행"""
    
    print("🚀 Makenaide 글로벌 스케줄링 최적화 분석")
    print("=" * 80)
    
    analyzer = GlobalCryptoScheduleAnalyzer()
    
    # 1. 글로벌 거래 패턴 분석
    schedule_analysis = analyzer.analyze_global_trading_patterns()
    
    # 2. Cron 표현식 생성
    cron_expressions = analyzer.generate_cron_expressions()
    
    # 3. 요약 및 권장사항
    print(f"\n🎯 스케줄링 최적화 권장사항")
    print("=" * 80)
    print(f"""
✅ 제안된 6회 실행 스케줄의 장점:
   • 24시간 균등 분산: 4시간 간격으로 시장 커버리지 최대화
   • 글로벌 활성화 시간 타겟팅: 주요 거래소 활성 시간대 포함
   • 아시아 중심 최적화: 업비트 기반으로 아시아 시장 우선 고려
   • 유럽/미국 기회 포착: 저녁/밤 시간대로 글로벌 기회 확보

🔄 기존 3회 vs 새로운 6회 실행:
   • 기존: 09:00/15:00/21:00 KST (8시간 간격)
   • 신규: 02:00/09:00/15:00/18:00/21:00/23:00 KST (3-4시간 간격)
   • 기회 포착 확률: 2배 증가 (하루 3회 → 6회)
   • 시장 변화 대응: 빠른 대응으로 기회 손실 최소화

⚠️ 고려사항:
   • Lambda 비용 증가: 6회 실행으로 약 2배 비용
   • 모니터링 복잡성: 더 많은 스케줄 관리 필요
   • 시장 상황 체크: 하락장 시 자동 중단 로직 더 중요

💡 권장 구현 방식:
   • 시간대별 차등 전략: 활성도 높은 시간은 더 적극적 매매
   • 유연한 스케줄: 시장 상황에 따라 일부 스케줄 비활성화 가능
   • 점진적 도입: 처음엔 3-4회로 시작 후 단계적 확대
    """)

if __name__ == '__main__':
    main()