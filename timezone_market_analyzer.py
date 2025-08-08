#!/usr/bin/env python3
"""
🌏 시간대별 시장 분석기
- 글로벌 거래소의 시간대별 거래 활성도 분석
- 지역별 거래 특성 및 패턴 파악
- 최적 거래 전략 제안
"""

from datetime import datetime, timezone, timedelta
import pytz
from typing import Dict, List, Tuple, Optional
import json
import logging

logger = logging.getLogger(__name__)

class TimezoneMarketAnalyzer:
    """시간대별 글로벌 시장 분석기"""
    
    def __init__(self):
        # 주요 거래소 시간대 및 가중치 (글로벌 거래량 기준)
        self.timezone_config = {
            'Asia/Seoul': {
                'weight': 0.20,  # 한국 거래소 (업비트, 빗썸)
                'peak_hours': [9, 14, 18, 21],  # 오전, 오후, 저녁, 밤
                'exchanges': ['Upbit', 'Bithumb'],
                'trading_style': 'momentum_driven',
                'volatility_preference': 'high'
            },
            'Asia/Tokyo': {
                'weight': 0.15,  # 일본 거래소
                'peak_hours': [9, 15, 20],
                'exchanges': ['bitFlyer', 'Liquid'],
                'trading_style': 'conservative',
                'volatility_preference': 'medium'
            },
            'Asia/Singapore': {
                'weight': 0.10,  # 싱가포르/홍콩 (바이낸스 아시아)
                'peak_hours': [10, 16, 21],
                'exchanges': ['Binance Asia'],
                'trading_style': 'institutional',
                'volatility_preference': 'medium'
            },
            'Europe/London': {
                'weight': 0.20,  # 유럽 거래소
                'peak_hours': [8, 14, 19],
                'exchanges': ['Binance Europe', 'Bitstamp'],
                'trading_style': 'value_based',
                'volatility_preference': 'medium'
            },
            'America/New_York': {
                'weight': 0.25,  # 미국 동부 거래소
                'peak_hours': [9, 15, 20],
                'exchanges': ['Coinbase', 'Gemini', 'Kraken'],
                'trading_style': 'institutional',
                'volatility_preference': 'low'
            },
            'America/Los_Angeles': {
                'weight': 0.10,  # 미국 서부
                'peak_hours': [9, 14, 18],
                'exchanges': ['Kraken US West'],
                'trading_style': 'tech_driven',
                'volatility_preference': 'medium'
            }
        }
        
        # 시간별 활성도 패턴 (0-24시 기준)
        self.hourly_activity_pattern = {
            0: 0.3, 1: 0.2, 2: 0.15, 3: 0.1, 4: 0.1, 5: 0.15,    # 심야-새벽
            6: 0.4, 7: 0.6, 8: 0.8, 9: 0.9, 10: 0.85, 11: 0.8,   # 아침-오전
            12: 0.7, 13: 0.65, 14: 0.8, 15: 0.85, 16: 0.8, 17: 0.75,  # 점심-오후
            18: 0.9, 19: 0.95, 20: 0.9, 21: 0.85, 22: 0.7, 23: 0.5    # 저녁-밤
        }
        
        # 거래 전략 조정 파라미터
        self.strategy_modifiers = {
            'position_size': {
                'very_high': 1.0,    # 100% (활성도 80% 이상)
                'high': 0.8,         # 80% (활성도 60-80%)
                'medium': 0.6,       # 60% (활성도 40-60%)
                'low': 0.4,          # 40% (활성도 20-40%)
                'very_low': 0.2      # 20% (활성도 20% 미만)
            },
            'stop_loss': {
                'high_volatility': 0.10,    # 10% (아시아 스타일)
                'medium_volatility': 0.08,  # 8% (유럽 스타일)
                'low_volatility': 0.05      # 5% (미국 스타일)
            },
            'take_profit': {
                'momentum_driven': [0.20, 0.40, 0.80],    # 20%, 40%, 80%
                'value_based': [0.15, 0.30, 0.50],        # 15%, 30%, 50%
                'institutional': [0.10, 0.20, 0.35],      # 10%, 20%, 35%
                'conservative': [0.08, 0.15, 0.25]        # 8%, 15%, 25%
            }
        }

    def convert_kst_to_timezone(self, kst_hour: int, target_tz: str) -> int:
        """KST 시간을 다른 시간대로 변환"""
        kst = pytz.timezone('Asia/Seoul')
        target = pytz.timezone(target_tz)
        
        # 현재 날짜 기준으로 변환 (DST 고려)
        now = datetime.now(kst)
        kst_time = now.replace(hour=kst_hour, minute=0, second=0, microsecond=0)
        target_time = kst_time.astimezone(target)
        
        return target_time.hour

    def calculate_timezone_activity(self, local_hour: int, peak_hours: List[int]) -> float:
        """특정 시간대의 활성도 계산 (0-1)"""
        base_activity = self.hourly_activity_pattern.get(local_hour, 0.5)
        
        # 피크 시간대 보너스
        if local_hour in peak_hours:
            base_activity = min(1.0, base_activity * 1.2)
        
        # 인접 피크 시간 보너스 (피크 시간 전후 1시간)
        for peak in peak_hours:
            if abs(local_hour - peak) == 1:
                base_activity = min(1.0, base_activity * 1.1)
                break
        
        return base_activity

    def calculate_global_activity_score(self, kst_hour: int) -> Dict:
        """KST 시간 기준 글로벌 거래 활성도 종합 점수"""
        regional_activities = {}
        weighted_sum = 0
        total_weight = 0
        
        for tz_name, config in self.timezone_config.items():
            # KST를 해당 시간대로 변환
            local_hour = self.convert_kst_to_timezone(kst_hour, tz_name)
            
            # 해당 시간대의 활성도 계산
            activity = self.calculate_timezone_activity(local_hour, config['peak_hours'])
            
            # 지역별 가중치 적용
            weighted_activity = activity * config['weight']
            weighted_sum += weighted_activity
            total_weight += config['weight']
            
            regional_activities[tz_name] = {
                'local_hour': local_hour,
                'activity': activity,
                'weighted_activity': weighted_activity,
                'is_peak': local_hour in config['peak_hours'],
                'exchanges': config['exchanges']
            }
        
        # 정규화된 글로벌 활성도 점수 (0-100)
        global_score = (weighted_sum / total_weight) * 100 if total_weight > 0 else 0
        
        return {
            'global_score': round(global_score, 1),
            'regional_activities': regional_activities,
            'timestamp': datetime.now(pytz.UTC).isoformat()
        }

    def identify_dominant_regions(self, kst_hour: int) -> List[Tuple[str, float]]:
        """해당 시간의 주도적인 거래 지역 식별"""
        activities = []
        
        for tz_name, config in self.timezone_config.items():
            local_hour = self.convert_kst_to_timezone(kst_hour, tz_name)
            activity = self.calculate_timezone_activity(local_hour, config['peak_hours'])
            weighted_activity = activity * config['weight']
            
            activities.append((tz_name, weighted_activity))
        
        # 활성도 기준 내림차순 정렬
        activities.sort(key=lambda x: x[1], reverse=True)
        
        # 상위 3개 지역 반환
        return activities[:3]

    def get_trading_style_for_hour(self, kst_hour: int) -> Dict:
        """시간대별 최적 거래 스타일 결정"""
        dominant_regions = self.identify_dominant_regions(kst_hour)
        
        if not dominant_regions:
            return {
                'primary_style': 'conservative',
                'volatility_preference': 'low',
                'confidence': 0.5
            }
        
        # 가장 활성도가 높은 지역의 스타일
        primary_region = dominant_regions[0][0]
        primary_config = self.timezone_config[primary_region]
        
        # 두 번째 지역과의 활성도 차이로 신뢰도 계산
        confidence = 1.0
        if len(dominant_regions) > 1:
            activity_diff = dominant_regions[0][1] - dominant_regions[1][1]
            confidence = min(1.0, 0.5 + activity_diff * 2)
        
        return {
            'primary_style': primary_config['trading_style'],
            'volatility_preference': primary_config['volatility_preference'],
            'dominant_region': primary_region,
            'confidence': round(confidence, 2)
        }

    def calculate_position_size_modifier(self, global_score: float) -> float:
        """글로벌 활성도에 따른 포지션 크기 조정"""
        if global_score >= 80:
            return self.strategy_modifiers['position_size']['very_high']
        elif global_score >= 60:
            return self.strategy_modifiers['position_size']['high']
        elif global_score >= 40:
            return self.strategy_modifiers['position_size']['medium']
        elif global_score >= 20:
            return self.strategy_modifiers['position_size']['low']
        else:
            return self.strategy_modifiers['position_size']['very_low']

    def get_risk_parameters(self, trading_style: Dict) -> Dict:
        """거래 스타일에 따른 리스크 파라미터"""
        volatility_pref = trading_style['volatility_preference']
        style = trading_style['primary_style']
        
        # 손절 설정
        stop_loss = self.strategy_modifiers['stop_loss'].get(
            f"{volatility_pref}_volatility",
            self.strategy_modifiers['stop_loss']['medium_volatility']
        )
        
        # 익절 레벨 설정
        take_profit_levels = self.strategy_modifiers['take_profit'].get(
            style,
            self.strategy_modifiers['take_profit']['conservative']
        )
        
        return {
            'stop_loss_pct': stop_loss,
            'take_profit_levels': take_profit_levels,
            'trailing_stop': volatility_pref == 'high',  # 고변동성에서만 추적 손절
            'max_holding_hours': 24 if style == 'momentum_driven' else 72
        }

    def generate_comprehensive_analysis(self, kst_hour: int) -> Dict:
        """종합적인 시간대별 시장 분석"""
        # 글로벌 활성도 점수
        activity_analysis = self.calculate_global_activity_score(kst_hour)
        global_score = activity_analysis['global_score']
        
        # 주도 지역 식별
        dominant_regions = self.identify_dominant_regions(kst_hour)
        
        # 거래 스타일 결정
        trading_style = self.get_trading_style_for_hour(kst_hour)
        
        # 포지션 크기 조정
        position_modifier = self.calculate_position_size_modifier(global_score)
        
        # 리스크 파라미터
        risk_params = self.get_risk_parameters(trading_style)
        
        # 시장 상태 판단
        market_condition = self._assess_market_condition(global_score)
        
        return {
            'kst_hour': kst_hour,
            'global_activity_score': global_score,
            'market_condition': market_condition,
            'dominant_regions': [
                {
                    'timezone': region,
                    'activity': round(activity * 100, 1),
                    'local_time': self.convert_kst_to_timezone(kst_hour, region)
                }
                for region, activity in dominant_regions
            ],
            'trading_style': trading_style,
            'strategy_adjustments': {
                'position_size_modifier': position_modifier,
                'risk_parameters': risk_params
            },
            'regional_breakdown': activity_analysis['regional_activities'],
            'recommendations': self._generate_recommendations(
                global_score, trading_style, market_condition
            ),
            'analysis_timestamp': datetime.now(pytz.UTC).isoformat()
        }

    def _assess_market_condition(self, global_score: float) -> str:
        """시장 상태 평가"""
        if global_score >= 80:
            return "VERY_ACTIVE"
        elif global_score >= 60:
            return "ACTIVE"
        elif global_score >= 40:
            return "MODERATE"
        elif global_score >= 20:
            return "QUIET"
        else:
            return "VERY_QUIET"

    def _generate_recommendations(self, global_score: float, 
                                 trading_style: Dict, 
                                 market_condition: str) -> List[str]:
        """시간대별 거래 권장사항 생성"""
        recommendations = []
        
        # 활성도 기반 권장사항
        if market_condition in ["VERY_ACTIVE", "ACTIVE"]:
            recommendations.append("✅ 적극적 거래 권장 - 높은 유동성과 명확한 추세")
            recommendations.append("📊 큰 포지션 가능 - 슬리피지 위험 낮음")
        elif market_condition == "MODERATE":
            recommendations.append("⚠️ 선별적 거래 권장 - 중간 수준 유동성")
            recommendations.append("🎯 명확한 시그널만 진입")
        else:
            recommendations.append("🚫 보수적 접근 필요 - 낮은 유동성")
            recommendations.append("💡 포지션 축소 또는 관망 권장")
        
        # 거래 스타일 기반 권장사항
        style = trading_style['primary_style']
        if style == 'momentum_driven':
            recommendations.append("🚀 모멘텀 전략 활용 - 추세 추종 매매")
        elif style == 'value_based':
            recommendations.append("💎 가치 투자 접근 - 과매도 구간 매수")
        elif style == 'institutional':
            recommendations.append("🏛️ 기관 스타일 - 안정적 대형주 중심")
        
        # 지역별 특이사항
        dominant_region = trading_style.get('dominant_region', '')
        if 'Asia' in dominant_region:
            recommendations.append("🌏 아시아 주도 - 알트코인 변동성 주의")
        elif 'Europe' in dominant_region:
            recommendations.append("🌍 유럽 주도 - BTC/ETH 중심 거래")
        elif 'America' in dominant_region:
            recommendations.append("🌎 미국 주도 - 규제 뉴스 모니터링")
        
        return recommendations


def main():
    """테스트 실행"""
    analyzer = TimezoneMarketAnalyzer()
    
    print("🌏 시간대별 시장 분석 테스트")
    print("=" * 80)
    
    # 6개 실행 시간에 대한 분석
    test_hours = [2, 9, 15, 18, 21, 23]
    
    for kst_hour in test_hours:
        analysis = analyzer.generate_comprehensive_analysis(kst_hour)
        
        print(f"\n📊 KST {kst_hour:02d}:00 분석")
        print(f"글로벌 활성도: {analysis['global_activity_score']}% ({analysis['market_condition']})")
        print(f"주도 지역: {analysis['trading_style']['dominant_region']}")
        print(f"거래 스타일: {analysis['trading_style']['primary_style']}")
        print(f"포지션 크기: {analysis['strategy_adjustments']['position_size_modifier'] * 100}%")
        print(f"손절선: {analysis['strategy_adjustments']['risk_parameters']['stop_loss_pct'] * 100}%")
        
        print("\n권장사항:")
        for rec in analysis['recommendations']:
            print(f"  {rec}")
        
        print("-" * 80)

if __name__ == '__main__':
    main()