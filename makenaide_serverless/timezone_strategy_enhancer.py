#!/usr/bin/env python3
"""
🌏 Timezone Strategy Enhancer
- 시간대 분석 결과를 기반으로 거래 전략 파라미터를 동적으로 조정
- Phase 0-6 Lambda 함수에서 공통으로 사용하는 전략 조정 모듈
- 글로벌 거래 활성도에 따른 포지션 크기, 손절/익절 레벨 최적화
"""

import boto3
import json
import logging
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta
import math

logger = logging.getLogger(__name__)

class TimezoneStrategyEnhancer:
    """시간대 분석 기반 거래 전략 조정 클래스"""
    
    def __init__(self, s3_bucket: str = 'makenaide-bucket-901361833359'):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = s3_bucket
        
        # 기본 전략 파라미터 (보수적 설정)
        self.base_strategy = {
            'position_size_base': 0.5,      # 기본 포지션 크기 (50%)
            'stop_loss_base': 0.08,         # 기본 손절 비율 (8%)
            'take_profit_levels': [0.15, 0.30, 0.50],  # 기본 익절 레벨
            'max_positions': 3,             # 최대 동시 포지션
            'risk_per_trade': 0.02,         # 거래당 최대 위험 (2%)
            'volatility_threshold': 0.05    # 변동성 임계값 (5%)
        }
        
        # 시간대별 전략 조정 매트릭스
        self.timezone_adjustments = {
            'Asia': {
                'volatility_multiplier': 1.2,    # 높은 변동성
                'position_multiplier': 0.8,      # 보수적 포지션
                'stop_loss_tightness': 0.9,      # 타이트한 손절
                'momentum_sensitivity': 1.3      # 높은 모멘텀 민감도
            },
            'Europe': {
                'volatility_multiplier': 1.0,
                'position_multiplier': 1.0,
                'stop_loss_tightness': 1.0,
                'momentum_sensitivity': 1.0
            },
            'America': {
                'volatility_multiplier': 0.9,
                'position_multiplier': 1.1,      # 공격적 포지션
                'stop_loss_tightness': 1.1,      # 느슨한 손절
                'momentum_sensitivity': 0.8      # 낮은 모멘텀 민감도
            }
        }
        
        # 글로벌 활성도별 전략 조정
        self.activity_strategy_matrix = {
            'very_high': {  # 80% 이상
                'position_multiplier': 1.3,
                'profit_target_multiplier': 1.4,
                'trailing_stop_activation': 0.12,
                'max_holding_hours': 18
            },
            'high': {  # 60-80%
                'position_multiplier': 1.1,
                'profit_target_multiplier': 1.2,
                'trailing_stop_activation': 0.15,
                'max_holding_hours': 24
            },
            'moderate': {  # 40-60%
                'position_multiplier': 0.8,
                'profit_target_multiplier': 1.0,
                'trailing_stop_activation': 0.18,
                'max_holding_hours': 36
            },
            'low': {  # 40% 미만
                'position_multiplier': 0.5,
                'profit_target_multiplier': 0.8,
                'trailing_stop_activation': 0.20,
                'max_holding_hours': 48
            }
        }
    
    def load_timezone_analysis_from_s3(self) -> Optional[Dict]:
        """S3에서 최신 시간대 분석 결과 로드"""
        try:
            # 현재 시장 상황 파일 읽기
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='market_sentiment/current_sentiment.json'
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data.get('timezone_analysis')
            
        except Exception as e:
            logger.warning(f"시간대 분석 데이터 로드 실패: {e}")
            return None
    
    def classify_global_activity(self, activity_score: float) -> str:
        """글로벌 활성도 점수를 카테고리로 분류"""
        if activity_score >= 80:
            return 'very_high'
        elif activity_score >= 60:
            return 'high'
        elif activity_score >= 40:
            return 'moderate'
        else:
            return 'low'
    
    def get_dominant_region(self, timezone_analysis: Dict) -> str:
        """주도 지역 파악"""
        dominant_regions = timezone_analysis.get('dominant_regions', [])
        if not dominant_regions:
            return 'Asia'  # 기본값
        
        primary_timezone = dominant_regions[0]['timezone']
        
        # 지역 매핑
        if any(region in primary_timezone for region in ['Asia/Seoul', 'Asia/Tokyo', 'Asia/Singapore']):
            return 'Asia'
        elif 'Europe' in primary_timezone:
            return 'Europe'
        elif 'America' in primary_timezone:
            return 'America'
        else:
            return 'Asia'  # 기본값
    
    def calculate_dynamic_position_size(
        self, 
        base_amount: float, 
        timezone_analysis: Dict,
        market_volatility: float = 0.05
    ) -> float:
        """동적 포지션 크기 계산"""
        try:
            # 기본 포지션 크기
            position_size = base_amount * self.base_strategy['position_size_base']
            
            # 글로벌 활성도 조정
            activity_score = timezone_analysis.get('global_activity_score', 50)
            activity_category = self.classify_global_activity(activity_score)
            activity_multiplier = self.activity_strategy_matrix[activity_category]['position_multiplier']
            
            # 지역별 조정
            dominant_region = self.get_dominant_region(timezone_analysis)
            region_multiplier = self.timezone_adjustments[dominant_region]['position_multiplier']
            
            # 변동성 조정 (변동성이 높으면 포지션 축소)
            volatility_adjustment = max(0.3, 1.0 - (market_volatility - 0.03) * 2)
            
            # 최종 포지션 크기 계산
            adjusted_position = position_size * activity_multiplier * region_multiplier * volatility_adjustment
            
            # 안전 범위 제한 (최대 원래 금액의 80%, 최소 20%)
            max_position = base_amount * 0.8
            min_position = base_amount * 0.2
            
            adjusted_position = max(min_position, min(max_position, adjusted_position))
            
            logger.info(f"포지션 크기 조정: {base_amount:,.0f} → {adjusted_position:,.0f} KRW")
            logger.info(f"조정 요인: 활성도({activity_multiplier:.2f}) × 지역({region_multiplier:.2f}) × 변동성({volatility_adjustment:.2f})")
            
            return adjusted_position
            
        except Exception as e:
            logger.error(f"포지션 크기 계산 실패: {e}")
            return base_amount * self.base_strategy['position_size_base']
    
    def calculate_dynamic_stop_loss(
        self, 
        entry_price: float, 
        timezone_analysis: Dict,
        market_volatility: float = 0.05
    ) -> Dict:
        """동적 손절 레벨 계산"""
        try:
            # 기본 손절 비율
            base_stop_loss_pct = self.base_strategy['stop_loss_base']
            
            # 글로벌 활성도에 따른 조정
            activity_score = timezone_analysis.get('global_activity_score', 50)
            activity_category = self.classify_global_activity(activity_score)
            
            # 지역별 조정
            dominant_region = self.get_dominant_region(timezone_analysis)
            tightness_multiplier = self.timezone_adjustments[dominant_region]['stop_loss_tightness']
            
            # 변동성 조정 (변동성이 높으면 손절선 완화)
            volatility_multiplier = max(0.7, min(1.5, 1.0 + (market_volatility - 0.05) * 2))
            
            # 최종 손절 비율 계산
            adjusted_stop_loss_pct = base_stop_loss_pct * tightness_multiplier * volatility_multiplier
            
            # 안전 범위 제한 (5% ~ 15%)
            adjusted_stop_loss_pct = max(0.05, min(0.15, adjusted_stop_loss_pct))
            
            # 손절 가격 계산
            stop_loss_price = entry_price * (1 - adjusted_stop_loss_pct)
            
            # 트레일링 스탑 설정
            trailing_activation_pct = self.activity_strategy_matrix[activity_category]['trailing_stop_activation']
            trailing_activation_price = entry_price * (1 + trailing_activation_pct)
            
            return {
                'stop_loss_price': stop_loss_price,
                'stop_loss_percentage': adjusted_stop_loss_pct * 100,
                'trailing_stop_activation': trailing_activation_price,
                'trailing_stop_percentage': adjusted_stop_loss_pct * 0.7,  # 트레일링은 더 타이트
                'reason': f"활성도:{activity_category}, 지역:{dominant_region}, 변동성:{volatility_multiplier:.2f}"
            }
            
        except Exception as e:
            logger.error(f"손절 레벨 계산 실패: {e}")
            return {
                'stop_loss_price': entry_price * (1 - self.base_strategy['stop_loss_base']),
                'stop_loss_percentage': self.base_strategy['stop_loss_base'] * 100,
                'trailing_stop_activation': entry_price * 1.15,
                'trailing_stop_percentage': 6.0,
                'reason': "기본값(계산실패)"
            }
    
    def calculate_dynamic_take_profit(
        self, 
        entry_price: float, 
        timezone_analysis: Dict,
        trading_style: str = 'momentum_driven'
    ) -> List[Dict]:
        """동적 익절 레벨 계산 (다단계)"""
        try:
            # 기본 익절 레벨
            base_levels = self.base_strategy['take_profit_levels']
            
            # 글로벌 활성도 조정
            activity_score = timezone_analysis.get('global_activity_score', 50)
            activity_category = self.classify_global_activity(activity_score)
            profit_multiplier = self.activity_strategy_matrix[activity_category]['profit_target_multiplier']
            
            # 거래 스타일별 조정
            style_multipliers = {
                'momentum_driven': 1.2,      # 모멘텀: 높은 수익 목표
                'volatility_based': 0.9,     # 변동성: 보수적 수익
                'range_bound': 0.8,          # 횡보: 작은 수익 목표
                'trend_following': 1.1       # 추세추종: 중간 수익 목표
            }
            style_multiplier = style_multipliers.get(trading_style, 1.0)
            
            # 최대 보유 시간 (시간이 길수록 더 높은 수익 목표)
            max_holding_hours = self.activity_strategy_matrix[activity_category]['max_holding_hours']
            time_multiplier = 1.0 + (max_holding_hours - 24) / 100  # 24시간 기준
            
            take_profit_levels = []
            
            for i, level in enumerate(base_levels):
                # 조정된 익절 레벨 계산
                adjusted_level = level * profit_multiplier * style_multiplier * time_multiplier
                
                # 안전 범위 제한
                adjusted_level = max(0.08, min(0.80, adjusted_level))  # 8% ~ 80%
                
                # 익절 가격 계산
                target_price = entry_price * (1 + adjusted_level)
                
                # 물량 비율 설정 (첫 번째는 30%, 두 번째는 40%, 세 번째는 30%)
                quantity_ratios = [0.3, 0.4, 0.3]
                quantity_ratio = quantity_ratios[i] if i < len(quantity_ratios) else 0.2
                
                take_profit_levels.append({
                    'level': i + 1,
                    'target_price': target_price,
                    'target_percentage': adjusted_level * 100,
                    'quantity_ratio': quantity_ratio,
                    'max_holding_hours': max_holding_hours
                })
            
            logger.info(f"익절 레벨 설정: {len(take_profit_levels)}단계")
            for level in take_profit_levels:
                logger.info(f"  Level {level['level']}: +{level['target_percentage']:.1f}% ({level['quantity_ratio']*100:.0f}% 물량)")
            
            return take_profit_levels
            
        except Exception as e:
            logger.error(f"익절 레벨 계산 실패: {e}")
            # 기본 익절 레벨 반환
            return [
                {
                    'level': i + 1,
                    'target_price': entry_price * (1 + level),
                    'target_percentage': level * 100,
                    'quantity_ratio': [0.3, 0.4, 0.3][i],
                    'max_holding_hours': 24
                }
                for i, level in enumerate(base_levels)
            ]
    
    def generate_comprehensive_strategy_config(
        self, 
        entry_price: float,
        base_amount: float,
        market_volatility: float = 0.05
    ) -> Dict:
        """종합적인 거래 전략 설정 생성"""
        try:
            # 시간대 분석 로드
            timezone_analysis = self.load_timezone_analysis_from_s3()
            
            if not timezone_analysis:
                logger.warning("시간대 분석 없음 - 기본 전략 사용")
                return self._get_default_strategy_config(entry_price, base_amount)
            
            # 동적 파라미터 계산
            position_size = self.calculate_dynamic_position_size(
                base_amount, timezone_analysis, market_volatility
            )
            
            stop_loss_config = self.calculate_dynamic_stop_loss(
                entry_price, timezone_analysis, market_volatility
            )
            
            trading_style = timezone_analysis.get('trading_style', {}).get('primary_style', 'momentum_driven')
            take_profit_levels = self.calculate_dynamic_take_profit(
                entry_price, timezone_analysis, trading_style
            )
            
            # 종합 설정 구성
            strategy_config = {
                'position_management': {
                    'position_size_krw': position_size,
                    'position_ratio': position_size / base_amount,
                    'max_risk_per_trade': position_size * 0.08  # 최대 8% 손실
                },
                'risk_management': {
                    'stop_loss': stop_loss_config,
                    'take_profit_levels': take_profit_levels,
                    'max_holding_hours': self.activity_strategy_matrix[
                        self.classify_global_activity(timezone_analysis.get('global_activity_score', 50))
                    ]['max_holding_hours']
                },
                'market_context': {
                    'global_activity_score': timezone_analysis.get('global_activity_score', 50),
                    'dominant_region': self.get_dominant_region(timezone_analysis),
                    'trading_style': trading_style,
                    'market_volatility': market_volatility
                },
                'execution_params': {
                    'entry_price': entry_price,
                    'slippage_tolerance': 0.002,  # 0.2%
                    'partial_fill_acceptable': True,
                    'market_order_threshold': 0.001  # 0.1% 이내에서는 시장가
                },
                'monitoring': {
                    'profit_check_interval_minutes': 5,
                    'stop_loss_check_interval_minutes': 1,
                    'position_review_hours': 6
                },
                'metadata': {
                    'strategy_version': '2.1',
                    'timezone_analysis_timestamp': timezone_analysis.get('analysis_timestamp'),
                    'config_generated_at': datetime.utcnow().isoformat()
                }
            }
            
            logger.info("종합 거래 전략 설정 완료")
            logger.info(f"포지션 크기: {position_size:,.0f} KRW ({position_size/base_amount*100:.1f}%)")
            logger.info(f"손절 레벨: -{stop_loss_config['stop_loss_percentage']:.1f}%")
            logger.info(f"익절 레벨: {len(take_profit_levels)}단계")
            
            return strategy_config
            
        except Exception as e:
            logger.error(f"전략 설정 생성 실패: {e}")
            return self._get_default_strategy_config(entry_price, base_amount)
    
    def _get_default_strategy_config(self, entry_price: float, base_amount: float) -> Dict:
        """기본 전략 설정"""
        position_size = base_amount * self.base_strategy['position_size_base']
        stop_loss_price = entry_price * (1 - self.base_strategy['stop_loss_base'])
        
        return {
            'position_management': {
                'position_size_krw': position_size,
                'position_ratio': self.base_strategy['position_size_base'],
                'max_risk_per_trade': position_size * self.base_strategy['stop_loss_base']
            },
            'risk_management': {
                'stop_loss': {
                    'stop_loss_price': stop_loss_price,
                    'stop_loss_percentage': self.base_strategy['stop_loss_base'] * 100,
                    'trailing_stop_activation': entry_price * 1.15,
                    'trailing_stop_percentage': 6.0,
                    'reason': "기본 설정"
                },
                'take_profit_levels': [
                    {
                        'level': i + 1,
                        'target_price': entry_price * (1 + level),
                        'target_percentage': level * 100,
                        'quantity_ratio': [0.3, 0.4, 0.3][i],
                        'max_holding_hours': 24
                    }
                    for i, level in enumerate(self.base_strategy['take_profit_levels'])
                ],
                'max_holding_hours': 24
            },
            'market_context': {
                'global_activity_score': 50,
                'dominant_region': 'Asia',
                'trading_style': 'momentum_driven',
                'market_volatility': 0.05
            },
            'execution_params': {
                'entry_price': entry_price,
                'slippage_tolerance': 0.002,
                'partial_fill_acceptable': True,
                'market_order_threshold': 0.001
            },
            'metadata': {
                'strategy_version': '2.1-default',
                'config_generated_at': datetime.utcnow().isoformat()
            }
        }
    
    def save_strategy_config_to_s3(self, strategy_config: Dict, phase_name: str) -> bool:
        """전략 설정을 S3에 저장"""
        try:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            
            # 현재 설정 파일
            current_key = f'trading_strategy/current_{phase_name}_config.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=current_key,
                Body=json.dumps(strategy_config, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            # 백업 파일
            backup_key = f'trading_strategy/history/{phase_name}_config_{timestamp}.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=backup_key,
                Body=json.dumps(strategy_config, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"전략 설정 S3 저장 완료: {phase_name}")
            return True
            
        except Exception as e:
            logger.error(f"전략 설정 S3 저장 실패: {e}")
            return False

def create_strategy_for_phase(phase_name: str, entry_price: float, base_amount: float, market_volatility: float = 0.05) -> Dict:
    """특정 Phase를 위한 전략 설정 생성"""
    enhancer = TimezoneStrategyEnhancer()
    strategy_config = enhancer.generate_comprehensive_strategy_config(
        entry_price, base_amount, market_volatility
    )
    
    # Phase별 특화 설정
    phase_customizations = {
        'ticker_scanner': {
            'scan_interval_minutes': 5,
            'volatility_threshold': 0.03,
            'volume_spike_threshold': 1.5
        },
        'data_collector': {
            'collection_interval_minutes': 1,
            'data_retention_hours': 48,
            'indicators_to_calculate': ['RSI', 'MACD', 'BB', 'ADX']
        },
        'pattern_analyzer': {
            'analysis_depth_minutes': 60,
            'pattern_confidence_threshold': 0.7,
            'breakout_confirmation_candles': 3
        }
    }
    
    if phase_name in phase_customizations:
        strategy_config['phase_specific'] = phase_customizations[phase_name]
    
    # S3에 저장
    enhancer.save_strategy_config_to_s3(strategy_config, phase_name)
    
    return strategy_config

# 사용 예시
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    # 예시: BTC 가격 159,348,000 KRW, 100만원 투자
    config = create_strategy_for_phase(
        phase_name='ticker_scanner',
        entry_price=159348000,
        base_amount=1000000,
        market_volatility=0.045
    )
    
    print("Generated Strategy Config:")
    print(json.dumps(config, ensure_ascii=False, indent=2))