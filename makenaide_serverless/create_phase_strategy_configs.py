#!/usr/bin/env python3
"""
📊 Phase별 전용 전략 설정 파일 생성 및 S3 저장
- 각 Phase별로 특화된 시간대 전략 설정 생성
- S3에 자동 배포하여 Lambda 함수들이 읽을 수 있도록 구성
- 시간대 분석을 활용한 동적 거래 파라미터 제공
"""

import boto3
import json
import logging
from datetime import datetime
from typing import Dict, List
import sys
import os

# timezone_strategy_enhancer 모듈 로드
try:
    from timezone_strategy_enhancer import TimezoneStrategyEnhancer, create_strategy_for_phase
    STRATEGY_ENHANCER_AVAILABLE = True
except ImportError:
    STRATEGY_ENHANCER_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PhaseStrategyConfigBuilder:
    """Phase별 전용 전략 설정 생성 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = 'makenaide-bucket-901361833359'
        
        # Phase별 설정
        self.phase_configs = {
            'scanner': {
                'base_amount': 1000000,
                'entry_price': 159348000,
                'phase_specific': {
                    'scan_interval_minutes': 5,
                    'volatility_threshold': 0.03,
                    'volume_spike_threshold': 1.5,
                    'max_scan_symbols': 50,
                    'breakout_confirmation_candles': 2
                },
                'description': 'Ticker Scanner - 시장 스캔 및 종목 발굴'
            },
            'data_collector': {
                'base_amount': 1000000,
                'entry_price': 159348000,
                'phase_specific': {
                    'collection_interval_minutes': 1,
                    'data_retention_hours': 48,
                    'indicators_to_calculate': ['RSI', 'MACD', 'BB', 'ADX', 'STOCH'],
                    'timeframes': ['1m', '5m', '15m', '1h', '4h', '1d'],
                    'data_validation': True
                },
                'description': 'Data Collector - 실시간 시장 데이터 수집'
            },
            'filter_phase2': {
                'base_amount': 1000000,
                'entry_price': 159348000,
                'phase_specific': {
                    'filter_criteria': {
                        'min_volume_24h': 1000000000,  # 10억원 이상
                        'min_price_change': 0.02,      # 2% 이상 변동
                        'rsi_range': [30, 70],
                        'volume_spike_min': 1.3
                    },
                    'ranking_weights': {
                        'volume': 0.3,
                        'momentum': 0.25,
                        'volatility': 0.2,
                        'technical': 0.25
                    }
                },
                'description': 'Comprehensive Filter - 종합 필터링 및 종목 선별'
            },
            'gpt_analysis_phase3': {
                'base_amount': 1000000,
                'entry_price': 159348000,
                'phase_specific': {
                    'analysis_depth': 'comprehensive',
                    'market_sentiment_weight': 0.3,
                    'technical_analysis_weight': 0.4,
                    'fundamental_weight': 0.3,
                    'news_analysis_enabled': True,
                    'confidence_threshold': 0.7
                },
                'description': 'GPT Analysis - AI 기반 시장 분석 및 예측'
            },
            'analysis_4h_phase4': {
                'base_amount': 1000000,
                'entry_price': 159348000,
                'phase_specific': {
                    'analysis_timeframe': '4h',
                    'trend_confirmation_periods': 3,
                    'support_resistance_levels': 5,
                    'pattern_recognition': True,
                    'volume_profile_analysis': True
                },
                'description': '4H Analysis - 4시간 차트 기술적 분석'
            },
            'condition_check_phase5': {
                'base_amount': 1000000,
                'entry_price': 159348000,
                'phase_specific': {
                    'entry_conditions': {
                        'min_confidence': 0.75,
                        'max_risk_per_trade': 0.02,
                        'market_condition_required': 'BULL_OR_NEUTRAL',
                        'position_correlation_max': 0.7
                    },
                    'risk_checks': [
                        'portfolio_correlation',
                        'position_sizing',
                        'market_condition',
                        'volatility_check'
                    ]
                },
                'description': 'Condition Check - 진입 조건 및 리스크 검증'
            },
            'trade_execution_phase6': {
                'base_amount': 1000000,
                'entry_price': 159348000,
                'phase_specific': {
                    'execution_strategy': 'TWAP',  # Time Weighted Average Price
                    'slippage_tolerance': 0.002,
                    'max_execution_time_minutes': 10,
                    'partial_fill_acceptable': True,
                    'order_splitting': {
                        'enabled': True,
                        'max_chunks': 5,
                        'time_interval_seconds': 30
                    }
                },
                'description': 'Trade Execution - 실제 거래 실행 및 체결 관리'
            }
        }
    
    def get_current_btc_price(self) -> float:
        """현재 BTC 가격 조회"""
        try:
            import urllib3
            http = urllib3.PoolManager()
            response = http.request('GET', 'https://api.upbit.com/v1/ticker?markets=KRW-BTC')
            
            if response.status == 200:
                data = json.loads(response.data.decode('utf-8'))[0]
                return float(data['trade_price'])
            else:
                logger.warning("BTC 가격 조회 실패 - 기본값 사용")
                return 159348000
                
        except Exception as e:
            logger.warning(f"BTC 가격 조회 중 오류: {e}")
            return 159348000
    
    def create_phase_strategy_config(self, phase_name: str) -> Dict:
        """특정 Phase를 위한 전략 설정 생성"""
        try:
            if phase_name not in self.phase_configs:
                logger.error(f"알 수 없는 Phase: {phase_name}")
                return None
            
            phase_config = self.phase_configs[phase_name]
            current_btc_price = self.get_current_btc_price()
            
            logger.info(f"📊 {phase_name} 전략 설정 생성 중... (BTC: {current_btc_price:,.0f})")
            
            # 기본 전략 설정
            if STRATEGY_ENHANCER_AVAILABLE:
                try:
                    enhancer = TimezoneStrategyEnhancer()
                    
                    # 동적 전략 생성
                    strategy_config = enhancer.generate_comprehensive_strategy_config(
                        entry_price=current_btc_price,
                        base_amount=phase_config['base_amount'],
                        market_volatility=0.05
                    )
                    
                    # Phase별 특화 설정 추가
                    strategy_config['phase_specific'] = phase_config['phase_specific']
                    strategy_config['phase_info'] = {
                        'phase_name': phase_name,
                        'description': phase_config['description'],
                        'btc_price_at_config': current_btc_price
                    }
                    
                    logger.info(f"✅ {phase_name} 동적 전략 설정 생성 완료")
                    
                except Exception as e:
                    logger.warning(f"동적 전략 생성 실패 ({phase_name}): {e}")
                    strategy_config = self._create_fallback_strategy(phase_name, current_btc_price)
            else:
                logger.warning(f"TimezoneStrategyEnhancer 없음 - {phase_name} 기본 전략 사용")
                strategy_config = self._create_fallback_strategy(phase_name, current_btc_price)
            
            return strategy_config
            
        except Exception as e:
            logger.error(f"Phase 전략 설정 생성 실패 ({phase_name}): {e}")
            return None
    
    def _create_fallback_strategy(self, phase_name: str, btc_price: float) -> Dict:
        """기본 전략 설정 (TimezoneStrategyEnhancer 없을 때)"""
        phase_config = self.phase_configs[phase_name]
        
        return {
            'position_management': {
                'position_size_krw': phase_config['base_amount'] * 0.5,
                'position_ratio': 0.5,
                'max_risk_per_trade': phase_config['base_amount'] * 0.02
            },
            'risk_management': {
                'stop_loss': {
                    'stop_loss_price': btc_price * 0.92,
                    'stop_loss_percentage': 8.0,
                    'trailing_stop_activation': btc_price * 1.15,
                    'trailing_stop_percentage': 6.0,
                    'reason': "기본 설정 (8% 손절)"
                },
                'take_profit_levels': [
                    {'level': 1, 'target_price': btc_price * 1.15, 'target_percentage': 15.0, 'quantity_ratio': 0.3},
                    {'level': 2, 'target_price': btc_price * 1.30, 'target_percentage': 30.0, 'quantity_ratio': 0.4},
                    {'level': 3, 'target_price': btc_price * 1.50, 'target_percentage': 50.0, 'quantity_ratio': 0.3}
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
                'entry_price': btc_price,
                'slippage_tolerance': 0.002,
                'partial_fill_acceptable': True,
                'market_order_threshold': 0.001
            },
            'phase_specific': phase_config['phase_specific'],
            'phase_info': {
                'phase_name': phase_name,
                'description': phase_config['description'],
                'btc_price_at_config': btc_price
            },
            'metadata': {
                'strategy_version': '2.1-fallback',
                'config_generated_at': datetime.utcnow().isoformat(),
                'timezone_strategy_available': False
            }
        }
    
    def save_strategy_to_s3(self, phase_name: str, strategy_config: Dict) -> bool:
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
            
            logger.info(f"✅ {phase_name} 전략 설정 S3 저장 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ {phase_name} 전략 설정 S3 저장 실패: {e}")
            return False
    
    def create_all_phase_configs(self) -> Dict[str, bool]:
        """모든 Phase별 전략 설정 생성 및 저장"""
        logger.info("🚀 Phase별 전용 전략 설정 일괄 생성 시작")
        logger.info("=" * 80)
        
        results = {}
        
        for phase_name in self.phase_configs.keys():
            logger.info(f"\n📊 {phase_name} 처리 중...")
            
            # 전략 설정 생성
            strategy_config = self.create_phase_strategy_config(phase_name)
            if not strategy_config:
                results[phase_name] = False
                continue
            
            # S3에 저장
            save_success = self.save_strategy_to_s3(phase_name, strategy_config)
            results[phase_name] = save_success
            
            if save_success:
                # 주요 정보 로깅
                pos_size = strategy_config['position_management']['position_size_krw']
                stop_loss = strategy_config['risk_management']['stop_loss']['stop_loss_percentage']
                
                logger.info(f"✅ {phase_name} 완료 - 포지션: {pos_size:,.0f}KRW, 손절: {stop_loss:.1f}%")
        
        # 결과 요약
        success_count = sum(results.values())
        total_count = len(results)
        
        logger.info("=" * 80)
        logger.info(f"🎯 Phase 전략 설정 생성 완료: {success_count}/{total_count}")
        
        print(f"""
📊 Phase별 전용 전략 설정 생성 완료!

📈 생성 결과:
   • 총 Phase: {total_count}개
   • 성공: {success_count}개
   • 실패: {total_count - success_count}개

🔍 상세 결과:
{chr(10).join(f'   • {phase}: {"✅ 성공" if status else "❌ 실패"}' for phase, status in results.items())}

🌏 전략 설정 특징:
   • 시간대 분석 기반 동적 파라미터 조정
   • Phase별 특화된 거래 로직 설정
   • S3 자동 저장으로 Lambda 함수 연동
   • 실시간 시장 상황 반영

📂 S3 저장 위치:
   • 현재 설정: s3://{self.s3_bucket}/trading_strategy/current_{{phase}}_config.json
   • 히스토리: s3://{self.s3_bucket}/trading_strategy/history/{{phase}}_config_{{timestamp}}.json

🎯 다음 단계:
   • Lambda 함수들이 S3에서 전략 설정 로드
   • EventBridge 스케줄 실행 시 자동 적용
   • 시간대별 전략 갱신 모니터링
        """)
        
        return results
    
    def create_master_strategy_index(self) -> bool:
        """마스터 전략 인덱스 파일 생성"""
        try:
            logger.info("📋 마스터 전략 인덱스 생성 중...")
            
            index = {
                'last_updated': datetime.utcnow().isoformat(),
                'timezone_strategy_available': STRATEGY_ENHANCER_AVAILABLE,
                'phases': {},
                'global_settings': {
                    'base_amount_default': 1000000,
                    'strategy_refresh_hours': 6,
                    'risk_per_trade_max': 0.02,
                    'max_concurrent_positions': 3
                }
            }
            
            # 각 Phase 정보 수집
            for phase_name, config in self.phase_configs.items():
                index['phases'][phase_name] = {
                    'description': config['description'],
                    'config_file': f'trading_strategy/current_{phase_name}_config.json',
                    'last_updated': datetime.utcnow().isoformat(),
                    'phase_specific_keys': list(config['phase_specific'].keys())
                }
            
            # S3에 저장
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key='trading_strategy/master_index.json',
                Body=json.dumps(index, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            
            logger.info("✅ 마스터 전략 인덱스 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 마스터 전략 인덱스 생성 실패: {e}")
            return False

def main():
    """메인 실행"""
    builder = PhaseStrategyConfigBuilder()
    
    # Phase별 전략 설정 생성
    results = builder.create_all_phase_configs()
    
    # 마스터 인덱스 생성
    index_success = builder.create_master_strategy_index()
    
    success_count = sum(results.values())
    total_count = len(results)
    
    if success_count >= total_count * 0.8 and index_success:
        print("🎉 Phase별 전략 설정 생성 성공!")
        exit(0)
    else:
        print("⚠️ 일부 Phase 전략 설정 생성 실패!")
        exit(1)

if __name__ == '__main__':
    main()