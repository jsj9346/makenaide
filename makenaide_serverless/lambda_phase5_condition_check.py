#!/usr/bin/env python3
"""
⚡ Phase 5: 최종 조건 검사 및 매매 신호 생성 Lambda
- Phase 4 4시간봉 분석 결과를 받아 실시간 재검증
- 리스크 관리 및 포지션 사이징 결정
- 최종 매수/매도 신호 생성 및 Phase 6로 전달
- 시장 상황 변화에 따른 동적 조정
"""

import boto3
import json
import logging
import pandas as pd
import numpy as np
import pytz
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import pyupbit
import warnings
warnings.filterwarnings('ignore')

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class FinalConditionCheckPhase5:
    """최종 조건 검사 및 신호 생성 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.secrets_client = boto3.client('secretsmanager')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 리스크 관리 설정
        self.risk_config = {
            'max_position_per_trade': float(os.environ.get('MAX_POSITION_PCT', '10.0')),  # 1회 거래당 최대 포지션 비율 (%)
            'max_total_exposure': float(os.environ.get('MAX_TOTAL_EXPOSURE', '50.0')),  # 총 노출 한도 (%)
            'max_daily_trades': int(os.environ.get('MAX_DAILY_TRADES', '5')),  # 일일 최대 거래 횟수
            'stop_loss_pct': float(os.environ.get('STOP_LOSS_PCT', '8.0')),  # 기본 손절 비율 (%)
            'take_profit_pct': float(os.environ.get('TAKE_PROFIT_PCT', '25.0')),  # 기본 익절 비율 (%)
            'volatility_adjustment': True,  # 변동성에 따른 포지션 조정
            'correlation_limit': float(os.environ.get('CORRELATION_LIMIT', '0.7'))  # 상관관계 제한
        }
        
        # 실시간 검증 임계값
        self.validation_thresholds = {
            'price_change_limit': float(os.environ.get('PRICE_CHANGE_LIMIT', '5.0')),  # 분석 시점 대비 가격 변화 한도 (%)
            'volume_drop_threshold': float(os.environ.get('VOLUME_DROP_THRESHOLD', '0.5')),  # 거래량 급감 임계값
            'market_correlation_btc': float(os.environ.get('BTC_CORRELATION_LIMIT', '0.8')),  # BTC 상관관계 한도
            'rsi_overbought_limit': float(os.environ.get('RSI_OVERBOUGHT_LIMIT', '80')),  # RSI 과매수 한도
            'spread_limit_pct': float(os.environ.get('SPREAD_LIMIT_PCT', '1.0'))  # 스프레드 한도 (%)
        }

    def load_phase4_signals(self) -> Optional[List[Dict[str, Any]]]:
        """Phase 4 4시간봉 분석에서 매수 신호 종목들 로드"""
        try:
            logger.info("📊 Phase 4 4시간봉 분석 결과 로드 중...")
            
            # 최신 Phase 4 결과 파일 찾기
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix='phase4/4h_analysis_results_'
            )
            
            if 'Contents' not in response or not response['Contents']:
                logger.warning("Phase 4 결과 파일이 없습니다")
                return None
            
            # 가장 최신 파일 선택
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=latest_file['Key']
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            analysis_results = data.get('analysis_results', [])
            
            # STRONG_BUY, BUY 신호 종목만 필터링
            trading_signals = [
                result for result in analysis_results
                if result.get('final_recommendation', {}).get('final_rating') in ['STRONG_BUY', 'BUY']
            ]
            
            if not trading_signals:
                logger.warning("Phase 4에서 매수 신호 종목이 없습니다")
                return None
                
            logger.info(f"✅ Phase 4 매수 신호 로드 완료: {len(trading_signals)}개 종목")
            return trading_signals
            
        except Exception as e:
            logger.error(f"❌ Phase 4 데이터 로드 실패: {e}")
            return None

    def get_current_portfolio_status(self) -> Dict[str, Any]:
        """현재 포트폴리오 상태 조회 (DynamoDB 또는 S3에서)"""
        try:
            # 포트폴리오 상태는 실제 환경에서 DynamoDB에서 조회
            # 여기서는 기본값으로 시뮬레이션
            portfolio_status = {
                'total_value': 1000000,  # 총 포트폴리오 가치 (원)
                'available_cash': 800000,  # 사용 가능 현금
                'current_positions': [],  # 현재 보유 종목
                'daily_trade_count': 0,  # 오늘 거래 횟수
                'total_exposure_pct': 0,  # 현재 총 노출 비율
                'last_updated': datetime.now(self.kst).isoformat()
            }
            
            logger.info(f"💰 포트폴리오 상태 조회 완료: 가용자금 {portfolio_status['available_cash']:,}원")
            return portfolio_status
            
        except Exception as e:
            logger.error(f"❌ 포트폴리오 상태 조회 실패: {e}")
            return {
                'total_value': 0,
                'available_cash': 0,
                'current_positions': [],
                'daily_trade_count': 0,
                'total_exposure_pct': 0,
                'error': str(e)
            }

    def validate_real_time_conditions(self, ticker: str, original_analysis: Dict) -> Dict[str, Any]:
        """실시간 조건 재검증"""
        try:
            logger.info(f"🔍 {ticker} 실시간 조건 재검증 중...")
            
            validation_result = {
                'ticker': ticker,
                'validation_time': datetime.now(self.kst).isoformat(),
                'checks': {},
                'warnings': [],
                'passed': True,
                'confidence_adjustment': 0
            }
            
            # 1. 현재 가격 및 기본 정보 조회
            current_ticker_info = pyupbit.get_ticker(ticker)
            current_price = float(current_ticker_info.get('trade_price', 0))
            
            if current_price == 0:
                validation_result['checks']['price_available'] = False
                validation_result['passed'] = False
                validation_result['warnings'].append("실시간 가격 조회 실패")
                return validation_result
            
            validation_result['checks']['price_available'] = True
            validation_result['current_price'] = current_price
            
            # 2. 가격 변화 검증
            original_price = original_analysis.get('phase4_timing_analysis', {}).get('signals', {}).get('price_data', {}).get('current_price', current_price)
            if original_price:
                price_change_pct = ((current_price - original_price) / original_price) * 100
                validation_result['price_change_pct'] = price_change_pct
                
                if abs(price_change_pct) > self.validation_thresholds['price_change_limit']:
                    validation_result['checks']['price_stability'] = False
                    validation_result['warnings'].append(f"가격 변화 {price_change_pct:.2f}% > 임계값 {self.validation_thresholds['price_change_limit']}%")
                    validation_result['confidence_adjustment'] -= 20
                else:
                    validation_result['checks']['price_stability'] = True
            
            # 3. 거래량 검증
            current_volume = float(current_ticker_info.get('acc_trade_volume_24h', 0))
            original_volume = original_analysis.get('phase4_timing_analysis', {}).get('signals', {}).get('volume', {}).get('current', current_volume)
            
            if original_volume and original_volume > 0:
                volume_change_ratio = current_volume / original_volume
                validation_result['volume_change_ratio'] = volume_change_ratio
                
                if volume_change_ratio < self.validation_thresholds['volume_drop_threshold']:
                    validation_result['checks']['volume_maintained'] = False
                    validation_result['warnings'].append(f"거래량 급감: {volume_change_ratio:.2f}배")
                    validation_result['confidence_adjustment'] -= 15
                else:
                    validation_result['checks']['volume_maintained'] = True
            
            # 4. 스프레드 검증 (호가창)
            orderbook = pyupbit.get_orderbook(ticker)
            if orderbook and orderbook[0]:
                ask_price = float(orderbook[0]['orderbook_units'][0]['ask_price'])
                bid_price = float(orderbook[0]['orderbook_units'][0]['bid_price'])
                spread_pct = ((ask_price - bid_price) / current_price) * 100
                
                validation_result['spread_pct'] = spread_pct
                
                if spread_pct > self.validation_thresholds['spread_limit_pct']:
                    validation_result['checks']['spread_acceptable'] = False
                    validation_result['warnings'].append(f"스프레드 과도: {spread_pct:.2f}%")
                    validation_result['confidence_adjustment'] -= 10
                else:
                    validation_result['checks']['spread_acceptable'] = True
            
            # 5. RSI 재검증 (간단 계산)
            recent_prices = pyupbit.get_ohlcv(ticker, interval="minute5", count=15)
            if recent_prices is not None and not recent_prices.empty:
                closes = recent_prices['close'].values
                
                # 간단한 RSI 계산
                deltas = np.diff(closes)
                gains = np.where(deltas > 0, deltas, 0)
                losses = np.where(deltas < 0, -deltas, 0)
                
                if len(gains) > 5:
                    avg_gain = np.mean(gains[-10:]) if len(gains) >= 10 else np.mean(gains)
                    avg_loss = np.mean(losses[-10:]) if len(losses) >= 10 else np.mean(losses)
                    
                    if avg_loss > 0:
                        rs = avg_gain / avg_loss
                        current_rsi = 100 - (100 / (1 + rs))
                        
                        validation_result['current_rsi'] = current_rsi
                        
                        if current_rsi > self.validation_thresholds['rsi_overbought_limit']:
                            validation_result['checks']['not_overbought'] = False
                            validation_result['warnings'].append(f"RSI 과매수: {current_rsi:.1f}")
                            validation_result['confidence_adjustment'] -= 25
                        else:
                            validation_result['checks']['not_overbought'] = True
            
            # 6. 최종 검증 결과
            failed_checks = [k for k, v in validation_result['checks'].items() if v is False]
            if failed_checks:
                validation_result['passed'] = False
                logger.warning(f"⚠️ {ticker} 실시간 검증 실패: {failed_checks}")
            else:
                logger.info(f"✅ {ticker} 실시간 검증 통과")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ {ticker} 실시간 조건 검증 실패: {e}")
            return {
                'ticker': ticker,
                'validation_time': datetime.now(self.kst).isoformat(),
                'passed': False,
                'error': str(e),
                'warnings': [f"검증 오류: {str(e)}"]
            }

    def calculate_position_sizing(self, ticker: str, signal_data: Dict, portfolio_status: Dict, validation_result: Dict) -> Dict[str, Any]:
        """포지션 사이징 계산"""
        try:
            logger.info(f"💰 {ticker} 포지션 사이징 계산 중...")
            
            sizing_result = {
                'ticker': ticker,
                'calculation_time': datetime.now(self.kst).isoformat(),
                'recommended_position_krw': 0,
                'position_pct': 0,
                'stop_loss_price': 0,
                'take_profit_price': 0,
                'risk_reward_ratio': 0,
                'sizing_factors': {}
            }
            
            available_cash = portfolio_status.get('available_cash', 0)
            if available_cash <= 0:
                sizing_result['error'] = "사용 가능 자금 없음"
                return sizing_result
            
            current_price = validation_result.get('current_price', 0)
            if current_price <= 0:
                sizing_result['error'] = "유효한 현재 가격 없음"
                return sizing_result
            
            # 1. 기본 포지션 크기 (신호 강도 기반)
            signal_rating = signal_data.get('final_recommendation', {}).get('final_rating', 'SKIP')
            weighted_score = signal_data.get('final_recommendation', {}).get('weighted_score', 0)
            
            if signal_rating == 'STRONG_BUY':
                base_position_pct = min(self.risk_config['max_position_per_trade'], 10.0)
            elif signal_rating == 'BUY':
                base_position_pct = min(self.risk_config['max_position_per_trade'], 7.0)
            else:
                base_position_pct = 0
            
            sizing_result['sizing_factors']['base_position_pct'] = base_position_pct
            
            # 2. 신호 강도 조정
            score_adjustment = (weighted_score - 50) / 100  # -0.5 ~ +0.5
            adjusted_position_pct = base_position_pct * (1 + score_adjustment * 0.5)
            sizing_result['sizing_factors']['score_adjustment'] = score_adjustment
            
            # 3. 실시간 검증 결과 반영
            confidence_adjustment = validation_result.get('confidence_adjustment', 0)
            confidence_multiplier = max(0.3, 1 + confidence_adjustment / 100)  # 최소 30%는 유지
            adjusted_position_pct *= confidence_multiplier
            sizing_result['sizing_factors']['confidence_multiplier'] = confidence_multiplier
            
            # 4. 변동성 조정
            if self.risk_config['volatility_adjustment']:
                # 간단한 변동성 계산 (실제로는 더 정교한 계산 필요)
                price_change_pct = abs(validation_result.get('price_change_pct', 0))
                volatility_multiplier = max(0.5, 1 - (price_change_pct / 10))  # 변동성이 높을수록 포지션 축소
                adjusted_position_pct *= volatility_multiplier
                sizing_result['sizing_factors']['volatility_multiplier'] = volatility_multiplier
            
            # 5. 포트폴리오 제약 확인
            current_exposure = portfolio_status.get('total_exposure_pct', 0)
            max_additional_exposure = self.risk_config['max_total_exposure'] - current_exposure
            
            if adjusted_position_pct > max_additional_exposure:
                adjusted_position_pct = max(0, max_additional_exposure)
                sizing_result['sizing_factors']['exposure_limited'] = True
            
            # 6. 최종 포지션 크기 계산
            total_portfolio_value = portfolio_status.get('total_value', available_cash)
            position_krw = (adjusted_position_pct / 100) * total_portfolio_value
            position_krw = min(position_krw, available_cash * 0.9)  # 현금의 90% 이하
            
            # 7. 손절/익절 가격 계산
            stop_loss_pct = self.risk_config['stop_loss_pct']
            take_profit_pct = self.risk_config['take_profit_pct']
            
            # 변동성에 따른 손절/익절 조정
            if 'volatility_multiplier' in sizing_result['sizing_factors']:
                vol_mult = sizing_result['sizing_factors']['volatility_multiplier']
                stop_loss_pct *= (2 - vol_mult)  # 변동성이 높으면 손절 폭 확대
                take_profit_pct *= (2 - vol_mult)  # 변동성이 높으면 익절 목표 확대
            
            stop_loss_price = current_price * (1 - stop_loss_pct / 100)
            take_profit_price = current_price * (1 + take_profit_pct / 100)
            
            # 8. 리스크/리워드 비율
            risk_amount = current_price - stop_loss_price
            reward_amount = take_profit_price - current_price
            risk_reward_ratio = reward_amount / risk_amount if risk_amount > 0 else 0
            
            # 최종 결과
            sizing_result.update({
                'recommended_position_krw': round(position_krw, -3),  # 천원 단위로 반올림
                'position_pct': round(adjusted_position_pct, 2),
                'stop_loss_price': round(stop_loss_price, -1),  # 10원 단위로 반올림
                'take_profit_price': round(take_profit_price, -1),
                'risk_reward_ratio': round(risk_reward_ratio, 2),
                'entry_price_range': {
                    'min': round(current_price * 0.995, -1),  # 0.5% 아래
                    'max': round(current_price * 1.005, -1)   # 0.5% 위
                }
            })
            
            logger.info(f"💰 {ticker} 포지션 사이징 완료: {position_krw:,.0f}원 ({adjusted_position_pct:.1f}%)")
            return sizing_result
            
        except Exception as e:
            logger.error(f"❌ {ticker} 포지션 사이징 계산 실패: {e}")
            return {
                'ticker': ticker,
                'calculation_time': datetime.now(self.kst).isoformat(),
                'error': str(e),
                'recommended_position_krw': 0
            }

    def generate_final_trade_signal(self, ticker: str, signal_data: Dict, validation_result: Dict, sizing_result: Dict) -> Dict[str, Any]:
        """최종 거래 신호 생성"""
        try:
            trade_signal = {
                'ticker': ticker,
                'signal_time': datetime.now(self.kst).isoformat(),
                'action': 'HOLD',  # BUY, SELL, HOLD
                'priority': 'low',  # high, medium, low
                'execution_type': 'MARKET',  # MARKET, LIMIT
                'position_data': sizing_result,
                'validation_data': validation_result,
                'original_analysis': {
                    'phase3_confidence': signal_data.get('combined_assessment', {}).get('gpt_confidence', 0),
                    'phase4_timing_score': signal_data.get('combined_assessment', {}).get('timing_score', 0),
                    'phase4_rating': signal_data.get('final_recommendation', {}).get('final_rating', 'SKIP')
                },
                'execution_conditions': {},
                'monitoring_alerts': []
            }
            
            # 최종 거래 결정 로직
            validation_passed = validation_result.get('passed', False)
            position_size = sizing_result.get('recommended_position_krw', 0)
            original_rating = signal_data.get('final_recommendation', {}).get('final_rating', 'SKIP')
            
            if validation_passed and position_size > 10000:  # 최소 1만원 이상
                if original_rating == 'STRONG_BUY':
                    trade_signal['action'] = 'BUY'
                    trade_signal['priority'] = 'high'
                    trade_signal['execution_type'] = 'MARKET'  # 즉시 시장가 매수
                elif original_rating == 'BUY':
                    trade_signal['action'] = 'BUY'
                    trade_signal['priority'] = 'medium'
                    trade_signal['execution_type'] = 'LIMIT'  # 지정가 매수
                else:
                    trade_signal['action'] = 'HOLD'
                    trade_signal['priority'] = 'low'
            else:
                trade_signal['action'] = 'HOLD'
                trade_signal['priority'] = 'skip'
                
                # 거부 사유
                if not validation_passed:
                    trade_signal['rejection_reason'] = "실시간 검증 실패"
                    trade_signal['validation_warnings'] = validation_result.get('warnings', [])
                if position_size <= 10000:
                    trade_signal['rejection_reason'] = "포지션 크기 부족"
            
            # 실행 조건 설정
            if trade_signal['action'] == 'BUY':
                current_price = validation_result.get('current_price', 0)
                
                trade_signal['execution_conditions'] = {
                    'max_slippage_pct': 0.5,  # 최대 슬리피지
                    'execution_timeout_minutes': 5,  # 실행 타임아웃
                    'price_range': sizing_result.get('entry_price_range', {}),
                    'stop_loss_price': sizing_result.get('stop_loss_price', 0),
                    'take_profit_price': sizing_result.get('take_profit_price', 0)
                }
                
                # 모니터링 알림 설정
                trade_signal['monitoring_alerts'] = [
                    f"매수 후 {sizing_result.get('stop_loss_price', 0):,.0f}원 손절선 모니터링",
                    f"목표가 {sizing_result.get('take_profit_price', 0):,.0f}원 도달 시 익절 고려",
                    f"거래량 급감({validation_result.get('volume_change_ratio', 1):.2f}배) 모니터링"
                ]
            
            logger.info(f"🎯 {ticker} 최종 신호: {trade_signal['action']} ({trade_signal['priority']})")
            return trade_signal
            
        except Exception as e:
            logger.error(f"❌ {ticker} 최종 거래 신호 생성 실패: {e}")
            return {
                'ticker': ticker,
                'signal_time': datetime.now(self.kst).isoformat(),
                'action': 'HOLD',
                'priority': 'skip',
                'error': str(e)
            }

    def process_trading_signals(self, signals: List[Dict]) -> List[Dict[str, Any]]:
        """매수 신호들에 대해 최종 검사 실행"""
        try:
            logger.info(f"🔍 최종 조건 검사 시작: {len(signals)}개 매수 신호")
            
            # 포트폴리오 상태 조회
            portfolio_status = self.get_current_portfolio_status()
            
            # 일일 거래 한도 확인
            if portfolio_status.get('daily_trade_count', 0) >= self.risk_config['max_daily_trades']:
                logger.warning(f"⚠️ 일일 거래 한도 초과: {portfolio_status['daily_trade_count']}/{self.risk_config['max_daily_trades']}")
                return []
            
            final_signals = []
            
            for idx, signal in enumerate(signals):
                ticker = signal.get('ticker')
                if not ticker:
                    continue
                
                try:
                    logger.info(f"🔍 {ticker} 최종 검사 중... ({idx+1}/{len(signals)})")
                    
                    # 1. 실시간 조건 재검증
                    validation_result = self.validate_real_time_conditions(ticker, signal)
                    
                    # 2. 포지션 사이징 계산
                    sizing_result = self.calculate_position_sizing(ticker, signal, portfolio_status, validation_result)
                    
                    # 3. 최종 거래 신호 생성
                    trade_signal = self.generate_final_trade_signal(ticker, signal, validation_result, sizing_result)
                    
                    final_signals.append(trade_signal)
                    
                    action = trade_signal.get('action', 'HOLD')
                    position_size = sizing_result.get('recommended_position_krw', 0)
                    logger.info(f"✅ {ticker} 최종 검사 완료: {action}, {position_size:,.0f}원")
                    
                    # 포트폴리오 노출 업데이트 (시뮬레이션)
                    if action == 'BUY' and position_size > 0:
                        additional_exposure = sizing_result.get('position_pct', 0)
                        portfolio_status['total_exposure_pct'] += additional_exposure
                        portfolio_status['daily_trade_count'] += 1
                
                except Exception as e:
                    logger.error(f"❌ {ticker} 개별 최종 검사 실패: {e}")
                    continue
            
            logger.info(f"🎯 최종 조건 검사 완료: {len(final_signals)}개 최종 신호")
            return final_signals
            
        except Exception as e:
            logger.error(f"❌ 매수 신호 처리 실패: {e}")
            return []

    def save_results_to_s3(self, results: List[Dict[str, Any]]) -> bool:
        """최종 검사 결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            # 결과 통계 계산
            total_signals = len(results)
            buy_signals = len([r for r in results if r.get('action') == 'BUY'])
            high_priority = len([r for r in results if r.get('priority') == 'high'])
            
            output_data = {
                'phase': 'final_condition_check',
                'status': 'success',
                'timestamp': timestamp,
                'risk_config': self.risk_config,
                'validation_thresholds': self.validation_thresholds,
                'total_signals': total_signals,
                'final_trade_signals': results,
                'summary': {
                    'total_analyzed': total_signals,
                    'actions': {
                        'BUY': buy_signals,
                        'HOLD': total_signals - buy_signals
                    },
                    'priorities': {
                        'high': high_priority,
                        'medium': len([r for r in results if r.get('priority') == 'medium']),
                        'low': len([r for r in results if r.get('priority') == 'low']),
                        'skip': len([r for r in results if r.get('priority') == 'skip'])
                    },
                    'total_position_value': sum([r.get('position_data', {}).get('recommended_position_krw', 0) for r in results]),
                    'ready_for_execution': buy_signals
                }
            }
            
            # S3에 저장
            key = f'phase5/final_trade_signals_{timestamp}.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json.dumps(output_data, indent=2, ensure_ascii=False),
                ContentType='application/json'
            )
            
            logger.info(f"✅ Phase 5 결과 S3 저장 완료: s3://{self.s3_bucket}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 저장 실패: {e}")
            return False

    def trigger_next_phase(self, trade_signals: List[Dict]) -> bool:
        """Phase 6 트리거 (실제 매수 신호가 있는 경우만)"""
        try:
            executable_trades = [
                signal for signal in trade_signals 
                if signal.get('action') == 'BUY'
            ]
            
            if not executable_trades:
                logger.info("📭 실행할 거래 신호가 없어 Phase 6 트리거 생략")
                return False
            
            self.events_client.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.phase5',
                        'DetailType': 'Phase 5 Final Check Completed',
                        'Detail': json.dumps({
                            'status': 'completed',
                            'timestamp': datetime.now(self.kst).isoformat(),
                            'executable_trades': len(executable_trades),
                            'total_value': sum([t.get('position_data', {}).get('recommended_position_krw', 0) for t in executable_trades]),
                            'next_phase': 'phase6'
                        })
                    }
                ]
            )
            
            logger.info(f"✅ Phase 6 트리거 이벤트 발송 완료: {len(executable_trades)}개 실행 신호")
            return True
            
        except Exception as e:
            logger.error(f"❌ Phase 6 트리거 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    try:
        logger.info("🚀 Phase 5 Final Condition Check 시작")
        logger.info(f"📥 입력 이벤트: {json.dumps(event, indent=2, ensure_ascii=False)}")
        
        checker = FinalConditionCheckPhase5()
        
        # 1. Phase 4 매수 신호들 로드
        trading_signals = checker.load_phase4_signals()
        if not trading_signals:
            logger.error("❌ Phase 4 매수 신호가 없습니다")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Phase 4 매수 신호 없음'})
            }
        
        # 2. 최종 조건 검사 실행
        final_signals = checker.process_trading_signals(trading_signals)
        
        if not final_signals:
            logger.warning("⚠️ 최종 검사 결과가 없습니다")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'completed',
                    'final_signals': 0,
                    'message': '최종 검사 결과 없음'
                })
            }
        
        # 3. 결과 저장
        save_success = checker.save_results_to_s3(final_signals)
        
        if not save_success:
            logger.error("❌ 결과 저장 실패")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3 저장 실패'})
            }
        
        # 4. Phase 6 트리거
        trigger_success = checker.trigger_next_phase(final_signals)
        
        # 5. 최종 결과 반환
        buy_count = len([s for s in final_signals if s.get('action') == 'BUY'])
        total_value = sum([s.get('position_data', {}).get('recommended_position_krw', 0) for s in final_signals])
        
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'input_signals': len(trading_signals),
                'final_signals': len(final_signals),
                'executable_trades': buy_count,
                'total_position_value': total_value,
                'execution_summary': {
                    'high_priority': len([s for s in final_signals if s.get('priority') == 'high']),
                    'medium_priority': len([s for s in final_signals if s.get('priority') == 'medium']),
                    'market_orders': len([s for s in final_signals if s.get('execution_type') == 'MARKET']),
                    'limit_orders': len([s for s in final_signals if s.get('execution_type') == 'LIMIT'])
                },
                'ready_tickers': [
                    {
                        'ticker': s['ticker'],
                        'action': s['action'],
                        'priority': s['priority'],
                        'position_krw': s.get('position_data', {}).get('recommended_position_krw', 0)
                    } for s in final_signals if s.get('action') == 'BUY'
                ],
                'next_phase_triggered': trigger_success
            }, ensure_ascii=False, indent=2)
        }
        
        logger.info(f"✅ Phase 5 완료: {len(final_signals)}개 최종 신호, {buy_count}개 실행 예정")
        return result
        
    except Exception as e:
        logger.error(f"❌ Phase 5 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'status': 'failed'
            })
        }

if __name__ == "__main__":
    # 로컬 테스트용
    test_event = {
        'source': 'makenaide.phase4',
        'detail-type': 'Phase 4 4H Analysis Completed'
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))