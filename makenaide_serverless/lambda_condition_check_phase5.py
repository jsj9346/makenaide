#!/usr/bin/env python3
"""
📊 Phase 5: Condition Check Lambda
- Phase 4 4시간봉 분석 결과를 받아 최종 거래 조건 검증
- 리스크 관리 및 포지션 크기 계산
- 최종 BUY/SELL/HOLD 신호 생성
"""

import boto3
import json
import logging
import time
import pytz
import urllib3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ConditionChecker:
    """최종 거래 조건 검증 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.s3_bucket = 'makenaide-bucket-901361833359'
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 거래 조건 설정
        self.config = {
            'max_positions': 3,           # 최대 동시 보유 종목
            'position_size_pct': 0.3,     # 종목당 포트폴리오 비중 (30%)
            'min_volume_krw': 1000000000, # 최소 거래대금 (10억원)
            'price_range': {              # 가격대별 필터
                'min': 1000,              # 최소 1,000원
                'max': 100000             # 최대 100,000원
            },
            'risk_management': {
                'stop_loss_pct': 0.08,    # 손절 비율 8%
                'take_profit_pct': 0.25,  # 1차 익절 비율 25%
                'max_daily_loss': 0.02,   # 일일 최대 손실 2%
                'rsi_overbought': 80,     # RSI 과매수 구간
                'correlation_limit': 0.7   # 종목간 상관계수 제한
            },
            'market_condition': {
                'btc_correlation_max': 0.8, # BTC 상관계수 최대값
                'market_fear_min': 20,      # 공포탐욕지수 최소값
                'volatility_max': 0.15      # 최대 변동성 (15%)
            }
        }

    def load_phase4_data(self) -> Optional[List[Dict]]:
        """Phase 4 4시간봉 분석 결과 데이터 로드"""
        try:
            logger.info("Phase 4 4시간봉 분석 결과 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase4/4h_analysis_results.json'
            )
            
            data = json.loads(response['Body'].read())
            
            if data.get('status') != 'success':
                logger.error(f"Phase 4 데이터 상태 불량: {data.get('status')}")
                return None
            
            analysis_results = data.get('analysis_results', [])
            if not analysis_results:
                logger.warning("Phase 4 분석 결과가 없음")
                return None
            
            # 통과한 종목만 필터링
            passed_results = [r for r in analysis_results if r.get('passed', False)]
            
            logger.info(f"Phase 4 데이터 로드 완료: {len(passed_results)}개 종목 통과")
            return passed_results
            
        except Exception as e:
            logger.error(f"Phase 4 데이터 로드 실패: {e}")
            return None

    def fetch_current_market_data(self, ticker: str) -> Optional[Dict]:
        """현재 시장 데이터 수집 (실시간 가격, 거래량)"""
        try:
            http = urllib3.PoolManager()
            
            # 업비트 현재가 조회
            ticker_url = "https://api.upbit.com/v1/ticker"
            params = {'markets': ticker}
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{ticker_url}?{param_string}"
            
            response = http.request('GET', full_url)
            
            if response.status != 200:
                logger.warning(f"{ticker} 현재가 조회 실패: {response.status}")
                return None
            
            ticker_data = json.loads(response.data.decode('utf-8'))
            
            if not ticker_data:
                return None
            
            current_data = ticker_data[0]
            
            # 24시간 거래량 조회
            volume_url = "https://api.upbit.com/v1/candles/days"
            volume_params = {'market': ticker, 'count': 1}
            volume_param_string = '&'.join([f"{k}={v}" for k, v in volume_params.items()])
            volume_full_url = f"{volume_url}?{volume_param_string}"
            
            volume_response = http.request('GET', volume_full_url)
            
            if volume_response.status == 200:
                volume_data = json.loads(volume_response.data.decode('utf-8'))
                daily_volume_krw = volume_data[0]['candle_acc_trade_price'] if volume_data else 0
            else:
                daily_volume_krw = 0
            
            market_data = {
                'ticker': ticker,
                'current_price': float(current_data['trade_price']),
                'change_rate': float(current_data.get('signed_change_rate', 0)) * 100,
                'volume_24h': float(current_data.get('acc_trade_volume_24h', 0)),
                'volume_24h_krw': daily_volume_krw,
                'high_price': float(current_data.get('high_price', 0)),
                'low_price': float(current_data.get('low_price', 0)),
                'timestamp': datetime.now(self.kst).isoformat()
            }
            
            logger.info(f"{ticker} 현재 시장 데이터 수집 완료")
            return market_data
            
        except Exception as e:
            logger.error(f"{ticker} 시장 데이터 수집 실패: {e}")
            return None

    def check_basic_conditions(self, market_data: Dict, phase4_data: Dict) -> Dict:
        """기본 거래 조건 검증"""
        try:
            ticker = market_data['ticker']
            current_price = market_data['current_price']
            volume_24h_krw = market_data['volume_24h_krw']
            
            conditions = {
                'price_range': False,
                'volume_requirement': False,
                'volatility_check': False,
                'reasons': []
            }
            
            # 1. 가격대 검증
            if (self.config['price_range']['min'] <= current_price <= 
                self.config['price_range']['max']):
                conditions['price_range'] = True
                conditions['reasons'].append(f"가격대 적정 ({current_price:,.0f}원)")
            else:
                conditions['reasons'].append(f"가격대 부적정 ({current_price:,.0f}원)")
            
            # 2. 거래량 검증
            if volume_24h_krw >= self.config['min_volume_krw']:
                conditions['volume_requirement'] = True
                conditions['reasons'].append(f"거래량 충족 ({volume_24h_krw/1e8:.1f}억원)")
            else:
                conditions['reasons'].append(f"거래량 부족 ({volume_24h_krw/1e8:.1f}억원)")
            
            # 3. 변동성 검증 (고가-저가 비율)
            high_price = market_data['high_price']
            low_price = market_data['low_price']
            
            if high_price > 0 and low_price > 0:
                daily_volatility = (high_price - low_price) / low_price
                if daily_volatility <= self.config['market_condition']['volatility_max']:
                    conditions['volatility_check'] = True
                    conditions['reasons'].append(f"변동성 적정 ({daily_volatility:.1%})")
                else:
                    conditions['reasons'].append(f"변동성 과도 ({daily_volatility:.1%})")
            else:
                conditions['volatility_check'] = True  # 데이터 없으면 통과
                conditions['reasons'].append("변동성 데이터 없음")
            
            # 전체 조건 통과 여부
            conditions['passed'] = all([
                conditions['price_range'],
                conditions['volume_requirement'],
                conditions['volatility_check']
            ])
            
            logger.info(f"{'✅' if conditions['passed'] else '❌'} {ticker} 기본 조건 검증")
            
            return conditions
            
        except Exception as e:
            logger.error(f"기본 조건 검증 실패: {e}")
            return {'passed': False, 'reasons': ['검증 오류']}

    def calculate_risk_metrics(self, market_data: Dict, phase4_data: Dict) -> Dict:
        """리스크 지표 계산"""
        try:
            ticker = market_data['ticker']
            current_price = market_data['current_price']
            
            # Phase 4에서 가져온 기술적 지표
            indicators = phase4_data.get('indicators', {})
            rsi = indicators.get('rsi', 50)
            ma200 = indicators.get('ma200', current_price)
            
            # 리스크 메트릭 계산
            risk_metrics = {
                'stop_loss_price': current_price * (1 - self.config['risk_management']['stop_loss_pct']),
                'take_profit_price': current_price * (1 + self.config['risk_management']['take_profit_pct']),
                'ma200_distance': ((current_price - ma200) / ma200) if ma200 > 0 else 0,
                'rsi_risk_level': 'HIGH' if rsi >= self.config['risk_management']['rsi_overbought'] else 'NORMAL',
                'position_risk_score': 0,  # 기본값
                'risk_reward_ratio': self.config['risk_management']['take_profit_pct'] / self.config['risk_management']['stop_loss_pct']
            }
            
            # 위치 리스크 점수 계산 (0-100)
            risk_score = 0
            
            # RSI 과매수 리스크
            if rsi >= 80:
                risk_score += 30
            elif rsi >= 70:
                risk_score += 15
            
            # MA200 거리 리스크
            ma_distance = abs(risk_metrics['ma200_distance'])
            if ma_distance > 0.3:  # 30% 이상 괴리
                risk_score += 25
            elif ma_distance > 0.2:  # 20% 이상 괴리
                risk_score += 15
            
            # 24시간 변동률 리스크
            change_rate = abs(market_data.get('change_rate', 0))
            if change_rate > 10:  # 10% 이상 변동
                risk_score += 20
            elif change_rate > 5:  # 5% 이상 변동
                risk_score += 10
            
            risk_metrics['position_risk_score'] = min(100, risk_score)
            
            logger.info(f"{ticker} 리스크 점수: {risk_score}/100")
            
            return risk_metrics
            
        except Exception as e:
            logger.error(f"리스크 메트릭 계산 실패: {e}")
            return {}

    def calculate_position_size(self, market_data: Dict, risk_metrics: Dict) -> Dict:
        """포지션 크기 계산"""
        try:
            ticker = market_data['ticker']
            current_price = market_data['current_price']
            risk_score = risk_metrics.get('position_risk_score', 50)
            
            # 기본 포지션 크기 (포트폴리오의 30%)
            base_position_pct = self.config['position_size_pct']
            
            # 리스크 점수에 따른 포지션 조정
            if risk_score >= 70:
                adjusted_position_pct = base_position_pct * 0.5  # 50% 감소
            elif risk_score >= 50:
                adjusted_position_pct = base_position_pct * 0.7  # 30% 감소
            elif risk_score >= 30:
                adjusted_position_pct = base_position_pct * 0.85 # 15% 감소
            else:
                adjusted_position_pct = base_position_pct  # 원래 크기
            
            # 최소/최대 포지션 크기 제한
            adjusted_position_pct = max(0.1, min(0.4, adjusted_position_pct))
            
            position_info = {
                'ticker': ticker,
                'base_position_pct': base_position_pct,
                'adjusted_position_pct': adjusted_position_pct,
                'risk_adjustment_factor': adjusted_position_pct / base_position_pct,
                'entry_price': current_price,
                'stop_loss_price': risk_metrics.get('stop_loss_price', 0),
                'take_profit_price': risk_metrics.get('take_profit_price', 0),
                'max_loss_pct': self.config['risk_management']['stop_loss_pct'],
                'expected_profit_pct': self.config['risk_management']['take_profit_pct']
            }
            
            logger.info(f"{ticker} 포지션 크기: {adjusted_position_pct:.1%} (위험도 조정: {adjusted_position_pct/base_position_pct:.1%})")
            
            return position_info
            
        except Exception as e:
            logger.error(f"포지션 크기 계산 실패: {e}")
            return {}

    def generate_final_signal(self, market_data: Dict, phase4_data: Dict, 
                            basic_conditions: Dict, risk_metrics: Dict, 
                            position_info: Dict) -> Dict:
        """최종 거래 신호 생성"""
        try:
            ticker = market_data['ticker']
            
            # 종합 점수 계산
            technical_score = phase4_data.get('score', 0)  # Phase 4 기술적 점수 (0-7)
            gpt_score = phase4_data.get('gpt_score', 0)    # GPT 점수 (0-10)
            gpt_confidence = phase4_data.get('gpt_confidence', 0)  # GPT 신뢰도 (0-10)
            
            # 정규화된 종합 점수 (0-100)
            composite_score = (
                (technical_score / 7.0) * 40 +     # 기술적 분석 40%
                (gpt_score / 10.0) * 35 +          # GPT 점수 35%
                (gpt_confidence / 10.0) * 25       # GPT 신뢰도 25%
            ) * 100
            
            # 리스크 조정 점수
            risk_score = risk_metrics.get('position_risk_score', 50)
            risk_adjusted_score = composite_score * (1 - risk_score / 200)  # 리스크에 따라 감점
            
            # 최종 신호 결정
            signal = 'HOLD'
            confidence = 0
            reasons = []
            
            # BUY 신호 조건
            if (basic_conditions.get('passed', False) and 
                risk_adjusted_score >= 70 and 
                risk_score <= 60):
                signal = 'BUY'
                confidence = min(95, risk_adjusted_score)
                reasons.append(f"종합점수 우수 ({risk_adjusted_score:.1f})")
                reasons.append(f"리스크 수용 가능 ({risk_score})")
            
            # STRONG_BUY 신호 조건 (더 엄격)
            elif (basic_conditions.get('passed', False) and 
                  risk_adjusted_score >= 85 and 
                  risk_score <= 40 and
                  technical_score >= 6 and
                  gpt_confidence >= 8):
                signal = 'STRONG_BUY'
                confidence = min(99, risk_adjusted_score)
                reasons.append(f"최고 등급 신호 ({risk_adjusted_score:.1f})")
                reasons.append(f"저위험 고신뢰도 ({risk_score}, {gpt_confidence})")
            
            # HOLD/REJECT 신호
            else:
                if not basic_conditions.get('passed', False):
                    signal = 'REJECT'
                    reasons.append("기본 조건 미충족")
                elif risk_score > 70:
                    signal = 'REJECT'
                    reasons.append(f"고위험 ({risk_score})")
                elif risk_adjusted_score < 50:
                    signal = 'REJECT'
                    reasons.append(f"낮은 종합점수 ({risk_adjusted_score:.1f})")
                else:
                    signal = 'HOLD'
                    reasons.append("관망 권장")
                
                confidence = max(10, 100 - risk_adjusted_score)
            
            # 최종 신호 구성
            final_signal = {
                'ticker': ticker,
                'signal': signal,
                'confidence': confidence,
                'composite_score': composite_score,
                'risk_adjusted_score': risk_adjusted_score,
                'technical_score': technical_score,
                'gpt_score': gpt_score,
                'gpt_confidence': gpt_confidence,
                'risk_score': risk_score,
                'reasons': reasons,
                'market_data': market_data,
                'position_info': position_info,
                'risk_metrics': risk_metrics,
                'basic_conditions': basic_conditions,
                'analysis_timestamp': datetime.now(self.kst).isoformat(),
                'phase4_data': phase4_data
            }
            
            signal_emoji = {
                'STRONG_BUY': '🚀',
                'BUY': '✅',
                'HOLD': '⏳',
                'REJECT': '❌'
            }
            
            logger.info(f"{signal_emoji.get(signal, '❓')} {ticker} 최종 신호: {signal} (신뢰도: {confidence:.0f}%)")
            
            return final_signal
            
        except Exception as e:
            logger.error(f"최종 신호 생성 실패: {e}")
            return None

    def process_ticker(self, phase4_data: Dict) -> Optional[Dict]:
        """개별 종목 최종 조건 검증"""
        try:
            ticker = phase4_data.get('ticker', '')
            logger.info(f"최종 조건 검증 시작: {ticker}")
            
            # 1. 현재 시장 데이터 수집
            market_data = self.fetch_current_market_data(ticker)
            if not market_data:
                return None
            
            # API 호출 간격 조절
            time.sleep(0.3)
            
            # 2. 기본 조건 검증
            basic_conditions = self.check_basic_conditions(market_data, phase4_data)
            
            # 3. 리스크 메트릭 계산
            risk_metrics = self.calculate_risk_metrics(market_data, phase4_data)
            
            # 4. 포지션 크기 계산
            position_info = self.calculate_position_size(market_data, risk_metrics)
            
            # 5. 최종 신호 생성
            final_signal = self.generate_final_signal(
                market_data, phase4_data, basic_conditions, 
                risk_metrics, position_info
            )
            
            return final_signal
            
        except Exception as e:
            logger.error(f"종목 처리 실패: {e}")
            return None

    def process_all_tickers(self, phase4_results: List[Dict]) -> List[Dict]:
        """모든 종목 최종 조건 검증"""
        final_signals = []
        
        logger.info(f"최종 조건 검증 시작: {len(phase4_results)}개 종목")
        
        for i, phase4_data in enumerate(phase4_results):
            try:
                # API 호출 간격 조절
                if i > 0:
                    time.sleep(0.5)
                
                signal = self.process_ticker(phase4_data)
                if signal:
                    final_signals.append(signal)
                
                logger.info(f"진행 상황: {i+1}/{len(phase4_results)}")
                
            except Exception as e:
                logger.error(f"종목 처리 오류: {e}")
                continue
        
        # 신호별 정렬 (STRONG_BUY > BUY > HOLD > REJECT)
        signal_priority = {'STRONG_BUY': 4, 'BUY': 3, 'HOLD': 2, 'REJECT': 1}
        final_signals.sort(
            key=lambda x: (
                signal_priority.get(x.get('signal', 'REJECT'), 0),
                x.get('confidence', 0)
            ),
            reverse=True
        )
        
        # 통계 정보
        signal_counts = {}
        for signal in final_signals:
            sig = signal.get('signal', 'UNKNOWN')
            signal_counts[sig] = signal_counts.get(sig, 0) + 1
        
        logger.info(f"최종 조건 검증 완료: {signal_counts}")
        return final_signals

    def save_results_to_s3(self, final_signals: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            # BUY 이상 신호만 추출
            buy_signals = [s for s in final_signals if s.get('signal') in ['BUY', 'STRONG_BUY']]
            
            # 신호별 통계
            signal_stats = {}
            for signal in final_signals:
                sig = signal.get('signal', 'UNKNOWN')
                signal_stats[sig] = signal_stats.get(sig, 0) + 1
            
            output_data = {
                'phase': 'condition_check',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'processed_count': len(final_signals),
                'buy_signal_count': len(buy_signals),
                'final_signals': final_signals,
                'buy_candidates': [s['ticker'] for s in buy_signals],
                'signal_statistics': signal_stats,
                'config': self.config,
                'summary': {
                    'total_processed': len(final_signals),
                    'strong_buy': signal_stats.get('STRONG_BUY', 0),
                    'buy': signal_stats.get('BUY', 0),
                    'hold': signal_stats.get('HOLD', 0),
                    'reject': signal_stats.get('REJECT', 0),
                    'avg_confidence': sum(s.get('confidence', 0) for s in buy_signals) / len(buy_signals) if buy_signals else 0,
                    'top_pick': buy_signals[0]['ticker'] if buy_signals else None
                }
            }
            
            # 메인 결과 파일
            main_key = 'phase5/condition_check_results.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            # 백업 파일
            backup_key = f'phase5/backups/condition_check_{timestamp}.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=backup_key,
                Body=json.dumps(output_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            logger.info(f"결과 S3 저장 완료: {main_key}")
            return True
            
        except Exception as e:
            logger.error(f"S3 저장 실패: {e}")
            return False

    def trigger_execution_phase(self):
        """거래 실행 단계 트리거 이벤트 발송"""
        try:
            event_detail = {
                'phase': 'condition_check',
                'status': 'completed',
                'timestamp': datetime.now(self.kst).isoformat(),
                'next_phase': 'execution'
            }
            
            self.events_client.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.condition_check',
                        'DetailType': 'Condition Check Completed',
                        'Detail': json.dumps(event_detail)
                    }
                ]
            )
            
            logger.info("거래 실행 단계 트리거 이벤트 발송 완료")
            
        except Exception as e:
            logger.error(f"거래 실행 트리거 실패: {e}")

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 5: Condition Check 시작 ===")
        logger.info(f"이벤트: {json.dumps(event)}")
        
        # 조건 검증기 초기화
        checker = ConditionChecker()
        
        # Phase 4 데이터 로드
        phase4_data = checker.load_phase4_data()
        if not phase4_data:
            return {
                'statusCode': 400,
                'phase': 'condition_check',
                'error': 'Phase 4 데이터 없음',
                'message': 'Phase 4를 먼저 실행해주세요'
            }
        
        # 최종 조건 검증 실행
        final_signals = checker.process_all_tickers(phase4_data)
        
        # 결과 저장
        s3_saved = checker.save_results_to_s3(final_signals)
        
        # BUY 신호가 있으면 거래 실행 단계 트리거
        buy_signals = [s for s in final_signals if s.get('signal') in ['BUY', 'STRONG_BUY']]
        if buy_signals and s3_saved:
            checker.trigger_execution_phase()
        
        # 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 신호 통계
        signal_stats = {}
        for signal in final_signals:
            sig = signal.get('signal', 'UNKNOWN')
            signal_stats[sig] = signal_stats.get(sig, 0) + 1
        
        # 성공 응답
        response = {
            'statusCode': 200,
            'phase': 'condition_check',
            'input_tickers': len(phase4_data),
            'processed_tickers': len(final_signals),
            'buy_signals': len(buy_signals),
            'signal_statistics': signal_stats,
            'buy_candidates': [s['ticker'] for s in buy_signals],
            'top_pick': buy_signals[0]['ticker'] if buy_signals else None,
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"=== Phase 5 완료 ===")
        logger.info(f"결과: {response}")
        
        return response
        
    except Exception as e:
        logger.error(f"Phase 5 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': 'condition_check',
            'error': str(e),
            'message': 'Phase 5 실행 중 오류 발생'
        }