#!/usr/bin/env python3
"""
⚡ Phase 6: 거래 실행 및 포지션 관리 Lambda
- Phase 5 최종 검사 결과를 받아 실제 업비트 거래 실행
- 실시간 주문 체결 모니터링 및 오류 처리
- 포지션 관리: 손절/익절 자동 실행
- DynamoDB를 통한 거래 기록 및 포지션 추적
- SNS를 통한 거래 알림 발송
"""

import boto3
import json
import logging
import pandas as pd
import numpy as np
import pytz
import os
import time
import hashlib
import hmac
import base64
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN
import warnings
warnings.filterwarnings('ignore')

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class TradeExecutionPhase6:
    """거래 실행 및 포지션 관리 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb')
        self.sns_client = boto3.client('sns')
        self.secrets_client = boto3.client('secretsmanager')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')
        
        # DynamoDB 테이블
        self.trades_table = self.dynamodb.Table(os.environ.get('TRADES_TABLE', 'makenaide-trades'))
        self.positions_table = self.dynamodb.Table(os.environ.get('POSITIONS_TABLE', 'makenaide-positions'))
        
        # 업비트 API 설정
        self.upbit_api_url = "https://api.upbit.com"
        self.access_key, self.secret_key = self._get_upbit_credentials()
        
        # 거래 실행 설정
        self.execution_config = {
            'max_retry_attempts': int(os.environ.get('MAX_RETRY_ATTEMPTS', '3')),
            'retry_delay_seconds': int(os.environ.get('RETRY_DELAY', '5')),
            'order_timeout_seconds': int(os.environ.get('ORDER_TIMEOUT', '30')),
            'slippage_tolerance_pct': float(os.environ.get('SLIPPAGE_TOLERANCE', '1.0')),
            'min_order_amount': int(os.environ.get('MIN_ORDER_AMOUNT', '5000')),  # 최소 주문 금액
            'price_precision': int(os.environ.get('PRICE_PRECISION', '0')),  # 가격 소수점
            'volume_precision': int(os.environ.get('VOLUME_PRECISION', '8'))   # 수량 소수점
        }
        
        # SNS 토픽
        self.sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')

    def _get_upbit_credentials(self) -> Tuple[str, str]:
        """AWS Secrets Manager에서 업비트 API 키 조회"""
        try:
            secret_response = self.secrets_client.get_secret_value(SecretId='makenaide-upbit-api-keys')
            secret_data = json.loads(secret_response['SecretString'])
            return secret_data['access_key'], secret_data['secret_key']
        except Exception as e:
            logger.error(f"❌ 업비트 API 키 조회 실패: {e}")
            return "", ""

    def _generate_upbit_headers(self, query_params: str = "") -> Dict[str, str]:
        """업비트 API 인증 헤더 생성"""
        try:
            payload = {
                'access_key': self.access_key,
                'nonce': str(int(time.time() * 1000))
            }
            
            if query_params:
                payload['query_hash'] = hashlib.sha512(query_params.encode()).hexdigest()
                payload['query_hash_alg'] = 'SHA512'
            
            jwt_token = base64.b64encode(json.dumps(payload).encode()).decode()
            authorize_token = f"Bearer {jwt_token}"
            
            signature = hmac.new(
                self.secret_key.encode(),
                jwt_token.encode(),
                hashlib.sha512
            ).hexdigest()
            
            return {
                'Authorization': f"{authorize_token}.{signature}",
                'Content-Type': 'application/json'
            }
        except Exception as e:
            logger.error(f"❌ 업비트 인증 헤더 생성 실패: {e}")
            return {}

    def load_phase5_signals(self) -> Optional[List[Dict[str, Any]]]:
        """Phase 5 최종 검사에서 실행할 거래 신호들 로드"""
        try:
            logger.info("📊 Phase 5 최종 검사 결과 로드 중...")
            
            # 최신 Phase 5 결과 파일 찾기
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix='phase5/final_trade_signals_'
            )
            
            if 'Contents' not in response or not response['Contents']:
                logger.warning("Phase 5 결과 파일이 없습니다")
                return None
            
            # 가장 최신 파일 선택
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=latest_file['Key']
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            trade_signals = data.get('final_trade_signals', [])
            
            # BUY 액션 신호만 필터링
            executable_trades = [
                signal for signal in trade_signals
                if signal.get('action') == 'BUY'
            ]
            
            if not executable_trades:
                logger.warning("Phase 5에서 실행할 거래 신호가 없습니다")
                return None
                
            logger.info(f"✅ Phase 5 거래 신호 로드 완료: {len(executable_trades)}개 실행 예정")
            return executable_trades
            
        except Exception as e:
            logger.error(f"❌ Phase 5 데이터 로드 실패: {e}")
            return None

    def get_account_balance(self) -> Dict[str, Any]:
        """업비트 계좌 잔고 조회"""
        try:
            logger.info("💰 계좌 잔고 조회 중...")
            
            headers = self._generate_upbit_headers()
            response = requests.get(f"{self.upbit_api_url}/v1/accounts", headers=headers)
            
            if response.status_code != 200:
                logger.error(f"❌ 잔고 조회 실패: {response.status_code} - {response.text}")
                return {}
            
            accounts = response.json()
            
            # KRW 잔고 및 보유 코인 정리
            krw_balance = 0
            coin_balances = {}
            
            for account in accounts:
                currency = account['currency']
                balance = float(account['balance'])
                locked = float(account['locked'])
                
                if currency == 'KRW':
                    krw_balance = balance
                elif balance > 0:
                    coin_balances[currency] = {
                        'balance': balance,
                        'locked': locked,
                        'available': balance - locked
                    }
            
            account_info = {
                'krw_balance': krw_balance,
                'coin_balances': coin_balances,
                'total_coins': len(coin_balances),
                'query_time': datetime.now(self.kst).isoformat()
            }
            
            logger.info(f"✅ 잔고 조회 완료: KRW {krw_balance:,.0f}원, 보유코인 {len(coin_balances)}개")
            return account_info
            
        except Exception as e:
            logger.error(f"❌ 계좌 잔고 조회 실패: {e}")
            return {}

    def calculate_order_details(self, ticker: str, signal_data: Dict, account_balance: Dict) -> Dict[str, Any]:
        """주문 상세 정보 계산"""
        try:
            order_details = {
                'ticker': ticker,
                'calculation_time': datetime.now(self.kst).isoformat(),
                'order_valid': False,
                'order_amount_krw': 0,
                'order_price': 0,
                'order_volume': 0,
                'order_type': 'LIMIT'
            }
            
            # 포지션 데이터에서 정보 추출
            position_data = signal_data.get('position_data', {})
            execution_conditions = signal_data.get('execution_conditions', {})
            
            recommended_amount = position_data.get('recommended_position_krw', 0)
            krw_balance = account_balance.get('krw_balance', 0)
            
            # 사용 가능 금액 확인
            if recommended_amount > krw_balance:
                recommended_amount = krw_balance * 0.95  # 잔고의 95%까지만 사용
                logger.warning(f"⚠️ {ticker} 추천 금액이 잔고 초과, 조정: {recommended_amount:,.0f}원")
            
            if recommended_amount < self.execution_config['min_order_amount']:
                order_details['error'] = f"주문 금액 부족: {recommended_amount:,.0f}원"
                return order_details
            
            # 현재 시세 조회
            ticker_info = requests.get(f"{self.upbit_api_url}/v1/ticker?markets={ticker}").json()[0]
            current_price = float(ticker_info['trade_price'])
            
            # 주문 가격 결정
            execution_type = signal_data.get('execution_type', 'LIMIT')
            price_range = execution_conditions.get('price_range', {})
            
            if execution_type == 'MARKET':
                order_price = current_price  # 시장가는 참고용
                order_details['order_type'] = 'MARKET'
            else:
                # 지정가 주문: 현재가의 0.2% 위
                order_price = current_price * 1.002
                order_details['order_type'] = 'LIMIT'
                
                # 가격 범위 제한 확인
                if price_range:
                    max_price = price_range.get('max', order_price)
                    min_price = price_range.get('min', 0)
                    order_price = min(max(order_price, min_price), max_price)
            
            # 가격 정밀도 조정 (업비트 호가 단위)
            order_price = self._adjust_price_precision(order_price)
            
            # 주문 수량 계산
            if execution_type == 'MARKET':
                # 시장가 매수는 금액으로 주문
                order_volume = 0
                order_amount_krw = recommended_amount
            else:
                # 지정가 매수는 수량으로 주문
                order_volume = recommended_amount / order_price
                order_volume = self._adjust_volume_precision(order_volume)
                order_amount_krw = order_volume * order_price
            
            # 최종 검증
            if order_amount_krw >= self.execution_config['min_order_amount'] and order_amount_krw <= krw_balance:
                order_details.update({
                    'order_valid': True,
                    'order_amount_krw': round(order_amount_krw, 0),
                    'order_price': order_price,
                    'order_volume': order_volume,
                    'current_market_price': current_price,
                    'expected_slippage_pct': ((order_price - current_price) / current_price) * 100
                })
            else:
                order_details['error'] = "주문 금액 검증 실패"
            
            logger.info(f"💰 {ticker} 주문 상세 계산 완료: {order_amount_krw:,.0f}원, {order_price:,.0f}원")
            return order_details
            
        except Exception as e:
            logger.error(f"❌ {ticker} 주문 상세 계산 실패: {e}")
            return {
                'ticker': ticker,
                'calculation_time': datetime.now(self.kst).isoformat(),
                'order_valid': False,
                'error': str(e)
            }

    def _adjust_price_precision(self, price: float) -> float:
        """업비트 호가 단위에 맞게 가격 조정"""
        if price >= 2000000:      # 200만원 이상: 1000원 단위
            return round(price, -3)
        elif price >= 1000000:    # 100만원 이상: 500원 단위
            return round(price / 500) * 500
        elif price >= 500000:     # 50만원 이상: 100원 단위
            return round(price, -2)
        elif price >= 100000:     # 10만원 이상: 50원 단위
            return round(price / 50) * 50
        elif price >= 10000:      # 1만원 이상: 10원 단위
            return round(price, -1)
        elif price >= 1000:       # 1000원 이상: 5원 단위
            return round(price / 5) * 5
        elif price >= 100:        # 100원 이상: 1원 단위
            return round(price)
        elif price >= 10:         # 10원 이상: 0.1원 단위
            return round(price, 1)
        else:                     # 10원 미만: 0.01원 단위
            return round(price, 2)

    def _adjust_volume_precision(self, volume: float) -> float:
        """거래량 정밀도 조정"""
        return round(volume, self.execution_config['volume_precision'])

    def execute_buy_order(self, ticker: str, order_details: Dict) -> Dict[str, Any]:
        """매수 주문 실행"""
        try:
            logger.info(f"🛒 {ticker} 매수 주문 실행 중...")
            
            order_result = {
                'ticker': ticker,
                'execution_time': datetime.now(self.kst).isoformat(),
                'success': False,
                'order_uuid': '',
                'executed_price': 0,
                'executed_volume': 0,
                'executed_amount': 0,
                'order_status': 'FAILED'
            }
            
            # 주문 파라미터 준비
            order_params = {
                'market': ticker,
                'side': 'bid',  # 매수
            }
            
            if order_details['order_type'] == 'MARKET':
                # 시장가 매수
                order_params['ord_type'] = 'price'
                order_params['price'] = str(int(order_details['order_amount_krw']))
            else:
                # 지정가 매수
                order_params['ord_type'] = 'limit'
                order_params['volume'] = str(order_details['order_volume'])
                order_params['price'] = str(int(order_details['order_price']))
            
            # 쿼리 문자열 생성
            query_string = "&".join([f"{key}={value}" for key, value in order_params.items()])
            
            # API 호출
            headers = self._generate_upbit_headers(query_string)
            response = requests.post(
                f"{self.upbit_api_url}/v1/orders",
                json=order_params,
                headers=headers,
                timeout=self.execution_config['order_timeout_seconds']
            )
            
            if response.status_code == 201:
                # 주문 성공
                order_response = response.json()
                order_uuid = order_response['uuid']
                
                logger.info(f"✅ {ticker} 주문 접수 성공: {order_uuid}")
                
                # 주문 체결 대기 및 모니터링
                execution_result = self._monitor_order_execution(order_uuid, ticker)
                
                order_result.update({
                    'success': True,
                    'order_uuid': order_uuid,
                    'raw_response': order_response,
                    'execution_result': execution_result
                })
                
                # 체결 정보 업데이트
                if execution_result.get('filled', False):
                    order_result.update({
                        'executed_price': execution_result.get('avg_price', 0),
                        'executed_volume': execution_result.get('executed_volume', 0),
                        'executed_amount': execution_result.get('executed_amount', 0),
                        'order_status': 'FILLED'
                    })
                else:
                    order_result['order_status'] = 'PARTIAL' if execution_result.get('partial_filled', False) else 'PENDING'
                
            else:
                # 주문 실패
                error_message = response.text
                logger.error(f"❌ {ticker} 주문 실패: {response.status_code} - {error_message}")
                order_result['error'] = error_message
            
            return order_result
            
        except Exception as e:
            logger.error(f"❌ {ticker} 매수 주문 실행 실패: {e}")
            return {
                'ticker': ticker,
                'execution_time': datetime.now(self.kst).isoformat(),
                'success': False,
                'error': str(e),
                'order_status': 'ERROR'
            }

    def _monitor_order_execution(self, order_uuid: str, ticker: str) -> Dict[str, Any]:
        """주문 체결 모니터링"""
        try:
            logger.info(f"👀 {ticker} 주문 체결 모니터링 중: {order_uuid}")
            
            for attempt in range(self.execution_config['max_retry_attempts']):
                # 주문 상태 조회
                query_params = f"uuid={order_uuid}"
                headers = self._generate_upbit_headers(query_params)
                
                response = requests.get(
                    f"{self.upbit_api_url}/v1/order?uuid={order_uuid}",
                    headers=headers
                )
                
                if response.status_code == 200:
                    order_info = response.json()
                    state = order_info.get('state', 'wait')
                    
                    if state == 'done':
                        # 주문 완전 체결
                        return {
                            'filled': True,
                            'partial_filled': False,
                            'avg_price': float(order_info.get('price', 0)),
                            'executed_volume': float(order_info.get('executed_volume', 0)),
                            'executed_amount': float(order_info.get('paid_fee', 0)) + float(order_info.get('executed_volume', 0)) * float(order_info.get('price', 0)),
                            'fee': float(order_info.get('paid_fee', 0)),
                            'order_info': order_info
                        }
                    
                    elif state == 'cancel':
                        # 주문 취소됨
                        return {
                            'filled': False,
                            'cancelled': True,
                            'partial_filled': float(order_info.get('executed_volume', 0)) > 0,
                            'executed_volume': float(order_info.get('executed_volume', 0)),
                            'order_info': order_info
                        }
                    
                    else:
                        # 주문 대기 중
                        executed_volume = float(order_info.get('executed_volume', 0))
                        remaining_volume = float(order_info.get('remaining_volume', 0))
                        
                        if executed_volume > 0:
                            logger.info(f"📊 {ticker} 부분 체결: {executed_volume}")
                            return {
                                'filled': False,
                                'partial_filled': True,
                                'executed_volume': executed_volume,
                                'remaining_volume': remaining_volume,
                                'order_info': order_info
                            }
                
                # 재시도 대기
                if attempt < self.execution_config['max_retry_attempts'] - 1:
                    time.sleep(self.execution_config['retry_delay_seconds'])
            
            # 모니터링 타임아웃
            return {
                'filled': False,
                'timeout': True,
                'message': '주문 모니터링 타임아웃'
            }
            
        except Exception as e:
            logger.error(f"❌ 주문 모니터링 실패: {e}")
            return {
                'filled': False,
                'error': str(e)
            }

    def save_trade_record(self, trade_data: Dict) -> bool:
        """거래 기록을 DynamoDB에 저장"""
        try:
            trade_id = f"{trade_data['ticker']}_{int(datetime.now().timestamp())}"
            
            trade_record = {
                'trade_id': trade_id,
                'ticker': trade_data['ticker'],
                'action': 'BUY',
                'execution_time': trade_data.get('execution_time'),
                'order_uuid': trade_data.get('order_uuid', ''),
                'order_type': trade_data.get('order_type', 'LIMIT'),
                'order_status': trade_data.get('order_status', 'UNKNOWN'),
                'executed_price': Decimal(str(trade_data.get('executed_price', 0))),
                'executed_volume': Decimal(str(trade_data.get('executed_volume', 0))),
                'executed_amount': Decimal(str(trade_data.get('executed_amount', 0))),
                'fee': Decimal(str(trade_data.get('fee', 0))),
                'success': trade_data.get('success', False),
                'phase_chain': 'phase1-6',
                'created_at': datetime.now(self.kst).isoformat()
            }
            
            # 오류 정보 추가
            if not trade_data.get('success', False) and 'error' in trade_data:
                trade_record['error_message'] = str(trade_data['error'])
            
            self.trades_table.put_item(Item=trade_record)
            logger.info(f"✅ {trade_data['ticker']} 거래 기록 저장 완료: {trade_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 거래 기록 저장 실패: {e}")
            return False

    def update_position(self, ticker: str, trade_data: Dict, signal_data: Dict) -> bool:
        """포지션 정보 업데이트"""
        try:
            if not trade_data.get('success', False) or trade_data.get('order_status') != 'FILLED':
                return False
            
            position_data = signal_data.get('position_data', {})
            
            position_record = {
                'position_id': f"{ticker}_{int(datetime.now().timestamp())}",
                'ticker': ticker,
                'position_type': 'LONG',
                'entry_price': Decimal(str(trade_data.get('executed_price', 0))),
                'entry_volume': Decimal(str(trade_data.get('executed_volume', 0))),
                'entry_amount': Decimal(str(trade_data.get('executed_amount', 0))),
                'current_price': Decimal(str(trade_data.get('executed_price', 0))),
                'stop_loss_price': Decimal(str(position_data.get('stop_loss_price', 0))),
                'take_profit_price': Decimal(str(position_data.get('take_profit_price', 0))),
                'unrealized_pnl': Decimal('0'),
                'status': 'ACTIVE',
                'entry_time': trade_data.get('execution_time'),
                'last_updated': datetime.now(self.kst).isoformat(),
                'trade_uuid': trade_data.get('order_uuid', ''),
                'phase_origin': 'phase1-6'
            }
            
            self.positions_table.put_item(Item=position_record)
            logger.info(f"✅ {ticker} 포지션 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ {ticker} 포지션 업데이트 실패: {e}")
            return False

    def send_trade_notification(self, trade_data: Dict, signal_data: Dict) -> bool:
        """거래 알림 발송"""
        try:
            if not self.sns_topic_arn:
                logger.warning("SNS 토픽이 설정되지 않음")
                return False
            
            ticker = trade_data['ticker']
            success = trade_data.get('success', False)
            
            if success and trade_data.get('order_status') == 'FILLED':
                # 성공 알림
                message = f"""
🎉 Makenaide 매수 체결 알림

📈 종목: {ticker}
💰 체결가: {trade_data.get('executed_price', 0):,.0f}원
📊 수량: {trade_data.get('executed_volume', 0):.6f}
💵 체결금액: {trade_data.get('executed_amount', 0):,.0f}원
🕐 체결시간: {trade_data.get('execution_time')}

📋 Phase 분석 정보:
- GPT 신뢰도: {signal_data.get('original_analysis', {}).get('phase3_confidence', 0)}%
- 4H 타이밍점수: {signal_data.get('original_analysis', {}).get('phase4_timing_score', 0):.1f}%
- 최종 우선순위: {signal_data.get('priority', 'unknown')}

🎯 목표 설정:
- 손절가: {signal_data.get('position_data', {}).get('stop_loss_price', 0):,.0f}원
- 익절가: {signal_data.get('position_data', {}).get('take_profit_price', 0):,.0f}원

주문ID: {trade_data.get('order_uuid', '')}
"""
                subject = f"[Makenaide] {ticker} 매수 체결"
                
            else:
                # 실패 알림
                message = f"""
❌ Makenaide 매수 실패 알림

📈 종목: {ticker}
🚫 상태: {trade_data.get('order_status', 'FAILED')}
🕐 시도시간: {trade_data.get('execution_time')}
❗ 오류: {trade_data.get('error', '알 수 없는 오류')}

📋 시도한 주문 정보:
- 주문금액: {signal_data.get('position_data', {}).get('recommended_position_krw', 0):,.0f}원
- 주문유형: {signal_data.get('execution_type', 'UNKNOWN')}
- 우선순위: {signal_data.get('priority', 'unknown')}
"""
                subject = f"[Makenaide] {ticker} 매수 실패"
            
            self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=subject,
                Message=message
            )
            
            logger.info(f"✅ {ticker} 거래 알림 발송 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 거래 알림 발송 실패: {e}")
            return False

    def execute_trade_signals(self, signals: List[Dict]) -> List[Dict[str, Any]]:
        """거래 신호들 실행"""
        try:
            logger.info(f"🚀 거래 실행 시작: {len(signals)}개 신호")
            
            # 계좌 잔고 조회
            account_balance = self.get_account_balance()
            if not account_balance or account_balance.get('krw_balance', 0) < self.execution_config['min_order_amount']:
                logger.error("❌ 거래 가능 잔고 부족")
                return []
            
            execution_results = []
            
            # 우선순위별 정렬 (high > medium > low)
            priority_order = {'high': 0, 'medium': 1, 'low': 2}
            sorted_signals = sorted(signals, key=lambda x: priority_order.get(x.get('priority', 'low'), 2))
            
            for idx, signal in enumerate(sorted_signals):
                ticker = signal.get('ticker')
                if not ticker:
                    continue
                
                try:
                    logger.info(f"🎯 {ticker} 거래 실행 중... ({idx+1}/{len(signals)})")
                    
                    # 1. 주문 상세 계산
                    order_details = self.calculate_order_details(ticker, signal, account_balance)
                    
                    if not order_details.get('order_valid', False):
                        logger.warning(f"⚠️ {ticker} 주문 조건 미달: {order_details.get('error', '알 수 없는 오류')}")
                        execution_results.append({
                            'ticker': ticker,
                            'success': False,
                            'error': order_details.get('error'),
                            'order_status': 'INVALID'
                        })
                        continue
                    
                    # 2. 매수 주문 실행
                    trade_result = self.execute_buy_order(ticker, order_details)
                    
                    # 3. 거래 기록 저장
                    if trade_result.get('success', False):
                        self.save_trade_record(trade_result)
                        self.update_position(ticker, trade_result, signal)
                    
                    # 4. 알림 발송
                    self.send_trade_notification(trade_result, signal)
                    
                    execution_results.append(trade_result)
                    
                    # 5. 잔고 업데이트 (체결된 경우)
                    if trade_result.get('success', False) and trade_result.get('order_status') == 'FILLED':
                        executed_amount = trade_result.get('executed_amount', 0)
                        account_balance['krw_balance'] -= executed_amount
                        logger.info(f"💰 잔고 업데이트: -{executed_amount:,.0f}원, 잔여 {account_balance['krw_balance']:,.0f}원")
                        
                        # 추가 거래 불가능한 경우 중단
                        if account_balance['krw_balance'] < self.execution_config['min_order_amount']:
                            logger.warning("⚠️ 거래 가능 잔고 소진, 추가 거래 중단")
                            break
                    
                    # 거래간 간격 (API 제한 고려)
                    if idx < len(sorted_signals) - 1:
                        time.sleep(2)
                
                except Exception as e:
                    logger.error(f"❌ {ticker} 개별 거래 실행 실패: {e}")
                    execution_results.append({
                        'ticker': ticker,
                        'success': False,
                        'error': str(e),
                        'order_status': 'ERROR'
                    })
                    continue
            
            logger.info(f"🎯 거래 실행 완료: {len(execution_results)}개 처리")
            return execution_results
            
        except Exception as e:
            logger.error(f"❌ 거래 신호 실행 실패: {e}")
            return []

    def save_execution_summary(self, execution_results: List[Dict]) -> bool:
        """실행 결과 요약을 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            # 결과 통계
            total_executed = len(execution_results)
            successful_trades = len([r for r in execution_results if r.get('success', False)])
            filled_trades = len([r for r in execution_results if r.get('order_status') == 'FILLED'])
            total_amount = sum([r.get('executed_amount', 0) for r in execution_results])
            
            summary_data = {
                'phase': 'trade_execution',
                'status': 'completed',
                'execution_timestamp': timestamp,
                'execution_config': self.execution_config,
                'summary': {
                    'total_signals': total_executed,
                    'successful_orders': successful_trades,
                    'filled_orders': filled_trades,
                    'failed_orders': total_executed - successful_trades,
                    'total_executed_amount': total_amount,
                    'execution_rate': (successful_trades / total_executed * 100) if total_executed > 0 else 0,
                    'fill_rate': (filled_trades / successful_trades * 100) if successful_trades > 0 else 0
                },
                'execution_results': execution_results,
                'completed_at': datetime.now(self.kst).isoformat()
            }
            
            # S3에 저장
            key = f'phase6/trade_execution_results_{timestamp}.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json.dumps(summary_data, indent=2, ensure_ascii=False),
                ContentType='application/json'
            )
            
            logger.info(f"✅ 실행 요약 S3 저장 완료: s3://{self.s3_bucket}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 실행 요약 저장 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    try:
        logger.info("🚀 Phase 6 Trade Execution 시작")
        logger.info(f"📥 입력 이벤트: {json.dumps(event, indent=2, ensure_ascii=False)}")
        
        executor = TradeExecutionPhase6()
        
        # 1. Phase 5 거래 신호들 로드
        trade_signals = executor.load_phase5_signals()
        if not trade_signals:
            logger.error("❌ Phase 5 거래 신호가 없습니다")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Phase 5 거래 신호 없음'})
            }
        
        # 2. 거래 실행
        execution_results = executor.execute_trade_signals(trade_signals)
        
        if not execution_results:
            logger.warning("⚠️ 거래 실행 결과가 없습니다")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'completed',
                    'executed_trades': 0,
                    'message': '거래 실행 결과 없음'
                })
            }
        
        # 3. 실행 요약 저장
        save_success = executor.save_execution_summary(execution_results)
        
        # 4. 최종 결과 반환
        successful_trades = len([r for r in execution_results if r.get('success', False)])
        filled_trades = len([r for r in execution_results if r.get('order_status') == 'FILLED'])
        total_amount = sum([r.get('executed_amount', 0) for r in execution_results])
        
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'completed',
                'execution_summary': {
                    'total_signals': len(trade_signals),
                    'executed_orders': len(execution_results),
                    'successful_orders': successful_trades,
                    'filled_orders': filled_trades,
                    'total_executed_amount': total_amount,
                    'execution_rate_pct': (successful_trades / len(execution_results) * 100) if execution_results else 0,
                    'fill_rate_pct': (filled_trades / successful_trades * 100) if successful_trades > 0 else 0
                },
                'successful_trades': [
                    {
                        'ticker': r['ticker'],
                        'executed_price': r.get('executed_price', 0),
                        'executed_amount': r.get('executed_amount', 0),
                        'order_status': r.get('order_status', 'UNKNOWN')
                    } for r in execution_results if r.get('success', False)
                ],
                'failed_trades': [
                    {
                        'ticker': r['ticker'],
                        'error': r.get('error', '알 수 없는 오류'),
                        'order_status': r.get('order_status', 'FAILED')
                    } for r in execution_results if not r.get('success', False)
                ],
                's3_summary_saved': save_success
            }, ensure_ascii=False, indent=2)
        }
        
        logger.info(f"✅ Phase 6 완료: {filled_trades}개 체결, 총 {total_amount:,.0f}원")
        return result
        
    except Exception as e:
        logger.error(f"❌ Phase 6 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'status': 'failed'
            })
        }

if __name__ == "__main__":
    # 로컬 테스트용 (실제 거래 X)
    test_event = {
        'source': 'makenaide.phase5',
        'detail-type': 'Phase 5 Final Check Completed'
    }
    
    # 주의: 로컬 테스트 시 실제 거래가 발생하지 않도록 확인 필요
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))