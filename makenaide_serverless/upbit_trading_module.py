#!/usr/bin/env python3
"""
업비트 API 연동 모듈
JWT 인증 기반 실제 거래 실행 시스템
"""

import os
import jwt
import uuid
import hashlib
import requests
import json
import time
import boto3
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UpbitAPIClient:
    """
    업비트 API 클라이언트
    JWT 인증 및 모든 거래 기능 제공
    """
    
    def __init__(self, access_key: str = None, secret_key: str = None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.base_url = "https://api.upbit.com"
        self.session = requests.Session()
        
        # AWS 클라이언트 초기화
        self.dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
        self.secretsmanager = boto3.client('secretsmanager', region_name='ap-northeast-2')
        self.sns = boto3.client('sns', region_name='ap-northeast-2')
        
        # 테이블 참조
        self.trades_table = self.dynamodb.Table('makenaide-trades')
        self.positions_table = self.dynamodb.Table('makenaide-positions')
        self.params_table = self.dynamodb.Table('makenaide-trading-params')
        
        # API 키가 제공되지 않으면 Secrets Manager에서 로드
        if not self.access_key or not self.secret_key:
            self.load_api_credentials()
    
    def load_api_credentials(self):
        """
        AWS Secrets Manager에서 업비트 API 키 로드
        """
        try:
            secret_response = self.secretsmanager.get_secret_value(
                SecretId='upbit-api-keys'
            )
            
            credentials = json.loads(secret_response['SecretString'])
            self.access_key = credentials['access_key']
            self.secret_key = credentials['secret_key']
            
            logger.info("✅ API credentials loaded from Secrets Manager")
            
        except Exception as e:
            logger.error(f"❌ Failed to load API credentials: {str(e)}")
            raise
    
    def create_jwt_token(self, query_params: Dict = None) -> str:
        """
        JWT 토큰 생성 (업비트 API 인증용)
        """
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4()),
        }
        
        # 쿼리 파라미터가 있을 경우 해시 생성
        if query_params:
            query_string = "&".join([f"{k}={v}" for k, v in sorted(query_params.items())])
            m = hashlib.sha512()
            m.update(query_string.encode())
            query_hash = m.hexdigest()
            payload['query_hash'] = query_hash
            payload['query_hash_alg'] = 'SHA512'
        
        jwt_token = jwt.encode(payload, self.secret_key, algorithm='HS256')
        return jwt_token
    
    def get_headers(self, query_params: Dict = None) -> Dict:
        """
        API 요청용 헤더 생성
        """
        jwt_token = self.create_jwt_token(query_params)
        return {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json"
        }
    
    def get_accounts(self) -> List[Dict]:
        """
        계좌 정보 조회
        """
        try:
            headers = self.get_headers()
            response = self.session.get(
                f"{self.base_url}/v1/accounts",
                headers=headers
            )
            
            if response.status_code == 200:
                accounts = response.json()
                logger.info(f"✅ Retrieved {len(accounts)} accounts")
                return accounts
            else:
                logger.error(f"❌ Failed to get accounts: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error getting accounts: {str(e)}")
            return []
    
    def get_market_info(self) -> List[Dict]:
        """
        마켓 정보 조회 (인증 불필요)
        """
        try:
            response = self.session.get(f"{self.base_url}/v1/market/all?isDetails=true")
            
            if response.status_code == 200:
                markets = response.json()
                krw_markets = [m for m in markets if m['market'].startswith('KRW-')]
                logger.info(f"✅ Retrieved {len(krw_markets)} KRW markets")
                return krw_markets
            else:
                logger.error(f"❌ Failed to get market info: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error getting market info: {str(e)}")
            return []
    
    def get_ticker_price(self, market: str) -> Optional[Dict]:
        """
        특정 마켓의 현재 가격 조회
        """
        try:
            response = self.session.get(
                f"{self.base_url}/v1/ticker",
                params={'markets': market}
            )
            
            if response.status_code == 200:
                ticker = response.json()[0]
                logger.info(f"✅ {market} price: {ticker['trade_price']:,}")
                return ticker
            else:
                logger.error(f"❌ Failed to get {market} price: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error getting {market} price: {str(e)}")
            return None
    
    def get_orderbook(self, market: str) -> Optional[Dict]:
        """
        호가 정보 조회
        """
        try:
            response = self.session.get(
                f"{self.base_url}/v1/orderbook",
                params={'markets': market}
            )
            
            if response.status_code == 200:
                orderbook = response.json()[0]
                logger.info(f"✅ {market} orderbook retrieved")
                return orderbook
            else:
                logger.error(f"❌ Failed to get {market} orderbook: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error getting {market} orderbook: {str(e)}")
            return None
    
    def place_order(self, market: str, side: str, volume: str, price: str = None, 
                   ord_type: str = 'limit') -> Optional[Dict]:
        """
        주문 실행
        
        Args:
            market: 거래 마켓 (예: 'KRW-BTC')
            side: 거래 종류 ('bid': 매수, 'ask': 매도)
            volume: 주문량
            price: 주문 가격 (limit 주문 시 필수)
            ord_type: 주문 타입 ('limit', 'market', 'price')
        """
        try:
            query_params = {
                'market': market,
                'side': side,
                'ord_type': ord_type
            }
            
            if ord_type == 'limit':
                query_params['volume'] = volume
                query_params['price'] = price
            elif ord_type == 'price' and side == 'bid':
                # 시장가 매수 (금액 지정)
                query_params['price'] = volume
            elif ord_type == 'market' and side == 'ask':
                # 시장가 매도 (수량 지정)
                query_params['volume'] = volume
            
            headers = self.get_headers(query_params)
            
            response = self.session.post(
                f"{self.base_url}/v1/orders",
                json=query_params,
                headers=headers
            )
            
            if response.status_code == 201:
                order_result = response.json()
                logger.info(f"✅ Order placed: {market} {side} {volume}")
                
                # DynamoDB에 거래 이력 저장
                self.save_trade_record(order_result)
                
                # SNS 알림 발송
                self.send_trading_notification('trading', 
                    f"{market} {side} 주문이 실행되었습니다",
                    f"거래 실행: {market} {side}",
                    {
                        'market': market,
                        'side': side,
                        'volume': volume,
                        'price': price,
                        'order_id': order_result.get('uuid')
                    }
                )
                
                return order_result
            else:
                logger.error(f"❌ Order failed: {response.status_code} - {response.text}")
                
                # 실패 알림 발송
                self.send_trading_notification('system',
                    f"{market} {side} 주문 실패: {response.text}",
                    f"거래 실패: {market} {side}"
                )
                
                return None
                
        except Exception as e:
            logger.error(f"❌ Error placing order: {str(e)}")
            self.send_trading_notification('system',
                f"주문 실행 중 오류 발생: {str(e)}",
                "시스템 오류: 주문 실행 실패"
            )
            return None
    
    def cancel_order(self, uuid: str) -> Optional[Dict]:
        """
        주문 취소
        """
        try:
            query_params = {'uuid': uuid}
            headers = self.get_headers(query_params)
            
            response = self.session.delete(
                f"{self.base_url}/v1/order",
                params=query_params,
                headers=headers
            )
            
            if response.status_code == 200:
                cancel_result = response.json()
                logger.info(f"✅ Order cancelled: {uuid}")
                return cancel_result
            else:
                logger.error(f"❌ Cancel failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error cancelling order: {str(e)}")
            return None
    
    def get_orders(self, market: str = None, state: str = 'wait') -> List[Dict]:
        """
        주문 목록 조회
        
        Args:
            market: 특정 마켓 (선택사항)
            state: 주문 상태 ('wait', 'done', 'cancel')
        """
        try:
            query_params = {'state': state}
            if market:
                query_params['market'] = market
                
            headers = self.get_headers(query_params)
            
            response = self.session.get(
                f"{self.base_url}/v1/orders",
                params=query_params,
                headers=headers
            )
            
            if response.status_code == 200:
                orders = response.json()
                logger.info(f"✅ Retrieved {len(orders)} orders")
                return orders
            else:
                logger.error(f"❌ Failed to get orders: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error getting orders: {str(e)}")
            return []
    
    def save_trade_record(self, order_result: Dict):
        """
        거래 기록을 DynamoDB에 저장
        """
        try:
            trade_record = {
                'trade_id': order_result['uuid'],
                'timestamp': datetime.utcnow().isoformat(),
                'market': order_result['market'],
                'side': order_result['side'],
                'ord_type': order_result['ord_type'],
                'volume': str(order_result['volume']),
                'price': str(order_result.get('price', '0')),
                'state': order_result['state'],
                'created_at': order_result['created_at'],
                'executed_volume': str(order_result.get('executed_volume', '0')),
                'paid_fee': str(order_result.get('paid_fee', '0')),
                'remaining_fee': str(order_result.get('remaining_fee', '0'))
            }
            
            # Decimal 변환 (DynamoDB 호환성)
            for key in ['volume', 'price', 'executed_volume', 'paid_fee', 'remaining_fee']:
                if trade_record[key]:
                    trade_record[key] = Decimal(str(trade_record[key]))
            
            self.trades_table.put_item(Item=trade_record)
            logger.info(f"✅ Trade record saved: {trade_record['trade_id']}")
            
        except Exception as e:
            logger.error(f"❌ Error saving trade record: {str(e)}")
    
    def send_trading_notification(self, notification_type: str, message: str, 
                                subject: str, details: Dict = None):
        """
        거래 알림 발송
        """
        try:
            lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
            
            payload = {
                'type': notification_type,
                'message': message,
                'subject': subject,
                'details': details or {},
                'source': 'upbit-trader',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            lambda_client.invoke(
                FunctionName='makenaide-notification-handler',
                InvocationType='Event',  # 비동기 호출
                Payload=json.dumps(payload)
            )
            
            logger.info(f"✅ Notification sent: {notification_type} - {subject}")
            
        except Exception as e:
            logger.error(f"❌ Error sending notification: {str(e)}")

class TradingRiskManager:
    """
    거래 위험 관리 시스템
    마크 미너비니의 8% 손절 규칙 적용
    """
    
    def __init__(self, max_position_size: float = 0.05, max_daily_trades: int = 10,
                 stop_loss_percent: float = 0.08):
        self.max_position_size = max_position_size  # 총 자산의 5%
        self.max_daily_trades = max_daily_trades
        self.stop_loss_percent = stop_loss_percent  # 8% 손절
        
        # AWS 리소스
        self.dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
        self.trades_table = self.dynamodb.Table('makenaide-trades')
        self.positions_table = self.dynamodb.Table('makenaide-positions')
    
    def get_daily_trade_count(self) -> int:
        """
        오늘 거래 횟수 확인
        """
        try:
            today = datetime.utcnow().date().isoformat()
            
            response = self.trades_table.scan(
                FilterExpression='begins_with(#ts, :today)',
                ExpressionAttributeNames={'#ts': 'timestamp'},
                ExpressionAttributeValues={':today': today}
            )
            
            count = len(response['Items'])
            logger.info(f"📊 Daily trades: {count}/{self.max_daily_trades}")
            return count
            
        except Exception as e:
            logger.error(f"❌ Error getting daily trade count: {str(e)}")
            return 0
    
    def get_total_balance(self, upbit_client: UpbitAPIClient) -> float:
        """
        총 잔고 조회 (KRW 기준)
        """
        try:
            accounts = upbit_client.get_accounts()
            total_balance = 0.0
            
            for account in accounts:
                if account['currency'] == 'KRW':
                    total_balance += float(account['balance'])
                else:
                    # 암호화폐는 현재 가격으로 환산
                    market = f"KRW-{account['currency']}"
                    ticker = upbit_client.get_ticker_price(market)
                    if ticker:
                        crypto_value = float(account['balance']) * float(ticker['trade_price'])
                        total_balance += crypto_value
            
            logger.info(f"💰 Total balance: {total_balance:,.0f} KRW")
            return total_balance
            
        except Exception as e:
            logger.error(f"❌ Error getting total balance: {str(e)}")
            return 0.0
    
    def validate_trade(self, action: str, market: str, amount: float, 
                      current_price: float, upbit_client: UpbitAPIClient) -> Tuple[bool, str]:
        """
        거래 전 위험 검증
        
        Returns:
            (is_valid, message)
        """
        try:
            # 1. 일일 거래 한도 확인
            daily_count = self.get_daily_trade_count()
            if daily_count >= self.max_daily_trades:
                return False, f"일일 거래 한도 초과: {daily_count}/{self.max_daily_trades}"
            
            # 2. 포지션 크기 제한 (매수의 경우)
            if action == 'buy':
                total_balance = self.get_total_balance(upbit_client)
                trade_amount = amount * current_price
                
                if trade_amount > total_balance * self.max_position_size:
                    max_amount = total_balance * self.max_position_size
                    return False, f"포지션 크기 초과: {trade_amount:,.0f} > {max_amount:,.0f} KRW"
            
            # 3. 손절가 계산 및 알림 (매수의 경우)
            if action == 'buy':
                stop_loss_price = current_price * (1 - self.stop_loss_percent)
                logger.info(f"⚠️  손절가 설정: {stop_loss_price:,.0f} KRW ({self.stop_loss_percent*100}% 하락)")
            
            # 4. 매도 시 손절 규칙 적용
            if action == 'sell':
                is_stop_loss = self.check_stop_loss_condition(market, current_price)
                if is_stop_loss:
                    logger.warning(f"🚨 손절매 실행: {market} at {current_price:,.0f} KRW")
            
            return True, "거래 검증 통과"
            
        except Exception as e:
            logger.error(f"❌ Error validating trade: {str(e)}")
            return False, f"거래 검증 오류: {str(e)}"
    
    def check_stop_loss_condition(self, market: str, current_price: float) -> bool:
        """
        손절 조건 확인
        """
        try:
            # 해당 마켓의 최근 매수 기록 조회
            response = self.trades_table.scan(
                FilterExpression='#market = :market AND side = :side',
                ExpressionAttributeNames={'#market': 'market'},
                ExpressionAttributeValues={
                    ':market': market,
                    ':side': 'bid'
                }
            )
            
            if not response['Items']:
                return False
            
            # 가장 최근 매수 가격
            recent_buy = sorted(response['Items'], 
                              key=lambda x: x['timestamp'], reverse=True)[0]
            
            buy_price = float(recent_buy['price'])
            loss_percent = (buy_price - current_price) / buy_price
            
            if loss_percent >= self.stop_loss_percent:
                logger.warning(f"🚨 Stop loss triggered: {loss_percent*100:.1f}% loss")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking stop loss: {str(e)}")
            return False

class TradingExecutor:
    """
    거래 실행 통합 클래스
    EventBridge 신호를 받아 실제 거래 실행
    """
    
    def __init__(self):
        self.upbit_client = UpbitAPIClient()
        self.risk_manager = TradingRiskManager()
        
        # DynamoDB 테이블
        self.dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
        self.params_table = self.dynamodb.Table('makenaide-trading-params')
    
    def get_trading_parameters(self, signal_id: str) -> Optional[Dict]:
        """
        DynamoDB에서 거래 파라미터 조회
        """
        try:
            response = self.params_table.get_item(
                Key={
                    'signal_id': signal_id,
                    'timestamp': datetime.utcnow().date().isoformat()
                }
            )
            
            if 'Item' in response:
                params = response['Item']
                logger.info(f"✅ Trading parameters retrieved: {signal_id}")
                return params
            else:
                logger.warning(f"⚠️  No trading parameters found: {signal_id}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error getting trading parameters: {str(e)}")
            return None
    
    def execute_trading_signal(self, signal_data: Dict) -> Dict:
        """
        거래 신호 실행
        
        Args:
            signal_data: EventBridge에서 전달받은 거래 신호
            
        Returns:
            실행 결과 딕셔너리
        """
        try:
            action = signal_data.get('action')  # 'buy' or 'sell'
            tickers = signal_data.get('tickers', [])
            signal_strength = signal_data.get('signal_strength', 'medium')
            
            logger.info(f"🎯 Executing trading signal: {action} {len(tickers)} tickers")
            
            execution_results = []
            
            for ticker in tickers:
                market = f"KRW-{ticker}" if not ticker.startswith('KRW-') else ticker
                
                # 현재 가격 조회
                ticker_info = self.upbit_client.get_ticker_price(market)
                if not ticker_info:
                    logger.error(f"❌ Failed to get price for {market}")
                    continue
                
                current_price = float(ticker_info['trade_price'])
                
                # 거래량 계산 (신호 강도에 따라)
                trade_amount = self.calculate_trade_amount(
                    market, action, signal_strength, current_price
                )
                
                if not trade_amount:
                    continue
                
                # 위험 관리 검증
                is_valid, message = self.risk_manager.validate_trade(
                    action, market, trade_amount, current_price, self.upbit_client
                )
                
                if not is_valid:
                    logger.warning(f"⚠️  Trade rejected for {market}: {message}")
                    execution_results.append({
                        'market': market,
                        'action': action,
                        'status': 'rejected',
                        'reason': message
                    })
                    continue
                
                # 실제 거래 실행
                if action == 'buy':
                    order_result = self.execute_buy_order(market, trade_amount, current_price)
                elif action == 'sell':
                    order_result = self.execute_sell_order(market, trade_amount)
                else:
                    logger.error(f"❌ Invalid action: {action}")
                    continue
                
                if order_result:
                    execution_results.append({
                        'market': market,
                        'action': action,
                        'status': 'executed',
                        'order_id': order_result['uuid'],
                        'amount': trade_amount,
                        'price': current_price
                    })
                else:
                    execution_results.append({
                        'market': market,
                        'action': action,
                        'status': 'failed',
                        'reason': 'Order execution failed'
                    })
            
            # 실행 결과 요약
            executed_count = len([r for r in execution_results if r['status'] == 'executed'])
            total_count = len(execution_results)
            
            summary = {
                'signal_id': signal_data.get('signal_id', str(uuid.uuid4())),
                'action': action,
                'total_tickers': total_count,
                'executed_tickers': executed_count,
                'execution_rate': f"{executed_count/total_count*100:.1f}%" if total_count > 0 else "0%",
                'results': execution_results,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"✅ Trading execution completed: {executed_count}/{total_count} successful")
            
            # 실행 결과 알림
            self.upbit_client.send_trading_notification('reports',
                f"{action} 거래 실행 완료: {executed_count}/{total_count} 성공",
                f"거래 실행 결과: {action.upper()}",
                summary
            )
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Error executing trading signal: {str(e)}")
            
            # 오류 알림
            self.upbit_client.send_trading_notification('system',
                f"거래 실행 중 오류 발생: {str(e)}",
                "시스템 오류: 거래 실행 실패",
                {'error': str(e), 'signal_data': signal_data}
            )
            
            return {
                'status': 'error',
                'error': str(e),
                'signal_data': signal_data
            }
    
    def calculate_trade_amount(self, market: str, action: str, 
                             signal_strength: str, current_price: float) -> Optional[float]:
        """
        거래량 계산
        """
        try:
            # 신호 강도별 비중
            strength_weights = {
                'low': 0.02,      # 2%
                'medium': 0.03,   # 3%
                'high': 0.05      # 5%
            }
            
            weight = strength_weights.get(signal_strength, 0.03)
            
            if action == 'buy':
                # 매수: 총 잔고의 일정 비중
                total_balance = self.risk_manager.get_total_balance(self.upbit_client)
                trade_value = total_balance * weight
                trade_amount = trade_value / current_price
                
                logger.info(f"💰 Buy amount calculated: {trade_amount:.6f} {market.split('-')[1]} "
                           f"({trade_value:,.0f} KRW)")
                
                return trade_amount
                
            elif action == 'sell':
                # 매도: 보유 수량의 일정 비중
                accounts = self.upbit_client.get_accounts()
                currency = market.split('-')[1]
                
                for account in accounts:
                    if account['currency'] == currency:
                        available_amount = float(account['balance'])
                        trade_amount = available_amount * 0.5  # 50% 매도
                        
                        logger.info(f"💱 Sell amount calculated: {trade_amount:.6f} {currency}")
                        
                        return trade_amount
                
                logger.warning(f"⚠️  No {currency} balance found for selling")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error calculating trade amount: {str(e)}")
            return None
    
    def execute_buy_order(self, market: str, amount: float, current_price: float) -> Optional[Dict]:
        """
        매수 주문 실행
        """
        try:
            # 지정가 주문 (현재가 기준)
            order_price = str(int(current_price))
            order_volume = f"{amount:.8f}"
            
            logger.info(f"📈 Placing buy order: {market} {order_volume} at {order_price}")
            
            result = self.upbit_client.place_order(
                market=market,
                side='bid',
                volume=order_volume,
                price=order_price,
                ord_type='limit'
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error executing buy order: {str(e)}")
            return None
    
    def execute_sell_order(self, market: str, amount: float) -> Optional[Dict]:
        """
        매도 주문 실행
        """
        try:
            # 시장가 매도
            order_volume = f"{amount:.8f}"
            
            logger.info(f"📉 Placing sell order: {market} {order_volume} (market)")
            
            result = self.upbit_client.place_order(
                market=market,
                side='ask',
                volume=order_volume,
                ord_type='market'
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error executing sell order: {str(e)}")
            return None

def main():
    """
    테스트 실행 함수
    """
    print("🚀 Upbit Trading Module Test")
    print("=" * 40)
    
    try:
        # API 클라이언트 테스트
        upbit = UpbitAPIClient()
        
        # 계좌 정보 조회
        accounts = upbit.get_accounts()
        print(f"✅ Accounts retrieved: {len(accounts)}")
        
        # 마켓 정보 조회
        markets = upbit.get_market_info()
        print(f"✅ Markets retrieved: {len(markets)}")
        
        # 비트코인 가격 조회
        btc_price = upbit.get_ticker_price('KRW-BTC')
        if btc_price:
            print(f"✅ BTC Price: {btc_price['trade_price']:,} KRW")
        
        # 위험 관리 테스트
        risk_manager = TradingRiskManager()
        daily_trades = risk_manager.get_daily_trade_count()
        print(f"✅ Daily trades: {daily_trades}")
        
        print(f"\n🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")

if __name__ == "__main__":
    main()