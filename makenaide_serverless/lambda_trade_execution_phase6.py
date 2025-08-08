#!/usr/bin/env python3
"""
💼 Phase 6: Trade Execution Lambda
- Phase 5에서 생성된 최종 거래 신호를 받아 실제 거래 실행
- Upbit API를 통한 매수/매도 주문
- 리스크 관리: 손절/익절 자동화
- DynamoDB 거래 기록 및 포지션 추적
- SNS 알림 발송
"""

import boto3
import json
import logging
import urllib3
import hmac
import hashlib
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import base64
from urllib.parse import urlencode
from decimal import Decimal

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class UpbitTrader:
    """Upbit API 거래 실행 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb')
        self.sns_client = boto3.client('sns')
        self.secrets_client = boto3.client('secretsmanager')
        self.events_client = boto3.client('events')
        
        self.s3_bucket = 'makenaide-bucket-901361833359'
        self.kst_offset = 9
        
        # Upbit API 설정
        self.upbit_api_url = 'https://api.upbit.com'
        self.http = urllib3.PoolManager()
        
        # 거래 설정
        self.trading_config = {
            'max_positions': 5,           # 최대 동시 보유 종목
            'position_size_krw': 100000,  # 종목당 투자 금액 (10만원)
            'stop_loss_pct': 8.0,         # 손절 비율 (8%)
            'take_profit_levels': [       # 익절 단계
                {'pct': 20.0, 'sell_ratio': 0.33},  # 20% 수익시 1/3 매도
                {'pct': 40.0, 'sell_ratio': 0.50},  # 40% 수익시 추가 50% 매도
                {'pct': 80.0, 'sell_ratio': 1.00}   # 80% 수익시 전량 매도
            ],
            'simulation_mode': True       # 시뮬레이션 모드 (실거래 방지)
        }
        
        # DynamoDB 테이블
        self.trades_table = self.dynamodb.Table('makenaide-trades')
        self.positions_table = self.dynamodb.Table('makenaide-positions')
        
    def convert_floats_to_decimal(self, obj):
        """Python float를 DynamoDB Decimal로 변환"""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self.convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_floats_to_decimal(item) for item in obj]
        return obj
        
    def get_upbit_credentials(self) -> Dict:
        """AWS Secrets Manager에서 Upbit API 키 조회"""
        try:
            secret_name = "makenaide/upbit-api"
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            
            secret = json.loads(response['SecretString'])
            return {
                'access_key': secret['access_key'],
                'secret_key': secret['secret_key']
            }
            
        except Exception as e:
            logger.error(f"Upbit API 키 조회 실패: {e}")
            # 시뮬레이션 모드에서는 더미 키 사용
            return {
                'access_key': 'dummy_access_key',
                'secret_key': 'dummy_secret_key'
            }

    def generate_upbit_jwt(self, query_params: str = '') -> str:
        """Upbit API JWT 토큰 생성"""
        try:
            credentials = self.get_upbit_credentials()
            access_key = credentials['access_key']
            secret_key = credentials['secret_key']
            
            payload = {
                'access_key': access_key,
                'nonce': str(uuid.uuid4()),
            }
            
            if query_params:
                payload['query_hash'] = hashlib.sha512(query_params.encode()).hexdigest()
                payload['query_hash_alg'] = 'SHA512'
            
            import jwt
            token = jwt.encode(payload, secret_key, algorithm='HS256')
            return token
            
        except Exception as e:
            logger.error(f"JWT 토큰 생성 실패: {e}")
            return "dummy_token"

    def load_phase5_signals(self) -> Optional[List[Dict]]:
        """Phase 5 최종 거래 신호 로드"""
        try:
            logger.info("Phase 5 거래 신호 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase5/final_trading_signals.json'
            )
            
            data = json.loads(response['Body'].read())
            
            if data.get('status') != 'success':
                logger.error(f"Phase 5 데이터 상태 불량: {data.get('status')}")
                return None
            
            signals = data.get('final_signals', [])
            if not signals:
                logger.warning("Phase 5에서 거래 신호가 없음")
                return None
                
            logger.info(f"Phase 5 신호 로드 완료: {len(signals)}개")
            return signals
            
        except Exception as e:
            logger.error(f"Phase 5 신호 로드 실패: {e}")
            return None

    def get_account_balance(self) -> Dict:
        """계좌 잔고 조회"""
        try:
            if self.trading_config['simulation_mode']:
                # 시뮬레이션 모드: 더미 데이터 반환
                return {
                    'krw_balance': 1000000,  # 100만원
                    'available_krw': 500000,  # 50만원 사용 가능
                    'crypto_holdings': {}
                }
            
            # 실제 API 호출
            jwt_token = self.generate_upbit_jwt()
            headers = {'Authorization': f'Bearer {jwt_token}'}
            
            response = self.http.request(
                'GET',
                f'{self.upbit_api_url}/v1/accounts',
                headers=headers
            )
            
            if response.status == 200:
                accounts = json.loads(response.data.decode('utf-8'))
                
                balance_info = {
                    'krw_balance': 0,
                    'available_krw': 0,
                    'crypto_holdings': {}
                }
                
                for account in accounts:
                    if account['currency'] == 'KRW':
                        balance_info['krw_balance'] = float(account['balance'])
                        balance_info['available_krw'] = float(account['balance']) - float(account['locked'])
                    else:
                        balance_info['crypto_holdings'][account['currency']] = {
                            'balance': float(account['balance']),
                            'avg_buy_price': float(account['avg_buy_price'])
                        }
                
                return balance_info
            else:
                logger.error(f"계좌 조회 실패: {response.status}")
                return None
                
        except Exception as e:
            logger.error(f"계좌 조회 오류: {e}")
            return None

    def get_current_price(self, ticker: str) -> Optional[float]:
        """현재가 조회"""
        try:
            url = f"{self.upbit_api_url}/v1/ticker?markets={ticker}"
            response = self.http.request('GET', url)
            
            if response.status == 200:
                data = json.loads(response.data.decode('utf-8'))[0]
                return float(data['trade_price'])
            else:
                logger.error(f"{ticker} 현재가 조회 실패: {response.status}")
                return None
                
        except Exception as e:
            logger.error(f"{ticker} 현재가 조회 오류: {e}")
            return None

    def execute_buy_order(self, ticker: str, signal_data: Dict) -> Dict:
        """매수 주문 실행"""
        try:
            current_price = self.get_current_price(ticker)
            if not current_price:
                return {'success': False, 'error': '현재가 조회 실패'}
            
            # 투자 금액 계산
            invest_amount = self.trading_config['position_size_krw']
            quantity = invest_amount / current_price
            
            # 최소 주문 수량 체크
            if quantity * current_price < 5000:  # Upbit 최소 주문 금액 5000원
                return {'success': False, 'error': '최소 주문 금액 미달'}
            
            order_data = {
                'ticker': ticker,
                'side': 'buy',
                'ord_type': 'price',  # 시장가 주문
                'price': str(invest_amount),
                'signal_data': signal_data,
                'current_price': current_price,
                'expected_quantity': quantity,
                'timestamp': (datetime.utcnow() + timedelta(hours=self.kst_offset)).isoformat()
            }
            
            if self.trading_config['simulation_mode']:
                # 시뮬레이션 모드
                order_result = {
                    'success': True,
                    'order_id': f"SIM_{uuid.uuid4().hex[:8]}",
                    'executed_price': current_price,
                    'executed_quantity': quantity,
                    'executed_amount': invest_amount,
                    'simulation': True
                }
            else:
                # 실제 주문 실행
                order_result = self.place_upbit_order(order_data)
            
            # 거래 기록 저장
            if order_result['success']:
                self.save_trade_record(order_data, order_result)
                self.update_position_record(ticker, 'buy', order_result)
            
            return order_result
            
        except Exception as e:
            logger.error(f"{ticker} 매수 주문 실패: {e}")
            return {'success': False, 'error': str(e)}

    def execute_sell_order(self, ticker: str, quantity: float, reason: str) -> Dict:
        """매도 주문 실행"""
        try:
            current_price = self.get_current_price(ticker)
            if not current_price:
                return {'success': False, 'error': '현재가 조회 실패'}
            
            order_data = {
                'ticker': ticker,
                'side': 'sell',
                'ord_type': 'market',
                'volume': str(quantity),
                'reason': reason,
                'current_price': current_price,
                'timestamp': (datetime.utcnow() + timedelta(hours=self.kst_offset)).isoformat()
            }
            
            if self.trading_config['simulation_mode']:
                # 시뮬레이션 모드
                order_result = {
                    'success': True,
                    'order_id': f"SIM_{uuid.uuid4().hex[:8]}",
                    'executed_price': current_price,
                    'executed_quantity': quantity,
                    'executed_amount': current_price * quantity,
                    'simulation': True
                }
            else:
                # 실제 주문 실행
                order_result = self.place_upbit_order(order_data)
            
            # 거래 기록 저장
            if order_result['success']:
                self.save_trade_record(order_data, order_result)
                self.update_position_record(ticker, 'sell', order_result)
            
            return order_result
            
        except Exception as e:
            logger.error(f"{ticker} 매도 주문 실패: {e}")
            return {'success': False, 'error': str(e)}

    def place_upbit_order(self, order_data: Dict) -> Dict:
        """실제 Upbit API 주문 실행"""
        try:
            # 실제 구현에서는 여기에 Upbit API 호출 로직
            # 현재는 시뮬레이션 모드만 구현
            return {'success': False, 'error': '실거래 모드 미구현'}
            
        except Exception as e:
            logger.error(f"Upbit API 주문 실패: {e}")
            return {'success': False, 'error': str(e)}

    def check_risk_management(self) -> List[Dict]:
        """리스크 관리: 손절/익절 체크"""
        try:
            # 현재 포지션 조회
            response = self.positions_table.scan()
            positions = response.get('Items', [])
            
            risk_actions = []
            
            for position in positions:
                if position['status'] != 'active':
                    continue
                
                ticker = position['ticker']
                current_price = self.get_current_price(ticker)
                
                if not current_price:
                    continue
                
                avg_buy_price = float(position['avg_buy_price'])
                current_quantity = float(position['current_quantity'])
                
                # 수익률 계산
                profit_pct = ((current_price - avg_buy_price) / avg_buy_price) * 100
                
                # 손절 체크
                if profit_pct <= -self.trading_config['stop_loss_pct']:
                    risk_actions.append({
                        'action': 'stop_loss',
                        'ticker': ticker,
                        'quantity': current_quantity,
                        'reason': f'손절: {profit_pct:.1f}%',
                        'profit_pct': profit_pct
                    })
                    continue
                
                # 익절 체크
                for level in self.trading_config['take_profit_levels']:
                    if profit_pct >= level['pct']:
                        sell_quantity = current_quantity * level['sell_ratio']
                        
                        risk_actions.append({
                            'action': 'take_profit',
                            'ticker': ticker,
                            'quantity': sell_quantity,
                            'reason': f'익절: {profit_pct:.1f}% (레벨 {level["pct"]}%)',
                            'profit_pct': profit_pct
                        })
                        break
            
            return risk_actions
            
        except Exception as e:
            logger.error(f"리스크 관리 체크 실패: {e}")
            return []

    def save_trade_record(self, order_data: Dict, order_result: Dict):
        """거래 기록 DynamoDB 저장"""
        try:
            trade_record = {
                'trade_id': order_result.get('order_id', str(uuid.uuid4())),
                'ticker': order_data['ticker'],
                'side': order_data['side'],
                'executed_price': Decimal(str(order_result.get('executed_price', 0))),
                'executed_quantity': Decimal(str(order_result.get('executed_quantity', 0))),
                'executed_amount': Decimal(str(order_result.get('executed_amount', 0))),
                'timestamp': order_data['timestamp'],
                'simulation': order_result.get('simulation', False),
                'reason': order_data.get('reason', 'signal'),
                'signal_data': self.convert_floats_to_decimal(order_data.get('signal_data', {})),
                'status': 'completed' if order_result['success'] else 'failed'
            }
            
            self.trades_table.put_item(Item=trade_record)
            logger.info(f"거래 기록 저장: {trade_record['trade_id']}")
            
        except Exception as e:
            logger.error(f"거래 기록 저장 실패: {e}")

    def update_position_record(self, ticker: str, side: str, order_result: Dict):
        """포지션 기록 업데이트"""
        try:
            # 기존 포지션 조회
            response = self.positions_table.get_item(
                Key={'ticker': ticker}
            )
            
            if 'Item' in response:
                position = response['Item']
            else:
                position = {
                    'ticker': ticker,
                    'total_quantity': Decimal('0'),
                    'current_quantity': Decimal('0'),
                    'avg_buy_price': Decimal('0'),
                    'total_invested': Decimal('0'),
                    'status': 'active',
                    'created_at': (datetime.utcnow() + timedelta(hours=self.kst_offset)).isoformat()
                }
            
            executed_quantity = Decimal(str(order_result.get('executed_quantity', 0)))
            executed_amount = Decimal(str(order_result.get('executed_amount', 0)))
            
            if side == 'buy':
                # 매수: 평균 단가 및 수량 업데이트
                current_value = position['current_quantity'] * position['avg_buy_price']
                new_total_value = current_value + executed_amount
                new_quantity = position['current_quantity'] + executed_quantity
                
                position['avg_buy_price'] = new_total_value / new_quantity if new_quantity > 0 else Decimal('0')
                position['current_quantity'] = new_quantity
                position['total_quantity'] += executed_quantity
                position['total_invested'] += executed_amount
                
            elif side == 'sell':
                # 매도: 수량 감소
                position['current_quantity'] -= executed_quantity
                
                if position['current_quantity'] <= 0:
                    position['status'] = 'closed'
                    position['closed_at'] = (datetime.utcnow() + timedelta(hours=self.kst_offset)).isoformat()
            
            position['updated_at'] = (datetime.utcnow() + timedelta(hours=self.kst_offset)).isoformat()
            
            self.positions_table.put_item(Item=position)
            logger.info(f"포지션 업데이트: {ticker}")
            
        except Exception as e:
            logger.error(f"포지션 업데이트 실패: {e}")

    def send_trading_notification(self, message: str, subject: str = "Makenaide 거래 알림"):
        """거래 알림 발송"""
        try:
            topic_arn = "arn:aws:sns:ap-northeast-2:901361833359:makenaide-alerts"
            
            self.sns_client.publish(
                TopicArn=topic_arn,
                Subject=subject,
                Message=message
            )
            
            logger.info("거래 알림 발송 완료")
            
        except Exception as e:
            logger.error(f"거래 알림 발송 실패: {e}")

    def process_trading_signals(self, signals: List[Dict]) -> Dict:
        """거래 신호 처리"""
        try:
            results = {
                'processed_signals': 0,
                'successful_trades': 0,
                'failed_trades': 0,
                'trade_details': [],
                'risk_actions': []
            }
            
            # 계좌 잔고 확인
            balance = self.get_account_balance()
            if not balance:
                return {'error': '계좌 잔고 조회 실패'}
            
            logger.info(f"사용 가능 잔고: {balance['available_krw']:,.0f} KRW")
            
            # 거래 신호 처리
            for signal in signals:
                if signal.get('action') != 'BUY':
                    continue
                
                ticker = signal.get('ticker')
                if not ticker:
                    continue
                
                results['processed_signals'] += 1
                
                # 매수 실행
                order_result = self.execute_buy_order(ticker, signal)
                
                if order_result['success']:
                    results['successful_trades'] += 1
                    logger.info(f"✅ {ticker} 매수 성공: {order_result.get('executed_amount', 0):,.0f} KRW")
                else:
                    results['failed_trades'] += 1
                    logger.error(f"❌ {ticker} 매수 실패: {order_result.get('error', 'Unknown')}")
                
                results['trade_details'].append({
                    'ticker': ticker,
                    'action': 'buy',
                    'success': order_result['success'],
                    'result': order_result
                })
            
            # 리스크 관리 체크
            risk_actions = self.check_risk_management()
            for action in risk_actions:
                sell_result = self.execute_sell_order(
                    action['ticker'], 
                    action['quantity'], 
                    action['reason']
                )
                
                results['risk_actions'].append({
                    'ticker': action['ticker'],
                    'action': action['action'],
                    'success': sell_result['success'],
                    'result': sell_result
                })
                
                if sell_result['success']:
                    logger.info(f"✅ {action['ticker']} {action['reason']} 완료")
            
            return results
            
        except Exception as e:
            logger.error(f"거래 신호 처리 실패: {e}")
            return {'error': str(e)}

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.utcnow()
    
    try:
        logger.info("=== Phase 6: Trade Execution 시작 ===")
        logger.info(f"이벤트: {json.dumps(event)}")
        
        # 거래 실행기 초기화
        trader = UpbitTrader()
        
        # Phase 5 신호 로드
        signals = trader.load_phase5_signals()
        if not signals:
            return {
                'statusCode': 400,
                'phase': 'trade_execution',
                'error': 'Phase 5 신호 없음',
                'message': 'Phase 5를 먼저 실행해주세요'
            }
        
        # 거래 실행
        trading_results = trader.process_trading_signals(signals)
        
        # 알림 발송
        if trading_results.get('successful_trades', 0) > 0 or trading_results.get('risk_actions'):
            notification_message = f"""
[Makenaide 거래 실행 결과]

📊 처리 신호: {trading_results.get('processed_signals', 0)}개
✅ 성공 거래: {trading_results.get('successful_trades', 0)}개  
❌ 실패 거래: {trading_results.get('failed_trades', 0)}개
🛡️ 리스크 액션: {len(trading_results.get('risk_actions', []))}개

⚠️ 시뮬레이션 모드: {trader.trading_config['simulation_mode']}

⏰ 실행 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
            trader.send_trading_notification(notification_message, "💼 Makenaide 거래 실행")
        
        # 실행 시간 계산
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        
        # 성공 응답
        response = {
            'statusCode': 200,
            'phase': 'trade_execution',
            'input_signals': len(signals),
            'processed_signals': trading_results.get('processed_signals', 0),
            'successful_trades': trading_results.get('successful_trades', 0),
            'failed_trades': trading_results.get('failed_trades', 0),
            'risk_actions': len(trading_results.get('risk_actions', [])),
            'simulation_mode': trader.trading_config['simulation_mode'],
            'execution_time': f"{execution_time:.2f}초",
            'timestamp': start_time.isoformat()
        }
        
        logger.info(f"=== Phase 6 완료 ===")
        logger.info(f"결과: {response}")
        
        return response
        
    except Exception as e:
        logger.error(f"Phase 6 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': 'trade_execution',
            'error': str(e),
            'message': 'Phase 6 실행 중 오류 발생'
        }