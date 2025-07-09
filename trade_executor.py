import pyupbit
import psycopg2
import time
import os
from datetime import datetime
from dotenv import load_dotenv
from utils import retry
import logging
from db_manager import DBManager

load_dotenv()

#MIN_KRW_ORDER = 5000  # Upbit 최소 주문 금액 (KRW)
MIN_KRW_ORDER = 10000  # 사용자 지정 최소 주문 금액 (KRW) - 수수료 포함
MIN_KRW_SELL_ORDER = 5000  # 업비트 실제 최소 매도 금액 (KRW)

# 수수료 정책 (업비트 KRW 마켓 기준)
TAKER_FEE_RATE = 0.00139  # 0.139% (Taker 수수료)
MAKER_FEE_RATE = 0.0005   # 0.05% (Maker 수수료)

DB_PATH = "makenaide.db"

# 매매가 추천된 티커 조회
def get_trade_candidates():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.ticker, t.action, s.price, NULL as position_avg_price
        FROM trend_analysis t
        LEFT JOIN static_indicators s ON t.ticker = s.ticker
        WHERE t.action IN ('BUY', 'SELL')
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

# 데이터베이스에 평균 매수가 갱신 - static_indicators 테이블 기반으로 복구
def update_position(ticker, avg_price):
    """
    포지션 정보를 static_indicators와 ohlcv 테이블에서 조회하여 업데이트
    
    Args:
        ticker (str): 업데이트할 티커
        avg_price (float): 평균 매수가
    
    Returns:
        dict: 포지션 정보 (price, atr, ma_200, current_close 등)
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        cursor = conn.cursor()
        
        # 1. static_indicators에서 기본 정보 조회
        cursor.execute("""
            SELECT price, pivot, s1, r1, updated_at
            FROM static_indicators 
            WHERE ticker = %s
        """, (ticker,))
        
        static_data = cursor.fetchone()
        if not static_data:
            logging.warning(f"⚠️ {ticker} static_indicators 데이터 없음")
            return None
            
        price, pivot, s1, r1, updated_at = static_data
        
        # 2. ohlcv에서 최신 OHLC 및 기술적 지표 조회
        cursor.execute("""
            SELECT open, high, low, close, volume, 
                   atr, ma_50, ma_200, rsi_14, macd_histogram,
                   bb_upper, bb_lower, date
            FROM ohlcv 
            WHERE ticker = %s 
            ORDER BY date DESC 
            LIMIT 1
        """, (ticker,))
        
        ohlcv_data = cursor.fetchone()
        if not ohlcv_data:
            logging.warning(f"⚠️ {ticker} ohlcv 데이터 없음")
            return None
            
        open_price, high, low, close, volume, atr, ma_50, ma_200, rsi_14, macd_histogram, bb_upper, bb_lower, date = ohlcv_data
        
        # 3. 포지션 정보 구성
        position_info = {
            'ticker': ticker,
            'avg_price': avg_price,
            'current_price': price or close,  # static_indicators의 price 우선, 없으면 ohlcv의 close
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
            'atr': atr,
            'ma_50': ma_50,
            'ma_200': ma_200,
            'rsi_14': rsi_14,
            'macd_histogram': macd_histogram,
            'bb_upper': bb_upper,
            'bb_lower': bb_lower,
            'pivot': pivot,
            'support': s1,
            'resistance': r1,
            'last_update': updated_at,
            'ohlcv_date': date
        }
        
        # 4. 수익률 계산
        if avg_price and position_info['current_price']:
            pnl_rate = ((position_info['current_price'] - avg_price) / avg_price) * 100
            position_info['pnl_rate'] = pnl_rate
        else:
            position_info['pnl_rate'] = 0.0
            
        # 5. portfolio_history에 포지션 업데이트 기록
        cursor.execute("""
            INSERT INTO portfolio_history (ticker, action, qty, price, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            ticker,
            'POSITION_UPDATE',
            0,  # 수량은 실제 거래가 아니므로 0
            avg_price,
            datetime.now()
        ))
        
        conn.commit()
        
        logging.info(f"✅ {ticker} 포지션 정보 업데이트 완료")
        logging.info(f"   - 평균가: {avg_price:,.2f}, 현재가: {position_info['current_price']:,.2f}")
        logging.info(f"   - 수익률: {position_info['pnl_rate']:+.2f}%")
        logging.info(f"   - ATR: {atr}, 지지/저항: {s1:.2f}/{r1:.2f}")
        
        return position_info
        
    except Exception as e:
        logging.error(f"❌ {ticker} 포지션 업데이트 실패: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        return None
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@retry(max_attempts=3, initial_delay=1, backoff=2)
def buy_asset(upbit_client, ticker: str, current_price: float, trade_amount_krw: float, 
              gpt_confidence: float | None = None, gpt_reason: str | None = None) -> dict:
    """
    자산 매수를 실행하고 결과를 기록합니다.

    Args:
        upbit_client: Upbit API 클라이언트 인스턴스
        ticker (str): 매수할 티커
        current_price (float): 현재가 (매수 결정 시점의 가격)
        trade_amount_krw (float): 총 매수 금액 (KRW)
        gpt_confidence (float | None, optional): GPT 분석 신뢰도. Defaults to None.
        gpt_reason (str | None, optional): GPT 분석 근거. Defaults to None.

    Returns:
        dict: {"status": str, "order_id": str|None, "quantity": float|None, "price": float|None, "error": str|None}
    """
    db_mgr = DBManager()
    order_id = None
    executed_quantity = None
    executed_price_avg = None

    try:
        if current_price is None or current_price <= 0:
            logging.error(f"❌ {ticker} 매수 주문 실패: 유효하지 않은 현재가 ({current_price})")
            db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=0, order_id=None, status='FAILURE', error_message=f"Invalid current price: {current_price}")
            return {"status": "FAILURE", "order_id": None, "quantity": None, "price": None, "error": f"Invalid current price: {current_price}"}

        # 수수료를 고려한 최소 주문 금액 검증
        # trade_amount_krw는 이미 수수료가 포함된 금액이므로 직접 비교
        if trade_amount_krw < MIN_KRW_ORDER:
            logging.warning(f"⚠️ {ticker} 매수 금액 ({trade_amount_krw:.0f} KRW)이 최소 주문 금액 ({MIN_KRW_ORDER} KRW) 미만입니다. 주문하지 않습니다.")
            # 필요시 SKIPPED 상태로 DB 기록
            # db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=current_price, order_id=None, status='SKIPPED', error_message=f"Amount {trade_amount_krw} < MIN_ORDER {MIN_KRW_ORDER}")
            return {"status": "SKIPPED", "order_id": None, "quantity": None, "price": None, "error": f"Amount {trade_amount_krw} < MIN_ORDER {MIN_KRW_ORDER}"}

        # 🔧 [핵심 수정] pyupbit를 직접 사용
        access_key = os.getenv("UPBIT_ACCESS_KEY")
        secret_key = os.getenv("UPBIT_SECRET_KEY")
        
        if not access_key or not secret_key:
            error_msg = "Upbit API 키가 설정되지 않았습니다"
            logging.error(f"❌ {ticker} 매수 주문 실패: {error_msg}")
            db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=0, order_id=None, status='FAILURE', error_message=error_msg)
            return {"status": "FAILURE", "order_id": None, "quantity": None, "price": None, "error": error_msg}

        # pyupbit 객체 생성
        upbit = pyupbit.Upbit(access_key, secret_key)
        
        logging.info(f"🚀 {ticker} 시장가 매수 주문 시도: {trade_amount_krw:.0f} KRW (결정시 현재가: {current_price})")
        
        # 🔧 [핵심 수정] pyupbit를 직접 사용하여 매수 주문
        resp = upbit.buy_market_order(ticker, trade_amount_krw)
        
        if resp and resp.get('uuid'):
            order_id = resp['uuid']
            logging.info(f"✅ {ticker} 매수 주문 접수 성공. 주문 ID: {order_id}. 체결 확인 중...")
            
            time.sleep(5) # 실제 환경에서는 주문 상태 폴링 로직 권장
            
            # 🔧 [핵심 수정] pyupbit를 직접 사용하여 주문 조회
            order_detail = upbit.get_order(order_id)

            if order_detail and order_detail.get('state') == 'done':
                executed_quantity_str = order_detail.get('executed_volume', '0')
                executed_quantity = float(executed_quantity_str) if executed_quantity_str else 0.0
                
                trades = order_detail.get('trades', [])
                if trades and executed_quantity > 0:
                    total_executed_value = sum(float(trade['price']) * float(trade['volume']) for trade in trades)
                    total_executed_volume = sum(float(trade['volume']) for trade in trades) # executed_quantity와 거의 동일해야 함
                    
                    if total_executed_volume > 0: # 방어 코드
                        executed_price_avg = total_executed_value / total_executed_volume
                        logging.info(f"💰 {ticker} 매수 체결 완료: 수량 {executed_quantity:.8f}, 평균 단가 {executed_price_avg:.2f} (주문ID: {order_id})")
                        db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=executed_quantity, price=executed_price_avg, order_id=order_id, status='SUCCESS')
                        return {"status": "SUCCESS", "order_id": order_id, "quantity": executed_quantity, "price": executed_price_avg, "error": None}
                    else: # 체결 내역은 있으나 총 체결 수량이 0인 비정상 케이스
                        error_msg = f"Executed volume is zero despite trades. OrderID: {order_id}"
                        logging.error(f"❌ {ticker} 매수 체결 정보 오류: {error_msg}")
                        db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=0, order_id=order_id, status='FAILURE', error_message=error_msg)
                        return {"status": "FAILURE", "order_id": order_id, "quantity": 0.0, "price": 0.0, "error": error_msg}
                elif executed_quantity > 0: # trades 리스트가 비었지만 executed_volume은 있는 경우 (Upbit에서 가끔 발생)
                    logging.warning(f"⚠️ {ticker} 매수 체결 완료 (trades 정보 없음). 수량: {executed_quantity:.8f}. 평균단가는 현재가({current_price})로 기록. (주문ID: {order_id})")
                    db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=executed_quantity, price=current_price, order_id=order_id, status='SUCCESS_NO_AVG_PRICE')
                    return {"status": "SUCCESS_NO_AVG_PRICE", "order_id": order_id, "quantity": executed_quantity, "price": current_price, "error": "Trades info missing"}
                else: # executed_quantity도 0인 경우
                    error_msg = f"Order 'done' but executed_volume is zero and no trades. OrderID: {order_id}"
                    logging.error(f"❌ {ticker} 매수 체결 오류: {error_msg}")
                    db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=0, order_id=order_id, status='FAILURE', error_message=error_msg)
                    return {"status": "FAILURE", "order_id": order_id, "quantity": 0.0, "price": 0.0, "error": error_msg}

            elif order_detail:
                # 🔧 [핵심 수정] cancel 상태에서도 체결된 수량이 있으면 성공으로 처리
                order_state = order_detail.get('state')
                executed_quantity_str = order_detail.get('executed_volume', '0')
                executed_quantity = float(executed_quantity_str) if executed_quantity_str else 0.0
                
                if order_state == 'cancel' and executed_quantity > 0:
                    # cancel 상태이지만 체결된 수량이 있는 경우 (부분 체결 후 취소)
                    trades = order_detail.get('trades', [])
                    if trades:
                        total_executed_value = sum(float(trade['price']) * float(trade['volume']) for trade in trades)
                        total_executed_volume = sum(float(trade['volume']) for trade in trades)
                        if total_executed_volume > 0:
                            executed_price_avg = total_executed_value / total_executed_volume
                            logging.info(f"💰 {ticker} 매수 부분 체결 완료 (상태: cancel): 수량 {executed_quantity:.8f}, 평균 단가 {executed_price_avg:.2f} (주문ID: {order_id})")
                            db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=executed_quantity, price=executed_price_avg, order_id=order_id, status='SUCCESS_PARTIAL')
                            return {"status": "SUCCESS_PARTIAL", "order_id": order_id, "quantity": executed_quantity, "price": executed_price_avg, "error": "Partial execution"}
                        else:
                            logging.warning(f"⚠️ {ticker} 매수 부분 체결 (trades 정보 없음). 수량: {executed_quantity:.8f}. 평균단가는 현재가({current_price})로 기록. (주문ID: {order_id})")
                            db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=executed_quantity, price=current_price, order_id=order_id, status='SUCCESS_PARTIAL_NO_AVG')
                            return {"status": "SUCCESS_PARTIAL_NO_AVG", "order_id": order_id, "quantity": executed_quantity, "price": current_price, "error": "Partial execution without trades info"}
                    else:
                        logging.warning(f"⚠️ {ticker} 매수 부분 체결 (trades 정보 없음). 수량: {executed_quantity:.8f}. 평균단가는 현재가({current_price})로 기록. (주문ID: {order_id})")
                        db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=executed_quantity, price=current_price, order_id=order_id, status='SUCCESS_PARTIAL_NO_AVG')
                        return {"status": "SUCCESS_PARTIAL_NO_AVG", "order_id": order_id, "quantity": executed_quantity, "price": current_price, "error": "Partial execution without trades info"}
                else:
                    # 실제로 실패한 경우
                    error_msg = f"Order not 'done': state={order_state}, executed_volume={executed_quantity}, details={order_detail}. OrderID: {order_id}"
                    logging.error(f"❌ {ticker} 매수 주문 체결 실패: {error_msg}")
                    db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=0, order_id=order_id, status='FAILURE', error_message=error_msg)
                    return {"status": "FAILURE", "order_id": order_id, "quantity": None, "price": None, "error": error_msg}
            else:
                error_msg = f"Failed to get order details after submission. OrderID: {order_id}"
                logging.error(f"❌ {ticker} 매수 주문 상세 정보 조회 실패: {error_msg}")
                db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=0, order_id=order_id, status='FAILURE', error_message=error_msg)
                return {"status": "FAILURE", "order_id": order_id, "quantity": None, "price": None, "error": error_msg}

        elif resp and resp.get('error'):
            error_info = resp['error']
            error_msg = error_info.get('message', 'Unknown error')
            logging.error(f"❌ {ticker} 매수 주문 API 오류: {error_msg} (Details: {error_info})")
            db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=current_price, order_id=None, status='FAILURE', error_message=f"API Error: {error_msg}")
            return {"status": "FAILURE", "order_id": None, "quantity": None, "price": None, "error": f"API Error: {error_msg}"}
        else:
            error_msg = f"No/unexpected API response: {resp}"
            logging.error(f"❌ {ticker} 매수 주문 실패: {error_msg}")
            db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=current_price, order_id=None, status='FAILURE', error_message=error_msg)
            return {"status": "FAILURE", "order_id": None, "quantity": None, "price": None, "error": error_msg}

    except pyupbit.UpbitError as ue:
        error_msg = f"Upbit API Error: {str(ue)}"
        logging.error(f"❌ {ticker} 매수 중 Upbit API 오류 발생: {error_msg}")
        db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=current_price, order_id=order_id, status='FAILURE', error_message=error_msg) # order_id가 있을 수 있음
        return {"status": "FAILURE", "order_id": order_id, "quantity": None, "price": None, "error": error_msg}
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logging.error(f"❌ {ticker} 매수 중 예기치 않은 오류 발생: {error_msg}")
        db_mgr.save_trade_record(ticker=ticker, order_type='BUY', quantity=0, price=current_price, order_id=order_id, status='FAILURE', error_message=error_msg) # order_id가 있을 수 있음
        return {"status": "FAILURE", "order_id": order_id, "quantity": None, "price": None, "error": error_msg}

@retry(max_attempts=3, initial_delay=1, backoff=2)
def sell_asset(ticker: str, quantity_to_sell: float | None = None, upbit_client=None) -> dict:
    """
    자산 매도 실행. 특정 수량을 지정하지 않으면 전량 매도.
    
    Args:
        ticker (str): 매도할 티커 (KRW-BTC 형식)
        quantity_to_sell (float | None): 매도할 수량 (None이면 전량 매도)
        upbit_client: Upbit 클라이언트 (None이면 내부에서 생성)
    
    Returns:
        dict: {"status": str, "order_id": str|None, "quantity": float|None, "price": float|None, "error": str|None}
    """
    db_mgr = DBManager()
    order_id = None
    executed_quantity = None
    executed_price_avg = None
    current_price_for_record = 0 # 기록용 현재가

    try:
        # 티커 형식 확인 및 변환
        if not ticker.startswith("KRW-"):
            ticker = f"KRW-{ticker}"
        
        # 🔧 [핵심 수정] pyupbit를 직접 사용 (upbit_client가 없으면 내부 생성)
        if upbit_client is None:
            access_key = os.getenv("UPBIT_ACCESS_KEY")
            secret_key = os.getenv("UPBIT_SECRET_KEY")
            
            if not access_key or not secret_key:
                error_msg = "Upbit API 키가 설정되지 않았습니다"
                logging.error(f"❌ {ticker} 매도 주문 실패: {error_msg}")
                db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=0, price=0, order_id=None, status='FAILURE', error_message=error_msg)
                return {"status": "FAILURE", "order_id": None, "quantity": None, "price": None, "error": error_msg}

            # pyupbit 객체 생성
            upbit = pyupbit.Upbit(access_key, secret_key)
        else:
            upbit = upbit_client
        
        # 티커에서 심볼 추출 (KRW-BTC -> BTC)
        symbol = ticker.replace("KRW-", "")
        
        balance = upbit.get_balance(symbol) # 티커 심볼 (예: "BTC")
        if not isinstance(balance, (int, float)) or balance <= 0:
            logging.warning(f"⚠️ {ticker} 매도 시도: 현재 보유 수량 없음 또는 조회 실패 ({balance}). 매도 주문하지 않음.")
            return {"status": "SKIPPED", "order_id": None, "quantity": None, "price": None, "error": "No balance to sell or balance check failed"}

        sell_quantity = 0
        if quantity_to_sell is not None and quantity_to_sell > 0:
            if quantity_to_sell > balance:
                logging.warning(f"⚠️ {ticker} 매도 요청 수량({quantity_to_sell})이 보유 수량({balance})보다 많습니다. 보유 수량만큼 매도합니다.")
                sell_quantity = balance
            else:
                sell_quantity = quantity_to_sell
        else: # 전량 매도
            sell_quantity = balance
        
        if sell_quantity <= 0: # 최종적으로 팔 수량이 없는 경우
            logging.warning(f"⚠️ {ticker} 매도할 수량이 없습니다 (보유: {balance}, 요청: {quantity_to_sell}).")
            return {"status": "SKIPPED", "order_id": None, "quantity": None, "price": None, "error": f"No quantity to sell. Balance: {balance}, Requested: {quantity_to_sell}"}

        # 매도 주문 전 현재가 조회 (기록 및 최소 주문 금액 체크용)
        current_price_data = pyupbit.get_current_price(ticker)
        if current_price_data is None:
            logging.error(f"❌ {ticker} 매도 주문 위한 현재가 조회 실패. 매도 보류.")
            db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=sell_quantity, price=0, order_id=None, status='FAILURE', error_message="Failed to get current price before sell order")
            return {"status": "FAILURE", "order_id": None, "quantity": None, "price": None, "error": "Failed to get current price before sell order"}
        
        current_price_for_record = float(current_price_data) # float으로 변환

        # 최소 매도 금액 확인 (업비트는 KRW 마켓의 경우 5000원)
        estimated_sell_value = sell_quantity * current_price_for_record
        if estimated_sell_value < MIN_KRW_SELL_ORDER:
            logging.warning(f"⚠️ {ticker} 예상 매도 금액 ({estimated_sell_value:.0f} KRW)이 최소 주문 금액 ({MIN_KRW_SELL_ORDER} KRW) 미만입니다. 매도 주문하지 않습니다.")
            return {"status": "SKIPPED", "order_id": None, "quantity": None, "price": None, "error": f"Estimated value {estimated_sell_value} < MIN_SELL_ORDER {MIN_KRW_SELL_ORDER}"}

        logging.info(f"🚀 {ticker} 시장가 매도 주문 시도: 수량 {sell_quantity:.8f} (현재가: {current_price_for_record})")
        
        # 🔧 [핵심 수정] pyupbit를 직접 사용하여 매도 주문
        resp = upbit.sell_market_order(ticker, sell_quantity)

        if resp and resp.get('uuid'):
            order_id = resp['uuid']
            logging.info(f"✅ {ticker} 매도 주문 접수 성공. 주문 ID: {order_id}. 체결 확인 중...")

            time.sleep(5) 
            
            # 🔧 [핵심 수정] pyupbit를 직접 사용하여 주문 조회
            order_detail = upbit.get_order(order_id)

            if order_detail and order_detail.get('state') == 'done':
                executed_quantity_str = order_detail.get('executed_volume', '0')
                executed_quantity = float(executed_quantity_str) if executed_quantity_str else 0.0

                trades = order_detail.get('trades', [])
                if trades and executed_quantity > 0:
                    total_executed_value = sum(float(trade['price']) * float(trade['volume']) for trade in trades)
                    total_executed_volume = sum(float(trade['volume']) for trade in trades)
                    if total_executed_volume > 0:
                        executed_price_avg = total_executed_value / total_executed_volume
                        logging.info(f"💰 {ticker} 매도 체결 완료: 수량 {executed_quantity:.8f}, 평균 단가 {executed_price_avg:.2f} (주문ID: {order_id})")
                        db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=executed_quantity, price=executed_price_avg, order_id=order_id, status='SUCCESS')
                        return {"status": "SUCCESS", "order_id": order_id, "quantity": executed_quantity, "price": executed_price_avg, "error": None}
                    else:
                        error_msg = f"Sell order 'done', executed_volume is zero despite trades. OrderID: {order_id}"
                        logging.error(f"❌ {ticker} 매도 체결 정보 오류: {error_msg}")
                        db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=0, price=0, order_id=order_id, status='FAILURE', error_message=error_msg)
                        return {"status": "FAILURE", "order_id": order_id, "quantity": 0.0, "price": 0.0, "error": error_msg}
                elif executed_quantity > 0: # trades 리스트가 비었지만 executed_volume은 있는 경우
                    logging.warning(f"⚠️ {ticker} 매도 체결 완료 (trades 정보 없음). 수량: {executed_quantity:.8f}. 평균단가는 현재가({current_price_for_record})로 기록. (주문ID: {order_id})")
                    db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=executed_quantity, price=current_price_for_record, order_id=order_id, status='SUCCESS_NO_AVG_PRICE')
                    return {"status": "SUCCESS_NO_AVG_PRICE", "order_id": order_id, "quantity": executed_quantity, "price": current_price_for_record, "error": "Trades info missing"}
                else:
                    error_msg = f"Sell order 'done' but executed_volume is zero and no trades. OrderID: {order_id}"
                    logging.error(f"❌ {ticker} 매도 체결 오류: {error_msg}")
                    db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=0, price=0, order_id=order_id, status='FAILURE', error_message=error_msg)
                    return {"status": "FAILURE", "order_id": order_id, "quantity": 0.0, "price": 0.0, "error": error_msg}
            elif order_detail:
                # 🔧 [핵심 수정] cancel 상태에서도 체결된 수량이 있으면 성공으로 처리
                order_state = order_detail.get('state')
                executed_quantity_str = order_detail.get('executed_volume', '0')
                executed_quantity = float(executed_quantity_str) if executed_quantity_str else 0.0
                
                if order_state == 'cancel' and executed_quantity > 0:
                    # cancel 상태이지만 체결된 수량이 있는 경우 (부분 체결 후 취소)
                    trades = order_detail.get('trades', [])
                    if trades:
                        total_executed_value = sum(float(trade['price']) * float(trade['volume']) for trade in trades)
                        total_executed_volume = sum(float(trade['volume']) for trade in trades)
                        if total_executed_volume > 0:
                            executed_price_avg = total_executed_value / total_executed_volume
                            logging.info(f"💰 {ticker} 매도 부분 체결 완료 (상태: cancel): 수량 {executed_quantity:.8f}, 평균 단가 {executed_price_avg:.2f} (주문ID: {order_id})")
                            db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=executed_quantity, price=executed_price_avg, order_id=order_id, status='SUCCESS_PARTIAL')
                            return {"status": "SUCCESS_PARTIAL", "order_id": order_id, "quantity": executed_quantity, "price": executed_price_avg, "error": "Partial execution"}
                        else:
                            logging.warning(f"⚠️ {ticker} 매도 부분 체결 (trades 정보 없음). 수량: {executed_quantity:.8f}. 평균단가는 현재가({current_price_for_record})로 기록. (주문ID: {order_id})")
                            db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=executed_quantity, price=current_price_for_record, order_id=order_id, status='SUCCESS_PARTIAL_NO_AVG')
                            return {"status": "SUCCESS_PARTIAL_NO_AVG", "order_id": order_id, "quantity": executed_quantity, "price": current_price_for_record, "error": "Partial execution without trades info"}
                    else:
                        logging.warning(f"⚠️ {ticker} 매도 부분 체결 (trades 정보 없음). 수량: {executed_quantity:.8f}. 평균단가는 현재가({current_price_for_record})로 기록. (주문ID: {order_id})")
                        db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=executed_quantity, price=current_price_for_record, order_id=order_id, status='SUCCESS_PARTIAL_NO_AVG')
                        return {"status": "SUCCESS_PARTIAL_NO_AVG", "order_id": order_id, "quantity": executed_quantity, "price": current_price_for_record, "error": "Partial execution without trades info"}
                else:
                    # 실제로 실패한 경우
                    error_msg = f"Sell order not 'done': state={order_state}, executed_volume={executed_quantity}, details={order_detail}. OrderID: {order_id}"
                    logging.error(f"❌ {ticker} 매도 주문 체결 실패: {error_msg}")
                    db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=0, price=current_price_for_record, order_id=order_id, status='FAILURE', error_message=error_msg)
                    return {"status": "FAILURE", "order_id": order_id, "quantity": None, "price": None, "error": error_msg}
            else:
                error_msg = f"Failed to get sell order details. OrderID: {order_id}"
                logging.error(f"❌ {ticker} 매도 주문 상세 정보 조회 실패: {error_msg}")
                db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=0, price=current_price_for_record, order_id=order_id, status='FAILURE', error_message=error_msg)
                return {"status": "FAILURE", "order_id": order_id, "quantity": None, "price": None, "error": error_msg}

        elif resp and resp.get('error'):
            error_info = resp['error']
            error_msg = error_info.get('message', 'Unknown error')
            logging.error(f"❌ {ticker} 매도 주문 API 오류: {error_msg} (Details: {error_info})")
            db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=sell_quantity, price=current_price_for_record, order_id=None, status='FAILURE', error_message=f"API Error: {error_msg}")
            return {"status": "FAILURE", "order_id": None, "quantity": None, "price": None, "error": f"API Error: {error_msg}"}
        else:
            error_msg = f"No/unexpected API response for sell order: {resp}"
            logging.error(f"❌ {ticker} 매도 주문 실패: {error_msg}")
            db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=sell_quantity, price=current_price_for_record, order_id=None, status='FAILURE', error_message=error_msg)
            return {"status": "FAILURE", "order_id": None, "quantity": None, "price": None, "error": error_msg}

    except pyupbit.UpbitError as ue:
        error_msg = f"Upbit API Error during sell: {str(ue)}"
        logging.error(f"❌ {ticker} 매도 중 Upbit API 오류 발생: {error_msg}")
        db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=0, price=current_price_for_record, order_id=order_id, status='FAILURE', error_message=error_msg)
        return {"status": "FAILURE", "order_id": order_id, "quantity": None, "price": None, "error": error_msg}
    except Exception as e:
        error_msg = f"Unexpected error during sell: {str(e)}"
        logging.error(f"❌ {ticker} 매도 중 예기치 않은 오류 발생: {error_msg}")
        db_mgr.save_trade_record(ticker=ticker, order_type='SELL', quantity=0, price=current_price_for_record, order_id=order_id, status='FAILURE', error_message=error_msg)
        return {"status": "FAILURE", "order_id": order_id, "quantity": None, "price": None, "error": error_msg}

def check_and_execute_trailing_stop(trailing_manager, ticker, current_price, upbit_client):
    if trailing_manager.update(ticker, current_price):
        logging.info(f"📉 {ticker} 트레일링 스탑 조건 충족 → 시장가 매도 실행 (현재가: {current_price})")
        sell_asset(ticker)

class TrailingStopManager:
    def __init__(self, atr_multiplier=1.0, per_ticker_config=None):
        self.atr_multiplier = atr_multiplier
        self.per_ticker_config = per_ticker_config or {}
        self.entry_price = {}
        self.highest_price = {}
        self.atr = {}
        self.stop_price = {}

    def get_percent(self, ticker):
        return self.per_ticker_config.get(ticker, getattr(self, "atr_multiplier", 1.0))

    def update(self, ticker, current_price):
        # Fetch ATR from DB on first use
        if ticker not in self.highest_price:
            # Query ATR from static_indicators table
            conn = psycopg2.connect(
                host=os.getenv("PG_HOST"),
                port=os.getenv("PG_PORT"),
                dbname=os.getenv("PG_DATABASE"),
                user=os.getenv("PG_USER"),
                password=os.getenv("PG_PASSWORD")
            )
            cur = conn.cursor()
            cur.execute("SELECT atr FROM static_indicators WHERE ticker = %s", (ticker,))
            result = cur.fetchone()
            if result is None:
                print(f"[TrailingStopManager] ⚠️ static_indicators에 {ticker} 데이터가 없습니다. ATR=0으로 처리합니다.")
            elif result[0] is None:
                print(f"[TrailingStopManager] ⚠️ {ticker}의 ATR 값이 None입니다. ATR=0으로 처리합니다.")
            atr_value = (result[0] if result and result[0] is not None else 0)
            cur.close()
            conn.close()

            # Initialize position tracking
            self.entry_price[ticker] = current_price
            self.highest_price[ticker] = current_price
            self.atr[ticker] = atr_value

            # Initial stop-loss at entry_price - 1 ATR
            self.stop_price[ticker] = current_price - atr_value
            return False

        # Update high-water mark
        if current_price > self.highest_price[ticker]:
            self.highest_price[ticker] = current_price

        # Calculate dynamic stop levels
        atr_value = self.atr[ticker]
        trail_price = self.highest_price[ticker] - atr_value * self.atr_multiplier
        fixed_stop = self.entry_price[ticker] - atr_value

        # Choose the tighter stop (higher price)
        new_stop = max(trail_price, fixed_stop)
        self.stop_price[ticker] = new_stop

        # If price falls below stop_price, signal exit
        if current_price <= self.stop_price[ticker]:
            return True
        return False

def should_enter_trade(ticker, market_data, gpt_analysis=None):
    """
    GPT 분석 결과와 기술적 분석을 결합하여 매수 여부 판단
    """
    # 기술적 조건: Supertrend 상단 돌파 + MACD histogram 양전환 + ADX 강세
    tech_cond = (
        market_data.get("supertrend") is not None and
        market_data.get("price") > market_data.get("supertrend") and
        market_data.get("macd_histogram", 0) > 0 and
        market_data.get("adx", 0) > 25
    )

    # GPT 분석 결과 확인 (buy 라는 키워드 포함 여부)
    gpt_cond = gpt_analysis and "buy" in gpt_analysis.lower()

    if tech_cond and gpt_cond:
        print(f"🟢 {ticker} 매수 조건 충족 (기술적 + GPT 분석)")
        return True

    print(f"⚪ {ticker} 매수 조건 미충족")
    return False

def should_exit_trade(ticker, market_data, gpt_analysis=None):
    """
    기술적 지표와 GPT 분석을 바탕으로 매도 여부 판단
    """

    # 기술적 조건: Supertrend 하향 돌파 또는 MACD 히스토그램 음전환
    tech_exit = (
        market_data.get("supertrend") is not None and
        market_data.get("price") < market_data.get("supertrend")
    ) or (
        market_data.get("macd_histogram", 0) < 0
    )

    # GPT 분석 결과가 존재하고 'sell' 키워드가 포함된 경우
    gpt_exit = gpt_analysis and "sell" in gpt_analysis.lower()

    # 지지선 하회 조건
    support_break = (
        market_data.get("support") is not None and
        market_data.get("price") < market_data.get("support")
    )

    # 지지선 하회 시 우선 매도
    if support_break:
        print(f"🔻(Support Break) {ticker} 지지선 하회 → 매도")
        return True

    # 기술적 청산 AND GPT 매도 신호
    if tech_exit and gpt_exit:
        print(f"🔻(Tech+GPT) {ticker} 매도 조건 충족 (기술적 + GPT 분석)")
        return True

    print(f"⚪ {ticker} 매도 조건 미충족")
    return False
