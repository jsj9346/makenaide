import psycopg2
import pyupbit
import json
from trade_executor import buy_asset
from dotenv import load_dotenv
import os
import datetime
import logging
from db_manager import DBManager
from utils import load_blacklist, safe_float_convert
import pandas as pd
import numpy as np
load_dotenv()

class PortfolioManager:
    def __init__(self, upbit, risk_pct=0.02, atr_period=14, pyramiding_config=None):
        self.upbit = upbit
        self.risk_pct = risk_pct
        self.atr_period = atr_period
        self.pyramiding_config = pyramiding_config or {}
        self.max_add_ons = self.pyramiding_config.get('max_add_ons', 3)
        self.add_on_ratio = self.pyramiding_config.get('add_on_ratio', 0.5)
        self.purchase_info = {}  # {'KRW-BTC': {'price': 12345678.0, 'timestamp': '2025-04-04T15:00:00'}}
        self.db_mgr = DBManager()

    def get_total_balance(self):
        """총 보유 자산 조회"""
        try:
            balances = self.upbit.get_balances()
            if not isinstance(balances, list):
                logging.error("❌ get_total_balance: balances 반환값이 리스트가 아님")
                return 0
            
            # 블랙리스트 로드
            try:
                blacklist = load_blacklist()
                if not blacklist:
                    logging.warning("⚠️ 블랙리스트가 비어있습니다.")
            except Exception as e:
                logging.error(f"❌ 블랙리스트 로드 중 오류 발생: {str(e)}")
                blacklist = []
            
            total = 0
            for balance in balances:
                try:
                    currency = balance.get('currency')
                    if not currency:
                        continue
                    
                    # 블랙리스트에 포함된 종목 필터링
                    if currency != 'KRW' and f"KRW-{currency}" in blacklist:
                        logging.info(f"⏭️ {currency}는 블랙리스트에 포함되어 제외됩니다.")
                        continue
                    
                    if currency == 'KRW':
                        total += float(balance.get('balance', 0))
                    else:
                        ticker = f"KRW-{currency}"
                        current_price = pyupbit.get_current_price(ticker)
                        if current_price:
                            total += float(balance.get('balance', 0)) * current_price
                        else:
                            logging.warning(f"⚠️ {ticker} 현재가 조회 실패")
                except (ValueError, TypeError) as e:
                    logging.error(f"❌ {currency} 자산 계산 중 오류: {str(e)}")
                    continue
                
            return total
        except Exception as e:
            logging.error(f"❌ 총 자산 조회 중 오류 발생: {str(e)}")
            return 0

    def get_current_positions(self):
        """보유 자산 정보 반환"""
        try:
            balances = self.upbit.get_balances()
            if not isinstance(balances, list):
                logging.error("❌ get_current_positions: balances 반환값이 리스트가 아님")
                return []
            
            # 블랙리스트 로드
            try:
                blacklist = load_blacklist()
                if not blacklist:
                    logging.warning("⚠️ 블랙리스트가 비어있습니다.")
            except Exception as e:
                logging.error(f"❌ 블랙리스트 로드 중 오류 발생: {str(e)}")
                blacklist = []

            filtered = []
            for item in balances:
                try:
                    currency = item.get('currency')
                    if not currency:
                        continue
                    
                    # 블랙리스트에 포함된 종목 필터링
                    if f"KRW-{currency}" in blacklist:
                        logging.info(f"⏭️ {currency}는 블랙리스트에 포함되어 제외됩니다.")
                        continue
                    
                    balance = float(item.get('balance', 0))
                    avg_price = float(item.get('avg_buy_price', 0))
                    
                    if currency == 'KRW':
                        value = balance
                    else:
                        ticker = f"KRW-{currency}"
                        current_price = pyupbit.get_current_price(ticker)
                        if not current_price:
                            logging.warning(f"⚠️ {ticker} 현재가 조회 실패")
                            continue
                        value = balance * current_price
                    
                    if value >= 1.0:  # 1원 미만 자산 제외
                        filtered.append(item)
                except (ValueError, TypeError) as e:
                    logging.error(f"❌ {currency} 포지션 필터링 중 오류: {str(e)}")
                    continue
                
            return filtered
        except Exception as e:
            logging.error(f"❌ 보유 자산 조회 중 오류 발생: {str(e)}")
            return []

    # UNUSED: 호출되지 않는 함수
    # def allocate_funds(self, recommendations):
    #     """
    #     recommendations: [{'ticker': 'KRW-ETH', 'action': 'BUY'}, ...]
    #     - 매수 대상 종목 수만큼 비중을 나눠 자금 할당
    #     """
    #     total_balance = self.get_total_balance()
    #     buy_targets = [r for r in recommendations if r['action'] == 'BUY']
    #     num_targets = len(buy_targets)
    # 
    #     if num_targets == 0:
    #         print("✅ 매수할 종목 없음")
    #         return
    # 
    #     unit_amount = total_balance * 0.98 / num_targets  # 수수료 고려
    # 
    #     for rec in buy_targets:
    #         ticker = rec['ticker']
    #         # ⚠️ buy_asset는 현재 시장가 매수로 동작하며, 단가를 직접 입력하지 않음
    #         buy_asset(ticker, price=0, ratio=unit_amount / self.get_total_balance())
    #         now = datetime.datetime.now().isoformat()
    #         current_price = pyupbit.get_current_price(ticker)
    #         self.purchase_info[ticker] = {'price': current_price, 'timestamp': now}

    # UNUSED: 호출되지 않는 함수
    # def calculate_position_amount(self, ticker, custom_total=None):
    #     """
    #     Kelly 비율과 swing_score를 활용한 포지션 사이징 계산
    #     """
    #     try:
    #         # 전략 성과 데이터 조회
    #         strategy_performance = self.db_mgr.execute_query("""
    #         SELECT 
    #             win_rate,
    #             avg_return,
    #             mdd,
    #             kelly_ratio,
    #             swing_score
    #         FROM strategy_performance 
    #         WHERE strategy_combo = (
    #             SELECT strategy_combo 
    #             FROM trade_log 
    #             WHERE ticker = %s 
    #             ORDER BY executed_at DESC 
    #             LIMIT 1
    #         )
    #     """, (ticker,))
    # 
    #         if not strategy_performance:
    #             # 기본값 설정
    #             win_rate = 0.5
    #             avg_return = 0.02
    #             mdd = 0.1
    #             kelly_ratio = 0.1
    #             swing_score = 0.5
    #         else:
    #             win_rate = strategy_performance[0][0]
    #             avg_return = strategy_performance[0][1]
    #             mdd = strategy_performance[0][2]
    #             kelly_ratio = strategy_performance[0][3]
    #             swing_score = strategy_performance[0][4]
    # 
    #         # Kelly 비율 계산
    #         kelly = (win_rate * avg_return - (1 - win_rate) * mdd) / avg_return
    #         kelly = max(0, min(kelly, 0.5))  # 0~0.5 사이로 제한
    # 
    #         # swing_score 반영
    #         position_ratio = kelly * swing_score
    # 
    #         # 총 자산 계산
    #         total_balance = custom_total or self.get_total_balance()
    #             
    #         # 최종 매수 금액 계산
    #         position_amount = total_balance * position_ratio
    #             
    #         # 최소 주문 금액 체크
    #         if position_amount < 5000:  # 업비트 최소 주문 금액
    #             return 0, 0
    # 
    #         # 현재가 조회
    #         current_price = pyupbit.get_current_price(ticker)
    #         if not current_price:
    #             return 0, 0
    # 
    #         # 매수 수량 계산
    #         quantity = position_amount / current_price
    # 
    #         return position_ratio, quantity
    # 
    #     except Exception as e:
    #         logging.error(f"❌ 포지션 사이징 계산 중 오류 발생: {str(e)}")
    #         return 0, 0

    def check_pyramiding(self, ticker):
        """
        고도화된 피라미딩 전략:
        
        조건 A: 고점 돌파 + 거래량 증가 시 추가 진입
        조건 B: Supertrend 매수 유지 + ADX > 25 + MA20 상승 중 → 직전 진입가보다 5% 이상 상승 시 추가 진입
        
        특징:
        - 최대 피라미딩 횟수 제한: 2~3회
        - 누적 포지션 총액 제한
        - ATR 기반 리스크 조절 포지션 사이징
        - 평균 단가 기반 손절/익절 통합 관리
        """
        try:
            # 포지션 정보 조회
            info = self.purchase_info.get(ticker)
            if not info:
                return False
            
            # 현재가 조회
            current_price = pyupbit.get_current_price(ticker)
            if not current_price:
                logging.warning(f"⚠️ {ticker} 현재가 조회 실패")
                return False
            
            # 초기 설정 (첫 호출 시)
            if 'initialized' not in info:
                self._initialize_pyramiding_info(ticker, info, current_price)
                return False
            
            # 피라미딩 제한 조건 확인
            if not self._check_pyramiding_limits(ticker, info):
                return False
            
            # 시장 데이터 조회
            market_data = self._get_market_data_for_pyramiding(ticker)
            if not market_data:
                logging.warning(f"⚠️ {ticker} 시장 데이터 조회 실패")
                return False
            
            # 고도화된 피라미딩 조건 체크
            pyramid_conditions = self._evaluate_advanced_pyramiding_conditions(
                ticker, current_price, info, market_data
            )
            
            if pyramid_conditions['should_pyramid']:
                # 추가 매수 실행
                return self._execute_pyramiding(ticker, current_price, info, pyramid_conditions)
            else:
                logging.debug(f"📊 {ticker} 피라미딩 조건 미충족: {pyramid_conditions['reason']}")
                return False
                
        except Exception as e:
            logging.error(f"❌ {ticker} 피라미딩 체크 중 오류 발생: {e}")
            return False
    
    def _initialize_pyramiding_info(self, ticker, info, current_price):
        """피라미딩 정보 초기화"""
        try:
            # 기본 정보 설정
            info['initialized'] = True
            info['entry_price'] = info.get('price', current_price)
            info['pyramid_count'] = 0
            info['last_pyramid_price'] = info['entry_price']
            info['total_quantity'] = 0  # 총 보유 수량
            info['avg_entry_price'] = info['entry_price']  # 평균 진입가
            info['total_investment'] = 0  # 총 투자금액
            
            # 시장 데이터 조회
            market_data = self._get_market_data_for_pyramiding(ticker)
            if market_data:
                info['atr'] = market_data.get('atr', 0)
                info['initial_volume'] = market_data.get('volume', 0)
                info['high_water_mark'] = current_price
            
            # 피라미딩 설정
            info['max_pyramids'] = self.pyramiding_config.get('max_add_ons', 3)
            info['pyramid_threshold_pct'] = self.pyramiding_config.get('pyramid_threshold_pct', 5.0)  # 5% 상승 시 추가 매수
            info['max_total_position_pct'] = self.pyramiding_config.get('max_total_position_pct', 8.0)  # 최대 총 포지션 8%
            
            logging.info(f"✅ {ticker} 피라미딩 정보 초기화 완료")
            
        except Exception as e:
            logging.error(f"❌ {ticker} 피라미딩 정보 초기화 실패: {e}")
    
    def _check_pyramiding_limits(self, ticker, info):
        """피라미딩 제한 조건 확인"""
        try:
            # 최대 피라미딩 횟수 체크
            max_pyramids = info.get('max_pyramids', 3)
            if info['pyramid_count'] >= max_pyramids:
                return False
            
            # 총 포지션 한도 체크
            total_balance = self.get_total_balance()
            max_total_position_pct = info.get('max_total_position_pct', 8.0)
            max_total_position_krw = total_balance * (max_total_position_pct / 100)
            
            if info['total_investment'] >= max_total_position_krw:
                logging.debug(f"📊 {ticker} 총 포지션 한도 도달 ({max_total_position_pct}%)")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"❌ {ticker} 피라미딩 제한 조건 체크 실패: {e}")
            return False
    
    def _get_market_data_for_pyramiding(self, ticker):
        """피라미딩을 위한 시장 데이터 조회"""
        try:
            # static_indicators에서 기술적 지표 조회
            query = """
                SELECT atr, rsi_14, adx, ma20, supertrend_signal, volume_ratio, volume
                FROM static_indicators 
                WHERE ticker = %s 
                ORDER BY updated_at DESC 
                LIMIT 1
            """
            result = self.db_mgr.execute_query(query, (ticker,))
            
            if not result:
                return None
            
            atr, rsi, adx, ma20, supertrend_signal, volume_ratio, volume = result[0]
            
            # OHLCV 데이터에서 최근 고점 조회
            ohlcv_data = self._get_ohlcv_from_db(ticker, limit=20)
            recent_high = 0
            if not ohlcv_data.empty:
                recent_high = ohlcv_data['high'].max()
            
            return {
                'atr': safe_float_convert(atr, context=f"{ticker} ATR"),
                'rsi': safe_float_convert(rsi, context=f"{ticker} RSI"),
                'adx': safe_float_convert(adx, context=f"{ticker} ADX"),
                'ma20': safe_float_convert(ma20, context=f"{ticker} MA20"),
                'supertrend_signal': supertrend_signal,
                'volume_ratio': safe_float_convert(volume_ratio, context=f"{ticker} Volume Ratio"),
                'volume': safe_float_convert(volume, context=f"{ticker} Volume"),
                'recent_high': recent_high
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 시장 데이터 조회 실패: {e}")
            return None
    
    def _evaluate_advanced_pyramiding_conditions(self, ticker, current_price, info, market_data):
        """고도화된 피라미딩 조건 평가"""
        try:
            conditions_met = []
            conditions_failed = []
            
            # 기본 정보
            last_pyramid_price = info.get('last_pyramid_price', info['entry_price'])
            pyramid_threshold_pct = info.get('pyramid_threshold_pct', 5.0)
            
            # 조건 A: 고점 돌파 + 거래량 증가
            recent_high = market_data.get('recent_high', 0)
            volume_ratio = market_data.get('volume_ratio', 1.0)
            
            high_breakout = current_price > recent_high * 1.01  # 최근 고점 1% 돌파
            volume_surge = volume_ratio > 1.3  # 거래량 30% 증가
            
            condition_a = high_breakout and volume_surge
            if condition_a:
                conditions_met.append("고점돌파+거래량증가")
            else:
                conditions_failed.append(f"고점돌파({high_breakout})+거래량증가({volume_surge})")
            
            # 조건 B: Supertrend 매수 유지 + ADX > 25 + MA20 상승 중 + 5% 이상 상승
            supertrend_bullish = market_data.get('supertrend_signal') == 'bull'
            adx_strong = market_data.get('adx', 0) > 25
            ma20_rising = current_price > market_data.get('ma20', current_price)
            price_advance = (current_price - last_pyramid_price) / last_pyramid_price * 100 >= pyramid_threshold_pct
            
            condition_b = supertrend_bullish and adx_strong and ma20_rising and price_advance
            if condition_b:
                conditions_met.append("Supertrend+ADX+MA20+상승")
            else:
                conditions_failed.append(f"Supertrend({supertrend_bullish})+ADX({adx_strong})+MA20({ma20_rising})+상승({price_advance})")
            
            # 추가 안전 조건들
            rsi = market_data.get('rsi', 50)
            rsi_not_overbought = rsi < 75  # RSI 과매수 방지
            if rsi_not_overbought:
                conditions_met.append("RSI정상")
            else:
                conditions_failed.append(f"RSI과매수({rsi})")
            
            # 최종 판단
            should_pyramid = (condition_a or condition_b) and rsi_not_overbought
            
            # 피라미딩 크기 계산
            pyramid_size_pct = self._calculate_pyramid_position_size(info, market_data, current_price)
            
            return {
                'should_pyramid': should_pyramid,
                'condition_a': condition_a,
                'condition_b': condition_b,
                'conditions_met': conditions_met,
                'conditions_failed': conditions_failed,
                'pyramid_size_pct': pyramid_size_pct,
                'reason': f"조건충족: {conditions_met}" if should_pyramid else f"조건미충족: {conditions_failed}"
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 피라미딩 조건 평가 실패: {e}")
            return {'should_pyramid': False, 'reason': f"평가 실패: {e}"}
    
    def _calculate_pyramid_position_size(self, info, market_data, current_price):
        """피라미딩 포지션 크기 계산 (ATR 기반 리스크 조절)"""
        try:
            # 기본 피라미딩 비율
            base_pyramid_ratio = self.pyramiding_config.get('add_on_ratio', 0.5)  # 초기 진입의 50%
            
            # ATR 기반 변동성 조정
            atr = market_data.get('atr', 0)
            if atr > 0:
                volatility_factor = min(atr / current_price * 100, 10) / 10  # 0~1 범위로 정규화
                # 변동성이 높을수록 포지션 크기 축소
                volatility_adjustment = 1 - (volatility_factor * 0.3)  # 최대 30% 축소
            else:
                volatility_adjustment = 1.0
            
            # 피라미딩 횟수에 따른 크기 조정 (점진적 축소)
            pyramid_count = info.get('pyramid_count', 0)
            size_decay_factor = 1.0 / (1 + pyramid_count * 0.3)  # 피라미딩 횟수마다 30% 감소
            
            # 최종 피라미딩 크기 계산
            final_pyramid_ratio = base_pyramid_ratio * volatility_adjustment * size_decay_factor
            
            # 최소/최대 한도 적용
            final_pyramid_ratio = max(0.1, min(final_pyramid_ratio, 1.0))  # 10%~100% 범위
            
            # 총 자산 대비 퍼센트로 변환
            total_balance = self.get_total_balance()
            initial_ratio = info.get('initial_ratio', 0.02)  # 초기 진입 비율
            pyramid_size_pct = initial_ratio * final_pyramid_ratio
            
            return pyramid_size_pct
            
        except Exception as e:
            logging.error(f"❌ 피라미딩 포지션 크기 계산 실패: {e}")
            return 0.01  # 기본값 1%
    
    def _execute_pyramiding(self, ticker, current_price, info, conditions):
        """피라미딩 매수 실행"""
        try:
            pyramid_size_pct = conditions['pyramid_size_pct']
            total_balance = self.get_total_balance()
            pyramid_amount_krw = total_balance * pyramid_size_pct
            
            # 최소 주문 금액 체크
            if pyramid_amount_krw < 5000:  # 업비트 최소 주문 금액
                logging.warning(f"⚠️ {ticker} 피라미딩 금액({pyramid_amount_krw:.0f}원)이 최소 주문 금액 미만")
                return False
            
            # 매수 실행
            from trade_executor import buy_asset
            access_key = os.getenv("UPBIT_ACCESS_KEY")
            secret_key = os.getenv("UPBIT_SECRET_KEY")
            
            if not access_key or not secret_key:
                logging.error(f"❌ {ticker} 피라미딩 매수 실패: API 키 설정 안됨")
                return False
            
            upbit = pyupbit.Upbit(access_key, secret_key)
            buy_result = buy_asset(upbit, ticker, current_price, pyramid_amount_krw)
            
            if buy_result and buy_result.get('status') == 'SUCCESS':
                # 피라미딩 정보 업데이트
                pyramid_count = info['pyramid_count'] + 1
                executed_quantity = buy_result.get('quantity', 0)
                executed_price = buy_result.get('price', current_price)
                
                # 평균 진입가 업데이트
                prev_total_value = info['total_quantity'] * info['avg_entry_price']
                new_total_value = executed_quantity * executed_price
                new_total_quantity = info['total_quantity'] + executed_quantity
                new_avg_price = (prev_total_value + new_total_value) / new_total_quantity if new_total_quantity > 0 else executed_price
                
                # 정보 업데이트
                info.update({
                    'pyramid_count': pyramid_count,
                    'last_pyramid_price': current_price,
                    'total_quantity': new_total_quantity,
                    'avg_entry_price': new_avg_price,
                    'total_investment': info['total_investment'] + pyramid_amount_krw,
                    'high_water_mark': max(info.get('high_water_mark', current_price), current_price),
                    'last_pyramid_timestamp': datetime.datetime.now().isoformat()
                })
                
                # 로그 기록
                logging.info(f"🔼 {ticker} 피라미딩 매수 #{pyramid_count} 실행완료")
                logging.info(f"   💰 매수금액: {pyramid_amount_krw:,.0f}원 ({pyramid_size_pct:.2f}%)")
                logging.info(f"   📊 매수가격: {executed_price:,.2f}원, 수량: {executed_quantity:.8f}")
                logging.info(f"   📈 새로운 평균가: {new_avg_price:,.2f}원")
                logging.info(f"   ✅ 조건: {conditions['conditions_met']}")
                
                # 피라미딩 로그 DB에 기록
                self._log_pyramiding_transaction(ticker, pyramid_count, executed_price, executed_quantity, 
                                               pyramid_amount_krw, conditions['reason'])
                
                return True
            else:
                error_msg = buy_result.get('error') if buy_result else "Unknown error"
                logging.error(f"❌ {ticker} 피라미딩 매수 실패: {error_msg}")
                return False
                
        except Exception as e:
            logging.error(f"❌ {ticker} 피라미딩 실행 중 오류 발생: {e}")
            return False
    
    def _log_pyramiding_transaction(self, ticker, pyramid_level, price, quantity, amount_krw, reason):
        """피라미딩 거래 로그 기록"""
        try:
            log_data = {
                'ticker': ticker,
                'action': 'pyramid_buy',
                'pyramid_level': pyramid_level,
                'price': price,
                'quantity': quantity,
                'amount_krw': amount_krw,
                'reason': reason,
                'timestamp': datetime.datetime.now()
            }
            
            # trade_log 테이블에 기록 (trading_log에서 통합됨)
            insert_query = """
                INSERT INTO trade_log (ticker, action, price, qty, executed_at, status, strategy_combo)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            strategy_combo = f"피라미딩 #{pyramid_level}: {reason}, 금액: {amount_krw:,.0f}원"
            
            self.db_mgr.execute_query(insert_query, (
                ticker, 'pyramid_buy', price, quantity, 
                datetime.datetime.now(), 'completed', strategy_combo
            ))
            
            logging.info(f"📝 {ticker} 피라미딩 거래 로그 기록 완료")
            
        except Exception as e:
            logging.error(f"❌ {ticker} 피라미딩 거래 로그 기록 실패: {e}")
    
    # UNUSED: 호출되지 않는 함수
    # def get_pyramiding_status(self, ticker):
    #     """피라미딩 상태 조회"""
    #     try:
    #         info = self.purchase_info.get(ticker)
    #         if not info or not info.get('initialized'):
    #             return None
    #             
    #         current_price = pyupbit.get_current_price(ticker)
    #         if not current_price:
    #             return None
    #             
    #         # 수익률 계산
    #         avg_entry_price = info.get('avg_entry_price', info.get('entry_price', current_price))
    #         total_return_pct = (current_price - avg_entry_price) / avg_entry_price * 100
    #             
    #         return {
    #             'ticker': ticker,
    #             'pyramid_count': info.get('pyramid_count', 0),
    #             'max_pyramids': info.get('max_pyramids', 3),
    #             'avg_entry_price': avg_entry_price,
    #             'current_price': current_price,
    #             'total_return_pct': total_return_pct,
    #             'total_quantity': info.get('total_quantity', 0),
    #             'total_investment': info.get('total_investment', 0),
    #             'high_water_mark': info.get('high_water_mark', current_price),
    #             'last_pyramid_price': info.get('last_pyramid_price', avg_entry_price)
    #         }
    #             
    #     except Exception as e:
    #         logging.error(f"❌ {ticker} 피라미딩 상태 조회 실패: {e}")
    #         return None

    # UNUSED: 호출되지 않는 함수
    # def get_portfolio_breakdown(self):
    #     """
    #     현재 포트폴리오 내 각 자산(코인 및 현금)의 평가금액 및 비중을 계산하여 출력
    #     """
    #     balances = self.upbit.get_balances()
    #     print("[DEBUG] balances:", balances)
    #     breakdown = []
    #     total_value = 0
    # 
    #     # 1차: 전체 평가금액 계산
    #     for item in balances:
    #         currency = item['currency']
    #         balance = float(item['balance'])
    #         avg_price = float(item['avg_buy_price'])
    # 
    #         if currency == "KRW":
    #             value = balance
    #         else:
    #             value = balance * avg_price
    # 
    #         total_value += value
    # 
    #     # 2차: 비중 계산 및 출력
    #     print("📊 포트폴리오 현황:")
    #     for item in balances:
    #         currency = item['currency']
    #         balance = float(item['balance'])
    #         avg_price = float(item['avg_buy_price'])
    # 
    #         if currency == "KRW":
    #             value = balance
    #             ticker = "KRW"
    #         else:
    #             value = balance * avg_price
    #             ticker = f"KRW-{currency}"
    # 
    #         if total_value > 0:
    #             percent = (value / total_value) * 100
    #         else:
    #             percent = 0
    # 
    #         print(f"[{ticker}] 비중: {percent:.2f}%, 평가금액: {value:,.0f}원")

    # UNUSED: 호출되지 않는 함수
    # def rebalance(self):
    #     # TODO: 리밸런싱 전략 추후 구현
    #     pass

    # UNUSED: 호출되지 않는 함수
    # def show_portfolio_summary(self):
    #     """
    #     현재 포트폴리오 상태를 요약하여 터미널에 출력
    #     - 현재 보유 종목별 티커, 수량, 평균단가, 현재가, 평가금액, 손익률, 손익금액, 자산 비중 표시
    #     - 현금 보유량과 비중 표시
    #     - 전체 자산 = 현금 + 모든 종목 평가금액의 합계
    #     """
    #     # 함수 실행 시작 로그 - 명확한 표시
    #     logging.info("===== PORTFOLIO_SUMMARY_START =====")
    #         
    #     summary_lines = ["\n======== 포트폴리오 요약 ========"]
    #         
    #     # 현재 보유 종목 정보 가져오기
    #     balances = self.upbit.get_balances()
    #         
    #     # 블랙리스트 로드
    #     try:
    #         blacklist = load_blacklist()
    #         if not blacklist:
    #             logging.warning("⚠️ 블랙리스트가 비어있습니다.")
    #     except Exception as e:
    #         logging.error(f"❌ 블랙리스트 로드 중 오류 발생: {str(e)}")
    #         blacklist = []
    #         
    #     # 전체 평가 금액 계산
    #     total_value = 0
    #     positions = []
    #     cash = 0
    #         
    #     # 각 종목별 정보 수집 및 전체 평가 금액 계산
    #     for item in balances:
    #         currency = item['currency']
    #         balance = float(item['balance'])
    #         avg_price = float(item['avg_buy_price'])
    #             
    #         if currency == "KRW":
    #             cash = balance
    #             total_value += cash
    #             continue
    #             
    #         ticker = f"KRW-{currency}"
    #             
    #         # 블랙리스트에 포함된 종목 필터링
    #         if ticker in blacklist:
    #             logging.info(f"⏭️ {ticker}는 블랙리스트에 포함되어 요약에서 제외됩니다.")
    #             continue
    #                 
    #         current_price = pyupbit.get_current_price(ticker)
    #             
    #         if not current_price:
    #             logging.warning(f"⚠️ {ticker} 현재가 조회 실패")
    #             continue
    #                 
    #         # 평가 금액 계산
    #         evaluation = balance * current_price
    #         total_value += evaluation
    #             
    #         # 손익률, 손익금액 계산
    #         profit_loss = evaluation - (balance * avg_price)
    #         profit_loss_pct = (current_price / avg_price - 1) * 100
    #             
    #         positions.append({
    #             'ticker': ticker,
    #             'balance': balance,
    #             'avg_price': avg_price,
    #             'current_price': current_price,
    #             'evaluation': evaluation,
    #             'profit_loss': profit_loss,
    #             'profit_loss_pct': profit_loss_pct
    #         })
    #         
    #     # 정보 출력 - 보유 종목
    #     if positions:
    #         summary_lines.append("\n[보유 종목]")
    #         summary_lines.append(f"{'티커':>10} | {'수량':>12} | {'평균단가':>12} | {'현재가':>12} | {'평가금액':>12} | {'손익률':>8} | {'손익금액':>12} | {'비중':>6}")
    #         summary_lines.append("-" * 100)
    #             
    #         for pos in positions:
    #             ticker = pos['ticker']
    #             balance = pos['balance']
    #             avg_price = pos['avg_price']
    #             current_price = pos['current_price']
    #             evaluation = pos['evaluation']
    #             profit_loss = pos['profit_loss']
    #             profit_loss_pct = pos['profit_loss_pct']
    #             weight = (evaluation / total_value) * 100 if total_value > 0 else 0
    #                 
    #             # 부호 표시
    #             profit_loss_sign = "+" if profit_loss >= 0 else ""
    #             profit_loss_pct_sign = "+" if profit_loss_pct >= 0 else ""
    #                 
    #             summary_lines.append(f"{ticker:>10} | {balance:>12,.8f} | {avg_price:>12,.2f} | {current_price:>12,.2f} | {evaluation:>12,.2f} | {profit_loss_pct_sign}{profit_loss_pct:>6.2f}% | {profit_loss_sign}{profit_loss:>10,.2f} | {weight:>5.2f}%")
    #     else:
    #         summary_lines.append("\n보유 종목이 없습니다.")
    #         
    #     # 정보 출력 - 현금
    #     cash_weight = (cash / total_value) * 100 if total_value > 0 else 0
    #     summary_lines.append("\n[현금]")
    #     summary_lines.append(f"{'보유현금':>10} | {cash:>12,.2f}원 | {'비중':>6} | {cash_weight:>5.2f}%")
    #         
    #     # 정보 출력 - 전체 자산
    #     summary_lines.append("\n[전체 자산]")
    #     summary_lines.append(f"{'총 평가금액':>10} | {total_value:>12,.2f}원")
    #     summary_lines.append("\n===============================\n")
    #         
    #     # 로그로 출력하고 동시에 터미널에도 출력
    #     for line in summary_lines:
    #         logging.info(line)
    #         print(line)
    #             
    #     # 함수 실행 종료 로그 - 명확한 표시
    #     logging.info("===== PORTFOLIO_SUMMARY_END =====")

    def simple_portfolio_summary(self):
        """
        보유 종목의 포지션 요약을 터미널과 로그에 직접 출력합니다.
        예제 코드를 참고한 단순하고 명확한 출력 방식 사용.
        """
        try:
            # 명확한 로그 식별자
            logging.info("===== SIMPLE_PORTFOLIO_SUMMARY_START =====")
            print("\n===== 📦 현재 보유 포지션 요약 =====")
            
            balances = self.upbit.get_balances()
            if not balances:
                logging.info("보유 중인 자산이 없습니다.")
                print("보유 중인 자산이 없습니다.")
                return
            
            # 블랙리스트 로드
            try:
                blacklist = load_blacklist()
                if not blacklist:
                    logging.warning("⚠️ 블랙리스트가 비어있습니다.")
            except Exception as e:
                logging.error(f"❌ 블랙리스트 로드 중 오류 발생: {str(e)}")
                blacklist = []
                
            total_valuation = 0
            portfolio_data = {}
            cash_krw = 0
            
            # 1차: 데이터 준비 및 총 평가금액 계산
            for item in balances:
                currency = item['currency']
                balance = float(item['balance'])
                avg_price = float(item['avg_buy_price'])
                
                if currency == "KRW":
                    cash_krw = balance
                    total_valuation += cash_krw
                    continue
                
                # 블랙리스트에 포함된 종목 필터링
                ticker = f"KRW-{currency}"
                if ticker in blacklist:
                    logging.info(f"⏭️ {ticker}는 블랙리스트에 포함되어 요약에서 제외됩니다.")
                    continue
                
                # API 오류 처리 개선
                try:
                    current_price = pyupbit.get_current_price(ticker)
                    if not current_price:
                        logging.warning(f"⚠️ {ticker} 현재가 조회 실패 (응답이 없음)")
                        
                        # 현재가 조회 실패 시 평균 매수가로 대체 (임시)
                        current_price = avg_price
                        logging.warning(f"⚠️ {ticker} 현재가를 평균 매수가({avg_price:.2f})로 대체합니다.")
                except Exception as e:
                    error_msg = str(e)
                    logging.warning(f"⚠️ {ticker} 현재가 조회 실패: {error_msg}")
                    
                    # 'Code not found' 에러 처리
                    if "Code not found" in error_msg:
                        logging.warning(f"⚠️ {ticker}는 현재 거래가 지원되지 않거나 상장 폐지된 종목일 수 있습니다.")
                    
                    # 현재가 조회 실패 시 평균 매수가로 대체 (임시)
                    current_price = avg_price
                    logging.warning(f"⚠️ {ticker} 현재가를 평균 매수가({avg_price:.2f})로 대체합니다.")
                
                portfolio_data[ticker] = {
                    'quantity': balance,
                    'avg_price': avg_price,
                    'current_price': current_price
                }
                
                valuation = balance * current_price
                total_valuation += valuation
                portfolio_data[ticker]['valuation'] = valuation
            
            # 2차: 출력
            for ticker, data in portfolio_data.items():
                avg_price = data['avg_price']
                quantity = data['quantity']
                current_price = data['current_price']
                valuation = data['valuation']
                
                # 수익률 계산
                pnl_rate = ((current_price - avg_price) / avg_price) * 100
                pnl_value = (current_price - avg_price) * quantity
                ratio = (valuation / total_valuation) * 100 if total_valuation > 0 else 0
                
                # 티커에 대한 추가 정보 (현재가 조회 실패 시 표시)
                price_status = ""
                if current_price == avg_price:
                    price_status = " (추정)"
                
                print(f"\n📊 {ticker}{price_status}")
                print(f" ├ 보유 수량 : {quantity:.8f}")
                print(f" ├ 평균 단가 : {avg_price:,.2f} KRW")
                print(f" ├ 현재가    : {current_price:,.2f} KRW{price_status}")
                print(f" ├ 평가 금액 : {valuation:,.2f} KRW")
                print(f" ├ 손익률    : {pnl_rate:+.2f}%")
                print(f" ├ 손익 금액 : {pnl_value:+,.2f} KRW")
                print(f" └ 자산 비중 : {ratio:.2f}%")
                
                # 동일한 정보를 로그에도 기록
                logging.info(f"📊 {ticker}{price_status}")
                logging.info(f" ├ 보유 수량 : {quantity:.8f}")
                logging.info(f" ├ 평균 단가 : {avg_price:,.2f} KRW")
                logging.info(f" ├ 현재가    : {current_price:,.2f} KRW{price_status}")
                logging.info(f" ├ 평가 금액 : {valuation:,.2f} KRW")
                logging.info(f" ├ 손익률    : {pnl_rate:+.2f}%")
                logging.info(f" ├ 손익 금액 : {pnl_value:+,.2f} KRW")
                logging.info(f" └ 자산 비중 : {ratio:.2f}%")
            
            # 현금 및 전체 자산 정보 출력
            cash_ratio = (cash_krw / total_valuation) * 100 if total_valuation > 0 else 0
            
            print(f"\n💰 보유 현금 : {cash_krw:,.2f} KRW")
            print(f"💼 전체 자산 : {total_valuation:,.2f} KRW")
            print(f"🔢 현금 비중 : {cash_ratio:.2f}%")
            print("\n===============================\n")
            
            logging.info(f"💰 보유 현금 : {cash_krw:,.2f} KRW")
            logging.info(f"💼 전체 자산 : {total_valuation:,.2f} KRW")
            logging.info(f"🔢 현금 비중 : {cash_ratio:.2f}%")
            
            # 명확한 로그 식별자
            logging.info("===== SIMPLE_PORTFOLIO_SUMMARY_END =====")
            
        except Exception as e:
            err_msg = f"❌ 포트폴리오 요약 출력 중 오류 발생: {str(e)}"
            logging.error(err_msg)
            print(err_msg)
            
            # 스택 트레이스 출력 (디버깅용)
            import traceback
            logging.error(traceback.format_exc())

    def check_advanced_sell_conditions(self, portfolio_data=None):
        """
        🔧 [5단계 개선] 켈리 공식 + ATR 통합 고도화된 매도 조건 점검
        
        ✅ 손절매 조건 (켈리 공식 기반)
        - 켈리 공식 기반 동적 손절가 계산
        - ATR 기반 변동성 조정 손절
        - 포트폴리오 리스크 기반 손절
        
        ✅ 이익실현 조건 (켈리 공식 기반)
        - 켈리 공식 기반 동적 익절가 계산
        - ATR 기반 트레일링 스탑 강화
        - 시장 상황 기반 동적 조정
        - 포트폴리오 밸런싱 기반 매도
        
        Args:
            portfolio_data: 포트폴리오 데이터 (None이면 자동 조회)
        """
        try:
            # 포트폴리오 데이터 조회 (없으면 자동 조회)
            if portfolio_data is None:
                portfolio_data = self.get_current_positions()
                
            if not portfolio_data:
                logging.warning("⚠️ 매도 조건 점검 실패: 포트폴리오 데이터 없음")
                return
                
            # DataFrame이 아닌 경우 변환 시도
            if not isinstance(portfolio_data, pd.DataFrame):
                # 리스트인 경우 DataFrame으로 변환
                if isinstance(portfolio_data, list):
                    try:
                        # 딕셔너리 리스트를 DataFrame으로 변환
                        portfolio_data = pd.DataFrame(portfolio_data)
                        
                        # 'currency' 필드가 있고 'ticker' 필드가 없는 경우 변환
                        if 'currency' in portfolio_data.columns and 'ticker' not in portfolio_data.columns:
                            # 'KRW-' 접두사 추가하여 티커 생성 (KRW는 제외)
                            portfolio_data['ticker'] = portfolio_data['currency'].apply(
                                lambda x: f"KRW-{x}" if x != 'KRW' else x
                            )
                            
                        # 인덱스 설정 (티커를 인덱스로)
                        if 'ticker' in portfolio_data.columns:
                            portfolio_data = portfolio_data.set_index('ticker')
                        elif 'currency' in portfolio_data.columns:
                            temp_tickers = portfolio_data['currency'].apply(
                                lambda x: f"KRW-{x}" if x != 'KRW' else x
                            )
                            portfolio_data = portfolio_data.set_index(temp_tickers)
                            
                        # 'avg_buy_price' 필드를 'avg_price'로 변환
                        if 'avg_buy_price' in portfolio_data.columns and 'avg_price' not in portfolio_data.columns:
                            portfolio_data['avg_price'] = portfolio_data['avg_buy_price']
                            
                    except Exception as e:
                        logging.error(f"❌ 포트폴리오 데이터 변환 실패: {str(e)}")
                        return
                # 다른 타입인 경우 종료
                else:
                    logging.error(f"❌ 포트폴리오 데이터 타입 오류: {type(portfolio_data)}")
                    return
                
            # 비어있는지 확인    
            if portfolio_data.empty:
                logging.warning("⚠️ 매도 조건 점검 실패: 포트폴리오 데이터가 비어있음")
                return
                
            # 시장 데이터 조회
            from filter_tickers import fetch_static_indicators_data, fetch_market_data_4h
            market_df = fetch_static_indicators_data()
            
            if market_df is None or market_df.empty:
                logging.warning("⚠️ 매도 조건 점검 실패: 시장 데이터 없음")
                return
            
            # 4시간봉 데이터 조회 (4시간봉 데이터는 이미 정리되었으므로 조회하지 않음)
            market_df_4h = None
                
            # 각 보유 종목에 대해 고도화된 매도 조건 점검
            from datetime import datetime, timedelta
            
            # KRW를 제외한 항목만 처리
            for ticker in portfolio_data.index:
                try:
                    # KRW는 처리하지 않음
                    if ticker == 'KRW':
                        continue

                    # 티커 형식 확인 및 변환
                    ticker_krw = f"KRW-{ticker.replace('KRW-', '')}" if ticker != 'KRW' else ticker
                    
                    # 현재가 조회
                    current_price = pyupbit.get_current_price(ticker_krw)
                    if current_price is None:
                        logging.warning(f"⚠️ {ticker_krw} 현재가 조회 실패")
                        continue
                        
                    # 평균 매수가 및 수량 조회
                    avg_price = None
                    balance = None
                    
                    if 'avg_price' in portfolio_data.columns:
                        avg_price = safe_float_convert(portfolio_data.loc[ticker, 'avg_price'], context=f"{ticker_krw} avg_price")
                    elif 'avg_buy_price' in portfolio_data.columns:
                        avg_price = safe_float_convert(portfolio_data.loc[ticker, 'avg_buy_price'], context=f"{ticker_krw} avg_buy_price")
                    else:
                        avg_price = None
                        
                    if 'balance' in portfolio_data.columns:
                        balance = safe_float_convert(portfolio_data.loc[ticker, 'balance'], context=f"{ticker_krw} balance")
                    else:
                        balance = None
                    
                    if avg_price is None or avg_price <= 0 or balance is None or balance <= 0:
                        logging.warning(f"⚠️ {ticker_krw} 평균 매수가 또는 수량 정보 없음 (avg_price: {avg_price}, balance: {balance})")
                        continue
                        
                    # 수익률 계산
                    return_rate = (current_price - avg_price) / avg_price * 100
                    
                    # 기술적 지표 데이터 조회
                    ticker_data = market_df.loc[ticker_krw] if ticker_krw in market_df.index else None
                    ticker_data_4h = market_df_4h.loc[ticker_krw] if market_df_4h is not None and ticker_krw in market_df_4h.index else None
                    
                    if ticker_data is None:
                        logging.warning(f"⚠️ {ticker_krw} 기술적 지표 데이터 없음")
                        continue
                    
                    # ATR 및 기타 지표 조회
                    atr = safe_float_convert(ticker_data.get('atr', 0), context=f"{ticker_krw} ATR")
                    rsi = safe_float_convert(ticker_data.get('rsi_14', 50), context=f"{ticker_krw} RSI")
                    ma20 = safe_float_convert(ticker_data.get('ma20', current_price), context=f"{ticker_krw} MA20")
                    macd = safe_float_convert(ticker_data.get('macd', 0), context=f"{ticker_krw} MACD")
                    macd_signal = safe_float_convert(ticker_data.get('macd_signal', 0), context=f"{ticker_krw} MACD Signal")
                    bb_upper = safe_float_convert(ticker_data.get('bb_upper', current_price * 1.1), context=f"{ticker_krw} BB Upper")
                    bb_lower = safe_float_convert(ticker_data.get('bb_lower', current_price * 0.9), context=f"{ticker_krw} BB Lower")
                    
                    # 보유기간 계산 (trading_log에서 최근 매수 시점 조회)
                    holding_days = self._calculate_holding_days(ticker_krw)
                    
                    # OHLCV 데이터에서 최고가 조회 (trailing stop용)
                    ohlcv_data = self._get_ohlcv_from_db(ticker_krw, limit=30)
                    max_price_since_buy = current_price
                    
                    if ohlcv_data is not None and not ohlcv_data.empty:
                        max_price_since_buy = ohlcv_data['high'].max()
                    
                    # 저항선 계산 (최근 30일 고점들의 평균)
                    resistance_level = self._calculate_resistance_level(ticker_krw, ohlcv_data)
                    
                    # 🔧 [5단계 개선] 켈리 공식 기반 매도 조건 계산
                    kelly_sell_conditions = self._calculate_kelly_based_sell_conditions(
                        ticker_krw, current_price, avg_price, atr, return_rate, 
                        max_price_since_buy, holding_days, market_df
                    )
                    
                    # 매도 조건 체크 시작
                    sell_reason = None
                    sell_type = None
                    
                    # ========== 1. 켈리 공식 기반 손절매 조건 ==========
                    
                    # 조건 A: 켈리 공식 기반 동적 손절가
                    if kelly_sell_conditions['stop_loss_triggered']:
                        sell_reason = kelly_sell_conditions['stop_loss_reason']
                        sell_type = "kelly_stop_loss"
                    
                    # 조건 B: ATR 기반 변동성 조정 손절
                    elif atr > 0:
                        atr_ratio = atr / current_price
                        # 변동성에 따른 동적 손절 비율 조정
                        if atr_ratio > 0.05:  # 고변동성
                            atr_multiplier = 1.5  # 더 보수적
                        elif atr_ratio > 0.03:  # 중변동성
                            atr_multiplier = 2.0  # 기본
                        else:  # 저변동성
                            atr_multiplier = 2.5  # 더 관대
                        
                        atr_stop_loss_pct = min(max((atr / avg_price) * 100 * atr_multiplier, 2.0), 10.0)
                        if return_rate <= -atr_stop_loss_pct:
                            sell_reason = f"ATR 기반 동적 손절 (수익률: {return_rate:.1f}%, 기준: -{atr_stop_loss_pct:.1f}%, 변동성: {atr_ratio:.2%})"
                            sell_type = "atr_dynamic_stop_loss"
                    
                    # ========== 2. 켈리 공식 기반 이익실현 조건 ==========
                    
                    # 조건 A: 켈리 공식 기반 동적 익절가
                    if not sell_reason and kelly_sell_conditions['take_profit_triggered']:
                        sell_reason = kelly_sell_conditions['take_profit_reason']
                        sell_type = "kelly_take_profit"
                    
                    # 조건 B: ATR 기반 강화된 트레일링 스탑
                    if not sell_reason and atr > 0 and max_price_since_buy > avg_price * 1.03:  # 3% 이상 상승했을 때만
                        # 변동성에 따른 동적 트레일링 스탑
                        if atr_ratio > 0.05:  # 고변동성
                            trailing_multiplier = 1.5  # 더 관대
                        elif atr_ratio > 0.03:  # 중변동성
                            trailing_multiplier = 2.0  # 기본
                        else:  # 저변동성
                            trailing_multiplier = 2.5  # 더 보수적
                        
                        trailing_stop_pct = min(max((atr / current_price) * 100 * trailing_multiplier, 1.5), 8.0)
                        drawdown_from_peak = (max_price_since_buy - current_price) / max_price_since_buy * 100
                        
                        if drawdown_from_peak >= trailing_stop_pct:
                            sell_reason = f"ATR 기반 강화 트레일링 스탑 (고점 대비 -{drawdown_from_peak:.1f}%, 기준: -{trailing_stop_pct:.1f}%, 변동성: {atr_ratio:.2%})"
                            sell_type = "atr_enhanced_trailing_stop"
                    
                    # 조건 C: 시장 상황 기반 동적 익절
                    if not sell_reason:
                        market_based_exit = self._check_market_based_exit_conditions(
                            ticker_krw, current_price, avg_price, return_rate, 
                            rsi, ma20, macd, macd_signal, bb_upper, bb_lower, holding_days
                        )
                        if market_based_exit['should_exit']:
                            sell_reason = market_based_exit['reason']
                            sell_type = market_based_exit['type']
                    
                    # 조건 D: 포트폴리오 밸런싱 기반 매도
                    if not sell_reason:
                        portfolio_based_exit = self._check_portfolio_based_exit_conditions(
                            ticker_krw, current_price, avg_price, return_rate, 
                            portfolio_data, market_df
                        )
                        if portfolio_based_exit['should_exit']:
                            sell_reason = portfolio_based_exit['reason']
                            sell_type = portfolio_based_exit['type']
                    
                    # ========== 매도 실행 ==========
                    if sell_reason:
                        logging.info(f"🔴 {ticker_krw} 켈리 기반 매도 조건 충족: {sell_reason}")
                        
                        # 매도 실행 (trade_executor.py의 sell_asset 함수 사용)
                        from trade_executor import sell_asset
                        sell_result = sell_asset(ticker_krw)
                        
                        if sell_result and sell_result.get('status') == 'SUCCESS':
                            # 매도 로그 기록
                            self._log_sell_decision(ticker_krw, current_price, avg_price, return_rate, 
                                                   sell_type, sell_reason, holding_days)
                            logging.info(f"✅ {ticker_krw} 켈리 기반 매도 완료: {sell_reason}")
                        else:
                            error_msg = sell_result.get('error') if sell_result else "Unknown error"
                            logging.error(f"❌ {ticker_krw} 매도 실패: {sell_reason} - {error_msg}")
                    else:
                        # 매도 조건 미충족 시 상태 로깅
                        logging.debug(f"📊 {ticker_krw} 켈리 기반 매도 조건 미충족 - 수익률: {return_rate:.1f}%, "
                                   f"보유기간: {holding_days}일, RSI: {rsi:.1f}, 현재가: {current_price:,.0f}")
                        
                except Exception as e:
                    logging.error(f"❌ {ticker} 켈리 기반 매도 조건 점검 중 오류 발생: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"❌ 켈리 기반 매도 조건 점검 중 오류 발생: {e}")
            raise
    
    def _calculate_holding_days(self, ticker):
        """보유기간 계산 (trade_log에서 최근 매수 시점 조회)"""
        try:
            query = """
                SELECT executed_at 
                FROM trade_log 
                WHERE ticker = %s AND action = 'buy' 
                ORDER BY executed_at DESC 
                LIMIT 1
            """
            result = self.db_mgr.execute_query(query, (ticker,))
            
            if result and len(result) > 0:
                buy_date = result[0][0]
                if buy_date:
                    from datetime import datetime
                    if isinstance(buy_date, str):
                        buy_date = datetime.fromisoformat(buy_date.replace('Z', '+00:00'))
                    
                    holding_days = (datetime.now() - buy_date.replace(tzinfo=None)).days
                    return holding_days
            
            return None
        except Exception as e:
            logging.error(f"❌ {ticker} 보유기간 계산 중 오류: {e}")
            return None
    
    def _calculate_resistance_level(self, ticker, ohlcv_data):
        """저항선 계산 (최근 30일 고점들의 평균)"""
        try:
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 10:
                return None
            
            # 최근 30일 데이터에서 상위 20% 고점들의 평균을 저항선으로 사용
            high_prices = ohlcv_data['high'].values
            top_20_pct_count = max(2, int(len(high_prices) * 0.2))
            top_highs = sorted(high_prices, reverse=True)[:top_20_pct_count]
            
            resistance_level = sum(top_highs) / len(top_highs)
            return resistance_level
        except Exception as e:
            logging.error(f"❌ {ticker} 저항선 계산 중 오류: {e}")
            return None
    
    def _log_sell_decision(self, ticker, current_price, avg_price, return_rate, sell_type, sell_reason, holding_days):
        """매도 결정 로그 기록"""
        try:
            from datetime import datetime
            
            log_data = {
                'ticker': ticker,
                'action': 'sell',
                'price': current_price,
                'avg_buy_price': avg_price,
                'return_rate': return_rate,
                'sell_type': sell_type,
                'sell_reason': sell_reason,
                'holding_days': holding_days,
                'created_at': datetime.now()
            }
            
            # trade_log 테이블에 기록 (trading_log에서 통합됨)
            insert_query = """
                INSERT INTO trade_log (ticker, action, price, qty, executed_at, status, strategy_combo)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            strategy_combo = f"매도사유: {sell_reason}, 수익률: {return_rate:.1f}%, 보유기간: {holding_days}일"
            
            self.db_mgr.execute_query(insert_query, (
                ticker, 'sell', current_price, 0, datetime.now(), 'completed', strategy_combo
            ))
            
            logging.info(f"📝 {ticker} 매도 로그 기록 완료")
            
        except Exception as e:
            logging.error(f"❌ {ticker} 매도 로그 기록 중 오류: {e}")
    
    def _get_ohlcv_from_db(self, ticker: str, limit: int = 250) -> pd.DataFrame:
        """DB에서 OHLCV 데이터 조회"""
        try:
            query = """
                SELECT date, open, high, low, close, volume
                FROM ohlcv 
                WHERE ticker = %s 
                ORDER BY date DESC 
                LIMIT %s
            """
            
            result = self.db_mgr.execute_query(query, (ticker, limit))
            
            if not result:
                return pd.DataFrame()
            
            df = pd.DataFrame(result, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            df = df.sort_index()  # 날짜 순으로 정렬
            
            return df
            
        except Exception as e:
            logging.error(f"❌ {ticker} OHLCV 데이터 조회 중 오류: {e}")
            return pd.DataFrame()

    def detect_manual_interventions(self):
        """
        수동 개입을 감지하고 기록합니다.
        
        Returns:
            dict: 감지된 수동 개입 정보
        """
        try:
            logging.info("🔍 수동 개입 감지 시작")
            
            # 1. 현재 실제 보유 자산 조회
            actual_holdings = self._get_actual_holdings()
            
            # 2. trade_log 기반 예상 보유 자산 조회
            expected_holdings = self._get_expected_holdings()
            
            # 3. 차이점 분석
            interventions = self._analyze_holding_differences(actual_holdings, expected_holdings)
            
            # 4. 감지된 수동 개입 기록
            if interventions:
                self._log_manual_interventions(interventions)
                
            logging.info(f"✅ 수동 개입 감지 완료: {len(interventions)}건 발견")
            
            return {
                'total_interventions': len(interventions),
                'interventions': interventions,
                'actual_holdings': actual_holdings,
                'expected_holdings': expected_holdings
            }
            
        except Exception as e:
            logging.error(f"❌ 수동 개입 감지 실패: {e}")
            return {'error': str(e)}
    
    def _get_actual_holdings(self):
        """업비트 API를 통해 실제 보유 자산 조회"""
        try:
            import pyupbit
            import os
            from dotenv import load_dotenv
            
            load_dotenv()
            access_key = os.getenv('UPBIT_ACCESS_KEY')
            secret_key = os.getenv('UPBIT_SECRET_KEY')
            
            if not access_key or not secret_key:
                raise ValueError("업비트 API 키가 설정되지 않았습니다.")
            
            upbit = pyupbit.Upbit(access_key, secret_key)
            balances = upbit.get_balances()
            
            # KRW 제외한 암호화폐만 추출
            actual_holdings = {}
            for balance in balances:
                if balance['currency'] != 'KRW' and float(balance['balance']) > 0:
                    ticker = f"KRW-{balance['currency']}"
                    actual_holdings[ticker] = {
                        'quantity': float(balance['balance']),
                        'avg_price': float(balance['avg_buy_price']),
                        'locked': float(balance['locked']) if balance['locked'] else 0
                    }
            
            logging.info(f"📊 실제 보유 자산: {len(actual_holdings)}개")
            return actual_holdings
            
        except Exception as e:
            logging.error(f"❌ 실제 보유 자산 조회 실패: {e}")
            return {}
    
    def _get_expected_holdings(self):
        """trade_log 기반 예상 보유 자산 계산"""
        try:
            # trade_log에서 각 ticker별 매수/매도 기록 조회
            query = """
                SELECT ticker, action, qty, executed_at
                FROM trade_log
                WHERE status = 'completed'
                ORDER BY ticker, executed_at
            """
            
            trades = self.db_mgr.execute_query(query)
            
            # 각 ticker별로 예상 보유량 계산
            expected_holdings = {}
            
            for trade in trades:
                ticker = trade[0]
                action = trade[1]
                quantity = float(trade[2])
                
                if ticker not in expected_holdings:
                    expected_holdings[ticker] = {
                        'quantity': 0,
                        'total_bought': 0,
                        'total_sold': 0,
                        'buy_count': 0,
                        'sell_count': 0,
                        'last_trade_date': trade[3]
                    }
                
                if action in ['buy', 'pyramid_buy']:
                    expected_holdings[ticker]['quantity'] += quantity
                    expected_holdings[ticker]['total_bought'] += quantity
                    expected_holdings[ticker]['buy_count'] += 1
                elif action == 'sell':
                    expected_holdings[ticker]['quantity'] -= quantity
                    expected_holdings[ticker]['total_sold'] += quantity
                    expected_holdings[ticker]['sell_count'] += 1
                    
                expected_holdings[ticker]['last_trade_date'] = trade[3]
            
            # 0보다 작거나 같은 것은 제거 (완전 매도된 것)
            expected_holdings = {k: v for k, v in expected_holdings.items() if v['quantity'] > 0.00000001}
            
            logging.info(f"📊 예상 보유 자산: {len(expected_holdings)}개")
            return expected_holdings
            
        except Exception as e:
            logging.error(f"❌ 예상 보유 자산 계산 실패: {e}")
            return {}
    
    def _analyze_holding_differences(self, actual_holdings, expected_holdings):
        """실제 보유와 예상 보유 간의 차이점 분석 (블랙리스트 종목 제외)"""
        interventions = []
        
        try:
            # 블랙리스트 로드
            from utils import load_blacklist
            blacklist = load_blacklist()
            
            # 1. 실제 보유하고 있지만 trading_log에 매수 기록이 없는 경우 (수동 매수)
            for ticker, actual_data in actual_holdings.items():
                # 블랙리스트 종목은 수동 개입 감지에서 제외
                if ticker in blacklist:
                    logging.info(f"⏭️ {ticker}는 블랙리스트에 포함되어 수동 개입 감지에서 제외됩니다.")
                    continue
                    
                if ticker not in expected_holdings:
                    interventions.append({
                        'ticker': ticker,
                        'detection_type': 'manual_buy',
                        'expected_quantity': 0,
                        'actual_quantity': actual_data['quantity'],
                        'quantity_diff': actual_data['quantity'],
                        'description': f"수동 매수 추정: {ticker}를 {actual_data['quantity']:.8f}개 보유 중이나 매수 기록 없음"
                    })
            
            # 2. trading_log에는 매수 기록이 있지만 실제로는 보유하지 않은 경우 (수동 매도)
            for ticker, expected_data in expected_holdings.items():
                # 블랙리스트 종목은 수동 개입 감지에서 제외
                if ticker in blacklist:
                    logging.info(f"⏭️ {ticker}는 블랙리스트에 포함되어 수동 개입 감지에서 제외됩니다.")
                    continue
                    
                if ticker not in actual_holdings:
                    interventions.append({
                        'ticker': ticker,
                        'detection_type': 'manual_sell',
                        'expected_quantity': expected_data['quantity'],
                        'actual_quantity': 0,
                        'quantity_diff': -expected_data['quantity'],
                        'description': f"수동 매도 추정: {ticker}를 {expected_data['quantity']:.8f}개 보유 예상이나 실제 보유 없음"
                    })
            
            # 3. 수량 차이가 있는 경우 (부분 수동 개입)
            for ticker in set(actual_holdings.keys()) & set(expected_holdings.keys()):
                # 블랙리스트 종목은 수동 개입 감지에서 제외
                if ticker in blacklist:
                    logging.info(f"⏭️ {ticker}는 블랙리스트에 포함되어 수동 개입 감지에서 제외됩니다.")
                    continue
                    
                actual_qty = actual_holdings[ticker]['quantity']
                expected_qty = expected_holdings[ticker]['quantity']
                
                # 소량 차이는 무시 (거래 수수료 등)
                if abs(actual_qty - expected_qty) > 0.00000001:
                    interventions.append({
                        'ticker': ticker,
                        'detection_type': 'quantity_mismatch',
                        'expected_quantity': expected_qty,
                        'actual_quantity': actual_qty,
                        'quantity_diff': actual_qty - expected_qty,
                        'description': f"수량 불일치: {ticker} 예상 {expected_qty:.8f}개 vs 실제 {actual_qty:.8f}개"
                    })
            
            return interventions
            
        except Exception as e:
            logging.error(f"❌ 보유 차이점 분석 실패: {e}")
            return []
    
    def _log_manual_interventions(self, interventions):
        """감지된 수동 개입을 DB에 기록"""
        try:
            for intervention in interventions:
                # 중복 기록 방지를 위해 동일한 감지 내용이 있는지 확인
                existing_check = self.db_mgr.execute_query(
                    """
                    SELECT id FROM manual_override_log 
                    WHERE ticker = %s AND detection_type = %s 
                    AND ABS(quantity_diff - %s) < 0.00000001
                    AND detected_at >= NOW() - INTERVAL '24 hours'
                    """,
                    (intervention['ticker'], intervention['detection_type'], intervention['quantity_diff'])
                )
                
                if existing_check:
                    logging.info(f"⏭️ {intervention['ticker']} 수동 개입 이미 기록됨 (24시간 내)")
                    continue
                
                # 새로운 수동 개입 기록
                self.db_mgr.execute_query(
                    """
                    INSERT INTO manual_override_log 
                    (ticker, detection_type, expected_quantity, actual_quantity, quantity_diff, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        intervention['ticker'],
                        intervention['detection_type'],
                        intervention['expected_quantity'],
                        intervention['actual_quantity'],
                        intervention['quantity_diff'],
                        intervention['description']
                    )
                )
                
                logging.warning(f"⚠️ 수동 개입 감지 및 기록: {intervention['description']}")
            
        except Exception as e:
            logging.error(f"❌ 수동 개입 기록 실패: {e}")

    def _calculate_kelly_based_sell_conditions(self, ticker, current_price, avg_price, atr, return_rate, 
                                             max_price_since_buy, holding_days, market_df):
        """
        켈리 공식 기반 매도 조건 계산
        
        Args:
            ticker: 티커 심볼
            current_price: 현재가
            avg_price: 평균 매수가
            atr: ATR 값
            return_rate: 수익률
            max_price_since_buy: 매수 후 최고가
            holding_days: 보유기간
            market_df: 시장 데이터
            
        Returns:
            dict: 켈리 기반 매도 조건 결과
        """
        try:
            # 1. 켈리 공식 기반 손절가 계산
            kelly_stop_loss = self._calculate_kelly_stop_loss(ticker, current_price, avg_price, atr, market_df)
            
            # 2. 켈리 공식 기반 익절가 계산
            kelly_take_profit = self._calculate_kelly_take_profit(ticker, current_price, avg_price, atr, return_rate, market_df)
            
            # 3. 손절 조건 체크
            stop_loss_triggered = False
            stop_loss_reason = None
            
            if kelly_stop_loss['stop_loss_price'] > 0 and current_price <= kelly_stop_loss['stop_loss_price']:
                stop_loss_triggered = True
                stop_loss_reason = f"켈리 기반 손절 (현재가: {current_price:,.0f}, 손절가: {kelly_stop_loss['stop_loss_price']:,.0f}, 켈리비율: {kelly_stop_loss['kelly_ratio']:.1%})"
            
            # 4. 익절 조건 체크
            take_profit_triggered = False
            take_profit_reason = None
            
            if kelly_take_profit['take_profit_price'] > 0 and current_price >= kelly_take_profit['take_profit_price']:
                take_profit_triggered = True
                take_profit_reason = f"켈리 기반 익절 (현재가: {current_price:,.0f}, 익절가: {kelly_take_profit['take_profit_price']:,.0f}, 켈리비율: {kelly_take_profit['kelly_ratio']:.1%})"
            
            return {
                'stop_loss_triggered': stop_loss_triggered,
                'stop_loss_reason': stop_loss_reason,
                'stop_loss_price': kelly_stop_loss['stop_loss_price'],
                'kelly_stop_ratio': kelly_stop_loss['kelly_ratio'],
                'take_profit_triggered': take_profit_triggered,
                'take_profit_reason': take_profit_reason,
                'take_profit_price': kelly_take_profit['take_profit_price'],
                'kelly_take_ratio': kelly_take_profit['kelly_ratio']
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 켈리 기반 매도 조건 계산 오류: {str(e)}")
            return {
                'stop_loss_triggered': False,
                'stop_loss_reason': None,
                'stop_loss_price': 0,
                'kelly_stop_ratio': 0,
                'take_profit_triggered': False,
                'take_profit_reason': None,
                'take_profit_price': 0,
                'kelly_take_ratio': 0
            }
    
    def _calculate_kelly_stop_loss(self, ticker, current_price, avg_price, atr, market_df):
        """켈리 공식 기반 손절가 계산"""
        try:
            # 1. 시장 데이터에서 승률 추정
            market_data = market_df.loc[ticker] if ticker in market_df.index else None
            if market_data is None:
                return {'stop_loss_price': 0, 'kelly_ratio': 0}
            
            # 2. 기술적 지표 기반 승률 추정
            rsi = safe_float_convert(market_data.get('rsi_14', 50), context=f"{ticker} RSI")
            macd = safe_float_convert(market_data.get('macd', 0), context=f"{ticker} MACD")
            macd_signal = safe_float_convert(market_data.get('macd_signal', 0), context=f"{ticker} MACD Signal")
            
            # RSI 기반 승률 추정
            if rsi > 70:
                base_win_rate = 0.3  # 과매수 상태
            elif rsi > 60:
                base_win_rate = 0.4  # 약간 과매수
            elif rsi < 30:
                base_win_rate = 0.6  # 과매도 상태 (반등 기대)
            elif rsi < 40:
                base_win_rate = 0.5  # 약간 과매도
            else:
                base_win_rate = 0.45  # 중립
            
            # MACD 기반 승률 조정
            if macd > macd_signal:
                macd_adjustment = 0.1  # 상승 신호
            else:
                macd_adjustment = -0.1  # 하락 신호
            
            estimated_win_rate = max(0.2, min(base_win_rate + macd_adjustment, 0.8))
            
            # 3. 켈리 공식 기반 손절가 계산
            if atr > 0:
                # ATR 기반 리스크/리워드 비율 설정
                risk_reward_ratio = 1.5  # 기본 1.5:1
                
                # 켈리 공식: f = (bp - q) / b
                # b = 리스크/리워드 비율, p = 승률, q = 패배 확률
                kelly_ratio = (risk_reward_ratio * estimated_win_rate - (1 - estimated_win_rate)) / risk_reward_ratio
                kelly_ratio = max(0, min(kelly_ratio, 0.3))  # 0-30% 범위로 제한
                
                # 켈리 비율 기반 손절가 계산
                kelly_stop_distance = atr * (2.0 + kelly_ratio * 5.0)  # 2-4.5x ATR
                stop_loss_price = avg_price - kelly_stop_distance
                
                return {
                    'stop_loss_price': stop_loss_price,
                    'kelly_ratio': kelly_ratio,
                    'estimated_win_rate': estimated_win_rate,
                    'risk_reward_ratio': risk_reward_ratio
                }
            else:
                return {'stop_loss_price': 0, 'kelly_ratio': 0}
                
        except Exception as e:
            logging.error(f"❌ {ticker} 켈리 손절가 계산 오류: {str(e)}")
            return {'stop_loss_price': 0, 'kelly_ratio': 0}
    
    def _calculate_kelly_take_profit(self, ticker, current_price, avg_price, atr, return_rate, market_df):
        """켈리 공식 기반 익절가 계산"""
        try:
            # 1. 시장 데이터에서 승률 추정
            market_data = market_df.loc[ticker] if ticker in market_df.index else None
            if market_data is None:
                return {'take_profit_price': 0, 'kelly_ratio': 0}
            
            # 2. 현재 수익률 기반 승률 조정
            if return_rate > 20:
                profit_adjustment = 0.2  # 고수익 상태에서 승률 증가
            elif return_rate > 10:
                profit_adjustment = 0.1  # 중간 수익 상태
            elif return_rate < -10:
                profit_adjustment = -0.1  # 손실 상태에서 승률 감소
            else:
                profit_adjustment = 0
            
            # 3. 기술적 지표 기반 승률 추정
            rsi = safe_float_convert(market_data.get('rsi_14', 50), context=f"{ticker} RSI")
            macd = safe_float_convert(market_data.get('macd', 0), context=f"{ticker} MACD")
            macd_signal = safe_float_convert(market_data.get('macd_signal', 0), context=f"{ticker} MACD Signal")
            
            # RSI 기반 승률 추정
            if rsi > 70:
                base_win_rate = 0.6  # 과매수 상태에서 익절 기대
            elif rsi > 60:
                base_win_rate = 0.5  # 약간 과매수
            elif rsi < 30:
                base_win_rate = 0.3  # 과매도 상태에서 익절 어려움
            elif rsi < 40:
                base_win_rate = 0.4  # 약간 과매도
            else:
                base_win_rate = 0.45  # 중립
            
            # MACD 기반 승률 조정
            if macd > macd_signal:
                macd_adjustment = 0.1  # 상승 신호
            else:
                macd_adjustment = -0.1  # 하락 신호
            
            estimated_win_rate = max(0.2, min(base_win_rate + macd_adjustment + profit_adjustment, 0.8))
            
            # 4. 켈리 공식 기반 익절가 계산
            if atr > 0:
                # ATR 기반 리스크/리워드 비율 설정 (익절은 더 보수적)
                risk_reward_ratio = 2.0  # 기본 2:1
                
                # 켈리 공식: f = (bp - q) / b
                kelly_ratio = (risk_reward_ratio * estimated_win_rate - (1 - estimated_win_rate)) / risk_reward_ratio
                kelly_ratio = max(0, min(kelly_ratio, 0.25))  # 0-25% 범위로 제한
                
                # 켈리 비율 기반 익절가 계산
                kelly_take_distance = atr * (3.0 + kelly_ratio * 4.0)  # 3-4x ATR
                take_profit_price = avg_price + kelly_take_distance
                
                return {
                    'take_profit_price': take_profit_price,
                    'kelly_ratio': kelly_ratio,
                    'estimated_win_rate': estimated_win_rate,
                    'risk_reward_ratio': risk_reward_ratio
                }
            else:
                return {'take_profit_price': 0, 'kelly_ratio': 0}
                
        except Exception as e:
            logging.error(f"❌ {ticker} 켈리 익절가 계산 오류: {str(e)}")
            return {'take_profit_price': 0, 'kelly_ratio': 0}
    
    def _check_market_based_exit_conditions(self, ticker, current_price, avg_price, return_rate, 
                                          rsi, ma20, macd, macd_signal, bb_upper, bb_lower, holding_days):
        """시장 상황 기반 동적 익절 조건 체크"""
        try:
            should_exit = False
            reason = None
            exit_type = None
            
            # 1. 기술적 약세 신호 2개 이상
            bearish_signals = 0
            signal_details = []
            
            # MA20 이탈 체크
            if current_price < ma20 * 0.98:  # MA20 대비 2% 이하
                bearish_signals += 1
                signal_details.append("MA20 이탈")
            
            # MACD 데드크로스 체크
            if macd < macd_signal and macd < 0:
                bearish_signals += 1
                signal_details.append("MACD 데드크로스")
            
            # RSI 하락 체크
            if rsi < 40:
                bearish_signals += 1
                signal_details.append("RSI 하락")
            
            # 볼린저 밴드 하단 이탈 체크
            if current_price < bb_lower:
                bearish_signals += 1
                signal_details.append("볼린저 하단 이탈")
            
            if bearish_signals >= 2:
                should_exit = True
                reason = f"기술적 약세 신호 {bearish_signals}개 ({', '.join(signal_details)})"
                exit_type = "technical_bearish"
            
            # 2. 고수익 + 약세 시그널
            if not should_exit and return_rate >= 25.0:
                weak_bearish_signals = 0
                
                # 약세 시그널 체크 (더 완화된 조건)
                if current_price < ma20:
                    weak_bearish_signals += 1
                if rsi > 70:  # 과매수 상태
                    weak_bearish_signals += 1
                if macd < macd_signal:
                    weak_bearish_signals += 1
                
                if weak_bearish_signals >= 1:
                    should_exit = True
                    reason = f"고수익 + 약세신호 익절 (수익률: {return_rate:.1f}%, 약세신호: {weak_bearish_signals}개)"
                    exit_type = "high_profit_exit"
            
            # 3. 보유기간 기반 조건 분기
            if not should_exit and holding_days is not None:
                if holding_days <= 3:  # 3일 이내: 보수적 익절
                    if return_rate >= 15.0:
                        should_exit = True
                        reason = f"단기 보수적 익절 (보유 {holding_days}일, 수익률: {return_rate:.1f}%)"
                        exit_type = "short_term_exit"
                elif holding_days <= 7:  # 7일 이내: 일반 익절
                    if return_rate >= 20.0:
                        should_exit = True
                        reason = f"중기 익절 (보유 {holding_days}일, 수익률: {return_rate:.1f}%)"
                        exit_type = "medium_term_exit"
                else:  # 7일 초과: 적극적 익절
                    if return_rate >= 12.0:
                        should_exit = True
                        reason = f"장기 적극적 익절 (보유 {holding_days}일, 수익률: {return_rate:.1f}%)"
                        exit_type = "long_term_exit"
            
            return {
                'should_exit': should_exit,
                'reason': reason,
                'type': exit_type
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 시장 기반 익절 조건 체크 오류: {str(e)}")
            return {'should_exit': False, 'reason': None, 'type': None}
    
    def _check_portfolio_based_exit_conditions(self, ticker, current_price, avg_price, return_rate, 
                                             portfolio_data, market_df):
        """포트폴리오 밸런싱 기반 매도 조건 체크"""
        try:
            should_exit = False
            reason = None
            exit_type = None
            
            # 1. 포트폴리오 집중도 체크
            total_positions = len(portfolio_data)
            if total_positions > 10:  # 10개 이상 보유 시
                # 수익률이 낮은 종목부터 정리
                if return_rate < 5.0:
                    should_exit = True
                    reason = f"포트폴리오 정리 (보유 {total_positions}개, 수익률: {return_rate:.1f}%)"
                    exit_type = "portfolio_cleanup"
            
            # 2. 동일 섹터 과다 집중 체크 (간단한 구현)
            # 실제로는 섹터 분류가 필요하지만, 여기서는 보유 종목 수로 대체
            if total_positions > 15:  # 15개 이상 보유 시
                if return_rate < 10.0:
                    should_exit = True
                    reason = f"과다 집중 해소 (보유 {total_positions}개, 수익률: {return_rate:.1f}%)"
                    exit_type = "concentration_reduction"
            
            # 3. 시장 대비 상대 강도 체크
            if ticker in market_df.index:
                market_data = market_df.loc[ticker]
                relative_strength = safe_float_convert(market_data.get('relative_strength', 0), context=f"{ticker} relative_strength")
                
                if relative_strength < -0.1:  # 시장 대비 10% 이상 약세
                    if return_rate > 0:  # 수익 상태에서만
                        should_exit = True
                        reason = f"상대 강도 약세 (상대강도: {relative_strength:.2f}, 수익률: {return_rate:.1f}%)"
                        exit_type = "relative_strength_exit"
            
            return {
                'should_exit': should_exit,
                'reason': reason,
                'type': exit_type
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 포트폴리오 기반 익절 조건 체크 오류: {str(e)}")
            return {'should_exit': False, 'reason': None, 'type': None}
