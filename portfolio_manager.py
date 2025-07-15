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

    def get_total_balance(self, currency: str = "KRW", include_crypto: bool = True):
        """
        총 보유 자산 조회 (통합 버전)
        
        Args:
            currency: 기준 통화 (기본값: "KRW")
            include_crypto: 암호화폐 포함 여부 (기본값: True)
            
        Returns:
            float: 총 보유 자산 (KRW 기준)
        """
        try:
            balances = self.upbit.get_balances()
            
            # 응답 형식 검증 및 로깅
            logging.debug(f"🔍 get_total_balance 응답 타입: {type(balances)}")
            
            # None인 경우 처리
            if balances is None:
                logging.warning("⚠️ get_balances가 None을 반환했습니다.")
                return 100000.0  # 기본값 10만원
            
            # 문자열로 반환된 경우 JSON 파싱 시도
            if isinstance(balances, str):
                try:
                    import json
                    balances = json.loads(balances)
                    logging.info("✅ 문자열 응답을 JSON으로 파싱 완료")
                except json.JSONDecodeError as e:
                    logging.error(f"❌ JSON 파싱 실패: {e}")
                    return 100000.0  # 기본값 10만원
            
            # 리스트가 아닌 경우 처리 (Pyupbit API 응답 형태에 따라)
            if not isinstance(balances, list):
                logging.info(f"📊 get_total_balance: balances 반환값이 리스트가 아님 (타입: {type(balances)}) - 변환 시도")
                # 딕셔너리인 경우 리스트로 변환 시도
                if isinstance(balances, dict):
                    if 'data' in balances:
                        balances = balances['data']
                        logging.info("✅ 'data' 키에서 리스트 추출 완료")
                    elif 'result' in balances:
                        balances = balances['result']
                        logging.info("✅ 'result' 키에서 리스트 추출 완료")
                    else:
                        # 단일 잔고 정보인 경우 리스트로 변환
                        balances = [balances]
                        logging.info("✅ 단일 잔고 정보를 리스트로 변환 완료")
                else:
                    logging.error(f"❌ 예상치 못한 balances 형식: {type(balances)}")
                    return 100000.0  # 기본값 10만원
            
            # 블랙리스트 로드
            try:
                from utils import load_blacklist, safe_float_convert
                blacklist = load_blacklist()
                if not blacklist:
                    logging.warning("⚠️ 블랙리스트가 비어있습니다.")
            except Exception as e:
                logging.error(f"❌ 블랙리스트 로드 중 오류 발생: {str(e)}")
                blacklist = []
            
            total = 0
            krw_balance = 0
            
            for balance in balances:
                try:
                    # balance가 딕셔너리가 아닌 경우 처리
                    if not isinstance(balance, dict):
                        logging.warning(f"⚠️ 예상치 못한 balance 형식: {type(balance)} - {balance}")
                        continue
                    
                    currency_code = balance.get('currency')
                    if not currency_code:
                        continue
                    
                    # KRW 잔고 처리
                    if currency_code == 'KRW':
                        krw_balance = float(balance.get('balance', 0))
                        if currency == "KRW":
                            total = krw_balance
                            break  # KRW만 요청한 경우 즉시 반환
                        else:
                            total += krw_balance
                        continue
                    
                    # 암호화폐 포함하지 않는 경우 스킵
                    if not include_crypto:
                        continue
                    
                    # 블랙리스트에 포함된 종목 필터링
                    if f"KRW-{currency_code}" in blacklist:
                        logging.info(f"⏭️ {currency_code}는 블랙리스트에 포함되어 제외됩니다.")
                        continue
                    
                    # 암호화폐 가치 계산
                    ticker = f"KRW-{currency_code}"
                    current_price = pyupbit.get_current_price(ticker)
                    if current_price:
                        crypto_value = float(balance.get('balance', 0)) * current_price
                        total += crypto_value
                        logging.debug(f"💰 {ticker}: {float(balance.get('balance', 0)):.8f}개 @ {current_price:,.0f}원 = {crypto_value:,.0f}원")
                    else:
                        logging.warning(f"⚠️ {ticker} 현재가 조회 실패")
                        
                except (ValueError, TypeError) as e:
                    logging.error(f"❌ {currency_code} 자산 계산 중 오류: {str(e)}")
                    continue
            
            # 로깅
            if include_crypto:
                logging.info(f"💰 총 보유 자산: {total:,.0f}원 (KRW: {krw_balance:,.0f}원 + 암호화폐: {total - krw_balance:,.0f}원)")
            else:
                logging.info(f"💰 KRW 잔고: {total:,.0f}원")
                
            return total
            
        except Exception as e:
            logging.error(f"❌ 총 자산 조회 중 오류 발생: {str(e)}")
            return 100000.0  # 기본값 10만원

    def get_current_positions(self, include_krw: bool = False, min_value: float = 1.0):
        """
        보유 자산 정보 반환 (개선된 버전)
        
        Args:
            include_krw: KRW 포함 여부 (기본값: False)
            min_value: 최소 자산 가치 (기본값: 1.0원)
            
        Returns:
            list: 보유 자산 정보 리스트
        """
        try:
            # Upbit 객체에서 get_balances 메서드 호출
            balances = self.upbit.get_balances()
            
            # 응답 형식 검증 및 로깅
            logging.debug(f"🔍 get_balances 응답 타입: {type(balances)}")
            logging.debug(f"🔍 get_balances 응답 내용: {balances}")
            
            # None인 경우 처리
            if balances is None:
                logging.warning("⚠️ get_balances가 None을 반환했습니다.")
                return []
            
            # 문자열로 반환된 경우 JSON 파싱 시도
            if isinstance(balances, str):
                try:
                    import json
                    balances = json.loads(balances)
                    logging.info("✅ 문자열 응답을 JSON으로 파싱 완료")
                except json.JSONDecodeError as e:
                    logging.error(f"❌ JSON 파싱 실패: {e}")
                    return []
            
            # 리스트가 아닌 경우 처리 (Pyupbit API 응답 형태에 따라)
            if not isinstance(balances, list):
                logging.info(f"📊 get_balances 반환값이 리스트가 아님 (타입: {type(balances)}) - 변환 시도")
                # 딕셔너리인 경우 리스트로 변환 시도
                if isinstance(balances, dict):
                    if 'data' in balances:
                        balances = balances['data']
                        logging.info("✅ 'data' 키에서 리스트 추출 완료")
                    elif 'result' in balances:
                        balances = balances['result']
                        logging.info("✅ 'result' 키에서 리스트 추출 완료")
                    else:
                        # 단일 잔고 정보인 경우 리스트로 변환
                        balances = [balances]
                        logging.info("✅ 단일 잔고 정보를 리스트로 변환 완료")
                else:
                    logging.error(f"❌ 예상치 못한 balances 형식: {type(balances)}")
                    return []
            
            # 빈 리스트인 경우
            if not balances:
                logging.info("📊 보유 자산이 없습니다.")
                return []
            
            # 블랙리스트 로드
            try:
                from filter_tickers import load_blacklist
                from utils import safe_float_convert
                blacklist = load_blacklist()
                if not blacklist:
                    logging.warning("⚠️ 블랙리스트가 비어있습니다.")
            except Exception as e:
                logging.error(f"❌ 블랙리스트 로드 중 오류 발생: {str(e)}")
                blacklist = []

            filtered = []
            total_portfolio_value = 0
            
            for item in balances:
                try:
                    # item이 딕셔너리가 아닌 경우 처리
                    if not isinstance(item, dict):
                        logging.warning(f"⚠️ 예상치 못한 item 형식: {type(item)} - {item}")
                        continue
                    
                    currency = item.get('currency')
                    if not currency:
                        continue
                    
                    # KRW 처리
                    if currency == 'KRW':
                        if include_krw:
                            balance = float(item.get('balance', 0))
                            if balance >= min_value:
                                filtered.append({
                                    'currency': currency,
                                    'ticker': currency,
                                    'balance': balance,
                                    'avg_buy_price': balance,  # KRW는 평균가 = 잔고
                                    'value': balance,
                                    'locked': float(item.get('locked', 0))
                                })
                                total_portfolio_value += balance
                        continue
                    
                    # 블랙리스트에 포함된 종목 필터링
                    if f"KRW-{currency}" in blacklist:
                        logging.info(f"⏭️ {currency}는 블랙리스트에 포함되어 제외됩니다.")
                        continue
                    
                    balance = float(item.get('balance', 0))
                    avg_price = float(item.get('avg_buy_price', 0))
                    locked = float(item.get('locked', 0))
                    
                    if balance <= 0:
                        continue
                    
                    # 현재가 조회
                    ticker = f"KRW-{currency}"
                    current_price = pyupbit.get_current_price(ticker)
                    if not current_price:
                        logging.warning(f"⚠️ {ticker} 현재가 조회 실패")
                        continue
                    
                    # 자산 가치 계산
                    value = balance * current_price
                    
                    # 최소 가치 필터링
                    if value >= min_value:
                        position_info = {
                            'currency': currency,
                            'ticker': ticker,
                            'balance': balance,
                            'avg_buy_price': avg_price,
                            'current_price': current_price,
                            'value': value,
                            'locked': locked,
                            'return_rate': ((current_price - avg_price) / avg_price * 100) if avg_price > 0 else 0,
                            'unrealized_pnl': (current_price - avg_price) * balance if avg_price > 0 else 0
                        }
                        filtered.append(position_info)
                        total_portfolio_value += value
                        
                        logging.debug(f"💰 {ticker}: {balance:.8f}개 @ {avg_price:,.0f}원 (현재가: {current_price:,.0f}원, 수익률: {position_info['return_rate']:.1f}%)")
                        
                except (ValueError, TypeError) as e:
                    logging.error(f"❌ {item.get('currency', 'unknown')} 포지션 필터링 중 오류: {str(e)}")
                    continue
                
            logging.info(f"📊 필터링된 보유 자산: {len(filtered)}개, 총 가치: {total_portfolio_value:,.0f}원")
            return filtered
            
        except Exception as e:
            logging.error(f"❌ 보유 자산 조회 중 오류 발생: {str(e)}")
            return []

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
    
    def check_advanced_sell_conditions(self, portfolio_data=None):
        """
        🔧 [통합 개선] 우선순위 기반 통합 매도 조건 체크
        
        ✅ 우선순위 기반 매도 조건:
        1순위: 손절매 조건 (최우선)
        2순위: 추세전환 조건 (중간 우선순위)  
        3순위: 이익실현 조건 (낮은 우선순위)
        4순위: 트레일링 스탑 (가장 낮은 우선순위)
        
        ✅ 암호화폐 시장 특성 반영:
        - 높은 변동성을 고려한 동적 조정
        - 단기 변동성에 대한 관대한 트레일링 스탑
        - 갭하락에 대한 즉시 대응
        
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
            from filter_tickers import fetch_static_indicators_data
            market_df = fetch_static_indicators_data()
            
            if market_df is None or market_df.empty:
                logging.warning("⚠️ 매도 조건 점검 실패: 시장 데이터 없음")
                return
                
            # 각 보유 종목에 대해 우선순위 기반 매도 조건 체크
            for ticker in portfolio_data.index:
                try:
                    # KRW는 처리하지 않음
                    if ticker == 'KRW':
                        continue

                    # 티커 형식 확인 및 변환
                    ticker_krw = f"KRW-{ticker.replace('KRW-', '')}" if ticker != 'KRW' else ticker
                    
                    # 기본 데이터 조회
                    current_price = pyupbit.get_current_price(ticker_krw)
                    if current_price is None:
                        logging.warning(f"⚠️ {ticker_krw} 현재가 조회 실패")
                        continue
                        
                    # 평균 매수가 및 수량 조회
                    avg_price = self._get_avg_price(portfolio_data, ticker)
                    balance = self._get_balance(portfolio_data, ticker)
                    
                    if avg_price is None or avg_price <= 0 or balance is None or balance <= 0:
                        logging.warning(f"⚠️ {ticker_krw} 평균 매수가 또는 수량 정보 없음 (avg_price: {avg_price}, balance: {balance})")
                        continue
                        
                    # 수익률 계산
                    return_rate = (current_price - avg_price) / avg_price * 100
                    
                    # 기술적 지표 데이터 조회
                    market_data = self._get_market_data(ticker_krw, market_df)
                    if market_data is None:
                        logging.warning(f"⚠️ {ticker_krw} 기술적 지표 데이터 없음")
                        continue
                    
                    # ATR 및 기타 지표 조회
                    atr = safe_float_convert(market_data.get('atr', 0), context=f"{ticker_krw} ATR")
                    atr_ratio = atr / current_price if current_price > 0 else 0
                    
                    # 보유기간 계산
                    holding_days = self._calculate_holding_days(ticker_krw)
                    
                    # OHLCV 데이터 조회
                    ohlcv_data = self._get_ohlcv_from_db(ticker_krw, limit=30)
                    
                    # 🔧 [통합 개선] 우선순위 기반 매도 조건 체크
                    sell_decision = self._check_priority_based_sell_conditions(
                        ticker_krw, current_price, avg_price, atr, return_rate, 
                        holding_days, atr_ratio, market_data, ohlcv_data
                    )
                    
                    # 매도 실행
                    if sell_decision['should_exit']:
                        self._execute_sell_order(ticker_krw, sell_decision)
                    else:
                        # 매도 조건 미충족 시 상태 로깅
                        logging.debug(f"📊 {ticker_krw} 매도 조건 미충족 - 수익률: {return_rate:.1f}%, "
                                   f"보유기간: {holding_days}일, ATR비율: {atr_ratio:.2%}")
                        
                except Exception as e:
                    logging.error(f"❌ {ticker_krw} 매도 조건 체크 중 오류: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"❌ 통합 매도 조건 점검 중 오류: {e}")

    def _get_avg_price(self, portfolio_data, ticker):
        """평균 매수가 조회"""
        try:
            if 'avg_price' in portfolio_data.columns:
                return safe_float_convert(portfolio_data.loc[ticker, 'avg_price'], context=f"{ticker} avg_price")
            elif 'avg_buy_price' in portfolio_data.columns:
                return safe_float_convert(portfolio_data.loc[ticker, 'avg_buy_price'], context=f"{ticker} avg_buy_price")
            return None
        except Exception as e:
            logging.error(f"❌ {ticker} 평균 매수가 조회 실패: {e}")
            return None

    def _get_balance(self, portfolio_data, ticker):
        """보유 수량 조회"""
        try:
            if 'balance' in portfolio_data.columns:
                return safe_float_convert(portfolio_data.loc[ticker, 'balance'], context=f"{ticker} balance")
            return None
        except Exception as e:
            logging.error(f"❌ {ticker} 보유 수량 조회 실패: {e}")
            return None

    def _get_market_data(self, ticker_krw, market_df):
        """시장 데이터 조회"""
        try:
            return market_df.loc[ticker_krw] if ticker_krw in market_df.index else None
        except Exception as e:
            logging.error(f"❌ {ticker_krw} 시장 데이터 조회 실패: {e}")
            return None

    def _check_priority_based_sell_conditions(self, ticker, current_price, avg_price, atr, return_rate, 
                                            holding_days, atr_ratio, market_data, ohlcv_data):
        """
        우선순위 기반 매도 조건 체크
        
        우선순위:
        1순위: 손절매 조건 (최우선)
        2순위: 추세전환 조건 (중간 우선순위)
        3순위: 이익실현 조건 (낮은 우선순위)
        4순위: 트레일링 스탑 (가장 낮은 우선순위)
        """
        try:
            # 1순위: 손절매 조건 (최우선)
            stop_loss_result = self._check_unified_stop_loss(
                ticker, current_price, avg_price, atr, return_rate, holding_days, atr_ratio
            )
            if stop_loss_result['should_exit']:
                return stop_loss_result
            
            # 2순위: 추세전환 조건 (중간 우선순위)
            trend_reversal_result = self._check_unified_trend_reversal(
                ticker, current_price, avg_price, return_rate, market_data, ohlcv_data
            )
            if trend_reversal_result['should_exit']:
                return trend_reversal_result
            
            # 3순위: 이익실현 및 트레일링스탑 조건 (통합)
            profit_taking_result = self._check_unified_profit_taking_and_trailing_stop(
                ticker, current_price, avg_price, atr, return_rate, holding_days, atr_ratio, market_data, ohlcv_data
            )
            if profit_taking_result['should_exit']:
                return profit_taking_result
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 우선순위 기반 매도 조건 체크 중 오류: {e}")
            return {'should_exit': False}

    def _check_unified_stop_loss(self, ticker, current_price, avg_price, atr, return_rate, holding_days, atr_ratio):
        """
        통합 손절매 로직
        
        - 기본 8% 손절 (최우선)
        - 암호화폐 변동성 기반 동적 손절 (보조)
        """
        try:
            # 1. 기본 손절 (최우선) - Makenaide 전략의 핵심
            if return_rate <= -8.0:
                return {
                    'should_exit': True,
                    'reason': f"기본 손절 (수익률: {return_rate:.1f}%)",
                    'type': "basic_stop_loss",
                    'priority': 1
                }
            
            # 2. 암호화폐 변동성 기반 동적 손절 (보조)
            crypto_volatility_multiplier = self._get_crypto_volatility_multiplier(atr_ratio)
            dynamic_stop_pct = min(max((atr / avg_price) * 100 * crypto_volatility_multiplier, 3.0), 12.0)
            
            if return_rate <= -dynamic_stop_pct:
                return {
                    'should_exit': True,
                    'reason': f"변동성 기반 손절 (수익률: {return_rate:.1f}%, 기준: -{dynamic_stop_pct:.1f}%, 변동성: {atr_ratio:.2%})",
                    'type': "volatility_stop_loss",
                    'priority': 2
                }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 통합 손절매 체크 중 오류: {e}")
            return {'should_exit': False}

    def _get_crypto_volatility_multiplier(self, atr_ratio):
        """
        암호화폐 시장 변동성에 맞춘 배수 조정
        
        암호화폐 시장은 전통 시장보다 변동성이 높으므로 더 보수적인 접근
        """
        if atr_ratio > 0.08:  # 극고변동성 (8% 이상)
            return 1.2  # 더 보수적
        elif atr_ratio > 0.05:  # 고변동성 (5-8%)
            return 1.5
        elif atr_ratio > 0.03:  # 중변동성 (3-5%)
            return 2.0
        else:  # 저변동성 (3% 미만)
            return 2.5

    def _check_unified_trend_reversal(self, ticker, current_price, avg_price, return_rate, market_data, ohlcv_data):
        """
        통합 추세전환 로직 (우선순위 기반)
        
        우선순위:
        1순위: 갭하락 감지 (즉시 매도)
        2순위: 와인스타인 Stage 4 진입 (하락 추세 시작)
        3순위: 나쁜 뉴스 감지 (복합적 약세 신호)
        4순위: 와인스타인 Stage 3 분배 (고점 근처 횡보)
        5순위: 기술적 약세 신호 (2개 이상)
        """
        try:
            # 1순위: 갭하락 감지 (즉시 매도)
            gap_down_result = self._check_gap_down_exit(ticker, current_price, avg_price, return_rate, ohlcv_data)
            if gap_down_result['should_exit']:
                return {
                    'should_exit': True,
                    'reason': gap_down_result['reason'],
                    'type': 'gap_down_exit',
                    'priority': 2
                }
            
            # 2순위: 와인스타인 Stage 4 진입 (하락 추세 시작)
            weinstein_stage4_result = self._check_weinstein_stage4_exit(ticker, current_price, avg_price, return_rate, market_data, ohlcv_data)
            if weinstein_stage4_result['should_exit']:
                return {
                    'should_exit': True,
                    'reason': weinstein_stage4_result['reason'],
                    'type': 'weinstein_stage4_exit',
                    'priority': 2
                }
            
            # 3순위: 나쁜 뉴스 감지 (복합적 약세 신호)
            bad_news_result = self._check_bad_news_exit(ticker, current_price, avg_price, return_rate, market_data, ohlcv_data)
            if bad_news_result['should_exit']:
                return {
                    'should_exit': True,
                    'reason': bad_news_result['reason'],
                    'type': 'bad_news_exit',
                    'priority': 2
                }
            
            # 4순위: 와인스타인 Stage 3 분배 (고점 근처 횡보)
            weinstein_stage3_result = self._check_weinstein_stage3_exit(ticker, current_price, avg_price, return_rate, market_data, ohlcv_data)
            if weinstein_stage3_result['should_exit']:
                return {
                    'should_exit': True,
                    'reason': weinstein_stage3_result['reason'],
                    'type': 'weinstein_stage3_exit',
                    'priority': 2
                }
            
            # 5순위: 기술적 약세 신호 (2개 이상)
            technical_bearish = self._check_technical_bearish_signals(ticker, market_data)
            if technical_bearish['should_exit']:
                return technical_bearish
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 통합 추세전환 체크 중 오류: {e}")
            return {'should_exit': False}

    def _check_weinstein_stage4_exit(self, ticker, current_price, avg_price, return_rate, market_data, ohlcv_data):
        """와인스타인 Stage 4 진입 감지 (하락 추세 시작)"""
        try:
            # MA200 값 조회
            ma200 = safe_float_convert(market_data.get('ma200', 0), context=f"{ticker} MA200")
            if ma200 <= 0:
                return {'should_exit': False}
            
            # 거래량 비율 계산
            volume_ratio = self._calculate_volume_ratio(ticker, ohlcv_data)
            
            # Stage 4 진입 조건 (MA200 하향 이탈 + 거래량 급증)
            if (current_price < ma200 and  # MA200 하향 이탈
                volume_ratio > 1.5):  # 거래량 급증 (공포 매도)
                return {
                    'should_exit': True,
                    'reason': f'와인스타인 Stage 4 진입 감지 (MA200 하향 이탈, 거래량 {volume_ratio:.1f}배)'
                }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 와인스타인 Stage 4 체크 중 오류: {e}")
            return {'should_exit': False}

    def _check_weinstein_stage3_exit(self, ticker, current_price, avg_price, return_rate, market_data, ohlcv_data):
        """와인스타인 Stage 3 분배 단계 감지 (고점 근처 횡보)"""
        try:
            # MA200 값 조회
            ma200 = safe_float_convert(market_data.get('ma200', 0), context=f"{ticker} MA200")
            if ma200 <= 0:
                return {'should_exit': False}
            
            # 거래량 비율 계산
            volume_ratio = self._calculate_volume_ratio(ticker, ohlcv_data)
            
            # Stage 3 분배 단계 감지 (고점 근처 횡보 + 거래량 패턴 변화)
            if (current_price > ma200 and  # 아직 MA200 위
                current_price < ma200 * 1.05 and  # 고점 근처 횡보
                return_rate > 10):  # 수익 구간에서만 적용
                
                # 거래량 패턴 변화 확인
                volume_pattern = self._check_volume_pattern_change(ticker, ohlcv_data)
                
                if volume_ratio > 1.2 or volume_pattern['pattern_detected']:
                    pattern_reason = volume_pattern.get('reason', '') if volume_pattern['pattern_detected'] else ''
                    return {
                        'should_exit': True,
                        'reason': f'와인스타인 Stage 3 분배 단계 (거래량 {volume_ratio:.1f}배, {pattern_reason})'
                    }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 와인스타인 Stage 3 체크 중 오류: {e}")
            return {'should_exit': False}

    def _check_bad_news_exit(self, ticker, current_price, avg_price, return_rate, market_data, ohlcv_data):
        """나쁜 뉴스 감지 (복합적 약세 신호)"""
        try:
            # 기술적 지표 기반 나쁜 뉴스 감지
            rsi = safe_float_convert(market_data.get('rsi_14', 50), context=f"{ticker} RSI")
            macd = safe_float_convert(market_data.get('macd', 0), context=f"{ticker} MACD")
            macd_signal = safe_float_convert(market_data.get('macd_signal', 0), context=f"{ticker} MACD Signal")
            adx = safe_float_convert(market_data.get('adx', 25), context=f"{ticker} ADX")
            
            # 나쁜 뉴스 감지 조건들
            bad_news_conditions = []
            
            # 1. RSI 과매도 + MACD 데드크로스
            if rsi < 30 and macd < macd_signal:
                bad_news_conditions.append("RSI 과매도 + MACD 데드크로스")
            
            # 2. ADX 강한 하락 추세
            if adx > 30 and return_rate < -5:
                bad_news_conditions.append("강한 하락 추세")
            
            # 3. 급격한 거래량 증가 + 가격 하락
            if ohlcv_data is not None and not ohlcv_data.empty:
                ohlcv_data['volume'] = ohlcv_data['volume'].astype(float)
                print(f"DEBUG tail(3): {ohlcv_data['volume'].tail(3).tolist()}, tail(20): {ohlcv_data['volume'].tail(20).tolist()}")
                recent_volume = ohlcv_data['volume'].tail(3).mean()
                long_term_volume = ohlcv_data['volume'].tail(20).mean()
                print(f"DEBUG recent_volume: {recent_volume}, long_term_volume: {long_term_volume}, cond: {recent_volume > long_term_volume * 2.0}")
                
                if (recent_volume > long_term_volume * 2.0 and  # 거래량 2배 이상
                    return_rate < -3):  # 3% 이상 손실
                    bad_news_conditions.append("거래량 급증 + 가격 하락")
            
            print(f"DEBUG bad_news_conditions: {bad_news_conditions}")
            # 나쁜 뉴스 조건이 2개 이상 충족되면 매도
            if len(bad_news_conditions) >= 2:
                return {
                    'should_exit': True,
                    'reason': f'나쁜 뉴스 감지: {", ".join(bad_news_conditions)}'
                }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 나쁜 뉴스 체크 중 오류: {e}")
            return {'should_exit': False}

    def _check_technical_bearish_signals(self, ticker, market_data):
        """기술적 약세 신호 체크 (2개 이상)"""
        try:
            bearish_signals = 0
            signal_details = []
            
            # RSI 과매수 체크
            rsi = safe_float_convert(market_data.get('rsi_14', 50), context=f"{ticker} RSI")
            if rsi > 70:
                bearish_signals += 1
                signal_details.append("RSI 과매수")
            
            # MA20 이탈 체크
            ma20 = safe_float_convert(market_data.get('ma20', 0), context=f"{ticker} MA20")
            current_price = safe_float_convert(market_data.get('price', 0), context=f"{ticker} price")
            if ma20 > 0 and current_price < ma20 * 0.98:  # MA20 대비 2% 이하
                bearish_signals += 1
                signal_details.append("MA20 이탈")
            
            # MACD 데드크로스 체크
            macd = safe_float_convert(market_data.get('macd', 0), context=f"{ticker} MACD")
            macd_signal = safe_float_convert(market_data.get('macd_signal', 0), context=f"{ticker} MACD Signal")
            if macd < macd_signal and macd < 0:
                bearish_signals += 1
                signal_details.append("MACD 데드크로스")
            
            # 볼린저 밴드 하단 이탈 체크
            bb_lower = safe_float_convert(market_data.get('bb_lower', 0), context=f"{ticker} BB Lower")
            if bb_lower > 0 and current_price < bb_lower:
                bearish_signals += 1
                signal_details.append("볼린저 하단 이탈")
            
            if bearish_signals >= 2:
                return {
                    'should_exit': True,
                    'reason': f"기술적 약세 신호 {bearish_signals}개 ({', '.join(signal_details)})",
                    'type': "technical_bearish",
                    'priority': 2
                }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 기술적 약세 신호 체크 중 오류: {e}")
            return {'should_exit': False}

    # 기존 중복 함수들 제거됨 - 통합 함수로 대체

    def _execute_sell_order(self, ticker, sell_decision):
        """매도 주문 실행 (피라미딩 고려, 매도 전 정보 캐싱)"""
        try:
            logging.info(f"🔴 {ticker} 매도 조건 충족: {sell_decision['reason']}")
            
            # 1. 매도 전 정확한 정보 캐싱 (DB 기반 + 일관성 검증)
            consistency_check = self._validate_pyramiding_consistency(ticker)
            
            if consistency_check['consistent']:
                # 일관성이 있으면 DB 정보 사용
                position_info = self._calculate_weighted_average_price_from_db(ticker)
                logging.info(f"✅ {ticker} DB와 업비트 API 정보 일치, DB 정보 사용")
            else:
                # 일관성이 없으면 권장 가격 사용
                logging.warning(f"⚠️ {ticker} 정보 불일치: {consistency_check['reason']}")
                
                if consistency_check['recommended_price']:
                    # 권장 가격이 있으면 사용
                    position_info = {
                        'avg_price': consistency_check['recommended_price'],
                        'total_quantity': self._get_balance(portfolio_data, ticker.replace('KRW-', '')) if 'portfolio_data' in locals() else 0,
                        'total_investment': 0,
                        'buy_count': 0,
                        'pyramid_count': consistency_check['pyramid_count']
                    }
                else:
                    # 권장 가격이 없으면 업비트 API 사용 (fallback)
                    logging.warning(f"⚠️ {ticker} 권장 가격 없음, 업비트 API 사용")
                    portfolio_data = self.get_current_positions()
                    avg_price = self._get_avg_price(portfolio_data, ticker.replace('KRW-', ''))
                    position_info = {
                        'avg_price': avg_price,
                        'total_quantity': self._get_balance(portfolio_data, ticker.replace('KRW-', '')),
                        'total_investment': 0,
                        'buy_count': 0,
                        'pyramid_count': 0
                    }
            
            # 2. 매도 실행
            from trade_executor import sell_asset
            sell_result = sell_asset(ticker)
            
            if sell_result and sell_result.get('status') == 'SUCCESS':
                # 3. 매도 후 캐싱된 값으로 로그/DB 기록
                current_price = pyupbit.get_current_price(ticker)
                return_rate = (current_price - position_info['avg_price']) / position_info['avg_price'] * 100 if position_info['avg_price'] else 0
                holding_days = self._calculate_holding_days(ticker)
                
                # 피라미딩 정보 포함 로그
                pyramid_info = f" (매수{position_info['buy_count']}회, 피라미딩{position_info['pyramid_count']}회)" if position_info['pyramid_count'] > 0 else ""
                
                self._log_sell_decision(ticker, current_price, position_info['avg_price'], 
                                       return_rate, sell_decision['type'], 
                                       sell_decision['reason'] + pyramid_info, holding_days)
                
                logging.info(f"✅ {ticker} 매도 완료: {sell_decision['reason']}{pyramid_info}")
                logging.info(f"   💰 평균매수가: {position_info['avg_price']:,.0f}원, 수익률: {return_rate:.1f}%")
            else:
                error_msg = sell_result.get('error') if sell_result else "Unknown error"
                logging.error(f"❌ {ticker} 매도 실패: {sell_decision['reason']} - {error_msg}")
                
        except Exception as e:
            logging.error(f"❌ {ticker} 매도 주문 실행 중 오류: {e}")

    def _get_position_avg_price(self, ticker):
        """포지션의 평균 매수가 조회"""
        try:
            portfolio_data = self.get_current_positions()
            if portfolio_data is not None and not portfolio_data.empty:
                ticker_clean = ticker.replace('KRW-', '')
                if ticker_clean in portfolio_data.index:
                    return self._get_avg_price(portfolio_data, ticker_clean)
            return None
        except Exception as e:
            logging.error(f"❌ {ticker} 포지션 평균 매수가 조회 실패: {e}")
            return None

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
        """매도 결정 로그 기록 - 개선된 버전"""
        try:
            from datetime import datetime
            
            # 🔧 [4순위 개선] 즉시 매도 감지 로깅
            if holding_days is not None and holding_days < 1:
                logging.warning(f"⚠️ {ticker} 즉시 매도 감지: {sell_reason}")
                logging.warning(f"   - 보유기간: {holding_days}일, 수익률: {return_rate:.1f}%")
                logging.warning(f"   - 매수가: {avg_price:,.0f}, 매도가: {current_price:,.0f}")
                logging.warning(f"   - 매도 타입: {sell_type}")
            
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
            
            # 응답 형식 검증 및 로깅
            logging.debug(f"🔍 _get_actual_holdings 응답 타입: {type(balances)}")
            logging.debug(f"🔍 _get_actual_holdings 응답 내용: {balances}")
            
            # 문자열로 반환된 경우 JSON 파싱 시도
            if isinstance(balances, str):
                try:
                    import json
                    balances = json.loads(balances)
                    logging.info("✅ 문자열 응답을 JSON으로 파싱 완료")
                except json.JSONDecodeError as e:
                    logging.error(f"❌ JSON 파싱 실패: {e}")
                    return {}
            
            # 리스트가 아닌 경우 처리 (Pyupbit API 응답 형태에 따라)
            if not isinstance(balances, list):
                logging.info(f"📊 _get_actual_holdings: balances 반환값이 리스트가 아님 (타입: {type(balances)}) - 변환 시도")
                # 딕셔너리인 경우 리스트로 변환 시도
                if isinstance(balances, dict):
                    if 'data' in balances:
                        balances = balances['data']
                        logging.info("✅ 'data' 키에서 리스트 추출 완료")
                    elif 'result' in balances:
                        balances = balances['result']
                        logging.info("✅ 'result' 키에서 리스트 추출 완료")
                    else:
                        # 단일 잔고 정보인 경우 리스트로 변환
                        balances = [balances]
                        logging.info("✅ 단일 잔고 정보를 리스트로 변환 완료")
                else:
                    logging.error(f"❌ 예상치 못한 balances 형식: {type(balances)}")
                    return {}
            
            # KRW 제외한 암호화폐만 추출
            actual_holdings = {}
            for balance in balances:
                try:
                    # balance가 딕셔너리가 아닌 경우 처리
                    if not isinstance(balance, dict):
                        logging.warning(f"⚠️ 예상치 못한 balance 형식: {type(balance)} - {balance}")
                        continue
                    
                    currency = balance.get('currency')
                    if not currency:
                        continue
                    
                    if currency != 'KRW' and float(balance.get('balance', 0)) > 0:
                        ticker = f"KRW-{currency}"
                        actual_holdings[ticker] = {
                            'quantity': float(balance.get('balance', 0)),
                            'avg_price': float(balance.get('avg_buy_price', 0)),
                            'locked': float(balance.get('locked', 0)) if balance.get('locked') else 0
                        }
                except (ValueError, TypeError) as e:
                    logging.error(f"❌ {currency} balance 처리 중 오류: {str(e)}")
                    continue
            
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
        켈리 공식 기반 매도 조건 계산 (개선된 버전)
        
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
            # 1. 개선된 켈리 공식 기반 손절가 계산
            enhanced_kelly_stop = self._calculate_enhanced_kelly_stop_loss(ticker, current_price, avg_price, atr, market_df)
            
            # 2. 기존 켈리 공식 기반 익절가 계산 (유지)
            kelly_take_profit = self._calculate_kelly_take_profit(ticker, current_price, avg_price, atr, return_rate, market_df)
            
            # 3. 손절 조건 체크 (개선된 로직)
            stop_loss_triggered = False
            stop_loss_reason = None
            
            if enhanced_kelly_stop['enabled'] and enhanced_kelly_stop['stop_loss_price'] is not None:
                if current_price <= enhanced_kelly_stop['stop_loss_price']:
                    stop_loss_triggered = True
                    stop_loss_reason = f"켈리 기반 손절 (현재가: {current_price:,.0f}, 손절가: {enhanced_kelly_stop['stop_loss_price']:,.0f}, 켈리비율: {enhanced_kelly_stop['kelly_ratio']:.1%}, 승률: {enhanced_kelly_stop['win_rate']:.1%})"
                else:
                    # 로깅 (켈리 손절매가 활성화되었지만 아직 발동되지 않음)
                    if enhanced_kelly_stop.get('enabled', False):
                        logging.info(f"📊 {ticker} 켈리 손절매 모니터링 - 현재가: {current_price:,.0f}, 손절가: {enhanced_kelly_stop['stop_loss_price']:,.0f}, 켈리비율: {enhanced_kelly_stop['kelly_ratio']:.1%}")
            else:
                # 켈리 손절매가 비활성화된 이유 로깅
                if enhanced_kelly_stop.get('reason'):
                    logging.debug(f"📊 {ticker} 켈리 손절매 비활성화: {enhanced_kelly_stop['reason']}")
            
            # 4. 익절 조건 체크 (기존 로직 유지)
            take_profit_triggered = False
            take_profit_reason = None
            
            if kelly_take_profit['take_profit_price'] > 0 and current_price >= kelly_take_profit['take_profit_price']:
                take_profit_triggered = True
                take_profit_reason = f"켈리 기반 익절 (현재가: {current_price:,.0f}, 익절가: {kelly_take_profit['take_profit_price']:,.0f}, 켈리비율: {kelly_take_profit['kelly_ratio']:.1%})"
            
            return {
                'stop_loss_triggered': stop_loss_triggered,
                'stop_loss_reason': stop_loss_reason,
                'stop_loss_price': enhanced_kelly_stop.get('stop_loss_price', 0),
                'kelly_stop_ratio': enhanced_kelly_stop.get('kelly_ratio', 0),
                'win_rate': enhanced_kelly_stop.get('win_rate', 0),
                'stop_loss_enabled': enhanced_kelly_stop.get('enabled', False),
                'stop_loss_reason_disabled': enhanced_kelly_stop.get('reason', ''),
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
                'win_rate': 0,
                'stop_loss_enabled': False,
                'stop_loss_reason_disabled': f'오류: {e}',
                'take_profit_triggered': False,
                'take_profit_reason': None,
                'take_profit_price': 0,
                'kelly_take_ratio': 0
            }
    
    # Kelly 기반 이익실현 함수 제거됨 - 통합 함수로 대체
    
    # 시장 기반 익절 함수 제거됨 - 통합 함수로 대체
    
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

    def simple_portfolio_summary(self):
        """간단한 포트폴리오 요약"""
        try:
            positions = self.get_current_positions()
            if not positions:
                return "보유 종목 없음"
            
            summary = []
            total_value = 0
            
            # positions가 리스트인 경우 처리
            if isinstance(positions, list):
                for item in positions:
                    if isinstance(item, dict):
                        currency = item.get('currency', '')
                        balance = float(item.get('balance', 0))
                        avg_price = float(item.get('avg_buy_price', 0))
                        
                        if currency == 'KRW':
                            value = balance
                            summary.append(f"{currency}: {balance:,.0f}원")
                        else:
                            ticker = f"KRW-{currency}"
                            current_price = pyupbit.get_current_price(ticker)
                            if current_price:
                                value = balance * current_price
                                summary.append(f"{ticker}: {balance:.8f}개 @ {avg_price:,.0f}원 (현재가: {current_price:,.0f}원)")
                            else:
                                value = balance * avg_price
                                summary.append(f"{ticker}: {balance:.8f}개 @ {avg_price:,.0f}원")
                        
                        total_value += value
            # positions가 딕셔너리인 경우 처리 (기존 로직)
            elif isinstance(positions, dict):
                for ticker, data in positions.items():
                    value = data['quantity'] * data['avg_price']
                    total_value += value
                    summary.append(f"{ticker}: {data['quantity']:.8f}개 @ {data['avg_price']:,.0f}원")
            else:
                return f"포트폴리오 요약 실패: 예상치 못한 데이터 형식 ({type(positions)})"
            
            return f"총 {len(summary)}개 종목, 총 가치: {total_value:,.0f}원\n" + "\n".join(summary)
            
        except Exception as e:
            logging.error(f"❌ 포트폴리오 요약 생성 중 오류: {e}")
            return f"포트폴리오 요약 실패: {e}"

    def _check_recent_price_trend(self, ticker, days=3):
        """최근 N일간의 가격 추세 확인"""
        try:
            # 최근 N일간의 OHLCV 데이터 조회
            ohlcv_data = self._get_ohlcv_from_db(ticker, limit=days+5)  # 여유분 포함
            
            if ohlcv_data.empty or len(ohlcv_data) < days:
                return {'is_uptrend': False, 'reason': '데이터 부족'}
            
            # 최근 N일간의 종가 추이 확인
            recent_closes = ohlcv_data['close'].tail(days).values
            
            # 상승 추세 판단 (단순한 조건)
            price_increases = sum(1 for i in range(1, len(recent_closes)) 
                                if recent_closes[i] > recent_closes[i-1])
            
            # 60% 이상의 날짜에서 상승했으면 상승추세로 판단
            is_uptrend = price_increases >= len(recent_closes) * 0.6
            
            return {
                'is_uptrend': is_uptrend,
                'price_increases': price_increases,
                'total_days': len(recent_closes),
                'reason': f"상승일: {price_increases}/{len(recent_closes)}"
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 최근 가격 추세 확인 중 오류: {e}")
            return {'is_uptrend': False, 'reason': f'오류: {e}'}

    def _check_strong_uptrend_conditions(self, ticker, current_price, avg_price, return_rate, 
                                        rsi, ma20, macd, macd_signal):
        """강한 상승추세 조건 확인"""
        try:
            from config import TRAILING_STOP_CONFIG
            
            config = TRAILING_STOP_CONFIG.get('strong_uptrend_conditions', {})
            
            # 강한 상승추세 조건 체크
            conditions = []
            
            # RSI 조건
            rsi_min = config.get('rsi_min', 60)
            rsi_max = config.get('rsi_max', 80)
            if rsi_min <= rsi <= rsi_max:
                conditions.append(True)
            else:
                conditions.append(False)
            
            # MA20 대비 상승률 조건
            ma20_rise_pct = config.get('ma20_rise_pct', 2.0)
            if ma20 > 0 and current_price > ma20 * (1 + ma20_rise_pct/100):
                conditions.append(True)
            else:
                conditions.append(False)
            
            # MACD 양수 조건
            if config.get('macd_positive', True):
                if macd > macd_signal:
                    conditions.append(True)
                else:
                    conditions.append(False)
            
            # 최소 수익률 조건
            min_profit_pct = config.get('min_profit_pct', 10.0)
            if return_rate >= min_profit_pct:
                conditions.append(True)
            else:
                conditions.append(False)
            
            # 모든 조건이 만족되면 강한 상승추세
            is_strong_uptrend = all(conditions)
            
            return {
                'is_strong_uptrend': is_strong_uptrend,
                'conditions_met': sum(conditions),
                'total_conditions': len(conditions),
                'reason': f"조건 충족: {conditions_met}/{len(conditions)}"
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 강한 상승추세 조건 확인 중 오류: {e}")
            return {'is_strong_uptrend': False, 'reason': f'오류: {e}'}

    def _calculate_enhanced_kelly_stop_loss(self, ticker, current_price, avg_price, atr, market_df):
        """개선된 켈리 공식 기반 손절가 계산"""
        try:
            from config import TRAILING_STOP_CONFIG
            
            kelly_config = TRAILING_STOP_CONFIG.get('kelly_stop_loss', {})
            
            # 1. 기본 설정값 가져오기
            min_holding_days = kelly_config.get('min_holding_days', 3)
            min_win_rate = kelly_config.get('min_win_rate', 0.4)
            min_kelly_ratio = kelly_config.get('min_kelly_ratio', 0.05)
            max_stop_loss_pct = kelly_config.get('max_stop_loss_pct', 15.0)
            min_stop_loss_pct = kelly_config.get('min_stop_loss_pct', 5.0)
            atr_multiplier = kelly_config.get('atr_multiplier', 2.0)
            profit_threshold_pct = kelly_config.get('profit_threshold_pct', 5.0)
            
            # 2. 보유기간 확인
            holding_days = self._calculate_holding_days(ticker)
            if holding_days is None or holding_days < min_holding_days:
                return {
                    'stop_loss_price': None,
                    'stop_loss_pct': None,
                    'kelly_ratio': 0.0,
                    'win_rate': 0.0,
                    'reason': f'보유기간 부족 ({holding_days}일 < {min_holding_days}일)',
                    'enabled': False
                }
            
            # 3. 수익률 확인 (수익 구간에서만 켈리 손절매 적용)
            return_rate = ((current_price - avg_price) / avg_price) * 100
            if return_rate < profit_threshold_pct:
                return {
                    'stop_loss_price': None,
                    'stop_loss_pct': None,
                    'kelly_ratio': 0.0,
                    'win_rate': 0.0,
                    'reason': f'수익 구간 미진입 (수익률: {return_rate:.1f}% < {profit_threshold_pct}%)',
                    'enabled': False
                }
            
            # 4. 시장 데이터에서 승률 추정
            if ticker not in market_df.index:
                return {
                    'stop_loss_price': None,
                    'stop_loss_pct': None,
                    'kelly_ratio': 0.0,
                    'win_rate': 0.0,
                    'reason': '시장 데이터 없음',
                    'enabled': False
                }
            
            market_data = market_df.loc[ticker]
            
            # 승률 추정 (기술적 지표 기반)
            win_rate = self._estimate_win_rate_from_indicators(market_data)
            
            if win_rate < min_win_rate:
                return {
                    'stop_loss_price': None,
                    'stop_loss_pct': None,
                    'kelly_ratio': 0.0,
                    'win_rate': win_rate,
                    'reason': f'승률 부족 ({win_rate:.1%} < {min_win_rate:.1%})',
                    'enabled': False
                }
            
            # 5. 켈리비율 계산
            avg_win_pct = safe_float_convert(market_data.get('avg_win_pct', 8.0), context=f"{ticker} avg_win_pct")
            avg_loss_pct = safe_float_convert(market_data.get('avg_loss_pct', 5.0), context=f"{ticker} avg_loss_pct")
            
            if avg_loss_pct <= 0:
                avg_loss_pct = 5.0  # 기본값
            
            # 켈리 공식: f = (bp - q) / b
            # b = 평균 수익률 / 평균 손실률
            # p = 승률, q = 패률 (1-p)
            b = avg_win_pct / avg_loss_pct
            p = win_rate
            q = 1 - p
            
            kelly_ratio = (b * p - q) / b if b > 0 else 0.0
            
            # 켈리비율 제한 (0.1 ~ 0.3)
            kelly_ratio = max(0.0, min(kelly_ratio, 0.3))
            
            if kelly_ratio < min_kelly_ratio:
                return {
                    'stop_loss_price': None,
                    'stop_loss_pct': None,
                    'kelly_ratio': kelly_ratio,
                    'win_rate': win_rate,
                    'reason': f'켈리비율 부족 ({kelly_ratio:.1%} < {min_kelly_ratio:.1%})',
                    'enabled': False
                }
            
            # 6. 손절가 계산
            # ATR 기반 기본 손절 비율
            if atr > 0:
                base_stop_loss_pct = (atr / current_price) * 100 * atr_multiplier
            else:
                base_stop_loss_pct = 8.0  # 기본값
            
            # 켈리비율 기반 조정
            kelly_adjusted_pct = base_stop_loss_pct * (1 + kelly_ratio)
            
            # 최소/최대 제한
            final_stop_loss_pct = max(min_stop_loss_pct, min(kelly_adjusted_pct, max_stop_loss_pct))
            
            # 손절가 계산
            stop_loss_price = current_price * (1 - final_stop_loss_pct / 100)
            
            # 7. 추세 고려 (강한 상승추세 시 손절가 완화)
            if kelly_config.get('trend_consideration', True):
                trend_adjustment = self._calculate_trend_adjustment(ticker, current_price, market_data)
                if trend_adjustment > 1.0:
                    final_stop_loss_pct *= trend_adjustment
                    stop_loss_price = current_price * (1 - final_stop_loss_pct / 100)
            
            return {
                'stop_loss_price': stop_loss_price,
                'stop_loss_pct': final_stop_loss_pct,
                'kelly_ratio': kelly_ratio,
                'win_rate': win_rate,
                'avg_win_pct': avg_win_pct,
                'avg_loss_pct': avg_loss_pct,
                'reason': f'켈리 손절매 활성화 (승률: {win_rate:.1%}, 켈리비율: {kelly_ratio:.1%})',
                'enabled': True
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 개선된 켈리 손절가 계산 중 오류: {e}")
            return {
                'stop_loss_price': None,
                'stop_loss_pct': None,
                'kelly_ratio': 0.0,
                'win_rate': 0.0,
                'reason': f'계산 오류: {e}',
                'enabled': False
            }

    def _estimate_win_rate_from_indicators(self, market_data):
        """기술적 지표 기반 승률 추정"""
        try:
            # 기본 승률
            base_win_rate = 0.5
            
            # RSI 기반 조정
            rsi = safe_float_convert(market_data.get('rsi_14', 50), context="rsi_14")
            if 40 <= rsi <= 70:  # 적정 구간
                base_win_rate += 0.1
            elif rsi < 30 or rsi > 80:  # 극단 구간
                base_win_rate -= 0.1
            
            # ADX 기반 조정 (추세 강도)
            adx = safe_float_convert(market_data.get('adx', 25), context="adx")
            if adx > 25:  # 강한 추세
                base_win_rate += 0.05
            elif adx < 20:  # 약한 추세
                base_win_rate -= 0.05
            
            # MACD 기반 조정
            macd = safe_float_convert(market_data.get('macd', 0), context="macd")
            macd_signal = safe_float_convert(market_data.get('macd_signal', 0), context="macd_signal")
            if macd > macd_signal and macd > 0:  # 양수 MACD + 골든크로스
                base_win_rate += 0.1
            elif macd < macd_signal and macd < 0:  # 음수 MACD + 데드크로스
                base_win_rate -= 0.1
            
            # 볼린저 밴드 기반 조정
            bb_position = safe_float_convert(market_data.get('bb_position', 0.5), context="bb_position")
            if bb_position > 0.8:  # 상단 근처
                base_win_rate -= 0.05
            elif bb_position < 0.2:  # 하단 근처
                base_win_rate += 0.05
            
            # 최종 승률 제한 (0.3 ~ 0.8)
            final_win_rate = max(0.3, min(base_win_rate, 0.8))
            
            return final_win_rate
            
        except Exception as e:
            logging.error(f"❌ 승률 추정 중 오류: {e}")
            return 0.5  # 기본값

    def _calculate_trend_adjustment(self, ticker, current_price, market_data):
        """추세 기반 손절가 조정 계수"""
        try:
            # 기본 조정 계수
            adjustment = 1.0
            
            # RSI 기반 조정
            rsi = safe_float_convert(market_data.get('rsi_14', 50), context="rsi_14")
            if 60 <= rsi <= 75:  # 강한 상승추세
                adjustment *= 1.2
            elif rsi > 75:  # 과매수 구간
                adjustment *= 0.8
            
            # MACD 기반 조정
            macd = safe_float_convert(market_data.get('macd', 0), context="macd")
            macd_signal = safe_float_convert(market_data.get('macd_signal', 0), context="macd_signal")
            if macd > macd_signal and macd > 0:
                adjustment *= 1.1
            
            # MA20 기반 조정
            ma20 = safe_float_convert(market_data.get('ma_20', current_price), context="ma_20")
            if current_price > ma20 * 1.05:  # MA20 대비 5% 이상 상승
                adjustment *= 1.15
            
            # 최종 조정 계수 제한 (0.8 ~ 1.5)
            final_adjustment = max(0.8, min(adjustment, 1.5))
            
            return final_adjustment
            
        except Exception as e:
            logging.error(f"❌ {ticker} 추세 조정 계산 중 오류: {e}")
            return 1.0  # 기본값
    
    # 기존 _check_weinstein_stage_exit 함수는 _check_weinstein_stage4_exit와 _check_weinstein_stage3_exit로 분리됨
    
    def _calculate_volume_ratio(self, ticker, ohlcv_data):
        """거래량 비율 계산 (최근 5일 평균 대비)"""
        try:
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 10:
                return 1.0
            
            # 최근 5일 거래량 평균
            recent_volume_avg = ohlcv_data['volume'].tail(5).mean()
            
            # 최근 20일 거래량 평균
            long_term_volume_avg = ohlcv_data['volume'].tail(20).mean()
            
            if long_term_volume_avg > 0:
                volume_ratio = recent_volume_avg / long_term_volume_avg
                return volume_ratio
            
            return 1.0
            
        except Exception as e:
            logging.error(f"❌ {ticker} 거래량 비율 계산 중 오류: {e}")
            return 1.0
    
    def _check_volume_pattern_change(self, ticker, ohlcv_data):
        """거래량 패턴 변화 감지 (와인스타인 Stage 3 분배 단계 감지용)"""
        try:
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 10:
                return {'pattern_detected': False}
            
            # 최근 10일 데이터 분석
            recent_data = ohlcv_data.tail(10)
            
            # 거래량 증가 추세 확인
            volume_trend = recent_data['volume'].values
            price_trend = recent_data['close'].values
            
            # 거래량은 증가하지만 가격은 정체되는 패턴 감지
            volume_increasing = volume_trend[-3:].mean() > volume_trend[:3].mean() * 1.2
            price_stagnant = abs(price_trend[-1] - price_trend[-5]) / price_trend[-5] < 0.02  # 2% 이내 변동
            
            if volume_increasing and price_stagnant:
                return {
                    'pattern_detected': True,
                    'type': 'volume_increase_price_stagnant',
                    'reason': '거래량 증가하지만 가격 정체 (분배 단계 의심)'
                }
            
            # 거래량 급감 패턴 감지 (매도 압력 소진)
            volume_decreasing = volume_trend[-3:].mean() < volume_trend[:3].mean() * 0.7
            if volume_decreasing:
                return {
                    'pattern_detected': True,
                    'type': 'volume_decreasing',
                    'reason': '거래량 급감 (매도 압력 소진)'
                }
            
            return {'pattern_detected': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 거래량 패턴 분석 중 오류: {e}")
            return {'pattern_detected': False}
    
    def _check_gap_down_exit(self, ticker, current_price, avg_price, return_rate, ohlcv_data):
        """갭하락 감지 및 즉시 매도 조건 (오닐 전략) - 개선된 버전"""
        try:
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 2:
                return {'should_exit': False}
            
            # 🔧 [1순위 개선] 보유기간 체크 추가
            holding_days = self._calculate_holding_days(ticker)
            
            # 매수 직후 24시간 이내에는 갭하락 매도 비활성화
            if holding_days is not None and holding_days < 1:
                logging.info(f"🛡️ {ticker} 매수 직후 24시간 이내 - 갭하락 매도 조건 비활성화")
                return {'should_exit': False}
            
            # 최근 2일 데이터 조회
            recent_data = ohlcv_data.tail(2)
            if len(recent_data) < 2:
                return {'should_exit': False}
            
            # 전일 고가와 당일 저가 비교
            prev_high = recent_data.iloc[-2]['high']
            today_low = recent_data.iloc[-1]['low']
            
            # 갭하락 감지 (전일 고가보다 당일 저가가 낮은 경우)
            if today_low < prev_high:
                gap_size = (prev_high - today_low) / prev_high * 100
                
                # 🔧 [1순위 개선] 보유기간별 갭하락 임계값 조정 (암호화폐 변동성 고려)
                if holding_days is not None:
                    if holding_days <= 3:  # 3일 이내: 매우 관대한 임계값
                        gap_threshold = 8.0  # 8% 이상
                        logging.info(f"🛡️ {ticker} 단기 보유({holding_days}일) - 갭하락 임계값: {gap_threshold}%")
                    elif holding_days <= 7:  # 7일 이내: 관대한 임계값
                        gap_threshold = 6.0  # 6% 이상
                        logging.info(f"🛡️ {ticker} 중기 보유({holding_days}일) - 갭하락 임계값: {gap_threshold}%")
                    else:  # 7일 초과: 기본 임계값
                        gap_threshold = 4.0  # 4% 이상
                        logging.info(f"🛡️ {ticker} 장기 보유({holding_days}일) - 갭하락 임계값: {gap_threshold}%")
                else:
                    gap_threshold = 6.0  # 기본값 (암호화폐 변동성 고려)
                
                # 갭 크기가 임계값 이상인 경우 매도
                if gap_size >= gap_threshold:
                    return {
                        'should_exit': True,
                        'reason': f'오닐 갭하락 즉시 매도 (갭 크기: {gap_size:.1f}%, 임계값: {gap_threshold}%)',
                        'type': 'oneil_gap_down_exit'
                    }
                
                # 🔧 [1순위 개선] 수익률 조건 강화 (암호화폐 변동성 고려)
                elif gap_size >= 2.0 and return_rate < -8:  # 손실이 8% 이상일 때만
                    return {
                        'should_exit': True,
                        'reason': f'오닐 갭하락 매도 (갭 크기: {gap_size:.1f}%, 수익률: {return_rate:.1f}%)',
                        'type': 'oneil_gap_down_exit'
                    }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 갭하락 체크 중 오류: {e}")
            return {'should_exit': False}
    
    # 기존 _check_bad_news_exit 함수는 통합 추세전환 로직으로 이동됨

    def _check_atr_based_exit_conditions(self, ticker, current_price, avg_price, atr, return_rate, 
                                       max_price_since_buy, holding_days, atr_ratio):
        """
        ATR 기반 매도 조건 통합 함수
        
        이 함수는 ATR(Average True Range)을 기반으로 한 두 가지 매도 전략을 통합 관리합니다:
        1. ATR 기반 동적 손절매: 변동성에 따른 동적 손절가 계산
        2. ATR 기반 트레일링 스탑: 고점 대비 하락률 기반 익절
        
        Args:
            ticker: 티커 심볼
            current_price: 현재가
            avg_price: 평균 매수가
            atr: ATR 값
            return_rate: 수익률
            max_price_since_buy: 매수 후 최고가
            holding_days: 보유기간
            atr_ratio: ATR 비율 (atr / current_price)
            
        Returns:
            dict: 매도 조건 결과
        """
        try:
            from config import TRAILING_STOP_CONFIG
            
            # 1. ATR 기반 동적 손절매 조건 체크
            stop_loss_result = self._check_atr_stop_loss(
                ticker, current_price, avg_price, atr, return_rate, 
                holding_days, atr_ratio
            )
            
            if stop_loss_result['should_exit']:
                return stop_loss_result
            
            # 2. ATR 기반 트레일링 스탑 조건 체크
            trailing_stop_result = self._check_atr_trailing_stop(
                ticker, current_price, avg_price, atr, return_rate,
                max_price_since_buy, holding_days, atr_ratio, TRAILING_STOP_CONFIG
            )
            
            if trailing_stop_result['should_exit']:
                return trailing_stop_result
            
            # 3. 매도 조건 미충족
            return {
                'should_exit': False,
                'reason': None,
                'type': None
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} ATR 기반 매도 조건 체크 중 오류: {e}")
            return {
                'should_exit': False,
                'reason': None,
                'type': None
            }
    
    def _check_atr_stop_loss(self, ticker, current_price, avg_price, atr, return_rate, 
                           holding_days, atr_ratio):
        """
        Makenaide 전략 기반 ATR 손절매 조건 체크
        - 1차: 손실이 -8% 이하이면 무조건 손절 (보유기간/수익률 무관)
        - 2차: ATR 기반 동적 손절은 보조적(추가적)으로만 활용
        - 보유기간/수익률 조건은 경고만 남기고, 손절 자체는 막지 않음
        Args:
            ticker: 티커
            current_price: 현재가
            avg_price: 평균 매수가
            atr: ATR 값
            return_rate: 수익률(%)
            holding_days: 보유기간
            atr_ratio: ATR/현재가
        Returns:
            dict: {'should_exit': bool, 'reason': str, 'type': str}
        """
        try:
            from config import TRAILING_STOP_CONFIG
            kelly_config = TRAILING_STOP_CONFIG.get('kelly_stop_loss', {})
            min_holding_days = kelly_config.get('min_holding_days', 3)
            profit_threshold_pct = kelly_config.get('profit_threshold_pct', 5.0)

            # 1차: -8% 이하 손실이면 무조건 손절
            if return_rate <= -8.0:
                reason = f"Makenaide 1차 손절: -8% 이하 손실 (수익률: {return_rate:.1f}%)"
                if holding_days is not None and holding_days < min_holding_days:
                    reason += f" [경고: 보유기간 {holding_days}일 < {min_holding_days}일]"
                if return_rate < profit_threshold_pct:
                    reason += f" [경고: 수익률 {return_rate:.1f}% < {profit_threshold_pct}%]"
                return {
                    'should_exit': True,
                    'reason': reason,
                    'type': 'makenaide_basic_stop_loss'
                }

            # 2차: ATR 기반 동적 손절 (보조적)
            # 변동성에 따른 동적 손절 비율 조정
            if atr_ratio > 0.05:  # 고변동성
                atr_multiplier = 1.5
            elif atr_ratio > 0.03:  # 중변동성
                atr_multiplier = 2.0
            else:  # 저변동성
                atr_multiplier = 2.5
            atr_stop_loss_pct = min(max((atr / avg_price) * 100 * atr_multiplier, 2.0), 10.0)
            if return_rate <= -atr_stop_loss_pct:
                return {
                    'should_exit': True,
                    'reason': f"ATR 기반 동적 손절 (수익률: {return_rate:.1f}%, 기준: -{atr_stop_loss_pct:.1f}%, 변동성: {atr_ratio:.2%})",
                    'type': "atr_stop_loss"
                }

            # 3차: 손절 조건 미충족
            return {'should_exit': False}

        except Exception as e:
            logging.error(f"❌ {ticker} ATR 손절매 체크 중 오류: {e}")
            return {'should_exit': False}
    
    # ATR 트레일링스탑 함수 제거됨 - 통합 함수로 대체

    def get_portfolio_summary(self, include_krw: bool = True):
        """
        포트폴리오 요약 정보 반환
        
        Args:
            include_krw: KRW 포함 여부 (기본값: True)
            
        Returns:
            dict: 포트폴리오 요약 정보
        """
        try:
            positions = self.get_current_positions(include_krw=include_krw)
            
            if not positions:
                return {
                    'total_positions': 0,
                    'total_value': 0,
                    'total_krw': 0,
                    'total_crypto_value': 0,
                    'avg_return_rate': 0,
                    'total_unrealized_pnl': 0,
                    'positions': []
                }
            
            total_value = sum(pos.get('value', 0) for pos in positions)
            total_krw = sum(pos.get('value', 0) for pos in positions if pos.get('currency') == 'KRW')
            total_crypto_value = total_value - total_krw
            
            # 수익률 계산 (KRW 제외)
            crypto_positions = [pos for pos in positions if pos.get('currency') != 'KRW']
            if crypto_positions:
                total_unrealized_pnl = sum(pos.get('unrealized_pnl', 0) for pos in crypto_positions)
                avg_return_rate = sum(pos.get('return_rate', 0) for pos in crypto_positions) / len(crypto_positions)
            else:
                total_unrealized_pnl = 0
                avg_return_rate = 0
            
            return {
                'total_positions': len(positions),
                'total_value': total_value,
                'total_krw': total_krw,
                'total_crypto_value': total_crypto_value,
                'avg_return_rate': avg_return_rate,
                'total_unrealized_pnl': total_unrealized_pnl,
                'positions': positions
            }
            
        except Exception as e:
            logging.error(f"❌ 포트폴리오 요약 생성 중 오류: {e}")
            return {
                'total_positions': 0,
                'total_value': 0,
                'total_krw': 0,
                'total_crypto_value': 0,
                'avg_return_rate': 0,
                'total_unrealized_pnl': 0,
                'positions': [],
                'error': str(e)
            }
    
    def get_position_by_ticker(self, ticker: str):
        """
        특정 티커의 포지션 정보 조회
        
        Args:
            ticker: 티커 심볼 (예: "KRW-BTC")
            
        Returns:
            dict: 포지션 정보 또는 None
        """
        try:
            positions = self.get_current_positions()
            
            for position in positions:
                if position.get('ticker') == ticker:
                    return position
            
            return None
            
        except Exception as e:
            logging.error(f"❌ {ticker} 포지션 조회 중 오류: {e}")
            return None
    
    def calculate_portfolio_metrics(self):
        """
        포트폴리오 메트릭 계산 (변동성, 베타, 샤프 비율 등)
        
        Returns:
            dict: 포트폴리오 메트릭
        """
        try:
            positions = self.get_current_positions()
            
            if not positions:
                return {
                    'total_value': 0,
                    'diversification_score': 0,
                    'risk_score': 0,
                    'concentration_risk': 0
                }
            
            # 총 가치
            total_value = sum(pos.get('value', 0) for pos in positions)
            
            # 포지션별 비중 계산
            position_weights = []
            for pos in positions:
                weight = pos.get('value', 0) / total_value if total_value > 0 else 0
                position_weights.append(weight)
            
            # 다양화 점수 (Herfindahl-Hirschman Index 기반)
            hhi = sum(weight ** 2 for weight in position_weights)
            diversification_score = 1 - hhi  # 0에 가까울수록 집중, 1에 가까울수록 다양화
            
            # 리스크 점수 (수익률 변동성 기반)
            return_rates = [pos.get('return_rate', 0) for pos in positions if pos.get('return_rate') is not None]
            if return_rates:
                import numpy as np
                risk_score = np.std(return_rates) if len(return_rates) > 1 else 0
            else:
                risk_score = 0
            
            # 집중도 리스크 (최대 포지션 비중)
            concentration_risk = max(position_weights) if position_weights else 0
            
            return {
                'total_value': total_value,
                'diversification_score': diversification_score,
                'risk_score': risk_score,
                'concentration_risk': concentration_risk,
                'position_count': len(positions)
            }
            
        except Exception as e:
            logging.error(f"❌ 포트폴리오 메트릭 계산 중 오류: {e}")
            return {
                'total_value': 0,
                'diversification_score': 0,
                'risk_score': 0,
                'concentration_risk': 0,
                'error': str(e)
            }
    
    def validate_portfolio_health(self):
        """
        포트폴리오 건강도 검증
        
        Returns:
            dict: 포트폴리오 건강도 정보
        """
        try:
            metrics = self.calculate_portfolio_metrics()
            positions = self.get_current_positions()
            
            health_issues = []
            warnings = []
            
            # 1. 집중도 리스크 체크
            if metrics['concentration_risk'] > 0.3:  # 30% 이상
                health_issues.append(f"높은 집중도 리스크: {metrics['concentration_risk']:.1%}")
            elif metrics['concentration_risk'] > 0.2:  # 20% 이상
                warnings.append(f"집중도 주의: {metrics['concentration_risk']:.1%}")
            
            # 2. 다양화 점수 체크
            if metrics['diversification_score'] < 0.5:
                health_issues.append(f"낮은 다양화: {metrics['diversification_score']:.1%}")
            elif metrics['diversification_score'] < 0.7:
                warnings.append(f"다양화 개선 필요: {metrics['diversification_score']:.1%}")
            
            # 3. 포지션 수 체크
            if len(positions) < 3:
                health_issues.append(f"포지션 수 부족: {len(positions)}개")
            elif len(positions) < 5:
                warnings.append(f"포지션 수 적음: {len(positions)}개")
            
            # 4. 손실 포지션 체크
            loss_positions = [pos for pos in positions if pos.get('return_rate', 0) < -10]
            if len(loss_positions) > len(positions) * 0.5:  # 50% 이상 손실
                health_issues.append(f"손실 포지션 과다: {len(loss_positions)}/{len(positions)}")
            elif len(loss_positions) > len(positions) * 0.3:  # 30% 이상 손실
                warnings.append(f"손실 포지션 많음: {len(loss_positions)}/{len(positions)}")
            
            # 5. 총 자산 체크
            total_balance = self.get_total_balance()
            if total_balance < 100000:  # 10만원 미만
                health_issues.append(f"총 자산 부족: {total_balance:,.0f}원")
            
            return {
                'is_healthy': len(health_issues) == 0,
                'health_score': max(0, 100 - len(health_issues) * 20 - len(warnings) * 10),
                'health_issues': health_issues,
                'warnings': warnings,
                'metrics': metrics,
                'total_positions': len(positions),
                'total_value': metrics['total_value']
            }
            
        except Exception as e:
            logging.error(f"❌ 포트폴리오 건강도 검증 중 오류: {e}")
            return {
                'is_healthy': False,
                'health_score': 0,
                'health_issues': [f"검증 오류: {str(e)}"],
                'warnings': [],
                'error': str(e)
            }

    def _check_unified_profit_taking_and_trailing_stop(self, ticker, current_price, avg_price, atr, return_rate, 
                                                      holding_days, atr_ratio, market_data, ohlcv_data):
        """
        통합 이익실현 및 트레일링스탑 로직 (우선순위 기반)
        
        우선순위:
        1. Big Winner 보유 (매도하지 않음)
        2. 기본 익절 (20% 이상)
        3. 보유기간 기반 익절
        4. 시장 상황 기반 익절
        5. 트레일링스탑 (ATR 기반)
        
        Args:
            ticker: 티커 심볼
            current_price: 현재 가격
            avg_price: 평균 매수가
            atr: ATR 값
            return_rate: 수익률 (%)
            holding_days: 보유기간 (일)
            atr_ratio: ATR 비율
            market_data: 시장 데이터
            ohlcv_data: OHLCV 데이터
            
        Returns:
            dict: 매도 결정 정보
        """
        try:
            # 1. Big Winner 보유 (매도하지 않음) - 최우선
            if return_rate >= 100.0:
                logging.info(f"🏆 {ticker} Big Winner 보유 중 (수익률: {return_rate:.1f}%) - 계속 보유")
                return {'should_exit': False}
            
            # 2. 기본 익절 (20% 이상) - 높은 우선순위
            if return_rate >= 20.0:
                return {
                    'should_exit': True,
                    'reason': f"기본 익절 (수익률: {return_rate:.1f}%)",
                    'type': "basic_take_profit",
                    'priority': 1
                }
            
            # 3. 보유기간 기반 익절 - 중간 우선순위
            holding_based_exit = self._check_holding_based_profit_taking(return_rate, holding_days)
            if holding_based_exit['should_exit']:
                return holding_based_exit
            
            # 4. 시장 상황 기반 익절 - 중간 우선순위
            market_based_exit = self._check_market_based_profit_taking(ticker, return_rate, market_data)
            if market_based_exit['should_exit']:
                return market_based_exit
            
            # 5. 트레일링스탑 (ATR 기반) - 낮은 우선순위
            trailing_stop_exit = self._check_unified_trailing_stop(ticker, current_price, avg_price, atr, 
                                                                 return_rate, holding_days, atr_ratio, ohlcv_data)
            if trailing_stop_exit['should_exit']:
                return trailing_stop_exit
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 통합 이익실현 및 트레일링스탑 체크 중 오류: {e}")
            return {'should_exit': False}

    def _check_holding_based_profit_taking(self, return_rate, holding_days):
        """
        보유기간 기반 이익실현
        
        - 단기 (3일 이내): 보수적 익절 (15% 이상)
        - 중기 (7일 이내): 일반 익절 (18% 이상)
        - 장기 (7일 초과): 적극적 익절 (12% 이상)
        """
        try:
            if holding_days is None:
                return {'should_exit': False}
            
            if holding_days <= 3:  # 단기: 보수적 익절
                if return_rate >= 15.0:
                    return {
                        'should_exit': True,
                        'reason': f"단기 보수적 익절 (보유 {holding_days}일, 수익률: {return_rate:.1f}%)",
                        'type': "short_term_exit",
                        'priority': 2
                    }
            elif holding_days <= 7:  # 중기: 일반 익절
                if return_rate >= 18.0:
                    return {
                        'should_exit': True,
                        'reason': f"중기 익절 (보유 {holding_days}일, 수익률: {return_rate:.1f}%)",
                        'type': "medium_term_exit",
                        'priority': 2
                    }
            else:  # 장기: 적극적 익절
                if return_rate >= 12.0:
                    return {
                        'should_exit': True,
                        'reason': f"장기 적극적 익절 (보유 {holding_days}일, 수익률: {return_rate:.1f}%)",
                        'type': "long_term_exit",
                        'priority': 2
                    }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"보유기간 기반 익절 체크 중 오류: {e}")
            return {'should_exit': False}

    def _check_market_based_profit_taking(self, ticker, return_rate, market_data):
        """
        시장 상황 기반 이익실현
        
        - 고수익 + 약세 신호 조합
        - 기술적 지표 기반 판단
        """
        try:
            # 고수익 + 약세 시그널 (25% 이상)
            if return_rate >= 25.0:
                bearish_signals = 0
                
                # 기술적 지표 체크
                current_price = safe_float_convert(market_data.get('price', 0), context=f"{ticker} price")
                ma20 = safe_float_convert(market_data.get('ma20', 0), context=f"{ticker} MA20")
                rsi = safe_float_convert(market_data.get('rsi_14', 50), context=f"{ticker} RSI")
                macd = safe_float_convert(market_data.get('macd', 0), context=f"{ticker} MACD")
                macd_signal = safe_float_convert(market_data.get('macd_signal', 0), context=f"{ticker} MACD Signal")
                
                # 약세 신호 카운트
                if ma20 > 0 and current_price < ma20:
                    bearish_signals += 1
                if rsi > 70:  # 과매수 상태
                    bearish_signals += 1
                if macd < macd_signal:
                    bearish_signals += 1
                
                if bearish_signals >= 1:
                    return {
                        'should_exit': True,
                        'reason': f"고수익 + 약세신호 익절 (수익률: {return_rate:.1f}%, 약세신호: {bearish_signals}개)",
                        'type': "high_profit_exit",
                        'priority': 2
                    }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 시장 상황 기반 익절 체크 중 오류: {e}")
            return {'should_exit': False}

    def _check_unified_trailing_stop(self, ticker, current_price, avg_price, atr, return_rate, 
                                   holding_days, atr_ratio, ohlcv_data):
        """
        통합 트레일링스탑 (ATR 기반)
        
        - 암호화폐 시장 특성 반영
        - 변동성 기반 동적 조정
        - 보유기간 기반 점진적 완화
        """
        try:
            from config import TRAILING_STOP_CONFIG
            
            # 기본 설정
            min_rise_pct = TRAILING_STOP_CONFIG.get('min_rise_pct', 8.0)
            min_holding_days = TRAILING_STOP_CONFIG.get('min_holding_days', 3)
            
            # 트레일링스탑 활성화 조건 확인
            if atr <= 0 or return_rate < min_rise_pct:
                return {'should_exit': False}
            
            # 보유기간 체크
            if holding_days is None or holding_days < min_holding_days:
                return {'should_exit': False}
            
            # 고점 대비 하락률 계산
            max_price_since_buy = current_price
            if ohlcv_data is not None and not ohlcv_data.empty:
                max_price_since_buy = ohlcv_data['high'].max()
            
            # 변동성 기반 트레일링스탑 배수 계산
            trailing_multiplier = self._calculate_trailing_stop_multiplier(atr_ratio, holding_days)
            
            # 트레일링스탑 비율 계산
            min_trailing_pct = TRAILING_STOP_CONFIG.get('min_trailing_pct', 3.0)
            max_trailing_pct = TRAILING_STOP_CONFIG.get('max_trailing_pct', 10.0)
            
            trailing_stop_pct = min(max((atr / current_price) * 100 * trailing_multiplier, min_trailing_pct), max_trailing_pct)
            drawdown_from_peak = (max_price_since_buy - current_price) / max_price_since_buy * 100
            
            if drawdown_from_peak >= trailing_stop_pct:
                return {
                    'should_exit': True,
                    'reason': f"통합 트레일링스탑 (고점 대비 -{drawdown_from_peak:.1f}%, 기준: -{trailing_stop_pct:.1f}%, 변동성: {atr_ratio:.2%})",
                    'type': "unified_trailing_stop",
                    'priority': 3
                }
            
            return {'should_exit': False}
            
        except Exception as e:
            logging.error(f"❌ {ticker} 통합 트레일링스탑 체크 중 오류: {e}")
            return {'should_exit': False}

    def _calculate_trailing_stop_multiplier(self, atr_ratio, holding_days):
        """
        트레일링스탑 배수 계산
        
        - 변동성 기반 기본 배수
        - 보유기간 기반 점진적 완화
        """
        # 변동성 기반 기본 배수
        if atr_ratio > 0.05:  # 고변동성
            base_multiplier = 1.2
        elif atr_ratio > 0.03:  # 중변동성
            base_multiplier = 1.5
        else:  # 저변동성
            base_multiplier = 2.0
        
        # 보유기간 기반 완화 (암호화폐 특성 반영)
        if holding_days <= 1:  # 1일 이내
            holding_adjustment = 1.5
        elif holding_days <= 3:  # 3일 이내
            holding_adjustment = 1.3
        elif holding_days <= 7:  # 7일 이내
            holding_adjustment = 1.2
        else:  # 7일 초과
            holding_adjustment = 1.1
        
        return base_multiplier * holding_adjustment

    def _calculate_weighted_average_price_from_db(self, ticker: str) -> dict:
        """
        DB의 trade_log에서 가중평균 매수가 계산 (피라미딩 고려)
        
        Args:
            ticker: 티커 심볼 (예: 'KRW-BTC')
            
        Returns:
            dict: {
                'avg_price': float,      # 가중평균 매수가
                'total_quantity': float, # 총 보유 수량
                'total_investment': float, # 총 투자금액
                'buy_count': int,        # 매수 횟수
                'pyramid_count': int     # 피라미딩 횟수
            } 또는 None (데이터 없음)
        """
        try:
            query = """
                SELECT action, qty, price, executed_at
                FROM trade_log 
                WHERE ticker = %s AND action IN ('buy', 'pyramid_buy')
                ORDER BY executed_at ASC
            """
            
            result = self.db_mgr.execute_query(query, (ticker,))
            
            if not result:
                logging.debug(f"📊 {ticker} DB에서 매수 기록 없음")
                return None
                
            total_quantity = 0
            total_investment = 0
            buy_count = 0
            pyramid_count = 0
            
            for row in result:
                action, qty, price, executed_at = row
                
                # None 값 처리
                if qty is None or price is None:
                    logging.warning(f"⚠️ {ticker} DB에서 NULL 값 발견: action={action}, qty={qty}, price={price}")
                    continue
                
                if action == 'buy':
                    buy_count += 1
                elif action == 'pyramid_buy':
                    pyramid_count += 1
                    
                total_quantity += qty
                total_investment += qty * price
            
            if total_quantity <= 0:
                logging.warning(f"⚠️ {ticker} 총 보유 수량이 0 이하: {total_quantity}")
                return None
                
            avg_price = total_investment / total_quantity
            
            logging.debug(f"📊 {ticker} DB 기반 평균 매수가 계산 완료: "
                         f"평균가={avg_price:,.0f}, 수량={total_quantity:.8f}, "
                         f"매수={buy_count}회, 피라미딩={pyramid_count}회")
            
            return {
                'avg_price': avg_price,
                'total_quantity': total_quantity,
                'total_investment': total_investment,
                'buy_count': buy_count,
                'pyramid_count': pyramid_count
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} DB 기반 평균 매수가 계산 실패: {e}")
            return None

    def _validate_pyramiding_consistency(self, ticker: str) -> dict:
        """
        피라미딩 정보의 일관성 검증
        
        Args:
            ticker: 티커 심볼 (예: 'KRW-BTC')
            
        Returns:
            dict: {
                'consistent': bool,           # 일관성 여부
                'db_avg_price': float,        # DB 기반 평균가
                'upbit_avg_price': float,     # 업비트 API 기반 평균가
                'price_diff_pct': float,      # 가격 차이 퍼센트
                'quantity_diff_pct': float,   # 수량 차이 퍼센트
                'pyramid_count': int,         # 피라미딩 횟수
                'recommended_price': float,   # 권장 사용 가격
                'reason': str                 # 일관성 여부 이유
            }
        """
        try:
            # 1. DB 기반 계산
            db_info = self._calculate_weighted_average_price_from_db(ticker)
            
            # 2. 업비트 API 기반 정보
            portfolio_data = self.get_current_positions()
            upbit_avg_price = self._get_avg_price(portfolio_data, ticker.replace('KRW-', ''))
            upbit_quantity = self._get_balance(portfolio_data, ticker.replace('KRW-', ''))
            
            if not db_info:
                return {
                    'consistent': False, 
                    'reason': 'DB에서 매수 기록 없음',
                    'db_avg_price': None,
                    'upbit_avg_price': upbit_avg_price,
                    'price_diff_pct': 0,
                    'quantity_diff_pct': 0,
                    'pyramid_count': 0,
                    'recommended_price': upbit_avg_price
                }
            
            if not upbit_avg_price or not upbit_quantity:
                return {
                    'consistent': False, 
                    'reason': '업비트 API에서 포지션 정보 없음',
                    'db_avg_price': db_info['avg_price'],
                    'upbit_avg_price': None,
                    'price_diff_pct': 0,
                    'quantity_diff_pct': 0,
                    'pyramid_count': db_info['pyramid_count'],
                    'recommended_price': db_info['avg_price']
                }
            
            # 3. 일관성 검증 (1% 오차 허용)
            price_diff_pct = abs(db_info['avg_price'] - upbit_avg_price) / upbit_avg_price * 100
            quantity_diff_pct = abs(db_info['total_quantity'] - upbit_quantity) / upbit_quantity * 100 if upbit_quantity > 0 else 0
            
            is_consistent = price_diff_pct <= 1.0 and quantity_diff_pct <= 1.0
            
            # 4. 권장 가격 결정 (DB 우선, 일관성 있으면 DB 사용)
            recommended_price = db_info['avg_price'] if is_consistent else upbit_avg_price
            
            # 5. 일관성 이유 결정
            if is_consistent:
                reason = "DB와 업비트 API 정보 일치"
            else:
                reasons = []
                if price_diff_pct > 1.0:
                    reasons.append(f"가격차이 {price_diff_pct:.2f}%")
                if quantity_diff_pct > 1.0:
                    reasons.append(f"수량차이 {quantity_diff_pct:.2f}%")
                reason = f"불일치: {', '.join(reasons)}"
            
            logging.info(f"📊 {ticker} 피라미딩 일관성 검증: "
                        f"일관성={'✅' if is_consistent else '⚠️'}, "
                        f"가격차이={price_diff_pct:.2f}%, "
                        f"피라미딩={db_info['pyramid_count']}회")
            
            return {
                'consistent': is_consistent,
                'db_avg_price': db_info['avg_price'],
                'upbit_avg_price': upbit_avg_price,
                'price_diff_pct': price_diff_pct,
                'quantity_diff_pct': quantity_diff_pct,
                'pyramid_count': db_info['pyramid_count'],
                'recommended_price': recommended_price,
                'reason': reason
            }
            
        except Exception as e:
            logging.error(f"❌ {ticker} 피라미딩 일관성 검증 실패: {e}")
            return {
                'consistent': False, 
                'reason': f'검증 실패: {str(e)}',
                'db_avg_price': None,
                'upbit_avg_price': None,
                'price_diff_pct': 0,
                'quantity_diff_pct': 0,
                'pyramid_count': 0,
                'recommended_price': None
            }

    def generate_pyramiding_report(self, ticker: str = None) -> dict:
        """
        피라미딩 통계 리포트 생성
        
        Args:
            ticker: 특정 티커 (None이면 전체)
            
        Returns:
            dict: {
                'total_tickers': int,           # 전체 티커 수
                'pyramiding_tickers': int,      # 피라미딩 티커 수
                'total_pyramiding_trades': int, # 총 피라미딩 거래 수
                'avg_pyramiding_count': float,  # 평균 피라미딩 횟수
                'details': dict                 # 상세 정보
            }
        """
        try:
            if ticker:
                tickers = [ticker]
            else:
                # 현재 보유 중인 모든 티커
                portfolio_data = self.get_current_positions()
                tickers = [pos['ticker'] for pos in portfolio_data if pos['ticker'] != 'KRW']
            
            report = {
                'total_tickers': len(tickers),
                'pyramiding_tickers': 0,
                'total_pyramiding_trades': 0,
                'avg_pyramiding_count': 0,
                'details': {}
            }
            
            for ticker in tickers:
                position_info = self._calculate_weighted_average_price_from_db(ticker)
                if position_info and position_info['pyramid_count'] > 0:
                    report['pyramiding_tickers'] += 1
                    report['total_pyramiding_trades'] += position_info['pyramid_count']
                    
                    report['details'][ticker] = {
                        'avg_price': position_info['avg_price'],
                        'total_quantity': position_info['total_quantity'],
                        'buy_count': position_info['buy_count'],
                        'pyramid_count': position_info['pyramid_count'],
                        'total_investment': position_info['total_investment'],
                        'consistency_check': self._validate_pyramiding_consistency(ticker)
                    }
            
            if report['pyramiding_tickers'] > 0:
                report['avg_pyramiding_count'] = report['total_pyramiding_trades'] / report['pyramiding_tickers']
            
            # 리포트 로깅
            logging.info(f"📊 피라미딩 리포트 생성 완료:")
            logging.info(f"   - 전체 티커: {report['total_tickers']}개")
            logging.info(f"   - 피라미딩 티커: {report['pyramiding_tickers']}개")
            logging.info(f"   - 총 피라미딩 거래: {report['total_pyramiding_trades']}회")
            logging.info(f"   - 평균 피라미딩 횟수: {report['avg_pyramiding_count']:.1f}회")
            
            return report
            
        except Exception as e:
            logging.error(f"❌ 피라미딩 리포트 생성 실패: {e}")
            return {'error': str(e)}
