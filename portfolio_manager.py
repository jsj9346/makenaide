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
            
            # 리스트가 아닌 경우 처리
            if not isinstance(balances, list):
                logging.error(f"❌ get_current_positions: balances 반환값이 리스트가 아님 (타입: {type(balances)})")
                # 딕셔너리인 경우 리스트로 변환 시도
                if isinstance(balances, dict):
                    if 'data' in balances:
                        balances = balances['data']
                    elif 'result' in balances:
                        balances = balances['result']
                    else:
                        balances = [balances]
                    logging.info("✅ 딕셔너리를 리스트로 변환 완료")
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
                blacklist = load_blacklist()
                if not blacklist:
                    logging.warning("⚠️ 블랙리스트가 비어있습니다.")
            except Exception as e:
                logging.error(f"❌ 블랙리스트 로드 중 오류 발생: {str(e)}")
                blacklist = []

            filtered = []
            for item in balances:
                try:
                    # item이 딕셔너리가 아닌 경우 처리
                    if not isinstance(item, dict):
                        logging.warning(f"⚠️ 예상치 못한 item 형식: {type(item)} - {item}")
                        continue
                    
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
                    logging.error(f"❌ {item.get('currency', 'unknown')} 포지션 필터링 중 오류: {str(e)}")
                    continue
                
            logging.info(f"📊 필터링된 보유 자산: {len(filtered)}개")
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
                    
                    # 조건 B: ATR 기반 변동성 조정 손절 (개선)
                    elif atr > 0:
                        # 🔧 [핵심 개선] 보유기간 및 수익률 제한 추가
                        from config import TRAILING_STOP_CONFIG
                        
                        kelly_config = TRAILING_STOP_CONFIG.get('kelly_stop_loss', {})
                        min_holding_days = kelly_config.get('min_holding_days', 3)
                        profit_threshold_pct = kelly_config.get('profit_threshold_pct', 5.0)
                        
                        # 보유기간 확인 (3일 미만 시 ATR 손절매 비활성화)
                        if holding_days is not None and holding_days < min_holding_days:
                            logging.debug(f"📊 {ticker_krw} ATR 손절매 비활성화: 보유기간 {holding_days}일 < {min_holding_days}일")
                        # 수익률 확인 (5% 미만 수익 시 ATR 손절매 비활성화)
                        elif return_rate < profit_threshold_pct:
                            logging.debug(f"📊 {ticker_krw} ATR 손절매 비활성화: 수익률 {return_rate:.1f}% < {profit_threshold_pct}%")
                        else:
                            # ATR 기반 손절매 로직 (기존)
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
                    
                    # 조건 B: ATR 기반 강화된 트레일링 스탑 (개선)
                    # 🔧 [핵심 개선] 트레일링스탑 활성화 조건 강화
                    from config import TRAILING_STOP_CONFIG
                    
                    config = TRAILING_STOP_CONFIG
                    min_rise_pct = config.get('min_rise_pct', 8.0)  # 3% → 8%로 증가
                    min_holding_days = config.get('min_holding_days', 3)  # 최소 보유기간 3일
                    
                    if not sell_reason and atr > 0 and max_price_since_buy > avg_price * (1 + min_rise_pct/100):
                        # 🔧 [핵심 개선] 보유기간 체크 추가
                        if holding_days is None or holding_days < min_holding_days:
                            if config.get('logging', {}).get('log_deactivation_reasons', True):
                                logging.debug(f"🔧 {ticker_krw} 트레일링스탑 비활성화: 보유기간 {holding_days}일 < 최소 {min_holding_days}일")
                            continue
                        
                        # 🔧 [핵심 개선] 추가 상승 확인 조건
                        # 최근 3일간의 상승 추세 확인
                        recent_trend_days = config.get('recent_trend_check_days', 3)
                        recent_trend = self._check_recent_price_trend(ticker_krw, recent_trend_days)
                        if not recent_trend['is_uptrend']:
                            if config.get('logging', {}).get('log_deactivation_reasons', True):
                                logging.debug(f"🔧 {ticker_krw} 트레일링스탑 비활성화: 최근 {recent_trend_days}일간 상승추세 아님 ({recent_trend['reason']})")
                            continue
                        
                        # 🔧 [핵심 개선] 강한 상승추세 시 트레일링스탑 비활성화
                        if config.get('strong_uptrend_disable', True):
                            strong_uptrend = self._check_strong_uptrend_conditions(
                                ticker_krw, current_price, avg_price, return_rate, 
                                rsi, ma20, macd, macd_signal
                            )
                            if strong_uptrend['is_strong_uptrend']:
                                if config.get('logging', {}).get('log_deactivation_reasons', True):
                                    logging.info(f"🔧 {ticker_krw} 강한 상승추세 감지 - 트레일링스탑 비활성화")
                                continue
                        
                        # 변동성에 따른 동적 트레일링 스탑 (설정 기반)
                        volatility_multipliers = config.get('volatility_multipliers', {
                            'high': 1.5, 'medium': 2.0, 'low': 2.5
                        })
                        
                        if atr_ratio > 0.05:  # 고변동성
                            trailing_multiplier = volatility_multipliers.get('high', 1.5)
                        elif atr_ratio > 0.03:  # 중변동성
                            trailing_multiplier = volatility_multipliers.get('medium', 2.0)
                        else:  # 저변동성
                            trailing_multiplier = volatility_multipliers.get('low', 2.5)
                        
                        # 🔧 [핵심 개선] 보유기간 기반 추가 완화
                        holding_adjustments = config.get('holding_adjustments', {3: 2.0, 7: 1.5, 14: 1.2})
                        holding_adjustment = 1.0
                        
                        if holding_days is not None:
                            for days, adjustment in sorted(holding_adjustments.items()):
                                if holding_days <= days:
                                    holding_adjustment = adjustment
                                    if config.get('logging', {}).get('log_activation_conditions', True):
                                        logging.info(f"🔧 {ticker_krw} 보유 {holding_days}일 이내 - 트레일링스탑 {((adjustment-1)*100):.0f}% 완화")
                                    break
                        
                        # 트레일링스탑 비율에 보유기간 조정 적용
                        trailing_multiplier *= holding_adjustment
                        
                        # 🔧 [핵심 개선] 최소/최대 트레일링스탑 비율 조정
                        min_trailing_pct = config.get('min_trailing_pct', 3.0)  # 1.5% → 3.0%로 증가
                        max_trailing_pct = config.get('max_trailing_pct', 10.0)  # 8% → 10%로 증가
                        
                        trailing_stop_pct = min(max((atr / current_price) * 100 * trailing_multiplier, min_trailing_pct), max_trailing_pct)
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
            
            # 리스트가 아닌 경우 처리
            if not isinstance(balances, list):
                logging.error(f"❌ _get_actual_holdings: balances 반환값이 리스트가 아님 (타입: {type(balances)})")
                # 딕셔너리인 경우 리스트로 변환 시도
                if isinstance(balances, dict):
                    if 'data' in balances:
                        balances = balances['data']
                    elif 'result' in balances:
                        balances = balances['result']
                    else:
                        balances = [balances]
                    logging.info("✅ 딕셔너리를 리스트로 변환 완료")
                else:
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
