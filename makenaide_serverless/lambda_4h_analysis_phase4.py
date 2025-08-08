#!/usr/bin/env python3
"""
📊 Phase 4: 4H Analysis Lambda
- Phase 3 GPT 분석 결과를 받아 4시간봉 단기 추세 분석
- 마켓타이밍 필터링을 통한 최적 매수 시점 포착
- 7가지 지표 기반 점수제 시스템
"""

import boto3
import json
import logging
import math
import statistics
import time
import pytz
import urllib3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class FourHourAnalyzer:
    """4시간봉 분석 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.s3_bucket = 'makenaide-bucket-901361833359'
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 분석 설정
        self.config = {
            'min_score': 5,  # 7개 지표 중 최소 5개 통과
            'rsi_max': 80,   # RSI 과열 임계값
            'adx_min': 25,   # ADX 추세 강도 임계값
            'stoch_min': 20, # 스토캐스틱 K 최소값
            'cci_min': 100,  # CCI 돌파 임계값
            'lookback_periods': 100  # 4시간봉 데이터 조회 기간
        }

    def load_phase3_data(self) -> Optional[List[Dict]]:
        """Phase 3 GPT 분석 결과 데이터 로드"""
        try:
            logger.info("Phase 3 GPT 분석 결과 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase3/gpt_analysis_results.json'
            )
            
            data = json.loads(response['Body'].read())
            
            if data.get('status') != 'success':
                logger.error(f"Phase 3 데이터 상태 불량: {data.get('status')}")
                return None
            
            gpt_results = data.get('gpt_results', [])
            if not gpt_results:
                logger.warning("Phase 3 GPT 분석 결과가 없음")
                return None
            
            # BUY 신호만 필터링
            buy_results = [r for r in gpt_results if r.get('action') == 'BUY']
            
            logger.info(f"Phase 3 데이터 로드 완료: {len(buy_results)}개 BUY 신호")
            return buy_results
            
        except Exception as e:
            logger.error(f"Phase 3 데이터 로드 실패: {e}")
            return None

    def fetch_4h_ohlcv(self, ticker: str) -> Optional[List[Dict]]:
        """4시간봉 OHLCV 데이터 수집 (업비트 API 직접 호출)"""
        try:
            http = urllib3.PoolManager()
            
            # 업비트 API 호출
            url = "https://api.upbit.com/v1/candles/minutes/240"
            params = {
                'market': ticker,
                'count': self.config['lookback_periods']
            }
            
            # URL 파라미터 생성 
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{url}?{param_string}"
            
            response = http.request('GET', full_url)
            
            if response.status != 200:
                logger.warning(f"{ticker} API 호출 실패: {response.status}")
                return None
            
            data = json.loads(response.data.decode('utf-8'))
            
            if not data:
                logger.warning(f"{ticker} 4시간봉 데이터 없음")
                return None
            
            # API 응답을 OHLCV 형태로 변환 (시간순 정렬)
            ohlcv_data = []
            for candle in reversed(data):  # API는 최신순이므로 역순으로 변환
                ohlcv_data.append({
                    'timestamp': candle['candle_date_time_kst'],
                    'open': float(candle['opening_price']),
                    'high': float(candle['high_price']),
                    'low': float(candle['low_price']),
                    'close': float(candle['trade_price']),
                    'volume': float(candle['candle_acc_trade_volume'])
                })
            
            logger.info(f"{ticker} 4시간봉 데이터 수집 완료: {len(ohlcv_data)}개")
            return ohlcv_data
            
        except Exception as e:
            logger.error(f"{ticker} 4시간봉 데이터 수집 실패: {e}")
            return None

    def calculate_technical_indicators(self, ohlcv_data: List[Dict]) -> Dict:
        """기술적 지표 계산 (간소화 버전)"""
        try:
            if len(ohlcv_data) < 50:
                return {}
            
            # 가격 배열 생성
            closes = [d['close'] for d in ohlcv_data]
            highs = [d['high'] for d in ohlcv_data]
            lows = [d['low'] for d in ohlcv_data]
            volumes = [d['volume'] for d in ohlcv_data]
            
            current_price = closes[-1]
            
            # 1. 이동평균 계산
            ma10 = sum(closes[-10:]) / 10 if len(closes) >= 10 else current_price
            ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else current_price  
            ma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else current_price
            ma200 = sum(closes[-200:]) / min(200, len(closes))
            
            # 2. RSI 계산 (간소화)
            rsi = self.calculate_simple_rsi(closes[-15:]) if len(closes) >= 15 else 50
            
            # 3. MACD 계산 (간소화)
            macd_data = self.calculate_simple_macd(closes)
            
            # 4. 스토캐스틱 계산 (간소화)
            stoch_data = self.calculate_simple_stochastic(highs[-14:], lows[-14:], closes[-14:])
            
            # 5. ADX 추정 (간소화)
            adx = self.estimate_adx(highs, lows, closes)
            
            # 6. 볼린저 밴드 계산
            bb_data = self.calculate_bollinger_bands(closes[-20:])
            
            # 7. CCI 추정 (간소화)
            cci = self.estimate_cci(highs[-20:], lows[-20:], closes[-20:])
            
            indicators = {
                'current_price': current_price,
                'ma10': ma10,
                'ma20': ma20,
                'ma50': ma50,
                'ma200': ma200,
                'rsi': rsi,
                'macd': macd_data['macd'],
                'macd_signal': macd_data['signal'],
                'macd_histogram': macd_data['histogram'],
                'stoch_k': stoch_data['k'],
                'stoch_d': stoch_data['d'],
                'adx': adx,
                'bb_upper': bb_data['upper'],
                'bb_middle': bb_data['middle'],
                'bb_lower': bb_data['lower'],
                'cci': cci
            }
            
            return indicators
            
        except Exception as e:
            logger.error(f"기술적 지표 계산 실패: {e}")
            return {}

    def calculate_simple_rsi(self, prices: List[float], period: int = 14) -> float:
        """간소화된 RSI 계산"""
        if len(prices) < 2:
            return 50.0
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if not gains or not losses:
            return 50.0
        
        avg_gain = sum(gains) / len(gains)
        avg_loss = sum(losses) / len(losses)
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi

    def calculate_simple_macd(self, prices: List[float]) -> Dict:
        """간소화된 MACD 계산"""
        if len(prices) < 26:
            return {'macd': 0, 'signal': 0, 'histogram': 0}
        
        # EMA 계산 (간소화)
        ema12 = sum(prices[-12:]) / 12
        ema26 = sum(prices[-26:]) / 26
        
        macd = ema12 - ema26
        signal = macd * 0.9  # 신호선 간소화
        histogram = macd - signal
        
        return {
            'macd': macd,
            'signal': signal,
            'histogram': histogram
        }

    def calculate_simple_stochastic(self, highs: List[float], lows: List[float], closes: List[float]) -> Dict:
        """간소화된 스토캐스틱 계산"""
        if len(highs) < 14 or len(lows) < 14 or len(closes) < 14:
            return {'k': 50, 'd': 50}
        
        highest_high = max(highs)
        lowest_low = min(lows)
        current_close = closes[-1]
        
        if highest_high == lowest_low:
            k = 50
        else:
            k = ((current_close - lowest_low) / (highest_high - lowest_low)) * 100
        
        # %D는 %K의 3일 이동평균 (간소화)
        k_values = [k] * 3  # 간소화
        d = sum(k_values) / len(k_values)
        
        return {'k': k, 'd': d}

    def estimate_adx(self, highs: List[float], lows: List[float], closes: List[float]) -> float:
        """ADX 추정 (간소화)"""
        if len(closes) < 14:
            return 20  # 기본값
        
        # 간소화된 ADX 계산 (실제 복잡한 계산 대신 추세 강도 추정)
        recent_prices = closes[-14:]
        price_changes = [abs(recent_prices[i] - recent_prices[i-1]) for i in range(1, len(recent_prices))]
        avg_change = sum(price_changes) / len(price_changes)
        
        # 변동성을 ADX로 근사
        current_price = closes[-1]
        price_range = max(closes[-14:]) - min(closes[-14:])
        
        if current_price == 0:
            return 20
        
        estimated_adx = min(50, (price_range / current_price) * 1000)  # 간소화된 계산
        
        return max(10, estimated_adx)

    def calculate_bollinger_bands(self, prices: List[float], period: int = 20) -> Dict:
        """볼린저 밴드 계산"""
        if len(prices) < period:
            current_price = prices[-1] if prices else 0
            return {
                'upper': current_price * 1.02,
                'middle': current_price,
                'lower': current_price * 0.98
            }
        
        sma = sum(prices) / len(prices)
        variance = sum((p - sma) ** 2 for p in prices) / len(prices)
        std_dev = math.sqrt(variance)
        
        return {
            'upper': sma + (std_dev * 2),
            'middle': sma,
            'lower': sma - (std_dev * 2)
        }

    def estimate_cci(self, highs: List[float], lows: List[float], closes: List[float]) -> float:
        """CCI 추정 (간소화)"""
        if len(highs) < 14 or len(lows) < 14 or len(closes) < 14:
            return 0
        
        # Typical Price 계산
        typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
        
        # 간소화된 CCI 계산
        sma_tp = sum(typical_prices) / len(typical_prices)
        current_tp = typical_prices[-1]
        
        # Mean Deviation 간소화
        mean_dev = sum(abs(tp - sma_tp) for tp in typical_prices) / len(typical_prices)
        
        if mean_dev == 0:
            return 0
        
        cci = (current_tp - sma_tp) / (0.015 * mean_dev)
        
        return max(-300, min(300, cci))  # CCI 범위 제한

    def apply_timing_filter(self, ticker: str, indicators: Dict, gpt_data: Dict) -> Dict:
        """4시간봉 마켓타이밍 필터 적용"""
        try:
            score = 0
            reasons = []
            
            current_price = indicators.get('current_price', 0)
            
            # 1. MACD Signal 상향 돌파
            macd = indicators.get('macd', 0)
            macd_signal = indicators.get('macd_signal', 0)
            macd_histogram = indicators.get('macd_histogram', 0)
            
            if macd > macd_signal and macd_histogram > 0:
                score += 1
                reasons.append("MACD 상향 돌파")
            
            # 2. Stochastic 상승
            stoch_k = indicators.get('stoch_k', 50)
            stoch_d = indicators.get('stoch_d', 50)
            
            if stoch_k > stoch_d and stoch_k > self.config['stoch_min']:
                score += 1
                reasons.append("스토캐스틱 상승")
            
            # 3. CCI 돌파
            cci = indicators.get('cci', 0)
            
            if cci > self.config['cci_min']:
                score += 1
                reasons.append("CCI 돌파")
            
            # 4. ADX 추세 강도 (간소화 - plus_di/minus_di 없이)
            adx = indicators.get('adx', 20)
            
            if adx > self.config['adx_min']:
                score += 1
                reasons.append("ADX 강한 추세")
            
            # 5. MA200 돌파
            ma200 = indicators.get('ma200', 0)
            
            if current_price > ma200:
                score += 1
                reasons.append("MA200 돌파")
            
            # 6. Supertrend 상승 (간소화 - MA50 기준)
            ma50 = indicators.get('ma50', 0)
            
            if current_price > ma50:
                score += 1
                reasons.append("중기 상승 추세")
            
            # 7. Bollinger Band 상단 돌파
            bb_upper = indicators.get('bb_upper', current_price)
            
            if current_price > bb_upper:
                score += 1
                reasons.append("볼린저 밴드 상단 돌파")
            
            # RSI 과열 방지
            rsi = indicators.get('rsi', 50)
            if rsi >= self.config['rsi_max']:
                score = max(0, score - 2)  # 페널티
                reasons.append("RSI 과열 페널티")
            
            # 최종 판정
            passed = score >= self.config['min_score']
            
            result = {
                'ticker': ticker,
                'score': score,
                'max_score': 7,
                'passed': passed,
                'reasons': reasons,
                'indicators': indicators,
                'gpt_score': gpt_data.get('score', 0),
                'gpt_confidence': gpt_data.get('confidence', 0),
                'analysis_timestamp': datetime.now(self.kst).isoformat()
            }
            
            logger.info(f"{'✅' if passed else '❌'} {ticker} 4H 분석 - 점수: {score}/7")
            
            return result
            
        except Exception as e:
            logger.error(f"{ticker} 타이밍 필터 적용 실패: {e}")
            return None

    def analyze_ticker(self, gpt_data: Dict) -> Optional[Dict]:
        """개별 종목 4시간봉 분석"""
        try:
            ticker = gpt_data.get('ticker', '')
            logger.info(f"4시간봉 분석 시작: {ticker}")
            
            # 4시간봉 데이터 수집
            ohlcv_data = self.fetch_4h_ohlcv(ticker)
            if not ohlcv_data:
                return None
            
            # 기술적 지표 계산
            indicators = self.calculate_technical_indicators(ohlcv_data)
            if not indicators:
                return None
            
            # 타이밍 필터 적용
            result = self.apply_timing_filter(ticker, indicators, gpt_data)
            
            return result
            
        except Exception as e:
            logger.error(f"종목 분석 실패: {e}")
            return None

    def analyze_all_tickers(self, gpt_results: List[Dict]) -> List[Dict]:
        """모든 종목 4시간봉 분석"""
        analysis_results = []
        
        logger.info(f"4시간봉 분석 시작: {len(gpt_results)}개 BUY 종목")
        
        for i, gpt_data in enumerate(gpt_results):
            try:
                # API 호출 간격 조절
                if i > 0:
                    time.sleep(0.5)  # 0.5초 대기
                
                result = self.analyze_ticker(gpt_data)
                if result:
                    analysis_results.append(result)
                    
                logger.info(f"진행 상황: {i+1}/{len(gpt_results)}")
                
            except Exception as e:
                logger.error(f"종목 분석 오류: {e}")
                continue
        
        # 점수순 정렬
        analysis_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        # 통과한 종목만 추출
        passed_results = [r for r in analysis_results if r.get('passed', False)]
        
        logger.info(f"4시간봉 분석 완료: {len(passed_results)}개 종목 통과")
        return analysis_results

    def save_results_to_s3(self, results: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            # 통과한 종목만 추출
            passed_results = [r for r in results if r.get('passed', False)]
            final_candidates = [r['ticker'] for r in passed_results]
            
            output_data = {
                'phase': '4h_analysis',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'analyzed_count': len(results),
                'passed_count': len(passed_results),
                'analysis_results': results,
                'final_candidates': final_candidates,
                'config': self.config,
                'analysis_summary': {
                    'total_analyzed': len(results),
                    'timing_passed': len(passed_results),
                    'avg_score': sum(r.get('score', 0) for r in results) / len(results) if results else 0,
                    'max_score': 7
                }
            }
            
            # 메인 결과 파일
            main_key = 'phase4/4h_analysis_results.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            # 백업 파일
            backup_key = f'phase4/backups/4h_analysis_{timestamp}.json'
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

    def trigger_next_phase(self):
        """Phase 5 트리거 이벤트 발송"""
        try:
            event_detail = {
                'phase': '4h_analysis',
                'status': 'completed',
                'timestamp': datetime.now(self.kst).isoformat(),
                'next_phase': 'condition_check'
            }
            
            self.events_client.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.4h_analysis',
                        'DetailType': '4H Analysis Completed',
                        'Detail': json.dumps(event_detail)
                    }
                ]
            )
            
            logger.info("Phase 5 트리거 이벤트 발송 완료")
            
        except Exception as e:
            logger.error(f"Phase 5 트리거 실패: {e}")

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 4: 4H Analysis 시작 ===")
        logger.info(f"이벤트: {json.dumps(event)}")
        
        # 4시간봉 분석기 초기화
        analyzer = FourHourAnalyzer()
        
        # Phase 3 데이터 로드
        phase3_data = analyzer.load_phase3_data()
        if not phase3_data:
            return {
                'statusCode': 400,
                'phase': '4h_analysis',
                'error': 'Phase 3 데이터 없음',
                'message': 'Phase 3을 먼저 실행해주세요'
            }
        
        # 4시간봉 분석 실행
        analysis_results = analyzer.analyze_all_tickers(phase3_data)
        
        # 결과 저장
        s3_saved = analyzer.save_results_to_s3(analysis_results)
        
        # 다음 단계 트리거
        passed_results = [r for r in analysis_results if r.get('passed', False)]
        if passed_results and s3_saved:
            analyzer.trigger_next_phase()
        
        # 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 성공 응답
        response = {
            'statusCode': 200,
            'phase': '4h_analysis',
            'input_tickers': len(phase3_data),
            'analyzed_tickers': len(analysis_results),
            'timing_passed': len(passed_results),
            'final_candidates': [r['ticker'] for r in passed_results],
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"=== Phase 4 완료 ===")
        logger.info(f"결과: {response}")
        
        return response
        
    except Exception as e:
        logger.error(f"Phase 4 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': '4h_analysis',
            'error': str(e),
            'message': 'Phase 4 실행 중 오류 발생'
        }