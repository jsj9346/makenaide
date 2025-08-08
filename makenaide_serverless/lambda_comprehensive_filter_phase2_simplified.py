#!/usr/bin/env python3
"""
⚡ Phase 2: Comprehensive Filtering Lambda (Simplified)
- 기본 Python 패키지만 사용하는 기술적 분석
- pandas/numpy/talib 의존성 제거
- 핵심 필터링 로직 구현
"""

import boto3
import json
import logging
import math
import statistics
import pytz
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SimplifiedTechnicalAnalyzer:
    """간소화된 기술적 분석 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.s3_bucket = 'makenaide-bucket-901361833359'
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 필터링 기준값들
        self.config = {
            'volume_multiplier': 1.5,
            'ma_slope_threshold': 0.5,
            'rsi_threshold_low': 40,
            'rsi_threshold_high': 70,
            'price_from_high_threshold': -25,  # 52주 고점 대비 25% 이내
            'consolidation_threshold': 0.15
        }

    def load_phase1_data(self) -> Optional[List[Dict]]:
        """Phase 1 결과 데이터 로드"""
        try:
            logger.info("Phase 1 결과 데이터 로드 중...")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase1/filtered_tickers_with_data.json'
            )
            
            data = json.loads(response['Body'].read())
            
            if data.get('status') != 'success':
                logger.error(f"Phase 1 데이터 상태 불량: {data.get('status')}")
                return None
            
            tickers_data = data.get('filtered_data', [])
            if not tickers_data:
                logger.warning("Phase 1에서 필터링된 데이터가 없음")
                return None
                
            logger.info(f"Phase 1 데이터 로드 완료: {len(tickers_data)}개 티커")
            return tickers_data
            
        except Exception as e:
            logger.error(f"Phase 1 데이터 로드 실패: {e}")
            return None

    def simple_moving_average(self, prices: List[float], period: int) -> List[float]:
        """단순 이동평균 계산"""
        if len(prices) < period:
            return []
        
        sma = []
        for i in range(period - 1, len(prices)):
            avg = sum(prices[i - period + 1:i + 1]) / period
            sma.append(avg)
        
        return sma

    def simple_rsi(self, prices: List[float], period: int = 14) -> float:
        """간단한 RSI 계산"""
        if len(prices) < period + 1:
            return 50.0  # 기본값
        
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
        
        if len(gains) < period:
            return 50.0
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi

    def calculate_slope(self, values: List[float], periods: int = 5) -> float:
        """기울기 계산 (백분율)"""
        if len(values) < periods:
            return 0.0
        
        recent_values = values[-periods:]
        if recent_values[0] == 0:
            return 0.0
        
        slope = ((recent_values[-1] - recent_values[0]) / recent_values[0]) * 100
        return slope

    def calculate_volatility(self, prices: List[float]) -> float:
        """변동성 계산 (표준편차)"""
        if len(prices) < 2:
            return 0.0
        
        try:
            return statistics.stdev(prices)
        except:
            return 0.0

    def analyze_ticker(self, ticker_data: Dict) -> Dict:
        """개별 티커 기술적 분석"""
        try:
            ticker = ticker_data.get('ticker', '')
            price_data = ticker_data.get('price_data', {})
            
            if not price_data or not price_data.get('close'):
                return {'score': 0, 'pass': False, 'error': 'No price data'}
            
            # 가격 데이터 추출
            close_prices = [float(p) for p in price_data.get('close', [])]
            volumes = [float(v) for v in price_data.get('volume', [])]
            
            if len(close_prices) < 50:  # 최소 50일 데이터 필요
                return {'score': 0, 'pass': False, 'error': 'Insufficient data'}
            
            current_price = close_prices[-1]
            
            # 이동평균 계산
            ma20 = self.simple_moving_average(close_prices, 20)
            ma50 = self.simple_moving_average(close_prices, 50)
            ma200 = self.simple_moving_average(close_prices, 200)
            
            # 기술적 지표 계산
            rsi = self.simple_rsi(close_prices)
            
            # 거래량 분석
            current_volume = volumes[-1] if volumes else 0
            avg_volume_20 = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else current_volume
            volume_ratio = current_volume / avg_volume_20 if avg_volume_20 > 0 else 1.0
            
            # 52주 고점 분석
            lookback_days = min(252, len(close_prices))
            recent_high = max(close_prices[-lookback_days:])
            price_from_high = ((current_price - recent_high) / recent_high) * 100
            
            # 분석 결과
            analysis = {
                'ticker': ticker,
                'current_price': current_price,
                'ma20': ma20[-1] if ma20 else 0,
                'ma50': ma50[-1] if ma50 else 0,
                'ma200': ma200[-1] if ma200 else 0,
                'rsi': rsi,
                'volume_ratio': volume_ratio,
                'price_from_high': price_from_high,
                'recent_high': recent_high
            }
            
            # MA200 기울기
            if ma200 and len(ma200) >= 5:
                ma200_slope = self.calculate_slope(ma200)
                analysis['ma200_slope'] = ma200_slope
            else:
                analysis['ma200_slope'] = 0
            
            return analysis
            
        except Exception as e:
            logger.warning(f"티커 {ticker} 분석 실패: {e}")
            return {'score': 0, 'pass': False, 'error': str(e)}

    def score_weinstein_stage2(self, analysis: Dict) -> Dict:
        """와인스타인 Stage 2 점수화"""
        score = 0
        reasons = []
        
        current_price = analysis.get('current_price', 0)
        ma200 = analysis.get('ma200', 0)
        ma200_slope = analysis.get('ma200_slope', 0)
        volume_ratio = analysis.get('volume_ratio', 0)
        
        # 1. 현재가 > MA200 (25점)
        if current_price > ma200 > 0:
            score += 25
            reasons.append("현재가 > MA200")
        
        # 2. MA200 상승 기울기 (25점)
        if ma200_slope > self.config['ma_slope_threshold']:
            score += 25
            reasons.append(f"MA200 상승 {ma200_slope:.1f}%")
        
        # 3. 거래량 증가 (25점)
        if volume_ratio >= self.config['volume_multiplier']:
            score += 25
            reasons.append(f"거래량 {volume_ratio:.1f}배")
        
        # 4. 추세 지속성 (25점) - 20일선/50일선 정배열
        ma20 = analysis.get('ma20', 0)
        ma50 = analysis.get('ma50', 0)
        if current_price > ma20 > ma50 > 0:
            score += 25
            reasons.append("이동평균 정배열")
        
        return {
            'stage2_score': score,
            'stage2_pass': score >= 75,
            'stage2_reasons': reasons
        }

    def score_minervini_vcp(self, analysis: Dict) -> Dict:
        """미너비니 VCP 점수화"""
        score = 0
        reasons = []
        
        price_from_high = analysis.get('price_from_high', -100)
        rsi = analysis.get('rsi', 50)
        
        # 1. 52주 고점 대비 위치 (40점)
        if price_from_high >= self.config['price_from_high_threshold']:
            if price_from_high >= -10:
                score += 40
                reasons.append(f"고점 대비 {price_from_high:.1f}% (우수)")
            elif price_from_high >= -15:
                score += 30
                reasons.append(f"고점 대비 {price_from_high:.1f}% (양호)")
            else:
                score += 20
                reasons.append(f"고점 대비 {price_from_high:.1f}% (보통)")
        
        # 2. RSI 건전성 (30점)
        if self.config['rsi_threshold_low'] <= rsi <= self.config['rsi_threshold_high']:
            score += 30
            reasons.append(f"RSI {rsi:.1f} 건전")
        elif rsi > self.config['rsi_threshold_high']:
            score += 15
            reasons.append(f"RSI {rsi:.1f} 과열")
        
        # 3. 상대적 강도 (30점) - MA20 > MA50 조건
        ma20 = analysis.get('ma20', 0)
        ma50 = analysis.get('ma50', 0)
        if ma20 > ma50 > 0:
            score += 30
            reasons.append("상대적 강도 양호")
        
        return {
            'vcp_score': score,
            'vcp_pass': score >= 60,
            'vcp_reasons': reasons
        }

    def score_oneill_breakout(self, analysis: Dict) -> Dict:
        """오닐 브레이크아웃 점수화"""
        score = 0
        reasons = []
        
        current_price = analysis.get('current_price', 0)
        ma20 = analysis.get('ma20', 0)
        ma50 = analysis.get('ma50', 0)
        volume_ratio = analysis.get('volume_ratio', 0)
        rsi = analysis.get('rsi', 50)
        
        # 1. 이동평균 배열 (30점)
        if current_price > ma20 > ma50 > 0:
            score += 30
            reasons.append("이동평균 정배열")
        elif current_price > ma20 > 0:
            score += 15
            reasons.append("20일선 상회")
        
        # 2. 거래량 돌파 (35점)
        if volume_ratio >= 2.0:
            score += 35
            reasons.append(f"거래량 {volume_ratio:.1f}배 급증")
        elif volume_ratio >= 1.5:
            score += 20
            reasons.append(f"거래량 {volume_ratio:.1f}배 증가")
        
        # 3. RSI 모멘텀 (25점)
        if 50 <= rsi <= 75:
            score += 25
            reasons.append(f"RSI {rsi:.1f} 상승 모멘텀")
        elif rsi > 75:
            score += 10
            reasons.append(f"RSI {rsi:.1f} 과열")
        
        # 4. 20일선 지지 (10점)
        if ma20 > 0:
            ma20_distance = ((current_price - ma20) / ma20) * 100
            if 0 <= ma20_distance <= 5:
                score += 10
                reasons.append("20일선 근접")
        
        return {
            'breakout_score': score,
            'breakout_pass': score >= 60,
            'breakout_reasons': reasons
        }

    def comprehensive_filter(self, tickers_data: List[Dict]) -> List[Dict]:
        """종합 필터링 실행"""
        filtered_results = []
        
        logger.info(f"종합 필터링 시작: {len(tickers_data)}개 티커")
        
        for i, ticker_data in enumerate(tickers_data):
            try:
                ticker = ticker_data.get('ticker', f'Unknown_{i}')
                logger.info(f"분석 중: {ticker} ({i+1}/{len(tickers_data)})")
                
                # 기술적 분석
                analysis = self.analyze_ticker(ticker_data)
                
                if analysis.get('error'):
                    logger.warning(f"{ticker} 분석 실패: {analysis['error']}")
                    continue
                
                # 3가지 전략 점수화
                weinstein = self.score_weinstein_stage2(analysis)
                vcp = self.score_minervini_vcp(analysis)
                breakout = self.score_oneill_breakout(analysis)
                
                # 종합 점수 계산 (가중평균)
                total_score = (
                    weinstein['stage2_score'] * 0.4 +
                    vcp['vcp_score'] * 0.35 +
                    breakout['breakout_score'] * 0.25
                )
                
                # 통과 조건: 총점 60점 이상 또는 개별 전략 2개 이상 통과
                passes = sum([
                    weinstein['stage2_pass'],
                    vcp['vcp_pass'],
                    breakout['breakout_pass']
                ])
                
                final_pass = total_score >= 60 or passes >= 2
                
                result = {
                    'ticker': ticker,
                    'market': ticker_data.get('market', ''),
                    'total_score': round(total_score, 1),
                    'passes': passes,
                    'final_pass': final_pass,
                    'analysis': analysis,
                    'weinstein': weinstein,
                    'vcp': vcp,
                    'breakout': breakout,
                    'analysis_time': datetime.now(self.kst).isoformat(),
                    'phase': 'comprehensive_filtering'
                }
                
                if final_pass:
                    filtered_results.append(result)
                    logger.info(f"✅ {ticker} 통과 - 점수: {total_score:.1f}, 통과: {passes}/3")
                else:
                    logger.info(f"❌ {ticker} 탈락 - 점수: {total_score:.1f}, 통과: {passes}/3")
                
            except Exception as e:
                logger.error(f"티커 분석 실패: {e}")
                continue
        
        logger.info(f"종합 필터링 완료: {len(filtered_results)}개 티커 통과")
        return filtered_results

    def save_results_to_s3(self, results: List[Dict]) -> bool:
        """결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            output_data = {
                'phase': 'comprehensive_filtering',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'filtered_count': len(results),
                'filtered_tickers': results,
                'config': self.config,
                'version': 'simplified_v1.0'
            }
            
            # 메인 결과 파일
            main_key = 'phase2/comprehensive_filtered_tickers.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            # 백업 파일
            backup_key = f'phase2/backups/comprehensive_filtered_{timestamp}.json'
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
        """Phase 3 트리거 이벤트 발송"""
        try:
            event_detail = {
                'phase': 'comprehensive_filtering',
                'status': 'completed',
                'timestamp': datetime.now(self.kst).isoformat(),
                'next_phase': 'gpt_analysis'
            }
            
            self.events_client.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.comprehensive_filtering',
                        'DetailType': 'Comprehensive Filtering Completed',
                        'Detail': json.dumps(event_detail)
                    }
                ]
            )
            
            logger.info("Phase 3 트리거 이벤트 발송 완료")
            
        except Exception as e:
            logger.error(f"Phase 3 트리거 실패: {e}")

def lambda_handler(event, context):
    """Lambda 핸들러"""
    start_time = datetime.now()
    
    try:
        logger.info("=== Phase 2: Comprehensive Filtering (Simplified) 시작 ===")
        logger.info(f"이벤트: {json.dumps(event)}")
        
        # 분석 엔진 초기화
        analyzer = SimplifiedTechnicalAnalyzer()
        
        # Phase 1 데이터 로드
        phase1_data = analyzer.load_phase1_data()
        if not phase1_data:
            return {
                'statusCode': 400,
                'phase': 'comprehensive_filtering',
                'error': 'Phase 1 데이터 없음',
                'message': 'Phase 1을 먼저 실행해주세요'
            }
        
        # 종합 필터링 실행
        filtered_results = analyzer.comprehensive_filter(phase1_data)
        
        # 결과 저장
        s3_saved = analyzer.save_results_to_s3(filtered_results)
        
        # 다음 단계 트리거
        if filtered_results and s3_saved:
            analyzer.trigger_next_phase()
        
        # 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 성공 응답
        response = {
            'statusCode': 200,
            'phase': 'comprehensive_filtering',
            'input_tickers': len(phase1_data),
            'filtered_tickers': len(filtered_results),
            'execution_time': f"{execution_time:.2f}초",
            's3_saved': s3_saved,
            'top_tickers': [r['ticker'] for r in filtered_results[:10]],
            'timestamp': datetime.now().isoformat(),
            'version': 'simplified_v1.0'
        }
        
        logger.info(f"=== Phase 2 완료 ===")
        logger.info(f"결과: {response}")
        
        return response
        
    except Exception as e:
        logger.error(f"Phase 2 실행 실패: {e}")
        return {
            'statusCode': 500,
            'phase': 'comprehensive_filtering',
            'error': str(e),
            'message': 'Phase 2 실행 중 오류 발생'
        }