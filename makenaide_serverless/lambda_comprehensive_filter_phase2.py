#!/usr/bin/env python3
"""
⚡ Phase 2: Comprehensive Filtering Lambda
- 와인스타인/미너비니/오닐의 기술적 분석 필터링
- Stage 2 돌파 패턴 및 VCP 분석
- Phase 1 결과를 입력으로 받아 정밀 필터링
"""

import boto3
import json
import logging
import pandas as pd
import numpy as np
import pytz
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import yfinance as yf
import talib

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ComprehensiveFilterPhase2:
    """종합 기술적 분석 필터링 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.s3_bucket = 'makenaide-serverless-data'
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 필터링 기준값들 (환경변수에서 설정 가능)
        self.config = {
            'volume_multiplier': float(os.environ.get('VOLUME_MULTIPLIER', '1.5')),
            'ma_slope_threshold': float(os.environ.get('MA_SLOPE_THRESHOLD', '0.5')),
            'adx_threshold': int(os.environ.get('ADX_THRESHOLD', '20')),
            'rsi_lower': int(os.environ.get('RSI_LOWER', '40')),
            'rsi_upper': int(os.environ.get('RSI_UPPER', '70')),
            'lookback_days': int(os.environ.get('LOOKBACK_DAYS', '252')),
            'consolidation_threshold': float(os.environ.get('CONSOLIDATION_THRESHOLD', '0.25'))
        }

    def load_phase1_data(self) -> Optional[pd.DataFrame]:
        """Phase 1 결과 데이터 로드"""
        try:
            logger.info("Phase 1 결과 데이터 로드 중...")
            
            # S3에서 Phase 1 결과 파일 다운로드
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key='phase1/filtered_tickers_with_data.json'
            )
            
            data = json.loads(response['Body'].read())
            
            if data.get('status') != 'success':
                logger.error(f"Phase 1 데이터 상태 불량: {data.get('status')}")
                return None
            
            # DataFrame으로 변환
            tickers_data = data.get('filtered_data', [])
            if not tickers_data:
                logger.warning("Phase 1에서 필터링된 데이터가 없음")
                return None
                
            df = pd.DataFrame(tickers_data)
            logger.info(f"Phase 1 데이터 로드 완료: {len(df)}개 티커")
            
            return df
            
        except Exception as e:
            logger.error(f"Phase 1 데이터 로드 실패: {e}")
            return None

    def calculate_technical_indicators(self, ticker_data: Dict) -> Dict:
        """기술적 지표 계산"""
        try:
            # 가격 데이터 추출
            prices = ticker_data.get('price_data', {})
            if not prices:
                return {}
            
            # 가격 배열 생성
            close_prices = np.array([float(p) for p in prices.get('close', [])])
            high_prices = np.array([float(p) for p in prices.get('high', [])])
            low_prices = np.array([float(p) for p in prices.get('low', [])])
            volumes = np.array([float(v) for v in prices.get('volume', [])])
            
            if len(close_prices) < 200:  # 최소 200일 데이터 필요
                return {}
            
            # 이동평균 계산
            ma20 = talib.SMA(close_prices, timeperiod=20)
            ma50 = talib.SMA(close_prices, timeperiod=50)
            ma200 = talib.SMA(close_prices, timeperiod=200)
            
            # 기술적 지표 계산
            rsi = talib.RSI(close_prices, timeperiod=14)
            adx = talib.ADX(high_prices, low_prices, close_prices, timeperiod=14)
            atr = talib.ATR(high_prices, low_prices, close_prices, timeperiod=14)
            
            # 볼륨 지표
            volume_sma = talib.SMA(volumes, timeperiod=20)
            
            current_price = close_prices[-1]
            current_volume = volumes[-1]
            avg_volume = volume_sma[-1] if not np.isnan(volume_sma[-1]) else 0
            
            # MA200 기울기 계산 (최근 5일)
            ma200_slope = 0
            if len(ma200) >= 5 and not np.isnan(ma200[-5]):
                ma200_slope = ((ma200[-1] - ma200[-5]) / ma200[-5]) * 100
            
            return {
                'current_price': current_price,
                'ma20': ma20[-1] if not np.isnan(ma20[-1]) else 0,
                'ma50': ma50[-1] if not np.isnan(ma50[-1]) else 0,
                'ma200': ma200[-1] if not np.isnan(ma200[-1]) else 0,
                'ma200_slope': ma200_slope,
                'rsi': rsi[-1] if not np.isnan(rsi[-1]) else 50,
                'adx': adx[-1] if not np.isnan(adx[-1]) else 0,
                'atr': atr[-1] if not np.isnan(atr[-1]) else 0,
                'current_volume': current_volume,
                'avg_volume': avg_volume,
                'volume_ratio': current_volume / avg_volume if avg_volume > 0 else 0
            }
            
        except Exception as e:
            logger.warning(f"기술적 지표 계산 실패: {e}")
            return {}

    def check_weinstein_stage2(self, indicators: Dict) -> Dict:
        """와인스타인 Stage 2 돌파 조건 검사"""
        try:
            score = 0
            reasons = []
            
            current_price = indicators.get('current_price', 0)
            ma200 = indicators.get('ma200', 0)
            ma200_slope = indicators.get('ma200_slope', 0)
            volume_ratio = indicators.get('volume_ratio', 0)
            adx = indicators.get('adx', 0)
            
            # 1. 현재가 > MA200 (25점)
            if current_price > ma200 > 0:
                score += 25
                reasons.append("현재가 > MA200")
            
            # 2. MA200 상승 기울기 (25점)
            if ma200_slope > self.config['ma_slope_threshold']:
                score += 25
                reasons.append(f"MA200 상승기울기 {ma200_slope:.2f}%")
            
            # 3. 거래량 증가 (25점)
            if volume_ratio >= self.config['volume_multiplier']:
                score += 25
                reasons.append(f"거래량 {volume_ratio:.1f}배 증가")
            
            # 4. ADX 추세 강도 (25점)
            if adx >= self.config['adx_threshold']:
                score += 25  
                reasons.append(f"ADX {adx:.1f} 강한 추세")
            
            return {
                'stage2_score': score,
                'stage2_pass': score >= 75,  # 4개 중 3개 이상
                'reasons': reasons
            }
            
        except Exception as e:
            logger.warning(f"와인스타인 Stage 2 분석 실패: {e}")
            return {'stage2_score': 0, 'stage2_pass': False, 'reasons': []}

    def check_minervini_vcp(self, ticker_data: Dict, indicators: Dict) -> Dict:
        """미너비니 VCP (Volatility Contraction Pattern) 분석"""
        try:
            score = 0
            reasons = []
            
            prices = ticker_data.get('price_data', {})
            if not prices:
                return {'vcp_score': 0, 'vcp_pass': False, 'reasons': []}
            
            close_prices = np.array([float(p) for p in prices.get('close', [])])
            high_prices = np.array([float(p) for p in prices.get('high', [])])
            
            if len(close_prices) < 60:  # 최소 60일 데이터 필요
                return {'vcp_score': 0, 'vcp_pass': False, 'reasons': []}
            
            current_price = indicators.get('current_price', 0)
            rsi = indicators.get('rsi', 50)
            
            # 최근 52주 고점 계산
            lookback = min(252, len(high_prices))
            recent_high = np.max(high_prices[-lookback:])
            
            # 1. 52주 고점 대비 위치 (30점)
            price_from_high = ((current_price - recent_high) / recent_high) * 100
            if price_from_high >= -25:  # 고점 대비 25% 이내
                score += 30
                reasons.append(f"52주 고점 대비 {price_from_high:.1f}%")
            
            # 2. RSI 건전성 (30점)
            if self.config['rsi_lower'] <= rsi <= self.config['rsi_upper']:
                score += 30
                reasons.append(f"RSI {rsi:.1f} 건전")
            
            # 3. 변동성 수축 패턴 분석 (40점)
            if len(close_prices) >= 40:
                # 최근 20일 vs 이전 20일 변동성 비교
                recent_volatility = np.std(close_prices[-20:])
                previous_volatility = np.std(close_prices[-40:-20])
                
                if recent_volatility < previous_volatility:
                    score += 20
                    reasons.append("변동성 수축 확인")
                
                # 가격 수렴 패턴 (20점)
                recent_range = (np.max(close_prices[-20:]) - np.min(close_prices[-20:])) / np.mean(close_prices[-20:])
                if recent_range < 0.15:  # 15% 이내 수렴
                    score += 20
                    reasons.append("가격 수렴 패턴")
            
            return {
                'vcp_score': score,
                'vcp_pass': score >= 60,  # 100점 중 60점 이상
                'reasons': reasons,
                'price_from_high': price_from_high
            }
            
        except Exception as e:
            logger.warning(f"미너비니 VCP 분석 실패: {e}")
            return {'vcp_score': 0, 'vcp_pass': False, 'reasons': []}

    def check_oneill_breakout(self, indicators: Dict) -> Dict:
        """오닐 차트 패턴 및 브레이크아웃 분석"""
        try:
            score = 0
            reasons = []
            
            current_price = indicators.get('current_price', 0)
            ma20 = indicators.get('ma20', 0)
            ma50 = indicators.get('ma50', 0)
            volume_ratio = indicators.get('volume_ratio', 0)
            rsi = indicators.get('rsi', 50)
            
            # 1. 이동평균 배열 (30점)
            if current_price > ma20 > ma50:
                score += 30
                reasons.append("이동평균 정배열")
            
            # 2. 거래량 돌파 (30점)
            if volume_ratio >= 2.0:  # 2배 이상 급증
                score += 30
                reasons.append(f"거래량 {volume_ratio:.1f}배 급증")
            elif volume_ratio >= 1.5:
                score += 15
                reasons.append(f"거래량 {volume_ratio:.1f}배 증가")
            
            # 3. RSI 모멘텀 (25점)
            if 50 <= rsi <= 75:  # 건전한 상승 모멘텀
                score += 25
                reasons.append(f"RSI {rsi:.1f} 상승 모멘텀")
            
            # 4. 20일선 지지 (15점)
            ma20_support = ((current_price - ma20) / ma20) * 100
            if 0 <= ma20_support <= 5:  # 20일선 근처에서 지지
                score += 15
                reasons.append("20일선 지지 확인")
            
            return {
                'breakout_score': score,
                'breakout_pass': score >= 60,  # 100점 중 60점 이상
                'reasons': reasons
            }
            
        except Exception as e:
            logger.warning(f"오닐 브레이크아웃 분석 실패: {e}")
            return {'breakout_score': 0, 'breakout_pass': False, 'reasons': []}

    def comprehensive_filter(self, df: pd.DataFrame) -> List[Dict]:
        """종합 필터링 실행"""
        filtered_results = []
        
        logger.info(f"종합 필터링 시작: {len(df)}개 티커")
        
        for idx, row in df.iterrows():
            try:
                ticker = row.get('ticker', '')
                if not ticker:
                    continue
                
                logger.info(f"분석 중: {ticker} ({idx+1}/{len(df)})")
                
                # 기술적 지표 계산
                indicators = self.calculate_technical_indicators(row.to_dict())
                if not indicators:
                    continue
                
                # 3가지 전략 분석
                weinstein_result = self.check_weinstein_stage2(indicators)
                vcp_result = self.check_minervini_vcp(row.to_dict(), indicators)
                breakout_result = self.check_oneill_breakout(indicators)
                
                # 종합 점수 계산
                total_score = (
                    weinstein_result.get('stage2_score', 0) * 0.4 +  # 40% 가중치
                    vcp_result.get('vcp_score', 0) * 0.35 +         # 35% 가중치  
                    breakout_result.get('breakout_score', 0) * 0.25  # 25% 가중치
                )
                
                # 통과 조건: 총점 60점 이상 또는 개별 전략 2개 이상 통과
                passes = sum([
                    weinstein_result.get('stage2_pass', False),
                    vcp_result.get('vcp_pass', False),
                    breakout_result.get('breakout_pass', False)
                ])
                
                final_pass = total_score >= 60 or passes >= 2
                
                if final_pass:
                    filtered_results.append({
                        'ticker': ticker,
                        'market': row.get('market', ''),
                        'total_score': round(total_score, 1),
                        'passes': passes,
                        'indicators': indicators,
                        'weinstein': weinstein_result,
                        'vcp': vcp_result,
                        'breakout': breakout_result,
                        'analysis_time': datetime.now(self.kst).isoformat(),
                        'phase': 'comprehensive_filtering'
                    })
                    
                    logger.info(f"✅ {ticker} 통과 - 점수: {total_score:.1f}, 통과: {passes}/3")
                else:
                    logger.info(f"❌ {ticker} 탈락 - 점수: {total_score:.1f}, 통과: {passes}/3")
                
            except Exception as e:
                logger.error(f"티커 {ticker} 분석 실패: {e}")
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
                'config': self.config
            }
            
            # 메인 결과 파일
            main_key = 'phase2/comprehensive_filtered_tickers.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(output_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            # 백업 파일 (타임스탬프 포함)
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
        logger.info("=== Phase 2: Comprehensive Filtering 시작 ===")
        logger.info(f"이벤트: {json.dumps(event)}")
        
        # Phase 2 필터링 실행
        filter_engine = ComprehensiveFilterPhase2()
        
        # Phase 1 데이터 로드
        phase1_data = filter_engine.load_phase1_data()
        if phase1_data is None or len(phase1_data) == 0:
            return {
                'statusCode': 400,
                'phase': 'comprehensive_filtering', 
                'error': 'Phase 1 데이터 없음',
                'message': 'Phase 1을 먼저 실행해주세요'
            }
        
        # 종합 필터링 실행
        filtered_results = filter_engine.comprehensive_filter(phase1_data)
        
        # 결과 저장
        s3_saved = filter_engine.save_results_to_s3(filtered_results)
        
        # 다음 단계 트리거 (결과가 있을 때만)
        if filtered_results and s3_saved:
            filter_engine.trigger_next_phase()
        
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
            'top_tickers': [r['ticker'] for r in filtered_results[:10]],  # 상위 10개
            'timestamp': datetime.now().isoformat()
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