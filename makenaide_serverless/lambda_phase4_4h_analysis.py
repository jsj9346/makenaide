#!/usr/bin/env python3
"""
⚡ Phase 4: 4시간봉 기술적 분석 Lambda
- GPT 분석 결과를 받아 4시간봉 차트로 진입 타이밍 분석
- RSI, 볼린저밴드, MACD, 스토캐스틱 등 단기 지표 분석
- 장중 변동성 및 실시간 모멘텀 검증
- Phase 5로 최종 매매 신호 전달
"""

import boto3
import json
import logging
import pandas as pd
import numpy as np
import pytz
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import pyupbit
import warnings
warnings.filterwarnings('ignore')

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class FourHourAnalysisPhase4:
    """4시간봉 기술적 분석 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.events_client = boto3.client('events')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-serverless-data')
        self.kst = pytz.timezone('Asia/Seoul')
        
        # 4시간봉 분석 설정
        self.analysis_config = {
            'lookback_periods': int(os.environ.get('LOOKBACK_4H', '168')),  # 168 * 4시간 = 28일
            'rsi_period': 14,
            'bb_period': 20,
            'bb_std': 2.0,
            'volume_sma': 24  # 24 * 4시간 = 4일
        }
        
        # 진입 신호 임계값
        self.signal_thresholds = {
            'rsi_oversold': 30,
            'rsi_overbought': 70,
            'bb_squeeze_threshold': 0.02,  # 2% 이하 대역폭
            'volume_surge_ratio': 2.0,
            'volatility_threshold': 0.05  # 5%
        }

    def load_phase3_recommendations(self) -> Optional[List[Dict[str, Any]]]:
        """Phase 3 GPT 분석에서 BUY 추천 종목들 로드"""
        try:
            logger.info("📊 Phase 3 GPT 분석 결과 로드 중...")
            
            # 최신 Phase 3 결과 파일 찾기
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix='phase3/gpt_analysis_results_'
            )
            
            if 'Contents' not in response or not response['Contents']:
                logger.warning("Phase 3 결과 파일이 없습니다")
                return None
            
            # 가장 최신 파일 선택
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=latest_file['Key']
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            analysis_results = data.get('analysis_results', [])
            
            # BUY 추천 종목만 필터링
            buy_recommendations = [
                result for result in analysis_results
                if result.get('gpt_analysis', {}).get('overall_rating') == 'BUY'
            ]
            
            if not buy_recommendations:
                logger.warning("Phase 3에서 BUY 추천 종목이 없습니다")
                return None
                
            logger.info(f"✅ Phase 3 BUY 추천 로드 완료: {len(buy_recommendations)}개 종목")
            return buy_recommendations
            
        except Exception as e:
            logger.error(f"❌ Phase 3 데이터 로드 실패: {e}")
            return None

    def get_4h_ohlcv_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """4시간봉 OHLCV 데이터 조회"""
        try:
            logger.info(f"📈 {ticker} 4시간봉 데이터 조회 중...")
            
            # 업비트에서 4시간봉 데이터 조회
            df = pyupbit.get_ohlcv(ticker, interval="minute240", count=self.analysis_config['lookback_periods'])
            
            if df is None or df.empty:
                logger.warning(f"{ticker} 4시간봉 데이터 조회 실패")
                return None
            
            # 데이터 정리
            df.index.name = 'datetime'
            df.reset_index(inplace=True)
            df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            
            logger.info(f"✅ {ticker} 4시간봉 데이터 조회 완료: {len(df)}개 캔들")
            return df
            
        except Exception as e:
            logger.error(f"❌ {ticker} 4시간봉 데이터 조회 실패: {e}")
            return None

    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """RSI 계산"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std: float = 2.0):
        """볼린저밴드 계산"""
        ma = prices.rolling(window=period).mean()
        mstd = prices.rolling(window=period).std()
        upper = ma + (mstd * std)
        lower = ma - (mstd * std)
        return upper, ma, lower

    def calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
        """MACD 계산"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal).mean()
        macd_histogram = macd - macd_signal
        return macd, macd_signal, macd_histogram

    def analyze_entry_timing(self, ticker: str, df: pd.DataFrame) -> Dict[str, Any]:
        """진입 타이밍 분석"""
        try:
            analysis = {
                'ticker': ticker,
                'analysis_time': datetime.now(self.kst).isoformat(),
                'signals': {},
                'scores': {},
                'risk_factors': [],
                'entry_conditions': {}
            }
            
            current_price = df['close'].iloc[-1]
            current_volume = df['volume'].iloc[-1]
            
            # 1. RSI 분석
            rsi = self.calculate_rsi(df['close'])
            current_rsi = rsi.iloc[-1]
            
            if not np.isnan(current_rsi):
                analysis['signals']['rsi'] = {
                    'value': current_rsi,
                    'condition': 'oversold' if current_rsi < self.signal_thresholds['rsi_oversold'] 
                                else 'overbought' if current_rsi > self.signal_thresholds['rsi_overbought'] 
                                else 'neutral',
                    'trend': 'bullish' if current_rsi > rsi.iloc[-2] else 'bearish'
                }
                
                # RSI 점수 (40-65 구간이 건전)
                if 40 <= current_rsi <= 65:
                    analysis['scores']['rsi'] = 80
                elif 30 <= current_rsi <= 75:
                    analysis['scores']['rsi'] = 60
                else:
                    analysis['scores']['rsi'] = 30
            
            # 2. 볼린저밴드 분석
            bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(df['close'])
            current_upper = bb_upper.iloc[-1]
            current_lower = bb_lower.iloc[-1]
            current_middle = bb_middle.iloc[-1]
            
            if not any(np.isnan([current_upper, current_lower, current_middle])):
                bb_width = (current_upper - current_lower) / current_middle
                bb_position = (current_price - current_lower) / (current_upper - current_lower)
                
                analysis['signals']['bollinger'] = {
                    'position': bb_position,
                    'width': bb_width,
                    'squeeze': bb_width < self.signal_thresholds['bb_squeeze_threshold'],
                    'breakout_ready': bb_width < 0.03 and bb_position > 0.8
                }
                
                # 볼린저밴드 점수
                if 0.3 <= bb_position <= 0.8:
                    analysis['scores']['bollinger'] = 70 + (30 if analysis['signals']['bollinger']['squeeze'] else 0)
                else:
                    analysis['scores']['bollinger'] = 40
            
            # 3. MACD 분석
            macd, macd_signal, macd_hist = self.calculate_macd(df['close'])
            current_macd = macd.iloc[-1]
            current_signal = macd_signal.iloc[-1]
            current_hist = macd_hist.iloc[-1]
            
            if not any(np.isnan([current_macd, current_signal, current_hist])):
                macd_bullish = current_macd > current_signal
                macd_momentum = current_hist - macd_hist.iloc[-2]
                
                analysis['signals']['macd'] = {
                    'line': current_macd,
                    'signal': current_signal,
                    'histogram': current_hist,
                    'bullish': macd_bullish,
                    'momentum': macd_momentum,
                    'crossover': (current_macd > current_signal) and (macd.iloc[-2] <= macd_signal.iloc[-2])
                }
                
                # MACD 점수
                score = 50
                if macd_bullish:
                    score += 20
                if macd_momentum > 0:
                    score += 15
                if analysis['signals']['macd']['crossover']:
                    score += 15
                    
                analysis['scores']['macd'] = min(score, 100)
            
            # 4. 거래량 분석
            volume_sma = df['volume'].rolling(self.analysis_config['volume_sma']).mean().iloc[-1]
            
            if not np.isnan(volume_sma) and volume_sma > 0:
                volume_ratio = current_volume / volume_sma
                
                analysis['signals']['volume'] = {
                    'current': current_volume,
                    'average': volume_sma,
                    'ratio': volume_ratio,
                    'surge': volume_ratio > self.signal_thresholds['volume_surge_ratio']
                }
                
                # 거래량 점수
                if volume_ratio > 2.0:
                    analysis['scores']['volume'] = 90
                elif volume_ratio > 1.5:
                    analysis['scores']['volume'] = 75
                elif volume_ratio > 1.0:
                    analysis['scores']['volume'] = 60
                else:
                    analysis['scores']['volume'] = 30
            
            # 5. 이동평균선 배열 분석
            sma_12 = df['close'].rolling(12).mean().iloc[-1]  # 2일
            sma_24 = df['close'].rolling(24).mean().iloc[-1]  # 4일
            sma_72 = df['close'].rolling(72).mean().iloc[-1]  # 12일
            
            if not any(np.isnan([sma_12, sma_24, sma_72])):
                ma_bullish = current_price > sma_12 > sma_24 > sma_72
                
                analysis['signals']['moving_averages'] = {
                    'sma_12': sma_12,
                    'sma_24': sma_24,
                    'sma_72': sma_72,
                    'arrangement': 'bullish' if ma_bullish else 'bearish',
                    'price_vs_sma12': ((current_price - sma_12) / sma_12) * 100
                }
                
                # 이동평균 점수
                if ma_bullish:
                    analysis['scores']['moving_averages'] = 85
                elif current_price < sma_12 < sma_24 < sma_72:
                    analysis['scores']['moving_averages'] = 20
                else:
                    analysis['scores']['moving_averages'] = 50
            
            # 6. 변동성 분석 (ATR 대체)
            high_low_pct = ((df['high'] - df['low']) / df['close']).rolling(14).mean().iloc[-1]
            
            if not np.isnan(high_low_pct):
                analysis['signals']['volatility'] = {
                    'ratio': high_low_pct,
                    'level': 'high' if high_low_pct > 0.05 else 'normal' if high_low_pct > 0.02 else 'low'
                }
                
                # 적당한 변동성 선호 (2-5%)
                if 0.02 <= high_low_pct <= 0.05:
                    analysis['scores']['volatility'] = 80
                elif high_low_pct > 0.08:
                    analysis['scores']['volatility'] = 30
                    analysis['risk_factors'].append("높은 변동성")
                else:
                    analysis['scores']['volatility'] = 60
            
            # 7. 종합 점수 및 진입 조건 계산
            all_scores = [score for score in analysis['scores'].values() if score > 0]
            if all_scores:
                analysis['overall_score'] = np.mean(all_scores)
                
                # 진입 조건 체크
                analysis['entry_conditions'] = {
                    'rsi_healthy': 30 < current_rsi < 75,
                    'macd_bullish': analysis['signals'].get('macd', {}).get('bullish', False),
                    'volume_confirmation': analysis['signals'].get('volume', {}).get('ratio', 0) > 1.2,
                    'ma_support': current_price > sma_12,
                    'not_overbought': current_rsi < 75,
                    'momentum_positive': analysis['signals'].get('macd', {}).get('momentum', 0) > -0.1
                }
                
                passed_conditions = sum(analysis['entry_conditions'].values())
                analysis['conditions_passed'] = passed_conditions
                analysis['entry_ready'] = passed_conditions >= 4  # 6개 중 4개 이상
                
            else:
                analysis['overall_score'] = 0
                analysis['entry_ready'] = False
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ {ticker} 진입 타이밍 분석 실패: {e}")
            return {'ticker': ticker, 'error': str(e), 'entry_ready': False}

    def process_buy_recommendations(self, recommendations: List[Dict]) -> List[Dict[str, Any]]:
        """BUY 추천 종목들에 대해 4시간봉 분석 실행"""
        try:
            logger.info(f"🔍 4시간봉 분석 시작: {len(recommendations)}개 BUY 추천 종목")
            analysis_results = []
            
            for idx, recommendation in enumerate(recommendations):
                ticker = recommendation.get('ticker')
                if not ticker:
                    continue
                
                try:
                    logger.info(f"📊 {ticker} 4시간봉 분석 중... ({idx+1}/{len(recommendations)})")
                    
                    # 1. 4시간봉 데이터 조회
                    df_4h = self.get_4h_ohlcv_data(ticker)
                    if df_4h is None or len(df_4h) < 50:
                        logger.warning(f"{ticker} 4시간봉 데이터 부족, 건너뛰기")
                        continue
                    
                    # 2. 진입 타이밍 분석
                    timing_analysis = self.analyze_entry_timing(ticker, df_4h)
                    
                    # 3. Phase 3 GPT 분석과 결합
                    combined_result = {
                        'ticker': ticker,
                        'phase3_gpt_analysis': recommendation.get('gpt_analysis', {}),
                        'phase4_timing_analysis': timing_analysis,
                        'combined_assessment': {
                            'gpt_rating': recommendation.get('gpt_analysis', {}).get('overall_rating', 'HOLD'),
                            'gpt_confidence': recommendation.get('gpt_analysis', {}).get('confidence_score', 0),
                            'timing_score': timing_analysis.get('overall_score', 0),
                            'entry_ready': timing_analysis.get('entry_ready', False),
                            'conditions_passed': timing_analysis.get('conditions_passed', 0)
                        },
                        'analysis_timestamp': datetime.now(self.kst).isoformat(),
                        'processing_order': idx + 1
                    }
                    
                    # 최종 추천 여부 결정
                    combined_result['final_recommendation'] = self._make_final_decision(combined_result)
                    
                    analysis_results.append(combined_result)
                    
                    entry_status = "진입 준비" if timing_analysis.get('entry_ready', False) else "대기"
                    logger.info(f"✅ {ticker} 4시간봉 분석 완료: {timing_analysis.get('overall_score', 0):.1f}점, {entry_status}")
                
                except Exception as e:
                    logger.error(f"❌ {ticker} 개별 4시간봉 분석 실패: {e}")
                    continue
            
            logger.info(f"🎯 4시간봉 분석 완료: {len(analysis_results)}개 분석 결과")
            return analysis_results
            
        except Exception as e:
            logger.error(f"❌ BUY 추천 종목 처리 실패: {e}")
            return []

    def _make_final_decision(self, combined_result: Dict) -> Dict[str, Any]:
        """GPT 분석 + 4시간봉 분석 결합하여 최종 결정"""
        try:
            gpt_confidence = combined_result['combined_assessment']['gpt_confidence']
            timing_score = combined_result['combined_assessment']['timing_score']
            entry_ready = combined_result['combined_assessment']['entry_ready']
            conditions_passed = combined_result['combined_assessment']['conditions_passed']
            
            # 가중 점수 계산 (GPT 60%, 4시간봉 40%)
            weighted_score = (gpt_confidence * 0.6) + (timing_score * 0.4)
            
            # 최종 결정 로직
            if entry_ready and weighted_score > 70 and conditions_passed >= 4:
                recommendation = "STRONG_BUY"
                priority = "high"
            elif entry_ready and weighted_score > 60:
                recommendation = "BUY"
                priority = "medium"
            elif weighted_score > 50 and conditions_passed >= 3:
                recommendation = "HOLD_WATCH"
                priority = "low"
            else:
                recommendation = "SKIP"
                priority = "skip"
            
            return {
                'final_rating': recommendation,
                'priority': priority,
                'weighted_score': weighted_score,
                'reasoning': f"GPT신뢰도 {gpt_confidence}% + 4H타이밍 {timing_score:.1f}% = 종합 {weighted_score:.1f}%",
                'entry_timing': "즉시" if recommendation == "STRONG_BUY" else "조건 충족시" if recommendation == "BUY" else "대기"
            }
            
        except Exception as e:
            logger.error(f"❌ 최종 결정 실패: {e}")
            return {
                'final_rating': 'SKIP',
                'priority': 'skip',
                'weighted_score': 0,
                'reasoning': f"결정 로직 오류: {str(e)}",
                'entry_timing': "대기"
            }

    def save_results_to_s3(self, results: List[Dict[str, Any]]) -> bool:
        """4시간봉 분석 결과를 S3에 저장"""
        try:
            timestamp = datetime.now(self.kst).strftime('%Y%m%d_%H%M%S')
            
            # 결과 통계 계산
            total_analyzed = len(results)
            strong_buy = len([r for r in results if r['final_recommendation']['final_rating'] == 'STRONG_BUY'])
            buy = len([r for r in results if r['final_recommendation']['final_rating'] == 'BUY'])
            watch = len([r for r in results if r['final_recommendation']['final_rating'] == 'HOLD_WATCH'])
            
            output_data = {
                'phase': '4h_analysis',
                'status': 'success',
                'timestamp': timestamp,
                'analysis_config': self.analysis_config,
                'signal_thresholds': self.signal_thresholds,
                'analyzed_count': total_analyzed,
                'analysis_results': results,
                'summary': {
                    'total_analyzed': total_analyzed,
                    'recommendations': {
                        'STRONG_BUY': strong_buy,
                        'BUY': buy,
                        'HOLD_WATCH': watch,
                        'SKIP': total_analyzed - strong_buy - buy - watch
                    },
                    'avg_timing_score': np.mean([r.get('phase4_timing_analysis', {}).get('overall_score', 0) for r in results]) if results else 0,
                    'avg_weighted_score': np.mean([r.get('final_recommendation', {}).get('weighted_score', 0) for r in results]) if results else 0,
                    'ready_for_phase5': strong_buy + buy
                }
            }
            
            # S3에 저장
            key = f'phase4/4h_analysis_results_{timestamp}.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json.dumps(output_data, indent=2, ensure_ascii=False),
                ContentType='application/json'
            )
            
            logger.info(f"✅ Phase 4 결과 S3 저장 완료: s3://{self.s3_bucket}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 저장 실패: {e}")
            return False

    def trigger_next_phase(self, analysis_results: List[Dict]) -> bool:
        """Phase 5 트리거"""
        try:
            actionable_recommendations = [
                r for r in analysis_results 
                if r['final_recommendation']['final_rating'] in ['STRONG_BUY', 'BUY']
            ]
            
            if not actionable_recommendations:
                logger.info("📭 매수 신호가 없어 Phase 5 트리거 생략")
                return False
            
            self.events_client.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.phase4',
                        'DetailType': 'Phase 4 4H Analysis Completed',
                        'Detail': json.dumps({
                            'status': 'completed',
                            'timestamp': datetime.now(self.kst).isoformat(),
                            'actionable_count': len(actionable_recommendations),
                            'next_phase': 'phase5'
                        })
                    }
                ]
            )
            
            logger.info(f"✅ Phase 5 트리거 이벤트 발송 완료: {len(actionable_recommendations)}개 매수 신호")
            return True
            
        except Exception as e:
            logger.error(f"❌ Phase 5 트리거 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러"""
    try:
        logger.info("🚀 Phase 4 4H Analysis 시작")
        logger.info(f"📥 입력 이벤트: {json.dumps(event, indent=2, ensure_ascii=False)}")
        
        analyzer = FourHourAnalysisPhase4()
        
        # 1. Phase 3 BUY 추천 종목들 로드
        buy_recommendations = analyzer.load_phase3_recommendations()
        if not buy_recommendations:
            logger.error("❌ Phase 3 BUY 추천 종목이 없습니다")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Phase 3 BUY 추천 없음'})
            }
        
        # 2. 4시간봉 분석 실행
        analysis_results = analyzer.process_buy_recommendations(buy_recommendations)
        
        if not analysis_results:
            logger.warning("⚠️ 4시간봉 분석 결과가 없습니다")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'completed',
                    'analyzed_count': 0,
                    'message': '4시간봉 분석 결과 없음'
                })
            }
        
        # 3. 결과 저장
        save_success = analyzer.save_results_to_s3(analysis_results)
        
        if not save_success:
            logger.error("❌ 결과 저장 실패")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3 저장 실패'})
            }
        
        # 4. Phase 5 트리거
        trigger_success = analyzer.trigger_next_phase(analysis_results)
        
        # 5. 최종 결과 반환
        strong_buy_count = len([r for r in analysis_results if r['final_recommendation']['final_rating'] == 'STRONG_BUY'])
        buy_count = len([r for r in analysis_results if r['final_recommendation']['final_rating'] == 'BUY'])
        
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'input_recommendations': len(buy_recommendations),
                'analyzed_count': len(analysis_results),
                'trading_signals': {
                    'STRONG_BUY': strong_buy_count,
                    'BUY': buy_count,
                    'total_actionable': strong_buy_count + buy_count
                },
                'top_signals': [
                    {
                        'ticker': r['ticker'],
                        'rating': r['final_recommendation']['final_rating'],
                        'weighted_score': r['final_recommendation']['weighted_score'],
                        'entry_timing': r['final_recommendation']['entry_timing']
                    } for r in sorted(analysis_results, key=lambda x: x['final_recommendation']['weighted_score'], reverse=True)[:3]
                ],
                'next_phase_triggered': trigger_success
            }, ensure_ascii=False, indent=2)
        }
        
        logger.info(f"✅ Phase 4 완료: {len(analysis_results)}개 분석, {strong_buy_count + buy_count}개 매수 신호")
        return result
        
    except Exception as e:
        logger.error(f"❌ Phase 4 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'status': 'failed'
            })
        }

if __name__ == "__main__":
    # 로컬 테스트용
    test_event = {
        'source': 'makenaide.phase3',
        'detail-type': 'Phase 3 GPT Analysis Completed'
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))