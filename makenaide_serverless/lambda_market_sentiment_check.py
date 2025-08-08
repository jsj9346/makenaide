#!/usr/bin/env python3
"""
📊 Market Sentiment Check Lambda
- 시장 상황을 판단하여 파이프라인 진행 여부 결정
- 하락장 감지 시 EC2/RDS 종료 및 비용 절감
- BTC 기준 멀티 타임프레임 분석
"""

import boto3
import json
import logging
import urllib3
from datetime import datetime, timedelta
from typing import Dict, Optional, List

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MarketSentimentAnalyzer:
    """시장 상황 분석 및 리소스 제어 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.ec2_client = boto3.client('ec2')
        self.rds_client = boto3.client('rds')
        self.events_client = boto3.client('events')
        self.sns_client = boto3.client('sns')
        
        self.s3_bucket = 'makenaide-bucket-901361833359'
        # KST = UTC+9
        self.kst_offset = 9
        
        # 설정값
        self.config = {
            'btc_ticker': 'KRW-BTC',
            'sentiment_thresholds': {
                'bull': 60,      # 60점 이상 상승장
                'neutral': 40,   # 40-60점 중립장
                'bear': 40       # 40점 미만 하락장
            },
            'ec2_instance_ids': [],  # 환경변수로 설정
            'rds_instance_id': '',   # 환경변수로 설정
            'sns_topic_arn': ''      # 환경변수로 설정
        }
    
    def fetch_btc_data(self) -> Dict:
        """BTC 가격 데이터 수집"""
        try:
            http = urllib3.PoolManager()
            
            # 현재가 조회
            ticker_url = f"https://api.upbit.com/v1/ticker?markets={self.config['btc_ticker']}"
            ticker_response = http.request('GET', ticker_url)
            
            if ticker_response.status != 200:
                logger.error(f"BTC 현재가 조회 실패: {ticker_response.status}")
                return None
            
            ticker_data = json.loads(ticker_response.data.decode('utf-8'))[0]
            current_price = float(ticker_data['trade_price'])
            change_24h = float(ticker_data['signed_change_rate']) * 100
            
            # 일봉 데이터 조회 (30일)
            candles_url = "https://api.upbit.com/v1/candles/days"
            candles_params = f"?market={self.config['btc_ticker']}&count=30"
            candles_response = http.request('GET', candles_url + candles_params)
            
            if candles_response.status != 200:
                logger.error(f"BTC 일봉 데이터 조회 실패: {candles_response.status}")
                return None
            
            candles_data = json.loads(candles_response.data.decode('utf-8'))
            
            # 7일, 30일 변화율 계산
            if len(candles_data) >= 7:
                price_7d_ago = float(candles_data[6]['trade_price'])
                change_7d = ((current_price - price_7d_ago) / price_7d_ago) * 100
            else:
                change_7d = 0
            
            if len(candles_data) >= 30:
                price_30d_ago = float(candles_data[29]['trade_price'])
                change_30d = ((current_price - price_30d_ago) / price_30d_ago) * 100
            else:
                change_30d = 0
            
            # MA200 계산 (간이 버전 - 30일 이동평균으로 대체)
            prices = [float(candle['trade_price']) for candle in candles_data]
            ma30 = sum(prices) / len(prices) if prices else current_price
            
            return {
                'current_price': current_price,
                'change_24h': change_24h,
                'change_7d': change_7d,
                'change_30d': change_30d,
                'ma30': ma30,
                'price_above_ma30': current_price > ma30,
                'timestamp': (datetime.utcnow() + timedelta(hours=self.kst_offset)).isoformat()
            }
            
        except Exception as e:
            logger.error(f"BTC 데이터 수집 실패: {e}")
            return None
    
    def calculate_market_sentiment(self, btc_data: Dict) -> Dict:
        """시장 상황 점수 계산"""
        try:
            sentiment_score = 0
            factors = []
            
            # 1. 단기 추세 (24시간) - 20점
            if btc_data['change_24h'] > 0:
                sentiment_score += 20
                factors.append(f"24시간: +{btc_data['change_24h']:.1f}%")
            else:
                factors.append(f"24시간: {btc_data['change_24h']:.1f}%")
            
            # 2. 중기 추세 (7일) - 30점
            if btc_data['change_7d'] > 0:
                sentiment_score += 30
                factors.append(f"7일: +{btc_data['change_7d']:.1f}%")
            else:
                factors.append(f"7일: {btc_data['change_7d']:.1f}%")
            
            # 3. 장기 추세 (30일) - 30점
            if btc_data['change_30d'] > 0:
                sentiment_score += 30
                factors.append(f"30일: +{btc_data['change_30d']:.1f}%")
            else:
                factors.append(f"30일: {btc_data['change_30d']:.1f}%")
            
            # 4. MA30 위치 - 20점
            if btc_data['price_above_ma30']:
                sentiment_score += 20
                factors.append("MA30 상향")
            else:
                factors.append("MA30 하향")
            
            # 시장 상황 판단
            if sentiment_score >= self.config['sentiment_thresholds']['bull']:
                market_condition = 'BULL'
            elif sentiment_score >= self.config['sentiment_thresholds']['neutral']:
                market_condition = 'NEUTRAL'
            else:
                market_condition = 'BEAR'
            
            return {
                'market_condition': market_condition,
                'sentiment_score': sentiment_score,
                'factors': factors,
                'should_continue': market_condition != 'BEAR',
                'btc_price': btc_data['current_price'],
                'analysis_time': (datetime.utcnow() + timedelta(hours=self.kst_offset)).isoformat()
            }
            
        except Exception as e:
            logger.error(f"시장 상황 계산 실패: {e}")
            return None
    
    def stop_ec2_instances(self) -> bool:
        """EC2 인스턴스 중지"""
        try:
            instance_ids = self.config.get('ec2_instance_ids', [])
            if not instance_ids:
                logger.info("중지할 EC2 인스턴스가 설정되지 않음")
                return True
            
            logger.info(f"EC2 인스턴스 중지: {instance_ids}")
            
            response = self.ec2_client.stop_instances(
                InstanceIds=instance_ids,
                Force=False
            )
            
            stopping_instances = response.get('StoppingInstances', [])
            logger.info(f"중지 중인 인스턴스: {len(stopping_instances)}개")
            
            return True
            
        except Exception as e:
            logger.error(f"EC2 인스턴스 중지 실패: {e}")
            return False
    
    def stop_rds_instance(self) -> bool:
        """RDS 인스턴스 중지"""
        try:
            instance_id = self.config.get('rds_instance_id', '')
            if not instance_id:
                logger.info("중지할 RDS 인스턴스가 설정되지 않음")
                return True
            
            logger.info(f"RDS 인스턴스 중지: {instance_id}")
            
            response = self.rds_client.stop_db_instance(
                DBInstanceIdentifier=instance_id
            )
            
            db_status = response['DBInstance']['DBInstanceStatus']
            logger.info(f"RDS 상태: {db_status}")
            
            return True
            
        except Exception as e:
            logger.error(f"RDS 인스턴스 중지 실패: {e}")
            return False
    
    def start_ec2_instances(self) -> bool:
        """EC2 인스턴스 시작"""
        try:
            instance_ids = self.config.get('ec2_instance_ids', [])
            if not instance_ids:
                logger.info("시작할 EC2 인스턴스가 설정되지 않음")
                return True
            
            logger.info(f"EC2 인스턴스 시작: {instance_ids}")
            
            response = self.ec2_client.start_instances(
                InstanceIds=instance_ids
            )
            
            starting_instances = response.get('StartingInstances', [])
            logger.info(f"시작 중인 인스턴스: {len(starting_instances)}개")
            
            return True
            
        except Exception as e:
            logger.error(f"EC2 인스턴스 시작 실패: {e}")
            return False
    
    def start_rds_instance(self) -> bool:
        """RDS 인스턴스 시작"""
        try:
            instance_id = self.config.get('rds_instance_id', '')
            if not instance_id:
                logger.info("시작할 RDS 인스턴스가 설정되지 않음")
                return True
            
            logger.info(f"RDS 인스턴스 시작: {instance_id}")
            
            response = self.rds_client.start_db_instance(
                DBInstanceIdentifier=instance_id
            )
            
            db_status = response['DBInstance']['DBInstanceStatus']
            logger.info(f"RDS 상태: {db_status}")
            
            return True
            
        except Exception as e:
            logger.error(f"RDS 인스턴스 시작 실패: {e}")
            return False
    
    def send_notification(self, message: str, subject: str = "Makenaide 시장 상황 알림"):
        """SNS 알림 발송"""
        try:
            topic_arn = self.config.get('sns_topic_arn', '')
            if not topic_arn:
                logger.info("SNS 토픽이 설정되지 않음")
                return
            
            self.sns_client.publish(
                TopicArn=topic_arn,
                Subject=subject,
                Message=message
            )
            
            logger.info("알림 발송 완료")
            
        except Exception as e:
            logger.error(f"알림 발송 실패: {e}")
    
    def trigger_pipeline(self):
        """파이프라인 시작 이벤트 발송"""
        try:
            event_detail = {
                'source': 'market_sentiment',
                'status': 'pipeline_start',
                'timestamp': (datetime.utcnow() + timedelta(hours=self.kst_offset)).isoformat()
            }
            
            self.events_client.put_events(
                Entries=[
                    {
                        'Source': 'makenaide.market_sentiment',
                        'DetailType': 'Pipeline Start Approved',
                        'Detail': json.dumps(event_detail)
                    }
                ]
            )
            
            logger.info("파이프라인 시작 이벤트 발송 완료")
            
        except Exception as e:
            logger.error(f"파이프라인 트리거 실패: {e}")
    
    def save_sentiment_to_s3(self, sentiment_data: Dict):
        """시장 상황 분석 결과 S3 저장"""
        try:
            timestamp = (datetime.utcnow() + timedelta(hours=self.kst_offset)).strftime('%Y%m%d_%H%M%S')
            
            # 메인 결과 파일
            main_key = 'market_sentiment/current_sentiment.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=main_key,
                Body=json.dumps(sentiment_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            # 백업 파일
            backup_key = f'market_sentiment/history/sentiment_{timestamp}.json'
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=backup_key,
                Body=json.dumps(sentiment_data, ensure_ascii=False),
                ContentType='application/json'
            )
            
            logger.info("시장 상황 분석 결과 S3 저장 완료")
            
        except Exception as e:
            logger.error(f"S3 저장 실패: {e}")

def lambda_handler(event, context):
    """Lambda 핸들러"""
    try:
        logger.info("=== Market Sentiment Check 시작 ===")
        logger.info(f"이벤트: {json.dumps(event)}")
        
        # 분석기 초기화
        analyzer = MarketSentimentAnalyzer()
        
        # 환경변수에서 설정 로드
        import os
        analyzer.config['ec2_instance_ids'] = os.environ.get('EC2_INSTANCE_IDS', '').split(',')
        analyzer.config['rds_instance_id'] = os.environ.get('RDS_INSTANCE_ID', '')
        analyzer.config['sns_topic_arn'] = os.environ.get('SNS_TOPIC_ARN', '')
        
        # BTC 데이터 수집
        btc_data = analyzer.fetch_btc_data()
        if not btc_data:
            logger.error("BTC 데이터 수집 실패")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'BTC 데이터 수집 실패',
                    'message': '시장 상황을 판단할 수 없습니다'
                })
            }
        
        # 시장 상황 분석
        sentiment = analyzer.calculate_market_sentiment(btc_data)
        if not sentiment:
            logger.error("시장 상황 분석 실패")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': '시장 상황 분석 실패',
                    'message': '분석 중 오류가 발생했습니다'
                })
            }
        
        # 분석 결과 저장
        analyzer.save_sentiment_to_s3(sentiment)
        
        # 시장 상황에 따른 처리
        if sentiment['should_continue']:
            # 상승장/중립장 - 파이프라인 진행
            logger.info(f"✅ 시장 상황: {sentiment['market_condition']} (점수: {sentiment['sentiment_score']})")
            logger.info("파이프라인 진행 결정")
            
            # 리소스 시작 (필요시)
            analyzer.start_ec2_instances()
            analyzer.start_rds_instance()
            
            # 파이프라인 시작 트리거
            analyzer.trigger_pipeline()
            
            # 알림 발송
            message = f"""
[Makenaide 시장 분석 결과]

📊 시장 상황: {sentiment['market_condition']}
📈 종합 점수: {sentiment['sentiment_score']}/100
💰 BTC 가격: {sentiment['btc_price']:,.0f} KRW

🔍 분석 요소:
{chr(10).join('• ' + factor for factor in sentiment['factors'])}

✅ 결정: 거래 파이프라인 진행
⏰ 분석 시간: {sentiment['analysis_time']}
"""
            analyzer.send_notification(message, "✅ Makenaide 거래 진행")
            
        else:
            # 하락장 - 파이프라인 중단 및 리소스 절감
            logger.info(f"🛑 시장 상황: {sentiment['market_condition']} (점수: {sentiment['sentiment_score']})")
            logger.info("하락장 감지 - 파이프라인 중단 및 비용 절감 모드")
            
            # EC2/RDS 중지
            ec2_stopped = analyzer.stop_ec2_instances()
            rds_stopped = analyzer.stop_rds_instance()
            
            # 알림 발송
            message = f"""
[Makenaide 시장 분석 결과]

📊 시장 상황: {sentiment['market_condition']} (하락장)
📉 종합 점수: {sentiment['sentiment_score']}/100
💰 BTC 가격: {sentiment['btc_price']:,.0f} KRW

🔍 분석 요소:
{chr(10).join('• ' + factor for factor in sentiment['factors'])}

🛑 결정: 거래 중단 및 비용 절감
💡 조치:
• EC2 인스턴스 중지: {'완료' if ec2_stopped else '실패'}
• RDS 인스턴스 중지: {'완료' if rds_stopped else '실패'}
• 다음 스케줄까지 대기

⏰ 분석 시간: {sentiment['analysis_time']}
💰 예상 비용 절감: 일일 약 $1-2
"""
            analyzer.send_notification(message, "🛑 Makenaide 거래 중단 (하락장)")
        
        # 응답 반환
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'market_condition': sentiment['market_condition'],
                'sentiment_score': sentiment['sentiment_score'],
                'should_continue': sentiment['should_continue'],
                'btc_price': sentiment['btc_price'],
                'factors': sentiment['factors'],
                'timestamp': sentiment['analysis_time']
            })
        }
        
        logger.info(f"=== Market Sentiment Check 완료 ===")
        logger.info(f"결과: {response}")
        
        return response
        
    except Exception as e:
        logger.error(f"Market Sentiment Check 실행 실패: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Market Sentiment Check 실행 중 오류 발생'
            })
        }