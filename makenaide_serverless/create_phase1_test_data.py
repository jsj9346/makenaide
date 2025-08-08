#!/usr/bin/env python3
"""
📊 Create Phase 1 Test Data for Phase 2 Testing
- Generate realistic Phase 1 filtered data structure
- Upload to S3 for Phase 2 Lambda testing
- Include price and volume data for technical analysis
"""

import boto3
import json
import logging
from datetime import datetime, timedelta
import random
import pytz

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Phase1TestDataCreator:
    """Phase 1 테스트 데이터 생성 클래스"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_bucket = 'makenaide-serverless-data'
        self.kst = pytz.timezone('Asia/Seoul')

    def generate_realistic_price_data(self, ticker: str, days: int = 200) -> dict:
        """현실적인 가격/거래량 데이터 생성"""
        try:
            # 기준 가격 설정 (티커별로 다르게)
            base_prices = {
                'BTC': 85000000,  # 8,500만원
                'ETH': 4200000,   # 420만원
                'ADA': 1200,      # 1,200원
                'DOGE': 350,      # 350원
                'MATIC': 1800,    # 1,800원
            }
            
            base_price = base_prices.get(ticker.replace('KRW-', ''), random.randint(1000, 10000000))
            
            # 200일 간의 가격 데이터 생성
            prices = []
            volumes = []
            
            current_price = base_price
            
            for i in range(days):
                # 일반적인 암호화폐 변동성 (±5% 범위)
                change_percent = random.uniform(-0.05, 0.05)
                current_price = current_price * (1 + change_percent)
                
                # 가격이 0 이하로 떨어지지 않게 제한
                if current_price < base_price * 0.1:
                    current_price = base_price * 0.1
                
                prices.append(current_price)
                
                # 거래량은 가격 변동성과 상관관계
                base_volume = random.randint(1000000, 10000000)  # 기본 거래량
                volume_multiplier = 1 + abs(change_percent) * 3  # 변동성에 따른 거래량 증가
                volume = base_volume * volume_multiplier
                volumes.append(volume)
            
            return {
                'close': prices,
                'volume': volumes,
                'high': [p * random.uniform(1.0, 1.02) for p in prices],
                'low': [p * random.uniform(0.98, 1.0) for p in prices],
                'open': [prices[max(0, i-1)] * random.uniform(0.995, 1.005) for i in range(len(prices))]
            }
            
        except Exception as e:
            logger.error(f"❌ {ticker} 가격 데이터 생성 실패: {e}")
            return {}

    def create_phase1_test_data(self) -> dict:
        """Phase 1 테스트 데이터 생성"""
        try:
            logger.info("📊 Phase 1 테스트 데이터 생성 중...")
            
            # 테스트용 티커 목록 (실제 업비트 티커 형식)
            test_tickers = [
                'KRW-BTC',
                'KRW-ETH', 
                'KRW-ADA',
                'KRW-DOGE',
                'KRW-MATIC'
            ]
            
            filtered_data = []
            
            for ticker in test_tickers:
                try:
                    logger.info(f"  생성 중: {ticker}")
                    
                    # 현실적인 가격 데이터 생성
                    price_data = self.generate_realistic_price_data(ticker)
                    
                    if not price_data:
                        continue
                    
                    # Phase 1 출력 형식에 맞게 구성
                    ticker_data = {
                        'ticker': ticker,
                        'price_data': price_data,
                        'phase1_score': random.uniform(60, 95),  # Phase 1 통과 점수
                        'phase1_reasons': [
                            'MA200 상향 돌파',
                            '거래량 급증',
                            '상승 추세 확인'
                        ],
                        'created_at': datetime.now(self.kst).isoformat()
                    }
                    
                    filtered_data.append(ticker_data)
                    logger.info(f"  ✅ {ticker} 데이터 생성 완료")
                    
                except Exception as e:
                    logger.error(f"❌ {ticker} 데이터 생성 실패: {e}")
                    continue
            
            # Phase 1 출력 형식 구성
            phase1_output = {
                'phase': 'basic_filtering',
                'status': 'success',
                'timestamp': datetime.now(self.kst).isoformat(),
                'total_tickers_processed': 100,  # 가정값
                'filtered_count': len(filtered_data),
                'filtered_data': filtered_data,
                'filtering_criteria': {
                    'ma200_breakout': True,
                    'volume_surge': True,
                    'trend_confirmation': True
                }
            }
            
            logger.info(f"✅ Phase 1 테스트 데이터 생성 완료: {len(filtered_data)}개 티커")
            return phase1_output
            
        except Exception as e:
            logger.error(f"❌ Phase 1 테스트 데이터 생성 실패: {e}")
            return {}

    def upload_to_s3(self, data: dict) -> bool:
        """S3에 Phase 1 데이터 업로드"""
        try:
            logger.info("📤 S3에 Phase 1 테스트 데이터 업로드 중...")
            
            # JSON 데이터 준비
            json_data = json.dumps(data, ensure_ascii=False, indent=2)
            
            # S3에 업로드
            key = 'phase1/filtered_tickers_with_data.json'
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"✅ S3 업로드 완료: s3://{self.s3_bucket}/{key}")
            
            # 업로드 검증
            response = self.s3_client.head_object(Bucket=self.s3_bucket, Key=key)
            file_size = response['ContentLength']
            logger.info(f"  파일 크기: {file_size:,} bytes")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 업로드 실패: {e}")
            return False

    def test_phase2_with_data(self) -> bool:
        """Phase 1 데이터를 이용한 Phase 2 테스트"""
        try:
            logger.info("🧪 Phase 2 Lambda 실제 데이터 테스트...")
            
            lambda_client = boto3.client('lambda')
            
            # Phase 2 테스트 이벤트
            test_event = {
                'source': 'phase1_complete_test',
                'detail-type': 'Phase 1 Completed',
                'timestamp': datetime.now().isoformat()
            }
            
            response = lambda_client.invoke(
                FunctionName='makenaide-phase2-comprehensive-filter',
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            if response['StatusCode'] == 200:
                payload = json.loads(response['Payload'].read().decode('utf-8'))
                
                logger.info(f"📊 Phase 2 테스트 결과:")
                logger.info(f"   Status Code: {payload.get('statusCode')}")
                logger.info(f"   Phase: {payload.get('phase')}")
                
                if payload.get('statusCode') == 200:
                    logger.info(f"   Input Tickers: {payload.get('input_tickers')}")
                    logger.info(f"   Filtered Tickers: {payload.get('filtered_tickers')}")
                    logger.info(f"   Execution Time: {payload.get('execution_time')}")
                    logger.info(f"   Market Condition: {payload.get('market_condition')}")
                    logger.info(f"   Top Tickers: {payload.get('top_tickers', [])}")
                    logger.info("✅ Phase 2 실제 데이터 테스트 성공!")
                    return True
                else:
                    logger.error(f"❌ Phase 2 테스트 실패: {payload.get('error')}")
                    return False
            else:
                logger.error(f"❌ Lambda 호출 실패: {response['StatusCode']}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Phase 2 테스트 실패: {e}")
            return False

    def create_and_test_complete(self) -> bool:
        """전체 프로세스 실행"""
        try:
            logger.info("🚀 Phase 1 데이터 생성 및 Phase 2 테스트 시작")
            
            # 1. Phase 1 테스트 데이터 생성
            phase1_data = self.create_phase1_test_data()
            if not phase1_data:
                return False
            
            # 2. S3에 업로드
            if not self.upload_to_s3(phase1_data):
                return False
            
            # 3. Phase 2 테스트
            if not self.test_phase2_with_data():
                return False
            
            logger.info("🎉 전체 프로세스 완료!")
            logger.info(f"   생성된 티커: {len(phase1_data.get('filtered_data', []))}개")
            logger.info("   S3 업로드: ✅")
            logger.info("   Phase 2 테스트: ✅")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 전체 프로세스 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        print("📊 Phase 1 Test Data Creation & Phase 2 Testing")
        print("=" * 60)
        
        creator = Phase1TestDataCreator()
        
        if creator.create_and_test_complete():
            print("\n✅ Phase 1 데이터 생성 및 Phase 2 테스트 성공!")
            print("\n📋 완료된 작업:")
            print("1. ✅ 현실적인 Phase 1 테스트 데이터 생성")
            print("2. ✅ S3에 데이터 업로드")
            print("3. ✅ Phase 2 Lambda 실제 데이터 테스트")
            print("\n📋 다음 단계:")
            print("1. Phase 3-6 테스트")
            print("2. 전체 워크플로우 검증")
            return True
        else:
            print("\n❌ 테스트 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")
        return False

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)