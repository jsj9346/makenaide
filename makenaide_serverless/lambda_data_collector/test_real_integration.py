#!/usr/bin/env python3
"""
실제 DB 연결 통합 테스트
Lambda Data Collector의 실제 데이터베이스 연결 및 기능 검증
"""

import sys
import os
import json
import time
from datetime import datetime

# 모듈 패스 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_real_db_connection():
    """실제 DB 연결 테스트"""
    print("🔗 실제 DB 연결 테스트 시작...")
    
    try:
        from data_collector import DatabaseManager, DataCollectorConfig
        
        # 실제 DB 설정 사용
        db_config = DataCollectorConfig.DB_CONFIG
        print(f"📊 DB 연결 정보: {db_config['host']}:{db_config['port']}")
        
        db_manager = DatabaseManager(db_config)
        
        # 연결 테스트
        start_time = time.time()
        connection = db_manager.get_connection()
        connection_time = time.time() - start_time
        
        print(f"✅ DB 연결 성공 (소요시간: {connection_time:.3f}초)")
        
        # 간단한 쿼리 테스트
        result = db_manager.execute_query("SELECT 1 as test_value", fetchone=True)
        if result and result[0] == 1:
            print("✅ 기본 쿼리 실행 성공")
        else:
            print("❌ 기본 쿼리 실행 실패")
            return False
        
        # 테이블 존재 확인
        tables_to_check = ['ohlcv', 'static_indicators', 'ohlcv_4h']
        for table in tables_to_check:
            try:
                result = db_manager.execute_query(
                    f"SELECT COUNT(*) FROM {table} LIMIT 1", 
                    fetchone=True
                )
                print(f"✅ 테이블 '{table}' 존재 확인 (레코드 수: {result[0]:,}개)")
            except Exception as e:
                print(f"⚠️ 테이블 '{table}' 확인 실패: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}")
        return False

def test_market_data_collector():
    """실제 마켓 데이터 수집기 테스트"""
    print("\n📊 마켓 데이터 수집기 실제 테스트 시작...")
    
    try:
        from data_collector import MarketDataCollector, DatabaseManager, DataCollectorConfig
        
        db_manager = DatabaseManager(DataCollectorConfig.DB_CONFIG)
        collector = MarketDataCollector(db_manager)
        
        # 테스트 티커 (소량)
        test_ticker = "KRW-BTC"
        
        print(f"🔍 {test_ticker} 기존 데이터 상태 확인...")
        
        # 기존 데이터 상태 확인
        existing_count = db_manager.get_existing_data_count(test_ticker)
        latest_date = db_manager.get_latest_timestamp(test_ticker)
        
        print(f"📊 기존 데이터: {existing_count:,}개 레코드")
        if latest_date:
            print(f"📅 최신 데이터: {latest_date.strftime('%Y-%m-%d')}")
        else:
            print("📅 최신 데이터: 없음")
        
        # 증분 수집 테스트 (force_fetch=False)
        print(f"\n🔄 {test_ticker} 증분 데이터 수집 테스트...")
        start_time = time.time()
        
        result = collector.collect_ohlcv_daily(test_ticker, force_fetch=False)
        
        collection_time = time.time() - start_time
        
        if result is not None:
            print(f"✅ 데이터 수집 성공: {len(result)}개 레코드 (소요시간: {collection_time:.3f}초)")
            
            # 수집 후 상태 재확인
            new_count = db_manager.get_existing_data_count(test_ticker)
            new_latest = db_manager.get_latest_timestamp(test_ticker)
            
            print(f"📊 수집 후 데이터: {new_count:,}개 레코드")
            if new_latest:
                print(f"📅 수집 후 최신: {new_latest.strftime('%Y-%m-%d')}")
            
            # 데이터 증가 확인
            if new_count > existing_count:
                print(f"✅ 데이터 증가 확인: +{new_count - existing_count}개")
            else:
                print("ℹ️ 최신 데이터로 증분 수집 불필요")
                
        else:
            print("ℹ️ 증분 수집 불필요 (최신 데이터 존재)")
        
        return True
        
    except Exception as e:
        print(f"❌ 마켓 데이터 수집기 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_technical_indicator_batch():
    """기술적 지표 배치 수집 테스트"""
    print("\n🔧 기술적 지표 배치 수집 테스트 시작...")
    
    try:
        from data_collector import TechnicalIndicatorBatchCollector, DatabaseManager, DataCollectorConfig
        
        db_manager = DatabaseManager(DataCollectorConfig.DB_CONFIG)
        collector = TechnicalIndicatorBatchCollector(db_manager)
        
        # 테스트 티커들
        test_tickers = ["KRW-BTC", "KRW-ETH", "KRW-ADA"]
        
        print(f"📊 배치 조회 대상: {', '.join(test_tickers)}")
        
        # 배치 조회 실행
        start_time = time.time()
        results = collector.get_technical_data_batch(test_tickers)
        batch_time = time.time() - start_time
        
        print(f"⚡ 배치 조회 완료: {batch_time:.3f}초")
        
        # 결과 검증
        success_count = 0
        for ticker in test_tickers:
            if ticker in results and results[ticker] is not None:
                data = results[ticker]
                print(f"✅ {ticker}: price={data.get('price', 'N/A')}, rsi_14={data.get('rsi_14', 'N/A')}")
                success_count += 1
            else:
                print(f"❌ {ticker}: 데이터 없음")
        
        print(f"📊 성공률: {success_count}/{len(test_tickers)} ({success_count/len(test_tickers)*100:.1f}%)")
        
        # 성능 검증 (Phase 1 최적화 효과)
        expected_max_time = 1.0  # 3개 티커를 1초 이내에 처리해야 함
        if batch_time <= expected_max_time:
            print(f"✅ 성능 목표 달성: {batch_time:.3f}초 <= {expected_max_time}초")
        else:
            print(f"⚠️ 성능 목표 미달성: {batch_time:.3f}초 > {expected_max_time}초")
        
        return success_count == len(test_tickers)
        
    except Exception as e:
        print(f"❌ 기술적 지표 배치 수집 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_lambda_function_simulation():
    """Lambda 함수 시뮬레이션 테스트"""
    print("\n🎯 Lambda 함수 시뮬레이션 테스트 시작...")
    
    try:
        from lambda_function import lambda_handler
        
        # 테스트 이벤트들
        test_events = [
            {
                'name': '기술적 지표 배치 조회',
                'event': {
                    'collection_type': 'technical_batch',
                    'tickers': ['KRW-BTC', 'KRW-ETH'],
                    'force_fetch': False
                }
            },
            {
                'name': '일봉 데이터 수집',
                'event': {
                    'collection_type': 'ohlcv_daily',
                    'tickers': ['KRW-BTC'],
                    'force_fetch': False
                }
            }
        ]
        
        results = []
        
        for test_case in test_events:
            print(f"\n📝 테스트 케이스: {test_case['name']}")
            
            start_time = time.time()
            try:
                result = lambda_handler(test_case['event'], None)
                execution_time = time.time() - start_time
                
                if result['statusCode'] == 200:
                    print(f"✅ 성공: {execution_time:.3f}초")
                    print(f"📊 응답: {json.dumps(result['body'], ensure_ascii=False, indent=2)}")
                    results.append(True)
                else:
                    print(f"❌ 실패: {result}")
                    results.append(False)
                    
            except Exception as e:
                print(f"❌ 예외 발생: {e}")
                results.append(False)
        
        success_rate = sum(results) / len(results) * 100
        print(f"\n📊 전체 성공률: {sum(results)}/{len(results)} ({success_rate:.1f}%)")
        
        return success_rate == 100.0
        
    except Exception as e:
        print(f"❌ Lambda 함수 시뮬레이션 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """메인 테스트 실행"""
    print("🧪 Lambda Data Collector 실제 통합 테스트")
    print("=" * 60)
    
    test_results = []
    
    # 1. DB 연결 테스트
    test_results.append(("DB 연결", test_real_db_connection()))
    
    # 2. 마켓 데이터 수집 테스트
    test_results.append(("마켓 데이터 수집", test_market_data_collector()))
    
    # 3. 기술적 지표 배치 테스트
    test_results.append(("기술적 지표 배치", test_technical_indicator_batch()))
    
    # 4. Lambda 함수 시뮬레이션 테스트
    test_results.append(("Lambda 함수 시뮬레이션", test_lambda_function_simulation()))
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("📋 테스트 결과 요약:")
    
    for test_name, result in test_results:
        status = "✅ 통과" if result else "❌ 실패"
        print(f"  {test_name}: {status}")
    
    passed_tests = sum([1 for _, result in test_results if result])
    total_tests = len(test_results)
    success_rate = passed_tests / total_tests * 100
    
    print(f"\n🎯 전체 결과: {passed_tests}/{total_tests} 테스트 통과 ({success_rate:.1f}%)")
    
    if success_rate == 100.0:
        print("🎉 모든 통합 테스트 통과!")
        return True
    else:
        print("⚠️ 일부 테스트 실패 - 로그를 확인하세요.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)