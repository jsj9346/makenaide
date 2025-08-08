#!/usr/bin/env python3
"""
Lambda Data Collector 테스트 스위트
Phase 2 아키텍처 개선용 테스트 코드

테스트 범위:
1. 로컬 단위 테스트
2. 통합 테스트 (DB 연결 필요)
3. Lambda 함수 실행 테스트
4. 성능 테스트
"""

import json
import unittest
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

# 테스트를 위한 모듈 패스 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class TestLambdaDataCollector(unittest.TestCase):
    """Lambda 데이터 컬렉터 단위 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.test_tickers = ['KRW-BTC', 'KRW-ETH']
        self.mock_db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
    
    def test_config_class(self):
        """DataCollectorConfig 클래스 테스트"""
        from data_collector import DataCollectorConfig
        
        config = DataCollectorConfig()
        
        # 기본 설정 확인
        self.assertEqual(config.DEFAULT_OHLCV_DAYS, 450)
        self.assertEqual(config.DEFAULT_4H_LIMIT, 200)
        self.assertEqual(config.MAJOR_COIN_DAYS, 600)
        self.assertEqual(config.BATCH_SIZE, 10)
        
        # 주요 코인 목록 확인
        self.assertIn('KRW-BTC', config.MAJOR_COINS)
        self.assertIn('KRW-ETH', config.MAJOR_COINS)
    
    @patch('psycopg2.connect')
    def test_database_manager_connection(self, mock_connect):
        """DatabaseManager 연결 테스트"""
        from data_collector import DatabaseManager
        
        # Mock 연결 설정
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        db_manager = DatabaseManager(self.mock_db_config)
        connection = db_manager.get_connection()
        
        # 연결이 올바르게 생성되었는지 확인
        mock_connect.assert_called_once_with(**self.mock_db_config)
        self.assertEqual(connection, mock_conn)
    
    @patch('psycopg2.connect')
    def test_database_manager_query_execution(self, mock_connect):
        """DatabaseManager 쿼리 실행 테스트"""
        from data_collector import DatabaseManager
        
        # Mock 설정
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('KRW-BTC', 100)]
        mock_connect.return_value = mock_conn
        
        db_manager = DatabaseManager(self.mock_db_config)
        result = db_manager.execute_query("SELECT ticker, count FROM test_table")
        
        # 쿼리 실행 확인
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchall.assert_called_once()
        self.assertEqual(result, [('KRW-BTC', 100)])
    
    def test_market_data_collector_initialization(self):
        """MarketDataCollector 초기화 테스트"""
        from data_collector import MarketDataCollector, DatabaseManager
        
        with patch.object(DatabaseManager, '__init__', return_value=None):
            mock_db = DatabaseManager(self.mock_db_config)
            collector = MarketDataCollector(mock_db)
            
            self.assertIsNotNone(collector)
            self.assertEqual(collector.db, mock_db)
    
    @patch('data_collector.MarketDataCollector._safe_pyupbit_call')
    @patch('data_collector.DatabaseManager')
    def test_collect_ohlcv_daily_new_ticker(self, mock_db_class, mock_pyupbit):
        """새 티커 일봉 수집 테스트"""
        from data_collector import MarketDataCollector
        
        # Mock DB 설정
        mock_db = Mock()
        mock_db.get_existing_data_count.return_value = 0
        mock_db.get_latest_timestamp.return_value = None
        mock_db.insert_ohlcv_batch.return_value = None
        mock_db_class.return_value = mock_db
        
        # Mock pyupbit 데이터
        mock_df = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            'close': [104, 105, 106],
            'volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3))
        
        mock_pyupbit.return_value = mock_df
        
        collector = MarketDataCollector(mock_db)
        result = collector.collect_ohlcv_daily('BTC')
        
        # 결과 확인
        self.assertIsNotNone(result)
        mock_db.insert_ohlcv_batch.assert_called_once()
    
    @patch('data_collector.MarketDataCollector._safe_pyupbit_call')
    @patch('data_collector.DatabaseManager')
    def test_collect_ohlcv_daily_incremental_update(self, mock_db_class, mock_pyupbit):
        """증분 업데이트 테스트"""
        from data_collector import MarketDataCollector
        
        # Mock DB 설정 (기존 데이터 있음)
        mock_db = Mock()
        mock_db.get_existing_data_count.return_value = 450
        mock_db.get_latest_timestamp.return_value = datetime.now() - timedelta(days=2)
        mock_db.get_connection.return_value = Mock()
        
        mock_db_class.return_value = mock_db
        
        # Mock pyupbit 데이터 (증분)
        mock_df = pd.DataFrame({
            'open': [107, 108],
            'high': [110, 111],
            'low': [105, 106],
            'close': [109, 110],
            'volume': [1300, 1400]
        }, index=pd.date_range(datetime.now() - timedelta(days=1), periods=2))
        
        mock_pyupbit.return_value = mock_df
        
        collector = MarketDataCollector(mock_db)
        result = collector.collect_ohlcv_daily('BTC', force_fetch=False)
        
        # 증분 업데이트가 호출되었는지 확인
        mock_db.get_existing_data_count.assert_called_once()
        mock_db.get_latest_timestamp.assert_called_once()
    
    def test_data_validation_and_cleaning(self):
        """데이터 검증 및 정제 테스트"""
        from data_collector import MarketDataCollector, DatabaseManager
        
        # 문제가 있는 테스트 데이터 생성
        bad_data = pd.DataFrame({
            'open': [100, 0, 102, 50],  # 0값 포함
            'high': [105, 0, 107, 45],  # high < low인 경우
            'low': [95, 0, 97, 55],     # 논리적 오류
            'close': [104, 0, 106, 48],
            'volume': [1000, 0, 1200, 800]
        }, index=pd.date_range('2023-01-01', periods=4))
        
        with patch.object(DatabaseManager, '__init__', return_value=None):
            mock_db = DatabaseManager({})
            collector = MarketDataCollector(mock_db)
            
            cleaned_data = collector._validate_and_clean_data(bad_data, 'KRW-BTC')
            
            # 0값과 논리적 오류가 제거되었는지 확인
            self.assertEqual(len(cleaned_data), 2)  # 2개만 남아야 함
            
            # 모든 값이 0보다 큰지 확인
            for col in ['open', 'high', 'low', 'close']:
                self.assertTrue((cleaned_data[col] > 0).all())
    
    @patch('data_collector.DatabaseManager')
    def test_technical_indicator_batch_collector(self, mock_db_class):
        """기술적 지표 배치 수집기 테스트"""
        from data_collector import TechnicalIndicatorBatchCollector
        
        # Mock DB 설정
        mock_db = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()
        
        # Mock 쿼리 결과
        mock_cursor.fetchone.side_effect = [
            (100000, 1500, 25, 0.15, 1, 99000, 45, 90000, 95000, 101000, 98000),  # BTC
            (3000, 200, 30, 0.20, -1, 2900, 55, 2800, 2950, 3100, 2850)           # ETH
        ]
        
        mock_conn.cursor.return_value = mock_cursor
        mock_db.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db
        
        collector = TechnicalIndicatorBatchCollector(mock_db)
        result = collector.get_technical_data_batch(['KRW-BTC', 'KRW-ETH'])
        
        # 결과 검증
        self.assertEqual(len(result), 2)
        self.assertIn('KRW-BTC', result)
        self.assertIn('KRW-ETH', result)
        
        # BTC 데이터 검증
        btc_data = result['KRW-BTC']
        self.assertEqual(btc_data['price'], 100000)
        self.assertEqual(btc_data['atr'], 1500)
        self.assertEqual(btc_data['rsi_14'], 45)
    
    def test_lambda_data_collector_initialization(self):
        """LambdaDataCollector 초기화 테스트"""
        with patch('data_collector.DatabaseManager'), \
             patch('data_collector.MarketDataCollector'), \
             patch('data_collector.TechnicalIndicatorBatchCollector'):
            
            from data_collector import LambdaDataCollector
            
            collector = LambdaDataCollector()
            
            self.assertIsNotNone(collector)
            self.assertIsNotNone(collector.db)
            self.assertIsNotNone(collector.market_collector)
            self.assertIsNotNone(collector.technical_collector)

class TestLambdaFunction(unittest.TestCase):
    """Lambda 함수 통합 테스트"""
    
    def test_lambda_handler_valid_event(self):
        """유효한 이벤트로 Lambda 핸들러 테스트"""
        from lambda_function import lambda_handler
        
        test_event = {
            'collection_type': 'ohlcv_daily',
            'tickers': ['KRW-BTC'],
            'force_fetch': False
        }
        
        # data_collector 모듈 레벨에서 Mock 처리
        with patch('data_collector.LambdaDataCollector') as mock_collector_class:
            mock_collector = Mock()
            mock_collector.process_data_collection_request.return_value = {
                'statusCode': 200,
                'body': {
                    'success': True,
                    'message': 'Success'
                }
            }
            mock_collector_class.return_value = mock_collector
            
            result = lambda_handler(test_event, None)
            
            # 성공 응답 확인
            self.assertEqual(result['statusCode'], 200)
            self.assertTrue(result['body']['success'])
    
    def test_lambda_handler_import_error(self):
        """Import 에러 시 Lambda 핸들러 테스트"""
        from lambda_function import lambda_handler
        
        test_event = {
            'collection_type': 'ohlcv_daily',
            'tickers': ['KRW-BTC'],
            'force_fetch': False
        }
        
        # Import 에러 발생시키기 (data_collector 모듈에서)
        with patch('data_collector.LambdaDataCollector', side_effect=ImportError("모듈 없음")):
            result = lambda_handler(test_event, None)
            
            # 에러 응답 확인
            self.assertEqual(result['statusCode'], 500)
            self.assertFalse(result['body']['success'])
            self.assertEqual(result['body']['error_type'], 'ImportError')

class TestPerformance(unittest.TestCase):
    """성능 테스트"""
    
    @patch('data_collector.DatabaseManager')
    def test_batch_processing_performance(self, mock_db_class):
        """배치 처리 성능 테스트"""
        from data_collector import TechnicalIndicatorBatchCollector
        
        # 대량 티커 리스트 생성
        large_ticker_list = [f'KRW-COIN{i:03d}' for i in range(100)]
        
        # Mock DB 설정
        mock_db = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()
        
        # 빠른 응답을 위한 Mock 설정
        mock_cursor.fetchone.return_value = (100, 10, 20, 0.1, 1, 90, 50, 80, 95, 110, 85)
        mock_conn.cursor.return_value = mock_cursor
        mock_db.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db
        
        collector = TechnicalIndicatorBatchCollector(mock_db)
        
        # 성능 측정
        start_time = time.time()
        result = collector.get_technical_data_batch(large_ticker_list)
        elapsed_time = time.time() - start_time
        
        # 성능 기준 (100개 티커를 5초 이내에 처리)
        self.assertLess(elapsed_time, 5.0)
        self.assertEqual(len(result), 100)
        
        print(f"📊 배치 처리 성능: {len(large_ticker_list)}개 티커를 {elapsed_time:.3f}초에 처리")

class TestIntegration(unittest.TestCase):
    """통합 테스트 (실제 DB 연결 필요)"""
    
    def setUp(self):
        """통합 테스트 설정"""
        # 환경변수에서 DB 설정 읽기 (테스트 DB 사용 권장)
        self.db_config = {
            'host': os.environ.get('TEST_DB_HOST', 'localhost'),
            'port': int(os.environ.get('TEST_DB_PORT', '5432')),
            'database': os.environ.get('TEST_DB_NAME', 'test_makenaide'),
            'user': os.environ.get('TEST_DB_USER', 'test_user'),
            'password': os.environ.get('TEST_DB_PASSWORD', 'test_pass')
        }
        
        # 테스트 DB 연결 가능 여부 확인
        self.db_available = self._check_db_connection()
    
    def _check_db_connection(self):
        """테스트 DB 연결 확인"""
        try:
            import psycopg2
            conn = psycopg2.connect(**self.db_config)
            conn.close()
            return True
        except:
            return False
    
    @unittest.skipUnless(os.environ.get('RUN_INTEGRATION_TESTS') == 'true', 
                        "통합 테스트는 RUN_INTEGRATION_TESTS=true 환경변수 설정시에만 실행")
    def test_real_database_connection(self):
        """실제 DB 연결 테스트"""
        if not self.db_available:
            self.skipTest("테스트 DB를 사용할 수 없습니다")
        
        from data_collector import DatabaseManager
        
        db_manager = DatabaseManager(self.db_config)
        connection = db_manager.get_connection()
        
        self.assertIsNotNone(connection)
        
        # 간단한 쿼리 테스트
        result = db_manager.execute_query("SELECT 1", fetchone=True)
        self.assertEqual(result[0], 1)

def run_tests():
    """테스트 실행 함수"""
    print("🧪 Lambda Data Collector 테스트 시작")
    print("=" * 50)
    
    # 테스트 스위트 생성
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 테스트 클래스 추가
    suite.addTests(loader.loadTestsFromTestCase(TestLambdaDataCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestLambdaFunction))
    suite.addTests(loader.loadTestsFromTestCase(TestPerformance))
    
    # 통합 테스트는 환경변수 설정시에만 추가
    if os.environ.get('RUN_INTEGRATION_TESTS') == 'true':
        suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
        print("🔗 통합 테스트 포함")
    else:
        print("⚠️ 통합 테스트 건너뜀 (RUN_INTEGRATION_TESTS=true 설정시 실행)")
    
    # 테스트 실행
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # 결과 출력
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print("✅ 모든 테스트 통과!")
    else:
        print(f"❌ 테스트 실패: {len(result.failures)}개 실패, {len(result.errors)}개 에러")
        
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)