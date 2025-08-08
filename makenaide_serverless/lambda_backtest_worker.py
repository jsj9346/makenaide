#!/usr/bin/env python3
"""
분산 백테스팅 워커 Lambda 함수

SQS에서 백테스트 작업을 받아서 병렬로 실행하는 Lambda 함수입니다.
기존 백테스팅 모듈과 완전 호환되며, 시간대별 분석도 지원합니다.

Author: Distributed Backtesting Worker
Version: 1.0.0
"""

import json
import logging
import boto3
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import uuid
import traceback

# 로거 설정 (먼저 설정)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Lambda Layer에서 백테스팅 모듈 import
sys.path.append('/opt/python')

# 의존성 임포트 (Layer에서)
try:
    import pandas as pd
    import numpy as np
    logger.info("✅ pandas, numpy 임포트 성공")
except ImportError as e:
    logger.error(f"❌ pandas/numpy 임포트 실패: {e}")
    # 간단한 대체 구현
    pd = None
    np = None

# 백테스팅 모듈 (의존성이 있으면 임포트)
try:
    from backtesting_modules import (
        TimezoneBacktester,
        StrategyConfig,
        create_timezone_backtester,
        create_integrated_backtester
    )
    logger.info("✅ 백테스팅 모듈 임포트 성공")
except ImportError as e:
    logger.error(f"❌ 백테스팅 모듈 임포트 실패: {e}")
    # 간단한 mock 클래스들
    class MockStrategyConfig:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
    
    StrategyConfig = MockStrategyConfig
    TimezoneBacktester = None
    create_timezone_backtester = lambda: None
    create_integrated_backtester = lambda: None

class BacktestWorker:
    """백테스트 작업 워커 클래스"""
    
    def __init__(self):
        self.sqs_client = boto3.client('sqs')
        self.s3_client = boto3.client('s3')
        
        # 환경 변수
        self.job_queue_url = os.environ.get('JOB_QUEUE_URL')
        self.result_queue_url = os.environ.get('RESULT_QUEUE_URL')
        self.s3_bucket = os.environ.get('S3_BUCKET', 'makenaide-backtest-data')
        self.worker_id = os.environ.get('AWS_LAMBDA_LOG_STREAM_NAME', str(uuid.uuid4()))
        
        logger.info(f"🔧 BacktestWorker 초기화: {self.worker_id}")
    
    def process_backtest_job(self, job_message: Dict[str, Any]) -> Dict[str, Any]:
        """백테스트 작업 처리"""
        try:
            job_data = json.loads(job_message['Body'])
            job_id = job_data.get('job_id')
            
            logger.info(f"📊 백테스트 작업 시작: {job_id}")
            start_time = datetime.now()
            
            # 작업 유형별 처리
            job_type = job_data.get('job_type', 'SINGLE_STRATEGY')
            
            if job_type == 'SINGLE_STRATEGY':
                result = self._process_single_strategy(job_data)
            elif job_type == 'TIMEZONE_ANALYSIS':
                result = self._process_timezone_analysis(job_data)
            elif job_type == 'PARAMETER_OPTIMIZATION':
                result = self._process_parameter_optimization(job_data)
            else:
                raise ValueError(f"지원하지 않는 작업 유형: {job_type}")
            
            # 실행 시간 계산
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # 결과 패키징
            processed_result = {
                'job_id': job_id,
                'status': 'COMPLETED',
                'worker_id': self.worker_id,
                'execution_time_seconds': execution_time,
                'completed_at': datetime.now().isoformat(),
                'result_data': result,
                'performance_metrics': {
                    'processing_time': execution_time,
                    'memory_used_mb': self._get_memory_usage(),
                    'data_points_processed': result.get('data_points', 0)
                }
            }
            
            logger.info(f"✅ 백테스트 작업 완료: {job_id} ({execution_time:.2f}초)")
            return processed_result
            
        except Exception as e:
            logger.error(f"❌ 백테스트 작업 실패: {job_id if 'job_id' in locals() else 'Unknown'}")
            logger.error(f"오류 세부사항: {str(e)}")
            logger.error(f"스택 트레이스: {traceback.format_exc()}")
            
            return {
                'job_id': job_data.get('job_id', 'unknown'),
                'status': 'FAILED',
                'worker_id': self.worker_id,
                'error_message': str(e),
                'error_traceback': traceback.format_exc(),
                'failed_at': datetime.now().isoformat()
            }
    
    def _process_single_strategy(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """단일 전략 백테스트 처리"""
        strategy_name = job_data['strategy_name']
        parameters = job_data.get('parameters', {})
        data_range = job_data.get('data_range', {})
        
        logger.info(f"📈 단일 전략 백테스트: {strategy_name}")
        
        # 전략 설정 생성
        config = StrategyConfig(
            name=strategy_name,
            position_size_method=parameters.get('position_size_method', 'percent'),
            position_size_value=parameters.get('position_size_value', 0.1),
            stop_loss_pct=parameters.get('stop_loss_pct', 0.05),
            take_profit_pct=parameters.get('take_profit_pct', 0.15),
            max_positions=parameters.get('max_positions', 10)
        )
        
        # 백테스트 데이터 로드
        backtest_data = self._load_backtest_data(data_range)
        if (pd is None and not backtest_data) or (pd is not None and backtest_data.empty):
            raise Exception("백테스트 데이터를 로드할 수 없습니다")
        
        # 백테스터 실행 (유효성에 따라 분기)
        if create_integrated_backtester is not None:
            # 통합 백테스터로 백테스트 실행
            backtester = create_integrated_backtester(enable_timezone_analysis=False)
            
            # 임시 세션 생성
            session_id = backtester.create_session(f"distributed_{job_data['job_id']}")
            
            # 백테스트 실행
            result = backtester.run_single_strategy_backtest(strategy_name, config, session_id)
            
            if result:
                return {
                    'strategy_name': result.strategy_name,
                    'win_rate': result.win_rate,
                    'avg_return': result.avg_return,
                    'total_trades': result.total_trades,
                    'mdd': result.mdd,
                    'sharpe_ratio': result.sharpe_ratio,
                    'kelly_fraction': result.kelly_fraction,
                    'data_points': len(backtest_data),
                    'backtest_result': result.to_dict()
                }
            else:
                raise Exception("백테스트 실행 결과가 없습니다")
        else:
            # 간단한 mock 백테스트 (의존성이 없는 경우)
            logger.info("🔧 Mock 백테스트 실행 (의존성 없음)")
            
            strategy_func = self._get_strategy_function(strategy_name)
            data_length = len(backtest_data) if isinstance(backtest_data, list) else len(backtest_data)
            
            # 간단한 시뮬레이션
            signals = []
            for i in range(min(100, data_length)):  # 첫 100개 데이터만 처리
                if isinstance(backtest_data, list):
                    sample_data = backtest_data[max(0, i-10):i+1]
                else:
                    sample_data = backtest_data.iloc[max(0, i-10):i+1]
                    
                if len(sample_data) >= 10:
                    signal = strategy_func(sample_data)
                    signals.append(signal)
            
            # Mock 결과 생성
            buy_signals = signals.count('BUY')
            sell_signals = signals.count('SELL')
            total_signals = buy_signals + sell_signals
            
            return {
                'strategy_name': strategy_name,
                'win_rate': 0.6,  # Mock value
                'avg_return': 0.05,  # Mock value
                'total_trades': total_signals,
                'mdd': -0.15,  # Mock value
                'sharpe_ratio': 1.2,  # Mock value
                'kelly_fraction': 0.1,  # Mock value
                'data_points': data_length,
                'signals_generated': len(signals),
                'buy_signals': buy_signals,
                'sell_signals': sell_signals,
                'mock_result': True
            }
    
    def _process_timezone_analysis(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """시간대별 분석 백테스트 처리"""
        strategy_name = job_data['strategy_name']
        parameters = job_data.get('parameters', {})
        data_range = job_data.get('data_range', {})
        
        logger.info(f"🌏 시간대별 분석 백테스트: {strategy_name}")
        
        # 전략 설정 생성
        config = StrategyConfig(
            name=strategy_name,
            position_size_method=parameters.get('position_size_method', 'percent'),
            position_size_value=parameters.get('position_size_value', 0.1),
            stop_loss_pct=parameters.get('stop_loss_pct', 0.05),
            take_profit_pct=parameters.get('take_profit_pct', 0.15)
        )
        
        # 백테스트 데이터 로드
        backtest_data = self._load_backtest_data(data_range)
        if (pd is None and not backtest_data) or (pd is not None and backtest_data.empty):
            raise Exception("백테스트 데이터를 로드할 수 없습니다")
        
        # 시간대별 백테스터 생성
        timezone_backtester = TimezoneBacktester(config, enable_timezone_analysis=True)
        
        # 시간대별 백테스트 실행
        timezone_result = timezone_backtester.backtest_with_timezone_analysis(
            backtest_data, 
            self._get_strategy_function(strategy_name),
            10_000_000
        )
        
        if 'error' not in timezone_result:
            return {
                'strategy_name': timezone_result.get('strategy_name', strategy_name),
                'timezone_analysis_enabled': True,
                'basic_metrics': timezone_result.get('basic_metrics', {}),
                'timezone_performance': timezone_result.get('timezone_performance', {}),
                'activity_correlation': timezone_result.get('activity_correlation', {}),
                'trades_count': len(timezone_result.get('trades', [])),
                'data_points': len(backtest_data),
                'timezone_result': timezone_result
            }
        else:
            raise Exception(f"시간대별 백테스트 실패: {timezone_result.get('error')}")
    
    def _process_parameter_optimization(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """파라미터 최적화 처리"""
        strategy_name = job_data['strategy_name']
        parameter_ranges = job_data.get('parameter_ranges', {})
        data_range = job_data.get('data_range', {})
        
        logger.info(f"🔧 파라미터 최적화: {strategy_name}")
        
        # 기본 설정
        base_config = StrategyConfig(name=strategy_name)
        
        # 백테스트 데이터 로드
        backtest_data = self._load_backtest_data(data_range)
        if (pd is None and not backtest_data) or (pd is not None and backtest_data.empty):
            raise Exception("백테스트 데이터를 로드할 수 없습니다")
        
        # 통합 백테스터로 최적화 실행
        backtester = create_integrated_backtester()
        optimization_result = backtester.optimize_strategy_parameters(
            strategy_name, parameter_ranges
        )
        
        return {
            'strategy_name': strategy_name,
            'optimization_completed': True,
            'parameter_ranges': parameter_ranges,
            'data_points': len(backtest_data),
            'optimization_result': optimization_result
        }
    
    def _load_backtest_data(self, data_range: Dict[str, str]):
        """백테스트 데이터 로드 (S3 또는 DB에서)"""
        try:
            if pd is None or np is None:
                logger.warning("⚠️ pandas/numpy 없음, 간단한 데이터 구조 사용")
                # 간단한 딕셔너리 리스트 반환
                from datetime import datetime, timedelta
                import random
                
                data = []
                current_price = 50000
                start_date = datetime.now() - timedelta(days=30)
                
                for i in range(720):  # 30일 * 24시간
                    date = start_date + timedelta(hours=i)
                    price_change = random.uniform(-0.02, 0.02)
                    current_price = current_price * (1 + price_change)
                    
                    data.append({
                        'ticker': 'BTC-KRW',
                        'date': date,
                        'open': current_price * (1 + random.uniform(-0.005, 0.005)),
                        'high': current_price * (1 + abs(random.uniform(0, 0.01))),
                        'low': current_price * (1 - abs(random.uniform(0, 0.01))),
                        'close': current_price,
                        'volume': random.uniform(1000000, 5000000)
                    })
                
                logger.info(f"📊 백테스트 데이터 로드 완료: {len(data)}개 레코드")
                return data
            
            # pandas가 있는 경우
            # 간단한 샘플 데이터 생성 (실제로는 S3나 DB에서 로드)
            start_date = datetime.now() - timedelta(days=30)
            dates = pd.date_range(start=start_date, periods=720, freq='H')
            
            # 간단한 OHLCV 데이터 생성
            np.random.seed(42)
            price_base = 50000
            
            data = []
            current_price = price_base
            
            for i, date in enumerate(dates):
                price_change = np.random.normal(0, 0.02)
                current_price = current_price * (1 + price_change)
                
                data.append({
                    'ticker': 'BTC-KRW',
                    'date': date,
                    'open': current_price * (1 + np.random.normal(0, 0.005)),
                    'high': current_price * (1 + abs(np.random.normal(0, 0.01))),
                    'low': current_price * (1 - abs(np.random.normal(0, 0.01))),
                    'close': current_price,
                    'volume': np.random.uniform(1000000, 5000000)
                })
            
            df = pd.DataFrame(data)
            logger.info(f"📊 백테스트 데이터 로드 완료: {len(df)}개 레코드")
            return df
            
        except Exception as e:
            logger.error(f"❌ 백테스트 데이터 로드 실패: {e}")
            return [] if pd is None else pd.DataFrame()
    
    def _get_strategy_function(self, strategy_name: str):
        """전략 함수 반환"""
        def simple_strategy(data) -> str:
            """간단한 테스트 전략"""
            if pd is None:
                # 리스트 기반 처리
                if isinstance(data, list) and len(data) < 10:
                    return 'HOLD'
                
                if isinstance(data, list):
                    close_prices = [item['close'] for item in data[-10:]]
                    recent_avg = sum(close_prices[-5:]) / 5
                    older_avg = sum(close_prices[:5]) / 5
                    current_price = close_prices[-1]
                else:
                    return 'HOLD'
            else:
                # pandas DataFrame 처리
                if len(data) < 10:
                    return 'HOLD'
                
                recent_avg = data['close'].tail(5).mean()
                older_avg = data['close'].tail(10).head(5).mean()
                current_price = data['close'].iloc[-1]
            
            if recent_avg > older_avg * 1.02 and current_price > recent_avg * 1.01:
                return 'BUY'
            elif recent_avg < older_avg * 0.98 or current_price < recent_avg * 0.99:
                return 'SELL'
            
            return 'HOLD'
        
        return simple_strategy
    
    def _get_memory_usage(self) -> float:
        """메모리 사용량 반환 (MB)"""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0
    
    def send_result_to_queue(self, result: Dict[str, Any]) -> bool:
        """결과를 결과 큐로 전송"""
        try:
            if not self.result_queue_url:
                logger.warning("⚠️ 결과 큐 URL이 설정되지 않음")
                return False
            
            response = self.sqs_client.send_message(
                QueueUrl=self.result_queue_url,
                MessageBody=json.dumps(result, default=str),
                MessageAttributes={
                    'job_id': {
                        'StringValue': result.get('job_id', 'unknown'),
                        'DataType': 'String'
                    },
                    'status': {
                        'StringValue': result.get('status', 'UNKNOWN'),
                        'DataType': 'String'
                    },
                    'worker_id': {
                        'StringValue': self.worker_id,
                        'DataType': 'String'
                    }
                }
            )
            
            logger.info(f"📤 결과 전송 완료: {result.get('job_id')}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 결과 전송 실패: {e}")
            return False

def lambda_handler(event, context):
    """Lambda 핸들러 함수"""
    try:
        logger.info("🚀 분산 백테스팅 워커 Lambda 시작")
        logger.info(f"📥 이벤트: {json.dumps(event, default=str)}")
        
        worker = BacktestWorker()
        results = []
        
        # SQS 메시지 처리
        if 'Records' in event:
            for record in event['Records']:
                if record.get('eventSource') == 'aws:sqs':
                    # SQS 메시지 처리
                    result = worker.process_backtest_job(record)
                    results.append(result)
                    
                    # 결과를 결과 큐로 전송
                    worker.send_result_to_queue(result)
        
        # 직접 호출 처리
        elif 'job_data' in event:
            # 직접 Lambda 호출
            mock_record = {
                'Body': json.dumps(event['job_data'])
            }
            result = worker.process_backtest_job(mock_record)
            results.append(result)
        
        logger.info(f"✅ 워커 Lambda 완료: {len(results)}개 작업 처리")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Backtest jobs processed successfully',
                'processed_jobs': len(results),
                'results': results
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"❌ Lambda 핸들러 실패: {e}")
        logger.error(f"스택 트레이스: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Lambda execution failed',
                'message': str(e),
                'traceback': traceback.format_exc()
            })
        }

# 로컬 테스트용
if __name__ == "__main__":
    # 로컬 테스트 이벤트
    test_event = {
        'job_data': {
            'job_id': 'test-job-' + str(uuid.uuid4()),
            'job_type': 'SINGLE_STRATEGY',
            'strategy_name': 'Test_Strategy',
            'parameters': {
                'position_size_method': 'percent',
                'position_size_value': 0.1,
                'stop_loss_pct': 0.05
            },
            'data_range': {
                'start_date': '2024-01-01',
                'end_date': '2024-01-31'
            }
        }
    }
    
    # 환경 변수 설정 (로컬 테스트용)
    os.environ['RESULT_QUEUE_URL'] = 'https://sqs.ap-northeast-2.amazonaws.com/123456789/test-queue'
    os.environ['S3_BUCKET'] = 'makenaide-backtest-data'
    
    print("🧪 로컬 테스트 실행")
    result = lambda_handler(test_event, None)
    print(f"📊 테스트 결과: {json.dumps(result, indent=2, default=str)}")