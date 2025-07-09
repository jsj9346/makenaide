import concurrent.futures
import logging
from typing import List, Callable, Any
import time

class ParallelProcessor:
    def __init__(self, max_workers=4):
        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    
    def process_tickers(self, tickers: List[str], func: Callable, *args, **kwargs) -> dict:
        """
        여러 티커에 대해 병렬로 함수 실행
        Args:
            tickers: 처리할 티커 리스트
            func: 실행할 함수
            *args, **kwargs: 함수에 전달할 인자들
        Returns:
            dict: {ticker: result} 형태의 결과
        """
        results = {}
        futures = {}
        
        # 작업 제출
        for ticker in tickers:
            future = self.executor.submit(func, ticker, *args, **kwargs)
            futures[future] = ticker
        
        # 결과 수집
        for future in concurrent.futures.as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                results[ticker] = result
            except Exception as e:
                logging.error(f"❌ {ticker} 처리 중 오류 발생: {str(e)}")
                results[ticker] = None
        
        return results
    
    def process_data(self, data_list: List[Any], func: Callable, *args, **kwargs) -> List[Any]:
        """
        데이터 리스트에 대해 병렬로 함수 실행
        Args:
            data_list: 처리할 데이터 리스트
            func: 실행할 함수
            *args, **kwargs: 함수에 전달할 인자들
        Returns:
            List: 처리 결과 리스트
        """
        results = []
        futures = []
        
        # 작업 제출
        for data in data_list:
            future = self.executor.submit(func, data, *args, **kwargs)
            futures.append(future)
        
        # 결과 수집
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.error(f"❌ 데이터 처리 중 오류 발생: {str(e)}")
                results.append(None)
        
        return results
    
    def shutdown(self):
        """스레드 풀 종료"""
        self.executor.shutdown(wait=True)

# 전역 병렬 처리기 인스턴스
parallel_processor = ParallelProcessor()

def process_tickers_parallel(tickers: List[str], func: Callable, *args, **kwargs) -> dict:
    """여러 티커에 대해 병렬로 함수 실행 (편의 함수)"""
    return parallel_processor.process_tickers(tickers, func, *args, **kwargs)

def process_data_parallel(data_list: List[Any], func: Callable, *args, **kwargs) -> List[Any]:
    """데이터 리스트에 대해 병렬로 함수 실행 (편의 함수)"""
    return parallel_processor.process_data(data_list, func, *args, **kwargs) 