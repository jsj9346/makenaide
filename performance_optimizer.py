"""
⚡ 성능 최적화 시스템 (performance_optimizer.py)

Makenaide 프로젝트의 메모리 사용량 감소 및 배치 처리 개선 시스템

주요 기능:
1. 메모리 사용량 모니터링 및 분석
2. 배치 처리 최적화
3. 캐시 관리 개선
4. 가비지 컬렉션 최적화
5. 성능 벤치마크

작성자: Makenaide Development Team
작성일: 2025-01-27
버전: 1.0.0
"""

import psutil
import gc
import sys
import time
import threading
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any, Callable
from datetime import datetime, timedelta
from functools import wraps
import tracemalloc
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
from utils import setup_logger

# 로거 설정
logger = setup_logger()

class PerformanceOptimizer:
    """
    ⚡ 성능 최적화 시스템
    
    주요 최적화 영역:
    1. 메모리 사용량 최적화
    2. 배치 처리 성능 향상
    3. 캐시 효율성 개선
    4. I/O 병목 제거
    5. CPU 활용률 최적화
    """
    
    def __init__(self):
        """성능 최적화 시스템 초기화"""
        self.monitoring_active = False
        self.monitor_thread = None
        
        # 성능 메트릭 저장소
        self.metrics = {
            'memory': deque(maxlen=1000),
            'cpu': deque(maxlen=1000),
            'io': deque(maxlen=1000),
            'gc': deque(maxlen=100)
        }
        
        # 메모리 추적 설정
        self.memory_tracker = None
        self.peak_memory = 0
        self.memory_snapshots = []
        
        # 배치 처리 설정
        self.optimal_batch_sizes = {
            'static_indicators': 50,
            'ohlcv_processing': 20,
            'indicator_calculation': 10,
            'db_operations': 100
        }
        
        # 캐시 설정
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'size': 0
        }
        
        logger.info("✅ Performance Optimizer 초기화 완료")
    
    def start_monitoring(self, interval: int = 5):
        """성능 모니터링 시작"""
        if self.monitoring_active:
            logger.warning("⚠️ 성능 모니터링이 이미 실행 중입니다")
            return
        
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop, 
            args=(interval,), 
            daemon=True
        )
        self.monitor_thread.start()
        
        # 메모리 추적 시작
        tracemalloc.start()
        self.memory_tracker = tracemalloc.take_snapshot()
        
        logger.info(f"✅ 성능 모니터링 시작 (간격: {interval}초)")
    
    def stop_monitoring(self):
        """성능 모니터링 중지"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        
        logger.info("⏹️ 성능 모니터링 중지")
    
    def _monitoring_loop(self, interval: int):
        """모니터링 메인 루프"""
        while self.monitoring_active:
            try:
                timestamp = datetime.now()
                
                # 1. 메모리 사용량 수집
                memory_info = self._collect_memory_metrics()
                self.metrics['memory'].append({
                    'timestamp': timestamp,
                    **memory_info
                })
                
                # 2. CPU 사용량 수집
                cpu_info = self._collect_cpu_metrics()
                self.metrics['cpu'].append({
                    'timestamp': timestamp,
                    **cpu_info
                })
                
                # 3. I/O 메트릭 수집
                io_info = self._collect_io_metrics()
                self.metrics['io'].append({
                    'timestamp': timestamp,
                    **io_info
                })
                
                # 4. 가비지 컬렉션 통계
                gc_info = self._collect_gc_metrics()
                if gc_info:
                    self.metrics['gc'].append({
                        'timestamp': timestamp,
                        **gc_info
                    })
                
                # 5. 메모리 스냅샷 (매 1분)
                if timestamp.second == 0:
                    self._take_memory_snapshot()
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"❌ 모니터링 루프 오류: {e}")
                time.sleep(interval)
    
    def _collect_memory_metrics(self) -> Dict[str, float]:
        """메모리 메트릭 수집"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            system_memory = psutil.virtual_memory()
            
            metrics = {
                'rss_mb': memory_info.rss / 1024 / 1024,  # 물리 메모리
                'vms_mb': memory_info.vms / 1024 / 1024,  # 가상 메모리
                'percent': process.memory_percent(),       # 메모리 사용률
                'available_mb': system_memory.available / 1024 / 1024,
                'system_percent': system_memory.percent
            }
            
            # 피크 메모리 업데이트
            if metrics['rss_mb'] > self.peak_memory:
                self.peak_memory = metrics['rss_mb']
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ 메모리 메트릭 수집 실패: {e}")
            return {}
    
    def _collect_cpu_metrics(self) -> Dict[str, float]:
        """CPU 메트릭 수집"""
        try:
            process = psutil.Process()
            
            metrics = {
                'cpu_percent': process.cpu_percent(),
                'system_cpu_percent': psutil.cpu_percent(),
                'num_threads': process.num_threads(),
                'cpu_times_user': process.cpu_times().user,
                'cpu_times_system': process.cpu_times().system
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ CPU 메트릭 수집 실패: {e}")
            return {}
    
    def _collect_io_metrics(self) -> Dict[str, float]:
        """I/O 메트릭 수집"""
        try:
            process = psutil.Process()
            io_counters = process.io_counters()
            
            metrics = {
                'read_bytes': io_counters.read_bytes,
                'write_bytes': io_counters.write_bytes,
                'read_count': io_counters.read_count,
                'write_count': io_counters.write_count
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ I/O 메트릭 수집 실패: {e}")
            return {}
    
    def _collect_gc_metrics(self) -> Optional[Dict[str, int]]:
        """가비지 컬렉션 메트릭 수집"""
        try:
            # 가비지 컬렉션 강제 실행 (필요 시)
            collected = gc.collect()
            
            if collected > 0:
                stats = gc.get_stats()
                return {
                    'collected_objects': collected,
                    'generation_0': stats[0]['collections'] if stats else 0,
                    'generation_1': stats[1]['collections'] if len(stats) > 1 else 0,
                    'generation_2': stats[2]['collections'] if len(stats) > 2 else 0,
                    'uncollectable': len(gc.garbage)
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ GC 메트릭 수집 실패: {e}")
            return None
    
    def _take_memory_snapshot(self):
        """메모리 스냅샷 수집"""
        try:
            if not tracemalloc.is_tracing():
                return
            
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')
            
            # 상위 10개 메모리 사용 라인 저장
            memory_snapshot = {
                'timestamp': datetime.now(),
                'total_memory': sum(stat.size for stat in top_stats),
                'top_consumers': []
            }
            
            for i, stat in enumerate(top_stats[:10]):
                memory_snapshot['top_consumers'].append({
                    'filename': stat.traceback.format()[0].split(',')[0],
                    'line': stat.traceback.format()[0].split(',')[1],
                    'size_mb': stat.size / 1024 / 1024,
                    'count': stat.count
                })
            
            self.memory_snapshots.append(memory_snapshot)
            
            # 스냅샷 개수 제한 (최대 50개)
            if len(self.memory_snapshots) > 50:
                self.memory_snapshots = self.memory_snapshots[-25:]
                
        except Exception as e:
            logger.error(f"❌ 메모리 스냅샷 수집 실패: {e}")
    
    def optimize_batch_processing(self, operation_type: str, data_size: int) -> int:
        """
        배치 처리 최적화 - 데이터 크기에 따른 최적 배치 크기 계산
        
        Args:
            operation_type: 작업 타입 ('static_indicators', 'ohlcv_processing' 등)
            data_size: 전체 데이터 크기
            
        Returns:
            최적 배치 크기
        """
        try:
            # 기본 배치 크기
            base_batch_size = self.optimal_batch_sizes.get(operation_type, 20)
            
            # 현재 메모리 사용량 고려
            current_memory = self._get_current_memory_usage()
            available_memory = self._get_available_memory()
            
            # 메모리 기반 조정
            if current_memory > 0.8:  # 80% 이상 사용 시
                batch_size = max(base_batch_size // 2, 5)
            elif current_memory < 0.5:  # 50% 미만 사용 시
                batch_size = min(base_batch_size * 2, 200)
            else:
                batch_size = base_batch_size
            
            # 데이터 크기 기반 조정
            if data_size < batch_size:
                batch_size = data_size
            elif data_size > batch_size * 100:  # 매우 큰 데이터셋
                batch_size = min(batch_size * 2, 100)
            
            logger.debug(f"⚡ {operation_type} 최적 배치 크기: {batch_size} (데이터: {data_size}개)")
            return batch_size
            
        except Exception as e:
            logger.error(f"❌ 배치 최적화 실패: {e}")
            return self.optimal_batch_sizes.get(operation_type, 20)
    
    def _get_current_memory_usage(self) -> float:
        """현재 메모리 사용률 반환 (0.0~1.0)"""
        try:
            memory = psutil.virtual_memory()
            return memory.percent / 100.0
        except:
            return 0.5  # 기본값
    
    def _get_available_memory(self) -> float:
        """사용 가능한 메모리 (MB) 반환"""
        try:
            memory = psutil.virtual_memory()
            return memory.available / 1024 / 1024
        except:
            return 1024.0  # 기본값 1GB
    
    def optimize_dataframe_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        DataFrame 메모리 사용량 최적화
        
        Args:
            df: 최적화할 DataFrame
            
        Returns:
            메모리 최적화된 DataFrame
        """
        try:
            original_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
            
            # 수치형 컬럼 최적화
            for col in df.select_dtypes(include=['int64']).columns:
                col_min = df[col].min()
                col_max = df[col].max()
                
                if col_min >= -128 and col_max <= 127:
                    df[col] = df[col].astype('int8')
                elif col_min >= -32768 and col_max <= 32767:
                    df[col] = df[col].astype('int16')
                elif col_min >= -2147483648 and col_max <= 2147483647:
                    df[col] = df[col].astype('int32')
            
            for col in df.select_dtypes(include=['float64']).columns:
                # float32로 변환 가능한지 확인
                if df[col].min() >= np.finfo(np.float32).min and df[col].max() <= np.finfo(np.float32).max:
                    df[col] = df[col].astype('float32')
            
            # 문자열 컬럼 최적화
            for col in df.select_dtypes(include=['object']).columns:
                num_unique_values = len(df[col].unique())
                num_total_values = len(df[col])
                if num_unique_values / num_total_values < 0.5:  # 50% 미만이 고유값
                    df[col] = df[col].astype('category')
            
            optimized_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
            memory_reduction = ((original_memory - optimized_memory) / original_memory) * 100
            
            logger.debug(f"⚡ DataFrame 메모리 최적화: {original_memory:.1f}MB → {optimized_memory:.1f}MB ({memory_reduction:.1f}% 감소)")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ DataFrame 메모리 최적화 실패: {e}")
            return df
    
    def parallel_process_tickers(self, tickers: List[str], process_func: Callable, 
                               max_workers: Optional[int] = None) -> List[Any]:
        """
        티커 병렬 처리 최적화
        
        Args:
            tickers: 처리할 티커 목록
            process_func: 처리 함수
            max_workers: 최대 워커 수
            
        Returns:
            처리 결과 목록
        """
        try:
            if max_workers is None:
                # CPU 코어 수 기반 최적 워커 수 계산
                cpu_count = psutil.cpu_count()
                current_load = psutil.cpu_percent(interval=1)
                
                if current_load > 80:
                    max_workers = max(1, cpu_count // 2)
                elif current_load > 50:
                    max_workers = max(2, cpu_count // 1.5)
                else:
                    max_workers = cpu_count
                
                max_workers = int(max_workers)
            
            # 메모리 사용량 고려한 배치 크기 계산
            batch_size = self.optimize_batch_processing('ticker_processing', len(tickers))
            
            logger.info(f"⚡ 병렬 처리 시작: {len(tickers)}개 티커, {max_workers}개 워커, 배치 크기: {batch_size}")
            
            results = []
            start_time = time.time()
            
            # 배치별로 처리
            for i in range(0, len(tickers), batch_size):
                batch_tickers = tickers[i:i + batch_size]
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(process_func, ticker): ticker for ticker in batch_tickers}
                    
                    for future in as_completed(futures):
                        ticker = futures[future]
                        try:
                            result = future.result(timeout=30)  # 30초 타임아웃
                            results.append(result)
                        except Exception as e:
                            logger.error(f"❌ {ticker} 처리 실패: {e}")
                            results.append(None)
                
                # 배치 간 메모리 정리
                gc.collect()
                
                logger.debug(f"⚡ 배치 {i//batch_size + 1} 완료: {len(batch_tickers)}개 티커")
            
            execution_time = time.time() - start_time
            success_count = sum(1 for r in results if r is not None)
            
            logger.info(f"✅ 병렬 처리 완료: {success_count}/{len(tickers)} 성공 ({execution_time:.2f}초)")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 병렬 처리 실패: {e}")
            return [None] * len(tickers)
    
    def cleanup_memory(self):
        """메모리 정리 및 최적화"""
        try:
            logger.info("🧹 메모리 정리 시작")
            
            before_memory = self._get_current_memory_usage()
            
            # 1. 가비지 컬렉션 강제 실행
            collected = gc.collect()
            
            # 2. pandas 캐시 정리
            try:
                import pandas as pd
                if hasattr(pd, '_cache'):
                    pd._cache.clear()
            except:
                pass
            
            # 3. matplotlib 캐시 정리 (사용 중인 경우)
            try:
                import matplotlib
                matplotlib.pyplot.close('all')
            except:
                pass
            
            # 4. 시스템 캐시 정리 제안 (Linux)
            try:
                import os
                if os.name == 'posix':
                    logger.debug("💡 시스템 캐시 정리 권장: sudo sysctl vm.drop_caches=3")
            except:
                pass
            
            after_memory = self._get_current_memory_usage()
            memory_freed = (before_memory - after_memory) * 100
            
            logger.info(f"✅ 메모리 정리 완료: {collected}개 객체 정리, {memory_freed:.1f}% 메모리 확보")
            
        except Exception as e:
            logger.error(f"❌ 메모리 정리 실패: {e}")
    
    def get_performance_report(self) -> Dict[str, Any]:
        """성능 보고서 생성"""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'monitoring_active': self.monitoring_active,
                'peak_memory_mb': self.peak_memory,
                'current_metrics': {}
            }
            
            # 현재 메트릭
            if self.metrics['memory']:
                latest_memory = self.metrics['memory'][-1]
                report['current_metrics']['memory'] = {
                    'rss_mb': latest_memory.get('rss_mb', 0),
                    'percent': latest_memory.get('percent', 0),
                    'available_mb': latest_memory.get('available_mb', 0)
                }
            
            if self.metrics['cpu']:
                latest_cpu = self.metrics['cpu'][-1]
                report['current_metrics']['cpu'] = {
                    'process_percent': latest_cpu.get('cpu_percent', 0),
                    'system_percent': latest_cpu.get('system_cpu_percent', 0),
                    'num_threads': latest_cpu.get('num_threads', 0)
                }
            
            # 최근 1시간 평균
            one_hour_ago = datetime.now() - timedelta(hours=1)
            
            recent_memory = [m for m in self.metrics['memory'] 
                           if m['timestamp'] >= one_hour_ago]
            if recent_memory:
                report['hourly_averages'] = {
                    'memory_mb': np.mean([m.get('rss_mb', 0) for m in recent_memory]),
                    'cpu_percent': np.mean([c.get('cpu_percent', 0) for c in self.metrics['cpu'] 
                                          if c['timestamp'] >= one_hour_ago])
                }
            
            # 최적화 권장사항
            recommendations = []
            
            if report['current_metrics'].get('memory', {}).get('percent', 0) > 80:
                recommendations.append("메모리 사용량이 높습니다. cleanup_memory() 실행을 권장합니다.")
            
            if report['current_metrics'].get('cpu', {}).get('system_percent', 0) > 90:
                recommendations.append("CPU 사용량이 높습니다. 병렬 처리 워커 수 감소를 권장합니다.")
            
            report['recommendations'] = recommendations
            
            return report
            
        except Exception as e:
            logger.error(f"❌ 성능 보고서 생성 실패: {e}")
            return {'error': str(e)}
    
    def benchmark_function(self, func: Callable, *args, iterations: int = 10, **kwargs) -> Dict[str, Any]:
        """함수 성능 벤치마크"""
        try:
            logger.info(f"🚀 함수 벤치마크 시작: {func.__name__} ({iterations}회 반복)")
            
            execution_times = []
            memory_usages = []
            
            for i in range(iterations):
                # 메모리 측정 시작
                before_memory = self._get_current_memory_usage()
                
                # 실행 시간 측정
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    success = True
                except Exception as e:
                    logger.error(f"❌ 반복 {i+1} 실행 실패: {e}")
                    result = None
                    success = False
                
                execution_time = time.time() - start_time
                
                # 메모리 측정 종료
                after_memory = self._get_current_memory_usage()
                memory_delta = after_memory - before_memory
                
                if success:
                    execution_times.append(execution_time)
                    memory_usages.append(memory_delta)
                
                # 반복 간 정리
                gc.collect()
                time.sleep(0.1)
            
            if not execution_times:
                return {'error': '모든 실행이 실패했습니다'}
            
            benchmark_result = {
                'function_name': func.__name__,
                'iterations': len(execution_times),
                'execution_time': {
                    'min': min(execution_times),
                    'max': max(execution_times),
                    'avg': np.mean(execution_times),
                    'std': np.std(execution_times)
                },
                'memory_usage': {
                    'min_delta': min(memory_usages),
                    'max_delta': max(memory_usages),
                    'avg_delta': np.mean(memory_usages),
                    'std_delta': np.std(memory_usages)
                },
                'performance_score': self._calculate_performance_score(execution_times, memory_usages)
            }
            
            logger.info(f"✅ 벤치마크 완료: 평균 {benchmark_result['execution_time']['avg']:.3f}초")
            
            return benchmark_result
            
        except Exception as e:
            logger.error(f"❌ 함수 벤치마크 실패: {e}")
            return {'error': str(e)}
    
    def _calculate_performance_score(self, execution_times: List[float], memory_usages: List[float]) -> float:
        """성능 점수 계산 (0.0~10.0)"""
        try:
            # 실행 시간 점수 (빠를수록 높은 점수)
            avg_time = np.mean(execution_times)
            time_score = max(0, 5 - avg_time)  # 5초 기준
            
            # 메모리 점수 (적게 사용할수록 높은 점수)
            avg_memory = np.mean([abs(m) for m in memory_usages])
            memory_score = max(0, 5 - avg_memory * 100)  # 1% 메모리 증가당 -1점
            
            # 안정성 점수 (표준편차가 낮을수록 높은 점수)
            time_stability = max(0, 3 - np.std(execution_times))
            memory_stability = max(0, 2 - np.std([abs(m) for m in memory_usages]) * 100)
            
            total_score = time_score + memory_score + time_stability + memory_stability
            return min(total_score, 10.0)
            
        except:
            return 5.0  # 기본 점수

# 전역 성능 최적화 인스턴스
performance_optimizer = PerformanceOptimizer()

def start_performance_monitoring(interval: int = 5):
    """성능 모니터링 시작"""
    performance_optimizer.start_monitoring(interval)

def stop_performance_monitoring():
    """성능 모니터링 중지"""
    performance_optimizer.stop_monitoring()

def optimize_memory():
    """메모리 최적화 실행"""
    performance_optimizer.cleanup_memory()

def get_performance_status():
    """현재 성능 상태 조회"""
    return performance_optimizer.get_performance_report()

def performance_benchmark(func, *args, iterations=10, **kwargs):
    """함수 성능 벤치마크"""
    return performance_optimizer.benchmark_function(func, *args, iterations=iterations, **kwargs)

# 데코레이터: 함수 실행 시 자동 성능 측정
def monitor_performance(func):
    """성능 모니터링 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_memory = performance_optimizer._get_current_memory_usage()
        
        try:
            result = func(*args, **kwargs)
            success = True
        except Exception as e:
            logger.error(f"❌ {func.__name__} 실행 실패: {e}")
            result = None
            success = False
            raise
        finally:
            execution_time = time.time() - start_time
            end_memory = performance_optimizer._get_current_memory_usage()
            memory_delta = end_memory - start_memory
            
            if execution_time > 1.0:  # 1초 이상 실행 시 로깅
                logger.info(f"⚡ {func.__name__} 성능: {execution_time:.3f}초, 메모리 변화: {memory_delta:+.3f}%")
        
        return result
    
    return wrapper

# 시스템 초기화 로그
logger.info("✅ Performance Optimization System 초기화 완료")
logger.info("   ⚡ 메모리 모니터링, 배치 최적화, 병렬 처리 최적화 시스템 활성화")
logger.info("   🔧 사용법: start_performance_monitoring() 호출로 모니터링 시작") 