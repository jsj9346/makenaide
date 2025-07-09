"""
메모리 최적화된 데이터 품질 모니터링 시스템

🧠 메모리 최적화 기능:
- 상세 이슈 저장 개수 제한 (FIFO 방식)
- 주기적 메모리 정리 메커니즘
- 메모리 사용량 실시간 추적
- 지표별 메모리 프로파일링

📊 품질 모니터링 기능:
- API 응답 품질 추적
- 지표 계산 성공률 모니터링
- DB 업데이트 품질 관리
- 자동 복구 메커니즘
"""

import time
import logging
import gc
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import deque
import psutil

# config 패키지에서 설정 import
from config import MEMORY_LIMITS, QUALITY_ALERT_THRESHOLDS, PERFORMANCE_MONITORING

logger = logging.getLogger(__name__)

class MemoryOptimizedDataQualityMonitor:
    """
    메모리 최적화된 데이터 품질 모니터링 클래스
    
    🧠 메모리 최적화 특징:
    - 상세 이슈 저장을 FIFO 방식으로 제한
    - 주기적 메모리 정리 (가비지 컬렉션)
    - 메모리 사용량 실시간 모니터링
    - 지표별 메모리 프로파일 관리
    
    📊 품질 모니터링:
    - 필수 지표 중심 품질 추적
    - 자동 복구 메커니즘
    - 실시간 알림 시스템
    """
    
    def __init__(self, max_issues: int = None):
        """
        모니터 초기화
        
        Args:
            max_issues: 최대 이슈 저장 개수 (기본값: config에서 가져옴)
        """
        self.max_issues = max_issues or MEMORY_LIMITS['DETAIL_ISSUES_LIMIT']
        
        # 기본 품질 통계
        self.quality_stats = {
            'api_calls_total': 0,
            'api_calls_1970_errors': 0,
            'indicator_calculations_total': 0,
            'indicator_calculations_failed': 0,
            'db_updates_total': 0,
            'db_updates_failed': 0,
            'memory_cleanups_performed': 0
        }
        
        # 메모리 최적화된 상세 이슈 저장 (FIFO 방식)
        self.detailed_issues = deque(maxlen=self.max_issues)
        
        # 지표별 성능 통계 (메모리 효율적)
        self.indicator_stats = {}
        
        # 자동 복구 설정
        self.auto_recovery_config = {
            'api_1970_retry_enabled': True,
            'max_retry_attempts': 3,
            'use_fallback_values': True,
            'fallback_validation_threshold': 0.8
        }
        
        # 메모리 모니터링 설정
        self.memory_monitor = {
            'last_cleanup_time': time.time(),
            'cleanup_interval': MEMORY_LIMITS.get('MEMORY_CLEANUP_INTERVAL', 300),  # 5분
            'peak_memory_mb': 0.0,
            'current_memory_mb': 0.0,
            'cleanup_threshold_mb': MEMORY_LIMITS['INDICATOR_MEMORY_THRESHOLD']
        }
        
        # 성능 벤치마킹 (간소화)
        self.performance_stats = {
            'calculation_times': {},
            'memory_usage_snapshots': deque(maxlen=100),  # 최근 100개만 저장
            'last_benchmark': None
        }
        
        # 알림 임계값 (config에서 가져옴)
        self.alert_thresholds = QUALITY_ALERT_THRESHOLDS.copy()
        
        # 메모리 정리 스레드 시작
        if PERFORMANCE_MONITORING.get('enable_memory_profiling', True):
            self._start_memory_cleanup_thread()
    
    def _start_memory_cleanup_thread(self):
        """메모리 정리 스레드 시작"""
        cleanup_thread = threading.Thread(
            target=self._memory_cleanup_worker,
            daemon=True
        )
        cleanup_thread.start()
        logger.info("🧹 메모리 정리 스레드 시작")
    
    def _memory_cleanup_worker(self):
        """메모리 정리 워커 스레드"""
        while True:
            try:
                time.sleep(self.memory_monitor['cleanup_interval'])
                self._perform_memory_cleanup()
                
            except Exception as e:
                logger.warning(f"⚠️ 메모리 정리 중 오류: {e}")
    
    def _perform_memory_cleanup(self):
        """메모리 정리 수행"""
        try:
            current_memory = self._get_current_memory_usage()
            
            if current_memory > self.memory_monitor['cleanup_threshold_mb']:
                logger.info(f"🧹 메모리 정리 시작: {current_memory:.1f}MB")
                
                # 가비지 컬렉션 수행
                gc.collect()
                
                # 오래된 성능 데이터 정리
                self._cleanup_old_performance_data()
                
                # 지표별 통계 정리
                self._cleanup_indicator_stats()
                
                after_memory = self._get_current_memory_usage()
                freed_memory = current_memory - after_memory
                
                logger.info(f"✅ 메모리 정리 완료: {freed_memory:.1f}MB 해제")
                
                self.quality_stats['memory_cleanups_performed'] += 1
                self.memory_monitor['last_cleanup_time'] = time.time()
        
        except Exception as e:
            logger.error(f"❌ 메모리 정리 실패: {e}")
    
    def _get_current_memory_usage(self) -> float:
        """현재 메모리 사용량 반환 (MB)"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            # 피크 메모리 업데이트
            if memory_mb > self.memory_monitor['peak_memory_mb']:
                self.memory_monitor['peak_memory_mb'] = memory_mb
            
            self.memory_monitor['current_memory_mb'] = memory_mb
            return memory_mb
            
        except Exception:
            return 0.0
    
    def _cleanup_old_performance_data(self):
        """오래된 성능 데이터 정리"""
        try:
            # 24시간 이전 데이터 제거
            cutoff_time = time.time() - 86400  # 24시간
            
            for indicator_name in list(self.indicator_stats.keys()):
                stats = self.indicator_stats[indicator_name]
                
                # 마지막 성공 시간이 24시간 이전이면 제거
                if stats.get('last_success'):
                    if isinstance(stats['last_success'], datetime):
                        last_success_timestamp = stats['last_success'].timestamp()
                        if last_success_timestamp < cutoff_time:
                            del self.indicator_stats[indicator_name]
                            logger.debug(f"🗑️ 오래된 지표 통계 제거: {indicator_name}")
            
        except Exception as e:
            logger.debug(f"성능 데이터 정리 중 오류: {e}")
    
    def _cleanup_indicator_stats(self):
        """지표별 통계 정리"""
        try:
            # 통계가 많은 지표들의 상세 데이터 압축
            for indicator_name, stats in self.indicator_stats.items():
                if stats.get('total_calculations', 0) > 1000:
                    # 상세 통계를 요약 통계로 압축
                    stats['compressed'] = True
                    stats.pop('detailed_times', None)  # 상세 시간 기록 제거
                    
        except Exception as e:
            logger.debug(f"지표 통계 정리 중 오류: {e}")
    
    def log_conversion_failure(self, ticker: str, column: str, original: Any, 
                              error: str, fallback_used: Any):
        """변환 실패 로깅 (메모리 최적화)"""
        try:
            # FIFO 방식으로 상세 이슈 저장
            issue = {
                'timestamp': datetime.now().isoformat(),
                'ticker': ticker,
                'column': column,
                'original_type': type(original).__name__,
                'error': str(error)[:100],  # 에러 메시지 길이 제한
                'fallback_used': str(fallback_used)[:50]  # 폴백 값 길이 제한
            }
            
            self.detailed_issues.append(issue)
            
            # 심각한 오류인 경우만 별도 로깅
            if '1970-01-01' in str(error) or 'datetime' in str(error).lower():
                logger.warning(f"⚠️ 중요 변환 실패: {ticker}.{column} - {error}")
            
        except Exception as e:
            logger.debug(f"변환 실패 로깅 중 오류: {e}")
    
    def log_api_response_quality(self, ticker: str, df: Any, api_params: Dict):
        """API 응답 품질 모니터링 (메모리 최적화)"""
        self.quality_stats['api_calls_total'] += 1
        
        if df is None or (hasattr(df, 'empty') and df.empty):
            self.log_conversion_failure(ticker, 'api_response', 'None', 
                                      'API 응답 없음', 'N/A')
            return False
        
        # 1970-01-01 오류 감지
        try:
            if hasattr(df, 'index') and len(df) > 0:
                first_date = df.index[0]
                if hasattr(first_date, 'year') and first_date.year == 1970:
                    self.quality_stats['api_calls_1970_errors'] += 1
                    
                    error_rate = (self.quality_stats['api_calls_1970_errors'] / 
                                self.quality_stats['api_calls_total']) * 100
                    
                    if error_rate > self.alert_thresholds['api_1970_error_rate']:
                        logger.error(f"🚨 {ticker} 1970-01-01 에러율 위험: {error_rate:.1f}%")
                    
                    return False
                    
        except Exception as e:
            logger.debug(f"API 품질 검사 중 오류: {e}")
        
        return True
    
    def log_indicator_calculation_quality(self, ticker: str, df: Any, 
                                        calculated_indicators: List[str], 
                                        essential_only: bool = True):
        """지표 계산 품질 모니터링 (메모리 최적화)"""
        self.quality_stats['indicator_calculations_total'] += 1
        
        try:
            from config import ESSENTIAL_TREND_INDICATORS, INDICATOR_MIN_PERIODS
            
            if essential_only:
                check_indicators = ESSENTIAL_TREND_INDICATORS
            else:
                check_indicators = set(INDICATOR_MIN_PERIODS.keys())
            
            total_expected = len(check_indicators)
            calculated_count = len([ind for ind in calculated_indicators 
                                  if ind in check_indicators])
            completion_rate = (calculated_count / total_expected) * 100
            
            if completion_rate < self.alert_thresholds['indicator_success_rate']:
                self.quality_stats['indicator_calculations_failed'] += 1
                
                missing_indicators = check_indicators - set(calculated_indicators)
                self.log_conversion_failure(ticker, 'indicators', 
                                          f'{total_expected} expected', 
                                          f'Only {calculated_count} calculated',
                                          str(missing_indicators)[:100])
                return False
            
            return True
            
        except Exception as e:
            logger.debug(f"지표 품질 검사 중 오류: {e}")
            return False
    
    def track_indicator_performance(self, indicator_name: str, calculation_time: float,
                                   success: bool = True, memory_usage_mb: Optional[float] = None):
        """지표별 성능 추적 (메모리 최적화)"""
        try:
            if indicator_name not in self.indicator_stats:
                self.indicator_stats[indicator_name] = {
                    'total_calculations': 0,
                    'successful_calculations': 0,
                    'total_time': 0.0,
                    'avg_time': 0.0,
                    'last_success': None,
                    'memory_usage_mb': 0.0
                }
            
            stats = self.indicator_stats[indicator_name]
            stats['total_calculations'] += 1
            
            if success:
                stats['successful_calculations'] += 1
                stats['total_time'] += calculation_time
                stats['avg_time'] = stats['total_time'] / stats['successful_calculations']
                stats['last_success'] = datetime.now()
                
                if memory_usage_mb is not None:
                    stats['memory_usage_mb'] = memory_usage_mb
                    
                    # 메모리 사용량이 높은 지표 알림
                    if memory_usage_mb > self.alert_thresholds['memory_usage_mb'] / 10:  # 100MB
                        logger.warning(f"🧠 {indicator_name} 높은 메모리 사용: {memory_usage_mb:.1f}MB")
            
            # 성공률 계산 및 알림
            success_rate = (stats['successful_calculations'] / stats['total_calculations']) * 100
            
            if (success_rate < self.alert_thresholds['indicator_success_rate'] and 
                stats['total_calculations'] >= 5):
                logger.warning(f"🚨 {indicator_name} 성공률 낮음: {success_rate:.1f}%")
            
        except Exception as e:
            logger.debug(f"지표 성능 추적 중 오류: {e}")
    
    def get_memory_report(self) -> Dict[str, Any]:
        """메모리 사용량 보고서 반환"""
        try:
            current_memory = self._get_current_memory_usage()
            
            return {
                'current_memory_mb': current_memory,
                'peak_memory_mb': self.memory_monitor['peak_memory_mb'],
                'memory_limit_mb': MEMORY_LIMITS['MAX_SINGLE_PROCESS_MB'],
                'memory_utilization_percent': (current_memory / MEMORY_LIMITS['MAX_SINGLE_PROCESS_MB']) * 100,
                'detailed_issues_count': len(self.detailed_issues),
                'detailed_issues_limit': self.max_issues,
                'memory_cleanups_performed': self.quality_stats['memory_cleanups_performed'],
                'last_cleanup_time': self.memory_monitor['last_cleanup_time'],
                'indicator_stats_count': len(self.indicator_stats)
            }
            
        except Exception as e:
            logger.error(f"메모리 보고서 생성 중 오류: {e}")
            return {}
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """품질 요약 반환 (메모리 효율적)"""
        try:
            total_api_calls = self.quality_stats['api_calls_total']
            total_indicator_calcs = self.quality_stats['indicator_calculations_total']
            total_db_updates = self.quality_stats['db_updates_total']
            
            # 비율 계산
            api_error_rate = 0.0
            if total_api_calls > 0:
                api_error_rate = (self.quality_stats['api_calls_1970_errors'] / total_api_calls) * 100
            
            indicator_fail_rate = 0.0
            if total_indicator_calcs > 0:
                indicator_fail_rate = (self.quality_stats['indicator_calculations_failed'] / total_indicator_calcs) * 100
            
            db_fail_rate = 0.0
            if total_db_updates > 0:
                db_fail_rate = (self.quality_stats['db_updates_failed'] / total_db_updates) * 100
            
            # 메모리 정보 추가
            memory_report = self.get_memory_report()
            
            return {
                'api_calls_total': total_api_calls,
                'api_error_rate': api_error_rate,
                'indicator_calculations_total': total_indicator_calcs,
                'indicator_fail_rate': indicator_fail_rate,
                'db_updates_total': total_db_updates,
                'db_fail_rate': db_fail_rate,
                'memory_info': memory_report,
                'top_failing_indicators': self._get_top_failing_indicators(5),
                'recent_critical_issues': self._get_recent_critical_issues(10)
            }
            
        except Exception as e:
            logger.error(f"품질 요약 생성 중 오류: {e}")
            return {}
    
    def _get_top_failing_indicators(self, limit: int = 5) -> List[Dict]:
        """실패율이 높은 상위 지표 반환"""
        try:
            failing_indicators = []
            
            for indicator, stats in self.indicator_stats.items():
                if stats['total_calculations'] >= 5:  # 최소 5회 이상 계산된 지표만
                    success_rate = (stats['successful_calculations'] / stats['total_calculations']) * 100
                    failing_indicators.append({
                        'indicator': indicator,
                        'success_rate': success_rate,
                        'total_calculations': stats['total_calculations'],
                        'avg_time': stats.get('avg_time', 0.0)
                    })
            
            # 성공률 기준으로 정렬
            failing_indicators.sort(key=lambda x: x['success_rate'])
            return failing_indicators[:limit]
            
        except Exception as e:
            logger.debug(f"실패 지표 조회 중 오류: {e}")
            return []
    
    def _get_recent_critical_issues(self, limit: int = 10) -> List[Dict]:
        """최근 중요 이슈 반환"""
        try:
            critical_issues = []
            
            # 최근 이슈 중 중요한 것들 필터링
            for issue in list(self.detailed_issues)[-limit:]:
                if any(keyword in issue['error'].lower() for keyword in ['1970', 'datetime', 'memory', 'critical']):
                    critical_issues.append(issue)
            
            return critical_issues[-limit:]
            
        except Exception as e:
            logger.debug(f"중요 이슈 조회 중 오류: {e}")
            return []

# 전역 모니터 인스턴스 (싱글톤)
_global_monitor = None
_monitor_lock = threading.Lock()

def get_optimized_monitor() -> MemoryOptimizedDataQualityMonitor:
    """최적화된 모니터 인스턴스 반환 (싱글톤)"""
    global _global_monitor
    
    if _global_monitor is None:
        with _monitor_lock:
            if _global_monitor is None:
                _global_monitor = MemoryOptimizedDataQualityMonitor()
    
    return _global_monitor

def print_memory_status():
    """메모리 상태 출력"""
    monitor = get_optimized_monitor()
    report = monitor.get_memory_report()
    
    print("🧠 메모리 사용량 상태")
    print("=" * 40)
    print(f"💾 현재 메모리: {report.get('current_memory_mb', 0):.1f}MB")
    print(f"📈 피크 메모리: {report.get('peak_memory_mb', 0):.1f}MB")
    print(f"🎯 메모리 사용률: {report.get('memory_utilization_percent', 0):.1f}%")
    print(f"📋 저장된 이슈: {report.get('detailed_issues_count', 0)}/{report.get('detailed_issues_limit', 0)}")
    print(f"🧹 메모리 정리 횟수: {report.get('memory_cleanups_performed', 0)}")
    print(f"📊 지표 통계 개수: {report.get('indicator_stats_count', 0)}")
    print("=" * 40)

if __name__ == "__main__":
    # 메모리 최적화 모니터 테스트
    print("🧠 메모리 최적화 모니터 테스트")
    
    monitor = get_optimized_monitor()
    
    # 테스트 데이터 생성
    for i in range(10):
        monitor.track_indicator_performance(f'test_indicator_{i}', 0.1, True, 50.0)
    
    # 상태 출력
    print_memory_status()
    
    # 품질 요약 출력
    summary = monitor.get_quality_summary()
    print("\n📊 품질 요약:")
    for key, value in summary.items():
        if key != 'memory_info':
            print(f"  {key}: {value}") 