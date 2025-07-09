"""
📊 데이터 품질 모니터링 시스템 (data_quality_monitor.py)

Makenaide 프로젝트의 실시간 데이터 품질 모니터링 및 알림 시스템

주요 기능:
1. 실시간 품질 지표 추적
2. 동일값 패턴 탐지 및 알림
3. 데이터 이상값 감지
4. 품질 대시보드 생성
5. 자동 알림 및 보고서

작성자: Makenaide Development Team
작성일: 2025-01-27
버전: 1.0.0
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import json
import os
from collections import defaultdict, deque
import threading
import time
from utils import setup_logger, get_db_connection

# 로거 설정
logger = setup_logger()

class DataQualityMonitor:
    """
    📊 실시간 데이터 품질 모니터링 시스템
    
    주요 모니터링 항목:
    1. 동일값 검출률
    2. 데이터 완성도
    3. 값 분포 이상 탐지
    4. 시간별 품질 변화
    5. 티커별 품질 점수
    """
    
    def __init__(self, monitor_interval: int = 300):  # 5분 간격
        """모니터링 시스템 초기화"""
        self.monitor_interval = monitor_interval
        self.is_running = False
        self.monitor_thread = None
        
        # 품질 지표 저장소
        self.quality_metrics = {
            'static_indicators': defaultdict(lambda: deque(maxlen=100)),
            'ohlcv': defaultdict(lambda: deque(maxlen=100))
        }
        
        # 지표 성능 추적 저장소 추가
        self.indicator_performance = defaultdict(lambda: {
            'success_count': 0,
            'failure_count': 0,
            'total_time': 0.0,
            'avg_time': 0.0,
            'last_updated': None
        })
        
        # 알림 설정
        self.alert_thresholds = {
            'duplicate_rate': 0.3,      # 30% 이상 동일값
            'completion_rate': 0.7,     # 70% 미만 완성도
            'quality_score': 5.0,       # 5.0 미만 품질 점수
            'consecutive_failures': 3    # 연속 3회 실패
        }
        
        # 이상 패턴 탐지기
        self.anomaly_detectors = {}
        self.setup_anomaly_detection()
        
        # 보고서 설정
        self.reports_dir = "reports/quality"
        os.makedirs(self.reports_dir, exist_ok=True)
        
        logger.info("✅ Data Quality Monitor 초기화 완료")
    
    def setup_anomaly_detection(self):
        """이상 패턴 탐지 설정"""
        
        # static_indicators 지표별 정상 범위 정의
        self.normal_ranges = {
            'ma200_slope': (-50.0, 50.0),
            'nvt_relative': (0.1, 100.0),
            'volume_change_7_30': (0.01, 50.0),
            'adx': (0.0, 100.0),
            'supertrend_signal': (0.0, 1.0)
        }
        
        # 동일값 패턴 탐지기
        self.duplicate_patterns = {
            'exact_match': {},      # 정확한 동일값
            'cluster_match': {},    # 유사값 클러스터
            'sequence_match': {}    # 연속된 동일값
        }
    
    def start_monitoring(self):
        """모니터링 시작"""
        if self.is_running:
            logger.warning("⚠️ 모니터링이 이미 실행 중입니다")
            return
        
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info(f"✅ 데이터 품질 모니터링 시작 (간격: {self.monitor_interval}초)")
    
    def stop_monitoring(self):
        """모니터링 중지"""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        
        logger.info("⏹️ 데이터 품질 모니터링 중지")
    
    def _monitoring_loop(self):
        """모니터링 메인 루프"""
        while self.is_running:
            try:
                start_time = time.time()
                
                # 1. static_indicators 품질 검사
                static_metrics = self._check_static_indicators_quality()
                
                # 2. ohlcv 품질 검사  
                ohlcv_metrics = self._check_ohlcv_quality()
                
                # 3. 이상 패턴 탐지
                anomalies = self._detect_anomalies(static_metrics, ohlcv_metrics)
                
                # 4. 알림 처리
                self._process_alerts(static_metrics, ohlcv_metrics, anomalies)
                
                # 5. 품질 지표 저장
                self._store_quality_metrics(static_metrics, ohlcv_metrics)
                
                # 6. 보고서 생성 (매시간)
                if datetime.now().minute == 0:
                    self._generate_hourly_report()
                
                # 실행 시간 로깅
                execution_time = time.time() - start_time
                logger.debug(f"📊 품질 모니터링 사이클 완료 ({execution_time:.2f}초)")
                
                # 다음 사이클까지 대기
                time.sleep(max(0, self.monitor_interval - execution_time))
                
            except Exception as e:
                logger.error(f"❌ 모니터링 루프 오류: {e}")
                time.sleep(60)  # 오류 시 1분 대기
    
    def _check_static_indicators_quality(self) -> Dict[str, Any]:
        """static_indicators 테이블 품질 검사"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 1. 전체 데이터 개수 및 완성도
                cursor.execute("SELECT COUNT(*) FROM static_indicators")
                total_count = cursor.fetchone()[0]
                
                # 2. 각 컬럼별 NULL 비율
                columns = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'adx', 'supertrend_signal']
                null_rates = {}
                
                for col in columns:
                    cursor.execute(f"SELECT COUNT(*) FROM static_indicators WHERE {col} IS NULL")
                    null_count = cursor.fetchone()[0]
                    null_rates[col] = null_count / total_count if total_count > 0 else 0
                
                # 3. 동일값 검출
                duplicate_stats = self._detect_duplicates_in_db('static_indicators', columns)
                
                # 4. 값 분포 이상 탐지
                distribution_anomalies = self._check_value_distributions('static_indicators', columns)
                
                # 5. 품질 점수 계산
                quality_score = self._calculate_table_quality_score(
                    null_rates, duplicate_stats, distribution_anomalies
                )
                
                metrics = {
                    'timestamp': datetime.now(),
                    'table': 'static_indicators',
                    'total_records': total_count,
                    'null_rates': null_rates,
                    'duplicate_stats': duplicate_stats,
                    'distribution_anomalies': distribution_anomalies,
                    'quality_score': quality_score,
                    'completion_rate': 1.0 - sum(null_rates.values()) / len(null_rates)
                }
                
                logger.debug(f"📊 static_indicators 품질: {quality_score:.1f}/10 (완성도: {metrics['completion_rate']:.1%})")
                return metrics
                
        except Exception as e:
            logger.error(f"❌ static_indicators 품질 검사 실패: {e}")
            return {'timestamp': datetime.now(), 'table': 'static_indicators', 'error': str(e)}
    
    def _check_ohlcv_quality(self) -> Dict[str, Any]:
        """ohlcv 테이블 품질 검사"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 최근 7일 데이터 대상
                cursor.execute("""
                    SELECT COUNT(*) FROM ohlcv 
                    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                """)
                total_count = cursor.fetchone()[0]
                
                # OHLCV 기본 컬럼 검사
                columns = ['open', 'high', 'low', 'close', 'volume']
                null_rates = {}
                
                for col in columns:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM ohlcv 
                        WHERE {col} IS NULL OR {col} = 0
                        AND date >= CURRENT_DATE - INTERVAL '7 days'
                    """)
                    null_count = cursor.fetchone()[0]
                    null_rates[col] = null_count / total_count if total_count > 0 else 0
                
                # 가격 논리 검증 (high >= low, close >= 0 등)
                logic_errors = self._check_ohlcv_logic_errors()
                
                quality_score = self._calculate_ohlcv_quality_score(null_rates, logic_errors)
                
                metrics = {
                    'timestamp': datetime.now(),
                    'table': 'ohlcv',
                    'total_records': total_count,
                    'null_rates': null_rates,
                    'logic_errors': logic_errors,
                    'quality_score': quality_score,
                    'completion_rate': 1.0 - sum(null_rates.values()) / len(null_rates)
                }
                
                logger.debug(f"📊 ohlcv 품질: {quality_score:.1f}/10 (완성도: {metrics['completion_rate']:.1%})")
                return metrics
                
        except Exception as e:
            logger.error(f"❌ ohlcv 품질 검사 실패: {e}")
            return {'timestamp': datetime.now(), 'table': 'ohlcv', 'error': str(e)}
    
    def _detect_duplicates_in_db(self, table: str, columns: List[str]) -> Dict[str, Any]:
        """DB에서 동일값 패턴 탐지"""
        duplicate_stats = {}
        
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                for col in columns:
                    # 동일값 빈도 상위 10개 조회
                    cursor.execute(f"""
                        SELECT {col}, COUNT(*) as count 
                        FROM {table} 
                        WHERE {col} IS NOT NULL
                        GROUP BY {col}
                        HAVING COUNT(*) > 1
                        ORDER BY count DESC
                        LIMIT 10
                    """)
                    
                    results = cursor.fetchall()
                    
                    if results:
                        total_duplicates = sum(count for _, count in results)
                        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL")
                        total_records = cursor.fetchone()[0]
                        
                        duplicate_stats[col] = {
                            'duplicate_rate': total_duplicates / total_records if total_records > 0 else 0,
                            'top_duplicates': results[:5],  # 상위 5개만 저장
                            'total_duplicate_records': total_duplicates
                        }
                    else:
                        duplicate_stats[col] = {
                            'duplicate_rate': 0.0,
                            'top_duplicates': [],
                            'total_duplicate_records': 0
                        }
        
        except Exception as e:
            logger.error(f"❌ {table} 동일값 탐지 실패: {e}")
        
        return duplicate_stats
    
    def _check_value_distributions(self, table: str, columns: List[str]) -> Dict[str, Any]:
        """값 분포 이상 탐지"""
        anomalies = {}
        
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                for col in columns:
                    if col not in self.normal_ranges:
                        continue
                    
                    # 기본 통계
                    cursor.execute(f"""
                        SELECT 
                            MIN({col}) as min_val,
                            MAX({col}) as max_val,
                            AVG({col}) as avg_val,
                            STDDEV({col}) as std_val,
                            COUNT(*) as count
                        FROM {table}
                        WHERE {col} IS NOT NULL
                    """)
                    
                    stats = cursor.fetchone()
                    if not stats or stats[4] == 0:  # count = 0
                        continue
                    
                    min_val, max_val, avg_val, std_val, count = stats
                    normal_min, normal_max = self.normal_ranges[col]
                    
                    # 이상 감지
                    issues = []
                    
                    if min_val < normal_min:
                        issues.append(f"최소값 이상: {min_val} < {normal_min}")
                    
                    if max_val > normal_max:
                        issues.append(f"최대값 이상: {max_val} > {normal_max}")
                    
                    if std_val and std_val > abs(avg_val) * 2:  # 표준편차가 평균의 2배 이상
                        issues.append(f"높은 변동성: std={std_val:.3f}, avg={avg_val:.3f}")
                    
                    anomalies[col] = {
                        'stats': {'min': min_val, 'max': max_val, 'avg': avg_val, 'std': std_val},
                        'issues': issues,
                        'severity': len(issues)
                    }
        
        except Exception as e:
            logger.error(f"❌ {table} 분포 검사 실패: {e}")
        
        return anomalies
    
    def _check_ohlcv_logic_errors(self) -> Dict[str, int]:
        """OHLCV 논리 오류 검사"""
        errors = {}
        
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # High < Low 오류
                cursor.execute("""
                    SELECT COUNT(*) FROM ohlcv 
                    WHERE high < low AND date >= CURRENT_DATE - INTERVAL '7 days'
                """)
                errors['high_less_than_low'] = cursor.fetchone()[0]
                
                # Close가 High/Low 범위 밖
                cursor.execute("""
                    SELECT COUNT(*) FROM ohlcv 
                    WHERE (close > high OR close < low) 
                    AND date >= CURRENT_DATE - INTERVAL '7 days'
                """)
                errors['close_out_of_range'] = cursor.fetchone()[0]
                
                # 음수 가격
                cursor.execute("""
                    SELECT COUNT(*) FROM ohlcv 
                    WHERE (open <= 0 OR high <= 0 OR low <= 0 OR close <= 0)
                    AND date >= CURRENT_DATE - INTERVAL '7 days'
                """)
                errors['negative_prices'] = cursor.fetchone()[0]
                
                # 극단적 가격 변화 (전일 대비 50% 이상)
                cursor.execute("""
                    WITH price_changes AS (
                        SELECT ticker, date, close,
                               LAG(close) OVER (PARTITION BY ticker ORDER BY date) as prev_close
                        FROM ohlcv 
                        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                    )
                    SELECT COUNT(*) FROM price_changes
                    WHERE prev_close IS NOT NULL 
                    AND ABS(close - prev_close) / prev_close > 0.5
                """)
                errors['extreme_price_changes'] = cursor.fetchone()[0]
        
        except Exception as e:
            logger.error(f"❌ OHLCV 논리 검사 실패: {e}")
        
        return errors
    
    def _calculate_table_quality_score(self, null_rates: Dict[str, float], 
                                     duplicate_stats: Dict[str, Any], 
                                     distribution_anomalies: Dict[str, Any]) -> float:
        """테이블 품질 점수 계산"""
        try:
            base_score = 10.0
            
            # NULL 비율 감점 (각 컬럼당 최대 1점 감점)
            null_penalty = sum(min(rate * 2, 1.0) for rate in null_rates.values())
            
            # 동일값 비율 감점
            duplicate_penalty = 0
            for col, stats in duplicate_stats.items():
                duplicate_penalty += min(stats['duplicate_rate'] * 3, 2.0)
            
            # 분포 이상 감점
            distribution_penalty = sum(min(anomaly['severity'] * 0.5, 1.0) 
                                     for anomaly in distribution_anomalies.values())
            
            final_score = max(0.0, base_score - null_penalty - duplicate_penalty - distribution_penalty)
            return min(final_score, 10.0)
            
        except:
            return 5.0  # 기본 점수
    
    def _calculate_ohlcv_quality_score(self, null_rates: Dict[str, float], 
                                     logic_errors: Dict[str, int]) -> float:
        """OHLCV 품질 점수 계산"""
        try:
            base_score = 10.0
            
            # NULL/0 값 감점
            null_penalty = sum(min(rate * 3, 2.0) for rate in null_rates.values())
            
            # 논리 오류 감점
            logic_penalty = min(sum(logic_errors.values()) * 0.1, 3.0)
            
            final_score = max(0.0, base_score - null_penalty - logic_penalty)
            return min(final_score, 10.0)
            
        except:
            return 5.0
    
    def _detect_anomalies(self, static_metrics: Dict[str, Any], 
                         ohlcv_metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """이상 패턴 탐지"""
        anomalies = []
        
        try:
            # static_indicators 이상 탐지
            if 'error' not in static_metrics:
                if static_metrics['quality_score'] < self.alert_thresholds['quality_score']:
                    anomalies.append({
                        'type': 'low_quality',
                        'table': 'static_indicators',
                        'score': static_metrics['quality_score'],
                        'threshold': self.alert_thresholds['quality_score'],
                        'severity': 'high' if static_metrics['quality_score'] < 3.0 else 'medium'
                    })
                
                # 높은 동일값 비율 탐지
                for col, stats in static_metrics.get('duplicate_stats', {}).items():
                    if stats['duplicate_rate'] > self.alert_thresholds['duplicate_rate']:
                        anomalies.append({
                            'type': 'high_duplicates',
                            'table': 'static_indicators',
                            'column': col,
                            'rate': stats['duplicate_rate'],
                            'threshold': self.alert_thresholds['duplicate_rate'],
                            'severity': 'high' if stats['duplicate_rate'] > 0.5 else 'medium'
                        })
            
            # ohlcv 이상 탐지
            if 'error' not in ohlcv_metrics:
                logic_errors = ohlcv_metrics.get('logic_errors', {})
                total_errors = sum(logic_errors.values())
                
                if total_errors > 10:  # 임계값: 10개 이상 논리 오류
                    anomalies.append({
                        'type': 'logic_errors',
                        'table': 'ohlcv',
                        'error_count': total_errors,
                        'errors': logic_errors,
                        'severity': 'high' if total_errors > 50 else 'medium'
                    })
        
        except Exception as e:
            logger.error(f"❌ 이상 탐지 실패: {e}")
        
        return anomalies
    
    def _process_alerts(self, static_metrics: Dict[str, Any], 
                       ohlcv_metrics: Dict[str, Any], 
                       anomalies: List[Dict[str, Any]]):
        """알림 처리"""
        try:
            if not anomalies:
                return
            
            # 심각도별 분류
            high_severity = [a for a in anomalies if a.get('severity') == 'high']
            medium_severity = [a for a in anomalies if a.get('severity') == 'medium']
            
            # 고심각도 알림
            if high_severity:
                self._send_alert('HIGH', high_severity, static_metrics, ohlcv_metrics)
            
            # 중간 심각도 알림 (1시간에 한 번만)
            if medium_severity and datetime.now().minute == 0:
                self._send_alert('MEDIUM', medium_severity, static_metrics, ohlcv_metrics)
        
        except Exception as e:
            logger.error(f"❌ 알림 처리 실패: {e}")
    
    def _send_alert(self, severity: str, anomalies: List[Dict[str, Any]], static_metrics: Dict[str, Any], ohlcv_metrics: Dict[str, Any]):
        """알림 발송"""
        try:
            alert_message = f"🚨 [{severity}] 데이터 품질 알림\n\n"
            alert_message += f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            alert_message += f"탐지된 이상: {len(anomalies)}개\n\n"
            
            for anomaly in anomalies:
                alert_message += f"• {anomaly['type']}: {anomaly.get('table', 'unknown')}\n"
                if 'column' in anomaly:
                    alert_message += f"  컬럼: {anomaly['column']}\n"
                if 'rate' in anomaly:
                    alert_message += f"  비율: {anomaly['rate']:.1%}\n"
                if 'score' in anomaly:
                    alert_message += f"  점수: {anomaly['score']:.1f}/10\n"
                alert_message += "\n"
            
            # 품질 요약
            static_score = static_metrics.get('quality_score', 0)
            ohlcv_score = ohlcv_metrics.get('quality_score', 0)
            alert_message += f"전체 품질 점수:\n"
            alert_message += f"• static_indicators: {static_score:.1f}/10\n"
            alert_message += f"• ohlcv: {ohlcv_score:.1f}/10\n"
            
            # 로그로 알림 (실제 환경에서는 Slack, 이메일 등으로 확장 가능)
            logger.warning(alert_message)
            
            # 알림 파일로 저장
            alert_file = os.path.join(self.reports_dir, f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            with open(alert_file, 'w', encoding='utf-8') as f:
                f.write(alert_message)
        
        except Exception as e:
            logger.error(f"❌ 알림 발송 실패: {e}")
    
    def _store_quality_metrics(self, static_metrics: Dict[str, Any], ohlcv_metrics: Dict[str, Any]):
        """품질 지표 저장"""
        try:
            # 메모리 저장
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.quality_metrics['static_indicators']['scores'].append({
                'timestamp': timestamp,
                'quality_score': static_metrics.get('quality_score', 0),
                'completion_rate': static_metrics.get('completion_rate', 0)
            })
            
            self.quality_metrics['ohlcv']['scores'].append({
                'timestamp': timestamp,
                'quality_score': ohlcv_metrics.get('quality_score', 0),
                'completion_rate': ohlcv_metrics.get('completion_rate', 0)
            })
            
        except Exception as e:
            logger.error(f"❌ 품질 지표 저장 실패: {e}")
    
    def _generate_hourly_report(self):
        """시간별 품질 보고서 생성"""
        try:
            report_time = datetime.now()
            report_file = os.path.join(
                self.reports_dir, 
                f"quality_report_{report_time.strftime('%Y%m%d_%H00')}.json"
            )
            
            # 최근 1시간 데이터 수집
            recent_static = list(self.quality_metrics['static_indicators']['scores'])[-12:]  # 5분*12 = 1시간
            recent_ohlcv = list(self.quality_metrics['ohlcv']['scores'])[-12:]
            
            report = {
                'timestamp': report_time.isoformat(),
                'period': '1_hour',
                'static_indicators': {
                    'avg_quality_score': np.mean([s['quality_score'] for s in recent_static]) if recent_static else 0,
                    'avg_completion_rate': np.mean([s['completion_rate'] for s in recent_static]) if recent_static else 0,
                    'trend': 'improving' if len(recent_static) >= 2 and recent_static[-1]['quality_score'] > recent_static[0]['quality_score'] else 'declining'
                },
                'ohlcv': {
                    'avg_quality_score': np.mean([s['quality_score'] for s in recent_ohlcv]) if recent_ohlcv else 0,
                    'avg_completion_rate': np.mean([s['completion_rate'] for s in recent_ohlcv]) if recent_ohlcv else 0,
                    'trend': 'improving' if len(recent_ohlcv) >= 2 and recent_ohlcv[-1]['quality_score'] > recent_ohlcv[0]['quality_score'] else 'declining'
                }
            }
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            logger.info(f"📊 시간별 품질 보고서 생성: {report_file}")
            
        except Exception as e:
            logger.error(f"❌ 시간별 보고서 생성 실패: {e}")
    
    def get_current_status(self) -> Dict[str, Any]:
        """현재 품질 상태 조회"""
        try:
            static_recent = list(self.quality_metrics['static_indicators']['scores'])
            ohlcv_recent = list(self.quality_metrics['ohlcv']['scores'])
            
            status = {
                'monitoring_active': self.is_running,
                'last_update': datetime.now().isoformat(),
                'static_indicators': {
                    'latest_score': static_recent[-1]['quality_score'] if static_recent else 0,
                    'latest_completion': static_recent[-1]['completion_rate'] if static_recent else 0,
                    'data_points': len(static_recent)
                },
                'ohlcv': {
                    'latest_score': ohlcv_recent[-1]['quality_score'] if ohlcv_recent else 0,
                    'latest_completion': ohlcv_recent[-1]['completion_rate'] if ohlcv_recent else 0,
                    'data_points': len(ohlcv_recent)
                }
            }
            
            return status
            
        except Exception as e:
            logger.error(f"❌ 상태 조회 실패: {e}")
            return {'error': str(e)}
    
    def generate_dashboard_data(self) -> Dict[str, Any]:
        """대시보드용 데이터 생성"""
        try:
            # 최근 24시간 데이터 (5분 간격 * 288 = 24시간)
            static_data = list(self.quality_metrics['static_indicators']['scores'])[-288:]
            ohlcv_data = list(self.quality_metrics['ohlcv']['scores'])[-288:]
            
            dashboard = {
                'generated_at': datetime.now().isoformat(),
                'charts': {
                    'quality_scores': {
                        'static_indicators': [
                            {'x': item['timestamp'], 'y': item['quality_score']} 
                            for item in static_data
                        ],
                        'ohlcv': [
                            {'x': item['timestamp'], 'y': item['quality_score']} 
                            for item in ohlcv_data
                        ]
                    },
                    'completion_rates': {
                        'static_indicators': [
                            {'x': item['timestamp'], 'y': item['completion_rate']} 
                            for item in static_data
                        ],
                        'ohlcv': [
                            {'x': item['timestamp'], 'y': item['completion_rate']} 
                            for item in ohlcv_data
                        ]
                    }
                },
                'summary': {
                    'avg_quality_24h': {
                        'static_indicators': np.mean([s['quality_score'] for s in static_data]) if static_data else 0,
                        'ohlcv': np.mean([s['quality_score'] for s in ohlcv_data]) if ohlcv_data else 0
                    },
                    'avg_completion_24h': {
                        'static_indicators': np.mean([s['completion_rate'] for s in static_data]) if static_data else 0,
                        'ohlcv': np.mean([s['completion_rate'] for s in ohlcv_data]) if ohlcv_data else 0
                    }
                }
            }
            
            return dashboard
            
        except Exception as e:
            logger.error(f"❌ 대시보드 데이터 생성 실패: {e}")
            return {'error': str(e)}

    def track_indicator_performance(self, indicator_name: str, calculation_time: float, success: bool = True):
        """
        지표 계산 성능을 추적하는 메서드
        
        Args:
            indicator_name (str): 지표 이름
            calculation_time (float): 계산 소요 시간 (초)
            success (bool): 계산 성공 여부
        """
        try:
            if indicator_name not in self.indicator_performance:
                self.indicator_performance[indicator_name] = {
                    'success_count': 0,
                    'failure_count': 0,
                    'total_time': 0.0,
                    'avg_time': 0.0,
                    'last_updated': datetime.now()
                }
            
            perf = self.indicator_performance[indicator_name]
            
            if success:
                perf['success_count'] += 1
            else:
                perf['failure_count'] += 1
            
            perf['total_time'] += calculation_time
            perf['avg_time'] = perf['total_time'] / (perf['success_count'] + perf['failure_count'])
            perf['last_updated'] = datetime.now()
            
            # 성능 임계값 체크 및 알림
            self._check_indicator_performance_thresholds(indicator_name, perf)
            
        except Exception as e:
            logger.error(f"❌ 지표 성능 추적 실패 ({indicator_name}): {e}")
    
    def _check_indicator_performance_thresholds(self, indicator_name: str, performance: Dict[str, Any]):
        """지표 성능 임계값 체크 및 알림"""
        try:
            total_attempts = performance['success_count'] + performance['failure_count']
            if total_attempts < 10:  # 최소 10회 시도 후 체크
                return
            
            failure_rate = performance['failure_count'] / total_attempts
            avg_time = performance['avg_time']
            
            # 실패율이 20% 이상이거나 평균 계산 시간이 1초 이상인 경우 알림
            if failure_rate > 0.2 or avg_time > 1.0:
                alert_msg = f"⚠️ 지표 성능 경고: {indicator_name}\n"
                alert_msg += f"   실패율: {failure_rate:.1%}\n"
                alert_msg += f"   평균 시간: {avg_time:.3f}초\n"
                alert_msg += f"   총 시도: {total_attempts}회"
                
                logger.warning(alert_msg)
                
        except Exception as e:
            logger.error(f"❌ 성능 임계값 체크 실패: {e}")
    
    def get_indicator_performance_summary(self) -> Dict[str, Any]:
        """지표 성능 요약 조회"""
        try:
            summary = {
                'total_indicators': len(self.indicator_performance),
                'indicators': {},
                'overall_stats': {
                    'total_success': 0,
                    'total_failure': 0,
                    'avg_calculation_time': 0.0
                }
            }
            
            total_success = 0
            total_failure = 0
            total_time = 0.0
            
            for indicator_name, perf in self.indicator_performance.items():
                total_attempts = perf['success_count'] + perf['failure_count']
                if total_attempts > 0:
                    success_rate = perf['success_count'] / total_attempts
                    failure_rate = perf['failure_count'] / total_attempts
                else:
                    success_rate = 0.0
                    failure_rate = 0.0
                
                summary['indicators'][indicator_name] = {
                    'success_count': perf['success_count'],
                    'failure_count': perf['failure_count'],
                    'success_rate': success_rate,
                    'failure_rate': failure_rate,
                    'avg_time': perf['avg_time'],
                    'last_updated': perf['last_updated'].isoformat() if perf['last_updated'] else None
                }
                
                total_success += perf['success_count']
                total_failure += perf['failure_count']
                total_time += perf['total_time']
            
            total_attempts = total_success + total_failure
            if total_attempts > 0:
                summary['overall_stats']['total_success'] = total_success
                summary['overall_stats']['total_failure'] = total_failure
                summary['overall_stats']['avg_calculation_time'] = total_time / total_attempts
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 지표 성능 요약 조회 실패: {e}")
            return {'error': str(e)}
    
    def log_indicator_calculation_quality(self, ticker: str, df: pd.DataFrame, available_indicators: list):
        """
        지표 계산 품질을 로깅하는 메서드
        
        Args:
            ticker (str): 티커명
            df (pd.DataFrame): 계산된 지표가 포함된 데이터프레임
            available_indicators (list): 사용 가능한 지표 목록
        """
        try:
            if df is None or df.empty:
                logger.warning(f"⚠️ {ticker} 지표 품질 로깅: 데이터프레임이 비어있음")
                return
            
            # 기본 품질 지표 계산
            total_indicators = len(available_indicators)
            valid_indicators = 0
            null_counts = {}
            
            for indicator in available_indicators:
                if indicator in df.columns:
                    null_count = df[indicator].isna().sum()
                    total_count = len(df[indicator])
                    
                    if total_count > 0:
                        null_ratio = null_count / total_count
                        null_counts[indicator] = {
                            'null_count': null_count,
                            'total_count': total_count,
                            'null_ratio': null_ratio
                        }
                        
                        # 유효한 지표 판단 (null 비율이 50% 미만)
                        if null_ratio < 0.5:
                            valid_indicators += 1
            
            # 품질 점수 계산 (0-10)
            quality_score = (valid_indicators / total_indicators) * 10 if total_indicators > 0 else 0
            
            # 로깅
            logger.info(f"📊 {ticker} 지표 계산 품질: {quality_score:.1f}/10")
            logger.info(f"   - 총 지표: {total_indicators}개")
            logger.info(f"   - 유효 지표: {valid_indicators}개")
            logger.info(f"   - 유효율: {(valid_indicators/total_indicators*100):.1f}%" if total_indicators > 0 else "0%")
            
            # 문제가 있는 지표들 로깅
            problematic_indicators = []
            for indicator, stats in null_counts.items():
                if stats['null_ratio'] > 0.3:  # 30% 이상 null
                    problematic_indicators.append(f"{indicator}({stats['null_ratio']:.1%})")
            
            if problematic_indicators:
                logger.warning(f"   - 문제 지표: {', '.join(problematic_indicators)}")
            
            # 품질 지표 저장
            self._store_calculation_quality(ticker, quality_score, valid_indicators, total_indicators)
            
        except Exception as e:
            logger.error(f"❌ {ticker} 지표 계산 품질 로깅 실패: {e}")
    
    def _store_calculation_quality(self, ticker: str, quality_score: float, valid_count: int, total_count: int):
        """계산 품질 정보 저장"""
        try:
            # 메모리에 품질 정보 저장
            if not hasattr(self, 'calculation_quality'):
                self.calculation_quality = {}
            
            self.calculation_quality[ticker] = {
                'timestamp': datetime.now(),
                'quality_score': quality_score,
                'valid_indicators': valid_count,
                'total_indicators': total_count,
                'validity_ratio': valid_count / total_count if total_count > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"❌ 계산 품질 저장 실패: {e}")
    
    def get_calculation_quality_summary(self) -> Dict[str, Any]:
        """계산 품질 요약 조회"""
        try:
            if not hasattr(self, 'calculation_quality') or not self.calculation_quality:
                return {'message': '품질 데이터가 없습니다'}
            
            total_tickers = len(self.calculation_quality)
            avg_quality = sum(data['quality_score'] for data in self.calculation_quality.values()) / total_tickers
            avg_validity = sum(data['validity_ratio'] for data in self.calculation_quality.values()) / total_tickers
            
            summary = {
                'total_tickers': total_tickers,
                'average_quality_score': avg_quality,
                'average_validity_ratio': avg_validity,
                'ticker_details': {}
            }
            
            # 티커별 상세 정보
            for ticker, data in self.calculation_quality.items():
                summary['ticker_details'][ticker] = {
                    'quality_score': data['quality_score'],
                    'valid_indicators': data['valid_indicators'],
                    'total_indicators': data['total_indicators'],
                    'validity_ratio': data['validity_ratio'],
                    'timestamp': data['timestamp'].isoformat()
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 계산 품질 요약 조회 실패: {e}")
            return {'error': str(e)}

    def log_api_response_quality(self, ticker: str, df: Any, api_params: Dict):
        """
        API 응답 품질 모니터링
        
        Args:
            ticker (str): 티커명
            df (Any): API 응답 데이터프레임
            api_params (Dict): API 호출 파라미터
            
        Returns:
            bool: 품질 검사 통과 여부
        """
        try:
            # API 호출 통계 업데이트
            if not hasattr(self, 'api_call_stats'):
                self.api_call_stats = {
                    'total_calls': 0,
                    'successful_calls': 0,
                    'failed_calls': 0,
                    'empty_responses': 0,
                    'invalid_responses': 0
                }
            
            self.api_call_stats['total_calls'] += 1
            
            # 기본 응답 검증
            if df is None:
                self.api_call_stats['failed_calls'] += 1
                logger.warning(f"⚠️ {ticker} API 응답 None")
                return False
                
            if hasattr(df, 'empty') and df.empty:
                self.api_call_stats['empty_responses'] += 1
                logger.warning(f"⚠️ {ticker} API 응답 빈 DataFrame")
                return False
            
            # 1970-01-01 오류 감지
            if hasattr(df, 'index') and len(df) > 0:
                try:
                    first_date = df.index[0]
                    if hasattr(first_date, 'year') and first_date.year == 1970:
                        self.api_call_stats['invalid_responses'] += 1
                        logger.error(f"🚨 {ticker} 1970-01-01 오류 감지")
                        return False
                except Exception as e:
                    logger.debug(f"API 날짜 검증 중 오류: {e}")
            
            # 성공적인 응답
            self.api_call_stats['successful_calls'] += 1
            return True
            
        except Exception as e:
            logger.error(f"❌ API 품질 검사 중 오류: {e}")
            return False

    def get_quality_summary(self) -> Dict[str, Any]:
        """전체 품질 요약 조회"""
        try:
            summary = {
                'total_api_calls': 0,
                'api_1970_error_rate': 0.0,
                'total_indicator_calculations': 0,
                'indicator_failure_rate': 0.0,
                'total_db_updates': 0,
                'db_failure_rate': 0.0
            }
            
            # API 통계
            if hasattr(self, 'api_call_stats'):
                total_api = self.api_call_stats['total_calls']
                invalid_api = self.api_call_stats['invalid_responses']
                
                summary['total_api_calls'] = total_api
                if total_api > 0:
                    summary['api_1970_error_rate'] = (invalid_api / total_api) * 100
            
            # 지표 계산 통계
            if hasattr(self, 'indicator_performance'):
                total_calcs = sum(perf['success_count'] + perf['failure_count'] 
                                for perf in self.indicator_performance.values())
                failed_calcs = sum(perf['failure_count'] 
                                 for perf in self.indicator_performance.values())
                
                summary['total_indicator_calculations'] = total_calcs
                if total_calcs > 0:
                    summary['indicator_failure_rate'] = (failed_calcs / total_calcs) * 100
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 품질 요약 생성 중 오류: {e}")
            return {}

# 전역 모니터링 시스템 인스턴스
quality_monitor = DataQualityMonitor()

def start_quality_monitoring(interval: int = 300):
    """품질 모니터링 시작"""
    global quality_monitor
    quality_monitor.monitor_interval = interval
    quality_monitor.start_monitoring()

def stop_quality_monitoring():
    """품질 모니터링 중지"""
    global quality_monitor
    quality_monitor.stop_monitoring()

def get_quality_status():
    """현재 품질 상태 조회"""
    return quality_monitor.get_current_status()

def generate_quality_dashboard():
    """품질 대시보드 데이터 생성"""
    return quality_monitor.generate_dashboard_data()

# 시스템 초기화 로그
logger.info("✅ Data Quality Monitoring System 초기화 완료")
logger.info("   📊 실시간 품질 모니터링, 이상 탐지, 자동 알림 시스템 활성화")
logger.info("   🔧 사용법: start_quality_monitoring() 호출로 모니터링 시작")
