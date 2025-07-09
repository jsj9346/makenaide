# """
# Enhanced Data Validation System for Makenaide
# 통합 데이터 검증 시스템
# 
# 주요 기능:
# 1. OHLCV 데이터 품질 검증
# 2. Static Indicators 무결성 검증  
# 3. 실시간 이상 탐지
# 4. 자동 복구 메커니즘
# 5. 포괄적인 리포트 생성
# """
# 
# import pandas as pd
# import numpy as np
# import psycopg2
# from psycopg2.extras import RealDictCursor
# import logging
# from datetime import datetime, timedelta
# from typing import Dict, List, Tuple, Optional, Any
# import json
# from dataclasses import dataclass, asdict
# from utils import get_db_connection
# 
# # 로깅 설정
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# 
# @dataclass
# class ValidationResult:
#     """검증 결과를 담는 데이터 클래스"""
#     is_valid: bool
#     severity: str  # 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
#     message: str
#     details: Dict[str, Any]
#     timestamp: datetime
#     affected_records: int = 0
#     
# class EnhancedDataValidator:
#     """강화된 데이터 검증 시스템"""
#     
#     def __init__(self):
#         self.validation_results = []
#         self.critical_thresholds = {
#             'null_ratio_threshold': 0.1,  # 10% 이상 NULL값
#             'zero_ratio_threshold': 0.05,  # 5% 이상 0값
#             'logic_error_threshold': 0.01,  # 1% 이상 논리 오류
#             'staleness_hours': 24,  # 24시간 이상 업데이트 없음
#             'price_anomaly_factor': 10,  # 일반적 가격 범위 벗어남
#             'volume_anomaly_factor': 100  # 일반적 거래량 범위 벗어남
#         }
#         
#     def add_result(self, result: ValidationResult):
#         """검증 결과 추가"""
#         self.validation_results.append(result)
#         
#         # 심각한 오류는 즉시 로깅
#         if result.severity in ['ERROR', 'CRITICAL']:
#             logger.error(f"❌ {result.message}")
#         elif result.severity == 'WARNING':
#             logger.warning(f"⚠️ {result.message}")
#         else:
#             logger.info(f"✅ {result.message}")
#     
#     def validate_ohlcv_data_quality(self, days: int = 7) -> List[ValidationResult]:
#         """OHLCV 데이터 품질 종합 검증"""
#         logger.info(f"🔍 OHLCV 데이터 품질 검증 시작 (최근 {days}일)")
#         results = []
#         
#         try:
#             conn = get_db_connection()
#             cursor = conn.cursor(cursor_factory=RealDictCursor)
#             
#             # 1. 기본 통계 수집
#             cursor.execute(f"""
#                 SELECT 
#                     COUNT(*) as total_records,
#                     COUNT(DISTINCT ticker) as unique_tickers,
#                     MIN(date) as earliest_date,
#                     MAX(date) as latest_date
#                 FROM ohlcv 
#                 WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
#             """)
#             basic_stats = cursor.fetchone()
#             
#             if basic_stats['total_records'] == 0:
#                 results.append(ValidationResult(
#                     is_valid=False,
#                     severity='CRITICAL',
#                     message=f"최근 {days}일간 OHLCV 데이터가 없음",
#                     details=dict(basic_stats),
#                     timestamp=datetime.now()
#                 ))
#                 return results
#             
#             # 2. NULL 값 검증
#             cursor.execute(f"""
#                 SELECT 
#                     SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) as null_open,
#                     SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) as null_high,
#                     SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) as null_low,
#                     SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
#                     SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume,
#                     COUNT(*) as total_count
#                 FROM ohlcv 
#                 WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
#             """)
#             null_stats = cursor.fetchone()
#             
#             total_count = null_stats['total_count']
#             for column in ['open', 'high', 'low', 'close', 'volume']:
#                 null_count = null_stats[f'null_{column}']
#                 null_ratio = null_count / total_count if total_count > 0 else 0
#                 
#                 if null_ratio > self.critical_thresholds['null_ratio_threshold']:
#                     results.append(ValidationResult(
#                         is_valid=False,
#                         severity='ERROR',
#                         message=f"OHLCV {column} 컬럼에 과도한 NULL값 ({null_ratio:.1%})",
#                         details={'column': column, 'null_count': null_count, 'null_ratio': null_ratio},
#                         timestamp=datetime.now(),
#                         affected_records=null_count
#                     ))
#                 elif null_ratio > 0:
#                     results.append(ValidationResult(
#                         is_valid=True,
#                         severity='WARNING',
#                         message=f"OHLCV {column} 컬럼에 소량의 NULL값 ({null_ratio:.1%})",
#                         details={'column': column, 'null_count': null_count, 'null_ratio': null_ratio},
#                         timestamp=datetime.now(),
#                         affected_records=null_count
#                     ))
#             
#             # 3. 0값 검증
#             cursor.execute(f"""
#                 SELECT 
#                     SUM(CASE WHEN open = 0 THEN 1 ELSE 0 END) as zero_open,
#                     SUM(CASE WHEN high = 0 THEN 1 ELSE 0 END) as zero_high,
#                     SUM(CASE WHEN low = 0 THEN 1 ELSE 0 END) as zero_low,
#                     SUM(CASE WHEN close = 0 THEN 1 ELSE 0 END) as zero_close,
#                     COUNT(*) as total_count
#                 FROM ohlcv 
#                 WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
#             """)
#             zero_stats = cursor.fetchone()
#             
#             for column in ['open', 'high', 'low', 'close']:
#                 zero_count = zero_stats[f'zero_{column}']
#                 zero_ratio = zero_count / total_count if total_count > 0 else 0
#                 
#                 if zero_ratio > self.critical_thresholds['zero_ratio_threshold']:
#                     results.append(ValidationResult(
#                         is_valid=False,
#                         severity='ERROR',
#                         message=f"OHLCV {column} 컬럼에 과도한 0값 ({zero_ratio:.1%})",
#                         details={'column': column, 'zero_count': zero_count, 'zero_ratio': zero_ratio},
#                         timestamp=datetime.now(),
#                         affected_records=zero_count
#                     ))
#                 elif zero_count > 0:
#                     results.append(ValidationResult(
#                         is_valid=True,
#                         severity='WARNING',
#                         message=f"OHLCV {column} 컬럼에 0값 발견 ({zero_count}건)",
#                         details={'column': column, 'zero_count': zero_count, 'zero_ratio': zero_ratio},
#                         timestamp=datetime.now(),
#                         affected_records=zero_count
#                     ))
#             
#             # 4. 논리적 일관성 검증
#             cursor.execute(f"""
#                 SELECT COUNT(*) as logic_errors
#                 FROM ohlcv 
#                 WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
#                   AND (high < low OR close > high OR close < low OR open > high OR open < low)
#             """)
#             logic_errors = cursor.fetchone()['logic_errors']
#             logic_error_ratio = logic_errors / total_count if total_count > 0 else 0
#             
#             if logic_error_ratio > self.critical_thresholds['logic_error_threshold']:
#                 results.append(ValidationResult(
#                     is_valid=False,
#                     severity='ERROR',
#                     message=f"OHLCV 가격 논리 오류 과다 ({logic_error_ratio:.1%})",
#                     details={'logic_errors': logic_errors, 'error_ratio': logic_error_ratio},
#                     timestamp=datetime.now(),
#                     affected_records=logic_errors
#                 ))
#             elif logic_errors > 0:
#                 results.append(ValidationResult(
#                     is_valid=True,
#                     severity='WARNING',
#                     message=f"OHLCV 가격 논리 오류 발견 ({logic_errors}건)",
#                     details={'logic_errors': logic_errors, 'error_ratio': logic_error_ratio},
#                     timestamp=datetime.now(),
#                     affected_records=logic_errors
#                 ))
#             
#             # 5. 가격 이상값 검증
#             cursor.execute(f"""
#                 SELECT ticker, date, close
#                 FROM ohlcv 
#                 WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
#                   AND (close > 100000000 OR close < 0.00000001)
#                 ORDER BY close DESC
#                 LIMIT 10
#             """)
#             extreme_prices = cursor.fetchall()
#             
#             if extreme_prices:
#                 results.append(ValidationResult(
#                     is_valid=True,
#                     severity='WARNING',
#                     message=f"극값 가격 데이터 발견 ({len(extreme_prices)}건)",
#                     details={'extreme_prices': [dict(row) for row in extreme_prices]},
#                     timestamp=datetime.now(),
#                     affected_records=len(extreme_prices)
#                 ))
#             
#             # 6. 데이터 최신성 검증
#             cursor.execute("SELECT MAX(date) as latest_date FROM ohlcv")
#             latest_date = cursor.fetchone()['latest_date']
#             if latest_date:
#                 staleness_days = (datetime.now().date() - latest_date).days
#                 if staleness_days > 1:
#                     results.append(ValidationResult(
#                         is_valid=False,
#                         severity='ERROR',
#                         message=f"OHLCV 데이터가 {staleness_days}일 지연됨 (최신: {latest_date})",
#                         details={'latest_date': str(latest_date), 'staleness_days': staleness_days},
#                         timestamp=datetime.now()
#                     ))
#             
#             cursor.close()
#             conn.close()
#             
#         except Exception as e:
#             results.append(ValidationResult(
#                 is_valid=False,
#                 severity='CRITICAL',
#                 message=f"OHLCV 검증 중 오류: {str(e)}",
#                 details={'error': str(e)},
#                 timestamp=datetime.now()
#             ))
#         
#         return results
#     
#     def validate_static_indicators_integrity(self, days: int = 7) -> List[ValidationResult]:
#         """Static Indicators 무결성 검증"""
#         logger.info(f"🔍 Static Indicators 무결성 검증 시작 (최근 {days}일)")
#         results = []
#         
#         try:
#             conn = get_db_connection()
#             cursor = conn.cursor(cursor_factory=RealDictCursor)
#             
#             # 1. 기본 통계
#             cursor.execute("""
#                 SELECT 
#                     COUNT(*) as total_records,
#                     COUNT(DISTINCT ticker) as unique_tickers,
#                     MAX(updated_at) as latest_update
#                 FROM static_indicators
#             """)
#             basic_stats = cursor.fetchone()
#             
#             if basic_stats['total_records'] == 0:
#                 results.append(ValidationResult(
#                     is_valid=False,
#                     severity='CRITICAL',
#                     message="Static indicators 테이블이 비어있음",
#                     details=dict(basic_stats),
#                     timestamp=datetime.now()
#                 ))
#                 return results
#             
#             # 2. 각 지표별 고유값 분포 확인
#             critical_indicators = ['ma200_slope', 'nvt_relative', 'volume_change_7_30', 'adx', 'supertrend_signal']
#             
#             for indicator in critical_indicators:
#                 cursor.execute(f"""
#                     SELECT 
#                         COUNT(DISTINCT {indicator}) as unique_values,
#                         COUNT(*) as total_records,
#                         SUM(CASE WHEN {indicator} IS NULL THEN 1 ELSE 0 END) as null_count
#                     FROM static_indicators
#                 """)
#                 indicator_stats = cursor.fetchone()
#                 
#                 unique_values = indicator_stats['unique_values']
#                 total_records = indicator_stats['total_records']
#                 null_count = indicator_stats['null_count']
#                 
#                 # 모든 값이 동일한 경우 (고유값이 1개)
#                 if unique_values <= 1:
#                     results.append(ValidationResult(
#                         is_valid=False,
#                         severity='ERROR',
#                         message=f"Static indicator '{indicator}' 모든 값이 동일함 (고유값: {unique_values}개)",
#                         details={'indicator': indicator, 'unique_values': unique_values, 'total_records': total_records},
#                         timestamp=datetime.now(),
#                         affected_records=total_records
#                     ))
#                 elif unique_values < 5:  # 고유값이 너무 적은 경우
#                     results.append(ValidationResult(
#                         is_valid=True,
#                         severity='WARNING',
#                         message=f"Static indicator '{indicator}' 고유값이 적음 ({unique_values}개)",
#                         details={'indicator': indicator, 'unique_values': unique_values, 'total_records': total_records},
#                         timestamp=datetime.now()
#                     ))
#                 
#                 # NULL 값 확인
#                 null_ratio = null_count / total_records if total_records > 0 else 0
#                 if null_ratio > 0.1:  # 10% 이상 NULL
#                     results.append(ValidationResult(
#                         is_valid=False,
#                         severity='ERROR',
#                         message=f"Static indicator '{indicator}' 과도한 NULL값 ({null_ratio:.1%})",
#                         details={'indicator': indicator, 'null_count': null_count, 'null_ratio': null_ratio},
#                         timestamp=datetime.now(),
#                         affected_records=null_count
#                     ))
#             
#             # 3. 업데이트 최신성 확인
#             latest_update = basic_stats['latest_update']
#             if latest_update:
#                 staleness_hours = (datetime.now() - latest_update).total_seconds() / 3600
#                 if staleness_hours > self.critical_thresholds['staleness_hours']:
#                     results.append(ValidationResult(
#                         is_valid=False,
#                         severity='ERROR',
#                         message=f"Static indicators 업데이트 지연 ({staleness_hours:.1f}시간)",
#                         details={'latest_update': str(latest_update), 'staleness_hours': staleness_hours},
#                         timestamp=datetime.now()
#                     ))
#             
#             # 4. 티커별 일관성 확인
#             cursor.execute("""
#                 SELECT ticker, COUNT(*) as count
#                 FROM static_indicators
#                 GROUP BY ticker
#                 HAVING COUNT(*) > 1
#             """)
#             duplicate_tickers = cursor.fetchall()
#             
#             if duplicate_tickers:
#                 results.append(ValidationResult(
#                     is_valid=False,
#                     severity='WARNING',
#                     message=f"중복 티커 발견 ({len(duplicate_tickers)}개)",
#                     details={'duplicate_tickers': [dict(row) for row in duplicate_tickers]},
#                     timestamp=datetime.now(),
#                     affected_records=len(duplicate_tickers)
#                 ))
#             
#             cursor.close()
#             conn.close()
#             
#         except Exception as e:
#             results.append(ValidationResult(
#                 is_valid=False,
#                 severity='CRITICAL',
#                 message=f"Static indicators 검증 중 오류: {str(e)}",
#                 details={'error': str(e)},
#                 timestamp=datetime.now()
#             ))
#         
#         return results
#     
#     def validate_data_synchronization(self) -> List[ValidationResult]:
#         """OHLCV와 Static Indicators 간 데이터 동기화 검증"""
#         logger.info("🔍 데이터 동기화 검증 시작")
#         results = []
#         
#         try:
#             conn = get_db_connection()
#             cursor = conn.cursor(cursor_factory=RealDictCursor)
#             
#             # 1. 티커 일치성 확인
#             cursor.execute("""
#                 SELECT 
#                     (SELECT COUNT(DISTINCT ticker) FROM ohlcv) as ohlcv_tickers,
#                     (SELECT COUNT(DISTINCT ticker) FROM static_indicators) as static_tickers,
#                     (SELECT COUNT(DISTINCT o.ticker) 
#                      FROM ohlcv o 
#                      INNER JOIN static_indicators s ON o.ticker = s.ticker) as common_tickers
#             """)
#             sync_stats = cursor.fetchone()
#             
#             ohlcv_tickers = sync_stats['ohlcv_tickers']
#             static_tickers = sync_stats['static_tickers']
#             common_tickers = sync_stats['common_tickers']
#             
#             coverage_ratio = common_tickers / static_tickers if static_tickers > 0 else 0
#             
#             if coverage_ratio < 0.9:  # 90% 미만 커버리지
#                 results.append(ValidationResult(
#                     is_valid=False,
#                     severity='ERROR',
#                     message=f"데이터 동기화 문제: 커버리지 {coverage_ratio:.1%}",
#                     details={
#                         'ohlcv_tickers': ohlcv_tickers,
#                         'static_tickers': static_tickers,
#                         'common_tickers': common_tickers,
#                         'coverage_ratio': coverage_ratio
#                     },
#                     timestamp=datetime.now()
#                 ))
#             elif coverage_ratio < 1.0:
#                 results.append(ValidationResult(
#                     is_valid=True,
#                     severity='WARNING',
#                     message=f"일부 티커 동기화 누락: 커버리지 {coverage_ratio:.1%}",
#                     details={
#                         'ohlcv_tickers': ohlcv_tickers,
#                         'static_tickers': static_tickers,
#                         'common_tickers': common_tickers,
#                         'coverage_ratio': coverage_ratio
#                     },
#                     timestamp=datetime.now()
#                 ))
#             else:
#                 results.append(ValidationResult(
#                     is_valid=True,
#                     severity='INFO',
#                     message="데이터 동기화 정상",
#                     details={
#                         'ohlcv_tickers': ohlcv_tickers,
#                         'static_tickers': static_tickers,
#                         'common_tickers': common_tickers,
#                         'coverage_ratio': coverage_ratio
#                     },
#                     timestamp=datetime.now()
#                 ))
#             
#             cursor.close()
#             conn.close()
#             
#         except Exception as e:
#             results.append(ValidationResult(
#                 is_valid=False,
#                 severity='CRITICAL',
#                 message=f"동기화 검증 중 오류: {str(e)}",
#                 details={'error': str(e)},
#                 timestamp=datetime.now()
#             ))
#         
#         return results
#     
#     def run_comprehensive_validation(self, days: int = 7) -> Dict[str, Any]:
#         """종합 데이터 검증 실행"""
#         logger.info(f"🔍 종합 데이터 검증 시작 (최근 {days}일)")
#         
#         start_time = datetime.now()
#         self.validation_results = []
#         
#         # 1. OHLCV 데이터 품질 검증
#         ohlcv_results = self.validate_ohlcv_data_quality(days)
#         for result in ohlcv_results:
#             self.add_result(result)
#         
#         # 2. Static Indicators 무결성 검증
#         static_results = self.validate_static_indicators_integrity(days)
#         for result in static_results:
#             self.add_result(result)
#         
#         # 3. 데이터 동기화 검증
#         sync_results = self.validate_data_synchronization()
#         for result in sync_results:
#             self.add_result(result)
#         
#         end_time = datetime.now()
#         duration = (end_time - start_time).total_seconds()
#         
#         # 결과 요약
#         summary = self._generate_summary()
#         summary['validation_duration'] = duration
#         summary['timestamp'] = start_time.isoformat()
#         
#         logger.info(f"🔍 종합 검증 완료 ({duration:.2f}초)")
#         
#         return summary
#     
#     def _generate_summary(self) -> Dict[str, Any]:
#         """검증 결과 요약 생성"""
#         total_results = len(self.validation_results)
#         
#         severity_counts = {
#             'INFO': 0,
#             'WARNING': 0,
#             'ERROR': 0,
#             'CRITICAL': 0
#         }
#         
#         valid_count = 0
#         total_affected_records = 0
#         
#         for result in self.validation_results:
#             severity_counts[result.severity] += 1
#             if result.is_valid:
#                 valid_count += 1
#             total_affected_records += result.affected_records
#         
#         overall_health = "HEALTHY"
#         if severity_counts['CRITICAL'] > 0:
#             overall_health = "CRITICAL"
#         elif severity_counts['ERROR'] > 0:
#             overall_health = "UNHEALTHY"
#         elif severity_counts['WARNING'] > 0:
#             overall_health = "WARNING"
#         
#         return {
#             'overall_health': overall_health,
#             'total_checks': total_results,
#             'valid_checks': valid_count,
#             'validation_rate': valid_count / total_results if total_results > 0 else 0,
#             'severity_breakdown': severity_counts,
#             'total_affected_records': total_affected_records,
#             'detailed_results': [asdict(result) for result in self.validation_results]
#         }
#     
#     def generate_report(self, include_details: bool = True) -> str:
#         """검증 보고서 생성"""
#         if not self.validation_results:
#             return "검증 결과가 없습니다. run_comprehensive_validation()을 먼저 실행하세요."
#         
#         summary = self._generate_summary()
#         
#         report_lines = []
#         report_lines.append("=" * 80)
#         report_lines.append("🔍 Enhanced Data Validation Report")
#         report_lines.append(f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#         report_lines.append("=" * 80)
#         
#         # 전체 상태
#         health_emoji = {
#             'HEALTHY': '✅',
#             'WARNING': '⚠️',
#             'UNHEALTHY': '❌',
#             'CRITICAL': '🚨'
#         }
#         
#         report_lines.append(f"\n📊 전체 상태: {health_emoji.get(summary['overall_health'], '❓')} {summary['overall_health']}")
#         report_lines.append(f"📈 검증률: {summary['validation_rate']:.1%} ({summary['valid_checks']}/{summary['total_checks']})")
#         report_lines.append(f"📝 영향받은 레코드: {summary['total_affected_records']:,}개")
#         
#         # 심각도별 요약
#         report_lines.append(f"\n📋 심각도별 요약:")
#         for severity, count in summary['severity_breakdown'].items():
#             if count > 0:
#                 emoji = {'INFO': '✅', 'WARNING': '⚠️', 'ERROR': '❌', 'CRITICAL': '🚨'}[severity]
#                 report_lines.append(f"   {emoji} {severity}: {count}건")
#         
#         # 상세 결과
#         if include_details:
#             report_lines.append(f"\n📋 상세 검증 결과:")
#             
#             for result in self.validation_results:
#                 emoji = {'INFO': '✅', 'WARNING': '⚠️', 'ERROR': '❌', 'CRITICAL': '🚨'}[result.severity]
#                 report_lines.append(f"\n{emoji} [{result.severity}] {result.message}")
#                 
#                 if result.affected_records > 0:
#                     report_lines.append(f"   📊 영향받은 레코드: {result.affected_records:,}개")
#                 
#                 if result.details:
#                     key_details = []
#                     for key, value in result.details.items():
#                         if key not in ['error'] and not isinstance(value, (list, dict)):
#                             key_details.append(f"{key}={value}")
#                     if key_details:
#                         report_lines.append(f"   📝 세부사항: {', '.join(key_details)}")
#         
#         # 권장 조치사항
#         report_lines.append(f"\n🔧 권장 조치사항:")
#         
#         if summary['severity_breakdown']['CRITICAL'] > 0:
#             report_lines.append("   🚨 CRITICAL 이슈 즉시 해결 필요")
#         if summary['severity_breakdown']['ERROR'] > 0:
#             report_lines.append("   ❌ ERROR 이슈 우선 해결 필요")
#         if summary['severity_breakdown']['WARNING'] > 0:
#             report_lines.append("   ⚠️ WARNING 이슈 검토 및 개선 고려")
#         if summary['overall_health'] == 'HEALTHY':
#             report_lines.append("   ✅ 모든 검증 통과 - 정기 모니터링 지속")
#         
#         report_lines.append("=" * 80)
#         
#         return "\n".join(report_lines)
#     
#     def save_report(self, filename: Optional[str] = None) -> str:
#         """검증 보고서 파일로 저장"""
#         if filename is None:
#             filename = f"data_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
#         
#         report = self.generate_report()
#         
#         with open(filename, 'w', encoding='utf-8') as f:
#             f.write(report)
#         
#         logger.info(f"📁 검증 보고서 저장: {filename}")
#         return filename
# 
# def main():
#     """메인 실행 함수"""
#     validator = EnhancedDataValidator()
#     
#     # 종합 검증 실행
#     results = validator.run_comprehensive_validation(days=7)
#     
#     # 리포트 생성 및 저장
#     report = validator.generate_report(include_details=True)
#     filename = validator.save_report()
#     
#     print(f"✅ 검증 완료: {filename}")
#     print(f"📊 검증 결과: {results['summary']}")
# 
# 
# if __name__ == "__main__":
#     main()
