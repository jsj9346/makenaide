"""
Auto Recovery System for Makenaide Data Issues
자동 데이터 복구 시스템

주요 기능:
1. 0값 OHLCV 데이터 자동 복구
2. Static Indicators 재계산 및 업데이트
3. 논리적 오류 데이터 수정
4. 누락된 데이터 보완
5. 복구 과정 로깅 및 검증
"""

import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import pyupbit
import pandas as pd
import numpy as np
from utils import get_db_connection
from psycopg2.extras import RealDictCursor
import time
import json

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class RecoveryResult:
    """복구 결과 데이터 클래스"""
    recovery_type: str
    ticker: str
    affected_records: int
    success: bool
    error_message: Optional[str]
    duration_seconds: float
    timestamp: datetime

class AutoRecoverySystem:
    """자동 데이터 복구 시스템"""
    
    def __init__(self):
        self.recovery_results = []
        # API 키가 필요하지 않은 공개 함수만 사용
        
        # 복구 통계
        self.stats = {
            'total_attempts': 0,
            'successful_recoveries': 0,
            'failed_recoveries': 0,
            'zero_values_fixed': 0,
            'static_indicators_recalculated': 0,
            'logical_errors_fixed': 0
        }
        
    def log_recovery_result(self, result: RecoveryResult):
        """복구 결과 로깅"""
        self.recovery_results.append(result)
        
        if result.success:
            self.stats['successful_recoveries'] += 1
            logger.info(f"✅ {result.recovery_type} 복구 성공: {result.ticker} ({result.affected_records}건)")
        else:
            self.stats['failed_recoveries'] += 1
            logger.error(f"❌ {result.recovery_type} 복구 실패: {result.ticker} - {result.error_message}")
        
        self.stats['total_attempts'] += 1
    
    def fix_zero_ohlcv_values(self, ticker: str, limit_days: int = 30) -> RecoveryResult:
        """0값 OHLCV 데이터 수정"""
        start_time = time.time()
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # 0값 데이터 찾기
            cursor.execute("""
                SELECT date, open, high, low, close, volume
                FROM ohlcv 
                WHERE ticker = %s 
                  AND (open = 0 OR high = 0 OR low = 0 OR close = 0)
                  AND date >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY date
            """, (ticker, limit_days))
            
            zero_records = cursor.fetchall()
            
            if not zero_records:
                cursor.close()
                conn.close()
                return RecoveryResult(
                    recovery_type="zero_ohlcv_fix",
                    ticker=ticker,
                    affected_records=0,
                    success=True,
                    error_message=None,
                    duration_seconds=time.time() - start_time,
                    timestamp=datetime.now()
                )
            
            logger.info(f"🔧 {ticker}: {len(zero_records)}개 0값 레코드 복구 시작")
            
            fixed_count = 0
            
            for record in zero_records:
                date = record['date']
                
                try:
                    # PyUpbit에서 해당 날짜의 올바른 데이터 가져오기
                    df = pyupbit.get_ohlcv(ticker, interval="day", count=1, to=date.strftime('%Y%m%d'))
                    
                    if df is not None and not df.empty:
                        # 첫 번째 행의 데이터 사용
                        row = df.iloc[0]
                        
                        # 0이 아닌 값들만 업데이트
                        update_fields = []
                        update_values = []
                        
                        if record['open'] == 0 and row['open'] > 0:
                            update_fields.append("open = %s")
                            update_values.append(float(row['open']))
                        
                        if record['high'] == 0 and row['high'] > 0:
                            update_fields.append("high = %s")
                            update_values.append(float(row['high']))
                        
                        if record['low'] == 0 and row['low'] > 0:
                            update_fields.append("low = %s")
                            update_values.append(float(row['low']))
                        
                        if record['close'] == 0 and row['close'] > 0:
                            update_fields.append("close = %s")
                            update_values.append(float(row['close']))
                        
                        if update_fields:
                            update_values.extend([ticker, date])
                            update_query = f"""
                                UPDATE ohlcv 
                                SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                                WHERE ticker = %s AND date = %s
                            """
                            
                            cursor.execute(update_query, update_values)
                            fixed_count += 1
                            
                            logger.debug(f"✅ {ticker} {date}: 0값 데이터 복구됨")
                    
                except Exception as e:
                    logger.warning(f"⚠️ {ticker} {date} 복구 실패: {e}")
                    continue
                
                # API 호출 제한 방지
                time.sleep(0.1)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.stats['zero_values_fixed'] += fixed_count
            
            return RecoveryResult(
                recovery_type="zero_ohlcv_fix",
                ticker=ticker,
                affected_records=fixed_count,
                success=True,
                error_message=None,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            return RecoveryResult(
                recovery_type="zero_ohlcv_fix",
                ticker=ticker,
                affected_records=0,
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
    
    def recalculate_static_indicators(self, ticker: str) -> RecoveryResult:
        """Static Indicators 재계산"""
        start_time = time.time()
        
        try:
            # data_fetcher에서 계산 함수 import
            from data_fetcher import calculate_static_indicators
            
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # 현재 static indicators 가져오기
            cursor.execute("SELECT * FROM static_indicators WHERE ticker = %s", (ticker,))
            current_static = cursor.fetchone()
            
            if not current_static:
                cursor.close()
                conn.close()
                return RecoveryResult(
                    recovery_type="static_indicators_recalc",
                    ticker=ticker,
                    affected_records=0,
                    success=False,
                    error_message="Static indicators record not found",
                    duration_seconds=time.time() - start_time,
                    timestamp=datetime.now()
                )
            
            # OHLCV 데이터 가져오기 (최근 200일)
            cursor.execute("""
                SELECT date, open, high, low, close, volume
                FROM ohlcv 
                WHERE ticker = %s
                ORDER BY date DESC
                LIMIT 200
            """, (ticker,))
            
            ohlcv_data = cursor.fetchall()
            
            if len(ohlcv_data) < 30:  # 최소 30일 데이터 필요
                cursor.close()
                conn.close()
                return RecoveryResult(
                    recovery_type="static_indicators_recalc",
                    ticker=ticker,
                    affected_records=0,
                    success=False,
                    error_message="Insufficient OHLCV data for calculation",
                    duration_seconds=time.time() - start_time,
                    timestamp=datetime.now()
                )
            
            # DataFrame으로 변환
            df = pd.DataFrame(ohlcv_data)
            df = df.sort_values('date').reset_index(drop=True)
            df.set_index('date', inplace=True)
            
            # Static indicators 재계산
            new_indicators = calculate_static_indicators(df, ticker)
            
            if new_indicators:
                # 업데이트 쿼리 실행
                cursor.execute("""
                    UPDATE static_indicators 
                    SET 
                        ma200_slope = %s,
                        nvt_relative = %s,
                        volume_change_7_30 = %s,
                        adx = %s,
                        supertrend_signal = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE ticker = %s
                """, (
                    new_indicators.get('ma200_slope'),
                    new_indicators.get('nvt_relative'),
                    new_indicators.get('volume_change_7_30'),
                    new_indicators.get('adx'),
                    new_indicators.get('supertrend_signal'),
                    ticker
                ))
                
                conn.commit()
                
                logger.info(f"✅ {ticker} Static indicators 재계산 완료")
                self.stats['static_indicators_recalculated'] += 1
                
                cursor.close()
                conn.close()
                
                return RecoveryResult(
                    recovery_type="static_indicators_recalc",
                    ticker=ticker,
                    affected_records=1,
                    success=True,
                    error_message=None,
                    duration_seconds=time.time() - start_time,
                    timestamp=datetime.now()
                )
            else:
                cursor.close()
                conn.close()
                return RecoveryResult(
                    recovery_type="static_indicators_recalc",
                    ticker=ticker,
                    affected_records=0,
                    success=False,
                    error_message="Failed to calculate new indicators",
                    duration_seconds=time.time() - start_time,
                    timestamp=datetime.now()
                )
                
        except Exception as e:
            return RecoveryResult(
                recovery_type="static_indicators_recalc",
                ticker=ticker,
                affected_records=0,
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
    
    def fix_logical_errors(self, ticker: str, limit_days: int = 30) -> RecoveryResult:
        """논리적 오류 수정 (high < low, close > high 등)"""
        start_time = time.time()
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # 논리적 오류 찾기
            cursor.execute("""
                SELECT date, open, high, low, close, volume
                FROM ohlcv 
                WHERE ticker = %s 
                  AND date >= CURRENT_DATE - INTERVAL '%s days'
                  AND (high < low OR close > high OR close < low OR open > high OR open < low)
                ORDER BY date
            """, (ticker, limit_days))
            
            error_records = cursor.fetchall()
            
            if not error_records:
                cursor.close()
                conn.close()
                return RecoveryResult(
                    recovery_type="logical_errors_fix",
                    ticker=ticker,
                    affected_records=0,
                    success=True,
                    error_message=None,
                    duration_seconds=time.time() - start_time,
                    timestamp=datetime.now()
                )
            
            logger.info(f"🔧 {ticker}: {len(error_records)}개 논리 오류 레코드 복구 시작")
            
            fixed_count = 0
            
            for record in error_records:
                date = record['date']
                
                try:
                    # PyUpbit에서 올바른 데이터 가져오기
                    df = pyupbit.get_ohlcv(ticker, interval="day", count=1, to=date.strftime('%Y%m%d'))
                    
                    if df is not None and not df.empty:
                        row = df.iloc[0]
                        
                        # 논리적으로 올바른 값인지 확인
                        if (row['low'] <= row['open'] <= row['high'] and 
                            row['low'] <= row['close'] <= row['high'] and
                            row['low'] <= row['high']):
                            
                            cursor.execute("""
                                UPDATE ohlcv 
                                SET 
                                    open = %s,
                                    high = %s,
                                    low = %s,
                                    close = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE ticker = %s AND date = %s
                            """, (
                                float(row['open']),
                                float(row['high']),
                                float(row['low']),
                                float(row['close']),
                                ticker,
                                date
                            ))
                            
                            fixed_count += 1
                            logger.debug(f"✅ {ticker} {date}: 논리 오류 수정됨")
                    
                except Exception as e:
                    logger.warning(f"⚠️ {ticker} {date} 논리 오류 수정 실패: {e}")
                    continue
                
                time.sleep(0.1)  # API 호출 제한
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.stats['logical_errors_fixed'] += fixed_count
            
            return RecoveryResult(
                recovery_type="logical_errors_fix",
                ticker=ticker,
                affected_records=fixed_count,
                success=True,
                error_message=None,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            return RecoveryResult(
                recovery_type="logical_errors_fix",
                ticker=ticker,
                affected_records=0,
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
    
    def auto_recover_ticker(self, ticker: str) -> List[RecoveryResult]:
        """특정 티커의 모든 문제 자동 복구"""
        logger.info(f"🔧 {ticker} 자동 복구 시작")
        
        results = []
        
        # 1. 0값 OHLCV 데이터 수정
        result1 = self.fix_zero_ohlcv_values(ticker)
        results.append(result1)
        self.log_recovery_result(result1)
        
        # 2. 논리적 오류 수정
        result2 = self.fix_logical_errors(ticker)
        results.append(result2)
        self.log_recovery_result(result2)
        
        # 3. Static indicators 재계산 (OHLCV 수정 후)
        if result1.success or result2.success:
            time.sleep(1)  # 잠시 대기
            result3 = self.recalculate_static_indicators(ticker)
            results.append(result3)
            self.log_recovery_result(result3)
        
        return results
    
    def recover_all_problematic_tickers(self, limit_tickers: int = 20) -> Dict[str, List[RecoveryResult]]:
        """문제가 있는 모든 티커 자동 복구"""
        logger.info("🔧 전체 자동 복구 시작")
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 문제가 있는 티커들 찾기
            cursor.execute("""
                SELECT DISTINCT ticker
                FROM ohlcv 
                WHERE (open = 0 OR high = 0 OR low = 0 OR close = 0
                       OR high < low OR close > high OR close < low 
                       OR open > high OR open < low)
                  AND date >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY ticker
                LIMIT %s
            """, (limit_tickers,))
            
            problematic_tickers = [row[0] for row in cursor.fetchall()]
            
            # Static indicators 문제 티커도 추가
            cursor.execute("""
                SELECT ticker FROM static_indicators 
                WHERE ma200_slope = 0.0 AND nvt_relative = 1.0 AND volume_change_7_30 = 1.0
                LIMIT %s
            """, (limit_tickers,))
            
            static_problem_tickers = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            # 중복 제거
            all_problem_tickers = list(set(problematic_tickers + static_problem_tickers))
            
            logger.info(f"🔍 복구 대상 티커: {len(all_problem_tickers)}개")
            
            all_results = {}
            
            for ticker in all_problem_tickers:
                try:
                    results = self.auto_recover_ticker(ticker)
                    all_results[ticker] = results
                    
                    # 너무 빠른 API 호출 방지
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ {ticker} 복구 중 오류: {e}")
                    all_results[ticker] = [RecoveryResult(
                        recovery_type="full_recovery",
                        ticker=ticker,
                        affected_records=0,
                        success=False,
                        error_message=str(e),
                        duration_seconds=0,
                        timestamp=datetime.now()
                    )]
            
            return all_results
            
        except Exception as e:
            logger.error(f"❌ 전체 복구 중 오류: {e}")
            return {}
    
    def generate_recovery_report(self, results: Dict[str, List[RecoveryResult]]) -> str:
        """복구 보고서 생성"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("🔧 자동 복구 시스템 보고서")
        report_lines.append(f"📅 생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # 전체 통계
        total_tickers = len(results)
        successful_tickers = sum(1 for ticker_results in results.values() 
                               if any(r.success for r in ticker_results))
        
        report_lines.append(f"\n📊 전체 통계:")
        report_lines.append(f"   처리 티커: {total_tickers}개")
        report_lines.append(f"   성공 티커: {successful_tickers}개")
        report_lines.append(f"   성공률: {successful_tickers/total_tickers:.1%}" if total_tickers > 0 else "   성공률: 0%")
        
        # 세부 통계
        report_lines.append(f"\n📈 복구 유형별 통계:")
        report_lines.append(f"   0값 수정: {self.stats['zero_values_fixed']}건")
        report_lines.append(f"   논리 오류 수정: {self.stats['logical_errors_fixed']}건")
        report_lines.append(f"   Static 지표 재계산: {self.stats['static_indicators_recalculated']}건")
        
        # 티커별 상세 결과 (성공한 것들만)
        if successful_tickers > 0:
            report_lines.append(f"\n✅ 성공한 복구 작업:")
            for ticker, ticker_results in results.items():
                successful_results = [r for r in ticker_results if r.success and r.affected_records > 0]
                if successful_results:
                    report_lines.append(f"\n   📍 {ticker}:")
                    for result in successful_results:
                        report_lines.append(f"      ✅ {result.recovery_type}: {result.affected_records}건 복구")
        
        # 실패한 복구 작업
        failed_results = []
        for ticker, ticker_results in results.items():
            for result in ticker_results:
                if not result.success:
                    failed_results.append((ticker, result))
        
        if failed_results:
            report_lines.append(f"\n❌ 실패한 복구 작업 ({len(failed_results)}건):")
            for ticker, result in failed_results[:10]:  # 최대 10개만 표시
                report_lines.append(f"   ❌ {ticker} - {result.recovery_type}: {result.error_message}")
        
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
    def save_recovery_report(self, results: Dict[str, List[RecoveryResult]], filename: Optional[str] = None):
        """복구 보고서 파일로 저장"""
        if filename is None:
            filename = f"recovery_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        report = self.generate_recovery_report(results)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(report)
        
        # JSON 형태로도 저장
        json_filename = filename.replace('.txt', '.json')
        json_data = {
            'timestamp': datetime.now().isoformat(),
            'stats': self.stats,
            'results': {
                ticker: [
                    {
                        'recovery_type': r.recovery_type,
                        'affected_records': r.affected_records,
                        'success': r.success,
                        'error_message': r.error_message,
                        'duration_seconds': r.duration_seconds,
                        'timestamp': r.timestamp.isoformat()
                    } for r in ticker_results
                ] for ticker, ticker_results in results.items()
            }
        }
        
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📁 복구 보고서 저장: {filename}, {json_filename}")

def main():
    """메인 실행 함수"""
    recovery_system = AutoRecoverySystem()
    
    print("🔧 Makenaide 자동 복구 시스템")
    print("=" * 50)
    print("1. 단일 티커 복구")
    print("2. 전체 자동 복구 (문제 티커들)")
    print("3. 0값 데이터만 복구")
    print("4. Static Indicators만 재계산")
    print("5. 종료")
    
    while True:
        try:
            choice = input("\n선택하세요 (1-5): ").strip()
            
            if choice == '1':
                ticker = input("티커를 입력하세요 (예: KRW-BTC): ").strip()
                if ticker:
                    results = recovery_system.auto_recover_ticker(ticker)
                    print("\n" + recovery_system.generate_recovery_report({ticker: results}))
            
            elif choice == '2':
                limit = input("최대 복구할 티커 수 (기본값: 20): ").strip()
                limit = int(limit) if limit.isdigit() else 20
                
                print(f"🔧 최대 {limit}개 티커 자동 복구 시작...")
                results = recovery_system.recover_all_problematic_tickers(limit)
                
                if results:
                    report = recovery_system.generate_recovery_report(results)
                    print("\n" + report)
                    
                    # 보고서 저장
                    recovery_system.save_recovery_report(results)
                else:
                    print("❌ 복구할 수 있는 티커가 없습니다.")
            
            elif choice == '3':
                ticker = input("티커를 입력하세요 (예: KRW-BTC): ").strip()
                if ticker:
                    result = recovery_system.fix_zero_ohlcv_values(ticker)
                    recovery_system.log_recovery_result(result)
                    print(f"\n결과: {result}")
            
            elif choice == '4':
                ticker = input("티커를 입력하세요 (예: KRW-BTC): ").strip()
                if ticker:
                    result = recovery_system.recalculate_static_indicators(ticker)
                    recovery_system.log_recovery_result(result)
                    print(f"\n결과: {result}")
            
            elif choice == '5':
                break
            
            else:
                print("❓ 잘못된 선택입니다.")
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ 오류: {e}")
    
    print("\n👋 자동 복구 시스템이 종료되었습니다.")

if __name__ == "__main__":
    main()
