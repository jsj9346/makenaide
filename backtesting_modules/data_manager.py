#!/usr/bin/env python3
"""
백테스트 데이터 관리 모듈

백테스트용 데이터 스냅샷 생성, 조회, 관리 기능을 제공합니다.
기존 backtester.py의 BacktestDataManager 클래스를 분리했습니다.

Author: Backtesting Refactoring
Version: 1.0.0
"""

import pandas as pd
import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from pathlib import Path

# 로거 설정
logger = logging.getLogger(__name__)

class BacktestDataManager:
    """백테스트 전용 데이터 관리 클래스"""
    
    def __init__(self):
        from db_manager import DBManager
        self.db_manager = DBManager()
        
    def create_backtest_snapshot(self, session_name: str, period_days: int = 1000) -> Optional[str]:
        """
        백테스트용 데이터 스냅샷 생성
        
        Args:
            session_name: 백테스트 세션 이름
            period_days: 백테스트할 과거 일수
            
        Returns:
            str: 생성된 세션 ID (실패 시 None)
        """
        session_id = str(uuid.uuid4())
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 백테스트 기간 계산
                end_date = datetime.now()
                start_date = end_date - timedelta(days=period_days)
                
                # 1. 백테스트 세션 등록
                cursor.execute("""
                    INSERT INTO backtest_sessions (session_id, name, period_start, period_end, description)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    session_id,
                    session_name,
                    start_date.date(),
                    end_date.date(),
                    f"백테스트 기간: {period_days}일, 생성: {datetime.now().isoformat()}"
                ))
                
                # 2. 기존 백테스트 데이터 정리 (중복 방지)
                cursor.execute("""
                    DELETE FROM backtest_ohlcv 
                    WHERE date >= %s AND date <= %s
                """, (start_date.date(), end_date.date()))
                
                # 3. 운영 데이터를 백테스트 테이블로 복사
                cursor.execute("""
                    INSERT INTO backtest_ohlcv (ticker, date, open, high, low, close, volume)
                    SELECT ticker, date, open, high, low, close, volume
                    FROM ohlcv 
                    WHERE date >= %s AND date <= %s
                    ORDER BY ticker, date
                """, (start_date.date(), end_date.date()))
                
                copied_rows = cursor.rowcount
                
                logger.info(f"✅ 백테스트 스냅샷 생성 완료")
                logger.info(f"   - 세션 ID: {session_id}")
                logger.info(f"   - 세션명: {session_name}")  
                logger.info(f"   - 기간: {start_date.date()} ~ {end_date.date()}")
                logger.info(f"   - 복사된 레코드: {copied_rows:,}개")
                
            return session_id
            
        except Exception as e:
            logger.error(f"❌ 백테스트 스냅샷 생성 실패: {e}")
            return None
    
    def get_backtest_data(self, session_id: Optional[str] = None, ticker: Optional[str] = None, 
                         limit_days: Optional[int] = None) -> pd.DataFrame:
        """
        백테스트 데이터 조회
        
        Args:
            session_id: 백테스트 세션 ID (None이면 최신 데이터)
            ticker: 특정 티커 (None이면 전체)
            limit_days: 조회할 최근 일수
            
        Returns:
            pd.DataFrame: 백테스트 데이터
        """
        try:
            with self.db_manager.get_connection() as conn:
                base_query = """
                    SELECT ticker, date, open, high, low, close, volume, created_at
                    FROM backtest_ohlcv 
                    WHERE 1=1
                """
                params = []
                
                # 티커 필터
                if ticker:
                    base_query += " AND ticker = %s"
                    params.append(ticker)
                
                # 기간 필터
                if limit_days:
                    base_query += " AND date >= %s"
                    limit_date = datetime.now().date() - timedelta(days=limit_days)
                    params.append(limit_date)
                
                base_query += " ORDER BY ticker, date"
                
                df = pd.read_sql_query(base_query, conn, params=params)
                
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    logger.info(f"📊 백테스트 데이터 조회 완료: {len(df):,}개 레코드")
                else:
                    logger.warning("⚠️ 조회된 백테스트 데이터가 없습니다")
                
                return df
                
        except Exception as e:
            logger.error(f"❌ 백테스트 데이터 조회 실패: {e}")
            return pd.DataFrame()

    def list_active_sessions(self, limit: int = 10) -> List[Dict]:
        """활성 백테스트 세션 목록 조회"""
        try:
            with self.db_manager.get_connection() as conn:
                query = """
                    SELECT session_id, name, period_start, period_end, 
                           description, created_at, status
                    FROM backtest_sessions 
                    WHERE status = 'active'
                    ORDER BY created_at DESC
                    LIMIT %s
                """
                
                cursor = conn.cursor()
                cursor.execute(query, (limit,))
                results = cursor.fetchall()
                
                columns = ['session_id', 'name', 'period_start', 'period_end', 
                          'description', 'created_at', 'status']
                
                sessions = []
                for row in results:
                    session_dict = dict(zip(columns, row))
                    sessions.append(session_dict)
                
                logger.info(f"📋 활성 세션 조회 완료: {len(sessions)}개")
                return sessions
                
        except Exception as e:
            logger.error(f"❌ 세션 목록 조회 실패: {e}")
            return []

    def get_session_info(self, session_id: str) -> Optional[Dict]:
        """특정 세션 정보 조회"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT session_id, name, period_start, period_end, 
                           description, created_at, status
                    FROM backtest_sessions 
                    WHERE session_id = %s
                """, (session_id,))
                
                result = cursor.fetchone()
                if result:
                    columns = ['session_id', 'name', 'period_start', 'period_end', 
                              'description', 'created_at', 'status']
                    return dict(zip(columns, result))
                    
                return None
                
        except Exception as e:
            logger.error(f"❌ 세션 정보 조회 실패: {e}")
            return None

    def save_backtest_results(self, results: Dict, session_id: Optional[str] = None) -> bool:
        """백테스트 결과 저장"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 결과 테이블에 저장
                cursor.execute("""
                    INSERT INTO backtest_results (
                        session_id, strategy_name, combo_name, period_start, period_end,
                        win_rate, avg_return, mdd, total_trades, winning_trades, losing_trades,
                        kelly_fraction, kelly_1_2, b_value, swing_score, 
                        sharpe_ratio, sortino_ratio, profit_factor, parameters
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    session_id,
                    results.get('strategy_name'),
                    results.get('combo_name'),
                    results.get('period_start'),
                    results.get('period_end'),
                    results.get('win_rate'),
                    results.get('avg_return'),
                    results.get('mdd'),
                    results.get('total_trades'),
                    results.get('winning_trades'),
                    results.get('losing_trades'),
                    results.get('kelly_fraction'),
                    results.get('kelly_1_2'),
                    results.get('b_value'),
                    results.get('swing_score'),
                    results.get('sharpe_ratio'),
                    results.get('sortino_ratio'),
                    results.get('profit_factor'),
                    results.get('parameters', {})
                ))
                
                result_id = cursor.fetchone()[0]
                
                # 개별 거래 기록 저장
                if 'trades' in results and results['trades']:
                    for trade in results['trades']:
                        cursor.execute("""
                            INSERT INTO backtest_trades (
                                result_id, ticker, entry_date, exit_date,
                                entry_price, exit_price, quantity, pnl, 
                                return_pct, hold_days, strategy_signal
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            result_id,
                            trade.get('ticker'),
                            trade.get('entry_date'),
                            trade.get('exit_date'),
                            trade.get('entry_price'),
                            trade.get('exit_price'),
                            trade.get('quantity'),
                            trade.get('pnl'),
                            trade.get('return_pct'),
                            trade.get('hold_days'),
                            trade.get('strategy_signal')
                        ))
                
                logger.info(f"✅ 백테스트 결과 저장 완료 (ID: {result_id})")
                return True
                
        except Exception as e:
            logger.error(f"❌ 백테스트 결과 저장 실패: {e}")
            return False

    def get_backtest_results_from_db(self, session_id: Optional[str] = None, 
                                    strategy_name: Optional[str] = None,
                                    limit_days: Optional[int] = None) -> pd.DataFrame:
        """저장된 백테스트 결과 조회"""
        try:
            with self.db_manager.get_connection() as conn:
                base_query = """
                    SELECT br.*, bs.name as session_name
                    FROM backtest_results br
                    LEFT JOIN backtest_sessions bs ON br.session_id = bs.session_id
                    WHERE 1=1
                """
                params = []
                
                if session_id:
                    base_query += " AND br.session_id = %s"
                    params.append(session_id)
                    
                if strategy_name:
                    base_query += " AND br.strategy_name ILIKE %s"
                    params.append(f"%{strategy_name}%")
                    
                if limit_days:
                    base_query += " AND br.created_at >= %s"
                    limit_date = datetime.now() - timedelta(days=limit_days)
                    params.append(limit_date)
                
                base_query += " ORDER BY br.created_at DESC"
                
                df = pd.read_sql_query(base_query, conn, params=params)
                logger.info(f"📊 백테스트 결과 조회 완료: {len(df)}개")
                
                return df
                
        except Exception as e:
            logger.error(f"❌ 백테스트 결과 조회 실패: {e}")
            return pd.DataFrame()

    def cleanup_old_backtest_results(self, days_to_keep: int = 30) -> Dict:
        """오래된 백테스트 결과 정리"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cutoff_date = datetime.now() - timedelta(days=days_to_keep)
                
                # 오래된 거래 기록 삭제
                cursor.execute("""
                    DELETE FROM backtest_trades 
                    WHERE result_id IN (
                        SELECT id FROM backtest_results 
                        WHERE created_at < %s
                    )
                """, (cutoff_date,))
                deleted_trades = cursor.rowcount
                
                # 오래된 결과 삭제
                cursor.execute("""
                    DELETE FROM backtest_results 
                    WHERE created_at < %s
                """, (cutoff_date,))
                deleted_results = cursor.rowcount
                
                # 관련 세션 삭제
                cursor.execute("""
                    DELETE FROM backtest_sessions 
                    WHERE created_at < %s 
                    AND session_id NOT IN (
                        SELECT DISTINCT session_id FROM backtest_results 
                        WHERE session_id IS NOT NULL
                    )
                """, (cutoff_date,))
                deleted_sessions = cursor.rowcount
                
                logger.info(f"🧹 오래된 데이터 정리 완료")
                logger.info(f"   - 삭제된 결과: {deleted_results}개")
                logger.info(f"   - 삭제된 거래: {deleted_trades}개") 
                logger.info(f"   - 삭제된 세션: {deleted_sessions}개")
                
                return {
                    'deleted_results': deleted_results,
                    'deleted_trades': deleted_trades,
                    'deleted_sessions': deleted_sessions,
                    'cutoff_date': cutoff_date.isoformat()
                }
                
        except Exception as e:
            logger.error(f"❌ 데이터 정리 실패: {e}")
            return {'error': str(e)}

    def generate_backtest_analysis_report(self, session_id: Optional[str] = None, 
                                        output_format: str = "markdown") -> str:
        """백테스트 분석 리포트 생성"""
        try:
            results_df = self.get_backtest_results_from_db(session_id=session_id)
            
            if results_df.empty:
                return "⚠️ 분석할 백테스트 결과가 없습니다."
            
            # 기본 통계 계산
            total_strategies = len(results_df)
            avg_return = results_df['avg_return'].mean()
            avg_win_rate = results_df['win_rate'].mean()
            avg_mdd = results_df['mdd'].mean()
            
            # 최고 성과 전략
            best_strategy = results_df.loc[results_df['avg_return'].idxmax()]
            
            # 리포트 생성
            if output_format == "markdown":
                report = f"""# 백테스트 분석 리포트

## 📊 전체 통계
- **총 전략 수**: {total_strategies}개
- **평균 수익률**: {avg_return:.2%}
- **평균 승률**: {avg_win_rate:.2%}
- **평균 최대 낙폭**: {avg_mdd:.2%}

## 🏆 최고 성과 전략
- **전략명**: {best_strategy['strategy_name']}
- **수익률**: {best_strategy['avg_return']:.2%}
- **승률**: {best_strategy['win_rate']:.2%}
- **최대 낙폭**: {best_strategy['mdd']:.2%}
- **총 거래 수**: {best_strategy['total_trades']}

## 📈 상위 5개 전략

| 순위 | 전략명 | 수익률 | 승률 | 최대낙폭 | 거래수 |
|------|--------|--------|------|----------|--------|
"""
                
                # 상위 5개 전략 추가
                top_5 = results_df.nlargest(5, 'avg_return')
                for i, (_, strategy) in enumerate(top_5.iterrows(), 1):
                    report += f"| {i} | {strategy['strategy_name']} | {strategy['avg_return']:.2%} | {strategy['win_rate']:.2%} | {strategy['mdd']:.2%} | {strategy['total_trades']} |\n"
                
                report += f"\n---\n생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                
            else:  # JSON format
                import json
                report_data = {
                    'summary': {
                        'total_strategies': total_strategies,
                        'avg_return': float(avg_return),
                        'avg_win_rate': float(avg_win_rate),
                        'avg_mdd': float(avg_mdd)
                    },
                    'best_strategy': {
                        'name': best_strategy['strategy_name'],
                        'return': float(best_strategy['avg_return']),
                        'win_rate': float(best_strategy['win_rate']),
                        'mdd': float(best_strategy['mdd']),
                        'trades': int(best_strategy['total_trades'])
                    },
                    'generated_at': datetime.now().isoformat()
                }
                report = json.dumps(report_data, indent=2, ensure_ascii=False)
            
            return report
            
        except Exception as e:
            logger.error(f"❌ 리포트 생성 실패: {e}")
            return f"❌ 리포트 생성 중 오류 발생: {str(e)}"