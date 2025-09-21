#!/usr/bin/env python3
"""
improved_gap_analysis.py - 개선된 갭 분석 로직

주요 개선사항:
1. KST 시간대 기준 갭 계산
2. 업비트 데이터 특성 고려
3. 더 정확한 디버깅 정보
4. 엣지 케이스 처리 강화
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import pytz
import logging

logger = logging.getLogger(__name__)

class ImprovedGapAnalyzer:
    """개선된 갭 분석기"""

    def __init__(self, db_path: str = "./makenaide_local.db"):
        self.db_path = db_path
        self.kst = pytz.timezone('Asia/Seoul')

    def get_latest_date_with_debug(self, ticker: str) -> Optional[datetime]:
        """디버깅 정보를 포함한 최신 데이터 날짜 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 최신 데이터와 전체 통계 조회
            cursor.execute("""
                SELECT
                    MAX(date) as latest_date,
                    COUNT(*) as total_records,
                    MIN(date) as earliest_date
                FROM ohlcv_data
                WHERE ticker = ?
            """, (ticker,))

            result = cursor.fetchone()
            conn.close()

            if result and result[0]:
                latest_date = datetime.fromisoformat(result[0])
                total_records = result[1]
                earliest_date = result[2]

                logger.debug(f"📊 {ticker} DB 상태:")
                logger.debug(f"   • 총 레코드: {total_records}개")
                logger.debug(f"   • 최신 데이터: {latest_date.date()}")
                logger.debug(f"   • 가장 오래된 데이터: {earliest_date}")

                return latest_date
            return None

        except Exception as e:
            logger.error(f"❌ {ticker} 최신 날짜 조회 실패: {e}")
            return None

    def get_current_kst_date(self) -> datetime:
        """KST 기준 현재 날짜 (00:00:00) 반환"""
        now_kst = datetime.now(self.kst)
        current_date_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        return current_date_kst

    def analyze_gap_improved(self, ticker: str) -> Dict[str, Any]:
        """개선된 갭 분석 및 수집 전략 결정"""
        try:
            # 1. 최신 데이터 조회 (디버깅 정보 포함)
            latest_date = self.get_latest_date_with_debug(ticker)

            # 2. KST 기준 현재 날짜 계산
            current_date_kst = self.get_current_kst_date()
            current_time_kst = datetime.now(self.kst)

            logger.debug(f"🕐 {ticker} 시간 분석:")
            logger.debug(f"   • 현재 KST 시간: {current_time_kst}")
            logger.debug(f"   • 현재 KST 날짜: {current_date_kst.date()}")

            # 3. 데이터 없는 경우
            if latest_date is None:
                return {
                    'strategy': 'full_collection',
                    'gap_days': 200,
                    'reason': 'No existing data',
                    'debug_info': {
                        'latest_date': None,
                        'current_date_kst': current_date_kst.date(),
                        'current_time_kst': current_time_kst,
                        'timezone': 'Asia/Seoul'
                    }
                }

            # 4. 개선된 갭 계산 (KST 기준)
            gap_days = (current_date_kst.date() - latest_date.date()).days

            # 5. 업비트 특성 고려한 전략 결정
            strategy, reason = self._determine_strategy_improved(
                gap_days, current_time_kst, latest_date
            )

            debug_info = {
                'latest_date': latest_date.date(),
                'current_date_kst': current_date_kst.date(),
                'current_time_kst': current_time_kst,
                'gap_days': gap_days,
                'timezone': 'Asia/Seoul',
                'upbit_consideration': True
            }

            logger.info(f"📊 {ticker} 갭 분석 (개선된 로직):")
            logger.info(f"   • 최신 데이터: {latest_date.date()}")
            logger.info(f"   • 현재 KST: {current_date_kst.date()}")
            logger.info(f"   • 갭: {gap_days}일")
            logger.info(f"   • 전략: {strategy}")
            logger.info(f"   • 이유: {reason}")

            return {
                'strategy': strategy,
                'gap_days': gap_days,
                'reason': reason,
                'debug_info': debug_info
            }

        except Exception as e:
            logger.error(f"❌ {ticker} 개선된 갭 분석 실패: {e}")
            return {
                'strategy': 'incremental',
                'gap_days': 1,
                'reason': f'Analysis failed: {e}',
                'debug_info': {'error': str(e)}
            }

    def _determine_strategy_improved(self, gap_days: int, current_time_kst: datetime, latest_date: datetime) -> tuple:
        """업비트 특성을 고려한 전략 결정"""

        # 현재 시간이 새벽 1시 이전이면 전날 취급 (업비트 데이터 반영 시간 고려)
        if current_time_kst.hour < 1:
            effective_gap = gap_days - 1
            time_consideration = " (새벽 시간 고려)"
        else:
            effective_gap = gap_days
            time_consideration = ""

        if effective_gap <= 0:
            return 'skip', f'Data is up to date{time_consideration}'
        elif effective_gap == 1:
            return 'yesterday_update', f'Yesterday data needs update{time_consideration}'
        else:
            return 'incremental', f'{effective_gap} days gap detected{time_consideration}'

    def compare_with_original(self, ticker: str) -> Dict[str, Any]:
        """원래 로직과 개선된 로직 비교"""
        try:
            # 원래 로직
            latest_date = self.get_latest_date_with_debug(ticker)
            current_date_local = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

            if latest_date:
                gap_days_original = (current_date_local.date() - latest_date.date()).days
            else:
                gap_days_original = None

            # 개선된 로직
            improved_result = self.analyze_gap_improved(ticker)

            return {
                'ticker': ticker,
                'original': {
                    'gap_days': gap_days_original,
                    'current_date': current_date_local.date(),
                    'timezone': 'Local'
                },
                'improved': {
                    'gap_days': improved_result['gap_days'],
                    'current_date': improved_result['debug_info']['current_date_kst'],
                    'timezone': 'Asia/Seoul',
                    'strategy': improved_result['strategy'],
                    'reason': improved_result['reason']
                },
                'difference': {
                    'gap_difference': (improved_result['gap_days'] - gap_days_original) if gap_days_original is not None else None,
                    'strategy_changed': True  # 추가 비교 로직 필요시 구현
                }
            }

        except Exception as e:
            logger.error(f"❌ {ticker} 로직 비교 실패: {e}")
            return {'error': str(e)}


def test_improved_gap_analysis():
    """개선된 갭 분석 테스트"""
    print("🔍 개선된 갭 분석 로직 테스트")
    print("=" * 60)

    analyzer = ImprovedGapAnalyzer()
    test_tickers = ['KRW-ONT', 'KRW-SUI', 'KRW-BTC', 'KRW-ETH']

    for ticker in test_tickers:
        print(f"\n📊 {ticker} 분석:")
        print("-" * 40)

        # 개선된 분석
        result = analyzer.analyze_gap_improved(ticker)
        print(f"전략: {result['strategy']}")
        print(f"갭: {result['gap_days']}일")
        print(f"이유: {result['reason']}")

        # 원래 로직과 비교
        comparison = analyzer.compare_with_original(ticker)
        if 'error' not in comparison:
            print(f"\n🔄 로직 비교:")
            print(f"• 원래 갭: {comparison['original']['gap_days']}일 (로컬)")
            print(f"• 개선 갭: {comparison['improved']['gap_days']}일 (KST)")
            if comparison['difference']['gap_difference']:
                print(f"• 차이: {comparison['difference']['gap_difference']}일")

    print(f"\n✅ 개선된 갭 분석 로직 테스트 완료")


if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    test_improved_gap_analysis()