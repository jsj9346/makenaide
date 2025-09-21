#!/usr/bin/env python3
"""
🚀 Phase 2 기술적 지표 NULL 값 업데이트 스크립트
실제 운영 DB의 technical_analysis 테이블에서 NULL인 핵심 지표들을 계산하여 업데이트
- ATR (Average True Range)
- Supertrend
- MACD Histogram
- ADX (Average Directional Index)
- Support Level
"""

import sqlite3
import pandas as pd
import sys
import os
import logging
from datetime import datetime

# 프로젝트 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_collector import SimpleDataCollector

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def update_technical_indicators():
    """🚀 Phase 2: technical_analysis 테이블의 NULL 지표들을 업데이트"""

    print("🚀 Phase 2: 핵심 기술적 지표 NULL 값 업데이트 시작")
    print("🎯 대상: ATR, Supertrend, MACD Histogram, ADX, Support Level")
    print("=" * 60)

    db_path = "./makenaide_local.db"
    temp_db_path = "./temp_update.db"
    collector = SimpleDataCollector(temp_db_path)  # 임시 DB 사용

    conn = sqlite3.connect(db_path)

    # technical_analysis 테이블에서 NULL 지표가 있는 종목들 조회
    null_check_query = """
    SELECT DISTINCT ticker,
           COUNT(*) as total_records,
           COUNT(CASE WHEN supertrend IS NULL THEN 1 END) as supertrend_null,
           COUNT(CASE WHEN atr IS NULL THEN 1 END) as atr_null,
           COUNT(CASE WHEN macd_histogram IS NULL THEN 1 END) as macd_null,
           COUNT(CASE WHEN adx IS NULL THEN 1 END) as adx_null,
           COUNT(CASE WHEN support_level IS NULL THEN 1 END) as support_null
    FROM technical_analysis
    GROUP BY ticker
    HAVING (supertrend IS NULL OR atr IS NULL OR macd_histogram IS NULL
            OR adx IS NULL OR support_level IS NULL)
    ORDER BY ticker
    """

    null_df = pd.read_sql_query(null_check_query, conn)

    if null_df.empty:
        print("✅ 업데이트할 NULL 지표 없음 - 모든 지표가 이미 계산됨")
        conn.close()
        return

    tickers = null_df['ticker'].tolist()
    print(f"📊 NULL 지표가 있는 종목: {len(tickers)}개")
    print(f"📋 대상 종목: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")

    updated_count = 0
    failed_count = 0

    for i, ticker in enumerate(tickers, 1):
        try:
            print(f"\n🔄 [{i}/{len(tickers)}] {ticker} 처리 중...")

            # OHLCV 데이터 조회 (기술적 지표 계산용)
            ohlcv_query = """
            SELECT date, open, high, low, close, volume
            FROM ohlcv_data
            WHERE ticker = ?
            ORDER BY date ASC
            """

            df = pd.read_sql_query(ohlcv_query, conn, params=(ticker,))

            if len(df) < 50:
                print(f"⚠️ {ticker}: OHLCV 데이터 부족 ({len(df)}개, 최소 50개 필요)")
                failed_count += 1
                continue

            # 날짜를 인덱스로 설정
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

            # 🚀 Phase 2 핵심: 새로운 기술적 지표 계산
            print(f"   📊 {len(df)}일간 데이터로 기술적 지표 계산 중...")
            df_with_indicators = collector.calculate_technical_indicators(df, ticker)

            # 새로운 지표들 추출
            new_indicators = {}
            indicators = ['atr', 'supertrend', 'macd_histogram', 'adx', 'support_level']

            for indicator in indicators:
                if indicator in df_with_indicators.columns:
                    # 가장 최근의 NULL이 아닌 값 사용
                    series = df_with_indicators[indicator].dropna()
                    if len(series) > 0:
                        new_indicators[indicator] = float(series.iloc[-1])
                        print(f"   ✅ {indicator}: {new_indicators[indicator]:.4f}")
                    else:
                        new_indicators[indicator] = None
                        print(f"   ❌ {indicator}: 계산 실패")
                else:
                    new_indicators[indicator] = None
                    print(f"   💥 {indicator}: 컬럼 없음")

            # technical_analysis 테이블의 최신 레코드 업데이트
            cursor = conn.cursor()

            # 해당 종목의 최신 analysis_date 찾기
            cursor.execute("""
                SELECT analysis_date FROM technical_analysis
                WHERE ticker = ? ORDER BY analysis_date DESC LIMIT 1
            """, [ticker])

            result = cursor.fetchone()
            if not result:
                print(f"   ⚠️ {ticker}: technical_analysis 레코드 없음")
                failed_count += 1
                continue

            latest_date = result[0]

            # 🎯 핵심: technical_analysis 테이블 업데이트
            update_query = """
            UPDATE technical_analysis
            SET atr = ?, supertrend = ?, macd_histogram = ?, adx = ?, support_level = ?
            WHERE ticker = ? AND analysis_date = ?
            """

            cursor.execute(update_query, [
                new_indicators['atr'],
                new_indicators['supertrend'],
                new_indicators['macd_histogram'],
                new_indicators['adx'],
                new_indicators['support_level'],
                ticker,
                latest_date
            ])

            conn.commit()

            # 업데이트 성공 확인
            success_count = sum(1 for v in new_indicators.values() if v is not None)
            print(f"   ✅ {ticker}: {success_count}/5개 지표 업데이트 완료 ({latest_date})")

            updated_count += 1

        except Exception as e:
            print(f"❌ {ticker} 처리 실패: {e}")
            failed_count += 1
            import traceback
            print(f"   오류 상세: {traceback.format_exc()}")
            continue

    conn.close()

    # 임시 DB 파일 정리
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)

    # 최종 결과 요약
    print("\n" + "=" * 60)
    print("🚀 Phase 2 기술적 지표 업데이트 완료")
    print("=" * 60)
    print(f"✅ 성공: {updated_count}개 종목")
    print(f"❌ 실패: {failed_count}개 종목")
    print(f"📋 전체: {len(tickers)}개 종목")
    print(f"📈 성공률: {updated_count/len(tickers)*100:.1f}%")

    # 검증: 업데이트 후 NULL 상태 확인
    if updated_count > 0:
        print("\n🔍 업데이트 후 NULL 상태 검증")
        verify_updated_indicators()

def verify_updated_indicators():
    """🔍 업데이트 후 새로운 기술적 지표 검증"""

    conn = sqlite3.connect("./makenaide_local.db")

    # 업데이트 후 NULL 상태 전체 확인
    verify_query = """
    SELECT COUNT(*) as total_records,
           COUNT(CASE WHEN supertrend IS NOT NULL THEN 1 END) as supertrend_not_null,
           COUNT(CASE WHEN atr IS NOT NULL THEN 1 END) as atr_not_null,
           COUNT(CASE WHEN macd_histogram IS NOT NULL THEN 1 END) as macd_histogram_not_null,
           COUNT(CASE WHEN adx IS NOT NULL THEN 1 END) as adx_not_null,
           COUNT(CASE WHEN support_level IS NOT NULL THEN 1 END) as support_level_not_null
    FROM technical_analysis
    """

    result = pd.read_sql_query(verify_query, conn).iloc[0]
    total = result['total_records']

    print("📊 업데이트 후 기술적 지표 상태:")
    print(f"   📈 전체 레코드: {total}개")
    print(f"   📊 Supertrend: {result['supertrend_not_null']}/{total} ({result['supertrend_not_null']/total*100:.1f}%)")
    print(f"   📊 ATR: {result['atr_not_null']}/{total} ({result['atr_not_null']/total*100:.1f}%)")
    print(f"   📊 MACD Histogram: {result['macd_histogram_not_null']}/{total} ({result['macd_histogram_not_null']/total*100:.1f}%)")
    print(f"   📊 ADX: {result['adx_not_null']}/{total} ({result['adx_not_null']/total*100:.1f}%)")
    print(f"   📊 Support Level: {result['support_level_not_null']}/{total} ({result['support_level_not_null']/total*100:.1f}%)")

    # 샘플 데이터 확인
    sample_query = """
    SELECT ticker, analysis_date, atr, supertrend, macd_histogram, adx, support_level
    FROM technical_analysis
    WHERE atr IS NOT NULL AND supertrend IS NOT NULL
    ORDER BY analysis_date DESC
    LIMIT 5
    """

    sample_df = pd.read_sql_query(sample_query, conn)
    conn.close()

    if not sample_df.empty:
        print("\n📊 업데이트된 샘플 데이터:")
        for _, row in sample_df.iterrows():
            print(f"   {row['ticker']} ({row['analysis_date']}):")
            print(f"     ATR: {row['atr']:.2f if pd.notna(row['atr']) else 'NULL'}")
            print(f"     Supertrend: {row['supertrend']:.2f if pd.notna(row['supertrend']) else 'NULL'}")
            print(f"     MACD: {row['macd_histogram']:.2f if pd.notna(row['macd_histogram']) else 'NULL'}")
            print(f"     ADX: {row['adx']:.2f if pd.notna(row['adx']) else 'NULL'}")
            print(f"     Support: {row['support_level']:.2f if pd.notna(row['support_level']) else 'NULL'}")
    else:
        print("⚠️ 업데이트된 데이터 샘플 없음")

def verify_technical_indicators():
    """기존 검증 함수 (호환성 유지)"""
    verify_updated_indicators()

if __name__ == "__main__":
    start_time = datetime.now()
    update_technical_indicators()
    end_time = datetime.now()

    duration = end_time - start_time
    print(f"\n⏱️ 총 실행 시간: {duration}")