import os
import logging
import pyupbit
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils import logger, setup_logger, load_blacklist, safe_strftime, setup_restricted_logger
import sys
import argparse

# 로거 초기화 (제한된 로깅 사용)
logger = setup_restricted_logger('scanner')

load_dotenv()

def update_tickers():
    """티커 정보를 업데이트합니다."""
    try:
        # 블랙리스트 로드
        blacklist = load_blacklist()
        if not blacklist:
            logger.warning("⚠️ 블랙리스트 로드 실패")
            blacklist = {}

        # 현재 거래 가능한 티커 목록 조회
        current_tickers = pyupbit.get_tickers(fiat="KRW")
        if not current_tickers:
            logger.error("❌ 티커 목록 조회 실패")
            return

        # 블랙리스트에 있는 티커 제외
        filtered_tickers = [ticker for ticker in current_tickers if ticker not in blacklist]
        if len(filtered_tickers) != len(current_tickers):
            blacklisted = set(current_tickers) - set(filtered_tickers)
            logger.info(f"⛔️ 블랙리스트 제외 티커: {sorted(blacklisted)}")

        # DB 연결
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        cursor = conn.cursor()

        try:
            # 기존 티커 조회
            cursor.execute("SELECT ticker, updated_at FROM tickers")
            existing_rows = cursor.fetchall()
            existing_ticker_times = {row[0]: row[1] for row in existing_rows}

            # 블랙리스트 티커 DB에서 삭제
            blacklisted_tickers = set(existing_ticker_times.keys()) & set(blacklist.keys())
            if blacklisted_tickers:
                for ticker in blacklisted_tickers:
                    cursor.execute("DELETE FROM tickers WHERE ticker = %s", (ticker,))
                conn.commit()
                logger.info(f"🗑️ 블랙리스트 티커 DB에서 삭제: {sorted(blacklisted_tickers)}")

            # 신규 티커 추가 (블랙리스트 제외)
            new_tickers = set(filtered_tickers) - set(existing_ticker_times.keys())
            if new_tickers:
                for new_ticker in new_tickers:
                    cursor.execute(
                        "INSERT INTO tickers (ticker, created_at) VALUES (%s, CURRENT_TIMESTAMP)",
                        (new_ticker,)
                    )
                conn.commit()
                logger.info(f"🎉 신규 티커 감지 및 추가됨: {sorted(new_tickers)}")

            # 기존 티커 업데이트 (24시간 이상 지난 경우)
            update_threshold = datetime.now() - timedelta(hours=24)
            for ticker in filtered_tickers:
                if ticker in existing_ticker_times:
                    last_update = existing_ticker_times[ticker]
                    if last_update < update_threshold:
                        try:
                            cursor.execute("""
                                UPDATE tickers
                                SET updated_at = CURRENT_TIMESTAMP
                                WHERE ticker = %s
                            """, (ticker,))
                            logger.info(f"✅ {ticker} 정보 업데이트 완료")
                        except Exception as e:
                            logger.error(f"❌ {ticker} 정보 업데이트 실패: {str(e)}")
                            continue

            conn.commit()
            logger.info("✅ 티커 정보 업데이트 완료")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ DB 작업 중 오류: {str(e)}")
            raise

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"❌ 티커 업데이트 중 오류: {str(e)}")

def sync_blacklist_with_is_active():
    """
    블랙리스트와 is_active 컬럼을 동기화하는 함수
    
    전략:
    - 블랙리스트에 있는 티커 → is_active = false
    - 정상 티커 → is_active = true
    - 두 시스템의 일관성 유지
    """
    try:
        blacklist = load_blacklist()
        if not blacklist:
            logger.warning("⚠️ 블랙리스트가 비어있거나 로드 실패")
            return False
            
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        cursor = conn.cursor()
        
        try:
            # is_active 컬럼 존재 확인
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'tickers' AND column_name = 'is_active'
            """)
            has_is_active = len(cursor.fetchall()) > 0
            
            if not has_is_active:
                logger.warning("⚠️ is_active 컬럼이 존재하지 않습니다. 먼저 init_db_pg.py를 실행하세요.")
                return False
            
            # 모든 티커를 일단 활성화
            cursor.execute("UPDATE tickers SET is_active = true")
            
            # 블랙리스트 티커들을 비활성화
            if blacklist:
                placeholders = ','.join(['%s'] * len(blacklist))
                cursor.execute(f"""
                    UPDATE tickers 
                    SET is_active = false 
                    WHERE ticker IN ({placeholders})
                """, list(blacklist.keys()))
            
            conn.commit()
            
            # 결과 확인
            cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = true")
            active_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM tickers WHERE is_active = false")
            inactive_count = cursor.fetchone()[0]
            
            logger.info(f"✅ 블랙리스트 동기화 완료")
            logger.info(f"   - 활성 티커: {active_count}개")
            logger.info(f"   - 비활성 티커: {inactive_count}개")
            logger.info(f"   - 블랙리스트 동기화: {len(blacklist)}개 티커 비활성화")
            return True
        
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ 동기화 작업 중 오류: {e}")
            return False
        finally:
            cursor.close()
            conn.close()
        
    except Exception as e:
        logger.error(f"❌ 블랙리스트 동기화 실패: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='티커 관리 및 동기화 도구')
    parser.add_argument('--sync-blacklist', action='store_true', 
                       help='블랙리스트와 is_active 컬럼 동기화')
    args = parser.parse_args()
    
    try:
        if args.sync_blacklist:
            logger.info("🔄 블랙리스트 동기화 시작")
            if sync_blacklist_with_is_active():
                logger.info("✅ 블랙리스트 동기화 완료")
            else:
                logger.error("❌ 블랙리스트 동기화 실패")
        else:
            logger.info("🔄 티커 정보 업데이트 시작")
            update_tickers()
            logger.info("✅ 티커 정보 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ 프로그램 실행 중 오류: {str(e)}")