#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide 티커 스캐너
기능: Upbit API 티커 조회 → 신규 티커 감지 → DB 업데이트 → 블랙리스트 필터링 → 거래량 필터링 → SQS 전송
"""

import json
import boto3
import logging
import os
import pyupbit
from datetime import datetime, timedelta
from typing import List, Dict, Any

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
sqs = boto3.client('sqs')

def get_db_connection():
    """PostgreSQL DB 연결 - psycopg2를 동적으로 import"""
    try:
        # 동적 import로 psycopg2 가져오기
        import psycopg2
        import psycopg2.extras
    except ImportError as e:
        logger.error(f"psycopg2 import 실패: {e}")
        raise Exception("psycopg2가 사용 불가능합니다.")
    
    try:
        # 환경변수 확인 및 기본값 설정
        pg_host = os.environ.get('PG_HOST', 'makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com')
        pg_port = int(os.environ.get('PG_PORT', '5432'))
        pg_database = os.environ.get('PG_DATABASE', 'makenaide')
        pg_user = os.environ.get('PG_USER', 'bruce')
        pg_password = os.environ.get('PG_PASSWORD')
        
        if not pg_password:
            raise Exception("PG_PASSWORD 환경변수가 설정되지 않았습니다.")
        
        logger.info(f"DB 연결 시도: {pg_host}:{pg_port}/{pg_database}")
        
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        raise

def load_blacklist_from_db() -> Dict[str, Any]:
    """DB에서 블랙리스트 로드"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT ticker, reason FROM blacklist WHERE is_active = true")
        results = cursor.fetchall()
        
        blacklist = {}
        for ticker, reason in results:
            blacklist[ticker] = reason
            
        cursor.close()
        conn.close()
        
        logger.info(f"블랙리스트 로드 완료: {len(blacklist)}개")
        return blacklist
        
    except Exception as e:
        logger.error(f"블랙리스트 로드 실패: {e}")
        return {}

def update_tickers():
    """
    Upbit API를 통해 티커 정보를 업데이트합니다.
    scanner.py의 update_tickers 함수를 Lambda에 맞게 포팅
    """
    try:
        logger.info("🔄 티커 정보 업데이트 시작")
        
        # 블랙리스트 로드
        blacklist = load_blacklist_from_db()
        if not blacklist:
            logger.warning("⚠️ 블랙리스트 로드 실패 또는 비어있음")
            blacklist = {}

        # 현재 거래 가능한 티커 목록 조회 (Upbit API)
        logger.info("📡 Upbit API에서 티커 목록 조회 중...")
        current_tickers = pyupbit.get_tickers(fiat="KRW")
        if not current_tickers:
            logger.error("❌ Upbit API 티커 목록 조회 실패")
            return False, "Upbit API 티커 목록 조회 실패"

        logger.info(f"✅ Upbit API에서 {len(current_tickers)}개 티커 조회 완료")

        # 블랙리스트에 있는 티커 제외
        filtered_tickers = [ticker for ticker in current_tickers if ticker not in blacklist]
        if len(filtered_tickers) != len(current_tickers):
            blacklisted = set(current_tickers) - set(filtered_tickers)
            logger.info(f"⛔️ 블랙리스트 제외 티커: {sorted(blacklisted)}")

        # DB 연결
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # 기존 티커 조회
            cursor.execute("SELECT ticker, updated_at FROM tickers")
            existing_rows = cursor.fetchall()
            existing_ticker_times = {row[0]: row[1] for row in existing_rows}

            logger.info(f"📊 DB에 기존 티커 {len(existing_ticker_times)}개 존재")

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
            updated_count = 0
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
                            updated_count += 1
                        except Exception as e:
                            logger.error(f"❌ {ticker} 정보 업데이트 실패: {str(e)}")
                            continue

            conn.commit()
            logger.info(f"✅ 티커 정보 업데이트 완료 - 신규: {len(new_tickers)}개, 업데이트: {updated_count}개")
            
            return True, {
                'total_api_tickers': len(current_tickers),
                'filtered_tickers': len(filtered_tickers),
                'new_tickers': len(new_tickers),
                'updated_tickers': updated_count,
                'blacklisted_removed': len(blacklisted_tickers)
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ DB 작업 중 오류: {str(e)}")
            raise

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"❌ 티커 업데이트 중 오류: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return False, str(e)

def get_active_tickers() -> List[str]:
    """활성 티커 목록 조회 (DB에서)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # tickers 테이블에서 is_active가 true인 티커들만 조회
        cursor.execute("""
            SELECT ticker 
            FROM tickers 
            WHERE is_active = true
            ORDER BY ticker
        """)
        
        tickers = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        logger.info(f"활성 티커 조회 완료: {len(tickers)}개")
        return tickers
        
    except Exception as e:
        logger.error(f"활성 티커 조회 실패: {e}")
        # 폴백: 모든 티커 조회
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT ticker FROM tickers ORDER BY ticker")
            tickers = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            logger.info(f"폴백으로 전체 티커 조회: {len(tickers)}개")
            return tickers
        except:
            return []

def filter_by_volume(tickers: List[str]) -> List[str]:
    """거래량 기반 필터링"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        filtered_tickers = []
        
        for ticker in tickers:
            # 최근 7일 평균 거래량 확인
            cursor.execute("""
                SELECT AVG(volume * close) as avg_trading_value
                FROM ohlcv 
                WHERE ticker = %s 
                AND date >= CURRENT_DATE - INTERVAL '7 days'
            """, (ticker,))
            
            result = cursor.fetchone()
            if result and result[0]:
                avg_trading_value = float(result[0])
                
                # 1억원 이상 거래대금
                if avg_trading_value >= 100_000_000:
                    filtered_tickers.append(ticker)
        
        cursor.close()
        conn.close()
        
        logger.info(f"거래량 필터링 완료: {len(filtered_tickers)}개 (전체: {len(tickers)}개)")
        return filtered_tickers
        
    except Exception as e:
        logger.error(f"거래량 필터링 실패: {e}")
        return tickers  # 실패 시 원본 반환

def send_to_sqs(tickers: List[str], queue_url: str):
    """SQS에 티커 목록 전송"""
    try:
        # 배치 단위로 전송 (10개씩)
        batch_size = 10
        
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            
            entries = []
            for j, ticker in enumerate(batch):
                entries.append({
                    'Id': str(i + j),
                    'MessageBody': json.dumps({
                        'ticker': ticker,
                        'timestamp': datetime.now().isoformat(),
                        'source': 'ticker_scanner'
                    })
                })
            
            response = sqs.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            
            logger.info(f"SQS 전송 완료: {len(entries)}개 (배치 {i//batch_size + 1})")
            
        logger.info(f"전체 SQS 전송 완료: {len(tickers)}개 티커")
        
    except Exception as e:
        logger.error(f"SQS 전송 실패: {e}")
        raise

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        logger.info("🚀 Makenaide 통합 티커 스캐너 시작")
        start_time = datetime.now()
        
        # 1. 티커 정보 업데이트 (Upbit API → DB)
        update_success, update_result = update_tickers()
        if not update_success:
            logger.error(f"❌ 티커 업데이트 실패: {update_result}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'티커 업데이트 실패: {update_result}',
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # 2. 활성 티커 조회 (DB에서)
        tickers = get_active_tickers()
        if not tickers:
            logger.warning("활성 티커가 없습니다")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': '활성 티커가 없습니다',
                    'update_result': update_result,
                    'processed_count': 0
                })
            }
        
        # 3. 블랙리스트 필터링 (추가 필터링)
        blacklist = load_blacklist_from_db()
        filtered_tickers = [t for t in tickers if t not in blacklist]
        
        logger.info(f"블랙리스트 추가 필터링: {len(tickers)} → {len(filtered_tickers)}")
        
        # 4. 거래량 필터링
        volume_filtered = filter_by_volume(filtered_tickers)
        
        # 5. SQS 전송
        queue_url = os.environ.get('OHLCV_QUEUE_URL')
        if queue_url and volume_filtered:
            send_to_sqs(volume_filtered, queue_url)
        
        # 6. 결과 반환
        execution_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': '통합 티커 스캐닝 완료',
                'update_result': update_result,
                'total_db_tickers': len(tickers),
                'blacklist_filtered': len(filtered_tickers),
                'volume_filtered': len(volume_filtered),
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            })
        }
        
        logger.info(f"✅ 통합 티커 스캐닝 완료: {len(volume_filtered)}개 티커 처리")
        return result
        
    except Exception as e:
        logger.error(f"❌ 통합 티커 스캐닝 실패: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        } 