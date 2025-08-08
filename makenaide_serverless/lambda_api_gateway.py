#!/usr/bin/env python3
"""
AWS Lambda 함수: Makenaide API Gateway 백엔드
기능: 로그, 리포트, DB조회를 위한 REST API 엔드포인트 제공
"""

import json
import boto3
import psycopg2
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import base64

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 초기화
s3 = boto3.client('s3')
logs_client = boto3.client('logs')

def get_db_connection():
    """PostgreSQL DB 연결"""
    try:
        conn = psycopg2.connect(
            host=os.environ['PG_HOST'],
            port=int(os.environ['PG_PORT']),
            database=os.environ['PG_DATABASE'],
            user=os.environ['PG_USER'],
            password=os.environ['PG_PASSWORD']
        )
        return conn
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        raise

def cors_headers():
    """CORS 헤더 반환"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Content-Type': 'application/json'
    }

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """API Gateway 응답 생성"""
    return {
        'statusCode': status_code,
        'headers': cors_headers(),
        'body': json.dumps(body, ensure_ascii=False, default=str)
    }

def get_system_status() -> Dict[str, Any]:
    """시스템 전체 상태 조회"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 최신 데이터 업데이트 시간
        cursor.execute("""
            SELECT 
                MAX(updated_at) as last_ohlcv_update,
                COUNT(DISTINCT ticker) as active_tickers
            FROM ohlcv 
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        ohlcv_stats = cursor.fetchone()
        
        # 정적 지표 상태
        cursor.execute("""
            SELECT 
                COUNT(*) as total_indicators,
                COUNT(CASE WHEN updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 day' THEN 1 END) as recent_updates
            FROM static_indicators
        """)
        static_stats = cursor.fetchone()
        
        # GPT 분석 상태
        cursor.execute("""
            SELECT 
                COUNT(*) as total_analyses,
                COUNT(CASE WHEN created_at >= CURRENT_TIMESTAMP - INTERVAL '1 day' THEN 1 END) as recent_analyses
            FROM gpt_analysis_results
            WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
        """)
        gpt_stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'system_status': 'operational',
            'ohlcv_data': {
                'last_update': ohlcv_stats[0].isoformat() if ohlcv_stats[0] else None,
                'active_tickers': ohlcv_stats[1] or 0
            },
            'static_indicators': {
                'total_count': static_stats[0] or 0,
                'recent_updates': static_stats[1] or 0
            },
            'gpt_analysis': {
                'total_analyses': gpt_stats[0] or 0,
                'recent_analyses': gpt_stats[1] or 0
            }
        }
        
    except Exception as e:
        logger.error(f"시스템 상태 조회 실패: {e}")
        return {
            'timestamp': datetime.now().isoformat(),
            'system_status': 'error',
            'error': str(e)
        }

def get_ticker_data(ticker: str, days: int = 30) -> Dict[str, Any]:
    """특정 티커의 데이터 조회"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # OHLCV 데이터
        cursor.execute("""
            SELECT date, open, high, low, close, volume, 
                   ma_20, ma_50, ma_200, rsi_14
            FROM ohlcv 
            WHERE ticker = %s 
            AND date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date DESC
            LIMIT 100
        """, (ticker, days))
        
        ohlcv_data = []
        for row in cursor.fetchall():
            ohlcv_data.append({
                'date': row[0].isoformat() if row[0] else None,
                'open': float(row[1]) if row[1] else None,
                'high': float(row[2]) if row[2] else None,
                'low': float(row[3]) if row[3] else None,
                'close': float(row[4]) if row[4] else None,
                'volume': float(row[5]) if row[5] else None,
                'ma_20': float(row[6]) if row[6] else None,
                'ma_50': float(row[7]) if row[7] else None,
                'ma_200': float(row[8]) if row[8] else None,
                'rsi_14': float(row[9]) if row[9] else None
            })
        
        # 정적 지표
        cursor.execute("""
            SELECT volume_change_7_30, nvt_relative, price, high_60, low_60,
                   pivot, s1, r1, resistance, support, atr, adx, supertrend_signal,
                   updated_at
            FROM static_indicators 
            WHERE ticker = %s
        """, (ticker,))
        
        static_result = cursor.fetchone()
        static_indicators = None
        if static_result:
            static_indicators = {
                'volume_change_7_30': float(static_result[0]) if static_result[0] else None,
                'nvt_relative': float(static_result[1]) if static_result[1] else None,
                'price': float(static_result[2]) if static_result[2] else None,
                'high_60': float(static_result[3]) if static_result[3] else None,
                'low_60': float(static_result[4]) if static_result[4] else None,
                'pivot': float(static_result[5]) if static_result[5] else None,
                's1': float(static_result[6]) if static_result[6] else None,
                'r1': float(static_result[7]) if static_result[7] else None,
                'resistance': float(static_result[8]) if static_result[8] else None,
                'support': float(static_result[9]) if static_result[9] else None,
                'atr': float(static_result[10]) if static_result[10] else None,
                'adx': float(static_result[11]) if static_result[11] else None,
                'supertrend_signal': static_result[12],
                'updated_at': static_result[13].isoformat() if static_result[13] else None
            }
        
        # GPT 분석 결과
        cursor.execute("""
            SELECT analysis_result, score, confidence, action, market_phase, created_at
            FROM gpt_analysis_results 
            WHERE ticker = %s 
            ORDER BY created_at DESC 
            LIMIT 5
        """, (ticker,))
        
        gpt_analyses = []
        for row in cursor.fetchall():
            gpt_analyses.append({
                'analysis_result': row[0],
                'score': float(row[1]) if row[1] else None,
                'confidence': float(row[2]) if row[2] else None,
                'action': row[3],
                'market_phase': row[4],
                'created_at': row[5].isoformat() if row[5] else None
            })
        
        cursor.close()
        conn.close()
        
        return {
            'ticker': ticker,
            'timestamp': datetime.now().isoformat(),
            'ohlcv_data': ohlcv_data,
            'static_indicators': static_indicators,
            'gpt_analyses': gpt_analyses
        }
        
    except Exception as e:
        logger.error(f"티커 데이터 조회 실패 ({ticker}): {e}")
        raise

def get_recent_logs(hours: int = 24, level: str = 'INFO') -> Dict[str, Any]:
    """최근 로그 조회"""
    try:
        log_groups = [
            '/aws/lambda/makenaide-ticker-scanner',
            '/aws/lambda/makenaide-ohlcv-collector',
            '/aws/lambda/makenaide-gpt-analyzer'
        ]
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        all_logs = []
        
        for log_group in log_groups:
            try:
                # 로그 스트림 조회
                streams_response = logs_client.describe_log_streams(
                    logGroupName=log_group,
                    orderBy='LastEventTime',
                    descending=True,
                    limit=5
                )
                
                for stream in streams_response['logStreams']:
                    # 로그 이벤트 조회
                    events_response = logs_client.get_log_events(
                        logGroupName=log_group,
                        logStreamName=stream['logStreamName'],
                        startTime=int(start_time.timestamp() * 1000),
                        endTime=int(end_time.timestamp() * 1000),
                        limit=100
                    )
                    
                    for event in events_response['events']:
                        message = event['message']
                        if level.upper() in message or level == 'ALL':
                            all_logs.append({
                                'timestamp': datetime.fromtimestamp(event['timestamp'] / 1000).isoformat(),
                                'log_group': log_group,
                                'log_stream': stream['logStreamName'],
                                'message': message
                            })
                            
            except Exception as e:
                logger.warning(f"로그 그룹 {log_group} 조회 실패: {e}")
                continue
        
        # 시간순 정렬
        all_logs.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'log_count': len(all_logs),
            'logs': all_logs[:200]  # 최대 200개
        }
        
    except Exception as e:
        logger.error(f"로그 조회 실패: {e}")
        raise

def get_performance_report() -> Dict[str, Any]:
    """성능 리포트 조회"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 최근 7일 처리 통계
        cursor.execute("""
            SELECT 
                DATE(updated_at) as date,
                COUNT(DISTINCT ticker) as processed_tickers,
                AVG(CASE WHEN adx > 0 THEN adx END) as avg_adx,
                AVG(CASE WHEN volume_change_7_30 > 0 THEN volume_change_7_30 END) as avg_volume_change
            FROM static_indicators 
            WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(updated_at)
            ORDER BY date DESC
        """)
        
        daily_stats = []
        for row in cursor.fetchall():
            daily_stats.append({
                'date': row[0].isoformat() if row[0] else None,
                'processed_tickers': row[1] or 0,
                'avg_adx': float(row[2]) if row[2] else None,
                'avg_volume_change': float(row[3]) if row[3] else None
            })
        
        # GPT 분석 성과
        cursor.execute("""
            SELECT 
                action,
                COUNT(*) as count,
                AVG(score) as avg_score,
                AVG(confidence) as avg_confidence
            FROM gpt_analysis_results 
            WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
            GROUP BY action
        """)
        
        gpt_performance = []
        for row in cursor.fetchall():
            gpt_performance.append({
                'action': row[0],
                'count': row[1],
                'avg_score': float(row[2]) if row[2] else None,
                'avg_confidence': float(row[3]) if row[3] else None
            })
        
        # 시스템 헬스 체크
        cursor.execute("""
            SELECT 
                COUNT(*) as total_tickers,
                COUNT(CASE WHEN updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 1 END) as recent_updates,
                COUNT(CASE WHEN adx IS NOT NULL AND adx > 0 THEN 1 END) as valid_adx_count,
                COUNT(CASE WHEN volume_change_7_30 IS NOT NULL AND volume_change_7_30 > 0 THEN 1 END) as valid_volume_count
            FROM static_indicators
        """)
        
        health_stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        health_score = 0
        if health_stats[0] > 0:  # total_tickers > 0
            health_score = (
                (health_stats[1] / health_stats[0]) * 0.3 +  # recent_updates ratio
                (health_stats[2] / health_stats[0]) * 0.35 +  # valid_adx ratio
                (health_stats[3] / health_stats[0]) * 0.35    # valid_volume ratio
            ) * 100
        
        return {
            'timestamp': datetime.now().isoformat(),
            'daily_statistics': daily_stats,
            'gpt_performance': gpt_performance,
            'system_health': {
                'total_tickers': health_stats[0],
                'recent_updates': health_stats[1],
                'valid_adx_count': health_stats[2],
                'valid_volume_count': health_stats[3],
                'health_score': round(health_score, 2)
            }
        }
        
    except Exception as e:
        logger.error(f"성능 리포트 조회 실패: {e}")
        raise

def lambda_handler(event, context):
    """Lambda 메인 핸들러"""
    try:
        # CORS preflight 처리
        if event.get('httpMethod') == 'OPTIONS':
            return create_response(200, {'message': 'CORS preflight'})
        
        # HTTP 메소드 및 경로 추출
        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '/')
        query_params = event.get('queryStringParameters') or {}
        
        logger.info(f"API 요청: {http_method} {path}")
        
        # 라우팅
        if path == '/status':
            # 시스템 상태 조회
            result = get_system_status()
            return create_response(200, result)
            
        elif path == '/ticker':
            # 티커 데이터 조회
            ticker = query_params.get('symbol')
            if not ticker:
                return create_response(400, {'error': 'ticker parameter required'})
            
            days = int(query_params.get('days', 30))
            result = get_ticker_data(ticker, days)
            return create_response(200, result)
            
        elif path == '/logs':
            # 로그 조회
            hours = int(query_params.get('hours', 24))
            level = query_params.get('level', 'INFO')
            result = get_recent_logs(hours, level)
            return create_response(200, result)
            
        elif path == '/performance':
            # 성능 리포트 조회
            result = get_performance_report()
            return create_response(200, result)
            
        elif path == '/tickers':
            # 전체 티커 목록 조회
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT DISTINCT ticker, MAX(updated_at) as last_update
                FROM static_indicators 
                GROUP BY ticker 
                ORDER BY last_update DESC 
                LIMIT 100
            """)
            
            tickers = []
            for row in cursor.fetchall():
                tickers.append({
                    'ticker': row[0],
                    'last_update': row[1].isoformat() if row[1] else None
                })
            
            cursor.close()
            conn.close()
            
            return create_response(200, {
                'timestamp': datetime.now().isoformat(),
                'tickers': tickers
            })
            
        else:
            # 알 수 없는 경로
            return create_response(404, {
                'error': 'Not Found',
                'available_endpoints': [
                    '/status',
                    '/ticker?symbol=BTC&days=30',
                    '/logs?hours=24&level=INFO',
                    '/performance',
                    '/tickers'
                ]
            })
            
    except Exception as e:
        logger.error(f"API 요청 처리 실패: {e}")
        return create_response(500, {
            'error': 'Internal Server Error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }) 