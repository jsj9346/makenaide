import time
from functools import wraps
import re
import psycopg2
import os
from dotenv import load_dotenv
import functools
import requests
from datetime import datetime, date, timedelta
import logging
import sys
import json
import pytz
import pandas as pd
import psutil

# Import optimized monitor
from optimized_data_monitor import get_optimized_monitor

def safe_strftime(date_obj, format_str='%Y-%m-%d'):
    """
    안전한 datetime 변환 유틸리티 함수
    
    조건부 처리:
    - pd.Timestamp인 경우: 직접 strftime 사용
    - pd.DatetimeIndex인 경우: 직접 strftime 사용
    - 정수형인 경우: pd.to_datetime()으로 변환 후 strftime
    - None/NaN인 경우: 기본값 반환
    
    Args:
        date_obj: 변환할 날짜 객체 (datetime, pd.Timestamp, int, str 등)
        format_str (str): 날짜 포맷 문자열 (기본값: '%Y-%m-%d')
        
    Returns:
        str: 포맷된 날짜 문자열 또는 기본값
    """
    try:
        # None 또는 NaN 체크
        if date_obj is None or (hasattr(date_obj, 'isna') and date_obj.isna()):
            return "N/A"
        
        # pandas NaT 체크
        if pd.isna(date_obj):
            return "N/A"
        
        # 빈 문자열 체크
        if isinstance(date_obj, str) and date_obj.strip() == "":
            return "N/A"
        
        # 리스트, 튜플, 딕셔너리 등 컨테이너 타입 체크
        if isinstance(date_obj, (list, tuple, dict)):
            return str(date_obj)
        
        # pandas Timestamp 객체인 경우 (우선 처리)
        if isinstance(date_obj, pd.Timestamp):
            return date_obj.strftime(format_str)
        
        # pandas DatetimeIndex 요소인 경우
        if hasattr(date_obj, '__class__') and 'pandas' in str(type(date_obj)):
            try:
                # pandas datetime-like 객체 처리
                return pd.Timestamp(date_obj).strftime(format_str)
            except:
                pass
            
        # 이미 datetime 객체이거나 strftime 메서드가 있는 경우
        if hasattr(date_obj, 'strftime'):
            return date_obj.strftime(format_str)
            
        # 정수형인 경우 (timestamp)
        if isinstance(date_obj, (int, float)):
            # Unix timestamp로 가정하고 변환 (나노초도 고려)
            if date_obj > 1e15:  # 나노초 timestamp
                dt = pd.to_datetime(date_obj, unit='ns')
            elif date_obj > 1e10:  # 밀리초 timestamp
                dt = pd.to_datetime(date_obj, unit='ms')
            else:  # 초 timestamp
                dt = pd.to_datetime(date_obj, unit='s')
            return dt.strftime(format_str)
            
        # 문자열인 경우
        if isinstance(date_obj, str):
            # 빈 문자열 재확인
            if date_obj.strip() == "":
                return "N/A"
            # 이미 포맷된 문자열이면 그대로 반환
            if len(date_obj) >= 10 and '-' in date_obj:
                return date_obj[:10]  # YYYY-MM-DD 부분만 추출
            # 그렇지 않으면 datetime으로 변환 시도
            dt = pd.to_datetime(date_obj)
            return dt.strftime(format_str)
            
        # 기타 경우: pandas to_datetime으로 변환 시도
        dt = pd.to_datetime(date_obj)
        return dt.strftime(format_str)
        
    except Exception as e:
        # 디버깅을 위한 상세한 로그
        logger.debug(f"safe_strftime 변환 실패 - 입력값: {date_obj} (타입: {type(date_obj)}), 오류: {e}")
        
        # 모든 변환이 실패한 경우 문자열로 변환하여 반환
        try:
            result = str(date_obj)
            if result == "":
                return "N/A"
            return result[:10] if len(result) >= 10 else result
        except:
            return "Invalid Date"

def retry(max_attempts=3, initial_delay=0.5, backoff=2):
    """
    Decorator to retry a function on exception.
    :param max_attempts: maximum number of attempts (including first)
    :param initial_delay: initial wait time between retries
    :param backoff: multiplier for successive delays
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise
                    time.sleep(delay)
                    delay *= backoff
        return wrapper
    return decorator

def compute_expected_phase(data):
    """
    Stan Weinstein Market Phases 룰을 부분적으로 구현한 간략화 로직.
    :param data: dict with keys 'current_price', 'ma_50', 'ma_200', 'volume_change_7_30', 'r1', 'r2', 'r3', 's1'
    :return: int Phase number (1 to 4)
    """
    cp = float(data.get("current_price", 0))
    ma50 = float(data.get("ma_50", 0))
    ma200 = float(data.get("ma_200", 0))
    vol_chg = float(data.get("volume_change_7_30", 0))
    r1, r2, r3 = map(float, (data.get("r1", 0), data.get("r2", 0), data.get("r3", 0)))
    s1 = float(data.get("s1", 0))

    # Stage 4: 명백한 하락추세 (MA-50 < MA-200 및 가격 < MA-50)
    if ma50 < ma200 and cp < ma50:
        return 4
    # Stage 3: 정점권 (고점 근처 + 거래량 감소)
    if cp >= r3 and vol_chg < 0:
        return 3
    # Stage 2: 상승추세 (MA 배열 + 거래량 증가 + 가격 > MA-50)
    if ma50 > ma200 and vol_chg > 0 and cp > ma50:
        return 2
    # Stage 1: 횡보권 (S1~R1 구간, MA 평탄, 거래량 평이)
    if s1 <= cp <= r1 and abs(ma50 - ma200) < (ma200 * 0.01) and abs(vol_chg) < 0.05:
        return 1
    # 기본적으로 Stage 1으로 반환
    return 1


def validate_and_correct_phase(ticker, reported_phase, data, reason):
    """
    reported_phase: string like "Stage 2"
    :param ticker: str
    :param reported_phase: str
    :param data: dict of analysis data passed to GPT
    :param reason: original reason string from GPT
    :return: (corrected_phase_str, corrected_reason)
    """
    m = re.search(r'\d+', reported_phase or "")
    reported = int(m.group()) if m else None
    expected = compute_expected_phase(data)
    if reported is not None and reported != expected:
        print(f"⚠️ GPT hallucination for {ticker}: reported Stage {reported}, expected Stage {expected}")
        corrected = f"Stage {expected}"
        corrected_reason = f"{reason} | Phase forced to Stage {expected} by rule-based check"
        return corrected, corrected_reason
    return reported_phase, reason

# === 공통 상수 ===
MIN_KRW_ORDER = 10000  # 최소 매수 금액
MIN_KRW_SELL_ORDER = 5000  # 최소 매도 금액
TAKER_FEE_RATE = 0.00139  # 업비트 KRW 마켓 Taker 수수료

# === 환경변수 로딩 ===
def load_env():
    load_dotenv()

# === DB 연결 함수 ===
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )

def setup_logger():
    """
    로깅 설정을 초기화하고 로거를 반환합니다.
    
    Returns:
        logging.Logger: 설정된 로거 객체
    """
    log_dir = "log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = safe_strftime(datetime.now(), "%Y%m%d") + "_makenaide.log"
    log_file_path = os.path.join(log_dir, log_filename)

    log_format = "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 기존 핸들러 삭제
    if logger.hasHandlers():
        logger.handlers.clear()

    # 스트림 핸들러 (터미널 출력)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(stream_handler)

    # 파일 핸들러 (파일 저장)
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)

    return logger

def setup_restricted_logger(logger_name: str = None):
    """
    제한된 로깅 설정을 초기화하고 로거를 반환합니다.
    특정 로그 파일 생성 제한을 적용합니다.
    
    Args:
        logger_name (str): 로거 이름 (None이면 기본 로거)
    
    Returns:
        logging.Logger: 설정된 로거 객체
    """
    log_dir = "log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 제한된 로그 파일명 (makenaide.log만 생성)
    log_filename = safe_strftime(datetime.now(), "%Y%m%d") + "_makenaide.log"
    log_file_path = os.path.join(log_dir, log_filename)

    log_format = "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"

    # 로거 생성
    if logger_name:
        logger = logging.getLogger(logger_name)
    else:
        logger = logging.getLogger()
    
    logger.setLevel(logging.INFO)

    # 기존 핸들러 삭제
    if logger.hasHandlers():
        logger.handlers.clear()

    # 스트림 핸들러 (터미널 출력)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(stream_handler)

    # 파일 핸들러 (makenaide.log만 사용)
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)

    return logger

def cleanup_old_log_files(retention_days: int = 7):
    """
    지정된 보관 기간을 초과한 로그 파일들을 삭제합니다.
    
    Args:
        retention_days (int): 로그 파일 보관 기간 (일)
    
    Returns:
        dict: 정리 결과 정보
    """
    try:
        log_dir = "log"
        if not os.path.exists(log_dir):
            return {"status": "success", "message": "로그 디렉토리가 존재하지 않음", "deleted_count": 0}
        
        # 현재 시간 기준으로 보관 기간 계산
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        deleted_count = 0
        error_count = 0
        
        # 로그 디렉토리의 모든 파일 검사
        for filename in os.listdir(log_dir):
            file_path = os.path.join(log_dir, filename)
            
            # 파일인지 확인
            if not os.path.isfile(file_path):
                continue
            
            try:
                # 파일 생성 시간 확인
                file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
                
                # 보관 기간을 초과한 파일 삭제
                if file_creation_time < cutoff_date:
                    os.remove(file_path)
                    deleted_count += 1
                    print(f"🗑️ 오래된 로그 파일 삭제: {filename}")
                    
            except Exception as e:
                error_count += 1
                print(f"⚠️ 로그 파일 삭제 중 오류 ({filename}): {e}")
        
        result = {
            "status": "success",
            "deleted_count": deleted_count,
            "error_count": error_count,
            "retention_days": retention_days,
            "cutoff_date": cutoff_date.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        if deleted_count > 0:
            print(f"✅ 로그 파일 정리 완료: {deleted_count}개 파일 삭제")
        else:
            print(f"ℹ️ 삭제할 오래된 로그 파일이 없습니다 (보관기간: {retention_days}일)")
            
        return result
        
    except Exception as e:
        error_result = {
            "status": "error",
            "message": f"로그 파일 정리 중 오류: {str(e)}",
            "deleted_count": 0,
            "error_count": 1
        }
        print(f"❌ 로그 파일 정리 실패: {e}")
        return error_result

def get_log_file_info():
    """
    현재 로그 디렉토리의 파일 정보를 반환합니다.
    
    Returns:
        dict: 로그 파일 정보
    """
    try:
        log_dir = "log"
        if not os.path.exists(log_dir):
            return {"status": "error", "message": "로그 디렉토리가 존재하지 않음"}
        
        log_files = []
        total_size = 0
        
        for filename in os.listdir(log_dir):
            file_path = os.path.join(log_dir, filename)
            
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
                
                log_files.append({
                    "filename": filename,
                    "size_bytes": file_size,
                    "size_mb": round(file_size / (1024 * 1024), 2),
                    "creation_time": file_creation_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "age_days": (datetime.now() - file_creation_time).days
                })
                
                total_size += file_size
        
        # 파일 크기순으로 정렬
        log_files.sort(key=lambda x: x["size_bytes"], reverse=True)
        
        return {
            "status": "success",
            "total_files": len(log_files),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "files": log_files
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"로그 파일 정보 조회 중 오류: {str(e)}"
        }

# 로거 초기화
logger = setup_logger()

# === 현재가 안전 조회 ===
def get_current_price_safe(ticker, retries=3, delay=0.3):
    import time
    import pyupbit
    attempt = 0
    while attempt < retries:
        try:
            price_data = pyupbit.get_current_price(ticker)
            if price_data is None:
                raise ValueError("No data returned")
            if isinstance(price_data, (int, float)):
                return price_data
            if isinstance(price_data, dict):
                if ticker in price_data:
                    return price_data[ticker]
                elif 'trade_price' in price_data:
                    return price_data['trade_price']
                else:
                    first_val = next(iter(price_data.values()), None)
                    if first_val is not None:
                        return first_val
            elif isinstance(price_data, list) and len(price_data) > 0:
                trade_price = price_data[0].get('trade_price')
                if trade_price is not None:
                    return trade_price
            raise ValueError(f"Unexpected data format: {price_data}")
        except Exception as e:
            logging.warning(f"❌ {ticker} 현재가 조회 중 예외 발생: {e}")
            attempt += 1
            time.sleep(delay)
    return None

def retry_on_error(max_retries=3, delay=5):
    """에러 발생 시 재시도하는 데코레이터"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"❌ 최대 재시도 횟수 초과: {str(e)}")
                        raise
                    logger.warning(f"⚠️ 재시도 중... ({attempt + 1}/{max_retries})")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def handle_api_error(e, context=""):
    """API 관련 에러를 처리하고 로깅합니다."""
    logger.error(f"❌ API 에러 발생 ({context}): {str(e)}")
    if hasattr(e, 'response'):
        logger.error(f"응답 상태: {e.response.status_code}")
        logger.error(f"응답 내용: {e.response.text}")

def handle_db_error(e, context=""):
    """DB 관련 에러를 처리하고 로깅합니다."""
    logger.error(f"❌ DB 에러 발생 ({context}): {str(e)}")

def handle_network_error(e, context=""):
    """네트워크 관련 에러를 처리하고 로깅합니다."""
    logger.error(f"❌ 네트워크 에러 발생 ({context}): {str(e)}")

def load_blacklist():
    """블랙리스트를 로드합니다. (에러 처리 강화)"""
    try:
        blacklist_path = 'blacklist.json'
        
        # 파일 존재 여부 확인
        if not os.path.exists(blacklist_path):
            logger.warning(f"⚠️ 블랙리스트 파일이 존재하지 않습니다: {blacklist_path}")
            # 빈 블랙리스트 파일 생성
            try:
                with open(blacklist_path, 'w', encoding='utf-8') as f:
                    json.dump({}, f, ensure_ascii=False, indent=2)
                logger.info(f"✅ 빈 블랙리스트 파일 생성 완료: {blacklist_path}")
                return {}
            except Exception as create_e:
                logger.error(f"❌ 블랙리스트 파일 생성 실패: {create_e}")
                return {}
        
        # 파일 읽기 권한 확인
        if not os.access(blacklist_path, os.R_OK):
            logger.error(f"❌ 블랙리스트 파일 읽기 권한 없음: {blacklist_path}")
            return {}
        
        # 파일 크기 확인
        file_size = os.path.getsize(blacklist_path)
        if file_size == 0:
            logger.warning(f"⚠️ 블랙리스트 파일이 비어있습니다: {blacklist_path}")
            return {}
        
        # JSON 파일 로드
        with open(blacklist_path, 'r', encoding='utf-8') as f:
            blacklist_data = json.load(f)
        
        # 데이터 타입 검증
        if not isinstance(blacklist_data, dict):
            logger.error(f"❌ 블랙리스트 파일 형식 오류: dict 타입이 아님 (현재: {type(blacklist_data)})")
            return {}
        
        # 블랙리스트 내용 검증
        valid_blacklist = {}
        invalid_entries = []
        
        for ticker, info in blacklist_data.items():
            # 티커 형식 검증
            if not isinstance(ticker, str) or not ticker.startswith('KRW-'):
                invalid_entries.append(f"{ticker}: 잘못된 티커 형식")
                continue
            
            # 정보 구조 검증
            if isinstance(info, dict):
                if 'reason' in info and 'added' in info:
                    valid_blacklist[ticker] = info
                else:
                    # 구조가 불완전한 경우 기본값으로 보완
                    valid_blacklist[ticker] = {
                        'reason': info.get('reason', '사유 없음'),
                        'added': info.get('added', datetime.now().isoformat())
                    }
                    logger.warning(f"⚠️ {ticker} 블랙리스트 정보 불완전, 기본값으로 보완")
            elif isinstance(info, str):
                # 구버전 호환성 (사유만 문자열로 저장된 경우)
                valid_blacklist[ticker] = {
                    'reason': info,
                    'added': datetime.now().isoformat()
                }
                logger.info(f"🔄 {ticker} 블랙리스트 구버전 형식 변환")
            else:
                invalid_entries.append(f"{ticker}: 잘못된 정보 형식")
        
        # 잘못된 항목 로그
        if invalid_entries:
            logger.warning(f"⚠️ 블랙리스트 잘못된 항목 {len(invalid_entries)}개 발견:")
            for entry in invalid_entries[:5]:  # 최대 5개만 표시
                logger.warning(f"   - {entry}")
            if len(invalid_entries) > 5:
                logger.warning(f"   - ... 외 {len(invalid_entries) - 5}개 더")
        
        # 성공 로그
        logger.info(f"✅ 블랙리스트 로드 완료: {len(valid_blacklist)}개 항목 (유효: {len(valid_blacklist)}, 무효: {len(invalid_entries)})")
        
        return valid_blacklist
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ 블랙리스트 JSON 파싱 오류: {e}")
        logger.error(f"   파일 위치: {os.path.abspath('blacklist.json')}")
        
        # 백업 파일 생성 시도
        try:
            backup_path = f"blacklist_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            import shutil
            shutil.copy2('blacklist.json', backup_path)
            logger.info(f"📋 손상된 블랙리스트 파일 백업 완료: {backup_path}")
        except Exception as backup_e:
            logger.error(f"❌ 백업 파일 생성 실패: {backup_e}")
        
        return {}
        
    except PermissionError as e:
        logger.error(f"❌ 블랙리스트 파일 접근 권한 오류: {e}")
        return {}
        
    except Exception as e:
        logger.error(f"❌ 블랙리스트 로드 중 예상치 못한 오류: {e}")
        logger.error(f"   파일 위치: {os.path.abspath('blacklist.json')}")
        
        # 상세 디버깅 정보
        try:
            import traceback
            logger.debug(f"🔍 상세 오류 정보:\n{traceback.format_exc()}")
        except:
            pass
        
        return {}

# === UNUSED: 블랙리스트 관리 함수들 ===
# def add_to_blacklist(ticker: str, reason: str) -> bool:
#     """
#     블랙리스트에 티커를 추가합니다.
#     
#     Args:
#         ticker (str): 추가할 티커
#         reason (str): 추가 사유
#         
#     Returns:
#         bool: 성공 여부
#     """
#     try:
#         blacklist_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "blacklist.json")
#         blacklist = load_blacklist()
#         
#         # 현재 UTC 시간
#         now = datetime.now(pytz.UTC)
#         
#         # 티커 추가
#         blacklist[ticker] = {
#             "reason": reason,
#             "added": now.isoformat()
#         }
#         
#         # JSON 파일 저장
#         with open(blacklist_path, 'w', encoding='utf-8') as f:
#             json.dump(blacklist, f, ensure_ascii=False, indent=2)
#             
#         logger.info(f"✅ {ticker} 블랙리스트 추가 완료 (사유: {reason})")
#         return True
#         
#     except Exception as e:
#         logger.error(f"❌ {ticker} 블랙리스트 추가 중 오류 발생: {e}")
#         return False

# def remove_from_blacklist(ticker: str) -> bool:
#     """
#     블랙리스트에서 티커를 제거합니다.
#     
#     Args:
#         ticker (str): 제거할 티커
#         
#     Returns:
#         bool: 성공 여부
#     """
#     try:
#         blacklist_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "blacklist.json")
#         blacklist = load_blacklist()
#         
#         if ticker not in blacklist:
#             logger.warning(f"⚠️ {ticker}는 블랙리스트에 없습니다.")
#             return False
#             
#         # 티커 제거
#         del blacklist[ticker]
#         
#         # JSON 파일 저장
#         with open(blacklist_path, 'w', encoding='utf-8') as f:
#             json.dump(blacklist, f, ensure_ascii=False, indent=2)
#             
#         logger.info(f"✅ {ticker} 블랙리스트 제거 완료")
#         return True
#         
#     except Exception as e:
#         logger.error(f"❌ {ticker} 블랙리스트 제거 중 오류 발생: {e}")
#         return False

# === 테이블 스키마 매핑 정규화 ===
COLUMN_MAPPING = {
    'static_indicators': {
        # 피벗 포인트 매핑
        'resistance_1': 'r1',
        'resistance_2': 'r2', 
        'resistance_3': 'r3',
        'support_1': 's1',
        'support_2': 's2',
        'support_3': 's3',
        
        # 피보나치 매핑 (실제 DB 스키마에 존재하는 컬럼만)
        'fib_382': 'fibo_382',
        'fib_618': 'fibo_618',
        
        # 기타 매핑
        'ma200': 'ma_200',
        'volume_ratio': 'volume_change_7_30'
    },
    'ohlcv': {
        # OHLCV 기본 컬럼 매핑
        'open_price': 'open',
        'high_price': 'high',
        'low_price': 'low',
        'close_price': 'close',
        'trading_volume': 'volume',
        'trade_date': 'date',
        
        # 역호환성을 위한 MACD 매핑
        'macd_hist': 'macd_histogram'
    }
}

def apply_column_mapping(df, table_name):
    """
    DataFrame의 컬럼명을 테이블 스키마에 맞게 매핑합니다.
    
    Args:
        df (pd.DataFrame): 매핑할 DataFrame
        table_name (str): 대상 테이블명
        
    Returns:
        pd.DataFrame: 컬럼명이 매핑된 DataFrame
    """
    if table_name not in COLUMN_MAPPING:
        logger.debug(f"⚠️ {table_name} 테이블에 대한 컬럼 매핑이 정의되지 않음")
        return df
        
    mapping = COLUMN_MAPPING[table_name]
    
    # 실제 존재하는 컬럼만 매핑
    columns_to_rename = {}
    for old_col, new_col in mapping.items():
        if old_col in df.columns:
            columns_to_rename[old_col] = new_col
            
    if columns_to_rename:
        df_mapped = df.rename(columns=columns_to_rename)
        logger.debug(f"✅ {table_name} 테이블 컬럼 매핑 적용: {list(columns_to_rename.keys())} → {list(columns_to_rename.values())}")
        return df_mapped
    else:
        logger.debug(f"ℹ️ {table_name} 테이블에 매핑할 컬럼이 없음")
        return df

# === UNUSED: 컬럼 매핑 함수 ===
# def get_mapped_column_name(table_name, column_name):
#     """
#     특정 테이블의 컬럼명을 매핑된 이름으로 반환합니다.
#     
#     Args:
#         table_name (str): 테이블명
#         column_name (str): 원본 컬럼명
#         
#     Returns:
#         str: 매핑된 컬럼명 또는 원본 컬럼명
#     """
#     if table_name in COLUMN_MAPPING and column_name in COLUMN_MAPPING[table_name]:
#         return COLUMN_MAPPING[table_name][column_name]
#     return column_name

# === Phase 2: 추가 안전장치 구현 ===

# === UNUSED: datetime 관련 함수들 ===
# def safe_dataframe_index_format(df, format_str='%Y-%m-%d'):
#     """
#     DataFrame 인덱스의 datetime 안전성을 보장하는 래퍼 함수
#     
#     Args:
#         df (pd.DataFrame): 처리할 DataFrame
#         format_str (str): 날짜 포맷 문자열
#         
#     Returns:
#         pd.DataFrame: 안전하게 포맷된 인덱스를 가진 DataFrame
#     """
#     try:
#         if df is None or df.empty:
#             return df
#             
#         # 인덱스가 datetime 타입인지 확인
#         if hasattr(df.index, 'strftime'):
#             # safe_strftime을 사용하여 각 인덱스 값을 안전하게 변환
#             safe_index = [safe_strftime(idx, format_str) for idx in df.index]
#             df_copy = df.copy()
#             df_copy.index = safe_index
#             return df_copy
#         else:
#             # datetime이 아닌 경우 그대로 반환
#             return df
#             
#     except Exception as e:
#         logger.error(f"❌ DataFrame 인덱스 포맷 중 오류 발생: {e}")
#         return df

# def safe_db_datetime_insert(datetime_value, format_str='%Y-%m-%d %H:%M:%S'):
#     """
#     DB 삽입용 datetime 안전 변환 함수
#     
#     Args:
#         datetime_value: 변환할 datetime 값
#         format_str (str): DB 삽입용 포맷 문자열
#         
#     Returns:
#         str: DB 삽입 가능한 안전한 datetime 문자열
#     """
#     try:
#         # None 체크
#         if datetime_value is None:
#             return None
#             
#         # safe_strftime을 사용하여 안전하게 변환
#         result = safe_strftime(datetime_value, format_str)
#         
#         # "N/A"나 "Invalid Date"인 경우 None 반환
#         if result in ["N/A", "Invalid Date"]:
#             return None
#             
#         return result
#         
#     except Exception as e:
#         logger.error(f"❌ DB datetime 변환 중 오류 발생: {e}")
#         return None

# def safe_log_datetime_format(datetime_value=None, format_str='%Y-%m-%d %H:%M:%S'):
#     """
#     로그용 datetime 안전 포맷 함수
#     
#     Args:
#         datetime_value: 변환할 datetime 값 (None이면 현재 시간 사용)
#         format_str (str): 로그용 포맷 문자열
#         
#     Returns:
#         str: 로그에 안전하게 사용할 수 있는 datetime 문자열
#     """
#     try:
#         # datetime_value가 None이면 현재 시간 사용
#         if datetime_value is None:
#             datetime_value = datetime.now()
#             
#         # safe_strftime을 사용하여 안전하게 변환
#         result = safe_strftime(datetime_value, format_str)
#         
#         # "N/A"나 "Invalid Date"인 경우 현재 시간으로 대체
#         if result in ["N/A", "Invalid Date"]:
#             result = safe_strftime(datetime.now(), format_str)
#         
#         return result
#         
#     except Exception as e:
#         logger.error(f"❌ 로그 datetime 포맷 중 오류 발생: {e}")
#         return safe_strftime(datetime.now(), format_str)

# def safe_datetime_operations():
#     """
#     DataFrame 전체의 datetime 안전성을 보장하는 래퍼 함수들의 통합 인터페이스
#     
#     Returns:
#         dict: 사용 가능한 안전 함수들의 딕셔너리
#     """
#     return {
#         'safe_strftime': safe_strftime,
#         'safe_dataframe_index_format': safe_dataframe_index_format,
#         'safe_db_datetime_insert': safe_db_datetime_insert,
#         'safe_log_datetime_format': safe_log_datetime_format
#     }

# === Phase 3: 모니터링 시스템 ===

# === UNUSED: DatetimeErrorMonitor 클래스 및 관련 함수들 ===
# class DatetimeErrorMonitor:
#     """
#     실행 중 datetime 관련 오류를 실시간 감지하고 로깅하는 클래스
#     """
#     
#     def __init__(self):
#         self.error_count = 0
#         self.error_patterns = {}
#         self.start_time = datetime.now()
#         
#     def log_datetime_error(self, error, context="", data=None):
#         """
#         datetime 오류를 로깅하고 패턴을 분석합니다.
#         
#         Args:
#             error (Exception): 발생한 오류
#             context (str): 오류 발생 컨텍스트
#             data: 오류 발생 시의 데이터
#         """
#         try:
#             self.error_count += 1
#             error_type = type(error).__name__
#             error_msg = str(error)
#             
#             # 오류 패턴 분석
#             pattern_key = f"{error_type}:{context}"
#             if pattern_key not in self.error_patterns:
#                 self.error_patterns[pattern_key] = {
#                     'count': 0,
#                     'first_occurrence': datetime.now(),
#                     'last_occurrence': None,
#                     'sample_data': str(data)[:200] if data else None
#                 }
#             
#             self.error_patterns[pattern_key]['count'] += 1
#             self.error_patterns[pattern_key]['last_occurrence'] = datetime.now()
#             
#             # 로깅
#             logger.error(f"🚨 DATETIME ERROR #{self.error_count} | {context} | {error_type}: {error_msg}")
#             if data:
#                 logger.error(f"📊 Error Data Sample: {str(data)[:200]}")
#                 
#             # 임계치 초과 시 알림
#             if self.error_patterns[pattern_key]['count'] >= 5:
#                 self._send_alert(pattern_key, self.error_patterns[pattern_key])
#                 
#         except Exception as e:
#             logger.error(f"❌ DatetimeErrorMonitor 자체 오류: {e}")
#     
#     def _send_alert(self, pattern_key, pattern_info):
#         """
#         오류 패턴이 임계치를 초과했을 때 알림을 발송합니다.
#         
#         Args:
#             pattern_key (str): 오류 패턴 키
#             pattern_info (dict): 오류 패턴 정보
#         """
#         try:
#             alert_msg = f"""
# 🚨 DATETIME ERROR ALERT 🚨
# 패턴: {pattern_key}
# 발생 횟수: {pattern_info['count']}
# 최초 발생: {safe_log_datetime_format(pattern_info['first_occurrence'])}
# 최근 발생: {safe_log_datetime_format(pattern_info['last_occurrence'])}
# 샘플 데이터: {pattern_info['sample_data']}
#             """
#             logger.critical(alert_msg)
#             
#         except Exception as e:
#             logger.error(f"❌ 알림 발송 중 오류: {e}")
#     
#     def generate_report(self):
#         """
#         오류 패턴 분석 보고서를 생성합니다.
#         
#         Returns:
#             str: 분석 보고서
#         """
#         try:
#             runtime = datetime.now() - self.start_time
#             
#             report = f"""
# 📊 DATETIME ERROR MONITORING REPORT 📊
# 실행 시간: {safe_log_datetime_format(self.start_time)} ~ {safe_log_datetime_format()}
# 총 실행 시간: {runtime}
# 총 오류 발생 횟수: {self.error_count}
# 
# === 오류 패턴 분석 ===
# """
#             
#             for pattern_key, pattern_info in self.error_patterns.items():
#                 report += f"""
# 패턴: {pattern_key}
# - 발생 횟수: {pattern_info['count']}
# - 최초 발생: {safe_log_datetime_format(pattern_info['first_occurrence'])}
# - 최근 발생: {safe_log_datetime_format(pattern_info['last_occurrence'])}
# - 샘플 데이터: {pattern_info['sample_data']}
# """
#             
#             return report
#             
#         except Exception as e:
#             logger.error(f"❌ 보고서 생성 중 오류: {e}")
#             return f"보고서 생성 실패: {e}"

# # 전역 모니터 인스턴스
# datetime_monitor = DatetimeErrorMonitor()

# def datetime_error_monitor():
#     """
#     실행 중 datetime 관련 오류를 실시간 감지하고 로깅
#     - 예외 발생 시 자동 알림
#     - 오류 패턴 분석 및 보고서 생성
#     
#     Returns:
#         DatetimeErrorMonitor: 모니터 인스턴스
#     """
#     return datetime_monitor

# def safe_strftime_with_monitoring(date_obj, format_str='%Y-%m-%d', context=""):
#     """
#     모니터링이 포함된 safe_strftime 함수
#     
#     Args:
#         date_obj: 변환할 날짜 객체
#         format_str (str): 날짜 포맷 문자열
#         context (str): 호출 컨텍스트 (모니터링용)
#         
#     Returns:
#         str: 포맷된 날짜 문자열
#     """
#     try:
#         return safe_strftime(date_obj, format_str)
#     except Exception as e:
#         datetime_monitor.log_datetime_error(e, context, date_obj)
#         # 오류 발생 시에도 안전한 값 반환
#         return "N/A"

# === 티커 필터링 시스템 검증 함수 ===
def validate_ticker_filtering_system():
    """
    티커 필터링 시스템의 다층 검증을 수행하는 함수
    
    검증 항목:
    1. tickers 테이블의 is_active 컬럼 존재 여부
    2. 블랙리스트 파일 접근 가능성
    3. 두 필터링 방식의 결과 비교
    """
    results = {
        "is_active_available": False,
        "blacklist_available": False,
        "filtering_consistency": False,
        "active_count": 0,
        "blacklist_filtered_count": 0,
        "consistency_rate": 0.0
    }
    
    try:
        # 1. is_active 컬럼 존재 확인
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'tickers' AND column_name = 'is_active'
        """)
        results["is_active_available"] = len(cursor.fetchall()) > 0
        
        # 2. 블랙리스트 접근 확인
        blacklist = load_blacklist()
        results["blacklist_available"] = blacklist is not None
        
        # 3. 두 방식 결과 비교 (is_active 컬럼이 있을 때만)
        if results["is_active_available"]:
            cursor.execute("SELECT ticker FROM tickers WHERE is_active = true")
            active_tickers = {row[0] for row in cursor.fetchall()}
            results["active_count"] = len(active_tickers)
            
            cursor.execute("SELECT ticker FROM tickers")
            all_tickers = {row[0] for row in cursor.fetchall()}
            
            blacklist_filtered = all_tickers - set(blacklist if blacklist else [])
            results["blacklist_filtered_count"] = len(blacklist_filtered)
            
            # 결과 일치도 검사
            overlap = len(active_tickers & blacklist_filtered)
            total = len(active_tickers | blacklist_filtered)
            consistency_rate = overlap / total if total > 0 else 0
            results["consistency_rate"] = consistency_rate
            
            results["filtering_consistency"] = consistency_rate > 0.8
        
        cursor.close()
        conn.close()
        return results
        
    except Exception as e:
        logger.error(f"❌ 티커 필터링 검증 중 오류: {e}")
        return results

# === UNUSED: 피보나치 컬럼 조회 함수 ===
# def get_safe_fibo_columns():
#     """실제 존재하는 피보나치 컬럼만 반환"""
#     try:
#         import psycopg2
#         import os
#         
#         conn = psycopg2.connect(
#             host=os.getenv("PG_HOST"),
#             user=os.getenv("PG_USER"),
#             password=os.getenv("PG_PASSWORD"),
#             port=os.getenv("PG_PORT"),
#             dbname=os.getenv("PG_DATABASE"),
#         )
#         cursor = conn.cursor()
#         
#         cursor.execute("""
#             SELECT column_name 
#             FROM information_schema.columns 
#             WHERE table_name = 'ohlcv' 
#             AND column_name LIKE 'fibo_%'
#             ORDER BY column_name
#         """)
#         available_fibo_columns = [row[0] for row in cursor.fetchall()]
#         cursor.close()
#         conn.close()
#         
#         # 실제 DB 스키마에 존재하는 피보나치 레벨들만
#         required_fibo_levels = ['fibo_382', 'fibo_618']
#         
#         # 실제 존재하는 컬럼만 반환
#         safe_columns = [col for col in required_fibo_levels if col in available_fibo_columns]
#         
#         logger.info(f"✅ 사용 가능한 피보나치 컬럼: {safe_columns}")
#         return safe_columns
#         
#     except Exception as e:
#         logger.error(f"❌ 피보나치 컬럼 확인 실패: {e}")
#         # 안전한 기본값 반환 (실제 DB 스키마 기준)
#         return ['fibo_382', 'fibo_618']

def get_safe_ohlcv_columns():
    """
    실제 ohlcv 테이블에 존재하는 컬럼들을 반환하는 안전한 함수
    
    Returns:
        list: 실제 존재하는 ohlcv 테이블 컬럼 목록
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'ohlcv' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        logger = setup_logger()
        logger.debug(f"📊 get_safe_ohlcv_columns() 조회 성공: {len(columns)}개 컬럼")
        
        return columns
        
    except Exception as e:
        logger = setup_logger()
        logger.error(f"❌ ohlcv 컬럼 조회 실패: {e}")
        # 기본 컬럼 반환
        return ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']

# === UNUSED: DB 쿼리 생성 함수들 ===
# def build_safe_ohlcv_query(ticker_param=True, limit_days=30):
#     """
#     수정 목표: static_indicators로 이동된 컬럼들을 ohlcv 쿼리에서 제외
#     
#     수정 내용:
#     1. problematic_columns 리스트에 'pivot', 'r1', 's1', 'support', 'resistance' 추가
#     2. 이 컬럼들을 possible_indicators에서 제거
#     3. 로깅에 "정적 지표 제외됨" 메시지 추가
#     """
#     safe_columns = get_safe_ohlcv_columns()
#     safe_fibo_columns = [col for col in safe_columns if col.startswith('fibo_')]
#     
#     # 필수 컬럼들 (확실히 존재함)
#     essential_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
#     
#     # 동적 지표 컬럼들 (ohlcv 테이블에 존재하는 것만)
#     possible_dynamic_indicators = [
#         'ht_trendline', 'ma_50', 'ma_200', 'bb_upper', 'bb_lower', 
#         'donchian_high', 'donchian_low', 'macd_histogram', 'rsi_14', 
#         'volume_20ma', 'stoch_k', 'stoch_d', 'cci'
#     ]
#     
#     # ⚠️ 정적 지표 컬럼들은 static_indicators 테이블에 있으므로 제외
#     # pivot, r1, s1, support, resistance 추가
#     static_indicators_columns = ['pivot', 'r1', 's1', 'support', 'resistance', 'atr', 'adx']
#     
#     # 실제 존재하는 동적 지표 컬럼만 필터링 (정적 지표는 제외)
#     safe_dynamic_indicators = [
#         col for col in possible_dynamic_indicators 
#         if col in safe_columns and col not in static_indicators_columns
#     ]
#     
#     # 최종 컬럼 목록 구성 (ohlcv 테이블의 안전한 컬럼만)
#     all_safe_columns = essential_columns + safe_dynamic_indicators + safe_fibo_columns
#     columns_str = ', '.join(all_safe_columns)
#     
#     if ticker_param:
#         query = f"""
#             SELECT {columns_str}
#             FROM ohlcv 
#             WHERE ticker = %s 
#             AND date >= CURRENT_DATE - INTERVAL '{limit_days} days'
#             ORDER BY date DESC
#             LIMIT {limit_days}
#         """
#     else:
#         query = f"""
#             SELECT {columns_str}
#             FROM ohlcv 
#             WHERE date >= CURRENT_DATE - INTERVAL '{limit_days} days'
#             ORDER BY date DESC
#             LIMIT {limit_days}
#         """
#     
#     logger.info(f"✅ 안전한 ohlcv 쿼리 생성: {len(all_safe_columns)}개 컬럼 사용 (정적 지표 제외됨)")
#     logger.debug(f"   - 사용된 컬럼: {all_safe_columns}")
#     logger.debug(f"   - 제외된 정적 지표: {static_indicators_columns}")
#     
#     return query, safe_fibo_columns

# def get_safe_static_indicators_columns():
#     """static_indicators 테이블에 실제 존재하는 모든 컬럼을 조회"""
#     try:
#         import psycopg2
#         import os
#         
#         conn = psycopg2.connect(
#             host=os.getenv("PG_HOST"),
#             user=os.getenv("PG_USER"),
#             password=os.getenv("PG_PASSWORD"),
#             port=os.getenv("PG_PORT"),
#             dbname=os.getenv("PG_DATABASE"),
#         )
#         cursor = conn.cursor()
#         
#         cursor.execute("""
#             SELECT column_name 
#             FROM information_schema.columns 
#             WHERE table_name = 'static_indicators' 
#             ORDER BY column_name
#         """)
#         all_columns = [row[0] for row in cursor.fetchall()]
#         cursor.close()
#         conn.close()
#         
#         logger.info(f"✅ static_indicators 테이블 전체 컬럼 조회 성공: {len(all_columns)}개")
#         return all_columns
#         
#     except Exception as e:
#         logger.error(f"❌ static_indicators 테이블 컬럼 조회 실패: {e}")
#         # 기본적으로 확실히 존재하는 컬럼들만 반환
#         return ['ticker', 'pivot', 'r1', 's1', 'support', 'resistance', 'atr', 'adx']

# def build_safe_static_indicators_query(ticker_param=True):
#     """static_indicators 테이블용 안전한 SELECT 쿼리 생성"""
#     safe_columns = get_safe_static_indicators_columns()
#     
#     # 필수 컬럼 (ticker는 항상 포함)
#     essential_columns = ['ticker']
#     
#     # 정적 지표 컬럼들 (존재하는 것만 선택)
#     possible_static_indicators = [
#         'pivot', 'r1', 's1', 'support', 'resistance', 'atr', 'adx',
#         'ma200_slope', 'nvt_relative', 'volume_change_7_30', 'price',
#         'high_60', 'low_60', 'supertrend_signal'
#     ]
#     
#     # 실제 존재하는 정적 지표 컬럼만 필터링
#     safe_static_indicators = [col for col in possible_static_indicators if col in safe_columns]
#     
#     # 최종 컬럼 목록 구성
#     all_safe_columns = essential_columns + safe_static_indicators
#     columns_str = ', '.join(all_safe_columns)
#     
#     if ticker_param:
#         query = f"""
#             SELECT {columns_str}
#             FROM static_indicators 
#             WHERE ticker = %s
#         """
#     else:
#         query = f"""
#             SELECT {columns_str}
#             FROM static_indicators
#         """
#     
#     logger.info(f"✅ 안전한 static_indicators 쿼리 생성: {len(all_safe_columns)}개 컬럼 사용")
#     logger.debug(f"   - 사용된 컬럼: {all_safe_columns}")
#     
#     return query, safe_static_indicators

def get_combined_ohlcv_and_static_data(ticker, limit_days=30):
    """
    ohlcv와 static_indicators를 안전하게 조합하여 데이터를 조회하는 함수
    - ohlcv에서 동적 지표와 기본 OHLCV 데이터
    - static_indicators에서 pivot, r1, s1 등 정적 지표
    """
    try:
        # 1. ohlcv 데이터 조회 (안전한 쿼리 사용)
        ohlcv_query, _ = build_safe_ohlcv_query(ticker_param=True, limit_days=limit_days)
        
        # 2. static_indicators 데이터 조회 (안전한 쿼리 사용) 
        static_query, static_columns = build_safe_static_indicators_query(ticker_param=True)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 3. ohlcv 데이터 실행
        cursor.execute(ohlcv_query, (ticker,))
        ohlcv_rows = cursor.fetchall()
        
        # 4. static_indicators 데이터 실행
        cursor.execute(static_query, (ticker,))
        static_rows = cursor.fetchone()  # 단일 행 (최신 정적 지표)
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ {ticker} 통합 데이터 조회 성공: OHLCV {len(ohlcv_rows)}행, 정적지표 {len(static_columns)}개 컬럼")
        
        return {
            'ohlcv_data': ohlcv_rows,
            'static_data': static_rows,
            'static_columns': static_columns
        }
        
    except Exception as e:
        logger.error(f"❌ {ticker} 통합 데이터 조회 실패: {e}")
        return {
            'ohlcv_data': [],
            'static_data': None,
            'static_columns': []
        }

# utils.py에 추가할 범용 타입 변환 함수

# === UNUSED: DB 쿼리 결과 검증 및 타입 변환 통계 함수들 ===
# def validate_db_query_results(results, expected_types, context=""):
#     """
#     DB 쿼리 결과의 타입 일관성을 검증합니다.
#     
#     Args:
#         results: DB 쿼리 결과 (리스트 또는 튜플의 리스트)
#         expected_types: 기대되는 타입들의 리스트
#         context: 로깅용 컨텍스트
#         
#     Returns:
#         dict: 검증 결과 통계
#     """
#     validation_stats = {
#         "total_rows": 0,
#         "total_columns": 0,
#         "type_mismatches": 0,
#         "none_values": 0,
#         "problematic_columns": set()
#     }
#     
#     if not results:
#         logger.warning(f"⚠️ {context} DB 쿼리 결과가 비어있습니다.")
#         return validation_stats
#     
#     validation_stats["total_rows"] = len(results)
#     validation_stats["total_columns"] = len(expected_types) if expected_types else 0
#     
#     for row_idx, row in enumerate(results):
#         if len(row) != len(expected_types):
#             logger.warning(f"⚠️ {context} Row {row_idx}: 컬럼 수 불일치 (실제: {len(row)}, 기대: {len(expected_types)})")
#             continue
#             
#         for col_idx, (value, expected_type) in enumerate(zip(row, expected_types)):
#             if value is None:
#                 validation_stats["none_values"] += 1
#                 continue
#                 
#             # pandas 객체나 datetime 계열 특별 처리
#             if expected_type in [float, int] and isinstance(value, (datetime, date)):
#                 validation_stats["type_mismatches"] += 1
#                 validation_stats["problematic_columns"].add(col_idx)
#                 logger.warning(f"⚠️ {context} Row {row_idx}, Col {col_idx}: datetime 타입을 숫자로 사용 시도 - 값: {value}")
#                 
#             elif not isinstance(value, expected_type) and expected_type is not None:
#                 # 타입 변환 가능성 체크
#                 if expected_type == float:
#                     try:
#                         float(value)  # 변환 가능하면 문제없음
#                     except (ValueError, TypeError):
#                         validation_stats["type_mismatches"] += 1
#                         validation_stats["problematic_columns"].add(col_idx)
#                         logger.warning(f"⚠️ {context} Row {row_idx}, Col {col_idx}: 예상치 못한 타입 - 값: {value} (타입: {type(value)}, 기대: {expected_type})")
#                 elif expected_type == int:
#                     try:
#                         int(float(value))  # float를 거쳐 int 변환 시도
#                     except (ValueError, TypeError):
#                         validation_stats["type_mismatches"] += 1
#                         validation_stats["problematic_columns"].add(col_idx)
#                         logger.warning(f"⚠️ {context} Row {row_idx}, Col {col_idx}: 예상치 못한 타입 - 값: {value} (타입: {type(value)}, 기대: {expected_type})")
#                 else:
#                     validation_stats["type_mismatches"] += 1
#                     validation_stats["problematic_columns"].add(col_idx)
#                     logger.warning(f"⚠️ {context} Row {row_idx}, Col {col_idx}: 예상치 못한 타입 - 값: {value} (타입: {type(value)}, 기대: {expected_type})")
#     
#     # 검증 결과 요약 로그
#     if validation_stats["type_mismatches"] > 0:
#         logger.warning(f"⚠️ {context} 타입 검증 완료: {validation_stats['type_mismatches']}개 불일치 발견")
#     else:
#         logger.info(f"✅ {context} 타입 검증 통과: 모든 데이터 타입 일치")
#     
#     return validation_stats

# 타입 변환 모니터링을 위한 글로벌 카운터
_conversion_stats = {
    "total_calls": 0,
    "successful_conversions": 0,
    "datetime_detections": 0,
    "conversion_failures": 0,
    "start_time": datetime.now()
}

# def get_conversion_stats():
#     """타입 변환 통계 조회"""
#     runtime = datetime.now() - _conversion_stats["start_time"]
#     return {
#         **_conversion_stats,
#         "runtime_seconds": runtime.total_seconds(),
#         "success_rate": (_conversion_stats["successful_conversions"] / _conversion_stats["total_calls"] * 100) if _conversion_stats["total_calls"] > 0 else 0,
#         "datetime_detection_rate": (_conversion_stats["datetime_detections"] / _conversion_stats["total_calls"] * 100) if _conversion_stats["total_calls"] > 0 else 0
#     }

# def reset_conversion_stats():
#     """타입 변환 통계 초기화"""
#     global _conversion_stats
#     _conversion_stats = {
#         "total_calls": 0,
#         "successful_conversions": 0,
#         "datetime_detections": 0,
#         "conversion_failures": 0,
#         "start_time": datetime.now()
#     }

def safe_float_convert(value, default=0.0, context=""):
    """
    모든 타입의 값을 안전하게 float로 변환합니다. (모니터링 기능 추가)
    
    Args:
        value: 변환할 값
        default (float): 변환 실패 시 기본값
        context (str): 로깅용 컨텍스트
        
    Returns:
        float: 변환된 값 또는 기본값
    """
    global _conversion_stats
    _conversion_stats["total_calls"] += 1
    
    if value is None:
        _conversion_stats["successful_conversions"] += 1
        return default
        
    # datetime 계열 객체는 변환하지 않음
    if isinstance(value, (datetime, date)):
        _conversion_stats["datetime_detections"] += 1
        logger.warning(f"⚠️ {context} datetime 객체를 float로 변환 시도: {value} -> {default}")
        return default
        
    # pandas Timestamp도 체크
    if hasattr(value, '__class__') and 'pandas' in str(type(value)):
        _conversion_stats["datetime_detections"] += 1
        logger.warning(f"⚠️ {context} pandas 객체를 float로 변환 시도: {value} -> {default}")
        return default
        
    try:
        result = float(value)
        _conversion_stats["successful_conversions"] += 1
        return result
    except (ValueError, TypeError) as e:
        _conversion_stats["conversion_failures"] += 1
        logger.warning(f"⚠️ {context} float 변환 실패: {value} (타입: {type(value)}) -> {default}")
        return default

# === UNUSED: 타입 변환 통계 요약 함수 ===
# def log_conversion_stats_summary():
#     """타입 변환 통계 요약 로그 출력"""
#     stats = get_conversion_stats()
#     logger.info("=" * 50)
#     logger.info("📊 타입 변환 모니터링 요약")
#     logger.info("=" * 50)
#     logger.info(f"총 변환 시도: {stats['total_calls']}회")
#     logger.info(f"성공적 변환: {stats['successful_conversions']}회 ({stats['success_rate']:.1f}%)")
#     logger.info(f"datetime 감지: {stats['datetime_detections']}회 ({stats['datetime_detection_rate']:.1f}%)")
#     logger.info(f"변환 실패: {stats['conversion_failures']}회")
#     logger.info(f"실행 시간: {stats['runtime_seconds']:.1f}초")
#     
#     if stats['datetime_detections'] > 0:
#         logger.warning(f"⚠️ datetime 타입이 {stats['datetime_detections']}회 감지되었습니다. 데이터 품질을 확인해주세요.")
#     
#     return stats

def safe_int_convert(value, default=0, context=""):
    """
    모든 타입의 값을 안전하게 int로 변환합니다.
    
    Args:
        value: 변환할 값
        default (int): 변환 실패 시 기본값
        context (str): 로깅용 컨텍스트
        
    Returns:
        int: 변환된 값 또는 기본값
    """
    if value is None:
        return default
        
    # datetime 계열 객체는 변환하지 않음
    if isinstance(value, (datetime, date)):
        logger.warning(f"⚠️ {context} datetime 객체를 int로 변환 시도: {value} -> {default}")
        return default
        
    try:
        return int(float(value))  # float를 거쳐서 int로 변환
    except (ValueError, TypeError) as e:
        logger.warning(f"⚠️ {context} int 변환 실패: {value} (타입: {type(value)}) -> {default}")
        return default

# === UNUSED: TypeConverter 클래스 및 관련 함수들 ===
# # ===== 간소화된 타입 변환기 (refactored_type_converter.py 통합) =====

# class TypeConverter:
#     """간소화된 타입 변환기 클래스 - 50라인 이하로 압축"""
#     
#     @staticmethod
#     def safe_convert(value, target_type, context=""):
#         """
#         datetime 감지 + 타입 변환 + JSON 호환성을 한 번에 처리
#         
#         Args:
#             value: 변환할 값
#             target_type: 목표 타입 ('float', 'int', 'date', 'str')
#             context: 변환 컨텍스트 (로깅용)
#             
#         Returns:
#             변환된 값 또는 기본값
#         """
#         if value is None:
#             return None
#             
#         try:
#             # datetime 감지 + 처리
#             if isinstance(value, (datetime, date, pd.Timestamp)):
#                 if target_type == 'date':
#                     return value.strftime('%Y-%m-%d')
#                 else:
#                     logger.warning(f"datetime in {target_type} column: {context}")
#                     return 0.0
#             
#             # 간단한 타입 변환
#             if target_type == 'float':
#                 converted = float(value)
#                 return converted.item() if hasattr(converted, 'item') else converted
#             elif target_type == 'int':
#                 converted = int(float(value))
#                 return converted.item() if hasattr(converted, 'item') else converted
#             elif target_type == 'date':
#                 return safe_strftime(value, '%Y-%m-%d')
#             else:
#                 return str(value)
#                 
#         except Exception as e:
#             logger.debug(f"변환 실패 {context}: {e}")
#             return {'float': 0.0, 'int': 0, 'date': '1970-01-01', 'str': 'N/A'}.get(target_type, 'N/A')

# def safe_ohlcv_column_convert(value, column_name, ticker, expected_type='float'):
#     """
#     간소화된 OHLCV 컬럼 변환 함수 (기존 200+ 라인을 50라인 이하로 압축)
#     """
#     converter = TypeConverter()
#     context = f"{ticker}.{column_name}"
#     
#     # 품질 모니터링
#     try:
#         monitor = get_optimized_monitor()
#         monitor.log_conversion_failure(ticker, column_name, value, "conversion_attempt", "attempting")
#     except:
#         pass  # 모니터링 실패해도 변환은 계속
#     
#     return converter.safe_convert(value, expected_type, context)

# JSON 직렬화 안전성 강화 함수들

# === UNUSED: JSON 관련 함수들 ===
# def validate_json_compatibility(data, context=""):
#     """
#     JSON 직렬화 호환성을 검증하고 문제가 있는 데이터를 찾아냅니다.
#     
#     Args:
#         data: 검증할 데이터 (dict, list, 기본 타입)
#         context: 로깅용 컨텍스트
#         
#     Returns:
#         dict: 검증 결과 {'valid': bool, 'issues': list, 'corrected_data': any}
#     """
#     import json
#     import numpy as np
#     
#     issues = []
#     corrected_data = data
#     
#     def check_and_fix_value(value, path=""):
#         """재귀적으로 값을 검사하고 수정"""
#         # None은 JSON 호환
#         if value is None:
#             return value
#             
#         # 기본 JSON 호환 타입들
#         if isinstance(value, (str, int, float, bool)):
#             # numpy 타입을 네이티브 파이썬 타입으로 변환
#             if hasattr(value, 'item'):
#                 return value.item()
#             return value
#             
#         # datetime 관련 객체들
#         if isinstance(value, (datetime, date)):
#             if hasattr(value, 'strftime'):
#                 converted = value.strftime('%Y-%m-%d')
#                 converted = value.strftime('%Y-%m-%d')
#                 issues.append(f"{path}: datetime 객체를 문자열로 변환 ({type(value).__name__} → str)")
#                 return converted
#             else:
#                 converted = str(value)
#                 issues.append(f"{path}: datetime 객체를 문자열로 변환 (fallback)")
#                 return converted
#                 
#         # pandas/numpy 객체들
#         if hasattr(value, '__class__'):
#             class_str = str(type(value))
#             if any(lib in class_str for lib in ['pandas', 'numpy']):
#                 if hasattr(value, 'item'):  # numpy scalar
#                     converted = value.item()
#                     issues.append(f"{path}: numpy scalar을 네이티브 타입으로 변환")
#                     return converted
#                 elif hasattr(value, 'strftime'):  # pandas Timestamp
#                     converted = value.strftime('%Y-%m-%d')
#                     issues.append(f"{path}: pandas Timestamp를 문자열로 변환")
#                     return converted
#                 else:
#                     converted = str(value)
#                     issues.append(f"{path}: pandas/numpy 객체를 문자열로 변환")
#                     return converted
#                     
#         # 딕셔너리 처리
#         if isinstance(value, dict):
#             fixed_dict = {}
#             for k, v in value.items():
#                 new_path = f"{path}.{k}" if path else k
#                 fixed_dict[k] = check_and_fix_value(v, new_path)
#             return fixed_dict
#             
#         # 리스트 처리
#         if isinstance(value, (list, tuple)):
#             fixed_list = []
#             for i, item in enumerate(value):
#                 new_path = f"{path}[{i}]" if path else f"[{i}]"
#                 fixed_list.append(check_and_fix_value(item, new_path))
#             return fixed_list
#             
#         # 기타 타입들 - 문자열로 변환
#         converted = str(value)
#         issues.append(f"{path}: 알 수 없는 타입 {type(value).__name__}을 문자열로 변환")
#         return converted
#     
#     try:
#         corrected_data = check_and_fix_value(data, context)
#         
#         # 최종 JSON 직렬화 테스트
#         json.dumps(corrected_data, ensure_ascii=False)
#         
#         if issues:
#             logger.debug(f"🔧 {context}: JSON 호환성 문제 {len(issues)}개 수정됨")
#             for issue in issues[:5]:  # 처음 5개만 로그
#                 logger.debug(f"  - {issue}")
#         else:
#             logger.debug(f"✅ {context}: JSON 호환성 검증 통과")
#             
#         return {
#             'valid': True,
#             'issues': issues,
#             'corrected_data': corrected_data
#         }
#         
#     except Exception as e:
#         logger.error(f"❌ {context}: JSON 호환성 검증 실패 - {e}")
#         return {
#             'valid': False,
#             'issues': issues + [f"JSON 직렬화 테스트 실패: {e}"],
#             'corrected_data': corrected_data
#         }

# def safe_json_structure_with_fallback(data_dict, context=""):
#     """
#     안전한 JSON 구조화 - 부분 실패 시에도 최대한 데이터 보존
#     
#     Args:
#         data_dict: 구조화할 딕셔너리
#         context: 로깅용 컨텍스트
#         
#     Returns:
#         dict: 안전하게 구조화된 데이터
#     """
#     safe_result = {}
#     issues = []
#     
#     for key, value in data_dict.items():
#         try:
#         # 개별 필드별 JSON 호환성 검증
#         validation_result = validate_json_compatibility(value, f"{context}.{key}")
#         
#         if validation_result['valid']:
#         safe_result[key] = validation_result['corrected_data']
#         if validation_result['issues']:
#         issues.extend(validation_result['issues'])
#         else:
#         # 실패한 필드는 안전한 기본값으로 대체
#         if isinstance(value, (dict, list)):
#         safe_result[key] = {} if isinstance(value, dict) else []
#         elif isinstance(value, (int, float)):
#         safe_result[key] = 0
#         else:
#         safe_result[key] = "Error"
#         
#         issues.append(f"{key}: 구조화 실패로 기본값 사용")
#         logger.warning(f"⚠️ {context}.{key}: JSON 구조화 실패, 기본값 사용")
#         
#         except Exception as field_error:
#         # 개별 필드 처리 실패
#         safe_result[key] = "ProcessingError"
#         issues.append(f"{key}: 처리 중 예외 발생 - {field_error}")
#         logger.warning(f"⚠️ {context}.{key}: 처리 중 예외 - {field_error}")
#     
#     # 최종 JSON 직렬화 검증
#     try:
#         import json
#         json.dumps(safe_result, ensure_ascii=False)
#         logger.debug(f"✅ {context}: 안전한 JSON 구조화 완료 ({len(issues)}개 이슈 해결)")
#     except Exception as final_error:
#         logger.error(f"❌ {context}: 최종 JSON 검증 실패 - {final_error}")
#         # 극단적인 경우를 위한 최소한의 안전한 구조
#         safe_result = {
#         "error": "JSON 구조화 실패",
#         "original_keys": list(data_dict.keys()),
#         "timestamp": safe_strftime(datetime.now())
#         }
#     
#     return safe_result

# def enhanced_safe_strftime_for_json(date_obj, format_str='%Y-%m-%d'):
#     """
#     JSON 직렬화에 최적화된 safe_strftime - 모든 날짜 필드에 적용
#     
#     Args:
#         date_obj: 변환할 날짜 객체
#         format_str: 날짜 포맷 (기본: ISO 날짜)
#         
#     Returns:
#         str: JSON 호환 날짜 문자열
#     """
#     try:
#         # 기존 safe_strftime 사용
#         result = safe_strftime(date_obj, format_str)
#         
#         # 추가 검증: 결과가 유효한 JSON 문자열인지 확인
#         if not isinstance(result, str):
#             logger.warning(f"⚠️ safe_strftime 결과가 문자열이 아님: {type(result)} - {result}")
#             return "1970-01-01"
#             
#         # ISO 형식 검증 (기본 포맷인 경우)
#         if format_str == '%Y-%m-%d' and len(result) >= 10:
#             # YYYY-MM-DD 형식인지 간단히 검증
#             if result[4] == '-' and result[7] == '-':
#                 return result
#             else:
#                 logger.warning(f"⚠️ 날짜 형식이 비정상적: {result}")
#                 return "1970-01-01"
#         
#         return result
#         
#     except Exception as e:
#         logger.error(f"❌ enhanced_safe_strftime_for_json 실패: {e}")
#         return "1970-01-01"

# def validate_dataframe_index_for_json(df):
#     """
#     DataFrame index의 JSON 호환성을 검증하고 수정
#     
#     Args:
#         df: pandas DataFrame
#         
#     Returns:
#         pandas.DataFrame: JSON 호환 인덱스를 가진 DataFrame
#     """
#     if df is None or df.empty:
#         return df
#         
#     try:
#         # 인덱스가 datetime 타입인지 확인
#         if hasattr(df.index, 'dtype') and 'datetime' in str(df.index.dtype):
#             logger.debug("🔧 DataFrame 인덱스를 JSON 호환 문자열로 변환")
#             # 인덱스를 문자열로 변환
#             df.index = df.index.map(lambda x: enhanced_safe_strftime_for_json(x))
#             
#         # 인덱스에 pandas Timestamp가 있는지 확인
#         elif hasattr(df.index, '__iter__'):
#             index_needs_conversion = False
#             for idx_val in df.index[:5]:  # 처음 5개만 확인
#                 if hasattr(idx_val, '__class__') and 'pandas' in str(type(idx_val)):
#                     index_needs_conversion = True
#                     break
#                     
#             if index_needs_conversion:
#                 logger.debug("🔧 DataFrame 인덱스의 pandas 객체를 문자열로 변환")
#                 df.index = df.index.map(lambda x: enhanced_safe_strftime_for_json(x) if hasattr(x, 'strftime') else str(x))
#         
#         return df
#         
#     except Exception as e:
#         logger.error(f"❌ DataFrame 인덱스 JSON 호환성 처리 실패: {e}")
#         return df

# 로깅 개선 및 디버깅 강화 함수들

# === UNUSED: DataProcessingMonitor 클래스 ===
# class DataProcessingMonitor:
#     """데이터 처리 과정을 모니터링하고 통계를 수집하는 클래스"""
#     
#     def __init__(self):
#         self.stats = {
#             'start_time': datetime.now(),
#             'tickers_processed': 0,
#             'conversion_attempts': 0,
#             'conversion_successes': 0,
#             'conversion_failures': 0,
#             'datetime_detections': 0,
#             'json_serialization_attempts': 0,
#             'json_serialization_successes': 0,
#             'db_query_attempts': 0,
#             'db_query_successes': 0,
#             'total_processing_time': 0.0,
#             'detailed_issues': []
#         }
#     
#     def log_ticker_start(self, ticker):
#         """티커 처리 시작 로그"""
#         self.stats['tickers_processed'] += 1
#         logger.info(f"📊 [{self.stats['tickers_processed']}] {ticker} 처리 시작")
#     
#     def log_conversion_attempt(self, ticker, column, value_type, expected_type):
#         """변환 시도 로그"""
#         self.stats['conversion_attempts'] += 1
#         logger.debug(f"🔄 {ticker}.{column}: {value_type} → {expected_type} 변환 시도 (#{self.stats['conversion_attempts']})")
#     
#     def log_conversion_success(self, ticker, column, original, converted):
#         """변환 성공 로그"""
#         self.stats['conversion_successes'] += 1
#         logger.debug(f"✅ {ticker}.{column}: 변환 성공 - {type(original).__name__}({original}) → {type(converted).__name__}({converted})")
#     
#     def log_conversion_failure(self, ticker, column, original, error, fallback_used):
#         """변환 실패 로그"""
#         self.stats['conversion_failures'] += 1
#         issue_detail = {
#             'ticker': ticker,
#             'column': column,
#             'original_value': str(original),
#             'original_type': type(original).__name__,
#             'error': str(error),
#             'fallback_used': fallback_used,
#             'timestamp': datetime.now().isoformat()
#         }
#         self.stats['detailed_issues'].append(issue_detail)
#         logger.warning(f"⚠️ {ticker}.{column}: 변환 실패 - {type(original).__name__}({original}) 오류: {error}, 대체값: {fallback_used}")
#     
#     def log_datetime_detection(self, ticker, column, value, detection_method):
#         """datetime 감지 로그"""
#         self.stats['datetime_detections'] += 1
#         logger.debug(f"🕒 {ticker}.{column}: datetime 감지 - {detection_method}, 값: {value}")
#     
#     def log_json_attempt(self, ticker, data_size):
#         """JSON 직렬화 시도 로그"""
#         self.stats['json_serialization_attempts'] += 1
#         logger.debug(f"📝 {ticker}: JSON 직렬화 시도 (데이터 크기: {data_size} bytes)")
#     
#     def log_json_success(self, ticker, json_size):
#         """JSON 직렬화 성공 로그"""
#         self.stats['json_serialization_successes'] += 1
#         logger.debug(f"✅ {ticker}: JSON 직렬화 성공 (결과 크기: {json_size} bytes)")
#     
#     def log_db_query_attempt(self, ticker, expected_columns):
#         """DB 쿼리 시도 로그"""
#         self.stats['db_query_attempts'] += 1
#         logger.debug(f"🗄️ {ticker}: DB 쿼리 시도 (예상 컬럼: {expected_columns}개)")
#     
#     def log_db_query_success(self, ticker, actual_columns, row_count):
#         """DB 쿼리 성공 로그"""
#         self.stats['db_query_successes'] += 1
#         logger.debug(f"✅ {ticker}: DB 쿼리 성공 (실제 컬럼: {actual_columns}개, 행: {row_count}개)")
#     
#     def log_processing_time(self, ticker, duration):
#         """처리 시간 로그"""
#         self.stats['total_processing_time'] += duration
#         logger.debug(f"⏱️ {ticker}: 처리 완료 (소요시간: {duration:.3f}초)")
#     
#     def get_summary_report(self):
#         """요약 리포트 생성"""
#         runtime = datetime.now() - self.stats['start_time']
#         conversion_success_rate = (self.stats['conversion_successes'] / self.stats['conversion_attempts'] * 100) if self.stats['conversion_attempts'] > 0 else 0
#         json_success_rate = (self.stats['json_serialization_successes'] / self.stats['json_serialization_attempts'] * 100) if self.stats['json_serialization_attempts'] > 0 else 0
#         db_success_rate = (self.stats['db_query_successes'] / self.stats['db_query_attempts'] * 100) if self.stats['db_query_attempts'] > 0 else 0
#         
#         return {
#             'runtime_seconds': runtime.total_seconds(),
#             'tickers_processed': self.stats['tickers_processed'],
#             'conversion_success_rate': f"{conversion_success_rate:.1f}%",
#             'json_success_rate': f"{json_success_rate:.1f}%",
#             'db_success_rate': f"{db_success_rate:.1f}%",
#             'datetime_detections': self.stats['datetime_detections'],
#             'total_issues': len(self.stats['detailed_issues']),
#             'avg_processing_time_per_ticker': self.stats['total_processing_time'] / max(self.stats['tickers_processed'], 1)
#         }
#     
#     def log_summary(self):
#         """요약 로그 출력"""
#         summary = self.get_summary_report()
#         logger.info("=" * 50)
#         logger.info("📊 데이터 처리 모니터링 요약 리포트")
#         logger.info("=" * 50)
#         logger.info(f"⏱️ 총 실행시간: {summary['runtime_seconds']:.1f}초")
#         logger.info(f"📈 처리된 티커: {summary['tickers_processed']}개")
#         logger.info(f"🔄 타입 변환 성공률: {summary['conversion_success_rate']}")
#         logger.info(f"📝 JSON 직렬화 성공률: {summary['json_success_rate']}")
#         logger.info(f"🗄️ DB 쿼리 성공률: {summary['db_success_rate']}")
#         logger.info(f"🕒 datetime 감지 횟수: {summary['datetime_detections']}회")
#         logger.info(f"⚠️ 총 이슈 발생: {summary['total_issues']}건")
#         logger.info(f"⚡ 티커당 평균 처리시간: {summary['avg_processing_time_per_ticker']:.3f}초")
#         
#         # 상위 문제점들 리포트
#         if self.stats['detailed_issues']:
#             issue_types = {}
#             for issue in self.stats['detailed_issues']:
#                 issue_key = f"{issue['column']}_{issue['original_type']}"
#                 issue_types[issue_key] = issue_types.get(issue_key, 0) + 1
#             
#             logger.info("🔍 주요 문제점 TOP 5:")
#             for i, (issue_type, count) in enumerate(sorted(issue_types.items(), key=lambda x: x[1], reverse=True)[:5], 1):
#                 logger.info(f"  {i}. {issue_type}: {count}회")

# === UNUSED: 로깅 관련 함수들 ===
# def log_db_query_structure(query_result, context="", expected_columns=None):
#     """
#     DB 쿼리 결과의 구조를 상세히 로깅
#     
#     Args:
#         query_result: DB 쿼리 결과 (list of tuples)
#         context: 로깅 컨텍스트
#         expected_columns: 예상 컬럼 수
#     """
#     if not query_result:
#         logger.warning(f"🔍 {context}: 빈 쿼리 결과")
#         return
#     
#     sample_row = query_result[0]
#     actual_columns = len(sample_row)
#     
#     logger.debug(f"🔍 {context}: 쿼리 결과 구조 분석")
#     logger.debug(f"  - 총 행 수: {len(query_result)}")
#     logger.debug(f"  - 컬럼 수: {actual_columns}")
#     
#     if expected_columns and actual_columns != expected_columns:
#         logger.warning(f"⚠️ {context}: 컬럼 수 불일치 (예상: {expected_columns}, 실제: {actual_columns})")
#     
#     # 첫 번째 행의 각 컬럼 타입 분석
#     logger.debug(f"  - 첫 번째 행 타입 분석:")
#     for i, value in enumerate(sample_row):
#         value_type = type(value).__name__
#         value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
#         logger.debug(f"    [{i}] {value_type}: {value_preview}")
#         
#         # datetime 객체 감지 경고
#         if isinstance(value, (datetime, date)):
#             logger.warning(f"⚠️ {context}: 컬럼 {i}에서 datetime 객체 감지 - {value}")

# def enhanced_conversion_logging(original_value, converted_value, conversion_type, context=""):
#     """
#     상세한 변환 로깅
#     
#     Args:
#         original_value: 원본 값
#         converted_value: 변환된 값
#         conversion_type: 변환 타입
#         context: 컨텍스트
#     """
#     logger.debug(f"🔄 {context}: {conversion_type} 변환 상세")
#     logger.debug(f"  - 원본: {type(original_value).__name__}({original_value})")
#     logger.debug(f"  - 결과: {type(converted_value).__name__}({converted_value})")
#     
#     # 특별한 경우들 체크
#     if hasattr(original_value, '__class__'):
#         class_str = str(type(original_value))
#         if 'pandas' in class_str:
#             logger.debug(f"  - pandas 객체 감지: {class_str}")
#         elif 'numpy' in class_str:
#             logger.debug(f"  - numpy 객체 감지: {class_str}")
#     
#     # 변환 과정에서 정보 손실 체크
#     if conversion_type == 'float' and isinstance(original_value, str):
#         try:
#             if '.' in original_value and len(original_value.split('.')[1]) > 6:
#                 logger.debug(f"  - 정밀도 손실 가능: 원본 소수점 {len(original_value.split('.')[1])}자리")
#         except:
#             pass

# === UNUSED: 글로벌 모니터 관련 함수들 ===
# # 글로벌 모니터 인스턴스
# _global_monitor = DataProcessingMonitor()

# def get_global_monitor():
#     """글로벌 모니터 인스턴스 반환"""
#     return _global_monitor

# def reset_global_monitor():
    """글로벌 모니터 초기화"""
    global _global_monitor
    _global_monitor = DataProcessingMonitor()

# 5단계: 일관성 확보 및 통합 처리 함수들

# === UNUSED: 날짜 변환 함수 ===
# def unified_date_conversion_for_json(date_obj, ticker="", position=0, total_length=1):
#     """
#     간단한 날짜 변환 함수
#     JSON 직렬화에 최적화됨
#     
#     Args:
#         date_obj: 변환할 날짜 객체
#         ticker: 티커명 (로깅용)
#         position: DataFrame에서의 위치 (fallback 계산용)
#         total_length: DataFrame 전체 길이 (fallback 계산용)
#         
#     Returns:
#         str: JSON 호환 날짜 문자열 또는 None
#     """
#     try:
#         # 1순위: Pandas Timestamp 직접 변환 (data_fetcher와 동일)
#         if isinstance(date_obj, pd.Timestamp) and hasattr(date_obj, 'year') and date_obj.year > 1990:
#             result = date_obj.strftime('%Y-%m-%d')
#             logger.debug(f"✅ {ticker}: Pandas Timestamp 변환 성공 - {result}")
#             return result
#         
#         # 2순위: datetime/date 객체 변환 (강화됨)
#         if isinstance(date_obj, (datetime, date)) and hasattr(date_obj, 'year') and date_obj.year > 1990:
#             result = date_obj.strftime('%Y-%m-%d')
#             logger.debug(f"✅ {ticker}: datetime 객체 변환 성공 - {result}")
#             return result
#         
#         # 3순위: date 속성이 있는 객체
#         if hasattr(date_obj, 'date') and hasattr(date_obj.date(), 'year') and date_obj.date().year > 1990:
#             result = date_obj.date().strftime('%Y-%m-%d')
#             logger.debug(f"✅ {ticker}: date 속성 변환 성공 - {result}")
#             return result
#         
#         # 4순위: 문자열 파싱 시도 (data_fetcher와 동일)
#         if isinstance(date_obj, str) and date_obj not in ["N/A", "Invalid Date", ""]:
#             try:
#                 parsed_date = pd.to_datetime(date_obj)
#                 if hasattr(parsed_date, 'year') and parsed_date.year > 1990:
#                     result = parsed_date.strftime('%Y-%m-%d')
#                     logger.debug(f"✅ {ticker}: 문자열 파싱 성공 - {result}")
#                     return result
#             except:
#                 pass
#         
#         # 5순위: fallback 메커니즘 (data_fetcher와 동일하지만 더 안전하게)
#         if total_length > 1 and 0 <= position < total_length:
#             days_offset = total_length - position - 1
#             estimated_date = (datetime.now() - timedelta(days=days_offset)).date()
#             
#             # 추정 날짜 유효성 검증 (10년 이내만 허용)
#             if (datetime.now().date() - estimated_date).days <= 3650:
#                 result = estimated_date.strftime('%Y-%m-%d')
#                 logger.warning(f"⚠️ {ticker}: 날짜 추정 적용 - 위치 {position} → {result}")
#                 return result
#         
#         # 모든 방법 실패 시
#         logger.error(f"❌ {ticker}: 날짜 변환 완전 실패 - 원본: {date_obj} (타입: {type(date_obj)})")
#         return None
#         
#     except Exception as e:
#         logger.error(f"❌ {ticker}: 날짜 변환 중 예외 발생 - {e}")
#         return None

# === UNUSED: DB 스키마 검증 함수 ===
# def validate_and_fix_db_schema_alignment():
#     """
#     실제 DB 스키마와 get_safe_ohlcv_columns() 결과의 일치성을 검증하고 수정
#     
#     Returns:
#         dict: 검증 결과 {'valid': bool, 'issues': list, 'corrected_mapping': dict}
#     """
#     try:
#         # 실제 DB 스키마 조회
#         actual_columns = get_safe_ohlcv_columns()
#         
#         # 코드에서 사용하는 예상 컬럼들
#         expected_core_columns = [
#             'ticker', 'date', 'open', 'high', 'low', 'close', 'volume',  # 기본 OHLCV
#             'pivot', 'r1', 's1', 'support', 'resistance',  # 피봇 포인트
#             'supertrend', 'macd_hist', 'rsi_14', 'adx', 'atr',  # 기술 지표
#             'bb_upper', 'bb_lower', 'volume_20ma', 'ht_trendline',  # 추가 지표
#             'ma_50', 'ma_200', 'donchian_high', 'donchian_low',  # 이동평균 및 돈치안
#             'macd', 'macd_signal', 'macd_histogram', 'plus_di', 'minus_di'  # MACD 및 ADX
#         ]
#         
#         # 피보나치 컬럼들
#         fibo_columns = get_safe_fibo_columns()
#         
#         issues = []
#         missing_columns = []
#         extra_columns = []
#         
#         # 누락된 필수 컬럼 확인
#         for col in expected_core_columns:
#             if col not in actual_columns:
#                 missing_columns.append(col)
#                 issues.append(f"필수 컬럼 누락: {col}")
#         
#         # 예상 외 추가 컬럼 확인
#         for col in actual_columns:
#             if col not in expected_core_columns and not col.startswith('fibo_'):
#                 extra_columns.append(col)
#         
#         # 검증 결과
#         is_valid = len(missing_columns) == 0
#         
#         logger.info(f"🔍 DB 스키마 검증 결과:")
#         logger.info(f"  - 실제 컬럼 수: {len(actual_columns)}")
#         logger.info(f"  - 예상 필수 컬럼: {len(expected_core_columns)}")
#         logger.info(f"  - 피보나치 컬럼: {len(fibo_columns)}")
#         logger.info(f"  - 누락 컬럼: {len(missing_columns)}")
#         logger.info(f"  - 추가 컬럼: {len(extra_columns)}")
#         
#         if missing_columns:
#             logger.warning(f"⚠️ 누락된 필수 컬럼들: {missing_columns}")
#         
#         if extra_columns:
#             logger.info(f"ℹ️ 예상 외 추가 컬럼들: {extra_columns[:10]}{'...' if len(extra_columns) > 10 else ''}")
#         
#         # 수정된 매핑 생성
#         corrected_mapping = {
#             'available_core_columns': [col for col in expected_core_columns if col in actual_columns],
#             'available_fibo_columns': fibo_columns,
#             'missing_columns': missing_columns,
#             'extra_columns': extra_columns,
#             'total_available': len(actual_columns)
#         }
#         
#         return {
#             'valid': is_valid,
#             'issues': issues,
#             'corrected_mapping': corrected_mapping
#         }
#         
#     except Exception as e:
#         logger.error(f"❌ DB 스키마 검증 중 오류: {e}")
#         return {
#             'valid': False,
#             'issues': [f"스키마 검증 실패: {e}"],
#             'corrected_mapping': {}
#         }

# === UNUSED: DataFrame 인덱스 처리 함수 ===
# def standardize_dataframe_index_processing(df, ticker=""):
#     """
#     pandas DataFrame의 인덱스를 표준화된 방식으로 처리
#     - 모든 날짜 인덱스를 동일한 방식으로 처리
#     - JSON 직렬화 호환성 보장
#     
#     Args:
#         df: pandas DataFrame
#         ticker: 티커명 (로깅용)
#         
#     Returns:
#         pandas.DataFrame: 표준화된 인덱스를 가진 DataFrame
#     """
#     if df is None or df.empty:
#         return df
#     
#     try:
#         logger.debug(f"🔧 {ticker}: DataFrame 인덱스 표준화 시작")
#         original_index_type = type(df.index).__name__
#         
#         # 현재 인덱스 타입 확인
#         if hasattr(df.index, 'dtype') and 'datetime' in str(df.index.dtype):
#             logger.debug(f"🔧 {ticker}: datetime 인덱스 감지 - {original_index_type}")
#             
#             # 인덱스를 표준화된 문자열로 변환
#             new_index = []
#             for i, idx_val in enumerate(df.index):
#                 converted_date = unified_date_conversion_for_json(
#                     idx_val, ticker, i, len(df.index)
#                 )
#                 if converted_date:
#                     new_index.append(converted_date)
#                 else:
#                     # fallback: 기본 포맷 사용
#                     fallback_date = safe_strftime(idx_val, '%Y-%m-%d')
#                     new_index.append(fallback_date)
#             
#             df.index = new_index
#             logger.debug(f"✅ {ticker}: 인덱스 표준화 완료 - {original_index_type} → 문자열")
#             
#         # pandas Timestamp가 섞여있는 경우 처리
#         elif hasattr(df.index, '__iter__'):
#             needs_conversion = False
#             for idx_val in df.index[:5]:  # 샘플링으로 확인
#                 if hasattr(idx_val, '__class__') and any(keyword in str(type(idx_val)) for keyword in ['pandas', 'datetime', 'Timestamp']):
#                     needs_conversion = True
#                     break
#             
#             if needs_conversion:
#                 logger.debug(f"🔧 {ticker}: 혼합 인덱스 타입 감지, 통일 작업 시작")
#                 
#                 new_index = []
#                 for i, idx_val in enumerate(df.index):
#                     if hasattr(idx_val, 'strftime') or isinstance(idx_val, (datetime, date)):
#                         converted_date = unified_date_conversion_for_json(
#                             idx_val, ticker, i, len(df.index)
#                         )
#                         new_index.append(converted_date if converted_date else str(idx_val))
#                     else:
#                         new_index.append(str(idx_val))
#                 
#                 df.index = new_index
#                 logger.debug(f"✅ {ticker}: 혼합 인덱스 통일 완료")
#         
#         # 최종 JSON 호환성 검증
#         try:
#             import json
#             json.dumps(df.index.tolist(), ensure_ascii=False)
#             logger.debug(f"✅ {ticker}: 인덱스 JSON 호환성 검증 통과")
#         except Exception as json_error:
#             logger.warning(f"⚠️ {ticker}: 인덱스 JSON 호환성 문제 - {json_error}")
#             # 강제로 문자열 변환
#             df.index = [str(idx) for idx in df.index]
#         
#         return df
#         
#     except Exception as e:
#         logger.error(f"❌ {ticker}: DataFrame 인덱스 표준화 실패 - {e}")
#         return df

# === UNUSED: 종합 데이터 검증 함수 ===
# def comprehensive_data_validation_and_fix(data, data_type="unknown", context=""):
#     """
#     포괄적인 데이터 검증 및 수정 - 모든 데이터 타입을 안전하게 처리
#     
#     Args:
#         data: 검증할 데이터 (DataFrame, dict, list, 기본 타입)
#         data_type: 데이터 타입 힌트
#         context: 로깅 컨텍스트
#         
#     Returns:
#         검증되고 수정된 데이터
#     """
#     monitor = get_global_monitor()
#     
#     try:
#         if data is None:
#             return None
#         
#         # DataFrame 처리
#         if isinstance(data, pd.DataFrame):
#             logger.debug(f"🔧 {context}: DataFrame 종합 검증 시작")
#             
#             # 1. 인덱스 표준화
#             data = standardize_dataframe_index_processing(data, context)
#             
#             # 2. JSON 호환성을 위한 데이터 타입 수정
#             data = validate_dataframe_index_for_json(data)
#             
#             # 3. 컬럼 값들의 JSON 호환성 검증
#             for col in data.columns:
#                 try:
#                     # 샘플링으로 컬럼 타입 확인
#                     sample_values = data[col].dropna().head(5)
#                     for val in sample_values:
#                         if isinstance(val, (datetime, date)) or (hasattr(val, '__class__') and 'pandas' in str(type(val))):
#                             logger.debug(f"🔧 {context}: {col} 컬럼의 비호환 타입 감지")
#                             # 전체 컬럼을 안전한 형태로 변환
#                             data[col] = data[col].apply(
#                                 lambda x: enhanced_safe_strftime_for_json(x) 
#                                 if hasattr(x, 'strftime') or isinstance(x, (datetime, date))
#                                 else x
#                             )
#                             break
#                 except Exception as col_error:
#                     logger.warning(f"⚠️ {context}: {col} 컬럼 검증 실패 - {col_error}")
#             
#             return data
#         
#         # Dictionary 처리
#         elif isinstance(data, dict):
#             return safe_json_structure_with_fallback(data, context)
#         
#         # List 처리
#         elif isinstance(data, (list, tuple)):
#             corrected_list = []
#             for i, item in enumerate(data):
#                 item_context = f"{context}[{i}]"
#                 corrected_item = comprehensive_data_validation_and_fix(item, "list_item", item_context)
#                 corrected_list.append(corrected_item)
#             return corrected_list
#         
#         # 기본 타입 처리
#         else:
#             validation_result = validate_json_compatibility(data, context)
#             return validation_result['corrected_data']
#     
#     except Exception as e:
#         logger.error(f"❌ {context}: 종합 데이터 검증 실패 - {e}")
#         monitor.log_conversion_failure(context, "validation", data, str(e), "Error")
#         return "ValidationError"

# === UNUSED: DB 스키마 및 연결 검증 함수 ===
# def validate_db_schema_and_connection():
#     """
#     DB 연결 및 스키마 상태 검증 함수
#     
#     검증 항목:
#     1. .env 파일의 DB 연결 정보 확인
#     2. ohlcv 테이블의 실제 컬럼 구조 조회
#     3. static_indicators 테이블의 컬럼 구조 조회
#     4. pivot, r1, s1 등이 어느 테이블에 있는지 확인
#     5. 검증 결과를 상세히 로깅
#     
#     반환값: 
#     {
#         'ohlcv_columns': [...],
#         'static_columns': [...],
#         'missing_columns': [...],
#         'status': 'success/error'
#     }
#     """
#     validation_result = {
#         'ohlcv_columns': [],
#         'static_columns': [],
#         'missing_columns': [],
#         'status': 'error',
#         'env_vars': {},
#         'pivot_locations': {}
#     }
#     
#     try:
#         logger.info("🔍 DB 연결 및 스키마 검증 시작")
#         
#         # 1. 환경 변수 확인
#         logger.info("📋 환경 변수 검증:")
#         required_env_vars = ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']
#         for var in required_env_vars:
#             value = os.getenv(var)
#             if value:
#                 validation_result['env_vars'][var] = "✅ 설정됨" if var != 'PG_PASSWORD' else "✅ 설정됨 (비밀번호)"
#                 logger.info(f"  - {var}: ✅ 설정됨")
#             else:
#                 validation_result['env_vars'][var] = "❌ 누락"
#                 logger.error(f"  - {var}: ❌ 누락")
#         
#         # 2. DB 연결 테스트
#         logger.info("🔌 DB 연결 테스트:")
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         
#         # 3. ohlcv 테이블 컬럼 조회
#         logger.info("🔍 ohlcv 테이블 컬럼 구조 조회:")
#         cursor.execute("""
#             SELECT column_name, data_type, is_nullable
#             FROM information_schema.columns 
#             WHERE table_name = 'ohlcv' 
#             ORDER BY ordinal_position
#         """)
#         ohlcv_columns_info = cursor.fetchall()
#         validation_result['ohlcv_columns'] = [col[0] for col in ohlcv_columns_info]
#         
#         logger.info(f"  - 총 {len(ohlcv_columns_info)}개 컬럼 발견")
#         for col_name, data_type, nullable in ohlcv_columns_info[:10]:  # 처음 10개만 표시
#             logger.info(f"    {col_name} ({data_type})")
#         if len(ohlcv_columns_info) > 10:
#             logger.info(f"    ... 외 {len(ohlcv_columns_info) - 10}개 컬럼")
#         
#         # 4. static_indicators 테이블 컬럼 조회
#         logger.info("🔍 static_indicators 테이블 컬럼 구조 조회:")
#         cursor.execute("""
#             SELECT column_name, data_type, is_nullable
#             FROM information_schema.columns 
#             WHERE table_name = 'static_indicators' 
#             ORDER BY ordinal_position
#         """)
#         static_columns_info = cursor.fetchall()
#         validation_result['static_columns'] = [col[0] for col in static_columns_info]
#         
#         logger.info(f"  - 총 {len(static_columns_info)}개 컬럼 발견")
#         for col_name, data_type, nullable in static_columns_info:
#             logger.info(f"    {col_name} ({data_type})")
#         
#         # 5. 핵심 지표 컬럼들의 위치 확인
#         logger.info("🎯 핵심 지표 컬럼 위치 확인:")
#         critical_columns = ['pivot', 'r1', 's1', 'support', 'resistance', 'atr', 'adx']
#         
#         for col in critical_columns:
#             locations = []
#             if col in validation_result['ohlcv_columns']:
#                 locations.append('ohlcv')
#             if col in validation_result['static_columns']:
#                 locations.append('static_indicators')
#             
#             validation_result['pivot_locations'][col] = locations
#             
#             if locations:
#                 location_str = ', '.join(locations)
#                 logger.info(f"  - {col}: {location_str}")
#             else:
#                 logger.warning(f"  - {col}: ❌ 어느 테이블에도 없음")
#                 validation_result['missing_columns'].append(col)
#         
#         # 6. 데이터 존재 여부 확인
#         logger.info("📊 데이터 존재 여부 확인:")
#         
#         # ohlcv 테이블 레코드 수
#         cursor.execute("SELECT COUNT(*) FROM ohlcv")
#         ohlcv_count = cursor.fetchone()[0]
#         logger.info(f"  - ohlcv 테이블: {ohlcv_count:,}개 레코드")
#         
#         # static_indicators 테이블 레코드 수
#         cursor.execute("SELECT COUNT(*) FROM static_indicators")
#         static_count = cursor.fetchone()[0]
#         logger.info(f"  - static_indicators 테이블: {static_count:,}개 레코드")
#         
#         # 최근 데이터 확인
#         cursor.execute("SELECT MAX(date) FROM ohlcv")
#         latest_ohlcv_date = cursor.fetchone()[0]
#         if latest_ohlcv_date:
#             logger.info(f"  - ohlcv 최신 데이터: {latest_ohlcv_date}")
#         
#         cursor.close()
#         conn.close()
#         
#         validation_result['status'] = 'success'
#         logger.info("✅ DB 연결 및 스키마 검증 완료")
#         
#         return validation_result
#         
#     except Exception as e:
#         logger.error(f"❌ DB 연결 및 스키마 검증 실패: {e}")
#         validation_result['status'] = 'error'
#         validation_result['error'] = str(e)
#         return validation_result

# === UNUSED: 성능 모니터링 함수 ===
# def monitor_data_processing_performance(
#     ticker: str,
#     current_progress: int,
#     total_records: int,
#     start_time: float,
#     operation_name: str = "데이터 처리",
#     log_level: str = "INFO"
# ) -> dict:
#     """
#     대용량 데이터 처리 성능을 실시간 모니터링하는 함수
#     
#     Args:
#         ticker: 처리 중인 티커
#         current_progress: 현재 처리된 레코드 수
#         total_records: 전체 레코드 수
#         start_time: 처리 시작 시간 (time.time())
#         operation_name: 작업 이름
#         log_level: 로그 레벨 (INFO, DEBUG)
#     
#     Returns:
#         dict: 성능 메트릭 딕셔너리
#         {
#             'progress_percentage': float,
#             'elapsed_time': float,
#             'estimated_remaining_time': float,
#             'processing_speed_per_sec': float,
#             'memory_usage_mb': float,
#             'eta_formatted': str
#         }
#     """
#     try:
#         current_time = time.time()
#         elapsed_time = current_time - start_time
#         
#         # 진행률 계산
#         progress_percentage = (current_progress / total_records) * 100 if total_records > 0 else 0
#         
#         # 처리 속도 계산 (records/sec)
#         processing_speed = current_progress / elapsed_time if elapsed_time > 0 else 0
#         
#         # 예상 완료 시간 계산
#         if processing_speed > 0:
#             remaining_records = total_records - current_progress
#             estimated_remaining_time = remaining_records / processing_speed
#         else:
#             estimated_remaining_time = 0
#         
#         # 메모리 사용량 측정
#         try:
#             process = psutil.Process()
#             memory_usage_mb = process.memory_info().rss / 1024 / 1024
#         except:
#             memory_usage_mb = 0
#         
#         # ETA 포맷팅
#         eta_minutes = int(estimated_remaining_time // 60)
#         eta_seconds = int(estimated_remaining_time % 60)
#         eta_formatted = f"{eta_minutes}분 {eta_seconds}초" if eta_minutes > 0 else f"{eta_seconds}초"
#         
#         # 성능 메트릭 딕셔너리
#         performance_metrics = {
#             'progress_percentage': round(progress_percentage, 1),
#             'elapsed_time': round(elapsed_time, 1),
#             'estimated_remaining_time': round(estimated_remaining_time, 1),
#             'processing_speed_per_sec': round(processing_speed, 2),
#             'memory_usage_mb': round(memory_usage_mb, 1),
#             'eta_formatted': eta_formatted
#         }
#         
#         # 로깅
#         logger = logging.getLogger(__name__)
#         
#         if log_level == "INFO":
#             logger.info(
#                 f"🔄 {ticker} {operation_name}: "
#                 f"{current_progress:,}/{total_records:,} ({progress_percentage:.1f}%) | "
#                 f"속도: {processing_speed:.1f} records/sec | "
#                 f"ETA: {eta_formatted} | "
#                 f"메모리: {memory_usage_mb:.1f}MB"
#             )
#         elif log_level == "DEBUG":
#             logger.debug(
#                 f"📊 {ticker} 상세 성능 메트릭:\n"
#                 f"   - 진행률: {progress_percentage:.1f}% ({current_progress:,}/{total_records:,})\n"
#                 f"   - 경과 시간: {elapsed_time:.1f}초\n"
#                 f"   - 처리 속도: {processing_speed:.2f} records/sec\n"
#                 f"   - 예상 남은 시간: {estimated_remaining_time:.1f}초 ({eta_formatted})\n"
#                 f"   - 메모리 사용량: {memory_usage_mb:.1f}MB"
#             )
#         
#         return performance_metrics
#         
#     except Exception as e:
#         logger = logging.getLogger(__name__)
#         logger.error(f"❌ {ticker} 성능 모니터링 실패: {e}")
#         
#         # 기본 메트릭 반환
#         return {
#             'progress_percentage': 0,
#             'elapsed_time': 0,
#             'estimated_remaining_time': 0,
#             'processing_speed_per_sec': 0,
#             'memory_usage_mb': 0,
#             'eta_formatted': "알 수 없음"
#         }

def validate_db_schema_and_indicators():
    """
    실제 DB 스키마와 지표 데이터 품질을 검증하는 함수
    
    작업 내용:
    1. .env 파일의 DB 연결 정보 확인
    2. ohlcv 테이블의 실제 컬럼 구조를 조회하여 존재하는 동적 지표 컬럼들만 확인
    3. static_indicators 테이블에 있는 정적 지표 컬럼들 확인
    4. 실제 DB에 존재하는 컬럼만을 대상으로 NULL 값 비율 확인
    5. 검증 결과를 상세히 로깅
    
    Returns:
        dict: 검증 결과 정보
    """
    logger = setup_logger()
    
    try:
        logger.info("🔍 DB 스키마 및 지표 구조 검증 시작")
        
        # 1. DB 연결 정보 확인
        required_env_vars = ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']
        missing_vars = []
        
        for var in required_env_vars:
            value = os.getenv(var)
            if not value:
                missing_vars.append(var)
            else:
                # 비밀번호는 마스킹
                if var == 'PG_PASSWORD':
                    logger.info(f"✅ {var}: {'*' * len(value)}")
                else:
                    logger.info(f"✅ {var}: {value}")
        
        if missing_vars:
            logger.error(f"❌ 필수 환경변수 누락: {missing_vars}")
            return {'status': 'error', 'error': f'Missing environment variables: {missing_vars}'}
        
        # 2. DB 연결
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 3. ohlcv 테이블 실제 컬럼 조회
            logger.info("🔍 ohlcv 테이블 컬럼 구조 조회 중...")
            cursor.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'ohlcv' 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            
            ohlcv_columns_info = cursor.fetchall()
            actual_ohlcv_columns = []
            
            for col_name, data_type, is_nullable in ohlcv_columns_info:
                actual_ohlcv_columns.append(col_name)
                null_status = "NOT NULL" if is_nullable == "NO" else "NULL"
                logger.info(f"   📊 {col_name}: {data_type} ({null_status})")
            
            # 4. static_indicators 테이블 실제 컬럼 조회
            logger.info("🔍 static_indicators 테이블 컬럼 구조 조회 중...")
            cursor.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'static_indicators' 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            
            static_columns_info = cursor.fetchall()
            actual_static_columns = []
            
            for col_name, data_type, is_nullable in static_columns_info:
                actual_static_columns.append(col_name)
                null_status = "NOT NULL" if is_nullable == "NO" else "NULL"
                logger.info(f"   📊 {col_name}: {data_type} ({null_status})")
            
            # 5. 동적 지표 컬럼 중 실제 존재하는 것만 필터링
            expected_dynamic_indicators = [
                'fibo_618', 'fibo_382', 'ht_trendline', 'ma_50', 'ma_200', 
                'bb_upper', 'bb_lower', 'donchian_high', 'donchian_low', 
                'macd_histogram', 'rsi_14', 'volume_20ma', 'stoch_k', 'stoch_d', 'cci'
            ]
            
            existing_dynamic_indicators = [col for col in expected_dynamic_indicators if col in actual_ohlcv_columns]
            missing_dynamic_indicators = [col for col in expected_dynamic_indicators if col not in actual_ohlcv_columns]
            
            logger.info(f"📊 동적 지표 현황:")
            logger.info(f"   • 존재하는 동적 지표: {len(existing_dynamic_indicators)}개")
            for col in existing_dynamic_indicators:
                logger.info(f"     ✅ {col}")
            
            if missing_dynamic_indicators:
                logger.warning(f"   • 누락된 동적 지표: {len(missing_dynamic_indicators)}개")
                for col in missing_dynamic_indicators:
                    logger.warning(f"     ❌ {col}")
            
            # 6. 각 지표의 NULL 비율 확인 (KRW-BTC 기준)
            logger.info("🔍 동적 지표 NULL 비율 검증 중...")
            null_analysis = {}
            
            for indicator in existing_dynamic_indicators:
                try:
                    cursor.execute(f"""
                        SELECT 
                            COUNT(*) as total, 
                            COUNT({indicator}) as non_null,
                            COUNT(*) - COUNT({indicator}) as null_count
                        FROM ohlcv 
                        WHERE ticker = 'KRW-BTC'
                        AND date >= CURRENT_DATE - INTERVAL '30 days'
                    """)
                    
                    result = cursor.fetchone()
                    if result and result[0] > 0:
                        total, non_null, null_count = result
                        null_ratio = (null_count / total) * 100 if total > 0 else 100
                        
                        null_analysis[indicator] = {
                            'total': total,
                            'non_null': non_null, 
                            'null_count': null_count,
                            'null_ratio': null_ratio
                        }
                        
                        if null_ratio > 80:
                            logger.warning(f"  ⚠️ {indicator}: NULL 비율 {null_ratio:.1f}% ({null_count}/{total})")
                        elif null_ratio > 50:
                            logger.info(f"  🔶 {indicator}: NULL 비율 {null_ratio:.1f}% ({null_count}/{total})")
                        else:
                            logger.info(f"  ✅ {indicator}: NULL 비율 {null_ratio:.1f}% ({null_count}/{total})")
                    else:
                        logger.warning(f"  ⚠️ {indicator}: 데이터 없음")
                        null_analysis[indicator] = {'error': 'no_data'}
                        
                except Exception as e:
                    logger.error(f"  ❌ {indicator} NULL 비율 확인 실패: {e}")
                    null_analysis[indicator] = {'error': str(e)}
            
            # 7. static_indicators 컬럼 검증
            expected_static_indicators = [
                'ma200_slope', 'nvt_relative', 'volume_change_7_30', 'price', 
                'high_60', 'low_60', 'pivot', 's1', 'r1', 'resistance', 
                'support', 'atr', 'adx', 'supertrend_signal'
            ]
            
            existing_static_indicators = [col for col in expected_static_indicators if col in actual_static_columns]
            missing_static_indicators = [col for col in expected_static_indicators if col not in actual_static_columns]
            
            logger.info(f"📊 정적 지표 현황:")
            logger.info(f"   • 존재하는 정적 지표: {len(existing_static_indicators)}개")
            for col in existing_static_indicators:
                logger.info(f"     ✅ {col}")
            
            if missing_static_indicators:
                logger.warning(f"   • 누락된 정적 지표: {len(missing_static_indicators)}개")
                for col in missing_static_indicators:
                    logger.warning(f"     ❌ {col}")
            
            # 8. 검증 결과 요약
            logger.info("📊 DB 스키마 검증 결과 요약:")
            logger.info(f"   • ohlcv 테이블 컬럼 수: {len(actual_ohlcv_columns)}")
            logger.info(f"   • static_indicators 테이블 컬럼 수: {len(actual_static_columns)}")
            logger.info(f"   • 존재하는 동적 지표: {len(existing_dynamic_indicators)}/{len(expected_dynamic_indicators)}")
            logger.info(f"   • 존재하는 정적 지표: {len(existing_static_indicators)}/{len(expected_static_indicators)}")
            
            total_missing = len(missing_dynamic_indicators) + len(missing_static_indicators)
            if total_missing > 0:
                logger.warning(f"   • 총 누락 컬럼 수: {total_missing}")
            
            return {
                'status': 'success',
                'ohlcv_columns': actual_ohlcv_columns,
                'static_columns': actual_static_columns,
                'existing_dynamic_indicators': existing_dynamic_indicators,
                'missing_dynamic_indicators': missing_dynamic_indicators,
                'existing_static_indicators': existing_static_indicators,
                'missing_static_indicators': missing_static_indicators,
                'null_analysis': null_analysis,
                'total_missing_columns': total_missing
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"❌ DB 스키마 검증 중 오류 발생: {e}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        return {'status': 'error', 'error': str(e)}