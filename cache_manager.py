# === UNUSED: 캐시 관리 모듈 ===
# import time
# import logging
# from functools import lru_cache
# from datetime import datetime, timedelta

# class CacheManager:
#     def __init__(self):
#         self._cache = {}
#         self._cache_timestamps = {}
#         self._default_ttl = 300  # 5분
#     
#     def get(self, key):
#         """캐시된 값을 조회"""
#         if key in self._cache:
#             if self._is_valid(key):
#                 return self._cache[key]
#             else:
#                 self._remove(key)
#         return None
#     
#     def set(self, key, value, ttl=None):
#         """값을 캐시에 저장"""
#         self._cache[key] = value
#         self._cache_timestamps[key] = datetime.now()
#         if ttl is not None:
#             self._cache_timestamps[key] = self._cache_timestamps[key] + timedelta(seconds=ttl)
#     
#     def _is_valid(self, key):
#         """캐시가 유효한지 확인"""
#         if key not in self._cache_timestamps:
#             return False
#         return datetime.now() < self._cache_timestamps[key]
#     
#     def _remove(self, key):
#         """캐시에서 항목 제거"""
#         if key in self._cache:
#             del self._cache[key]
#         if key in self._cache_timestamps:
#             del self._cache_timestamps[key]
#     
#     def clear(self):
#         """모든 캐시 제거"""
#         self._cache.clear()
#         self._cache_timestamps.clear()

# # 전역 캐시 매니저 인스턴스
# cache_manager = CacheManager()

# # LRU 캐시 데코레이터
# def cached(ttl=None):
#     def decorator(func):
#         @lru_cache(maxsize=128)
#         def wrapper(*args, **kwargs):
#             return func(*args, **kwargs)
#         return wrapper
#     return decorator

# # GPT 응답 캐싱
# def cache_gpt_response(ticker, data, response, ttl=7200):  # 2시간
#     key = f"gpt_{ticker}_{hash(str(data))}"
#     cache_manager.set(key, response, ttl)

# def get_cached_gpt_response(ticker, data, max_age_minutes=720):
#     key = f"gpt_{ticker}_{hash(str(data))}"
#     return cache_manager.get(key)

# # 시장 데이터 캐싱
# def cache_market_data(ticker, data, ttl=300):  # 5분
#     key = f"market_{ticker}"
#     cache_manager.set(key, data, ttl)

# def get_cached_market_data(ticker):
#     key = f"market_{ticker}"
#     return cache_manager.get(key)

# # 기술적 지표 캐싱
# def cache_technical_indicators(ticker, indicators, ttl=300):  # 5분
#     key = f"indicators_{ticker}"
#     cache_manager.set(key, indicators, ttl)

# def get_cached_technical_indicators(ticker):
#     key = f"indicators_{ticker}"
#     return cache_manager.get(key)

# # OHLCV 데이터 캐싱
# def cache_ohlcv_data(ticker, timeframe, data, ttl=300):  # 5분
#     key = f"ohlcv_{ticker}_{timeframe}"
#     cache_manager.set(key, data, ttl)

# def get_cached_ohlcv_data(ticker, timeframe):
#     key = f"ohlcv_{ticker}_{timeframe}"
#     return cache_manager.get(key) 