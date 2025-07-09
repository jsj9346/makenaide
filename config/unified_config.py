"""
고급 Config 관리 모듈 (축소 버전)

📚 기능:
- 실시간 설정 검증 및 업데이트
- 설정 export/import 기능
- 동적 설정 관리
- 설정 변경 이력 추적

기본 설정은 config.py에서 관리됩니다.
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Set
from dataclasses import dataclass, asdict
from datetime import datetime
import logging

# 기본 설정을 config.py에서 import
try:
    import config as base_config
    _BASE_CONFIG_AVAILABLE = True
except ImportError:
    _BASE_CONFIG_AVAILABLE = False
    print("⚠️ 기본 config.py를 로드할 수 없습니다. 제한된 기능으로 동작합니다.")

@dataclass
class ConfigValidationResult:
    """설정 검증 결과"""
    is_valid: bool
    errors: list
    warnings: list
    corrected_values: dict
    timestamp: datetime

class UnifiedConfig:
    """고급 설정 관리 클래스 (축소 버전)"""
    
    def __init__(self):
        self._config_cache = {}
        self._validation_history = []
        self._change_history = []
        self.logger = logging.getLogger(__name__)
        
        # 기본 설정 로드
        if _BASE_CONFIG_AVAILABLE:
            self._load_from_base_config()
        else:
            self._load_minimal_fallback()
    
    def _load_from_base_config(self):
        """기본 config.py에서 설정 로드"""
        try:
            self._config_cache = {
                'database': {
                    'connection': base_config.DB_CONFIG,
                    'pool': base_config.DB_POOL_CONFIG
                },
                'performance': {
                    'memory_limits': base_config.MEMORY_LIMITS,
                    'batch_processing': base_config.BATCH_PROCESSING_CONFIG,
                    'monitoring': base_config.PERFORMANCE_MONITORING
                },
                'indicators': {
                    'essential': base_config.ESSENTIAL_TREND_INDICATORS,
                    'non_essential': base_config.NON_ESSENTIAL_INDICATORS,
                    'min_periods': base_config.INDICATOR_MIN_PERIODS
                },
                'strategies': base_config.ALL_STRATEGIES,
                'alerts': {
                    'quality_thresholds': base_config.QUALITY_ALERT_THRESHOLDS,
                    'api_config': base_config.API_CONFIG
                },
                'environment': {
                    'mode': base_config.ENVIRONMENT,
                    'log_levels': base_config.LOG_LEVELS
                }
            }
            
            self.logger.info("✅ 기본 설정에서 고급 관리 설정 로드 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 기본 설정 로드 실패: {e}")
            self._load_minimal_fallback()
    
    def _load_minimal_fallback(self):
        """최소 Fallback 설정 로드"""
        self._config_cache = {
            'database': {'connection': {}, 'pool': {}},
            'performance': {'memory_limits': {}, 'batch_processing': {}, 'monitoring': {}},
            'indicators': {'essential': set(), 'non_essential': set(), 'min_periods': {}},
            'strategies': {},
            'alerts': {'quality_thresholds': {}, 'api_config': {}},
            'environment': {'mode': 'development', 'log_levels': {}}
        }
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """점 표기법으로 설정값 조회"""
        keys = key_path.split('.')
        current = self._config_cache
        
        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            return default
    
    def set(self, key_path: str, value: Any, track_change: bool = True) -> bool:
        """점 표기법으로 설정값 변경"""
        keys = key_path.split('.')
        current = self._config_cache
        
        try:
            # 기존 값 추적
            old_value = self.get(key_path) if track_change else None
            
            # 중첩된 딕셔너리 생성
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]
            
            # 값 설정
            current[keys[-1]] = value
            
            # 변경 이력 추적
            if track_change:
                self._track_change(key_path, old_value, value)
            
            self.logger.info(f"✅ 설정 업데이트: {key_path} = {value}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 설정 업데이트 실패: {key_path} - {e}")
            return False
    
    def _track_change(self, key_path: str, old_value: Any, new_value: Any):
        """설정 변경 이력 추적"""
        change_record = {
            'timestamp': datetime.now().isoformat(),
            'key_path': key_path,
            'old_value': old_value,
            'new_value': new_value,
            'change_type': 'update' if old_value is not None else 'create'
        }
        
        self._change_history.append(change_record)
        
        # 이력 제한 (최근 100개만 유지)
        if len(self._change_history) > 100:
            self._change_history = self._change_history[-100:]
    
    def validate_config(self) -> ConfigValidationResult:
        """설정 유효성 검증"""
        errors = []
        warnings = []
        corrected_values = {}
        
        try:
            # DB 설정 검증
            db_config = self.get('database.connection', {})
            if not db_config.get('host'):
                errors.append("DB 호스트가 설정되지 않았습니다.")
            
            if not db_config.get('dbname'):
                errors.append("DB 이름이 설정되지 않았습니다.")
            
            # 메모리 제한 검증
            memory_limits = self.get('performance.memory_limits', {})
            max_memory = memory_limits.get('max_total_mb', 0)
            
            if max_memory < 512:
                warnings.append(f"메모리 제한이 낮습니다: {max_memory}MB")
                corrected_values['performance.memory_limits.max_total_mb'] = 1024
            
            # 지표 설정 검증
            essential_indicators = self.get('indicators.essential', set())
            if len(essential_indicators) == 0:
                warnings.append("필수 지표가 설정되지 않았습니다.")
            
            # 전략 설정 검증
            strategies = self.get('strategies', {})
            if len(strategies) == 0:
                warnings.append("전략이 설정되지 않았습니다.")
            
            # 자동 수정 적용
            for key_path, value in corrected_values.items():
                self.set(key_path, value, track_change=False)
            
            result = ConfigValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                corrected_values=corrected_values,
                timestamp=datetime.now()
            )
            
            self._validation_history.append(result)
            
            # 검증 이력 제한 (최근 50개만 유지)
            if len(self._validation_history) > 50:
                self._validation_history = self._validation_history[-50:]
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 설정 검증 중 오류: {e}")
            return ConfigValidationResult(
                is_valid=False,
                errors=[f"검증 중 오류 발생: {str(e)}"],
                warnings=[],
                corrected_values={},
                timestamp=datetime.now()
            )
    
    def export_config(self, format_type: str = 'json', include_history: bool = False) -> str:
        """설정을 JSON/YAML 형식으로 export"""
        try:
            export_data = {
                'config': self._config_cache,
                'metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'version': '2.0_unified'
                }
            }
            
            if include_history:
                export_data['change_history'] = self._change_history[-20:]  # 최근 20개
                export_data['validation_history'] = [
                    asdict(result) for result in self._validation_history[-10:]  # 최근 10개
                ]
            
            if format_type.lower() == 'yaml':
                return yaml.dump(export_data, default_flow_style=False, allow_unicode=True)
            else:
                return json.dumps(export_data, indent=2, ensure_ascii=False, default=str)
                
        except Exception as e:
            self.logger.error(f"❌ 설정 export 실패: {e}")
            return "{}"
    
    def import_config(self, config_data: str, format_type: str = 'json') -> bool:
        """JSON/YAML 형식의 설정을 import"""
        try:
            if format_type.lower() == 'yaml':
                data = yaml.safe_load(config_data)
            else:
                data = json.loads(config_data)
            
            if 'config' in data:
                self._config_cache = data['config']
                
                # 변경 이력 복원 (선택적)
                if 'change_history' in data:
                    self._change_history.extend(data['change_history'])
                
                self.logger.info("✅ 설정 import 완료")
                return True
            else:
                self.logger.error("❌ 잘못된 설정 형식입니다.")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 설정 import 실패: {e}")
            return False
    
    def reload_from_base(self) -> bool:
        """기본 config.py에서 설정 재로드"""
        try:
            if _BASE_CONFIG_AVAILABLE:
                # 모듈 재로드
                import importlib
                importlib.reload(base_config)
                
                self._load_from_base_config()
                self.logger.info("✅ 기본 설정 재로드 완료")
                return True
            else:
                self.logger.warning("⚠️ 기본 설정 모듈을 사용할 수 없습니다.")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 기본 설정 재로드 실패: {e}")
            return False
    
    def get_config_summary(self) -> Dict[str, Any]:
        """설정 요약 정보 반환"""
        try:
            return {
                'config_sections': list(self._config_cache.keys()),
                'strategies_count': len(self.get('strategies', {})),
                'essential_indicators_count': len(self.get('indicators.essential', set())),
                'change_history_count': len(self._change_history),
                'validation_history_count': len(self._validation_history),
                'last_validation': self._validation_history[-1].timestamp.isoformat() if self._validation_history else None,
                'base_config_available': _BASE_CONFIG_AVAILABLE,
                'version': '2.0_unified_advanced'
            }
        except Exception as e:
            return {'error': str(e)}
    
    def get_change_history(self, limit: int = 20) -> list:
        """설정 변경 이력 반환"""
        return self._change_history[-limit:] if self._change_history else []
    
    def get_validation_history(self, limit: int = 10) -> list:
        """설정 검증 이력 반환"""
        return [asdict(result) for result in self._validation_history[-limit:]]

# 전역 인스턴스 (싱글톤 패턴)
_unified_config_instance = None

def get_unified_config() -> UnifiedConfig:
    """통합 설정 인스턴스 반환 (싱글톤)"""
    global _unified_config_instance
    if _unified_config_instance is None:
        _unified_config_instance = UnifiedConfig()
    return _unified_config_instance

def reset_unified_config():
    """통합 설정 인스턴스 재설정"""
    global _unified_config_instance
    _unified_config_instance = None

# 편의 함수들 (하위 호환성)
def validate_config() -> bool:
    """설정 유효성 검증 (간단 버전)"""
    result = get_unified_config().validate_config()
    return result.is_valid

def print_config_summary():
    """설정 요약 출력"""
    config = get_unified_config()
    summary = config.get_config_summary()
    
    print("🔧 통합 Config 관리 시스템 (고급 버전)")
    print(f"📊 설정 섹션: {', '.join(summary.get('config_sections', []))}")
    print(f"📈 전략 수: {summary.get('strategies_count', 0)}")
    print(f"📊 필수 지표 수: {summary.get('essential_indicators_count', 0)}")
    print(f"📝 변경 이력: {summary.get('change_history_count', 0)}건")
    print(f"✅ 검증 이력: {summary.get('validation_history_count', 0)}건")
    print(f"🔗 기본 Config 연결: {'✅' if summary.get('base_config_available') else '❌'}")
    print(f"🏷️ 버전: {summary.get('version', 'Unknown')}") 