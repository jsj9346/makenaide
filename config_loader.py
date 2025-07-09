import json
import yaml
from pathlib import Path
import os
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

# 전역 변수로 설정 캐시
_config_cache = None

# 시스템 프롬프트 캐시
_system_prompt_cache = None
_prompt_path_cache = None

# 트레이딩 설정 인스턴스 캐시
_trading_config_instance = None

class TradingConfig:
    """트레이딩 설정 관리 클래스"""
    
    def __init__(self, config_path: str = "config/strategy.yaml"):
        self.config_path = config_path
        self.config = {}
        self.last_modified = None
        self.load_config()
    
    def load_config(self) -> bool:
        """설정 파일 로드"""
        try:
            if not os.path.exists(self.config_path):
                logging.warning(f"⚠️ 설정 파일이 없습니다: {self.config_path}")
                self._create_default_config()
            
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            self.last_modified = os.path.getmtime(self.config_path)
            self._validate_config()
            
            logging.info(f"✅ 트레이딩 설정 로드 완료: {self.config_path}")
            return True
            
        except Exception as e:
            logging.error(f"❌ 설정 파일 로드 실패: {e}")
            return False
    
    def _create_default_config(self):
        """기본 설정 파일 생성"""
        default_config = {
            'gpt_analysis': {
                'score_threshold': 85,
                'confidence_threshold': 0.9,
                'batch_size': 5,
                'memory_threshold_mb': 500,
                'max_tokens': 4000,
                'temperature': 0.3,
                'retry_attempts': 3,
                'timeout_seconds': 30
            },
            'risk_management': {
                'base_stop_loss': 3.0,
                'base_take_profit': 6.0,
                'max_volatility_multiplier': 3.0,
                'max_position_size': 0.05,
                'base_position_size': 0.02,
                'kelly_fraction': 0.25,
                'max_daily_trades': 10,
                'max_portfolio_risk': 0.2
            },
            'filtering': {
                'max_filtered_tickers': 30,
                'min_market_cap': 100000000,
                'min_volume_24h': 1000000,
                'price_change_threshold': 5.0,
                'rsi_oversold': 30,
                'rsi_overbought': 70,
                'enable_volume_filter': True,
                'enable_volatility_filter': True
            },
            'backtest': {
                'default_period_days': 30,
                'initial_capital': 10000000,
                'commission_rate': 0.0005,
                'slippage_rate': 0.001,
                'enable_auto_report': True,
                'report_frequency': 'daily'
            },
            'performance': {
                'monitoring_enabled': True,
                'metrics_retention_days': 90,
                'alert_win_rate_threshold': 0.4,
                'alert_drawdown_threshold': 0.15,
                'performance_review_interval': 7
            },
            'adaptive_strategy': {
                'enable_auto_adjustment': True,
                'adjustment_frequency': 7,
                'min_trades_for_adjustment': 20,
                'performance_window_days': 14,
                'adjustment_sensitivity': 0.1
            }
        }
        
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, allow_unicode=True, default_flow_style=False)
    
    def _validate_config(self):
        """강화된 설정 검증 및 자동 복구"""
        required_sections = ['gpt_analysis', 'risk_management', 'filtering', 'backtest']
        
        # 1. 필수 섹션 확인
        for section in required_sections:
            if section not in self.config:
                logging.warning(f"⚠️ 필수 설정 섹션 누락: {section}")
                self._create_missing_section(section)
        
        # 2. 설정값 범위 검증 및 자동 보정
        self._validate_config_values()
        
        # 3. 설정 일관성 검증
        self._validate_config_consistency()
    
    def _validate_config_values(self):
        """설정값 범위 검증 및 자동 보정"""
        validations = {
            'gpt_analysis.score_threshold': {
                'range': (50, 100), 
                'type': (int, float), 
                'default': 85,
                'description': 'GPT 분석 점수 임계값'
            },
            'gpt_analysis.confidence_threshold': {
                'range': (0.1, 1.0), 
                'type': (int, float), 
                'default': 0.9,
                'description': 'GPT 분석 신뢰도 임계값'
            },
            'risk_management.base_stop_loss': {
                'range': (1.0, 10.0), 
                'type': (int, float), 
                'default': 3.0,
                'description': '기본 손절 비율(%)'
            },
            'risk_management.base_take_profit': {
                'range': (2.0, 25.0), 
                'type': (int, float), 
                'default': 6.0,
                'description': '기본 익절 비율(%)'
            },
            'risk_management.max_position_size': {
                'range': (0.01, 0.1), 
                'type': (int, float), 
                'default': 0.05,
                'description': '최대 포지션 크기'
            },
            'filtering.max_filtered_tickers': {
                'range': (5, 100), 
                'type': int, 
                'default': 30,
                'description': '최대 필터링 티커 수'
            },
            'backtest.default_period_days': {
                'range': (7, 365), 
                'type': int, 
                'default': 30,
                'description': '기본 백테스트 기간(일)'
            }
        }
        
        corrections_made = []
        
        for key_path, validation in validations.items():
            try:
                current_value = self.get(key_path)
                
                if current_value is None:
                    # 값이 없으면 기본값 설정
                    self._set_config_value_internal(key_path, validation['default'])
                    corrections_made.append(f"{key_path}: 누락값 → {validation['default']} ({validation['description']})")
                    continue
                
                # 타입 검증
                if not isinstance(current_value, validation['type']):
                    try:
                        # 타입 변환 시도
                        if validation['type'] == int:
                            corrected_value = int(float(current_value))
                        elif validation['type'] == float or validation['type'] == (int, float):
                            corrected_value = float(current_value)
                        else:
                            corrected_value = validation['default']
                        
                        self._set_config_value_internal(key_path, corrected_value)
                        corrections_made.append(f"{key_path}: 타입변환 {current_value} → {corrected_value}")
                        current_value = corrected_value
                    except:
                        self._set_config_value_internal(key_path, validation['default'])
                        corrections_made.append(f"{key_path}: 타입오류 {current_value} → {validation['default']}")
                        continue
                
                # 범위 검증
                min_val, max_val = validation['range']
                if not (min_val <= current_value <= max_val):
                    # 범위를 벗어난 경우 가장 가까운 유효값으로 보정
                    corrected_value = max(min_val, min(max_val, current_value))
                    self._set_config_value_internal(key_path, corrected_value)
                    corrections_made.append(f"{key_path}: 범위초과 {current_value} → {corrected_value} (범위: {min_val}-{max_val})")
                
            except Exception as e:
                # 검증 중 오류 발생 시 기본값으로 복구
                self._set_config_value_internal(key_path, validation['default'])
                corrections_made.append(f"{key_path}: 오류복구 → {validation['default']} (오류: {str(e)})")
        
        # 보정 결과 로깅
        if corrections_made:
            logging.warning("⚠️ 설정값 자동 보정 수행:")
            for correction in corrections_made:
                logging.warning(f"   - {correction}")
            
            # 보정된 설정을 파일에 저장
            self.save_config()
            logging.info("✅ 보정된 설정이 파일에 저장되었습니다.")
        else:
            logging.info("✅ 모든 설정값이 유효한 범위 내에 있습니다.")
    
    def _validate_config_consistency(self):
        """설정 일관성 검증 - 새로운 강화된 버전으로 대체"""
        return self._validate_config_consistency_enhanced()
    
    def _validate_config_consistency_enhanced(self):
        """향상된 설정 일관성 검증"""
        from datetime import datetime
        import json
        from typing import Dict, Any, List, Tuple
        
        # 의존성 규칙 정의
        dependency_rules = {
            'risk_management.base_take_profit': {
                'must_be_greater_than': 'risk_management.base_stop_loss',
                'ratio_range': (1.5, 4.0),  # 1.5배~4배 범위
                'market_condition_adjustment': True
            },
            'gpt_analysis.confidence_threshold': {
                'correlate_with': 'gpt_analysis.score_threshold',
                'correlation_type': 'positive',
                'strength': 0.7
            },
            'filtering.max_filtered_tickers': {
                'must_be_compatible_with': 'gpt_analysis.batch_size',
                'ratio_range': (2, 10),  # 배치 크기의 2-10배
                'description': '필터링된 티커 수는 GPT 배치 크기에 적절해야 함'
            },
            'risk_management.max_position_size': {
                'must_be_greater_than': 'risk_management.base_position_size',
                'safety_multiplier': 2.0,
                'description': '최대 포지션 크기는 기본 포지션 크기보다 커야 함'
            }
        }
        
        # 비즈니스 로직 검증
        business_rules = {
            'kelly_position_sizing': self._validate_kelly_sizing,
            'risk_parity': self._validate_risk_parity,
            'market_regime_consistency': self._validate_market_regime,
            'portfolio_correlation': self._validate_portfolio_correlation,
            'liquidity_constraints': self._validate_liquidity_constraints
        }
        
        validation_results = {
            'timestamp': datetime.now().isoformat(),
            'dependency_checks': [],
            'business_rule_checks': [],
            'warnings': [],
            'errors': [],
            'corrections_made': []
        }
        
        # 1. 의존성 규칙 검증
        for config_key, rule in dependency_rules.items():
            try:
                result = self._check_dependency_rule(config_key, rule)
                validation_results['dependency_checks'].append(result)
                
                if result['status'] == 'error':
                    validation_results['errors'].append(result['message'])
                elif result['status'] == 'warning':
                    validation_results['warnings'].append(result['message'])
                elif result['corrected']:
                    validation_results['corrections_made'].append(result['message'])
                    
            except Exception as e:
                error_msg = f"의존성 검증 실패 {config_key}: {str(e)}"
                validation_results['errors'].append(error_msg)
                logging.error(f"❌ {error_msg}")
        
        # 2. 비즈니스 로직 검증
        for rule_name, rule_func in business_rules.items():
            try:
                result = rule_func()
                validation_results['business_rule_checks'].append({
                    'rule_name': rule_name,
                    'result': result
                })
                
                if not result.get('valid', True):
                    validation_results['warnings'].append(f"비즈니스 규칙 위반: {rule_name} - {result.get('message', '알 수 없는 오류')}")
                    
            except Exception as e:
                error_msg = f"비즈니스 규칙 검증 실패 {rule_name}: {str(e)}"
                validation_results['errors'].append(error_msg)
                logging.error(f"❌ {error_msg}")
        
        # 3. 설정 변경 이력 기록
        self._record_config_validation(validation_results)
        
        # 4. 검증 결과 리포트
        self._report_validation_results(validation_results)
        
        # 5. 오류가 있으면 예외 발생
        if validation_results['errors']:
            raise ValueError(f"설정 검증 실패: {len(validation_results['errors'])}개의 오류 발견")
        
        return validation_results
    
    def _check_dependency_rule(self, config_key: str, rule: Dict[str, Any]) -> Dict[str, Any]:
        """개별 의존성 규칙 검증"""
        current_value = self.get(config_key)
        
        result = {
            'config_key': config_key,
            'status': 'valid',
            'message': '',
            'corrected': False
        }
        
        try:
            # must_be_greater_than 규칙
            if 'must_be_greater_than' in rule:
                compare_key = rule['must_be_greater_than']
                compare_value = self.get(compare_key)
                
                if current_value is not None and compare_value is not None:
                    if current_value <= compare_value:
                        # 비율 범위가 정의된 경우 자동 수정
                        if 'ratio_range' in rule:
                            min_ratio, max_ratio = rule['ratio_range']
                            corrected_value = compare_value * min(max_ratio, max(min_ratio, 2.0))
                            self._set_config_value_internal(config_key, corrected_value)
                            
                            result['status'] = 'corrected'
                            result['message'] = f"{config_key}({current_value}) <= {compare_key}({compare_value}) - 자동 수정: {corrected_value}"
                            result['corrected'] = True
                        else:
                            result['status'] = 'error'
                            result['message'] = f"{config_key}({current_value})는 {compare_key}({compare_value})보다 커야 합니다"
            
            # correlate_with 규칙
            if 'correlate_with' in rule:
                correlate_key = rule['correlate_with']
                correlate_value = self.get(correlate_key)
                correlation_type = rule.get('correlation_type', 'positive')
                
                if current_value is not None and correlate_value is not None:
                    correlation_strength = self._calculate_correlation_strength(current_value, correlate_value, correlation_type)
                    expected_strength = rule.get('strength', 0.5)
                    
                    if correlation_strength < expected_strength:
                        result['status'] = 'warning'
                        result['message'] = f"{config_key}와 {correlate_key}의 상관관계가 약함 (강도: {correlation_strength:.2f})"
            
            # must_be_compatible_with 규칙
            if 'must_be_compatible_with' in rule:
                compatible_key = rule['must_be_compatible_with']
                compatible_value = self.get(compatible_key)
                
                if current_value is not None and compatible_value is not None:
                    if 'ratio_range' in rule:
                        min_ratio, max_ratio = rule['ratio_range']
                        ratio = current_value / compatible_value
                        
                        if not (min_ratio <= ratio <= max_ratio):
                            # 자동 수정
                            optimal_ratio = (min_ratio + max_ratio) / 2
                            corrected_value = int(compatible_value * optimal_ratio)
                            self._set_config_value_internal(config_key, corrected_value)
                            
                            result['status'] = 'corrected'
                            result['message'] = f"{config_key} 호환성 문제로 자동 수정: {current_value} → {corrected_value}"
                            result['corrected'] = True
            
        except Exception as e:
            result['status'] = 'error'
            result['message'] = f"규칙 검증 중 오류: {str(e)}"
        
        return result
    
    def _validate_kelly_sizing(self) -> Dict[str, Any]:
        """켈리 공식 기반 포지션 사이징 검증"""
        try:
            kelly_fraction = self.get('risk_management.kelly_fraction', 0.25)
            base_position = self.get('risk_management.base_position_size', 0.02)
            max_position = self.get('risk_management.max_position_size', 0.05)
            
            # 켈리 비율 유효성 검증
            if not (0.01 <= kelly_fraction <= 0.5):
                return {
                    'valid': False,
                    'message': f'켈리 비율({kelly_fraction})이 권장 범위(0.01-0.5)를 벗어남'
                }
            
            # 포지션 크기 일관성 검증
            if base_position >= max_position:
                return {
                    'valid': False,
                    'message': f'기본 포지션({base_position})이 최대 포지션({max_position})보다 크거나 같음'
                }
            
            # 켈리 기반 권장 포지션과 설정값 비교
            if kelly_fraction * 2 < base_position:  # 켈리의 2배를 초과하면 위험
                return {
                    'valid': False,
                    'message': f'기본 포지션({base_position})이 켈리 추천({kelly_fraction})보다 과도함'
                }
            
            return {'valid': True, 'message': '켈리 사이징 검증 통과'}
            
        except Exception as e:
            return {'valid': False, 'message': f'켈리 사이징 검증 오류: {str(e)}'}
    
    def _validate_risk_parity(self) -> Dict[str, Any]:
        """리스크 패리티 검증"""
        try:
            max_portfolio_risk = self.get('risk_management.max_portfolio_risk', 0.2)
            max_position_size = self.get('risk_management.max_position_size', 0.05)
            max_daily_trades = self.get('risk_management.max_daily_trades', 10)
            
            # 전체 포트폴리오 리스크와 개별 포지션 리스크 균형
            individual_risk_limit = max_portfolio_risk / max(max_daily_trades, 5)
            
            if max_position_size > individual_risk_limit:
                return {
                    'valid': False,
                    'message': f'개별 포지션 크기({max_position_size})가 리스크 패리티 한계({individual_risk_limit:.3f})를 초과'
                }
            
            return {'valid': True, 'message': '리스크 패리티 검증 통과'}
            
        except Exception as e:
            return {'valid': False, 'message': f'리스크 패리티 검증 오류: {str(e)}'}
    
    def _validate_market_regime(self) -> Dict[str, Any]:
        """시장 상황별 설정 일관성 검증"""
        try:
            # 적응형 전략 설정 확인
            auto_adjustment = self.get('adaptive_strategy.enable_auto_adjustment', True)
            adjustment_frequency = self.get('adaptive_strategy.adjustment_frequency', 7)
            performance_window = self.get('adaptive_strategy.performance_window_days', 14)
            
            # 조정 빈도와 성능 윈도우 일관성
            if adjustment_frequency >= performance_window:
                return {
                    'valid': False,
                    'message': f'조정 빈도({adjustment_frequency}일)가 성능 윈도우({performance_window}일)보다 크거나 같음'
                }
            
            # 자동 조정이 비활성화된 경우 백테스트 주기 확인
            if not auto_adjustment:
                backtest_period = self.get('backtest.default_period_days', 30)
                if backtest_period < 14:
                    return {
                        'valid': False,
                        'message': f'자동 조정 비활성화 시 백테스트 기간({backtest_period}일)이 너무 짧음'
                    }
            
            return {'valid': True, 'message': '시장 상황 설정 검증 통과'}
            
        except Exception as e:
            return {'valid': False, 'message': f'시장 상황 검증 오류: {str(e)}'}
    
    def _validate_portfolio_correlation(self) -> Dict[str, Any]:
        """포트폴리오 상관관계 검증"""
        try:
            max_filtered_tickers = self.get('filtering.max_filtered_tickers', 30)
            max_daily_trades = self.get('risk_management.max_daily_trades', 10)
            
            # 너무 많은 티커를 동시에 거래하면 상관관계가 높아질 위험
            correlation_risk_ratio = max_daily_trades / max_filtered_tickers
            
            if correlation_risk_ratio > 0.5:  # 50% 이상이면 위험
                return {
                    'valid': False,
                    'message': f'일일 거래 비율({correlation_risk_ratio:.2%})이 높아 포트폴리오 상관관계 위험'
                }
            
            return {'valid': True, 'message': '포트폴리오 상관관계 검증 통과'}
            
        except Exception as e:
            return {'valid': False, 'message': f'포트폴리오 상관관계 검증 오류: {str(e)}'}
    
    def _validate_liquidity_constraints(self) -> Dict[str, Any]:
        """유동성 제약 조건 검증"""
        try:
            min_volume_24h = self.get('filtering.min_volume_24h', 1000000)
            max_position_size = self.get('risk_management.max_position_size', 0.05)
            
            # 포지션 크기에 비해 최소 거래량이 충분한지 확인
            # 일반적으로 일일 거래량의 1% 이하로 거래하는 것이 안전
            min_safe_volume = max_position_size * 100_000_000  # 1억원 기준
            
            if min_volume_24h < min_safe_volume:
                return {
                    'valid': False,
                    'message': f'최소 거래량({min_volume_24h:,})이 포지션 크기({max_position_size:.1%})에 비해 부족'
                }
            
            return {'valid': True, 'message': '유동성 제약 조건 검증 통과'}
            
        except Exception as e:
            return {'valid': False, 'message': f'유동성 제약 조건 검증 오류: {str(e)}'}
    
    def _calculate_correlation_strength(self, value1: float, value2: float, correlation_type: str) -> float:
        """상관관계 강도 계산"""
        try:
            if correlation_type == 'positive':
                # 값이 클수록 상관관계가 강해야 함
                normalized_diff = abs(value1 - value2) / max(value1, value2)
                return max(0, 1 - normalized_diff)
            elif correlation_type == 'negative':
                # 한 값이 클수록 다른 값은 작아야 함
                product = value1 * value2
                max_product = max(value1, value2) ** 2
                return max(0, 1 - (product / max_product))
            else:
                return 0.5  # 기본값
                
        except:
            return 0.0
    
    def _record_config_validation(self, validation_results: Dict[str, Any]):
        """설정 검증 이력 기록"""
        try:
            history_file = "config/validation_history.json"
            os.makedirs(os.path.dirname(history_file), exist_ok=True)
            
            # 기존 이력 로드
            history = []
            if os.path.exists(history_file):
                try:
                    with open(history_file, 'r', encoding='utf-8') as f:
                        history = json.load(f)
                except:
                    history = []
            
            # 새 검증 결과 추가
            history.append(validation_results)
            
            # 최근 100개만 유지
            history = history[-100:]
            
            # 파일에 저장
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=2, ensure_ascii=False)
                
            logging.debug(f"📝 설정 검증 이력 기록: {history_file}")
            
        except Exception as e:
            logging.warning(f"⚠️ 설정 검증 이력 기록 실패: {e}")
    
    def _report_validation_results(self, validation_results: Dict[str, Any]):
        """검증 결과 리포트"""
        total_checks = len(validation_results['dependency_checks']) + len(validation_results['business_rule_checks'])
        error_count = len(validation_results['errors'])
        warning_count = len(validation_results['warnings'])
        correction_count = len(validation_results['corrections_made'])
        
        if error_count > 0:
            logging.error(f"❌ 설정 검증 실패: {error_count}개 오류")
            for error in validation_results['errors']:
                logging.error(f"   - {error}")
        
        if warning_count > 0:
            logging.warning(f"⚠️ 설정 검증 경고: {warning_count}개")
            for warning in validation_results['warnings']:
                logging.warning(f"   - {warning}")
        
        if correction_count > 0:
            logging.info(f"🔧 설정 자동 수정: {correction_count}개")
            for correction in validation_results['corrections_made']:
                logging.info(f"   - {correction}")
        
        if error_count == 0 and warning_count == 0:
            logging.info(f"✅ 설정 검증 완료: {total_checks}개 규칙 모두 통과")
    
    def get_validation_history(self, days: int = 7) -> List[Dict[str, Any]]:
        """설정 검증 이력 조회"""
        try:
            history_file = "config/validation_history.json"
            if not os.path.exists(history_file):
                return []
            
            with open(history_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
            
            # 지정된 일수 내의 이력만 반환
            from datetime import datetime, timedelta
            cutoff_date = datetime.now() - timedelta(days=days)
            
            filtered_history = []
            for entry in history:
                try:
                    entry_date = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
                    if entry_date >= cutoff_date:
                        filtered_history.append(entry)
                except:
                    continue
            
            return filtered_history
            
        except Exception as e:
            logging.error(f"❌ 설정 검증 이력 조회 실패: {e}")
            return []
    
    def validate_runtime_config(self, config_changes: Dict[str, Any]) -> Dict[str, Any]:
        """런타임 설정 검증 API"""
        try:
            # 임시로 설정 변경 사항 적용
            original_config = self.config.copy()
            
            for key_path, value in config_changes.items():
                self._set_config_value_internal(key_path, value)
            
            # 검증 실행
            validation_results = self._validate_config_consistency_enhanced()
            
            # 원래 설정으로 복원
            self.config = original_config
            
            return {
                'valid': len(validation_results['errors']) == 0,
                'validation_results': validation_results,
                'safe_to_apply': len(validation_results['errors']) == 0 and len([w for w in validation_results['warnings'] if 'critical' in w.lower()]) == 0
            }
            
        except Exception as e:
            # 원래 설정으로 복원
            self.config = original_config
            return {
                'valid': False,
                'error': str(e),
                'safe_to_apply': False
            }
    
    def _create_missing_section(self, section: str):
        """누락된 설정 섹션 생성"""
        default_sections = {
            'gpt_analysis': {
                'score_threshold': 85,
                'confidence_threshold': 0.9,
                'batch_size': 5,
                'memory_threshold_mb': 500
            },
            'risk_management': {
                'base_stop_loss': 3.0,
                'base_take_profit': 6.0,
                'max_position_size': 0.05,
                'kelly_fraction': 0.25
            },
            'filtering': {
                'max_filtered_tickers': 30,
                'min_volume_24h': 1000000,
                'enable_volume_filter': True
            },
            'backtest': {
                'default_period_days': 30,
                'initial_capital': 10000000,
                'commission_rate': 0.0005
            }
        }
        
        if section in default_sections:
            self.config[section] = default_sections[section]
            logging.info(f"✅ 누락된 섹션 '{section}' 기본값으로 생성")
    
    def _set_config_value_internal(self, key_path: str, value: Any):
        """내부용 설정값 설정 (저장 없이)"""
        try:
            keys = key_path.split('.')
            config = self.config
            
            # 중첩된 딕셔너리 구조 생성
            for key in keys[:-1]:
                if key not in config:
                    config[key] = {}
                config = config[key]
            
            # 값 설정
            config[keys[-1]] = value
            
        except Exception as e:
            logging.error(f"❌ 내부 설정값 설정 실패: {key_path} = {value}, 오류: {e}")
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """점으로 구분된 키 경로로 설정값 조회
        
        Args:
            key_path: 'section.subsection.key' 형태의 키 경로
            default: 기본값
            
        Returns:
            설정값 또는 기본값
        """
        try:
            keys = key_path.split('.')
            value = self.config
            
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return default
            
            return value
            
        except Exception:
            return default
    
    def set(self, key_path: str, value: Any) -> bool:
        """설정값 업데이트"""
        try:
            keys = key_path.split('.')
            config = self.config
            
            # 중첩된 딕셔너리 구조 생성
            for key in keys[:-1]:
                if key not in config:
                    config[key] = {}
                config = config[key]
            
            # 값 설정
            config[keys[-1]] = value
            
            # 파일에 저장
            self.save_config()
            
            logging.info(f"✅ 설정 업데이트: {key_path} = {value}")
            return True
            
        except Exception as e:
            logging.error(f"❌ 설정 업데이트 실패: {key_path} = {value}, 오류: {e}")
            return False
    
    def save_config(self) -> bool:
        """설정을 파일에 저장"""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, allow_unicode=True, default_flow_style=False)
            
            self.last_modified = os.path.getmtime(self.config_path)
            return True
            
        except Exception as e:
            logging.error(f"❌ 설정 저장 실패: {e}")
            return False
    
    def reload_if_modified(self) -> bool:
        """파일이 수정되었으면 다시 로드"""
        try:
            if not os.path.exists(self.config_path):
                return False
            
            current_modified = os.path.getmtime(self.config_path)
            
            if current_modified > self.last_modified:
                logging.info("🔄 설정 파일이 수정되어 다시 로드합니다.")
                return self.load_config()
            
            return True
            
        except Exception as e:
            logging.error(f"❌ 설정 파일 수정 확인 실패: {e}")
            return False
    
    def get_gpt_config(self) -> Dict[str, Any]:
        """GPT 분석 설정 조회"""
        return self.config.get('gpt_analysis', {})
    
    def get_risk_config(self) -> Dict[str, Any]:
        """리스크 관리 설정 조회"""
        return self.config.get('risk_management', {})
    
    def get_filtering_config(self) -> Dict[str, Any]:
        """필터링 설정 조회"""
        return self.config.get('filtering', {})
    
    def get_backtest_config(self) -> Dict[str, Any]:
        """백테스트 설정 조회"""
        return self.config.get('backtest', {})
    
    def get_performance_config(self) -> Dict[str, Any]:
        """성능 모니터링 설정 조회"""
        return self.config.get('performance', {})
    
    def get_adaptive_config(self) -> Dict[str, Any]:
        """적응형 전략 설정 조회"""
        return self.config.get('adaptive_strategy', {})
    
    def export_to_json(self, output_path: str) -> bool:
        """설정을 JSON 파일로 내보내기"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            
            logging.info(f"✅ 설정 JSON 내보내기 완료: {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"❌ 설정 JSON 내보내기 실패: {e}")
            return False
    
    def get_summary(self) -> Dict[str, Any]:
        """설정 요약 정보 반환"""
        return {
            'config_path': self.config_path,
            'last_modified': datetime.fromtimestamp(self.last_modified).isoformat(),
            'sections': list(self.config.keys()),
            'gpt_score_threshold': self.get('gpt_analysis.score_threshold', 85),
            'risk_stop_loss': self.get('risk_management.base_stop_loss', 3.0),
            'backtest_period': self.get('backtest.default_period_days', 30),
            'auto_adjustment': self.get('adaptive_strategy.enable_auto_adjustment', True)
        }

def get_trading_config() -> TradingConfig:
    """글로벌 트레이딩 설정 인스턴스 제공 (통합 버전)"""
    global _trading_config_instance
    
    if _trading_config_instance is None:
        _trading_config_instance = TradingConfig()
    else:
        # 파일이 수정되었으면 다시 로드
        _trading_config_instance.reload_if_modified()
    
    return _trading_config_instance

def reset_trading_config():
    """트레이딩 설정 인스턴스 리셋 (테스트용)"""
    global _trading_config_instance
    _trading_config_instance = None

def load_config_unified(config_path=None):
    """통합 설정 로더 - 기존 함수와 새 기능 결합"""
    try:
        # 기본 설정 로드
        basic_config = load_config(config_path)
        
        # TradingConfig 추가
        trading_config = get_trading_config()
        
        # 통합 반환
        if basic_config and trading_config.config:
            return {**basic_config, **trading_config.config}
        
        return basic_config or trading_config.config
        
    except Exception as e:
        logging.error(f"❌ 통합 설정 로드 실패: {e}")
        return load_config(config_path)

def load_system_prompt_safe(prompt_path: str, fallback_prompt: str = None) -> str:
    """
    안전한 시스템 프롬프트 로딩
    1. 파일 존재 여부 확인
    2. fallback 프롬프트 제공
    3. 런타임 재로딩 지원
    """
    try:
        if os.path.exists(prompt_path):
            with open(prompt_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:  # 빈 파일 체크
                    return content
        
        # fallback 프롬프트 사용
        if fallback_prompt:
            logging.warning(f"프롬프트 파일 없음, fallback 사용: {prompt_path}")
            return fallback_prompt
        
        # 기본 프롬프트
        return """You are a professional cryptocurrency trader utilizing trend-following and breakout strategies inspired by Mark Minervini, Stan Weinstein, and William O'Neil."""
        
    except Exception as e:
        logging.error(f"프롬프트 로딩 실패: {e}")
        return fallback_prompt or "기본 분석 프롬프트"

def reload_system_prompt(prompt_path: str = None):
    """동적 프롬프트 업데이트 지원"""
    global _system_prompt_cache, _prompt_path_cache
    
    if prompt_path is None:
        prompt_path = _prompt_path_cache or "prompts/system_prompt.txt"
    
    _prompt_path_cache = prompt_path
    _system_prompt_cache = load_system_prompt_safe(prompt_path)
    logging.info("시스템 프롬프트 재로딩 완료")
    return _system_prompt_cache

def get_cached_system_prompt(prompt_path: str = None) -> str:
    """캐시된 시스템 프롬프트 반환"""
    global _system_prompt_cache, _prompt_path_cache
    
    if prompt_path is None:
        prompt_path = "prompts/system_prompt.txt"
    
    # 캐시가 없거나 경로가 변경된 경우 새로 로드
    if _system_prompt_cache is None or _prompt_path_cache != prompt_path:
        return reload_system_prompt(prompt_path)
    
    return _system_prompt_cache

def load_config(config_path=None):
    """Loads configuration from JSON or YAML file."""
    global _config_cache
    if _config_cache is not None:
        return _config_cache

    if config_path is None:
        # 기본 경로 설정 (프로젝트 루트에 config.json 또는 config.yaml이 있다고 가정)
        base_dir = os.path.dirname(os.path.abspath(__file__)) # 현재 파일의 디렉토리
        # config.json 또는 config.yaml을 찾기 위해 상위 디렉토리로 이동하거나,
        # 프로젝트 구조에 맞게 경로를 조정해야 할 수 있습니다.
        # 여기서는 예시로 현재 디렉토리의 부모 디렉토리를 사용합니다.
        # 실제 프로젝트 구조에 맞게 수정하세요.
        # 예를 들어, 프로젝트 루트에 config 파일이 있다면:
        # project_root = os.path.dirname(os.path.dirname(base_dir)) # 두 단계 위로 (만약 utils 폴더 등에 있다면)
        # 이 파일이 프로젝트 루트에 직접 있다면 아래 경로 사용
        project_root = base_dir

        if os.path.exists(os.path.join(project_root, 'config.json')):
            config_path = os.path.join(project_root, 'config.json')
        elif os.path.exists(os.path.join(project_root, 'config.yaml')):
            config_path = os.path.join(project_root, 'config.yaml')
        else:
            # 환경 변수에서 경로를 읽어올 수도 있습니다.
            config_path_env = os.getenv('APP_CONFIG_PATH')
            if config_path_env and os.path.exists(config_path_env):
                config_path = config_path_env
            else:
                raise FileNotFoundError("Configuration file (config.json or config.yaml) not found in default paths or APP_CONFIG_PATH env var.")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, 'r') as f:
        if config_path.endswith('.json'):
            config_data = json.load(f)
        elif config_path.endswith(('.yaml', '.yml')):
            config_data = yaml.safe_load(f)
        else:
            raise ValueError("Unsupported configuration file format. Use .json or .yaml")
    
    # 기본값 설정
    config_data.setdefault('gpt_confidence_threshold', 0.8)
    config_data.setdefault('buy_phases', ["Stage 2", "Stage1->Stage2"]) # 예시 기본값
    
    # config.py에서 추가 설정 로드
    try:
        from config import PYRAMIDING_CONFIG
        config_data['pyramiding'] = PYRAMIDING_CONFIG
        logging.info("✅ 피라미딩 설정 로드 완료")
    except ImportError as e:
        logging.warning(f"⚠️ config.py에서 PYRAMIDING_CONFIG 로드 실패: {e}")
        # 기본 피라미딩 설정 제공
        config_data['pyramiding'] = {
            'max_add_ons': 3,
            'add_on_ratio': 0.5,
            'pyramid_threshold_pct': 5.0,
            'max_total_position_pct': 8.0
        }
    
    _config_cache = config_data
    return _config_cache

def get_config_value(key, default=None):
    """Loads configuration if not already loaded, and returns the value for the given key."""
    config = load_config()
    return config.get(key, default)

# 사용자의 요청에 맞춘 get 함수 (get_config_value를 간단히 호출)
def get(key, default=None):
    """Helper function to get a configuration value."""
    return get_config_value(key, default)

if __name__ == '__main__':
    # For testing purposes
    try:
        config = load_config()
        print("Configuration loaded successfully:")
        # print(json.dumps(config, indent=2))
        
        print(f"DB Host: {get('db_host', 'Not Set')}")
        print(f"OpenAI API Key: {'Set' if get('openai_api_key') else 'Not Set'}")
        print(f"GPT Confidence Threshold: {get('gpt_confidence_threshold')}")
        print(f"Buy Phases: {get('buy_phases')}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure 'config.json' or 'config.yaml' exists in the project root or specify its path via APP_CONFIG_PATH.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
