#!/usr/bin/env python3
"""
🔐 MAKENAIDE 보안 검증 스크립트

이 스크립트는 Makenaide 프로젝트의 보안 상태를 점검합니다.
- API 키 노출 검사
- 환경변수 설정 검증
- 파일 권한 확인
- 민감한 정보 검출
"""

import os
import re
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Tuple
import logging

class SecurityChecker:
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.issues = []
        self.warnings = []
        self.passed_checks = []
        
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def run_all_checks(self) -> Dict[str, List[str]]:
        """모든 보안 검사를 실행합니다."""
        self.logger.info("🔐 보안 검사를 시작합니다...")
        
        checks = [
            self.check_env_file,
            self.check_api_keys_in_code,
            self.check_sensitive_files,
            self.check_file_permissions,
            self.check_gitignore,
            self.check_database_credentials,
            self.check_log_files,
            self.check_backup_files
        ]
        
        for check in checks:
            try:
                check()
            except Exception as e:
                self.warnings.append(f"검사 중 오류 발생: {check.__name__} - {str(e)}")
        
        return {
            'issues': self.issues,
            'warnings': self.warnings,
            'passed': self.passed_checks
        }
    
    def check_env_file(self):
        """환경변수 파일 검사를 수행합니다."""
        env_file = self.project_root / ".env"
        
        if not env_file.exists():
            self.issues.append("❌ .env 파일이 존재하지 않습니다. env.template을 참고하여 생성하세요.")
            return
        
        # .env 파일이 Git에 추적되지 않는지 확인
        if self._is_tracked_by_git(env_file):
            self.issues.append("🚨 .env 파일이 Git에 추적되고 있습니다! 즉시 제거하세요.")
        else:
            self.passed_checks.append("✅ .env 파일이 Git에서 제외되어 있습니다.")
        
        # .env 파일 권한 확인
        if self._check_file_permission(env_file, 0o600):
            self.passed_checks.append("✅ .env 파일 권한이 적절합니다 (600).")
        else:
            self.warnings.append("⚠️ .env 파일 권한을 600으로 설정하는 것을 권장합니다.")
    
    def check_api_keys_in_code(self):
        """코드에서 API 키 노출을 검사합니다."""
        api_key_patterns = [
            r'api_key["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{20,}["\']?',
            r'secret["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{20,}["\']?',
            r'token["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{20,}["\']?',
            r'password["\']?\s*[:=]\s*["\']?[^\s"\']+["\']?'
        ]
        
        python_files = list(self.project_root.rglob("*.py"))
        excluded_dirs = {'.git', '__pycache__', '.venv', 'venv', 'env'}
        
        for py_file in python_files:
            if any(excluded in str(py_file) for excluded in excluded_dirs):
                continue
            
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                for pattern in api_key_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    if matches:
                        # 실제 API 키인지 확인 (예시 값 제외)
                        for match in matches:
                            if not self._is_example_value(match):
                                self.issues.append(
                                    f"🚨 {py_file}에서 API 키 또는 비밀번호가 발견되었습니다: {match[:20]}..."
                                )
            except Exception as e:
                self.warnings.append(f"⚠️ {py_file} 파일 읽기 실패: {str(e)}")
        
        if not any("API 키" in issue for issue in self.issues):
            self.passed_checks.append("✅ 코드에서 API 키 노출이 발견되지 않았습니다.")
    
    def check_sensitive_files(self):
        """민감한 파일들이 Git에 추적되지 않는지 확인합니다."""
        sensitive_files = [
            '.env', 'secrets.json', 'api_keys.json', 'credentials.json',
            'database.ini', 'auth_tokens.json', 'session_data.json'
        ]
        
        for file_name in sensitive_files:
            file_path = self.project_root / file_name
            if file_path.exists() and self._is_tracked_by_git(file_path):
                self.issues.append(f"🚨 {file_name}이 Git에 추적되고 있습니다!")
            elif file_path.exists():
                self.passed_checks.append(f"✅ {file_name}이 Git에서 제외되어 있습니다.")
    
    def check_file_permissions(self):
        """중요 파일들의 권한을 확인합니다."""
        important_files = [
            '.env', 'config.json', 'blacklist.json'
        ]
        
        for file_name in important_files:
            file_path = self.project_root / file_name
            if file_path.exists():
                mode = file_path.stat().st_mode & 0o777
                if mode > 0o644:
                    self.warnings.append(f"⚠️ {file_name} 권한이 너무 개방적입니다: {oct(mode)}")
                else:
                    self.passed_checks.append(f"✅ {file_name} 권한이 적절합니다: {oct(mode)}")
    
    def check_gitignore(self):
        """.gitignore 파일이 적절히 설정되어 있는지 확인합니다."""
        gitignore_file = self.project_root / ".gitignore"
        
        if not gitignore_file.exists():
            self.issues.append("❌ .gitignore 파일이 존재하지 않습니다.")
            return
        
        required_patterns = [
            '.env', '*.log', 'log/', '__pycache__/', '*.pyc',
            'backtest_results/', 'charts/', 'reports/'
        ]
        
        try:
            with open(gitignore_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            missing_patterns = []
            for pattern in required_patterns:
                if pattern not in content:
                    missing_patterns.append(pattern)
            
            if missing_patterns:
                self.warnings.append(f"⚠️ .gitignore에 누락된 패턴: {', '.join(missing_patterns)}")
            else:
                self.passed_checks.append("✅ .gitignore 파일이 적절히 설정되어 있습니다.")
        except Exception as e:
            self.issues.append(f"❌ .gitignore 파일 읽기 실패: {str(e)}")
    
    def check_database_credentials(self):
        """데이터베이스 자격 증명이 안전하게 관리되는지 확인합니다."""
        # config.json에서 데이터베이스 정보 확인
        config_file = self.project_root / "config.json"
        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                # 데이터베이스 관련 키 검사
                db_keys = ['database', 'db', 'connection', 'url']
                for key in db_keys:
                    if key in str(config).lower():
                        self.warnings.append(f"⚠️ config.json에 데이터베이스 정보가 포함되어 있을 수 있습니다.")
                        break
                else:
                    self.passed_checks.append("✅ config.json에 민감한 데이터베이스 정보가 없습니다.")
            except Exception as e:
                self.warnings.append(f"⚠️ config.json 읽기 실패: {str(e)}")
    
    def check_log_files(self):
        """로그 파일에 민감한 정보가 포함되어 있는지 확인합니다."""
        log_dir = self.project_root / "log"
        if not log_dir.exists():
            return
        
        log_files = list(log_dir.glob("*.log"))
        for log_file in log_files:
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # API 키 패턴 검사
                api_patterns = [
                    r'api_key["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{20,}["\']?',
                    r'secret["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{20,}["\']?'
                ]
                
                for pattern in api_patterns:
                    if re.search(pattern, content, re.IGNORECASE):
                        self.issues.append(f"🚨 {log_file}에 API 키가 포함되어 있습니다!")
                        break
                else:
                    self.passed_checks.append(f"✅ {log_file.name}에 민감한 정보가 없습니다.")
            except Exception as e:
                self.warnings.append(f"⚠️ {log_file} 읽기 실패: {str(e)}")
    
    def check_backup_files(self):
        """백업 파일들이 Git에 추적되지 않는지 확인합니다."""
        backup_patterns = ['*.backup', '*.bak', '*.old', 'backup/']
        
        for pattern in backup_patterns:
            if pattern.endswith('/'):
                backup_dir = self.project_root / pattern[:-1]
                if backup_dir.exists() and self._is_tracked_by_git(backup_dir):
                    self.issues.append(f"🚨 {pattern} 디렉토리가 Git에 추적되고 있습니다!")
            else:
                backup_files = list(self.project_root.glob(pattern))
                for backup_file in backup_files:
                    if self._is_tracked_by_git(backup_file):
                        self.issues.append(f"🚨 {backup_file.name}이 Git에 추적되고 있습니다!")
    
    def _is_tracked_by_git(self, file_path: Path) -> bool:
        """파일이 Git에 추적되고 있는지 확인합니다."""
        try:
            import subprocess
            result = subprocess.run(
                ['git', 'ls-files', str(file_path)],
                capture_output=True,
                text=True,
                cwd=self.project_root
            )
            return bool(result.stdout.strip())
        except Exception:
            return False
    
    def _check_file_permission(self, file_path: Path, expected_mode: int) -> bool:
        """파일 권한을 확인합니다."""
        try:
            mode = file_path.stat().st_mode & 0o777
            return mode <= expected_mode
        except Exception:
            return False
    
    def _is_example_value(self, value: str) -> bool:
        """값이 예시 값인지 확인합니다."""
        example_patterns = [
            'your_', 'example_', 'test_', 'demo_', 'sample_',
            'placeholder', 'dummy', 'fake', 'mock'
        ]
        
        # 환경변수 접근 패턴은 허용
        env_patterns = [
            'os.getenv', 'os.environ', 'getenv', 'environ'
        ]
        
        # 예시 값 패턴 확인
        if any(pattern in value.lower() for pattern in example_patterns):
            return True
        
        # 환경변수 접근 패턴 확인
        if any(pattern in value for pattern in env_patterns):
            return True
        
        return False
    
    def generate_report(self) -> str:
        """보안 검사 보고서를 생성합니다."""
        report = []
        report.append("=" * 60)
        report.append("🔐 MAKENAIDE 보안 검사 보고서")
        report.append("=" * 60)
        report.append("")
        
        if self.issues:
            report.append("🚨 심각한 문제점:")
            for issue in self.issues:
                report.append(f"  • {issue}")
            report.append("")
        
        if self.warnings:
            report.append("⚠️ 주의사항:")
            for warning in self.warnings:
                report.append(f"  • {warning}")
            report.append("")
        
        if self.passed_checks:
            report.append("✅ 통과한 검사:")
            for check in self.passed_checks:
                report.append(f"  • {check}")
            report.append("")
        
        # 요약
        total_checks = len(self.issues) + len(self.warnings) + len(self.passed_checks)
        report.append("📊 요약:")
        report.append(f"  • 총 검사 항목: {total_checks}")
        report.append(f"  • 심각한 문제: {len(self.issues)}")
        report.append(f"  • 주의사항: {len(self.warnings)}")
        report.append(f"  • 통과: {len(self.passed_checks)}")
        report.append("")
        
        if not self.issues and not self.warnings:
            report.append("🎉 모든 보안 검사를 통과했습니다!")
        elif self.issues:
            report.append("🚨 심각한 보안 문제가 발견되었습니다. 즉시 수정하세요!")
        else:
            report.append("⚠️ 일부 주의사항이 있습니다. 검토 후 개선하세요.")
        
        report.append("=" * 60)
        return "\n".join(report)

def main():
    """메인 실행 함수"""
    checker = SecurityChecker()
    results = checker.run_all_checks()
    
    # 보고서 출력
    print(checker.generate_report())
    
    # 결과에 따른 종료 코드
    if results['issues']:
        exit(1)  # 심각한 문제가 있으면 1 반환
    elif results['warnings']:
        exit(2)  # 경고만 있으면 2 반환
    else:
        exit(0)  # 모든 검사 통과

if __name__ == "__main__":
    main() 