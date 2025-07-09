# 🔐 MAKENAIDE 보안 가이드라인

## 📋 개요
이 문서는 Makenaide 트레이딩 봇의 보안을 위한 필수 가이드라인을 제공합니다.

## 🚨 중요 보안 원칙

### 1. API 키 및 인증 정보 관리
- **절대 Git에 커밋하지 마세요**
- 모든 API 키는 `.env` 파일에 저장
- `.env` 파일은 `.gitignore`에 포함되어 있음
- 프로덕션 환경에서는 환경변수로 관리

### 2. 환경변수 설정 예시
```bash
# .env 파일 예시
BINANCE_API_KEY=your_api_key_here
BINANCE_SECRET_KEY=your_secret_key_here
DATABASE_URL=postgresql://user:password@localhost:5432/makenaide
OPENAI_API_KEY=your_openai_api_key_here
TELEGRAM_BOT_TOKEN=your_telegram_token_here
```

### 3. 데이터베이스 보안
- 데이터베이스 연결 정보는 환경변수로 관리
- 프로덕션 DB에는 강력한 비밀번호 사용
- 정기적인 백업 및 복구 테스트 수행
- DB 접근 로그 모니터링

## 📁 파일 보안 분류

### 🔴 절대 커밋 금지 (민감 정보)
- `.env` 파일
- `api_keys.json`
- `credentials.json`
- `database.ini`
- `auth_tokens.json`
- `session_data.json`

### 🟡 조건부 커밋 (운영 데이터)
- `blacklist.json` (민감하지 않은 거래 제한 목록)
- `config.json` (공개 가능한 설정)
- 로그 파일 (개인정보 제거 후)

### 🟢 안전한 커밋 (소스 코드)
- Python 소스 파일
- 설정 템플릿
- 문서화 파일
- 테스트 코드

## 🔧 보안 설정 체크리스트

### 초기 설정
- [ ] `.env` 파일 생성 및 API 키 설정
- [ ] 데이터베이스 연결 정보 환경변수화
- [ ] 로그 레벨 설정 (개인정보 노출 방지)
- [ ] 파일 권한 설정 (600 또는 700)

### 정기 점검
- [ ] API 키 정기 갱신
- [ ] 로그 파일 개인정보 검토
- [ ] 데이터베이스 접근 로그 확인
- [ ] 백업 파일 보안 검토

### 배포 전 검증
- [ ] 민감 정보가 코드에 하드코딩되지 않았는지 확인
- [ ] 환경변수 올바르게 설정되었는지 확인
- [ ] 로그 파일에 개인정보가 포함되지 않았는지 확인
- [ ] 데이터베이스 연결 보안 설정 확인

## 🛡️ 추가 보안 조치

### 1. 로그 보안
```python
# 개인정보 마스킹 예시
import logging

class SecureFormatter(logging.Formatter):
    def format(self, record):
        # API 키 마스킹
        if hasattr(record, 'msg'):
            record.msg = self.mask_sensitive_data(record.msg)
        return super().format(record)
    
    def mask_sensitive_data(self, message):
        # API 키 패턴 마스킹
        import re
        patterns = [
            r'api_key["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{20,}["\']?',
            r'secret["\']?\s*[:=]\s*["\']?[a-zA-Z0-9]{20,}["\']?'
        ]
        for pattern in patterns:
            message = re.sub(pattern, '[MASKED]', message)
        return message
```

### 2. 환경변수 검증
```python
# 필수 환경변수 검증
import os
from typing import List

def validate_environment_variables(required_vars: List[str]) -> bool:
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {missing_vars}")
    return True

# 사용 예시
required_vars = [
    'BINANCE_API_KEY',
    'BINANCE_SECRET_KEY',
    'DATABASE_URL',
    'OPENAI_API_KEY'
]
validate_environment_variables(required_vars)
```

### 3. 데이터 암호화
```python
# 민감 데이터 암호화 예시
from cryptography.fernet import Fernet
import base64

class DataEncryption:
    def __init__(self, key: str = None):
        if key is None:
            key = Fernet.generate_key()
        self.cipher = Fernet(key)
    
    def encrypt(self, data: str) -> str:
        return self.cipher.encrypt(data.encode()).decode()
    
    def decrypt(self, encrypted_data: str) -> str:
        return self.cipher.decrypt(encrypted_data.encode()).decode()
```

## 🚨 비상 대응

### 보안 사고 발생 시
1. **즉시 API 키 무효화**
2. **데이터베이스 접근 차단**
3. **로그 파일 보존**
4. **보안 감사 수행**
5. **새로운 인증 정보 발급**

### 연락처
- 보안 담당자: [담당자 정보]
- 긴급 연락처: [연락처 정보]

## 📚 참고 자료
- [Git 보안 모범 사례](https://git-scm.com/book/ko/v2/Git-도구-자격-증명-저장)
- [Python 보안 가이드](https://docs.python.org/3/library/security.html)
- [데이터베이스 보안 체크리스트](https://owasp.org/www-project-cheat-sheets/)

---
**⚠️ 주의**: 이 가이드라인을 준수하지 않으면 심각한 보안 위험이 발생할 수 있습니다. 정기적으로 검토하고 업데이트하세요. 