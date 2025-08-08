# Makenaide Basic Pipeline 배포 완료

## 🎯 배포 개요

4시간 간격으로 자동 실행되는 Makenaide Basic Pipeline이 성공적으로 배포되었습니다.

### 배포된 리소스

1. **Lambda 함수들**
   - `makenaide-basic-controller`: EC2 인스턴스 시작
   - `makenaide-basic-shutdown`: EC2 인스턴스 종료  
   - `makenaide-basic-orchestrator`: 전체 파이프라인 조정

2. **EventBridge 규칙**
   - `makenaide-basic-scheduler`: 4시간 간격 자동 실행 (rate(4 hours))

3. **IAM 역할**
   - `makenaide-basic-execution-role`: Lambda 실행 권한

## 🚀 실행 플로우

```
EventBridge (4시간마다) 
    ↓
makenaide-basic-orchestrator 
    ↓
makenaide-basic-controller (EC2 시작)
    ↓
EC2에서 makenaide.py 실행 (SSH)
    ↓
makenaide-basic-shutdown (EC2 종료)
```

## 📋 사용 방법

### 1. 자동 실행 (권장)
- 4시간마다 EventBridge가 자동으로 `makenaide-basic-orchestrator` 함수를 호출
- 별도의 작업 없이 자동으로 실행됩니다

### 2. 수동 실행
AWS Lambda 콘솔에서 직접 실행:

1. AWS Lambda 콘솔 접속
2. `makenaide-basic-orchestrator` 함수 선택
3. "Test" 버튼 클릭하여 실행

### 3. 개별 함수 테스트
필요에 따라 개별 함수를 테스트할 수 있습니다:

- `makenaide-basic-controller`: EC2 시작만 테스트
- `makenaide-basic-shutdown`: EC2 종료만 테스트

## 📊 모니터링

### CloudWatch 로그 확인
각 Lambda 함수의 실행 로그는 CloudWatch에서 확인할 수 있습니다:

- `/aws/lambda/makenaide-basic-controller`
- `/aws/lambda/makenaide-basic-shutdown`
- `/aws/lambda/makenaide-basic-orchestrator`

### EventBridge 실행 확인
EventBridge 콘솔에서 `makenaide-basic-scheduler` 규칙의 실행 기록을 확인할 수 있습니다.

## ⚙️ 설정 변경

### 실행 간격 변경
EventBridge 규칙에서 스케줄 표현식을 수정:
- 현재: `rate(4 hours)` (4시간마다)
- 예시: `rate(2 hours)` (2시간마다), `rate(1 day)` (1일마다)

### 함수 설정 변경
Lambda 함수의 환경변수, 타임아웃, 메모리 등을 AWS 콘솔에서 수정 가능

## 🔧 문제 해결

### 1. SSH 연결 실패
- EC2 인스턴스가 제대로 시작되었는지 확인
- 보안 그룹에서 SSH(22번 포트) 접근 허용 여부 확인
- PEM 키 파일이 Lambda 환경변수에 올바르게 설정되었는지 확인

### 2. makenaide.py 실행 실패
- EC2에 makenaide.py 파일이 `/home/ec2-user/makenaide/` 경로에 있는지 확인
- Python 의존성이 모두 설치되어 있는지 확인
- 환경변수(.env) 파일이 올바르게 설정되어 있는지 확인

### 3. EC2 종료 실패
- IAM 권한에서 EC2 stop 권한이 있는지 확인
- 인스턴스 ID가 올바른지 확인

## 📝 배포 파일들

- `deploy_basic_pipeline.py`: 전체 파이프라인 배포 스크립트
- `test_basic_pipeline.py`: 배포된 파이프라인 테스트 스크립트
- `lambda_basic_orchestrator.py`: 오케스트레이터 Lambda 함수
- `lambda_controller_basic.py`: EC2 시작 Lambda 함수
- `lambda_shutdown_basic.py`: EC2 종료 Lambda 함수

## 🔄 기존 파이프라인과의 차이점

### Basic Pipeline (신규)
- 간단한 4시간 간격 실행
- SSH 기반 직접 실행
- SQS 큐 없음
- 비용 최적화 목적

### 기존 Pipeline (유지)
- 복잡한 SQS 기반 파이프라인
- 더 많은 Lambda 함수 사용
- 세밀한 제어 가능

## ✅ 배포 완료 상태

- [x] IAM 역할 생성
- [x] Lambda 함수 3개 배포
- [x] EventBridge 규칙 생성  
- [x] 권한 설정 완료
- [x] 테스트 검증 완료

Basic Pipeline이 성공적으로 배포되어 4시간마다 자동으로 실행될 준비가 완료되었습니다! 