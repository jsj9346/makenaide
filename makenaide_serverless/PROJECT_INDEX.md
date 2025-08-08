# 📚 Makenaide 프로젝트 문서 인덱스

## 🎯 프로젝트 개요

**Makenaide**는 업비트(Upbit) API 기반 암호화폐 자동매매 봇으로, AWS Lambda 아키텍처를 활용한 서버리스 트레이딩 시스템입니다.

### 핵심 철학
- **전략 우선, 자동화는 도구**: 자동매매는 전략이 검증되었을 때만 의미가 있음
- **"지지 않는 것"에 집중**: 장기 생존과 복리 구조를 목표로 함
- **손실은 짧고 수익은 길게**: 체계적인 리스크 관리와 함께 수익 극대화 추구

---

## 📂 프로젝트 구조

### 🏗️ 아키텍처 개요
```
makenaide/
├── 📊 Core Trading System (로컬 실행)
├── ☁️ AWS Lambda Functions (서버리스)
├── 🗄️ PostgreSQL Database (RDS)  
├── 📈 Analysis & Monitoring
└── 📋 Documentation & Guides
```

---

## 🧩 주요 컴포넌트

### 1️⃣ **Core Trading System** (로컬 실행)

#### 트레이딩 엔진
- **`backtester.py`** - 백테스팅 엔진 및 전략 검증
- **`trade_executor.py`** - 실제 거래 실행 로직
- **`strategy_tuner.py`** - 전략 매개변수 최적화
- **`parameter_tuner.py`** - 동적 파라미터 조정

#### 데이터 수집 및 분석  
- **`scanner.py`** - 티커 스캐닝 및 기회 탐지
- **`data_quality_monitor.py`** - 데이터 품질 모니터링
- **`market_sentiment.py`** - 시장 감정 분석
- **`realtime_monitor.py`** - 실시간 시장 모니터링

#### 필터링 시스템
- **`filter_rules/`** - 거래 필터링 규칙
  - `rule_price.py` - 가격 기반 필터링
  - `rule_volume.py` - 거래량 기반 필터링  
  - `rule_momentum.py` - 모멘텀 기반 필터링

### 2️⃣ **AWS Lambda Functions** (서버리스)

#### 🔥 **최적화 완료** - 99.6% 패키지 크기 감소 달성

| Lambda 함수 | 역할 | 패키지 크기 | Layer 적용 | 최적화 상태 |
|-------------|------|-------------|------------|-------------|
| **makenaide-data-collector** | API 데이터 수집 | 2.5KB ⭐ | ✅ Core Layer | 🔥 최적화 완료 |
| **makenaide-scanner** | 티커 스캐닝 | 4.7KB | ✅ Core Layer | ✅ 최적화 완료 |
| **makenaide-db-initializer** | DB 스키마 초기화 | 3KB | ✅ Core Layer | ✅ 최적화 완료 |
| **makenaide-integrated-orchestrator** | 통합 오케스트레이터 | 7.7KB | ❌ 없음 | ⏳ 대기 |

#### Lambda Layer 아키텍처
- **`makenaide-core-layer:1`** (3.9MB)
  - psycopg2-binary==2.9.9 (PostgreSQL 드라이버)
  - pg8000==1.31.2 (Pure Python PostgreSQL)
  - requests==2.31.0 (HTTP 라이브러리)
  - pyupbit==0.2.30 (Upbit API)

### 3️⃣ **Configuration & Management**

#### 설정 관리
- **`config/unified_config.py`** - 통합 설정 관리
- **`config/mode_presets.py`** - 모드별 프리셋
- **`config_loader.py`** - 동적 설정 로딩

#### 데이터베이스
- **`init_db_pg.py`** - PostgreSQL 데이터베이스 초기화
- **`db_validation_system.py`** - DB 유효성 검증

### 4️⃣ **Monitoring & Analysis**

#### 성능 모니터링
- **`performance_monitor.py`** - 거래 성능 추적
- **`optimized_data_monitor.py`** - 최적화된 데이터 모니터링
- **`aws_cloudwatch_monitor.py`** - AWS CloudWatch 통합

#### 백테스팅 & 분석
- **`backtest_analyzer.py`** - 백테스트 결과 분석
- **`run_comprehensive_backtest.py`** - 종합 백테스트 실행

---

## 📖 Documentation Library

### 🏆 **최적화 성과 문서**
- **[Lambda 최적화 가이드](/lambda_scanner/lambda_optimization_guide.md)** - 99.6% 패키지 크기 감소 달성
- **[최적화 전략 문서](/lambda_scanner/optimization_strategy.md)** - 콜드 스타트 48% 성능 향상

### 📋 **운영 가이드**
- **[CLAUDE.md](/CLAUDE.md)** - 프로젝트 가이드라인 및 운영 철학
- **[SECURITY_GUIDELINES.md](/SECURITY_GUIDELINES.md)** - 보안 가이드라인
- **[README.MD](/README.MD)** - 프로젝트 개요 및 시작 가이드

### 📊 **분석 리포트**
- **[Phase1 Validation Report](/Phase1_Validation_Report.md)** - 1단계 검증 결과
- **[Kelly Backtest Report](/kelly_backtest_report.md)** - 켈리 기준 백테스트 결과
- **[Deployment Completion Report](/deployment_completion_report.md)** - AWS 배포 완료 보고서

### 🔧 **기술 문서**
- **[PSYCOPG2 Troubleshooting](/PSYCOPG2_TROUBLESHOOTING_REPORT.md)** - PostgreSQL 연결 문제 해결
- **[Basic Pipeline Guide](/README_BASIC_PIPELINE.md)** - 기본 파이프라인 구성 방법

---

## 🚀 **최적화 성과 하이라이트**

### Lambda 함수 최적화 달성 성과
```yaml
Data Collector 최적화:
  패키지_크기: 651KB → 2.5KB (99.6% 감소)
  콜드_스타트: 1.8초 → 0.94초 (48% 향상)
  웜_스타트: 0.08초 → 0.03초 (62% 향상)
  
최적화_기법:
  - 지연_로딩: Heavy 모듈의 필요시 로드
  - Lambda_Layer: 공통 의존성 분리 (3.9MB)
  - API_최적화: 타임아웃 및 데이터량 최적화
  - 캐싱_전략: 글로벌 변수 활용
```

### 아키텍처 개선 사항
- **3-Layer 구조**: Application + Lambda Layer + Runtime
- **지연 로딩 패턴**: 99.6% 패키지 크기 감소 달성
- **공통 Layer 재사용**: 모든 Lambda 함수에 적용 가능
- **자동화 배포**: 최적화 템플릿 및 스크립트 제공

---

## 🎯 **트레이딩 전략 이론**

### 3대 핵심 이론 기반
1. **스탠 와인스타인의 4단계 사이클 이론**
   - Stage 1: 기반 구축 (Accumulation)
   - Stage 2: 상승 돌파 ⭐ **핵심 매수 구간**
   - Stage 3: 분배 단계 (Distribution)  
   - Stage 4: 하락 단계 (Decline)

2. **마크 미너비니의 정밀 타이밍 전략**
   - VCP (Volatility Contraction Pattern)
   - 25% 법칙: 각 수축은 이전 고점 대비 25% 이내
   - 7-8% 손절 규칙: 무조건 손절 (예외 없음)

3. **윌리엄 오닐의 차트 패턴 및 브레이크아웃**
   - CANSLIM 시스템
   - 컵 앤 핸들, 플랫 베이스 패턴
   - 8% 규칙 및 2.5% 규칙

---

## 🔧 **개발 환경 & 도구**

### 기술 스택
- **언어**: Python 3.11
- **클라우드**: AWS (Lambda, RDS, CloudWatch)
- **데이터베이스**: PostgreSQL 17
- **API**: Upbit REST API
- **모니터링**: CloudWatch, 자체 개발 모니터링

### 의존성 관리
- **Lambda Layer**: 공통 패키지 중앙 관리
- **최적화된 패키지**: pg8000, psycopg2, requests, pyupbit
- **버전 관리**: Git 기반 코드 관리

---

## 📞 **Quick Reference**

### 주요 명령어
```bash
# 로컬 백테스팅 실행
python backtester.py

# AWS Lambda 배포 (최적화된)
./deploy_optimized_lambda.sh makenaide-data-collector

# 데이터베이스 초기화
python init_db_pg.py

# 실시간 모니터링 시작  
python realtime_monitor.py
```

### 중요 설정 파일
- **`config/unified_config.py`** - 메인 설정
- **`env.template`** - 환경 변수 템플릿
- **`CLAUDE.md`** - 프로젝트 가이드라인

### AWS 리소스
- **RDS Endpoint**: makenaide.cni2ka4ugf7f.ap-northeast-2.rds.amazonaws.com
- **Lambda Layer ARN**: arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1
- **Region**: ap-northeast-2 (Seoul)

---

## 🗺️ **Project Navigation**

### 신규 개발자 온보딩 순서
1. **[README.MD](/README.MD)** - 프로젝트 개요 파악
2. **[CLAUDE.md](/CLAUDE.md)** - 개발 가이드라인 숙지  
3. **[최적화 가이드](/lambda_scanner/lambda_optimization_guide.md)** - Lambda 개발 패턴 학습
4. **[Phase1 Report](/Phase1_Validation_Report.md)** - 검증 결과 확인

### 운영자 참고 문서
1. **[Deployment Report](/deployment_completion_report.md)** - 배포 상태 확인
2. **[Security Guidelines](/SECURITY_GUIDELINES.md)** - 보안 정책 준수
3. **[Troubleshooting](/PSYCOPG2_TROUBLESHOOTING_REPORT.md)** - 문제 해결 가이드

### 개발자 참고 자료
1. **[Lambda 최적화 템플릿](/lambda_scanner/lambda_optimization_guide.md)** - 개발 표준
2. **[코어 시스템](/scanner.py, /backtester.py)** - 핵심 로직 참고
3. **[AWS 스크립트](/aws_setup_scripts/)** - 인프라 자동화

---

**Last Updated**: 2025-08-05  
**Version**: v2.0 (최적화 완료)  
**Optimization Achievement**: 99.6% 패키지 크기 감소, 48% 콜드 스타트 성능 향상