# Lambda 함수 배포 패키지 최적화 전략

## 현재 상태 분석

### Lambda 함수별 크기 분석
| 함수명 | 코드 크기 | 메모리 | Layer 사용 | 최적화 우선도 |
|--------|-----------|--------|------------|---------------|
| **makenaide-data-collector** | 651KB | 512MB | ❌ 없음 | 🔥 **높음** |
| **makenaide-integrated-orchestrator** | 7.7KB | 512MB | ❌ 없음 | 🟡 **중간** |
| **makenaide-basic-shutdown** | 5KB | 128MB | ❌ 없음 | 🟡 **중간** |
| **makenaide-scanner** | 4.7KB | 256MB | ✅ **core-layer** | ✅ **완료** |
| **makenaide-db-initializer** | 3KB | 256MB | ✅ **core-layer** | ✅ **완료** |

## 최적화 전략

### Phase 1: 대용량 함수 최적화 (Data Collector)
- **문제점**: 651KB 패키지 크기 (Layer 미사용)
- **해결책**: Core Layer 적용 + 코드 분할
- **예상 효과**: 651KB → ~5KB (99% 감소)

### Phase 2: 중간 크기 함수 최적화
- **대상**: integrated-orchestrator, basic-shutdown
- **해결책**: 공통 Layer 적용 + 지연 loading
- **예상 효과**: 50-80% 크기 감소

### Phase 3: 콜드 스타트 최적화 기법
1. **지연 Import 패턴**: 필요시에만 모듈 로드
2. **모듈 분할**: 핵심 로직과 헬퍼 함수 분리
3. **Connection Pooling**: DB 연결 재사용
4. **Lambda Provisioned Concurrency**: 웜 인스턴스 유지

## 성능 목표
- **패키지 크기**: 평균 90% 감소
- **콜드 스타트**: 5초 → 2초 이하
- **메모리 사용량**: 30% 감소
- **비용 절감**: 패키지 크기 기반 20% 절약

## 구현 순서
1. Data Collector 최적화 (즉시 효과)
2. 콜드 스타트 최적화 패턴 적용
3. 성능 측정 및 검증
4. 나머지 함수들 일괄 적용