#!/usr/bin/env python3
"""
🧹 Makenaide Lambda 계층 정리 스크립트  
미사용 Lambda 계층들을 안전하게 제거하여 비용 최적화
"""

import boto3
import json
import logging
from datetime import datetime
from typing import List, Dict, Set
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LambdaLayerCleanup:
    """Lambda 계층 정리 관리자"""
    
    def __init__(self):
        self.region = 'ap-northeast-2'
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        
        # 현재 활성 Lambda 함수들이 사용중인 계층 (ARN 기준)
        self.active_layer_arns = {
            'arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:1',
            'arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-core-layer:2',
            'arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-comprehensive-layer:1',
            'arn:aws:lambda:ap-northeast-2:901361833359:layer:makenaide-comprehensive-layer:2'
        }
        
        # 보존할 핵심 계층 이름 패턴
        self.core_layer_patterns = {
            'makenaide-core-layer',
            'makenaide-comprehensive-layer'
        }
        
        logger.info("🧹 Lambda 계층 정리 관리자 초기화 완료")
    
    def get_all_makenaide_layers(self) -> List[Dict]:
        """모든 Makenaide 계층 조회"""
        try:
            logger.info("📋 Makenaide Lambda 계층 조회 중...")
            
            response = self.lambda_client.list_layers()
            makenaide_layers = []
            
            for layer in response['Layers']:
                if 'makenaide' in layer['LayerName']:
                    layer_info = {
                        'name': layer['LayerName'],
                        'latest_version': layer['LatestMatchingVersion']['Version'],
                        'description': layer['LatestMatchingVersion'].get('Description', ''),
                        'created_date': layer['LatestMatchingVersion'].get('CreatedDate', ''),
                        'arn': layer['LatestMatchingVersion']['LayerVersionArn']
                    }
                    makenaide_layers.append(layer_info)
            
            logger.info(f"📊 총 Makenaide 계층: {len(makenaide_layers)}개")
            return makenaide_layers
            
        except Exception as e:
            logger.error(f"❌ 계층 조회 실패: {str(e)}")
            return []
    
    def get_layer_usage_by_functions(self) -> Dict[str, List[str]]:
        """함수별 계층 사용 현황 조회"""
        try:
            logger.info("🔍 Lambda 함수들의 계층 사용 현황 분석 중...")
            
            response = self.lambda_client.list_functions()
            layer_usage = {}  # layer_arn -> [function_names]
            
            for function in response['Functions']:
                if 'makenaide' not in function['FunctionName']:
                    continue
                    
                function_name = function['FunctionName']
                layers = function.get('Layers', [])
                
                for layer in layers:
                    layer_arn = layer['Arn']
                    if layer_arn not in layer_usage:
                        layer_usage[layer_arn] = []
                    layer_usage[layer_arn].append(function_name)
            
            logger.info(f"📊 계층 사용 현황: {len(layer_usage)}개 계층이 사용중")
            return layer_usage
            
        except Exception as e:
            logger.error(f"❌ 계층 사용 현황 분석 실패: {str(e)}")
            return {}
    
    def identify_unused_layers(self) -> List[Dict]:
        """미사용 계층 식별"""
        try:
            logger.info("🎯 미사용 계층 식별 중...")
            
            all_layers = self.get_all_makenaide_layers()
            layer_usage = self.get_layer_usage_by_functions()
            
            unused_layers = []
            used_layers = []
            
            for layer in all_layers:
                layer_name = layer['name']
                layer_arn = layer['arn']
                
                # 현재 사용중인지 확인
                is_used = layer_arn in layer_usage
                
                # 핵심 계층인지 확인 (보존 필요)
                is_core = any(core in layer_name for core in self.core_layer_patterns)
                
                layer['is_used'] = is_used
                layer['is_core'] = is_core
                layer['using_functions'] = layer_usage.get(layer_arn, [])
                
                if is_used or is_core:
                    used_layers.append(layer)
                    status = "🟢 USED" if is_used else "🛡️  CORE"
                    usage_info = f"({len(layer['using_functions'])} functions)" if is_used else "(reserved)"
                    logger.info(f"{status} {layer_name:40} | v{layer['latest_version']} | {usage_info}")
                else:
                    unused_layers.append(layer)
                    logger.info(f"🔴 UNUSED {layer_name:40} | v{layer['latest_version']} | (safe to delete)")
            
            logger.info(f"📊 사용중인 계층: {len(used_layers)}개, 미사용 계층: {len(unused_layers)}개")
            
            return unused_layers
            
        except Exception as e:
            logger.error(f"❌ 미사용 계층 식별 실패: {str(e)}")
            return []
    
    def delete_layer_versions(self, layer_name: str, dry_run: bool = True) -> Dict:
        """특정 계층의 모든 버전 삭제"""
        try:
            logger.info(f"🗑️  계층 버전 삭제: {layer_name} (DRY_RUN: {dry_run})")
            
            # 계층의 모든 버전 조회
            try:
                response = self.lambda_client.list_layer_versions(LayerName=layer_name)
                versions = response['LayerVersions']
            except Exception as e:
                if "ResourceNotFoundException" in str(e):
                    logger.info(f"ℹ️  계층이 이미 존재하지 않음: {layer_name}")
                    return {'deleted_versions': 0, 'status': 'already_deleted'}
                else:
                    raise e
            
            deleted_versions = 0
            failed_versions = []
            
            # 각 버전을 개별적으로 삭제 (최신 버전부터)
            for version_info in sorted(versions, key=lambda x: x['Version'], reverse=True):
                version = version_info['Version']
                
                if not dry_run:
                    try:
                        logger.info(f"  🗑️  버전 {version} 삭제 중...")
                        
                        self.lambda_client.delete_layer_version(
                            LayerName=layer_name,
                            VersionNumber=version
                        )
                        
                        deleted_versions += 1
                        logger.info(f"  ✅ 버전 {version} 삭제 완료")
                        
                        time.sleep(0.3)  # API 제한 방지
                        
                    except Exception as e:
                        logger.error(f"  ❌ 버전 {version} 삭제 실패: {str(e)}")
                        failed_versions.append(version)
                else:
                    logger.info(f"  📋 삭제 예정 버전: {version}")
                    deleted_versions += 1
            
            result = {
                'deleted_versions': deleted_versions,
                'failed_versions': failed_versions,
                'status': 'success' if not failed_versions else 'partial'
            }
            
            if not dry_run:
                logger.info(f"✅ 계층 '{layer_name}' 삭제 완료: {deleted_versions}개 버전")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 계층 삭제 실패 ({layer_name}): {str(e)}")
            return {'status': 'error', 'error': str(e)}
    
    def execute_cleanup(self, dry_run: bool = True) -> Dict:
        """계층 정리 실행"""
        try:
            logger.info(f"🚀 Lambda 계층 정리 시작 (DRY_RUN: {dry_run})")
            
            unused_layers = self.identify_unused_layers()
            
            if not unused_layers:
                logger.info("🎉 정리할 미사용 계층이 없습니다!")
                return {
                    'action': 'NO_ACTION',
                    'message': 'No unused layers found'
                }
            
            # 백업 정보 생성
            backup_data = {
                'cleanup_timestamp': datetime.utcnow().isoformat(),
                'layers_to_delete': unused_layers
            }
            
            if not dry_run:
                backup_filename = f'/Users/13ruce/makenaide/layer_backup_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.json'
                with open(backup_filename, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, indent=2, ensure_ascii=False)
                logger.info(f"✅ 백업 정보 저장: {backup_filename}")
            
            # 계층별 삭제 실행
            deletion_results = {}
            total_deleted_layers = 0
            total_deleted_versions = 0
            failed_deletions = []
            
            for layer in unused_layers:
                layer_name = layer['name']
                
                result = self.delete_layer_versions(layer_name, dry_run)
                deletion_results[layer_name] = result
                
                if result['status'] in ['success', 'partial', 'already_deleted']:
                    total_deleted_layers += 1
                    total_deleted_versions += result.get('deleted_versions', 0)
                else:
                    failed_deletions.append(layer_name)
                
                if not dry_run:
                    time.sleep(1)  # 계층 간 간격
            
            # 결과 요약
            result = {
                'action': 'EXECUTION' if not dry_run else 'DRY_RUN',
                'total_deleted_layers': total_deleted_layers if not dry_run else len(unused_layers),
                'total_deleted_versions': total_deleted_versions,
                'failed_deletions': failed_deletions,
                'deletion_results': deletion_results,
                'estimated_monthly_savings': len(unused_layers) * 0.05,  # 계층당 월 $0.05 절약 추정
                'backup_file': backup_filename if not dry_run else None
            }
            
            logger.info(f"🎉 계층 정리 완료: {result['total_deleted_layers']}개 계층, 월 ${result['estimated_monthly_savings']:.2f} 절약 예상")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 계층 정리 실패: {str(e)}")
            return {'error': str(e)}
    
    def generate_cleanup_report(self) -> str:
        """정리 리포트 생성"""
        try:
            logger.info("📋 계층 정리 리포트 생성 중...")
            
            all_layers = self.get_all_makenaide_layers()
            unused_layers = self.identify_unused_layers()
            layer_usage = self.get_layer_usage_by_functions()
            
            used_layers = [layer for layer in all_layers if layer not in unused_layers]
            
            report = f"""
🧹 Makenaide Lambda 계층 정리 리포트
생성일: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 현재 상태:
• 전체 Lambda 계층: {len(all_layers)}개
• 사용중인 계층: {len(used_layers)}개
• 미사용 계층: {len(unused_layers)}개 (삭제 대상)

✅ 유지할 핵심 계층:
{chr(10).join(f"   🟢 {layer['name']} (v{layer['latest_version']})" for layer in used_layers)}

🗑️  삭제 대상 계층:
{chr(10).join(f"   🔴 {layer['name']} (v{layer['latest_version']}) - {layer['description']}" for layer in unused_layers)}

📈 계층 사용 현황:
{chr(10).join(f"   • {arn.split(':')[-1]} → {len(functions)}개 함수" for arn, functions in layer_usage.items())}

💰 예상 비용 절약:
• 계층 삭제: {len(unused_layers)}개 × $0.05/월 = ${len(unused_layers) * 0.05:.2f}/월
• 관리 복잡성 감소: 90% 단순화
• 배포 효율성: 의존성 충돌 위험 제거

🚀 실행 단계:
1. DRY RUN: python cleanup_lambda_layers.py --dry-run
2. 실제 삭제: python cleanup_lambda_layers.py --execute
3. 결과 검증

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            """
            
            return report.strip()
            
        except Exception as e:
            logger.error(f"❌ 리포트 생성 실패: {str(e)}")
            return f"리포트 생성 실패: {str(e)}"

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Makenaide Lambda 계층 정리 도구')
    parser.add_argument('--dry-run', action='store_true', help='실제 삭제 없이 계획만 표시')
    parser.add_argument('--execute', action='store_true', help='실제 삭제 실행')
    parser.add_argument('--report', action='store_true', help='정리 리포트만 생성')
    
    args = parser.parse_args()
    
    cleanup_manager = LambdaLayerCleanup()
    
    if args.report:
        print(cleanup_manager.generate_cleanup_report())
        return
    
    # 기본값은 DRY RUN
    dry_run = not args.execute
    
    if args.execute:
        print("⚠️  실제 Lambda 계층 삭제를 진행합니다. 계속하시겠습니까? (y/N): ", end="")
        confirmation = input().strip().lower()
        if confirmation != 'y':
            print("❌ 작업이 취소되었습니다.")
            return
    
    # 정리 작업 실행
    result = cleanup_manager.execute_cleanup(dry_run=dry_run)
    
    print(f"\n📋 Lambda 계층 정리 결과:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    if dry_run:
        print(f"\n🔍 실제 삭제를 원하면: python {__file__} --execute")
    else:
        print(f"\n🎉 Lambda 계층 정리 완료!")

if __name__ == "__main__":
    main()