"""
Disclaimer 동의 관리 시스템

🔧 주요 기능:
- Disclaimer 표시 및 동의 처리
- DB 기반 동의 상태 관리
- 버전 관리 및 업데이트 감지
- 재실행 시 동의 상태 확인
"""

import logging
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any
import os
import sys

logger = logging.getLogger(__name__)

class DisclaimerManager:
    """Disclaimer 동의 관리 클래스"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.current_version = "1.0.0"
        self.disclaimer_text = self._get_disclaimer_text()
        self.disclaimer_hash = self._calculate_hash(self.disclaimer_text)
    
    def _get_disclaimer_text(self) -> str:
        """Disclaimer 텍스트 반환"""
        return """
╔══════════════════════════════════════════════════════════════════════════════╗
║                           🚨 MAKENAIDE DISCLAIMER 🚨                         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ⚠️  투자 위험 고지                                                          ║
║                                                                              ║
║  1. 암호화폐 투자는 높은 위험을 수반합니다.                                  ║
║     - 가격 변동성이 매우 크며, 원금 손실 가능성이 있습니다.                  ║
║     - 단기간에 50% 이상의 손실이 발생할 수 있습니다.                        ║
║                                                                              ║
║  2. 자동매매 시스템의 한계                                                   ║
║     - 기술적 오류, 네트워크 장애, API 제한 등으로 인한 손실 가능성          ║
║     - 시장 급변 시 즉시 대응하지 못할 수 있습니다.                          ║
║     - 과거 성과가 미래 수익을 보장하지 않습니다.                            ║
║                                                                              ║
║  3. 법적 고지사항                                                           ║
║                                                                              ║
║     - 실제 투자에 사용 시 발생하는 손실에 대해 제작자는 책임지지 않습니다.   ║
║     - 투자자는 자신의 판단과 책임 하에 투자해야 합니다.                     ║
║     - 투자자는 자신의 판단과 책임 하에 투자해야 합니다.                     ║
║                                                                              ║
║  4. 시스템 사용 조건                                                        ║
║     - API 키 및 개인정보 보안에 유의하세요.                                 ║
║     - 정기적인 백업과 모니터링이 필요합니다.                                ║
║     - 시스템 업데이트 시 기존 설정을 확인하세요.                            ║
║                                                                              ║
║  📋 동의 여부: 이 시스템을 사용함으로써 위의 위험사항을 충분히 이해하고      ║
║     동의함을 확인합니다.                                                    ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
    
    def _calculate_hash(self, text: str) -> str:
        """텍스트 해시 계산"""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()
    
    def check_agreement_status(self) -> bool:
        """동의 상태 확인"""
        try:
            with self.db_manager.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, agreed_at, agreement_version 
                        FROM disclaimer_agreements 
                        WHERE is_active = TRUE 
                        AND agreement_version = %s
                        ORDER BY agreed_at DESC 
                        LIMIT 1
                    """, (self.current_version,))
                    
                    result = cursor.fetchone()
                    
                    if result:
                        logger.info(f"✅ Disclaimer 동의 확인됨 (버전: {self.current_version}, 동의일: {result[2]})")
                        return True
                    else:
                        logger.info("⚠️ Disclaimer 동의 필요")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Disclaimer 동의 상태 확인 실패: {e}")
            return False
    
    def display_disclaimer(self) -> bool:
        """Disclaimer 표시 및 동의 처리"""
        try:
            print(self.disclaimer_text)
            
            while True:
                response = input("\n위의 위험사항을 충분히 이해하고 동의하시겠습니까? (y/n): ").strip().lower()
                
                if response in ['y', 'yes', '예', '동의']:
                    return self._save_agreement()
                elif response in ['n', 'no', '아니오', '거부']:
                    print("\n❌ Disclaimer 동의가 거부되었습니다. 프로그램을 종료합니다.")
                    return False
                else:
                    print("⚠️ 'y' 또는 'n'으로 답변해 주세요.")
                    
        except KeyboardInterrupt:
            print("\n\n❌ 사용자가 프로그램을 중단했습니다.")
            return False
        except Exception as e:
            logger.error(f"❌ Disclaimer 표시 중 오류: {e}")
            return False
    
    def _save_agreement(self) -> bool:
        """동의 내용을 DB에 저장"""
        try:
            with self.db_manager.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO disclaimer_agreements 
                        (agreement_version, agreed_by, agreement_text_hash, is_active)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        self.current_version,
                        'user',
                        self.disclaimer_hash,
                        True
                    ))
                    conn.commit()
                    
            logger.info(f"✅ Disclaimer 동의 저장 완료 (버전: {self.current_version})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Disclaimer 동의 저장 실패: {e}")
            return False
    
    def ensure_agreement(self) -> bool:
        """동의 상태 확인 및 필요시 동의 요청"""
        try:
            # 테이블 존재 확인 및 생성
            # create_disclaimer_table 함수 호출 부분 삭제
            
            # 동의 상태 확인
            if self.check_agreement_status():
                return True
            
            # 동의가 필요한 경우 표시
            return self.display_disclaimer()
            
        except Exception as e:
            logger.error(f"❌ Disclaimer 동의 처리 중 오류: {e}")
            return False 