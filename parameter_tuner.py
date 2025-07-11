"""
파라미터 튜닝 시스템
켈리공식과 ATR 기반 파라미터 최적화
"""

import pandas as pd
import numpy as np
import logging
import itertools
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import json
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TuningResult:
    """튜닝 결과"""
    kelly_fraction: float
    atr_multiplier: float
    total_return: float
    win_rate: float
    profit_factor: float
    max_drawdown: float
    sharpe_ratio: float
    total_trades: int
    score: float  # 종합 점수

class ParameterTuner:
    """파라미터 튜닝 시스템"""
    
    def __init__(self):
        self.results: List[TuningResult] = []
        self.best_result: Optional[TuningResult] = None
    
    def tune_parameters(self, ohlcv_df: pd.DataFrame, initial_capital: float = 1000000,
                       kelly_range: Tuple[float, float] = (0.1, 0.9),
                       atr_range: Tuple[float, float] = (0.5, 3.0),
                       step_size: float = 0.1) -> Dict:
        """
        파라미터 튜닝 실행
        
        Args:
            ohlcv_df: OHLCV 데이터
            initial_capital: 초기 자본금
            kelly_range: 켈리 비율 범위
            atr_range: ATR 배수 범위
            step_size: 튜닝 스텝 크기
            
        Returns:
            Dict: 튜닝 결과
        """
        try:
            logger.info(f"🔧 파라미터 튜닝 시작")
            logger.info(f"   - 켈리 비율 범위: {kelly_range[0]:.1f} ~ {kelly_range[1]:.1f}")
            logger.info(f"   - ATR 배수 범위: {atr_range[0]:.1f} ~ {atr_range[1]:.1f}")
            logger.info(f"   - 스텝 크기: {step_size}")
            
            # 파라미터 조합 생성
            kelly_values = np.arange(kelly_range[0], kelly_range[1] + step_size, step_size)
            atr_values = np.arange(atr_range[0], atr_range[1] + step_size, step_size)
            
            total_combinations = len(kelly_values) * len(atr_values)
            logger.info(f"   - 총 조합 수: {total_combinations}")
            
            # 각 조합에 대해 백테스트 실행
            for i, (kelly, atr) in enumerate(itertools.product(kelly_values, atr_values)):
                logger.info(f"   진행률: {i+1}/{total_combinations} ({((i+1)/total_combinations)*100:.1f}%)")
                
                # 백테스트 실행
                result = self._run_backtest(ohlcv_df, initial_capital, kelly, atr)
                
                if result:
                    # 종합 점수 계산
                    score = self._calculate_score(result)
                    result.score = score
                    
                    self.results.append(result)
                    
                    # 최고 성과 업데이트
                    if not self.best_result or score > self.best_result.score:
                        self.best_result = result
                        logger.info(f"   🎯 새로운 최고 성과: 켈리={kelly:.2f}, ATR={atr:.2f}, 점수={score:.3f}")
            
            # 결과 분석
            analysis = self._analyze_results()
            
            logger.info(f"✅ 파라미터 튜닝 완료")
            logger.info(f"   - 최적 켈리 비율: {self.best_result.kelly_fraction:.2f}")
            logger.info(f"   - 최적 ATR 배수: {self.best_result.atr_multiplier:.2f}")
            logger.info(f"   - 최고 점수: {self.best_result.score:.3f}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ 파라미터 튜닝 실패: {e}")
            return {}
    
    def _run_backtest(self, ohlcv_df: pd.DataFrame, initial_capital: float,
                     kelly_fraction: float, atr_multiplier: float) -> Optional[TuningResult]:
        """개별 백테스트 실행"""
        try:
            from kelly_backtest import KellyBacktester
            
            backtester = KellyBacktester()
            result = backtester.run_kelly_backtest(
                ohlcv_df=ohlcv_df,
                initial_capital=initial_capital,
                kelly_fraction=kelly_fraction,
                atr_multiplier=atr_multiplier
            )
            
            if result:
                return TuningResult(
                    kelly_fraction=kelly_fraction,
                    atr_multiplier=atr_multiplier,
                    total_return=result.total_return,
                    win_rate=result.win_rate,
                    profit_factor=result.profit_factor,
                    max_drawdown=result.max_drawdown,
                    sharpe_ratio=result.sharpe_ratio,
                    total_trades=result.total_trades,
                    score=0.0  # 나중에 계산
                )
            
            return None
            
        except Exception as e:
            logger.error(f"❌ 백테스트 실행 실패: {e}")
            return None
    
    def _calculate_score(self, result: TuningResult) -> float:
        """종합 점수 계산"""
        try:
            # 가중치 설정
            weights = {
                'total_return': 0.3,
                'win_rate': 0.2,
                'profit_factor': 0.2,
                'max_drawdown': 0.15,
                'sharpe_ratio': 0.15
            }
            
            # 정규화된 점수 계산
            scores = {}
            
            # 총 수익률 (높을수록 좋음)
            scores['total_return'] = max(0, min(1, (result.total_return + 50) / 100))
            
            # 승률 (높을수록 좋음)
            scores['win_rate'] = result.win_rate / 100
            
            # 수익 팩터 (높을수록 좋음)
            scores['profit_factor'] = min(1, result.profit_factor / 3)
            
            # 최대 낙폭 (낮을수록 좋음)
            scores['max_drawdown'] = max(0, 1 - (result.max_drawdown / 50))
            
            # 샤프 비율 (높을수록 좋음)
            scores['sharpe_ratio'] = max(0, min(1, (result.sharpe_ratio + 2) / 4))
            
            # 가중 평균 계산
            total_score = sum(scores[key] * weights[key] for key in weights)
            
            return total_score
            
        except Exception as e:
            logger.error(f"❌ 점수 계산 실패: {e}")
            return 0.0
    
    def _analyze_results(self) -> Dict:
        """결과 분석"""
        try:
            if not self.results:
                return {}
            
            # 결과를 DataFrame으로 변환
            df = pd.DataFrame([asdict(result) for result in self.results])
            
            # 상위 10개 결과
            top_10 = df.nlargest(10, 'score')
            
            # 파라미터별 성과 분석
            kelly_analysis = df.groupby('kelly_fraction')['score'].mean().sort_values(ascending=False)
            atr_analysis = df.groupby('atr_multiplier')['score'].mean().sort_values(ascending=False)
            
            # 상관관계 분석
            correlation = df[['kelly_fraction', 'atr_multiplier', 'score']].corr()
            
            analysis = {
                'best_result': asdict(self.best_result),
                'top_10_results': top_10.to_dict('records'),
                'kelly_analysis': kelly_analysis.to_dict(),
                'atr_analysis': atr_analysis.to_dict(),
                'correlation': correlation.to_dict(),
                'total_combinations': len(self.results),
                'score_statistics': {
                    'mean': df['score'].mean(),
                    'std': df['score'].std(),
                    'min': df['score'].min(),
                    'max': df['score'].max()
                }
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ 결과 분석 실패: {e}")
            return {}
    
    def generate_tuning_report(self, analysis: Dict, output_path: str = "parameter_tuning_report.html") -> str:
        """튜닝 리포트 생성"""
        try:
            if not analysis:
                return "분석 데이터가 없습니다."
            
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>파라미터 튜닝 리포트</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .section {{ margin: 20px 0; }}
        .metric-card {{ background-color: #fff; border: 1px solid #ddd; padding: 15px; border-radius: 5px; margin: 10px 0; }}
        .metric-value {{ font-size: 20px; font-weight: bold; color: #333; }}
        .metric-label {{ color: #666; margin-top: 5px; }}
        .positive {{ color: #28a745; }}
        .negative {{ color: #dc3545; }}
        .warning {{ color: #ffc107; }}
        table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
        th, td {{ padding: 10px; text-align: left; border: 1px solid #ddd; }}
        th {{ background-color: #f0f0f0; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>파라미터 튜닝 리포트</h1>
        <p>생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="section">
        <h2>🎯 최적 파라미터</h2>
        <div class="metric-card">
            <div class="metric-value positive">켈리 비율: {analysis['best_result']['kelly_fraction']:.2f}</div>
            <div class="metric-value positive">ATR 배수: {analysis['best_result']['atr_multiplier']:.2f}</div>
            <div class="metric-value positive">종합 점수: {analysis['best_result']['score']:.3f}</div>
        </div>
    </div>
    
    <div class="section">
        <h2>📊 최적 파라미터 성과</h2>
        <table>
            <tr>
                <th>지표</th>
                <th>값</th>
            </tr>
            <tr>
                <td>총 수익률</td>
                <td class="{'positive' if analysis['best_result']['total_return'] > 0 else 'negative'}">{analysis['best_result']['total_return']:.2f}%</td>
            </tr>
            <tr>
                <td>승률</td>
                <td class="{'positive' if analysis['best_result']['win_rate'] > 50 else 'warning'}">{analysis['best_result']['win_rate']:.2f}%</td>
            </tr>
            <tr>
                <td>수익 팩터</td>
                <td class="{'positive' if analysis['best_result']['profit_factor'] > 1 else 'negative'}">{analysis['best_result']['profit_factor']:.2f}</td>
            </tr>
            <tr>
                <td>최대 낙폭</td>
                <td class="{'negative' if analysis['best_result']['max_drawdown'] > 5 else 'positive'}">{analysis['best_result']['max_drawdown']:.2f}%</td>
            </tr>
            <tr>
                <td>샤프 비율</td>
                <td class="{'positive' if analysis['best_result']['sharpe_ratio'] > 1 else 'warning'}">{analysis['best_result']['sharpe_ratio']:.2f}</td>
            </tr>
            <tr>
                <td>총 거래</td>
                <td>{analysis['best_result']['total_trades']}회</td>
            </tr>
        </table>
    </div>
    
    <div class="section">
        <h2>🏆 상위 10개 결과</h2>
        <table>
            <tr>
                <th>순위</th>
                <th>켈리 비율</th>
                <th>ATR 배수</th>
                <th>종합 점수</th>
                <th>총 수익률</th>
                <th>승률</th>
            </tr>
"""
            
            for i, result in enumerate(analysis['top_10_results'][:10], 1):
                html_content += f"""
            <tr>
                <td>{i}</td>
                <td>{result['kelly_fraction']:.2f}</td>
                <td>{result['atr_multiplier']:.2f}</td>
                <td class="positive">{result['score']:.3f}</td>
                <td class="{'positive' if result['total_return'] > 0 else 'negative'}">{result['total_return']:.2f}%</td>
                <td class="{'positive' if result['win_rate'] > 50 else 'warning'}">{result['win_rate']:.2f}%</td>
            </tr>
"""
            
            html_content += """
        </table>
    </div>
    
    <div class="section">
        <h2>📈 켈리 비율별 평균 성과</h2>
        <table>
            <tr>
                <th>켈리 비율</th>
                <th>평균 점수</th>
            </tr>
"""
            
            for kelly, score in list(analysis['kelly_analysis'].items())[:10]:
                html_content += f"""
            <tr>
                <td>{kelly:.2f}</td>
                <td class="positive">{score:.3f}</td>
            </tr>
"""
            
            html_content += """
        </table>
    </div>
    
    <div class="section">
        <h2>📊 ATR 배수별 평균 성과</h2>
        <table>
            <tr>
                <th>ATR 배수</th>
                <th>평균 점수</th>
            </tr>
"""
            
            for atr, score in list(analysis['atr_analysis'].items())[:10]:
                html_content += f"""
            <tr>
                <td>{atr:.2f}</td>
                <td class="positive">{score:.3f}</td>
            </tr>
"""
            
            html_content += """
        </table>
    </div>
    
    <div class="section">
        <h2>📋 통계 요약</h2>
        <div class="metric-card">
            <div class="metric-label">총 조합 수: {analysis['total_combinations']}</div>
            <div class="metric-label">평균 점수: {analysis['score_statistics']['mean']:.3f}</div>
            <div class="metric-label">표준편차: {analysis['score_statistics']['std']:.3f}</div>
            <div class="metric-label">최소 점수: {analysis['score_statistics']['min']:.3f}</div>
            <div class="metric-label">최대 점수: {analysis['score_statistics']['max']:.3f}</div>
        </div>
    </div>
</body>
</html>
"""
            
            # 파일 저장
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"📄 튜닝 리포트 생성 완료: {output_path}")
            return html_content
            
        except Exception as e:
            logger.error(f"❌ 튜닝 리포트 생성 실패: {e}")
            return ""
    
    def plot_results(self, output_path: str = "parameter_tuning_plots.png"):
        """결과 시각화"""
        try:
            if not self.results:
                logger.warning("⚠️ 시각화할 결과가 없습니다")
                return
            
            # 결과를 DataFrame으로 변환
            df = pd.DataFrame([asdict(result) for result in self.results])
            
            # 그래프 생성
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            fig.suptitle('파라미터 튜닝 결과 분석', fontsize=16)
            
            # 1. 켈리 비율 vs 점수
            axes[0, 0].scatter(df['kelly_fraction'], df['score'], alpha=0.6)
            axes[0, 0].set_xlabel('켈리 비율')
            axes[0, 0].set_ylabel('종합 점수')
            axes[0, 0].set_title('켈리 비율 vs 종합 점수')
            axes[0, 0].grid(True, alpha=0.3)
            
            # 2. ATR 배수 vs 점수
            axes[0, 1].scatter(df['atr_multiplier'], df['score'], alpha=0.6)
            axes[0, 1].set_xlabel('ATR 배수')
            axes[0, 1].set_ylabel('종합 점수')
            axes[0, 1].set_title('ATR 배수 vs 종합 점수')
            axes[0, 1].grid(True, alpha=0.3)
            
            # 3. 켈리 비율 vs ATR 배수 (색상으로 점수 표시)
            scatter = axes[1, 0].scatter(df['kelly_fraction'], df['atr_multiplier'], 
                                       c=df['score'], cmap='viridis', alpha=0.7)
            axes[1, 0].set_xlabel('켈리 비율')
            axes[1, 0].set_ylabel('ATR 배수')
            axes[1, 0].set_title('켈리 비율 vs ATR 배수 (색상: 점수)')
            axes[1, 0].grid(True, alpha=0.3)
            plt.colorbar(scatter, ax=axes[1, 0])
            
            # 4. 수익률 vs 최대 낙폭
            colors = ['red' if x < 0 else 'green' for x in df['total_return']]
            axes[1, 1].scatter(df['max_drawdown'], df['total_return'], c=colors, alpha=0.6)
            axes[1, 1].set_xlabel('최대 낙폭 (%)')
            axes[1, 1].set_ylabel('총 수익률 (%)')
            axes[1, 1].set_title('리스크 vs 수익률')
            axes[1, 1].grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"📊 시각화 완료: {output_path}")
            
        except Exception as e:
            logger.error(f"❌ 시각화 실패: {e}")

def main():
    """메인 실행 함수"""
    try:
        # 파라미터 튜너 초기화
        tuner = ParameterTuner()
        
        # 샘플 데이터 생성 (실제로는 DB에서 가져와야 함)
        dates = pd.date_range('2024-01-01', '2024-12-31', freq='D')
        np.random.seed(42)
        
        # 가격 데이터 생성 (랜덤 워크)
        price = 50000
        prices = []
        for _ in range(len(dates)):
            price *= (1 + np.random.normal(0, 0.02))
            prices.append(price)
        
        # OHLCV 데이터 생성
        ohlcv_data = []
        for i, (date, price) in enumerate(zip(dates, prices)):
            high = price * (1 + abs(np.random.normal(0, 0.01)))
            low = price * (1 - abs(np.random.normal(0, 0.01)))
            open_price = prices[i-1] if i > 0 else price
            volume = np.random.randint(1000, 10000)
            
            ohlcv_data.append({
                'date': date,
                'open': open_price,
                'high': high,
                'low': low,
                'close': price,
                'volume': volume
            })
        
        df = pd.DataFrame(ohlcv_data)
        df.set_index('date', inplace=True)
        
        # 파라미터 튜닝 실행 (간소화된 범위)
        analysis = tuner.tune_parameters(
            ohlcv_df=df,
            initial_capital=1000000,
            kelly_range=(0.2, 0.8),
            atr_range=(1.0, 2.5),
            step_size=0.2
        )
        
        if analysis:
            # 리포트 생성
            report = tuner.generate_tuning_report(analysis)
            print("✅ 파라미터 튜닝 완료")
            print(f"   - 최적 켈리 비율: {analysis['best_result']['kelly_fraction']:.2f}")
            print(f"   - 최적 ATR 배수: {analysis['best_result']['atr_multiplier']:.2f}")
            print(f"   - 최고 점수: {analysis['best_result']['score']:.3f}")
        
    except Exception as e:
        logger.error(f"❌ 메인 실행 실패: {e}")

if __name__ == "__main__":
    main() 