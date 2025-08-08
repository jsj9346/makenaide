#!/usr/bin/env python3
"""
시간대별 백테스트 결과 리포트 생성기

시간대별 백테스트 결과를 시각화하고 상세 리포트를 생성합니다.

주요 기능:
1. 시간대별 성과 차트 생성
2. 지역별 성과 분석 시각화  
3. 글로벌 활성도와 수익률 상관관계 그래프
4. 종합 리포트 HTML/PDF 생성
5. 전략별 시간대 최적화 권장사항

Author: Timezone Backtesting Visualization
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import base64
import io

# 한글 폰트 설정
plt.rcParams['font.family'] = ['Arial Unicode MS', 'DejaVu Sans', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

# 로거 설정
from utils import setup_logger
logger = setup_logger()

class TimezoneReportGenerator:
    """시간대별 백테스트 리포트 생성기"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # 색상 팔레트
        self.colors = {
            'primary': '#1f77b4',
            'secondary': '#ff7f0e', 
            'success': '#2ca02c',
            'danger': '#d62728',
            'warning': '#ff7f0e',
            'info': '#17a2b8',
            'asia': '#ff6b6b',
            'europe': '#4ecdc4', 
            'america': '#45b7d1',
            'performance': '#96ceb4',
            'risk': '#ffeaa7'
        }
        
        logger.info("🎨 시간대별 리포트 생성기 초기화 완료")
    
    def generate_comprehensive_report(self, timezone_results: Dict[str, Any], 
                                    output_filename: str = None) -> str:
        """종합 시간대별 백테스트 리포트 생성"""
        try:
            if not output_filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_filename = f"timezone_backtest_report_{timestamp}"
            
            logger.info(f"📊 종합 리포트 생성 시작: {output_filename}")
            
            # 1. 차트들 생성
            charts = self._generate_all_charts(timezone_results)
            
            # 2. HTML 리포트 생성
            html_report = self._generate_html_report(timezone_results, charts, output_filename)
            
            # 3. 요약 텍스트 리포트 생성
            text_report = self._generate_text_summary(timezone_results)
            
            # 4. 파일 저장
            html_path = self.output_dir / f"{output_filename}.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_report)
            
            text_path = self.output_dir / f"{output_filename}_summary.txt"
            with open(text_path, 'w', encoding='utf-8') as f:
                f.write(text_report)
            
            # 5. JSON 데이터 저장 (추가 분석용)
            json_path = self.output_dir / f"{output_filename}_data.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(timezone_results, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"✅ 종합 리포트 생성 완료:")
            logger.info(f"   - HTML: {html_path}")
            logger.info(f"   - 요약: {text_path}")
            logger.info(f"   - 데이터: {json_path}")
            
            return str(html_path)
            
        except Exception as e:
            logger.error(f"❌ 종합 리포트 생성 실패: {e}")
            return ""
    
    def _generate_all_charts(self, timezone_results: Dict[str, Any]) -> Dict[str, str]:
        """모든 차트 생성"""
        charts = {}
        
        try:
            # 1. 시간대별 성과 차트
            charts['hourly_performance'] = self._create_hourly_performance_chart(timezone_results)
            
            # 2. 지역별 성과 차트
            charts['regional_performance'] = self._create_regional_performance_chart(timezone_results)
            
            # 3. 활성도-수익률 상관관계 차트
            charts['activity_correlation'] = self._create_activity_correlation_chart(timezone_results)
            
            # 4. 전략별 시간대 히트맵
            charts['strategy_heatmap'] = self._create_strategy_timezone_heatmap(timezone_results)
            
            # 5. 글로벌 활성도 타임라인
            charts['global_activity'] = self._create_global_activity_timeline(timezone_results)
            
            logger.info(f"📈 {len(charts)}개 차트 생성 완료")
            
        except Exception as e:
            logger.error(f"❌ 차트 생성 실패: {e}")
        
        return charts
    
    def _create_hourly_performance_chart(self, timezone_results: Dict[str, Any]) -> str:
        """시간대별 성과 차트 생성"""
        try:
            # 데이터 추출
            hourly_data = self._extract_hourly_data(timezone_results)
            
            if not hourly_data:
                return self._create_empty_chart_placeholder("시간대별 성과 데이터 없음")
            
            # Plotly 차트 생성
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('시간대별 평균 수익률 (%)', '시간대별 거래 수'),
                specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
            )
            
            hours = list(hourly_data.keys())
            returns = [hourly_data[h].get('avg_return', 0) * 100 for h in hours]
            trade_counts = [hourly_data[h].get('trade_count', 0) for h in hours]
            
            # 수익률 차트
            fig.add_trace(
                go.Bar(
                    x=hours,
                    y=returns,
                    name='평균 수익률 (%)',
                    marker_color=[self.colors['success'] if r > 0 else self.colors['danger'] for r in returns],
                    text=[f"{r:.2f}%" for r in returns],
                    textposition='outside'
                ),
                row=1, col=1
            )
            
            # 거래량 차트
            fig.add_trace(
                go.Scatter(
                    x=hours,
                    y=trade_counts,
                    mode='lines+markers',
                    name='거래 수',
                    line=dict(color=self.colors['primary'], width=2),
                    marker=dict(size=8)
                ),
                row=2, col=1
            )
            
            fig.update_layout(
                title="시간대별 거래 성과 분석",
                height=600,
                showlegend=False
            )
            
            # HTML로 변환
            return fig.to_html(include_plotlyjs='cdn')
            
        except Exception as e:
            logger.error(f"❌ 시간대별 성과 차트 생성 실패: {e}")
            return self._create_empty_chart_placeholder("시간대별 성과 차트 오류")
    
    def _create_regional_performance_chart(self, timezone_results: Dict[str, Any]) -> str:
        """지역별 성과 차트 생성"""
        try:
            # 지역별 데이터 추출
            regional_data = self._extract_regional_data(timezone_results)
            
            if not regional_data:
                return self._create_empty_chart_placeholder("지역별 성과 데이터 없음")
            
            regions = list(regional_data.keys())
            returns = [regional_data[r].get('avg_return', 0) * 100 for r in regions]
            trade_counts = [regional_data[r].get('trade_count', 0) for r in regions]
            win_rates = [regional_data[r].get('win_rate', 0) * 100 for r in regions]
            
            # 색상 매핑
            colors_mapped = []
            for region in regions:
                if 'Asia' in region:
                    colors_mapped.append(self.colors['asia'])
                elif 'Europe' in region:
                    colors_mapped.append(self.colors['europe'])
                elif 'America' in region:
                    colors_mapped.append(self.colors['america'])
                else:
                    colors_mapped.append(self.colors['primary'])
            
            # 도넛 차트와 바 차트 결합
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=('지역별 거래량 비중', '지역별 평균 수익률'),
                specs=[[{"type": "domain"}, {"type": "xy"}]]
            )
            
            # 도넛 차트 (거래량 비중)
            fig.add_trace(
                go.Pie(
                    labels=regions,
                    values=trade_counts,
                    hole=0.4,
                    marker=dict(colors=colors_mapped),
                    textinfo='label+percent'
                ),
                row=1, col=1
            )
            
            # 바 차트 (수익률)
            fig.add_trace(
                go.Bar(
                    x=regions,
                    y=returns,
                    marker_color=colors_mapped,
                    text=[f"{r:.2f}%" for r in returns],
                    textposition='outside',
                    name='평균 수익률 (%)'
                ),
                row=1, col=2
            )
            
            fig.update_layout(
                title="지역별 거래 성과 분석",
                height=500,
                showlegend=False
            )
            
            return fig.to_html(include_plotlyjs='cdn')
            
        except Exception as e:
            logger.error(f"❌ 지역별 성과 차트 생성 실패: {e}")
            return self._create_empty_chart_placeholder("지역별 성과 차트 오류")
    
    def _create_activity_correlation_chart(self, timezone_results: Dict[str, Any]) -> str:
        """활성도-수익률 상관관계 차트 생성"""
        try:
            # 상관관계 데이터 추출
            correlation_data = self._extract_correlation_data(timezone_results)
            
            if not correlation_data:
                return self._create_empty_chart_placeholder("상관관계 데이터 없음")
            
            activity_ranges = list(correlation_data.keys())
            avg_returns = [correlation_data[r].get('avg_return', 0) * 100 for r in activity_ranges]
            trade_counts = [correlation_data[r].get('trade_count', 0) for r in activity_ranges]
            
            fig = go.Figure()
            
            # 산점도로 상관관계 표시
            fig.add_trace(
                go.Scatter(
                    x=[r.split('-')[0] for r in activity_ranges],  # 활성도 범위 시작점
                    y=avg_returns,
                    mode='markers+lines',
                    marker=dict(
                        size=[tc/2 for tc in trade_counts],  # 거래량에 비례한 크기
                        color=avg_returns,
                        colorscale='RdYlGn',
                        showscale=True,
                        colorbar=dict(title="수익률 (%)")
                    ),
                    text=[f"구간: {r}<br>수익률: {ret:.2f}%<br>거래수: {tc}" 
                          for r, ret, tc in zip(activity_ranges, avg_returns, trade_counts)],
                    hovertemplate='%{text}<extra></extra>',
                    name='활성도별 성과'
                )
            )
            
            fig.update_layout(
                title="글로벌 활성도와 수익률 상관관계",
                xaxis_title="글로벌 활성도 점수",
                yaxis_title="평균 수익률 (%)",
                height=400
            )
            
            return fig.to_html(include_plotlyjs='cdn')
            
        except Exception as e:
            logger.error(f"❌ 상관관계 차트 생성 실패: {e}")
            return self._create_empty_chart_placeholder("상관관계 차트 오류")
    
    def _create_strategy_timezone_heatmap(self, timezone_results: Dict[str, Any]) -> str:
        """전략별 시간대 히트맵 생성"""
        try:
            # 전략별 시간대 데이터 추출
            heatmap_data = self._extract_strategy_heatmap_data(timezone_results)
            
            if not heatmap_data:
                return self._create_empty_chart_placeholder("히트맵 데이터 없음")
            
            # 데이터프레임으로 변환
            df = pd.DataFrame(heatmap_data).fillna(0)
            
            fig = go.Figure(data=go.Heatmap(
                z=df.values,
                x=[f"{h:02d}:00" for h in df.columns],
                y=df.index,
                colorscale='RdYlGn',
                hoverongaps=False,
                colorbar=dict(title="수익률 (%)"),
                text=df.values,
                texttemplate="%{text:.2f}%",
                textfont={"size": 10}
            ))
            
            fig.update_layout(
                title="전략별 시간대 성과 히트맵",
                xaxis_title="시간대 (KST)",
                yaxis_title="전략명",
                height=400
            )
            
            return fig.to_html(include_plotlyjs='cdn')
            
        except Exception as e:
            logger.error(f"❌ 히트맵 생성 실패: {e}")
            return self._create_empty_chart_placeholder("히트맵 차트 오류")
    
    def _create_global_activity_timeline(self, timezone_results: Dict[str, Any]) -> str:
        """글로벌 활성도 타임라인 차트 생성"""
        try:
            # 24시간 글로벌 활성도 시뮬레이션
            from timezone_market_analyzer import TimezoneMarketAnalyzer
            
            analyzer = TimezoneMarketAnalyzer()
            timeline_data = []
            
            for hour in range(24):
                analysis = analyzer.generate_comprehensive_analysis(hour)
                timeline_data.append({
                    'hour': hour,
                    'activity_score': analysis['global_activity_score'],
                    'market_condition': analysis['market_condition'],
                    'dominant_region': analysis['trading_style']['dominant_region']
                })
            
            hours = [d['hour'] for d in timeline_data]
            scores = [d['activity_score'] for d in timeline_data]
            conditions = [d['market_condition'] for d in timeline_data]
            regions = [d['dominant_region'] for d in timeline_data]
            
            # 색상 매핑 (시장 상태별)
            condition_colors = {
                'VERY_ACTIVE': self.colors['success'],
                'ACTIVE': self.colors['performance'],
                'MODERATE': self.colors['warning'],
                'QUIET': self.colors['info'],
                'VERY_QUIET': self.colors['danger']
            }
            
            colors = [condition_colors.get(c, self.colors['primary']) for c in conditions]
            
            fig = go.Figure()
            
            # 글로벌 활성도 라인
            fig.add_trace(
                go.Scatter(
                    x=hours,
                    y=scores,
                    mode='lines+markers',
                    name='글로벌 활성도',
                    line=dict(width=3),
                    marker=dict(
                        size=10,
                        color=colors,
                        line=dict(width=2, color='white')
                    ),
                    text=[f"시간: {h:02d}:00<br>활성도: {s:.1f}%<br>상태: {c}<br>주도지역: {r}"
                          for h, s, c, r in zip(hours, scores, conditions, regions)],
                    hovertemplate='%{text}<extra></extra>'
                )
            )
            
            # 시장 상태별 배경 영역 표시
            for condition, color in condition_colors.items():
                condition_hours = [h for h, c in zip(hours, conditions) if c == condition]
                if condition_hours:
                    fig.add_hrect(
                        y0=min(scores), y1=max(scores),
                        x0=min(condition_hours)-0.5, x1=max(condition_hours)+0.5,
                        fillcolor=color, opacity=0.1,
                        layer="below", line_width=0
                    )
            
            fig.update_layout(
                title="24시간 글로벌 시장 활성도 타임라인",
                xaxis_title="시간 (KST)",
                yaxis_title="글로벌 활성도 점수 (%)",
                xaxis=dict(tickmode='array', tickvals=list(range(0, 24, 2)), 
                          ticktext=[f"{h:02d}:00" for h in range(0, 24, 2)]),
                height=400
            )
            
            return fig.to_html(include_plotlyjs='cdn')
            
        except Exception as e:
            logger.error(f"❌ 글로벌 활성도 타임라인 생성 실패: {e}")
            return self._create_empty_chart_placeholder("타임라인 차트 오류")
    
    def _generate_html_report(self, timezone_results: Dict[str, Any], 
                            charts: Dict[str, str], filename: str) -> str:
        """HTML 리포트 생성"""
        try:
            # 기본 통계 추출
            stats = self._extract_summary_stats(timezone_results)
            recommendations = self._generate_recommendations(timezone_results)
            
            html_template = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>시간대별 백테스트 리포트 - {filename}</title>
    <style>
        body {{
            font-family: 'Arial', 'Helvetica', sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f8f9fa;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
        }}
        .header {{
            text-align: center;
            margin-bottom: 40px;
            padding-bottom: 20px;
            border-bottom: 2px solid #e9ecef;
        }}
        .header h1 {{
            color: #2c3e50;
            margin-bottom: 10px;
        }}
        .header p {{
            color: #6c757d;
            font-size: 18px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }}
        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .stat-label {{
            font-size: 0.9em;
            opacity: 0.9;
        }}
        .chart-section {{
            margin-bottom: 50px;
        }}
        .chart-title {{
            font-size: 1.5em;
            color: #2c3e50;
            margin-bottom: 20px;
            padding-left: 10px;
            border-left: 4px solid #3498db;
        }}
        .recommendations {{
            background: #e8f5e8;
            padding: 20px;
            border-radius: 10px;
            border-left: 5px solid #28a745;
        }}
        .recommendations h3 {{
            color: #155724;
            margin-top: 0;
        }}
        .recommendation-item {{
            margin-bottom: 10px;
            padding: 10px;
            background: white;
            border-radius: 5px;
            border-left: 3px solid #28a745;
        }}
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #e9ecef;
            color: #6c757d;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌏 시간대별 백테스트 분석 리포트</h1>
            <p>{datetime.now().strftime('%Y년 %m월 %d일 %H:%M:%S')} 생성</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{stats.get('total_strategies', 0)}</div>
                <div class="stat-label">분석 전략 수</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{stats.get('total_trades', 0)}</div>
                <div class="stat-label">총 거래 수</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{stats.get('avg_return', 0):.2f}%</div>
                <div class="stat-label">평균 수익률</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{stats.get('best_hour', 'N/A')}</div>
                <div class="stat-label">최고 성과 시간대</div>
            </div>
        </div>
        
        <div class="chart-section">
            <div class="chart-title">📈 시간대별 성과 분석</div>
            {charts.get('hourly_performance', '차트 생성 오류')}
        </div>
        
        <div class="chart-section">
            <div class="chart-title">🌍 지역별 성과 분석</div>
            {charts.get('regional_performance', '차트 생성 오류')}
        </div>
        
        <div class="chart-section">
            <div class="chart-title">📊 활성도-수익률 상관관계</div>
            {charts.get('activity_correlation', '차트 생성 오류')}
        </div>
        
        <div class="chart-section">
            <div class="chart-title">🔥 전략별 시간대 히트맵</div>
            {charts.get('strategy_heatmap', '차트 생성 오류')}
        </div>
        
        <div class="chart-section">
            <div class="chart-title">⏰ 글로벌 활성도 타임라인</div>
            {charts.get('global_activity', '차트 생성 오류')}
        </div>
        
        <div class="recommendations">
            <h3>🎯 최적화 권장사항</h3>
            {self._format_recommendations_html(recommendations)}
        </div>
        
        <div class="footer">
            <p>Makenaide 시간대별 백테스팅 시스템 | 생성 일시: {datetime.now().isoformat()}</p>
        </div>
    </div>
</body>
</html>
            """
            
            return html_template
            
        except Exception as e:
            logger.error(f"❌ HTML 리포트 생성 실패: {e}")
            return f"<html><body><h1>리포트 생성 오류</h1><p>{str(e)}</p></body></html>"
    
    def _generate_text_summary(self, timezone_results: Dict[str, Any]) -> str:
        """텍스트 요약 리포트 생성"""
        try:
            stats = self._extract_summary_stats(timezone_results)
            recommendations = self._generate_recommendations(timezone_results)
            
            report = f"""
🌏 시간대별 백테스트 분석 리포트
{'=' * 60}

📊 전체 통계
- 분석 전략 수: {stats.get('total_strategies', 0)}개
- 총 거래 수: {stats.get('total_trades', 0)}개
- 평균 수익률: {stats.get('avg_return', 0):.2f}%
- 평균 승률: {stats.get('avg_win_rate', 0):.2f}%
- 최고 성과 시간대: {stats.get('best_hour', 'N/A')}
- 최고 성과 지역: {stats.get('best_region', 'N/A')}

🕐 시간대별 인사이트
{self._format_hourly_insights(timezone_results)}

🌍 지역별 인사이트  
{self._format_regional_insights(timezone_results)}

🎯 최적화 권장사항
{chr(10).join(['• ' + rec for rec in recommendations])}

📈 결론
{self._generate_conclusion(timezone_results)}

---
생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Makenaide 시간대별 백테스팅 시스템
            """
            
            return report.strip()
            
        except Exception as e:
            logger.error(f"❌ 텍스트 요약 생성 실패: {e}")
            return f"텍스트 리포트 생성 중 오류 발생: {str(e)}"
    
    # 헬퍼 메서드들
    def _extract_hourly_data(self, timezone_results: Dict[str, Any]) -> Dict:
        """시간대별 데이터 추출"""
        hourly_data = {}
        
        try:
            for strategy_name, result in timezone_results.get('timezone_results', {}).items():
                if 'timezone_performance' in result:
                    hourly_perf = result['timezone_performance'].get('hourly_performance', {})
                    for hour, metrics in hourly_perf.items():
                        if hour not in hourly_data:
                            hourly_data[hour] = {'avg_return': 0, 'trade_count': 0, 'strategies': 0}
                        
                        hourly_data[hour]['avg_return'] += metrics.get('avg_return', 0)
                        hourly_data[hour]['trade_count'] += metrics.get('trade_count', 0)
                        hourly_data[hour]['strategies'] += 1
            
            # 평균화
            for hour in hourly_data:
                if hourly_data[hour]['strategies'] > 0:
                    hourly_data[hour]['avg_return'] /= hourly_data[hour]['strategies']
                    
        except Exception as e:
            logger.error(f"❌ 시간대별 데이터 추출 실패: {e}")
        
        return hourly_data
    
    def _extract_regional_data(self, timezone_results: Dict[str, Any]) -> Dict:
        """지역별 데이터 추출"""
        regional_data = {}
        
        try:
            for strategy_name, result in timezone_results.get('timezone_results', {}).items():
                if 'timezone_performance' in result:
                    region_perf = result['timezone_performance'].get('region_performance', {})
                    for region, metrics in region_perf.items():
                        if region not in regional_data:
                            regional_data[region] = {'avg_return': 0, 'trade_count': 0, 'strategies': 0}
                        
                        regional_data[region]['avg_return'] += metrics.get('avg_return', 0)
                        regional_data[region]['trade_count'] += metrics.get('trade_count', 0)
                        regional_data[region]['strategies'] += 1
            
            # 평균화
            for region in regional_data:
                if regional_data[region]['strategies'] > 0:
                    regional_data[region]['avg_return'] /= regional_data[region]['strategies']
                    
        except Exception as e:
            logger.error(f"❌ 지역별 데이터 추출 실패: {e}")
        
        return regional_data
    
    def _extract_correlation_data(self, timezone_results: Dict[str, Any]) -> Dict:
        """상관관계 데이터 추출"""
        correlation_data = {}
        
        try:
            for strategy_name, result in timezone_results.get('timezone_results', {}).items():
                if 'activity_correlation' in result:
                    activity_analysis = result['activity_correlation'].get('activity_analysis', {})
                    for range_key, metrics in activity_analysis.items():
                        if range_key not in correlation_data:
                            correlation_data[range_key] = {'avg_return': 0, 'trade_count': 0, 'strategies': 0}
                        
                        correlation_data[range_key]['avg_return'] += metrics.get('avg_return', 0)
                        correlation_data[range_key]['trade_count'] += metrics.get('trade_count', 0)
                        correlation_data[range_key]['strategies'] += 1
            
            # 평균화
            for range_key in correlation_data:
                if correlation_data[range_key]['strategies'] > 0:
                    correlation_data[range_key]['avg_return'] /= correlation_data[range_key]['strategies']
                    
        except Exception as e:
            logger.error(f"❌ 상관관계 데이터 추출 실패: {e}")
        
        return correlation_data
    
    def _extract_strategy_heatmap_data(self, timezone_results: Dict[str, Any]) -> Dict:
        """전략별 히트맵 데이터 추출"""
        heatmap_data = {}
        
        try:
            for strategy_name, result in timezone_results.get('timezone_results', {}).items():
                if 'timezone_performance' in result:
                    hourly_perf = result['timezone_performance'].get('hourly_performance', {})
                    heatmap_data[strategy_name] = {}
                    
                    for hour in range(24):
                        if hour in hourly_perf:
                            heatmap_data[strategy_name][hour] = hourly_perf[hour].get('avg_return', 0) * 100
                        else:
                            heatmap_data[strategy_name][hour] = 0
                            
        except Exception as e:
            logger.error(f"❌ 히트맵 데이터 추출 실패: {e}")
        
        return heatmap_data
    
    def _extract_summary_stats(self, timezone_results: Dict[str, Any]) -> Dict:
        """요약 통계 추출"""
        stats = {
            'total_strategies': len(timezone_results.get('timezone_results', {})),
            'total_trades': 0,
            'avg_return': 0,
            'avg_win_rate': 0,
            'best_hour': 'N/A',
            'best_region': 'N/A'
        }
        
        try:
            all_returns = []
            all_trades = 0
            
            for strategy_name, result in timezone_results.get('timezone_results', {}).items():
                trades = result.get('trades', [])
                all_trades += len(trades)
                
                if trades:
                    strategy_returns = [t.get('return_pct', 0) for t in trades]
                    all_returns.extend(strategy_returns)
            
            stats['total_trades'] = all_trades
            if all_returns:
                stats['avg_return'] = np.mean(all_returns) * 100
                win_trades = [r for r in all_returns if r > 0]
                stats['avg_win_rate'] = len(win_trades) / len(all_returns) * 100
            
            # 최고 성과 시간대/지역 찾기
            global_insights = timezone_results.get('global_insights', {})
            best_hours = global_insights.get('best_global_hours', [])
            if best_hours:
                stats['best_hour'] = best_hours[0].get('hour', 'N/A')
            
            best_regions = global_insights.get('best_regions', [])
            if best_regions:
                stats['best_region'] = best_regions[0].get('region', 'N/A')
                
        except Exception as e:
            logger.error(f"❌ 요약 통계 추출 실패: {e}")
        
        return stats
    
    def _generate_recommendations(self, timezone_results: Dict[str, Any]) -> List[str]:
        """권장사항 생성"""
        recommendations = timezone_results.get('recommendations', [])
        
        # 기본 권장사항 추가
        if not recommendations:
            recommendations = [
                "시간대별 분석 결과를 활용하여 거래 시간대를 최적화하세요",
                "높은 성과를 보인 지역의 거래 시간에 집중하세요",
                "글로벌 활성도가 높은 시간대에 포지션 크기를 증대하세요"
            ]
        
        return recommendations[:5]  # 최대 5개만 반환
    
    def _format_recommendations_html(self, recommendations: List[str]) -> str:
        """권장사항을 HTML 형식으로 포맷팅"""
        html = ""
        for i, rec in enumerate(recommendations, 1):
            html += f'<div class="recommendation-item">{i}. {rec}</div>'
        return html
    
    def _format_hourly_insights(self, timezone_results: Dict[str, Any]) -> str:
        """시간대별 인사이트 포맷팅"""
        insights = []
        
        global_insights = timezone_results.get('global_insights', {})
        best_hours = global_insights.get('best_global_hours', [])
        
        for hour_info in best_hours[:3]:
            hour = hour_info.get('hour', 'N/A')
            ret = hour_info.get('avg_return', 0)
            insights.append(f"- {hour}: 평균 수익률 {ret:.3%}")
        
        return '\n'.join(insights) if insights else "- 시간대별 데이터가 없습니다"
    
    def _format_regional_insights(self, timezone_results: Dict[str, Any]) -> str:
        """지역별 인사이트 포맷팅"""
        insights = []
        
        global_insights = timezone_results.get('global_insights', {})
        best_regions = global_insights.get('best_regions', [])
        
        for region_info in best_regions[:3]:
            region = region_info.get('region', 'N/A')
            ret = region_info.get('avg_return', 0)
            insights.append(f"- {region}: 평균 수익률 {ret:.3%}")
        
        return '\n'.join(insights) if insights else "- 지역별 데이터가 없습니다"
    
    def _generate_conclusion(self, timezone_results: Dict[str, Any]) -> str:
        """결론 생성"""
        strategy_count = len(timezone_results.get('timezone_results', {}))
        
        if strategy_count == 0:
            return "분석할 전략 데이터가 없습니다."
        
        return f"""
시간대별 백테스팅 분석을 통해 {strategy_count}개 전략의 글로벌 시장 적응성을 평가했습니다.
시간대별 거래 패턴과 지역별 시장 특성을 고려한 최적화된 거래 전략을 수립하여
더 나은 투자 성과를 달성할 수 있을 것으로 예상됩니다.
        """.strip()
    
    def _create_empty_chart_placeholder(self, message: str) -> str:
        """빈 차트 플레이스홀더 생성"""
        return f"""
        <div style="
            height: 400px; 
            display: flex; 
            justify-content: center; 
            align-items: center; 
            background: #f8f9fa; 
            border: 2px dashed #dee2e6; 
            border-radius: 10px;
            color: #6c757d;
            font-size: 18px;
        ">
            📊 {message}
        </div>
        """


def main():
    """메인 실행 함수 (테스트용)"""
    print("🎨 시간대별 백테스트 리포트 생성기 테스트")
    
    # 샘플 데이터로 테스트
    sample_data = {
        'timezone_results': {
            'Test_Strategy': {
                'trades': [
                    {'return_pct': 0.05, 'entry_hour': 9},
                    {'return_pct': -0.02, 'entry_hour': 15},
                    {'return_pct': 0.03, 'entry_hour': 21}
                ],
                'timezone_performance': {
                    'hourly_performance': {
                        9: {'avg_return': 0.05, 'trade_count': 1},
                        15: {'avg_return': -0.02, 'trade_count': 1},
                        21: {'avg_return': 0.03, 'trade_count': 1}
                    }
                }
            }
        },
        'global_insights': {
            'best_global_hours': [
                {'hour': '09:00', 'avg_return': 0.05},
                {'hour': '21:00', 'avg_return': 0.03}
            ]
        },
        'recommendations': [
            "아시아 시간대 집중 거래 권장",
            "높은 활성도 시간대 포지션 확대"
        ]
    }
    
    generator = TimezoneReportGenerator()
    report_path = generator.generate_comprehensive_report(sample_data, "test_report")
    
    if report_path:
        print(f"✅ 테스트 리포트 생성 완료: {report_path}")
    else:
        print("❌ 테스트 리포트 생성 실패")


if __name__ == "__main__":
    main()