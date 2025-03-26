# -*- coding: utf-8 -*-
"""
招聘数据分析系统 - Web应用

该模块提供Web界面，用于展示招聘数据分析结果，
并提供交互式查询和可视化功能。
"""

import os
import json
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate

# 导入自定义模块
from data_crawler import JobCrawler
from data_cleaner import DataCleaner
from data_analyzer import JobAnalyzer
from llm_analyzer import LLMAnalyzer

# 配置目录
DATA_DIR = "./data"
ANALYSIS_DIR = "./analysis"

# 确保目录存在
for directory in [DATA_DIR, ANALYSIS_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# 初始化Dash应用
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    title="上海中侨大学招聘数据分析系统"
)

# 加载分析数据
def load_analysis_data():
    """
    加载最新的分析数据
    """
    try:
        latest_path = os.path.join(ANALYSIS_DIR, "analysis_latest.json")
        if os.path.exists(latest_path):
            with open(latest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    except Exception as e:
        print(f"加载分析数据出错: {str(e)}")
        return None

# 创建页面布局
def create_layout():
    """
    创建应用布局
    """
    return dbc.Container(
        [
            # 页面标题
            dbc.Row(
                dbc.Col(
                    html.H1("上海中侨大学数字化分析大屏", className="text-center my-4"),
                    width=12
                ),
                className="mb-4"
            ),
            
            # 基本统计信息卡片
            dbc.Row(id="stats-cards", className="mb-4"),
            
            # 薪资分析部分
            dbc.Row(
                [
                    # 左侧：职位薪资对比
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(html.H5("职位薪资对比")),
                                dbc.CardBody(dcc.Graph(id="position-salary-chart"))
                            ]
                        ),
                        md=6
                    ),
                    
                    # 右侧：城市薪资对比
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(html.H5("城市薪资对比")),
                                dbc.CardBody(dcc.Graph(id="city-salary-chart"))
                            ]
                        ),
                        md=6
                    )
                ],
                className="mb-4"
            ),
            
            # 技能分析部分
            dbc.Row(
                [
                    # 左侧：热门技能词云
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(html.H5("热门技能需求")),
                                dbc.CardBody(dcc.Graph(id="skills-chart"))
                            ]
                        ),
                        md=6
                    ),
                    
                    # 右侧：技能薪资关系
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(html.H5("技能与薪资关系")),
                                dbc.CardBody(dcc.Graph(id="skill-salary-chart"))
                            ]
                        ),
                        md=6
                    )
                ],
                className="mb-4"
            ),
            
            # 和学历分析
            dbc.Row(
                [
                    # 左侧：经验要求分布
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(html.H5("经验要求分布")),
                                dbc.CardBody(dcc.Graph(id="experience-chart"))
                            ]
                        ),
                        md=6
                    ),
                    
                    # 右侧：学历要求分布
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(html.H5("学历要求分布")),
                                dbc.CardBody(dcc.Graph(id="education-chart"))
                            ]
                        ),
                        md=6
                    )
                ],
                className="mb-4"
            ),
            
            # 大模型分析结果
            dbc.Row(
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(html.H5("大模型分析洞察")),
                            dbc.CardBody([
                                html.Div(id="llm-analysis-content"),
                                dbc.Button("生成新的分析报告", id="generate-llm-analysis-btn", color="primary", className="mt-3")
                            ])
                        ]
                    ),
                    width=12
                ),
                className="mb-4"
            ),
            
            # 数据更新控制
            dbc.Row(
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(html.H5("数据管理")),
                            dbc.CardBody([
                                html.P(id="last-update-time"),
                                dbc.Button("更新数据", id="update-data-btn", color="success", className="me-2"),
                                dbc.Button("重新分析", id="reanalyze-btn", color="info"),
                                html.Div(id="update-status", className="mt-3")
                            ])
                        ]
                    ),
                    width=12
                ),
                className="mb-4"
            ),
            
            # 页脚
            dbc.Row(
                dbc.Col(
                    html.Footer(
                        "© " + str(datetime.now().year) + " JobFree招聘数据分析系统 | 基于Spark的就业推荐系统",
                        className="text-center text-muted py-3"
                    ),
                    width=12
                )
            ),
            
            # 存储组件
            dcc.Store(id="analysis-data"),
            dcc.Interval(id="load-interval", interval=1000, n_intervals=0, max_intervals=1)  # 初始加载数据
        ],
        fluid=True,
        className="px-4 py-3"
    )

# 设置应用布局
app.layout = create_layout()

# 回调函数：初始加载数据
@app.callback(
    Output("analysis-data", "data"),
    Input("load-interval", "n_intervals")
)
def load_data(n):
    """
    初始加载分析数据
    """
    return load_analysis_data()

# 回调函数：更新基本统计卡片
@app.callback(
    Output("stats-cards", "children"),
    Input("analysis-data", "data")
)
def update_stats_cards(data):
    """
    更新基本统计信息卡片
    """
    if not data or "basic_stats" not in data:
        return []
    
    stats = data["basic_stats"]
    
    cards = [
        dbc.Col(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H3(f"{stats.get('total_jobs', 0):,}", className="card-title text-center"),
                            html.P("总职位数", className="card-text text-center")
                        ]
                    )
                ],
                className="text-white bg-primary"
            ),
            width=12, sm=6, md=3
        ),
        dbc.Col(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H3(f"{int(stats.get('avg_salary', 0)):,}", className="card-title text-center"),
                            html.P("平均月薪(元)", className="card-text text-center")
                        ]
                    )
                ],
                className="text-white bg-success"
            ),
            width=12, sm=6, md=3
        ),
        dbc.Col(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H3(f"{stats.get('unique_positions', 0)}", className="card-title text-center"),
                            html.P("职位类型数", className="card-text text-center")
                        ]
                    )
                ],
                className="text-white bg-info"
            ),
            width=12, sm=6, md=3
        ),
        dbc.Col(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H3(f"{stats.get('unique_cities', 0)}", className="card-title text-center"),
                            html.P("城市数量", className="card-text text-center")
                        ]
                    )
                ],
                className="text-white bg-warning"
            ),
            width=12, sm=6, md=3
        )
    ]
    
    return cards

# 回调函数：职位薪资图表
@app.callback(
    Output("position-salary-chart", "figure"),
    Input("analysis-data", "data")
)
def update_position_salary_chart(data):
    """
    更新职位薪资对比图表
    """
    if not data or "salary_analysis" not in data or "by_position" not in data["salary_analysis"]:
        return go.Figure()
    
    position_salary = data["salary_analysis"]["by_position"]
    
    # 提取前10个职位的薪资数据
    positions = list(position_salary.keys())[:10]
    mean_salaries = [position_salary[pos]["mean"] for pos in positions]
    median_salaries = [position_salary[pos]["median"] for pos in positions]
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=positions,
        y=mean_salaries,
        name="平均薪资",
        marker_color="#1f77b4"
    ))
    fig.add_trace(go.Bar(
        x=positions,
        y=median_salaries,
        name="中位薪资",
        marker_color="#ff7f0e"
    ))
    
    fig.update_layout(
        title="热门职位薪资对比",
        xaxis_title="职位",
        yaxis_title="薪资（元/月）",
        barmode="group",
        legend=dict(x=0, y=1.1, orientation="h"),
        margin=dict(l=50, r=50, t=80, b=50),
        height=400
    )
    
    return fig

# 回调函数：城市薪资图表
@app.callback(
    Output("city-salary-chart", "figure"),
    Input("analysis-data", "data")
)
def update_city_salary_chart(data):
    """
    更新城市薪资对比图表
    """
    if not data or "salary_analysis" not in data or "by_city" not in data["salary_analysis"]:
        return go.Figure()
    
    city_salary = data["salary_analysis"]["by_city"]
    
    # 提取前10个城市的薪资数据
    cities = list(city_salary.keys())[:10]
    mean_salaries = [city_salary[city]["mean"] for city in cities]
    job_counts = [city_salary[city]["count"] for city in cities]
    
    # 创建带有气泡大小的散点图
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=cities,
        y=mean_salaries,
        marker=dict(
            color="#2ca02c",
            opacity=0.8
        ),
        text=[f"职位数: {count}" for count in job_counts],
        hovertemplate="%{x}<br>平均薪资: %{y:,.0f}元/月<br>%{text}<extra></extra>"
    ))
    
    fig.update_layout(
        title="主要城市薪资水平",
        xaxis_title="城市",
        yaxis_title="平均薪资（元/月）",
        margin=dict(l=50, r=50, t=80, b=50),
        height=400
    )
    
    return fig

# 回调函数：技能图表
@app.callback(
    Output("skills-chart", "figure"),
    Input("analysis-data", "data")
)
def update_skills_chart(data):
    """
    更新技能需求图表
    """
    if not data or "skill_analysis" not in data or "top_skills" not in data["skill_analysis"]:
        return go.Figure()
    
    top_skills = data["skill_analysis"]["top_skills"]
    
    # 提取前20个技能
    skills = list(top_skills.keys())[:20]
    counts = [top_skills[skill] for skill in skills]
    
    # 创建水平条形图
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=skills,
        x=counts,
        orientation='h',
        marker_color="#636efa"
    ))
    
    fig.update_layout(
        title="热门技能需求",
        xaxis_title="需求数量",
        yaxis_title="技能",
        margin=dict(l=150, r=50, t=80, b=50),
        height=500
    )
    
    return fig

# 回调函数：技能薪资关系图表
@app.callback(
    Output("skill-salary-chart", "figure"),
    Input("analysis-data", "data")
)
def update_skill_salary_chart(data):
    """
    更新技能与薪资关系图表
    """
    if not data or "skill_analysis" not in data or "skill_salary" not in data["skill_analysis"]:
        return go.Figure()
    
    skill_salary = data["skill_analysis"]["skill_salary"]
    
    # 提取前15个技能的薪资数据
    skills = list(skill_salary.keys())[:15]
    mean_salaries = [skill_salary[skill]["mean"] for skill in skills]
    counts = [skill_salary[skill]["count"] for skill in skills]
    
    # 创建气泡图，气泡大小表示需求量
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=skills,
        y=mean_salaries,
        mode='markers',
        marker=dict(
            size=[count/5 for count in counts],  # 调整气泡大小
            sizemin=10,
            sizemode='area',
            sizeref=2.*max(counts)/(40.**2),
            color=mean_salaries,
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="平均薪资")
        ),
        text=[f"需求量: {count}" for count in counts],
        hovertemplate="%{x}<br>平均薪资: %{y:,.0f}元/月<br>%{text}<extra></extra>"
    ))
    
    fig.update_layout(
        title="技能与薪资关系",
        xaxis_title="技能",
        yaxis_title="平均薪资（元/月）",
        margin=dict(l=50, r=50, t=80, b=100),
        height=400,
        xaxis=dict(tickangle=45)
    )
    
    return fig

# 回调函数：经验要求图表
@app.callback(
    Output("experience-chart", "figure"),
    Input("analysis-data", "data")
)
def update_experience_chart(data):
    """
    更新经验要求分布图表
    """
    if not data or "experience_analysis" not in data or "experience_distribution" not in data["experience_analysis"]:
        return go.Figure()
    
    exp_distribution = data["experience_analysis"]["experience_distribution"]
    
    # 提取经验分布数据
    experiences = list(exp_distribution.keys())
    counts = list(exp_distribution.values())
    
    # 创建饼图
    fig = go.Figure()
    fig.add_trace(go.Pie(
        labels=experiences,
        values=counts,
        hole=0.4,
        marker=dict(colors=px.colors.qualitative.Pastel)
    ))
    
    fig.update_layout(
        title="工作经验要求分布",
        margin=dict(l=50, r=50, t=80, b=50),
        height=400,
        legend=dict(orientation="h", y=-0.1)
    )
    
    return fig

# 回调函数：学历要求图表
@app.callback(
    Output("education-chart", "figure"),
    Input("analysis-data", "data")
)
def update_education_chart(data):
    """
    更新学历要求分布图表
    """
    if not data or "education_analysis" not in data or "education_distribution" not in data["education_analysis"]:
        return go.Figure()
    
    edu_distribution = data["education_analysis"]["education_distribution"]
    
    # 提取学历分布数据
    educations = list(edu_distribution.keys())
    counts = list(edu_distribution.values())
    
    # 创建饼图
    fig = go.Figure()
    fig.add_trace(go.Pie(
        labels=educations,
        values=counts,
        marker=dict(colors=px.colors.qualitative.Set3)
    ))
    
    fig.update_layout(
        title="学历要求分布",
        margin=dict(l=50, r=50, t=80, b=50),
        height=400
    )
    
    return fig

# 回调函数：大模型分析内容
@app.callback(
    Output("llm-analysis-content", "children"),
    Input("analysis-data", "data")
)
def update_llm_analysis(data):
    """
    更新大模型分析内容
    """
    if not data or "llm_analysis" not in data:
        return html.P("暂无大模型分析结果，请点击下方按钮生成分析报告。")
    
    llm_analysis = data["llm_analysis"]
    
    # 将大模型分析结果转换为HTML格式
    analysis_content = []
    
    if "summary" in llm_analysis:
        analysis_content.append(html.H6("总体分析"))
        analysis_content.append(html.P(llm_analysis["summary"]))
    
    if "insights" in llm_analysis:
        analysis_content.append(html.H6("关键洞察", className="mt-3"))
        insights = [html.Li(insight) for insight in llm_analysis["insights"]]
        analysis_content.append(html.Ul(insights))
    
    if "recommendations" in llm_analysis:
        analysis_content.append(html.H6("就业建议", className="mt-3"))
        recommendations = [html.Li(rec) for rec in llm_analysis["recommendations"]]
        analysis_content.append(html.Ul(recommendations))
    
    return analysis_content

# 回调函数：生成大模型分析
@app.callback(
    Output("update-status", "children", allow_duplicate=True),
    Input("generate-llm-analysis-btn", "n_clicks"),
    State("analysis-data", "data"),
    prevent_initial_call=True
)
def generate_llm_analysis(n_clicks, data):
    """
    生成大模型分析报告
    """
    if not n_clicks:
        raise PreventUpdate
    
    if not data:
        return html.Div("无法生成分析报告：缺少分析数据", className="text-danger")
    
    try:
        # 这里应该调用大模型API进行分析
        # 由于实际实现需要集成特定的大模型API，这里只提供基本框架
        llm_analyzer = LLMAnalyzer()
        llm_analysis = llm_analyzer.analyze(data)
        
        # 将大模型分析结果添加到数据中并保存
        data["llm_analysis"] = llm_analysis
        
        # 保存更新后的分析结果
        latest_path = os.path.join(ANALYSIS_DIR, "analysis_latest.json")
        with open(latest_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return html.Div("大模型分析报告生成成功！", className="text-success")
    except Exception as e:
        return html.Div(f"生成分析报告出错: {str(e)}", className="text-danger")

# 回调函数：显示最后更新时间
@app.callback(
    Output("last-update-time", "children"),
    Input("analysis-data", "data")
)
def update_last_update_time(data):
    """
    显示数据最后更新时间
    """
    if not data or "timestamp" not in data:
        return "最后更新时间: 未知"
    
    return f"最后更新时间: {data['timestamp']}"

# 回调函数：更新数据
@app.callback(
    Output("update-status", "children"),
    Input("update-data-btn", "n_clicks"),
    prevent_initial_call=True
)
def update_data(n_clicks):
    """
    执行数据更新流程
    """
    if not n_clicks:
        raise PreventUpdate
    
    try:
        # 1. 爬取最新数据
        crawler = JobCrawler(output_dir=DATA_DIR)
        jobs = crawler.run_daily()
        
        if not jobs:
            return html.Div("未爬取到任何招聘信息", className="text-warning")
        
        # 2. 清洗数据
        cleaner = DataCleaner()
        df = pd.DataFrame(jobs)
        cleaned_df = cleaner.clean_data(df)
        
        # 3. 分析数据
        analyzer = JobAnalyzer(data_dir=DATA_DIR, output_dir=ANALYSIS_DIR)
        results = analyzer.analyze(cleaned_df)
        
        return html.Div("数据更新成功！请刷新页面查看最新分析结果。", className="text-success")
    except Exception as e:
        return html.Div(f"数据更新出错: {str(e)}", className="text-danger")

# 回调函数：重新分析数据
@app.callback(
    Output("update-status", "children", allow_duplicate=True),
    Input("reanalyze-btn", "n_clicks"),
    prevent_initial_call=True
)
def reanalyze_data(n_clicks):
    """
    重新分析现有数据
    """
    if not n_clicks:
        raise PreventUpdate
    
    try:
        # 加载最新数据并重新分析
        analyzer = JobAnalyzer(data_dir=DATA_DIR, output_dir=ANALYSIS_DIR)
        df = analyzer.load_data()
        results = analyzer.analyze(df)
        
        return html.Div("数据重新分析成功！请刷新页面查看最新分析结果。", className="text-success")
    except Exception as e:
        return html.Div(f"数据重新分析出错: {str(e)}", className="text-danger")

# 启动服务器
if __name__ == "__main__":
    app.run_server(debug=True, port=8050)