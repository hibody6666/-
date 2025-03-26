# -*- coding: utf-8 -*-
"""
数据分析模块

该模块负责对清洗后的招聘数据进行分析，
包括薪资分布、技能需求、地域分布等多维度分析。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import jieba.analyse
import os
import json
from datetime import datetime
from collections import Counter
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from wordcloud import WordCloud

# 设置中文字体，解决matplotlib中文显示问题
try:
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
except:
    pass

class JobAnalyzer:
    """
    招聘数据分析类，用于分析清洗后的招聘数据
    """
    
    def __init__(self, data_dir="./data", output_dir="./analysis"):
        """
        初始化分析器
        
        参数:
            data_dir (str): 数据目录
            output_dir (str): 分析结果输出目录
        """
        self.data_dir = data_dir
        self.output_dir = output_dir
        
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def load_data(self, file_path=None):
        """
        加载数据
        
        参数:
            file_path (str, optional): 数据文件路径，如果为None则加载最新数据
            
        返回:
            pandas.DataFrame: 加载的数据
        """
        if file_path is None:
            # 加载最新数据
            file_path = os.path.join(self.data_dir, "jobs_latest.csv")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"数据文件不存在: {file_path}")
        
        df = pd.read_csv(file_path)
        return df
    
    def analyze(self, df=None):
        """
        执行全面分析
        
        参数:
            df (pandas.DataFrame, optional): 要分析的数据，如果为None则自动加载
            
        返回:
            dict: 分析结果
        """
        if df is None:
            df = self.load_data()
        
        # 执行各项分析
        results = {
            "basic_stats": self.basic_statistics(df),
            "salary_analysis": self.analyze_salary(df),
            "position_analysis": self.analyze_positions(df),
            "skill_analysis": self.analyze_skills(df),
            "location_analysis": self.analyze_locations(df),
            "experience_analysis": self.analyze_experience(df),
            "education_analysis": self.analyze_education(df),
            "company_analysis": self.analyze_companies(df),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 保存分析结果
        self._save_results(results)
        
        return results

    # 添加公共的save_results方法
    def save_results(self, results):
        """
        保存分析结果
        
        参数:
            results (dict): 分析结果
        """
        self._save_results(results)
    
    def basic_statistics(self, df):
        """
        基本统计信息
        
        参数:
            df (pandas.DataFrame): 招聘数据
            
        返回:
            dict: 基本统计信息
        """
        stats = {
            "total_jobs": len(df),
            "unique_positions": df['position_normalized'].nunique() if 'position_normalized' in df.columns else 0,
            "unique_companies": df['company'].nunique() if 'company' in df.columns else 0,
            "unique_cities": df['city'].nunique() if 'city' in df.columns else 0,
            "avg_salary": df['salary_avg'].mean() if 'salary_avg' in df.columns else 0,
            "median_salary": df['salary_avg'].median() if 'salary_avg' in df.columns else 0
        }
        
        return stats
    
    def analyze_salary(self, df):
        """
        薪资分析
        
        参数:
            df (pandas.DataFrame): 招聘数据
            
        返回:
            dict: 薪资分析结果
        """
        if 'salary_avg' not in df.columns:
            return {}
        
        # 过滤掉薪资异常值
        salary_df = df[df['salary_avg'] > 0].copy()
        
        # 基本薪资统计
        salary_stats = {
            "mean": salary_df['salary_avg'].mean(),
            "median": salary_df['salary_avg'].median(),
            "min": salary_df['salary_avg'].min(),
            "max": salary_df['salary_avg'].max(),
            "std": salary_df['salary_avg'].std(),
            "quantiles": {
                "25%": salary_df['salary_avg'].quantile(0.25),
                "50%": salary_df['salary_avg'].quantile(0.5),
                "75%": salary_df['salary_avg'].quantile(0.75),
                "90%": salary_df['salary_avg'].quantile(0.9)
            }
        }
        
        # 按职位类型的薪资分布
        if 'position_normalized' in salary_df.columns:
            position_salary = salary_df.groupby('position_normalized')['salary_avg'].agg(['mean', 'median', 'std', 'count'])
            position_salary = position_salary.sort_values('mean', ascending=False)
            
            # 转换为可序列化的格式
            position_salary_dict = {}
            for position, row in position_salary.iterrows():
                position_salary_dict[position] = {
                    "mean": row['mean'],
                    "median": row['median'],
                    "std": row['std'],
                    "count": row['count']
                }
            
            salary_stats["by_position"] = position_salary_dict
        
        # 按城市的薪资分布
        if 'city' in salary_df.columns:
            # 只分析样本量足够的城市
            city_counts = salary_df['city'].value_counts()
            major_cities = city_counts[city_counts >= 10].index.tolist()
            
            city_salary = salary_df[salary_df['city'].isin(major_cities)].groupby('city')['salary_avg'].agg(['mean', 'median', 'std', 'count'])
            city_salary = city_salary.sort_values('mean', ascending=False)
            
            # 转换为可序列化的格式
            city_salary_dict = {}
            for city, row in city_salary.iterrows():
                city_salary_dict[city] = {
                    "mean": row['mean'],
                    "median": row['median'],
                    "std": row['std'],
                    "count": row['count']
                }
            
            salary_stats["by_city"] = city_salary_dict
        
        # 按经验要求的薪资分布
        if 'exp_min' in salary_df.columns:
            # 创建经验分组
            salary_df['exp_group'] = pd.cut(
                salary_df['exp_min'], 
                bins=[-0.1, 0, 1, 3, 5, 10, 100], 
                labels=['不限', '0-1年', '1-3年', '3-5年', '5-10年', '10年以上']
            )
            
            exp_salary = salary_df.groupby('exp_group')['salary_avg'].agg(['mean', 'median', 'std', 'count'])
            
            # 转换为可序列化的格式
            exp_salary_dict = {}
            for exp, row in exp_salary.iterrows():
                exp_salary_dict[str(exp)] = {
                    "mean": row['mean'],
                    "median": row['median'],
                    "std": row['std'],
                    "count": row['count']
                }
            
            salary_stats["by_experience"] = exp_salary_dict
        
        # 按学历要求的薪资分布
        if 'education_level' in salary_df.columns:
            edu_salary = salary_df.groupby('education_level')['salary_avg'].agg(['mean', 'median', 'std', 'count'])
            edu_salary = edu_salary.sort_values('mean', ascending=False)
            
            # 转换为可序列化的格式
            edu_salary_dict = {}
            for edu, row in edu_salary.iterrows():
                edu_salary_dict[edu] = {
                    "mean": row['mean'],
                    "median": row['median'],
                    "std": row['std'],
                    "count": row['count']
                }
            
            salary_stats["by_education"] = edu_salary_dict
        
        return salary_stats
    
    def analyze_positions(self, df):
        """
        职位分析
        
        参数:
            df (pandas.DataFrame): 招聘数据
            
        返回:
            dict: 职位分析结果
        """
        if 'position_normalized' not in df.columns:
            return {}
        
        # 职位分布
        position_counts = df['position_normalized'].value_counts()
        top_positions = position_counts.head(20).to_dict()
        
        # 职位与技能的关联分析
        position_skills = {}
        if 'skills' in df.columns and df['skills'].dtype == object:
            for position in top_positions.keys():
                position_df = df[df['position_normalized'] == position]
                
                # 合并所有技能列表并计数
                all_skills = []
                for skills_list in position_df['skills']:
                    if isinstance(skills_list, list):
                        all_skills.extend(skills_list)
                    elif isinstance(skills_list, str):
                        # 如果技能是字符串形式（可能是JSON字符串），尝试解析
                        try:
                            skills = json.loads(skills_list.replace("'", "\""))
                            all_skills.extend(skills)
                        except:
                            pass
                
                skill_counts = Counter(all_skills)
                position_skills[position] = {skill: count for skill, count in skill_counts.most_common(10)}
        
        return {
            "position_distribution": top_positions,
            "position_skills": position_skills
        }
    
    def analyze_skills(self, df):
        """
        技能需求分析
        
        参数:
            df (pandas.DataFrame): 招聘数据
            
        返回:
            dict: 技能分析结果
        """
        if 'skills' not in df.columns:
            return {}
        
        # 合并所有技能列表并计数
        all_skills = []
        for skills_list in df['skills']:
            if isinstance(skills_list, list):
                all_skills.extend(skills_list)
            elif isinstance(skills_list, str):
                # 如果技能是字符串形式（可能是JSON字符串），尝试解析
                try:
                    skills = json.loads(skills_list.replace("'", "\""))
                    all_skills.extend(skills)
                except:
                    pass
        
        skill_counts = Counter(all_skills)
        top_skills = {skill: count for skill, count in skill_counts.most_common(50)}
        
        # 技能与薪资的关联分析
        skill_salary = {}
        if 'salary_avg' in df.columns:
            for skill, count in top_skills.items():
                if count < 10:  # 只分析出现次数足够多的技能
                    continue
                
                # 找出包含该技能的职位
                skill_jobs = df[df['skills'].apply(lambda x: 
                    skill in x if isinstance(x, list) else 
                    (skill in json.loads(x.replace("'", "\"")) if isinstance(x, str) else False)
                )]
                
                if len(skill_jobs) > 0:
                    skill_salary[skill] = {
                        "mean": skill_jobs['salary_avg'].mean(),
                        "median": skill_jobs['salary_avg'].median(),
                        "count": len(skill_jobs)
                    }
        
        return {
            "top_skills": top_skills,
            "skill_salary": skill_salary
        }
    
    def analyze_locations(self, df):
        """
        地域分布分析
        
        参数:
            df (pandas.DataFrame): 招聘数据
            
        返回:
            dict: 地域分析结果
        """
        if 'city' not in df.columns:
            return {}
        
        # 城市分布
        city_counts = df['city'].value_counts()
        top_cities = city_counts.head(20).to_dict()
        
        # 城市与职位的关联分析
        city_positions = {}
        if 'position_normalized' in df.columns:
            for city in top_cities.keys():
                city_df = df[df['city'] == city]
                position_counts = city_df['position_normalized'].value_counts()
                city_positions[city] = position_counts.head(5).to_dict()
        
        return {
            "city_distribution": top_cities,
            "city_positions": city_positions
        }
    
    def analyze_experience(self, df):
        """
        工作经验要求分析
        
        参数:
            df (pandas.DataFrame): 招聘数据
            
        返回:
            dict: 经验分析结果
        """
        if 'exp_min' not in df.columns:
            return {}
        
        # 创建经验分组
        df['exp_group'] = pd.cut(
            df['exp_min'], 
            bins=[-0.1, 0, 1, 3, 5, 10, 100], 
            labels=['不限', '0-1年', '1-3年', '3-5年', '5-10年', '10年以上']
        )
        
        # 经验分布
        exp_counts = df['exp_group'].value_counts()
        exp_distribution = exp_counts.to_dict()
        
        # 经验与职位的关联分析
        exp_positions = {}
        if 'position_normalized' in df.columns:
            for exp in exp_counts.index:
                exp_df = df[df['exp_group'] == exp]
                position_counts = exp_df['position_normalized'].value_counts()
                exp_positions[str(exp)] = position_counts.head(5).to_dict()
        
        return {
            "experience_distribution": exp_distribution,
            "experience_positions": exp_positions
        }
    
    def analyze_education(self, df):
        """
        学历要求分析
        
        参数:
            df (pandas.DataFrame): 招聘数据
            
        返回:
            dict: 学历分析结果
        """
        if 'education_level' not in df.columns:
            return {}
        
        # 学历分布
        edu_counts = df['education_level'].value_counts()
        edu_distribution = edu_counts.to_dict()
        
        # 学历与职位的关联分析
        edu_positions = {}
        if 'position_normalized' in df.columns:
            for edu in edu_counts.index:
                edu_df = df[df['education_level'] == edu]
                position_counts = edu_df['position_normalized'].value_counts()
                edu_positions[edu] = position_counts.head(5).to_dict()
        
        # 学历与薪资的关联分析
        edu_salary = {}
        if 'salary_avg' in df.columns:
            for edu in edu_counts.index:
                edu_df = df[df['education_level'] == edu]
                if len(edu_df) > 0:
                    edu_salary[edu] = {
                        "mean": edu_df['salary_avg'].mean(),
                        "median": edu_df['salary_avg'].median(),
                        "count": len(edu_df)
                    }
        
        return {
            "education_distribution": edu_distribution,
            "education_positions": edu_positions,
            "education_salary": edu_salary
        }
    
    def analyze_companies(self, df):
        """
        公司分析
        
        参数:
            df (pandas.DataFrame): 招聘数据
            
        返回:
            dict: 公司分析结果
        """
        if 'company' not in df.columns:
            return {}
        
        # 公司分布
        company_counts = df['company'].value_counts()
        top_companies = company_counts.head(50).to_dict()
        
        # 公司规模分析
        company_size = {}
        if 'company_size' in df.columns:
            size_counts = df['company_size'].value_counts()
            company_size = size_counts.to_dict()
        
        # 公司行业分析
        company_industry = {}
        if 'industry' in df.columns:
            industry_counts = df['industry'].value_counts()
            company_industry = industry_counts.head(20).to_dict()
        
        return {
            "top_companies": top_companies,
            "company_size": company_size,
            "company_industry": company_industry
        }
    
    def _save_results(self, results):
        """
        保存分析结果
        
        参数:
            results (dict): 分析结果
        """
        # 保存为JSON文件
        date_str = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(self.output_dir, f"analysis_{date_str}.json")
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # 同时保存一份最新分析结果的副本
        latest_path = os.path.join(self.output_dir, "analysis_latest.json")
        with open(latest_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # 生成可视化图表
        self._generate_visualizations(results)
    
    def _generate_visualizations(self, results):
        """
        生成可视化图表
        
        参数:
            results (dict): 分析结果
        """
        # 创建可视化目录
        vis_dir = os.path.join(self.output_dir, "visualizations")
        if not os.path.exists(vis_dir):
            os.makedirs(vis_dir)
        
        # 这里可以根据分析结果生成各种可视化图表
        # 例如薪资分布图、技能词云图、地域热力图等
        # 由于实际实现需要根据具体数据和需求，这里只提供基本框架
        
        # 示例：生成职位分布饼图
        if 'position_analysis' in results and 'position_distribution' in results['position_analysis']:
            self._plot_position_distribution(results['position_analysis']['position_distribution'], vis_dir)
        
        # 示例：生成技能词云图
        if 'skill_analysis' in results and 'top_skills' in results['skill_analysis']:
            self._plot_skill_wordcloud(results['skill_analysis']['top_skills'], vis_dir)
        
        # 示例：生成城市薪资对比图
        if 'salary_analysis' in results and 'by_city' in results['salary_analysis']:
            self._plot_city_salary(results['salary_analysis']['by_city'], vis_dir)
    
    def _plot_position_distribution(self, position_distribution, vis_dir):
        """
        绘制职位分布饼图
        """
        plt.figure(figsize=(10, 8))
        positions = list(position_distribution.keys())[:10]  # 取前10个职位
        counts = [position_distribution[pos] for pos in positions]
        
        plt.pie(counts, labels=positions, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')  # 使饼图为正圆形
        plt.title('热门职位分布')
        
        # 保存图表
        plt.savefig(os.path.join(vis_dir, 'position_distribution.png'), dpi=300, bbox_inches='tight')
        plt.close()
    
    def _plot_skill_wordcloud(self, top_skills, vis_dir):
        """
        绘制技能词云图
        """
        wordcloud = WordCloud(
            width=800, 
            height=400, 
            background_color='white',
            font_path='simhei.ttf' if os.path.exists('simhei.ttf') else None,
            max_words=100
        ).generate_from_frequencies(top_skills)
        
        plt.figure(figsize=(10, 6))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('技能需求词云')
        
        # 保存图表
        plt.savefig(os.path.join(vis_dir, 'skill_wordcloud.png'), dpi=300, bbox_inches='tight')
        plt.close()
    
    def _plot_city_salary(self, city_salary, vis_dir):
        """
        绘制城市薪资对比图
        """
        # 提取前10个城市的平均薪资
        cities = list(city_salary.keys())[:10]
        mean_salaries = [city_salary[city]['mean'] for city in cities]
        
        plt.figure(figsize=(12, 6))
        bars = plt.bar(cities, mean_salaries)
        
        # 在柱状图上标注具体数值
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 500,
                    f'{int(height)}',
                    ha='center', va='bottom')
        
        plt.title('主要城市平均薪资对比')
        plt.xlabel('城市')
        plt.ylabel('平均薪资（元/月）')
        plt.xticks(rotation=45)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # 保存图表
        plt.savefig(os.path.join(vis_dir, 'city_salary_comparison.png'), dpi=300, bbox_inches='tight')
        plt.close()

# 示例用法
if __name__ == "__main__":
    analyzer = JobAnalyzer()
    try:
        df = analyzer.load_data()
        results = analyzer.analyze(df)
        print("分析完成，结果已保存到analysis目录")
    except Exception as e:
        print(f"分析过程中出错: {str(e)}")