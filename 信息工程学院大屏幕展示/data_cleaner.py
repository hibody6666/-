# -*- coding: utf-8 -*-
"""
数据清洗模块

该模块负责对从各大招聘平台爬取的原始数据进行清洗和预处理，
包括去除无效数据、标准化字段格式、处理缺失值等。
"""

import pandas as pd
import numpy as np
import re
import jieba
import jieba.analyse
from datetime import datetime

class DataCleaner:
    """
    数据清洗类，用于处理招聘数据
    """
    
    def __init__(self):
        """
        初始化数据清洗器
        """
        # 加载自定义词典
        self._load_custom_dict()
        
    def _load_custom_dict(self):
        """
        加载自定义词典，包括计算机专业相关术语
        """
        # 这里可以加载自定义词典文件
        # jieba.load_userdict("path/to/dict.txt")
        
        # 添加计算机专业相关术语
        computer_terms = [
            "人工智能", "机器学习", "深度学习", "数据挖掘", "自然语言处理",
            "计算机视觉", "大数据", "云计算", "区块链", "物联网",
            "前端开发", "后端开发", "全栈开发", "移动开发", "DevOps",
            "Python", "Java", "C++", "JavaScript", "Go", "Rust", "PHP", "C#",
            "React", "Vue", "Angular", "Node.js", "Django", "Flask", "Spring Boot",
            "TensorFlow", "PyTorch", "Hadoop", "Spark", "Kubernetes", "Docker"
        ]
        
        for term in computer_terms:
            jieba.add_word(term)
    
    def clean_data(self, df):
        """
        清洗招聘数据
        
        参数:
            df (pandas.DataFrame): 原始招聘数据
            
        返回:
            pandas.DataFrame: 清洗后的数据
        """
        if df.empty:
            return df
        
        # 创建数据副本，避免修改原始数据
        cleaned_df = df.copy()
        
        # 处理缺失值
        cleaned_df = self._handle_missing_values(cleaned_df)
        
        # 标准化职位名称
        if 'position' in cleaned_df.columns:
            cleaned_df['position_normalized'] = cleaned_df['position'].apply(self._normalize_position)
        
        # 提取薪资信息并标准化
        if 'salary' in cleaned_df.columns:
            salary_info = cleaned_df['salary'].apply(self._parse_salary)
            cleaned_df['salary_min'] = salary_info.apply(lambda x: x.get('min', np.nan))
            cleaned_df['salary_max'] = salary_info.apply(lambda x: x.get('max', np.nan))
            cleaned_df['salary_avg'] = cleaned_df.apply(
                lambda row: (row['salary_min'] + row['salary_max']) / 2 if pd.notna(row['salary_min']) and pd.notna(row['salary_max']) else np.nan, 
                axis=1
            )
            cleaned_df['salary_unit'] = salary_info.apply(lambda x: x.get('unit', ''))
        
        # 提取工作经验要求
        if 'experience' in cleaned_df.columns:
            exp_info = cleaned_df['experience'].apply(self._parse_experience)
            cleaned_df['exp_min'] = exp_info.apply(lambda x: x.get('min', np.nan))
            cleaned_df['exp_max'] = exp_info.apply(lambda x: x.get('max', np.nan))
        
        # 提取学历要求
        if 'education' in cleaned_df.columns:
            cleaned_df['education_level'] = cleaned_df['education'].apply(self._normalize_education)
        
        # 提取工作地点并标准化
        if 'location' in cleaned_df.columns:
            location_info = cleaned_df['location'].apply(self._parse_location)
            cleaned_df['city'] = location_info.apply(lambda x: x.get('city', ''))
            cleaned_df['district'] = location_info.apply(lambda x: x.get('district', ''))
        
        # 从职位描述中提取关键技能
        if 'description' in cleaned_df.columns:
            cleaned_df['skills'] = cleaned_df['description'].apply(self._extract_skills)
        
        # 添加数据处理时间戳
        cleaned_df['processed_at'] = datetime.now()
        
        return cleaned_df
    
    def _handle_missing_values(self, df):
        """
        处理数据中的缺失值
        """
        # 复制数据框以避免修改原始数据
        result = df.copy()
        
        # 对于不同的列采用不同的缺失值处理策略
        # 数值型列可以用均值、中位数或0填充
        numeric_cols = result.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            result[col] = result[col].fillna(result[col].median())
        
        # 类别型列可以用众数或'未知'填充
        categorical_cols = ['position', 'company', 'education', 'location']
        for col in categorical_cols:
            if col in result.columns:
                result[col] = result[col].fillna('未知')
        
        # 描述类文本，可以用空字符串填充
        text_cols = ['description', 'requirement']
        for col in text_cols:
            if col in result.columns:
                result[col] = result[col].fillna('')
        
        return result
    
    def _normalize_position(self, position):
        """
        标准化职位名称
        """
        if pd.isna(position):
            return '未知'
        
        # 转换为小写并去除多余空格
        normalized = position.lower().strip()
        
        # 职位名称映射字典
        position_mapping = {
            r'(java|java开发|java工程师|java后端)': 'Java开发工程师',
            r'(python|python开发|python工程师|python后端)': 'Python开发工程师',
            r'(前端|web前端|前端开发|前端工程师|h5开发)': '前端开发工程师',
            r'(后端|后台|服务端|服务器端|后端开发)': '后端开发工程师',
            r'(全栈|全栈工程师|全栈开发)': '全栈开发工程师',
            r'(算法|机器学习|深度学习|ai算法|人工智能算法)': '算法工程师',
            r'(数据分析|数据挖掘|大数据分析)': '数据分析师',
            r'(测试|qa|测试工程师|软件测试|自动化测试)': '测试工程师',
            r'(运维|devops|运维工程师|系统运维)': '运维工程师',
            r'(产品经理|产品|pm)': '产品经理',
            r'(ui|设计师|ui设计|交互设计)': 'UI设计师',
            r'(项目经理|项目管理)': '项目经理',
            r'(架构师|系统架构师|技术架构师)': '架构师',
            r'(安全|网络安全|信息安全|安全工程师)': '安全工程师',
            r'(数据库|dba|数据库管理|数据库工程师)': '数据库工程师'
        }
        
        # 应用映射规则
        for pattern, normalized_position in position_mapping.items():
            if re.search(pattern, normalized):
                return normalized_position
        
        # 如果没有匹配到任何规则，返回原始职位名称
        return position
    
    def _parse_salary(self, salary_text):
        """
        解析薪资信息，提取最低、最高薪资和单位
        
        示例输入:
            '15k-25k'
            '15000-25000元'
            '15-30万/年'
        """
        if pd.isna(salary_text) or salary_text == '':
            return {'min': np.nan, 'max': np.nan, 'unit': ''}
        
        # 统一格式，去除空格
        salary_text = str(salary_text).strip().lower()
        
        # 提取数值和单位
        # 匹配形如 "10k-20k"、"10-20万/年"、"10000-20000元/月" 的格式
        pattern = r'(\d+(?:\.\d+)?)[k千万]?\s*[\-~至]\s*(\d+(?:\.\d+)?)[k千万]?'
        match = re.search(pattern, salary_text)
        
        if not match:
            return {'min': np.nan, 'max': np.nan, 'unit': ''}
        
        min_salary, max_salary = float(match.group(1)), float(match.group(2))
        
        # 确定单位并统一转换为"元/月"
        unit = '元/月'  # 默认单位
        
        # 检测单位
        if 'k' in salary_text or '千' in salary_text:
            min_salary *= 1000
            max_salary *= 1000
        elif '万' in salary_text:
            min_salary *= 10000
            max_salary *= 10000
        
        # 检测是否为年薪
        if '/年' in salary_text or '年薪' in salary_text:
            min_salary /= 12
            max_salary /= 12
            unit = '元/月(年薪折算)'
        
        return {
            'min': min_salary,
            'max': max_salary,
            'unit': unit
        }
    
    def _parse_experience(self, exp_text):
        """
        解析工作经验要求
        
        示例输入:
            '3-5年'
            '1年以上'
            '应届毕业生'
        """
        if pd.isna(exp_text) or exp_text == '':
            return {'min': np.nan, 'max': np.nan}
        
        # 统一格式，去除空格
        exp_text = str(exp_text).strip()
        
        # 匹配形如 "3-5年" 的范围
        range_pattern = r'(\d+)\s*[\-~至]\s*(\d+)\s*年'
        range_match = re.search(range_pattern, exp_text)
        if range_match:
            return {
                'min': float(range_match.group(1)),
                'max': float(range_match.group(2))
            }
        
        # 匹配形如 "3年以上" 的下限
        min_pattern = r'(\d+)\s*年以上'
        min_match = re.search(min_pattern, exp_text)
        if min_match:
            return {
                'min': float(min_match.group(1)),
                'max': np.nan
            }
        
        # 匹配形如 "5年以下" 的上限
        max_pattern = r'(\d+)\s*年以下'
        max_match = re.search(max_pattern, exp_text)
        if max_match:
            return {
                'min': 0,
                'max': float(max_match.group(1))
            }
        
        # 特殊情况处理
        if '应届' in exp_text or '毕业生' in exp_text:
            return {'min': 0, 'max': 1}
        elif '经验不限' in exp_text or '不限经验' in exp_text:
            return {'min': 0, 'max': np.nan}
        
        return {'min': np.nan, 'max': np.nan}
    
    def _normalize_education(self, education):
        """
        标准化学历要求
        """
        if pd.isna(education) or education == '':
            return '未知'
        
        # 统一格式，去除空格
        edu = str(education).strip().lower()
        
        # 学历等级映射
        edu_mapping = {
            '博士': '博士',
            '硕士': '硕士',
            '研究生': '硕士',
            '本科': '本科',
            '大学本科': '本科',
            '学士': '本科',
            '大专': '大专',
            '专科': '大专',
            '高中': '高中',
            '中专': '中专',
            '初中': '初中',
            '小学': '小学',
            '不限': '不限',
            '学历不限': '不限'
        }
        
        for key, value in edu_mapping.items():
            if key in edu:
                return value
        
        return '未知'
    
    def _parse_location(self, location):
        """
        解析工作地点信息
        
        示例输入:
            '北京'
            '上海-浦东新区'
            '广州市天河区'
        """
        if pd.isna(location) or location == '':
            return {'city': '未知', 'district': ''}
        
        # 统一格式，去除空格
        location = str(location).strip()
        
        # 处理常见的分隔符
        for sep in ['-', '，', ',', ' ', '市']:
            if sep in location:
                parts = location.split(sep, 1)
                if len(parts) == 2:
                    city, district = parts[0], parts[1]
                    # 处理城市名称中可能包含的"市"字
                    if city.endswith('市'):
                        city = city[:-1]
                    return {'city': city, 'district': district}
        
        # 如果没有分隔符，则认为整个字符串是城市名
        city = location
        # 处理城市名称中可能包含的"市"字
        if city.endswith('市'):
            city = city[:-1]
        return {'city': city, 'district': ''}
    
    def _extract_skills(self, description):
        """
        从职位描述中提取关键技能
        
        参数:
            description (str): 职位描述文本
            
        返回:
            list: 提取出的技能列表
        """
        if pd.isna(description) or description == '':
            return []
        
        # 使用jieba提取关键词
        keywords = jieba.analyse.extract_tags(description, topK=10, withWeight=False)
        
        # 技能关键词列表（可以根据实际情况扩充）
        skill_keywords = [
            # 编程语言
            'python', 'java', 'c++', 'javascript', 'go', 'rust', 'php', 'c#', 'swift', 'kotlin',
            # 前端技术
            'html', 'css', 'react', 'vue', 'angular', 'jquery', 'bootstrap', 'webpack', 'sass', 'less',
            # 后端框架
            'django', 'flask', 'spring', 'springboot', 'express', 'node.js', 'laravel', 'asp.net',
            # 数据库
            'mysql', 'postgresql', 'mongodb', 'redis', 'oracle', 'sql server', 'sqlite', 'elasticsearch',
            # 大数据和AI
            'hadoop', 'spark', 'hive', 'tensorflow', 'pytorch', 'keras', 'scikit-learn', 'pandas', 'numpy',
            # 云计算和DevOps
            'docker', 'kubernetes', 'aws', 'azure', 'gcp', 'jenkins', 'git', 'ci/cd', 'linux', 'unix',
            # 移动开发
            'android', 'ios', 'flutter', 'react native', 'swift', 'objective-c',
            # 其他技能
            '微服务', '分布式系统', '敏捷开发', '测试驱动', '设计模式', 'restful', 'graphql'
        ]
        
        # 过滤出技能相关的关键词
        skills = [keyword for keyword in keywords if keyword.lower() in skill_keywords]
        
        # 从描述中直接匹配技能关键词（处理jieba可能未提取到的情况）
        description_lower = description.lower()
        for skill in skill_keywords:
            if skill in description_lower and skill not in skills:
                skills.append(skill)
        
        return skills[:15]  # 限制返回的技能数量