# -*- coding: utf-8 -*-
"""
数据采集模块

该模块负责从各大招聘平台爬取计算机专业的招聘信息，
包括工作内容、薪资、要求等数据，并定时执行（每天一次）。
"""

import requests
import pandas as pd
import time
import random
import logging
import json
import os
from datetime import datetime
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("JobCrawler")

class JobCrawler:
    """
    招聘信息爬虫类，用于从各大招聘平台爬取数据
    """
    
    def __init__(self, output_dir="./data"):
        """
        初始化爬虫
        
        参数:
            output_dir (str): 数据保存目录
        """
        self.output_dir = output_dir
        self.ua = UserAgent()
        self.session = requests.Session()
        
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 招聘平台配置
        self.platforms = {
            "zhilian": {
                "name": "智联招聘",
                "crawler": self._crawl_zhilian
            },
            "51job": {
                "name": "前程无忧",
                "crawler": self._crawl_51job
            },
            "lagou": {
                "name": "拉勾网",
                "crawler": self._crawl_lagou
            },
            "boss": {
                "name": "BOSS直聘",
                "crawler": self._crawl_boss
            }
        }
        
        # 计算机专业相关关键词
        self.keywords = [
            "Java", "Python", "C++", "前端", "后端", "全栈", 
            "算法", "人工智能", "机器学习", "深度学习", "数据分析", 
            "大数据", "云计算", "运维", "测试", "开发", "架构师",
            "数据库", "网络安全", "软件工程师", "程序员", "开发工程师"
        ]
    
    def run_daily(self):
        """
        执行每日爬取任务
        """
        logger.info("开始每日爬取任务")
        
        all_jobs = []
        
        # 对每个平台执行爬取
        for platform_id, platform_info in self.platforms.items():
            try:
                logger.info(f"开始爬取{platform_info['name']}")
                jobs = platform_info['crawler']()
                all_jobs.extend(jobs)
                logger.info(f"成功从{platform_info['name']}爬取{len(jobs)}条招聘信息")
                
                # 随机延时，避免请求过于频繁
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logger.error(f"爬取{platform_info['name']}时出错: {str(e)}")
        
        # 保存数据
        if all_jobs:
            self._save_data(all_jobs)
            logger.info(f"成功保存{len(all_jobs)}条招聘信息")
        else:
            logger.warning("未爬取到任何招聘信息")
        
        logger.info("每日爬取任务完成")
        
        return all_jobs
    
    def _get_headers(self):
        """
        获取随机请求头
        """
        return {
            "User-Agent": self.ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0"
        }
    
    def _save_data(self, jobs):
        """
        保存爬取的招聘数据
        
        参数:
            jobs (list): 招聘信息列表
        """
        # 生成文件名，包含日期
        date_str = datetime.now().strftime("%Y%m%d")
        file_path = os.path.join(self.output_dir, f"jobs_{date_str}.csv")
        
        # 转换为DataFrame并保存
        df = pd.DataFrame(jobs)
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        # 同时保存一份最新数据的副本
        latest_path = os.path.join(self.output_dir, "jobs_latest.csv")
        df.to_csv(latest_path, index=False, encoding='utf-8-sig')
    
    def _crawl_zhilian(self):
        """
        爬取智联招聘
        
        返回:
            list: 招聘信息列表
        """
        jobs = []
        
        # 对每个关键词进行搜索
        for keyword in self.keywords:
            try:
                # 这里是智联招聘的爬取逻辑
                # 由于实际爬取需要处理反爬机制，这里使用模拟数据进行测试
                
                # 生成模拟数据，用于测试系统功能
                logger.info(f"为关键词 '{keyword}' 生成模拟招聘数据")
                
                # 为每个关键词生成5-10条模拟数据
                num_jobs = random.randint(5, 10)
                for i in range(num_jobs):
                    # 职位名称：关键词 + 工程师/专家/开发者等后缀
                    suffixes = ["工程师", "开发工程师", "架构师", "专家", "开发者", "技术经理"]
                    position = f"{keyword}{random.choice(suffixes)}"
                    
                    # 公司名称
                    companies = ["科技有限公司", "网络科技", "信息技术有限公司", "软件开发公司", "互联网公司"]
                    company = f"智联{random.choice(['未来', '创新', '科技', '智慧', '云计算'])}{random.choice(companies)}"
                    
                    # 薪资范围
                    salary_min = random.randint(5, 30) * 1000
                    salary_max = salary_min + random.randint(5, 20) * 1000
                    salary = f"{salary_min // 1000}K-{salary_max // 1000}K"
                    salary_avg = (salary_min + salary_max) / 2
                    
                    # 工作地点
                    cities = ["北京", "上海", "广州", "深圳", "杭州", "成都", "南京", "武汉", "西安", "苏州"]
                    city = random.choice(cities)
                    
                    # 工作经验要求
                    exp_choices = ["无经验", "1-3年", "3-5年", "5-10年", "10年以上"]
                    experience = random.choice(exp_choices)
                    
                    # 修复经验解析逻辑，正确处理带有"年"字符的情况
                    if experience == "无经验":
                        exp_min = 0
                        exp_max = 1
                    elif "-" in experience:
                        # 提取数字部分，去除"年"字符
                        exp_parts = experience.replace("年", "").split("-")
                        exp_min = int(exp_parts[0])
                        exp_max = int(exp_parts[1])
                    else:
                        # 处理"10年以上"这样的格式
                        exp_min = int(experience.replace("年以上", ""))
                        exp_max = 15
                    
                    # 学历要求
                    education_choices = ["不限", "大专", "本科", "硕士", "博士"]
                    education_level = random.choice(education_choices)
                    
                    # 公司规模
                    size_choices = ["少于50人", "50-200人", "200-500人", "500-1000人", "1000-5000人", "5000-10000人", "10000人以上"]
                    company_size = random.choice(size_choices)
                    
                    # 公司行业
                    industry_choices = ["互联网/电子商务", "计算机软件", "IT服务", "电子技术/半导体/集成电路", "人工智能", "区块链", "云计算/大数据"]
                    industry = random.choice(industry_choices)
                    
                    # 技能要求
                    all_skills = ["Python", "Java", "C++", "JavaScript", "HTML", "CSS", "SQL", "Linux", "Docker", 
                                 "Kubernetes", "React", "Vue", "Angular", "Node.js", "Spring", "Django", "Flask", 
                                 "TensorFlow", "PyTorch", "数据分析", "机器学习", "深度学习", "自然语言处理", 
                                 "计算机视觉", "微服务", "敏捷开发", "CI/CD", "Git", "AWS", "Azure", "GCP"]
                    
                    # 根据职位关键词选择相关技能
                    relevant_skills = []
                    if "Java" in keyword:
                        relevant_skills.extend(["Java", "Spring", "微服务", "MySQL"])
                    elif "Python" in keyword:
                        relevant_skills.extend(["Python", "Django", "Flask", "数据分析"])
                    elif "前端" in keyword:
                        relevant_skills.extend(["JavaScript", "HTML", "CSS", "React", "Vue"])
                    elif "算法" in keyword or "人工智能" in keyword or "机器学习" in keyword:
                        relevant_skills.extend(["Python", "TensorFlow", "PyTorch", "机器学习", "深度学习"])
                    elif "大数据" in keyword:
                        relevant_skills.extend(["Hadoop", "Spark", "Hive", "数据仓库", "ETL"])
                    
                    # 如果没有匹配到特定技能，随机选择
                    if not relevant_skills:
                        relevant_skills = random.sample(all_skills, random.randint(3, 6))
                    else:
                        # 添加一些随机技能
                        additional_skills = random.sample([s for s in all_skills if s not in relevant_skills], 
                                                        random.randint(1, 3))
                        relevant_skills.extend(additional_skills)
                    
                    # 职位描述
                    descriptions = [
                        f"负责{keyword}相关技术研发和系统优化",
                        f"参与{keyword}项目的设计和开发",
                        f"负责{keyword}平台的架构设计和实现",
                        f"参与{keyword}核心模块的开发和维护",
                        f"负责{keyword}系统的性能优化和问题排查"
                    ]
                    description = random.choice(descriptions)
                    
                    # 创建职位信息字典
                    job = {
                        "position": position,
                        "position_normalized": keyword,
                        "company": company,
                        "salary": salary,
                        "salary_min": salary_min,
                        "salary_max": salary_max,
                        "salary_avg": salary_avg,
                        "city": city,
                        "experience": experience,
                        "exp_min": exp_min,
                        "exp_max": exp_max,
                        "education_level": education_level,
                        "company_size": company_size,
                        "industry": industry,
                        "skills": relevant_skills,
                        "description": description,
                        "source": "智联招聘(模拟数据)",
                        "url": f"https://sou.zhaopin.com/?kw={keyword}",
                        "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    jobs.append(job)
                
                # 随机延时，模拟真实爬取场景
                time.sleep(random.uniform(0.5, 1.5))
                
            except Exception as e:
                logger.error(f"爬取智联招聘关键词'{keyword}'时出错: {str(e)}")
        
        logger.info(f"从智联招聘获取了 {len(jobs)} 条招聘信息")
        return jobs
    
    def _crawl_51job(self):
        """
        爬取前程无忧
        
        返回:
            list: 招聘信息列表
        """
        jobs = []
        
        # 对每个关键词进行搜索
        for keyword in self.keywords:
            try:
                # 这里是前程无忧的爬取逻辑
                # 由于实际爬取需要处理反爬机制，这里只提供基本框架
                
                # URL编码关键词，解决中文编码问题
                import urllib.parse
                encoded_keyword = urllib.parse.quote(keyword)
                
                # 示例URL（实际使用时需要替换）
                url = f"https://search.51job.com/list/000000,000000,0000,00,9,99,{encoded_keyword},2,1.html"
                
                response = self.session.get(url, headers=self._get_headers(), timeout=10)
                if response.status_code == 200:
                    # 解析页面，提取招聘信息
                    # 这里需要根据实际网页结构编写解析代码
                    # jobs.extend(parsed_jobs)
                    pass
                
                # 随机延时
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                logger.error(f"爬取前程无忧关键词'{keyword}'时出错: {str(e)}")
        
        return jobs
    
    def _crawl_lagou(self):
        """
        爬取拉勾网
        
        返回:
            list: 招聘信息列表
        """
        jobs = []
        
        # 对每个关键词进行搜索
        for keyword in self.keywords:
            try:
                # 这里是拉勾网的爬取逻辑
                # 拉勾网使用AJAX加载数据，需要特殊处理
                
                # 示例URL和请求数据（实际使用时需要替换）
                url = "https://www.lagou.com/jobs/positionAjax.json"
                params = {
                    "city": "全国",
                    "needAddtionalResult": "false"
                }
                data = {
                    "first": "true",
                    "pn": 1,
                    "kd": keyword
                }
                
                # URL编码关键词，解决中文编码问题
                import urllib.parse
                encoded_keyword = urllib.parse.quote(keyword)
                
                # 拉勾网需要先访问搜索页获取cookies
                self.session.get(
                    f"https://www.lagou.com/jobs/list_{encoded_keyword}",
                    headers=self._get_headers(),
                    timeout=10
                )
                
                # 然后才能请求AJAX接口
                headers = self._get_headers()
                headers["Referer"] = f"https://www.lagou.com/jobs/list_{encoded_keyword}"
                headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
                headers["X-Requested-With"] = "XMLHttpRequest"
                
                response = self.session.post(
                    url, 
                    headers=headers, 
                    params=params, 
                    data=data,
                    timeout=10
                )
                
                if response.status_code == 200:
                    # 解析JSON响应，提取招聘信息
                    # 这里需要根据实际接口返回格式编写解析代码
                    # jobs.extend(parsed_jobs)
                    pass
                
                # 随机延时
                time.sleep(random.uniform(3, 5))  # 拉勾网反爬较严格，延时更长
                
            except Exception as e:
                logger.error(f"爬取拉勾网关键词'{keyword}'时出错: {str(e)}")
        
        return jobs
    
    def _crawl_boss(self):
        """
        爬取BOSS直聘
        
        返回:
            list: 招聘信息列表
        """
        jobs = []
        
        # 对每个关键词进行搜索
        for keyword in self.keywords:
            try:
                # 这里是BOSS直聘的爬取逻辑
                # BOSS直聘反爬较为严格，可能需要处理验证码等问题
                
                # URL编码关键词，解决中文编码问题
                import urllib.parse
                encoded_keyword = urllib.parse.quote(keyword)
                
                # 示例URL（实际使用时需要替换）
                url = f"https://www.zhipin.com/c100010000/?query={encoded_keyword}&ka=search_100010000"
                
                response = self.session.get(url, headers=self._get_headers(), timeout=10)
                if response.status_code == 200:
                    # 解析页面，提取招聘信息
                    # 这里需要根据实际网页结构编写解析代码
                    # jobs.extend(parsed_jobs)
                    pass
                
                # 随机延时
                time.sleep(random.uniform(3, 5))  # BOSS直聘反爬较严格，延时更长
                
            except Exception as e:
                logger.error(f"爬取BOSS直聘关键词'{keyword}'时出错: {str(e)}")
        
        return jobs

# 示例用法
if __name__ == "__main__":
    crawler = JobCrawler()
    jobs = crawler.run_daily()
    print(f"共爬取{len(jobs)}条招聘信息")