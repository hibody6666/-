# -*- coding: utf-8 -*-
"""
大模型分析模块

该模块负责调用大模型API对招聘数据进行深度分析，
提供更有价值的洞察和建议。
"""

import os
import json
import requests
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("llm_analyzer.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("LLMAnalyzer")

class LLMAnalyzer:
    """
    大模型分析类，用于调用大模型API分析招聘数据
    """
    
    def __init__(self, api_key=None, api_url=None):
        """
        初始化大模型分析器
        
        参数:
            api_key (str, optional): 大模型API密钥
            api_url (str, optional): 大模型API地址
        """
        # 从环境变量或配置文件中获取API密钥和URL
        self.api_key = api_key or os.environ.get("LLM_API_KEY", "")
        self.api_url = api_url or os.environ.get("LLM_API_URL", "https://api.example.com/v1/completions")
        
        # 检查API密钥是否存在
        if not self.api_key:
            logger.warning("未设置大模型API密钥，将使用模拟分析")
    
    def analyze(self, data):
        """
        分析招聘数据
        
        参数:
            data (dict): 招聘数据分析结果
            
        返回:
            dict: 大模型分析结果
        """
        try:
            # 如果没有API密钥，使用模拟分析
            if not self.api_key:
                return self._mock_analysis(data)
            
            # 准备发送给大模型的数据
            prompt = self._prepare_prompt(data)
            
            # 调用大模型API
            response = self._call_llm_api(prompt)
            
            # 解析大模型返回的结果
            analysis_result = self._parse_response(response)
            
            # 记录分析时间
            analysis_result["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"大模型分析出错: {str(e)}")
            # 出错时返回模拟分析结果
            return self._mock_analysis(data)
    
    def _prepare_prompt(self, data):
        """
        准备发送给大模型的提示词
        
        参数:
            data (dict): 招聘数据分析结果
            
        返回:
            str: 提示词
        """
        # 提取关键数据
        basic_stats = data.get("basic_stats", {})
        salary_analysis = data.get("salary_analysis", {})
        skill_analysis = data.get("skill_analysis", {})
        position_analysis = data.get("position_analysis", {})
        location_analysis = data.get("location_analysis", {})
        experience_analysis = data.get("experience_analysis", {})
        education_analysis = data.get("education_analysis", {})
        
        # 构建提示词
        prompt = f"""
        请分析以下计算机专业招聘数据，并提供深度洞察和就业建议：
        
        基本统计信息：
        - 总职位数：{basic_stats.get('total_jobs', 0)}
        - 平均薪资：{basic_stats.get('avg_salary', 0):.2f}元/月
        - 职位类型数：{basic_stats.get('unique_positions', 0)}
        - 城市数量：{basic_stats.get('unique_cities', 0)}
        
        薪资分析：
        - 平均薪资：{salary_analysis.get('mean', 0):.2f}元/月
        - 中位薪资：{salary_analysis.get('median', 0):.2f}元/月
        - 薪资范围：{salary_analysis.get('min', 0):.2f} - {salary_analysis.get('max', 0):.2f}元/月
        
        热门职位（前5）：
        {self._format_dict_for_prompt(position_analysis.get('position_distribution', {}), 5)}
        
        热门技能（前10）：
        {self._format_dict_for_prompt(skill_analysis.get('top_skills', {}), 10)}
        
        主要城市分布（前5）：
        {self._format_dict_for_prompt(location_analysis.get('city_distribution', {}), 5)}
        
        经验要求分布：
        {self._format_dict_for_prompt(experience_analysis.get('experience_distribution', {}))}
        
        学历要求分布：
        {self._format_dict_for_prompt(education_analysis.get('education_distribution', {}))}
        
        请提供以下内容：
        1. 对当前计算机专业就业市场的总体分析（300字左右）
        2. 5-8条关键洞察
        3. 对计算机专业学生和求职者的5-8条就业建议
        
        请以JSON格式返回，包含summary（总体分析）、insights（关键洞察数组）和recommendations（就业建议数组）字段。
        """
        
        return prompt
    
    def _format_dict_for_prompt(self, data_dict, limit=None):
        """
        将字典数据格式化为提示词中的列表
        
        参数:
            data_dict (dict): 要格式化的字典
            limit (int, optional): 限制条目数量
            
        返回:
            str: 格式化后的字符串
        """
        if not data_dict:
            return "无数据"
        
        # 按值排序并限制数量
        sorted_items = sorted(data_dict.items(), key=lambda x: x[1], reverse=True)
        if limit:
            sorted_items = sorted_items[:limit]
        
        # 格式化为字符串
        formatted_str = "\n".join([f"- {key}: {value}" for key, value in sorted_items])
        return formatted_str
    
    def _call_llm_api(self, prompt):
        """
        调用大模型API
        
        参数:
            prompt (str): 提示词
            
        返回:
            str: API返回的响应
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        data = {
            "model": "gpt-3.5-turbo",  # 或其他适合的模型
            "messages": [
                {"role": "system", "content": "你是一位专业的招聘数据分析师，擅长分析计算机专业的就业市场。"},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
            "max_tokens": 2000
        }
        
        try:
            response = requests.post(self.api_url, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"调用大模型API出错: {str(e)}")
            raise
    
    def _parse_response(self, response):
        """
        解析大模型API返回的响应
        
        参数:
            response (dict): API响应
            
        返回:
            dict: 解析后的分析结果
        """
        try:
            # 根据实际API返回格式进行解析
            # 这里假设返回格式为OpenAI API的格式
            content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
            
            # 尝试解析JSON内容
            # 如果返回的不是有效JSON，则进行文本处理
            try:
                result = json.loads(content)
            except json.JSONDecodeError:
                # 如果不是JSON格式，尝试从文本中提取信息
                result = self._extract_analysis_from_text(content)
            
            return result
        except Exception as e:
            logger.error(f"解析大模型响应出错: {str(e)}")
            raise
    
    def _extract_analysis_from_text(self, text):
        """
        从文本中提取分析结果
        
        参数:
            text (str): 大模型返回的文本
            
        返回:
            dict: 提取的分析结果
        """
        # 简单的文本处理逻辑，实际应用中可能需要更复杂的处理
        lines = text.split("\n")
        
        summary = ""
        insights = []
        recommendations = []
        
        current_section = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if "总体分析" in line or "市场分析" in line or "总结" in line:
                current_section = "summary"
                continue
            elif "洞察" in line or "发现" in line or "趋势" in line:
                current_section = "insights"
                continue
            elif "建议" in line or "推荐" in line or "策略" in line:
                current_section = "recommendations"
                continue
            
            # 处理列表项
            if line.startswith("-") or line.startswith("*") or (line[0].isdigit() and line[1] in ".)、"):
                line = line.lstrip("-*0123456789.)、 ")
                if current_section == "insights":
                    insights.append(line)
                elif current_section == "recommendations":
                    recommendations.append(line)
                continue
            
            # 处理普通文本
            if current_section == "summary":
                summary += line + " "
        
        return {
            "summary": summary.strip(),
            "insights": insights,
            "recommendations": recommendations
        }
    
    def _mock_analysis(self, data):
        """
        生成模拟分析结果（当无法调用API时使用）
        
        参数:
            data (dict): 招聘数据分析结果
            
        返回:
            dict: 模拟分析结果
        """
        # 提取一些基本数据用于生成模拟分析
        basic_stats = data.get("basic_stats", {})
        total_jobs = basic_stats.get("total_jobs", 0)
        avg_salary = basic_stats.get("avg_salary", 0)
        
        # 获取热门职位和技能
        position_analysis = data.get("position_analysis", {})
        position_distribution = position_analysis.get("position_distribution", {})
        top_positions = list(position_distribution.keys())[:3] if position_distribution else ["开发工程师", "算法工程师", "测试工程师"]
        
        skill_analysis = data.get("skill_analysis", {})
        top_skills = list(skill_analysis.get("top_skills", {}).keys())[:5] if skill_analysis.get("top_skills") else ["Python", "Java", "JavaScript", "算法", "数据分析"]
        
        # 生成模拟分析结果
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return {
            "summary": f"当前计算机专业就业市场总体活跃，共有{total_jobs}个职位空缺，平均薪资{avg_salary:.2f}元/月。从数据分析来看，{', '.join(top_positions)}等职位需求量较大，企业对人才的技能要求日益多元化，不仅注重专业技能，也越来越重视综合素质。技术发展日新月异，{', '.join(top_skills[:2])}等技术持续热门，而{', '.join(top_skills[2:])}等新兴技术也展现出强劲的发展势头。总体来看，计算机专业人才市场供需关系良好，但竞争也较为激烈，求职者需要不断提升自身技能以保持竞争力。",
            
            "insights": [
                f"{', '.join(top_positions[:2])}等职位是当前市场需求最大的岗位，就业机会较多。",
                f"平均薪资{avg_salary:.2f}元/月，高于全国平均工资水平，显示出计算机行业的薪资优势。",
                f"{', '.join(top_skills[:2])}是企业最看重的技能，掌握这些技能有助于提高就业竞争力。",
                "一线城市的招聘需求量大，但竞争也更加激烈。",
                "随着人工智能和大数据技术的发展，相关岗位需求呈上升趋势。",
                "企业对复合型人才的需求增加，既懂技术又了解业务的人才更受欢迎。",
                "远程工作和灵活办公模式正在计算机行业逐渐普及。"
            ],
            
            "recommendations": [
                f"重点学习{', '.join(top_skills[:3])}等热门技术，提高核心竞争力。",
                "积极参与开源项目和实践项目，积累实际工作经验。",
                "关注行业动态，及时了解新技术发展趋势。",
                "提升软技能，如沟通能力、团队协作能力和问题解决能力。",
                "建立专业社交网络，参加行业交流活动和技术社区。",
                "针对目标职位，有针对性地准备简历和面试。",
                "考虑获取相关专业认证，增加就业竞争优势。",
                "保持持续学习的习惯，适应技术快速迭代的特点。"
            ],
            
            "timestamp": timestamp
        }