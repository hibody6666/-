# -*- coding: utf-8 -*-
"""
招聘数据分析系统 - 主程序

该模块作为系统的入口点，负责初始化各个组件、
执行数据爬取和分析任务，并启动Web应用。
"""

import os
import time
import logging
import argparse
import schedule
import threading
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# 导入自定义模块
from data_crawler import JobCrawler
from data_cleaner import DataCleaner
from data_analyzer import JobAnalyzer
from llm_analyzer import LLMAnalyzer
import app

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("system.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("JobvncSystem")

# 加载环境变量
load_dotenv()

# 配置目录
DATA_DIR = os.environ.get("DATA_DIR", "./data")
ANALYSIS_DIR = os.environ.get("ANALYSIS_DIR", "./analysis")

# 确保目录存在
for directory in [DATA_DIR, ANALYSIS_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)


def run_data_pipeline():
    """
    执行完整的数据处理流程：爬取 -> 清洗 -> 分析 -> 大模型分析
    """
    logger.info("开始执行数据处理流程")
    
    try:
        # 1. 爬取数据
        logger.info("开始爬取招聘数据")
        crawler = JobCrawler(output_dir=DATA_DIR)
        jobs = crawler.run_daily()
        
        if not jobs:
            logger.warning("未爬取到任何招聘信息，流程终止")
            return False
            
        # 2. 数据清洗
        logger.info("开始清洗招聘数据")
        cleaner = DataCleaner()
        latest_file = os.path.join(DATA_DIR, "jobs_latest.csv")
        
        if os.path.exists(latest_file):
            df = pd.read_csv(latest_file, encoding='utf-8-sig')
            cleaned_df = cleaner.clean_data(df)
            cleaned_file = os.path.join(DATA_DIR, f"jobs_cleaned_{datetime.now().strftime('%Y%m%d')}.csv")
            cleaned_df.to_csv(cleaned_file, index=False, encoding='utf-8-sig')
            cleaned_df.to_csv(os.path.join(DATA_DIR, "jobs_cleaned_latest.csv"), index=False, encoding='utf-8-sig')
            logger.info(f"数据清洗完成，保存至 {cleaned_file}")
        else:
            logger.error(f"找不到最新数据文件: {latest_file}")
            return False
        
        # 3. 数据分析
        logger.info("开始分析招聘数据")
        analyzer = JobAnalyzer(data_dir=DATA_DIR, output_dir=ANALYSIS_DIR)
        analysis_results = analyzer.analyze()
        
        if not analysis_results:
            logger.warning("数据分析未产生结果")
            return False
        
        # 4. 大模型分析
        logger.info("开始大模型分析")
        llm_analyzer = LLMAnalyzer()
        llm_results = llm_analyzer.analyze(analysis_results)
        
        if llm_results:
            # 将大模型分析结果添加到分析结果中
            analysis_results["llm_analysis"] = llm_results
            
            # 保存更新后的分析结果
            analyzer.save_results(analysis_results)
            logger.info("大模型分析完成并保存结果")
        
        logger.info("数据处理流程执行完成")
        return True
        
    except Exception as e:
        logger.error(f"数据处理流程执行出错: {str(e)}")
        return False


def schedule_tasks():
    """
    设置定时任务
    """
    # 每天凌晨2点执行数据处理流程
    schedule.every().day.at("02:00").do(run_data_pipeline)
    
    logger.info("定时任务已设置")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次是否有待执行的任务


def start_scheduler():
    """
    在后台线程中启动定时任务
    """
    scheduler_thread = threading.Thread(target=schedule_tasks)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    logger.info("定时任务调度器已在后台启动")


def start_web_app(host='0.0.0.0', port=8050, debug=False):
    """
    启动Web应用
    """
    logger.info(f"启动Web应用，监听地址: {host}:{port}")
    app.app.run(host=host, port=port, debug=debug)  # 使用新的run方法替代已弃用的run_server


def main():
    """
    主函数，解析命令行参数并执行相应操作
    """
    parser = argparse.ArgumentParser(description="招聘数据分析系统")
    parser.add_argument("--run-pipeline", action="store_true", help="立即执行数据处理流程")
    parser.add_argument("--no-scheduler", action="store_true", help="不启动定时任务调度器")
    parser.add_argument("--host", default="0.0.0.0", help="Web应用监听地址")
    parser.add_argument("--port", type=int, default=8050, help="Web应用监听端口")
    parser.add_argument("--debug", action="store_true", help="启用调试模式")
    
    args = parser.parse_args()
    
    logger.info("招聘数据分析系统启动")
    
    # 如果指定了立即执行数据处理流程
    if args.run_pipeline:
        run_data_pipeline()
    
    # 如果未禁用定时任务调度器
    if not args.no_scheduler:
        start_scheduler()
    
    # 启动Web应用
    start_web_app(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()