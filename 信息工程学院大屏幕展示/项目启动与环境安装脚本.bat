@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

title 招聘数据分析系统 - 环境安装与启动

echo ======================================================
echo             上海中侨大学招聘数据分析系统 - 环境安装与启动
echo ======================================================
echo.

:: 检查Python是否已安装
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo [错误] 未检测到Python环境，请先安装Python 3.7或更高版本。
    echo 您可以从 https://www.python.org/downloads/ 下载并安装Python。
    echo.
    pause
    exit /b 1
)

:: 检查Python版本
for /f "tokens=2 delims=." %%a in ('python -V 2^>^&1') do (
    set PYTHON_MAJOR=%%a
)
if not defined PYTHON_MAJOR (
    echo [错误] 无法检测Python版本。
    pause
    exit /b 1
)
if %PYTHON_MAJOR% lss 3 (
    echo [错误] 检测到Python版本过低，请安装Python 3.7或更高版本。
    echo 当前Python版本: 
    python --version
    echo.
    pause
    exit /b 1
)

echo [信息] 检测到Python环境: 
python --version
echo.

:menu
cls
echo ======================================================
echo             上海中侨大学招聘数据分析系统 - 环境安装与启动
echo ======================================================
echo.
echo  请选择操作:
echo  [1] 安装环境依赖
echo  [2] 启动系统(Web界面)
echo  [3] 执行数据处理流程
echo  [4] 退出
echo.
echo ======================================================

set /p choice=请输入选项 [1-4]: 

if "%choice%"=="1" goto install
if "%choice%"=="2" goto start_web
if "%choice%"=="3" goto run_pipeline
if "%choice%"=="4" goto end

echo [错误] 无效的选项，请重新选择。
timeout /t 2 >nul
goto menu

:install
echo.
echo [信息] 开始安装环境依赖...
echo.

:: 创建数据目录
if not exist "data" mkdir data
if not exist "analysis" mkdir analysis

:: 安装依赖包
echo [信息] 正在安装依赖包，这可能需要几分钟时间...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo.
    echo [错误] 安装依赖包时出错，请检查网络连接或手动安装。
    echo.
    pause
    goto menu
)

echo.
echo [成功] 环境依赖安装完成！
echo.
pause
goto menu

:start_web
echo.
echo [信息] 正在启动招聘数据分析系统Web界面...
echo [信息] 系统启动后，请在浏览器中访问 http://localhost:8050
echo [信息] 按Ctrl+C可以停止系统运行
echo.

python main.py

echo.
echo [信息] 系统已停止运行
echo.
pause
goto menu

:run_pipeline
echo.
echo [信息] 正在执行数据处理流程(爬取、清洗、分析数据)...
echo.

python main.py --run-pipeline --no-scheduler

echo.
echo [信息] 数据处理流程已完成
echo.
pause
goto menu

:end
echo.
echo [信息] 感谢使用招聘数据分析系统！
echo.
timeout /t 3 >nul
exit /b 0