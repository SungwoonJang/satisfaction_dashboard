@echo off
cd /d "%~dp0"

echo ===================================================
echo  Chatbot Satisfaction Dashboard - Install
echo ===================================================
echo.

echo [1/4] Checking Python version...
python --version
if errorlevel 1 (
    echo [ERROR] Python not found. Please install Python 3.9+
    echo         https://www.python.org
    pause
    exit /b 1
)

echo.
echo [2/4] Creating virtual environment (.venv)...
python -m venv .venv
if errorlevel 1 (
    echo [ERROR] Failed to create virtual environment.
    pause
    exit /b 1
)

echo.
echo [3/4] Installing packages...
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip --quiet
pip install -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Package installation failed. Check your internet connection.
    pause
    exit /b 1
)

echo.
echo [4/4] Creating required folders...
if not exist "input"        mkdir "input"
if not exist "output"       mkdir "output"
if not exist "data\history" mkdir "data\history"
if not exist "data\uploads" mkdir "data\uploads"

echo.
echo ===================================================
echo  Install complete!
echo  Run 02_run_dashboard.bat to start the dashboard.
echo ===================================================
pause
