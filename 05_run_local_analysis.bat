@echo off
cd /d "%~dp0"

echo ===================================================
echo  Chatbot Satisfaction - Local Analysis
echo  Auto upload to S3 after analysis
echo  URL: http://localhost:8502
echo ===================================================
echo.

if not exist ".venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found.
    echo         Please run 01_install.bat first.
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat

echo Open browser: http://localhost:8502
echo Press Ctrl+C to stop.
echo.

streamlit run local_app.py --server.address localhost --server.port 8502 --browser.gatherUsageStats false --server.headless false

pause
