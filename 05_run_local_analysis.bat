@echo off
cd /d "%~dp0"

echo ===================================================
echo  챗봇 만족도 분석 - 로컬 전용
echo  분석 완료 후 S3 자동 업로드
echo  접속 주소: http://localhost:8502
echo ===================================================
echo.

if not exist ".venv\Scripts\activate.bat" (
    echo [ERROR] 가상환경이 없습니다. 01_install.bat 를 먼저 실행하세요.
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat

echo 브라우저에서 http://localhost:8502 로 접속하세요.
echo 종료하려면 이 창에서 Ctrl+C 를 누르세요.
echo.

streamlit run local_app.py --server.address localhost --server.port 8502 --browser.gatherUsageStats false --server.headless false

pause
