@echo off
cd /d "C:\Users\Administrator\Desktop\Telegram listener"
for /f "tokens=2 delims=," %%A in ('tasklist /v /fo csv ^| findstr /I "tg_listener_local_bridge.py"') do taskkill /PID %%~A /F >nul 2>&1
timeout /t 1 /nobreak >nul
call venv\Scripts\python.exe tg_listener_local_bridge.py
