chcp 65001
@echo off

cd %~dp0
cd ..
call ".\venv\Scripts\activate.bat"

python ".\RtdMonitor\main.py"

pause