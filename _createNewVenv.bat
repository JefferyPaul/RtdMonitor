chcp 65001
@echo off

echo.
echo 创建新venv
echo.

cd %~dp0
python -m venv venv

cd %~dp0
cd venv/scripts
call activate.bat

cd ../..
pip install -r requirements.txt


echo.
echo 创建完成
echo.
pause
