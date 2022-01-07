chcp 65001
@echo off

echo.
echo 生成requirements.txt
echo.

cd %~dp0
cd venv/scripts
call activate.bat
cd ../..
pip freeze >requirements.txt

echo.
echo 完成
echo.
pause