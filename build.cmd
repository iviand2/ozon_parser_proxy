set PYTHONOPTIMIZE=1 && ^
pyinstaller ^
-n "Ozon parser local V2-09" ^
--console ^
--exclude-module matplotlib ^
--onefile ^
main.py