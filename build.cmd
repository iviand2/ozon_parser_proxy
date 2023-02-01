set PYTHONOPTIMIZE=1 && ^
pyinstaller ^
-n "Ozon parser local V2-04" ^
--console ^
--exclude-module matplotlib ^
--onefile ^
main.py