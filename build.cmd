set PYTHONOPTIMIZE=0 && ^
pyinstaller ^
-n "Ozon parser local V2-16" ^
--console ^
--exclude-module matplotlib ^
--collect-all hyper ^
--onefile ^
main.py