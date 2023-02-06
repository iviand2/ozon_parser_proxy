set PYTHONOPTIMIZE=1 && ^
cd
pyinstaller ^
-n "Ozon parser local V2-10" ^
--console ^
--exclude-module matplotlib ^
--onefile ^
main.py