@echo off
:loop
python stock.py
timeout /t 10 /nobreak > nul
goto :loop