@echo off
cd C:\Users\RemoteAccess\Documents\modular_software
py .\loading_performance\loading.py
:loop
timeout 3600
py .\loading_performance\loading.py
goto loop