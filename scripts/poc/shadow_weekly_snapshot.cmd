@echo off
REM Wababa TTM Shadow Weekly Snapshot wrapper (Phase MF-TTM-SHADOW-FIRST-SNAPSHOT-AND-WEEKLY-AUTOMATION)
REM read-only 종가 스냅샷 1회. 실주문 0. public/canonical write 0. 리밸런싱 0.
REM exit: 0=성공/중복, 2=가격누락(WARNING), 3=거래일 판정 실패(WAIT), 9=경로/lock 오류
REM 동시실행 방지: 단순 lock + Task Scheduler MultipleInstances=IgnoreNew. hang 은 Task ExecutionTimeLimit(30분)로 종료.

set "PYEXE=C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
set "SCRIPT=C:\work\kr-stock-agent-data-new\scripts\poc\shadow_portfolio.py"
set "LOCK=C:\work\kr-stock-agent-data-new\_cache\ttm-poc-output\shadow-snapshots\.run.lock"
set "PYTHONUTF8=1"

if not exist "%PYEXE%" exit /b 9
if not exist "%SCRIPT%" exit /b 9
if exist "%LOCK%" exit /b 0
echo running > "%LOCK%"

"%PYEXE%" -X utf8 "%SCRIPT%" --real --strategy both
set "RC=%ERRORLEVEL%"

if exist "%LOCK%" del /f /q "%LOCK%"
exit /b %RC%
