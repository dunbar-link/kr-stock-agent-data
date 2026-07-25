@echo off
REM Wababa Magic Daily Top10 Email Preview wrapper (Phase MF-DAILY-TOP10-EMAIL-PIPELINE-ROOT-CAUSE-AND-RECOVERY)
REM read-only 미리보기 생성. 실제 이메일 발송 0(SMTP/API 호출 없음). 실주문 0. canonical/public write 0.
REM 등록 전 대장 승인 필요(이번 Phase는 스크립트/wrapper만 준비, Task Scheduler 등록은 하지 않음).
REM exit: 0=self-skip/발송준비완료/이미발송기록, 2=BLOCKED(신호없음/유니버스stale/top10불완전), 9=경로 오류

set "PYEXE=C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
set "SCRIPT=C:\work\kr-stock-agent-data-new\scripts\magic_daily_top10_email.py"
set "PYTHONUTF8=1"

if not exist "%PYEXE%" exit /b 9
if not exist "%SCRIPT%" exit /b 9

"%PYEXE%" -X utf8 "%SCRIPT%"
exit /b %ERRORLEVEL%
