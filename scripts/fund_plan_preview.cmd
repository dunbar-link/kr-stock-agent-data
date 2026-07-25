@echo off
REM Wababa Magic Daily Fund Plan Preview wrapper
REM (Phase MF-DAILY-TOP10-AUTONOMOUS-FUND-REPORTING-CONTRACT)
REM 마법공식 펀드 일일 실행계획 + 메일 라우팅 미리보기 생성.
REM read-only / TEMP-only. 실제 이메일 발송 0(SMTP/API 호출 없음). 실주문 0. canonical/public write 0.
REM 정상(PASS)은 운영계정 보관 대상으로만 표시하고 대장 개인메일 수신자는 0명이다.
REM WARNING/BLOCKED 만 대장 개인메일 알림 대상으로 표시한다(표시일 뿐, 실제 발송은 하지 않는다).
REM exit: 0=정상/경고/휴장일 self-skip/이미발송기록, 2=BLOCKED(apply 대상 아님), 9=경로 오류

set "PYEXE=C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
set "SCRIPT=C:\work\kr-stock-agent-data-new\scripts\magic_daily_fund_plan.py"
set "PYTHONUTF8=1"

if not exist "%PYEXE%" exit /b 9
if not exist "%SCRIPT%" exit /b 9

"%PYEXE%" -X utf8 "%SCRIPT%"
exit /b %ERRORLEVEL%
