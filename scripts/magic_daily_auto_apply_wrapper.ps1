# magic_daily_auto_apply_wrapper.ps1
# 와바바 마법공식 가상 장부 무인 자동반영 wrapper (Phase MF-CANONICAL-UNATTENDED-AUTO-APPLY)
#
# 1회 실행 = 최대 1거래일. 전 강제 게이트 PASS 일 때만 기존 안전 apply 경로로 가상 장부에 append.
# 실주문 0 · 브로커 API 0 · SMTP 실제 발송 0 · public publish 0 · deploy 0.
# 휴장일 self-skip · 최신 장부면 NO_ACTION · 게이트 실패면 canonical write 0.
#
# exit: 0 = PASS(APPLIED_AUTOMATICALLY / NO_ACTION_ALREADY_CURRENT / SKIPPED_NON_TRADING_DAY)
#       2 = BLOCKED(게이트 실패 / lock busy / canonical 불일치) — 장부 변경 없음
#       9 = 경로 오류

$ErrorActionPreference = "Continue"

$PyExe  = "C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$Repo2  = "C:\work\kr-stock-agent-data-new"
$Script = "$Repo2\scripts\magic_daily_auto_apply.py"

if (-not (Test-Path -LiteralPath $PyExe))  { Write-Host "BLOCKED: python 경로 없음: $PyExe";  exit 9 }
if (-not (Test-Path -LiteralPath $Script)) { Write-Host "BLOCKED: runner 없음: $Script";      exit 9 }

$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

Set-Location -LiteralPath $Repo2
& $PyExe -X utf8 $Script
exit $LASTEXITCODE
