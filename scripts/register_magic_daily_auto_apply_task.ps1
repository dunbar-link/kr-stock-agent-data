# register_magic_daily_auto_apply_task.ps1
# 와바바 마법공식 "가상 장부 무인 자동반영" Task Scheduler 작업 등록/갱신.
# (Phase MF-CANONICAL-UNATTENDED-AUTO-APPLY)
#
# 안전 원칙:
#  - 기존 작업(Signal/Dry Run/Status/Fund Plan Preview/Morning Combined)은 절대 수정·삭제·disable 안 함.
#  - 새 작업 "Wababa Magic Daily Auto Apply" 1개만 추가(있으면 동일 스펙 갱신).
#  - 실행 시각 16:25 KST = 장 마감(15:30) → signal(15:40) → dry-run(15:45) → status(16:05)
#    → Fund Plan Preview(16:20) 완료 이후.
#  - runner 는 1회 실행에 최대 1거래일만 반영하고, 전 게이트 PASS 일 때만 장부에 append 한다.
#  - 실주문 0 · 브로커 API 0 · SMTP 발송 0 · public publish 0 · deploy 0.
#  - python 은 기존 스케줄러와 동일한 안정 전체경로 사용(py/python PATH 의존 금지).
#
# 사용:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_magic_daily_auto_apply_task.ps1
#   (-WhatIf 미리보기 / -RunNow 등록 직후 1회 수동 실행)

param(
    [switch]$RunNow,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$TaskName  = "Wababa Magic Daily Auto Apply"
$PythonExe = "C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$Repo2     = "C:\work\kr-stock-agent-data-new"
$Script    = "$Repo2\scripts\magic_daily_auto_apply.py"
$RunTime   = "16:25"   # Fund Plan Preview(16:20) 이후

if (-not (Test-Path $PythonExe)) { throw "안정 python 경로 없음: $PythonExe" }
if (-not (Test-Path $Script))    { throw "무인 자동반영 runner 없음: $Script" }

# 평일(월~금) 16:25. 휴장일은 runner 가 SKIPPED_NON_TRADING_DAY 로 자체 처리한다.
$action  = New-ScheduledTaskAction -Execute $PythonExe -Argument "`"$Script`"" -WorkingDirectory $Repo2
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday -At $RunTime
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
            -MultipleInstances IgnoreNew
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERNAME" -LogonType Interactive -RunLevel Limited

if ($WhatIf) {
    Write-Output "[WhatIf] 등록 예정 작업: $TaskName"
    Write-Output "  Execute  : $PythonExe"
    Write-Output "  Argument : `"$Script`""
    Write-Output "  WorkDir  : $Repo2"
    Write-Output "  Trigger  : Weekly Mon-Fri $RunTime (Fund Plan Preview 이후)"
    Write-Output "  Settings : ExecutionTimeLimit 30m / MultipleInstances IgnoreNew / StartWhenAvailable"
    Write-Output "  Principal: $env:USERNAME / Interactive / Limited"
    Write-Output "  정책     : 1회 실행 = 최대 1거래일 · 전 게이트 PASS 시에만 장부 append · 실주문 0"
    return
}

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Principal $principal `
    -Description ("마법공식 가상 장부 무인 자동반영(평일 16:25). 1회 실행 최대 1거래일, " +
                  "전 강제 게이트 PASS 시에만 append. 실주문 0 · 브로커 API 0 · SMTP 0 · public publish 0 · deploy 0.") -Force | Out-Null

Write-Output "등록 완료: $TaskName (Weekly Mon-Fri $RunTime)"
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Output ("  NextRunTime   : " + $info.NextRunTime)
Write-Output ("  LastTaskResult: " + $info.LastTaskResult)

if ($RunNow) {
    Write-Output "수동 1회 실행..."
    Start-ScheduledTask -TaskName $TaskName
}
