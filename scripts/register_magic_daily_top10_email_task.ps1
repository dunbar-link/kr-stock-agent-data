# register_magic_daily_top10_email_task.ps1
# 와바바 마법공식 "일일 top10 매수검토 이메일 미리보기" Task Scheduler 작업 등록/갱신.
# (Phase MF-DAILY-TOP10-EMAIL-PREVIEW-SCHEDULER)
#
# 안전 원칙:
#  - 기존 작업(Wababa Magic Daily Signal/Dry Run/Status/Observe/Morning Combined)은 절대 수정/삭제/disable 안 함.
#  - 새 작업 "Wababa Magic Daily Top10 Email Preview" 1개만 추가(있으면 동일 스펙 갱신).
#  - 실행 스크립트는 미리보기 텍스트 생성뿐. 실제 이메일 발송 0(SMTP/Gmail API 코드 자체 없음).
#    apply/publish/commit/push/deploy/실주문 없음.
#  - python 은 기존 스케줄러와 동일한 안정 전체경로 사용(py/python PATH 의존 금지).
#  - 실행 시각 16:20 KST = 장 마감(15:30) + signal(15:40)/dry-run(15:45)/status(16:05) 완료 이후.
#
# 사용:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_magic_daily_top10_email_task.ps1
#   (-WhatIf 미리보기 / -RunNow 등록 직후 1회 수동 실행)

param(
    [switch]$RunNow,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$TaskName  = "Wababa Magic Daily Top10 Email Preview"
$PythonExe = "C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$Repo2     = "C:\work\kr-stock-agent-data-new"
$Script    = "$Repo2\scripts\magic_daily_top10_email.py"
$RunTime   = "16:20"   # 장 마감 후, 기존 signal/dry-run/status 예약 완료 이후

if (-not (Test-Path $PythonExe)) { throw "안정 python 경로 없음: $PythonExe" }
if (-not (Test-Path $Script))    { throw "top10 이메일 미리보기 스크립트 없음: $Script" }

# 평일(월~금) 16:20 실행. 스크립트가 휴장일은 SELF_SKIPPED_NON_TRADING_DAY 로 자체 처리한다.
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
    Write-Output "  Trigger  : Weekly Mon-Fri $RunTime (장 마감 후)"
    Write-Output "  Settings : ExecutionTimeLimit 30m / MultipleInstances IgnoreNew / StartWhenAvailable"
    Write-Output "  Principal: $env:USERNAME / Interactive / Limited"
    return
}

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Principal $principal `
    -Description "마법공식 일일 top10 매수검토 이메일 미리보기 생성(평일 16:20). 실제 이메일 발송 0 · 실주문 0 · canonical/public write 0." -Force | Out-Null

Write-Output "등록 완료: $TaskName (Weekly Mon-Fri $RunTime)"
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Output ("  NextRunTime   : " + $info.NextRunTime)
Write-Output ("  LastTaskResult: " + $info.LastTaskResult)

if ($RunNow) {
    Write-Output "수동 1회 실행..."
    Start-ScheduledTask -TaskName $TaskName
}
