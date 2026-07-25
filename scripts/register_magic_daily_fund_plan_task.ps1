# register_magic_daily_fund_plan_task.ps1
# 와바바 마법공식 "펀드 일일 실행계획 미리보기" Task Scheduler 작업 등록/갱신.
# (Phase MF-DAILY-TOP10-AUTONOMOUS-FUND-REPORTING-CONTRACT)
#
# 안전 원칙:
#  - 기존 작업(Wababa Magic Daily Signal/Dry Run/Status/Observe/Morning Combined)은 절대 수정/삭제/disable 안 함.
#  - 동일 역할 작업은 1개만 유지한다. 구명칭("Wababa Magic Daily Top10 Email Preview")이 남아 있으면
#    중복 방지를 위해 제거하고 신명칭 1개로 대체한다(중복 Task 신규 생성 금지).
#  - 실행 스크립트는 실행계획·라우팅 미리보기 텍스트 생성뿐. 실제 이메일 발송 0(SMTP/Gmail API 코드 자체 없음).
#    apply/publish/commit/push/deploy/실주문 없음.
#  - python 은 기존 스케줄러와 동일한 안정 전체경로 사용(py/python PATH 의존 금지).
#  - 실행 시각 16:20 KST = 장 마감(15:30) + signal(15:40)/dry-run(15:45)/status(16:05) 완료 이후.
#
# 사용:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_magic_daily_fund_plan_task.ps1
#   (-WhatIf 미리보기 / -RunNow 등록 직후 1회 수동 실행)

param(
    [switch]$RunNow,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$TaskName  = "Wababa Magic Daily Fund Plan Preview"
$LegacyTaskName = "Wababa Magic Daily Top10 Email Preview"   # 동일 역할 구명칭(있으면 대체)
$PythonExe = "C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$Repo2     = "C:\work\kr-stock-agent-data-new"
$Script    = "$Repo2\scripts\magic_daily_fund_plan.py"
$RunTime   = "16:20"   # 장 마감 후, 기존 signal/dry-run/status 예약 완료 이후

if (-not (Test-Path $PythonExe)) { throw "안정 python 경로 없음: $PythonExe" }
if (-not (Test-Path $Script))    { throw "펀드 실행계획 미리보기 스크립트 없음: $Script" }

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
    Write-Output "  Legacy   : '$LegacyTaskName' 존재 시 제거(동일 역할 1개만 유지)"
    return
}

# 동일 역할 구명칭 Task 가 남아 있으면 제거한다(중복 예약 방지).
$legacy = Get-ScheduledTask -TaskName $LegacyTaskName -ErrorAction SilentlyContinue
if ($legacy) {
    Unregister-ScheduledTask -TaskName $LegacyTaskName -Confirm:$false
    Write-Output "구명칭 작업 제거: $LegacyTaskName (동일 역할 1개만 유지)"
}

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Principal $principal `
    -Description "마법공식 펀드 일일 실행계획·라우팅 미리보기 생성(평일 16:20). 정상은 운영계정 보관 대상, WARNING/BLOCKED만 대장 알림 대상. 실제 이메일 발송 0 · 실주문 0 · canonical/public write 0." -Force | Out-Null

Write-Output "등록 완료: $TaskName (Weekly Mon-Fri $RunTime)"
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Output ("  NextRunTime   : " + $info.NextRunTime)
Write-Output ("  LastTaskResult: " + $info.LastTaskResult)

if ($RunNow) {
    Write-Output "수동 1회 실행..."
    Start-ScheduledTask -TaskName $TaskName
}
