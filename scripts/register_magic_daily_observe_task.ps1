# register_magic_daily_observe_task.ps1
# 와바바 마법공식 "일일 read-only 관찰 보고" Task Scheduler 작업 등록/갱신(read-only 관찰 자동화).
#
# 안전 원칙:
#  - 기존 Wababa Magic Daily Signal/Dry Run/Status 작업은 절대 수정/삭제하지 않는다.
#  - 새 작업 "Wababa Magic Daily Observe Report" 1개만 추가(있으면 동일 스펙으로 갱신).
#  - 이 작업이 실행하는 스크립트는 read-only 관찰뿐. apply/publish/commit/push/deploy 없음.
#  - python 은 기존 스케줄러와 동일한 안정 전체경로를 사용(py/python PATH 의존 금지).
#
# 사용:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_magic_daily_observe_task.ps1
#   (-WhatIf 로 미리보기 / -RunNow 로 등록 직후 1회 수동 실행)

param(
    [switch]$RunNow,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$TaskName   = "Wababa Magic Daily Observe Report"
$PythonExe  = "C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$Repo2      = "C:\work\kr-stock-agent-data-new"
$Script     = "$Repo2\scripts\magic_daily_observe_report.py"
$RunTime    = "16:10"   # 기존 status(16:05) 이후, 산출물 생성 여유

if (-not (Test-Path $PythonExe)) { throw "안정 python 경로 없음: $PythonExe" }
if (-not (Test-Path $Script))    { throw "관찰 스크립트 없음: $Script" }

# 매일 16:10 실행(스크립트가 비거래일/장마감 전을 WAIT로 처리하므로 Daily 트리거로 충분)
$action  = New-ScheduledTaskAction -Execute $PythonExe -Argument "`"$Script`"" -WorkingDirectory $Repo2
$trigger = New-ScheduledTaskTrigger -Daily -At $RunTime
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
            -MultipleInstances IgnoreNew

if ($WhatIf) {
    Write-Output "[WhatIf] 등록 예정 작업: $TaskName"
    Write-Output "  Execute : $PythonExe"
    Write-Output "  Argument: `"$Script`""
    Write-Output "  WorkDir : $Repo2"
    Write-Output "  Trigger : Daily $RunTime"
    return
}

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Description "마법공식 일일 read-only 관찰 보고 생성(16:10). apply/publish/push/deploy 없음." -Force | Out-Null

Write-Output "등록 완료: $TaskName (Daily $RunTime)"
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Output ("  NextRunTime  : " + $info.NextRunTime)
Write-Output ("  LastTaskResult: " + $info.LastTaskResult)

if ($RunNow) {
    Write-Output "수동 1회 실행..."
    Start-ScheduledTask -TaskName $TaskName
}
