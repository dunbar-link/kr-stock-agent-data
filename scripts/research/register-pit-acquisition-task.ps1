<#
.SYNOPSIS
    WABABA PIT Acquisition Resume — 30분마다 자동 재개 예약 등록 (Founder 수동 셔틀 제거).

.DESCRIPTION
    build_pit_snapshots.py 를 30분 간격으로 실행한다. 매 실행은 스스로 판단하고 끝낸다.

      1) preflight read-only 1콜로 KRX 차단 여부 확인
         - 차단(또는 판정불가)이면 **추가 호출 0건으로 즉시 종료**(exit 3). 재시도 루프 없음.
      2) 차단이 풀렸으면 checkpoint 를 읽어 **아직 안 받은 달만** 보수적 batch 로 수집
         - batch 크기 = 시간예산(기본 20분) × 분당안전콜(6) ÷ 월당콜(4) = 30개월
         - 월 처리 후 pacing(40초) + 지터. 차단 유발 속도(0.15초)의 260배 느리다.
      3) 매 건 원자적 checkpoint 저장 → 중간에 죽어도 진행분 보존, 다음 실행이 이어받음
      4) batch 상한 도달 시 정상 종료 → 30분 뒤 다음 batch

    2007-01~2026-08 (236개월) 기준 약 8회 실행(≈4시간)이면 수집이 끝난다.
    끝나면 매 실행이 "remaining=0"으로 즉시 종료되므로 부하가 없다(자동 무해화).

    ★ 이 스크립트는 **예약작업을 새로 만든다** = 전역 정책상 Founder 승인 게이트다.
      Claude 는 이 파일을 만들기만 하고 실행하지 않았다. 승인 시 1회만 실행하면 된다.
      해제: Unregister-ScheduledTask -TaskName "Wababa PIT Acquisition Resume" -Confirm:$false

    안전: read-only 시장데이터 조회 전용. canonical 장부·public JSON·홈페이지·운영 scheduler 미변경.
          실주문 0 · 브로커 API 0 · 유료 API 0 · 외부 발송 0.
          기존 와바바 운영 예약(Auto Apply/Publish 등)은 건드리지 않는다.

.NOTES
    PowerShell 5.1 호환. 같은 이름 예약이 있으면 덮어쓰지 않는다(-Force 없음).
#>
[CmdletBinding()]
param([switch]$WhatIfOnly)

$ErrorActionPreference = "Stop"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

$taskName  = "Wababa PIT Acquisition Resume"
$exe       = "C:\Users\duria\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$arguments = "scripts\research\build_pit_snapshots.py --start 2007-01 --end 2026-08 --minutes-budget 20"
$workDir   = "C:\work\kr-stock-agent-data-new"

Write-Host "=== Wababa PIT Acquisition Resume 등록 ===" -ForegroundColor Cyan
Write-Host ("작업명  : {0}" -f $taskName)
Write-Host ("실행    : {0} {1}" -f $exe, $arguments)
Write-Host ("시작위치: {0}" -f $workDir)
Write-Host ("스케줄  : 30분마다 (차단 상태면 1콜만 쓰고 즉시 종료)")
Write-Host ""

if (-not (Test-Path -LiteralPath $exe)) { throw "python 실행 파일 없음: $exe" }
if (-not (Test-Path -LiteralPath (Join-Path $workDir "scripts\research\build_pit_snapshots.py"))) {
  throw "수집 스크립트 없음 — 저장소 경로 확인 필요"
}

if ($WhatIfOnly) {
  Write-Host "[WHATIF] 등록하지 않고 종료(설정만 표시)" -ForegroundColor Yellow
  return
}

$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
  Write-Host "[SKIP] 같은 이름 예약이 이미 존재 — 덮어쓰지 않고 상태만 보고" -ForegroundColor Yellow
  $existing | Select-Object TaskName, State | Format-Table -AutoSize
  return
}

$action  = New-ScheduledTaskAction -Execute $exe -Argument $arguments -WorkingDirectory $workDir
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) `
             -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Days 2)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
             -ExecutionTimeLimit (New-TimeSpan -Minutes 45) -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
  -Settings $settings -Description "WABABA-PIT-DATA-ACQUISITION-RESUME-R2 — PIT 역사데이터 저속 수집 자동 재개(read-only)" | Out-Null

Write-Host "[OK] 등록 완료" -ForegroundColor Green
Get-ScheduledTask -TaskName $taskName | Select-Object TaskName, State | Format-Table -AutoSize
Write-Host ""
Write-Host "진행 확인 : python scripts\research\build_pit_snapshots.py --status"
Write-Host "수집 완료 후 해제 : Unregister-ScheduledTask -TaskName `"$taskName`" -Confirm:`$false"
