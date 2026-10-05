<#
scripts/chaos/inject_latency.ps1
Scenario: latency spike, no errors. Tests whether the agent correctly
distinguishes "slow but not failing" from "erroring" -- error_rate golden
signal should stay flat while latency_p95 climbs, and the LatencyP95High
alert (for: 5m) should fire instead of HighErrorRate.

Usage:
  .\scripts\chaos\inject_latency.ps1 -LatencyMs 1500 -DurationSeconds 360
#>
param(
    [string]$OrdersUrl = "http://localhost:8000",
    [int]$LatencyMs = 1500,
    [int]$DurationSeconds = 360,
    [int]$RequestsPerSecond = 3
)

Write-Host "injecting latency_ms=$LatencyMs into orders for ${DurationSeconds}s..." -ForegroundColor Yellow
Invoke-RestMethod -Uri "$OrdersUrl/chaos" -Method Post -ContentType "application/json" `
    -Body (@{ latency_ms = $LatencyMs } | ConvertTo-Json) | Out-Null

$endTime = (Get-Date).AddSeconds($DurationSeconds)
$sleepMs = [int](1000 / $RequestsPerSecond)
$count = 0

while ((Get-Date) -lt $endTime) {
    try { Invoke-RestMethod -Uri "$OrdersUrl/orders/$count" -TimeoutSec 10 | Out-Null } catch {}
    $count++
    Start-Sleep -Milliseconds $sleepMs
}

Write-Host "sent $count requests (each ~${LatencyMs}ms). LatencyP95High needs 5min sustained -- keep watching." -ForegroundColor Green
Write-Host "Check: http://localhost:9090/alerts" -ForegroundColor Cyan
