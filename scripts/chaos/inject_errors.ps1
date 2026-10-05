<#
scripts/chaos/inject_errors.ps1
Scenario: error storm. Sets a sustained 5xx rate on `orders` and generates
enough traffic to actually cross HighErrorRate's 5%-over-5-minutes window,
then leaves it running for DurationSeconds so the alert has time to fire
and stay firing long enough for a worker to pick it up.

Usage:
  .\scripts\chaos\inject_errors.ps1 -ErrorRate 0.3 -DurationSeconds 180
#>
param(
    [string]$OrdersUrl = "http://localhost:8000",
    [double]$ErrorRate = 0.3,
    [int]$DurationSeconds = 180,
    [int]$RequestsPerSecond = 5
)

Write-Host "injecting error_rate=$ErrorRate into orders for ${DurationSeconds}s..." -ForegroundColor Yellow
Invoke-RestMethod -Uri "$OrdersUrl/chaos" -Method Post -ContentType "application/json" `
    -Body (@{ error_rate = $ErrorRate } | ConvertTo-Json) | Out-Null

$endTime = (Get-Date).AddSeconds($DurationSeconds)
$sleepMs = [int](1000 / $RequestsPerSecond)
$count = 0

while ((Get-Date) -lt $endTime) {
    try { Invoke-RestMethod -Uri "$OrdersUrl/orders/$count" -TimeoutSec 5 | Out-Null } catch {}
    $count++
    Start-Sleep -Milliseconds $sleepMs
}

Write-Host "sent $count requests. Alert should be firing in Prometheus now." -ForegroundColor Green
Write-Host "Check: http://localhost:9090/alerts" -ForegroundColor Cyan
Write-Host "Run .\scripts\chaos\reset.ps1 once you're done watching it triage." -ForegroundColor Cyan
