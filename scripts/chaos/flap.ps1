<#
scripts/chaos/flap.ps1
Scenario: flapping alert. Oscillates error_rate above/below the 5%
threshold repeatedly -- tests classify()'s flap suppression (alert_count
> 5 on the same fingerprint should mark it a duplicate and skip a fresh
triage run, per sentinel/graph/nodes/classify.py).

Usage:
  .\scripts\chaos\flap.ps1 -Cycles 8
#>
param(
    [string]$OrdersUrl = "http://localhost:8000",
    [string]$ApiUrl = "http://localhost:8080",
    [int]$Cycles = 8,
    [int]$OnSeconds = 20,
    [int]$OffSeconds = 20
)

$body = @{
    alerts = @(@{
        status = "firing"
        labels = @{ alertname = "HighErrorRate"; service = "orders"; severity = "warning" }
        annotations = @{ summary = "flap test" }
    })
} | ConvertTo-Json -Depth 5

for ($i = 1; $i -le $Cycles; $i++) {
    Write-Host "cycle $i/$Cycles : errors ON" -ForegroundColor Yellow
    Invoke-RestMethod -Uri "$OrdersUrl/chaos" -Method Post -ContentType "application/json" `
        -Body '{"error_rate": 0.3}' | Out-Null

    1..15 | ForEach-Object { try { Invoke-RestMethod -Uri "$OrdersUrl/orders/$_" -TimeoutSec 5 | Out-Null } catch {} }
    try { Invoke-RestMethod -Uri "$ApiUrl/v1/alerts" -Method Post -ContentType "application/json" -Body $body | Out-Null } catch {}

    Start-Sleep -Seconds $OnSeconds

    Write-Host "cycle $i/$Cycles : errors OFF" -ForegroundColor Green
    Invoke-RestMethod -Uri "$OrdersUrl/chaos" -Method Post -ContentType "application/json" `
        -Body '{"error_rate": 0.0}' | Out-Null

    Start-Sleep -Seconds $OffSeconds
}

Write-Host "flap sequence done. Check alert_count on the open incident -- should be > $Cycles." -ForegroundColor Cyan
Write-Host 'docker compose exec postgres psql -U atlas -d atlas -c "SELECT id, alert_count, status FROM incidents ORDER BY last_seen DESC LIMIT 3;"' -ForegroundColor Cyan
