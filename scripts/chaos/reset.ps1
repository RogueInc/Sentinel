<#
scripts/chaos/reset.ps1
Clears all chaos config back to baseline (0 error rate, 0 latency, no leak).
Run this before AND after every scenario so runs don't bleed into each other.
#>
param(
    [string]$OrdersUrl = "http://localhost:8000"
)

Invoke-RestMethod -Uri "$OrdersUrl/chaos" -Method Post -ContentType "application/json" `
    -Body '{"latency_ms": 0, "error_rate": 0.0, "leak": false}' | Out-Null

Write-Host "chaos reset to baseline" -ForegroundColor Green
