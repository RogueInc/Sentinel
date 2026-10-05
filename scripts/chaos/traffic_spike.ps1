<#
scripts/chaos/traffic_spike.ps1
Scenario: throughput surge, no errors, no latency change -- tests whether
the agent avoids over-diagnosing. A healthy spike (e.g. "marketing
campaign") should NOT get an aggressive remediation action; propose_action
should favor no_action or page_secondary over anything destructive.

Usage:
  .\scripts\chaos\traffic_spike.ps1 -Multiplier 10 -DurationSeconds 300
#>
param(
    [string]$OrdersUrl = "http://localhost:8000",
    [int]$Multiplier = 10,
    [int]$DurationSeconds = 300,
    [int]$BaselineRps = 2
)

$targetRps = $BaselineRps * $Multiplier
$sleepMs = [Math]::Max(1, [int](1000 / $targetRps))
$endTime = (Get-Date).AddSeconds($DurationSeconds)
$count = 0

Write-Host "driving ~$targetRps req/s at orders for ${DurationSeconds}s (baseline chaos left untouched)..." -ForegroundColor Yellow

# Fire requests in parallel jobs so we can actually reach higher RPS than
# a single sequential loop allows.
$jobs = 1..4 | ForEach-Object {
    Start-Job -ScriptBlock {
        param($url, $end, $sleepMs)
        $c = 0
        while ((Get-Date) -lt $end) {
            try { Invoke-RestMethod -Uri "$url/orders/$c" -TimeoutSec 5 | Out-Null } catch {}
            $c++
            Start-Sleep -Milliseconds $sleepMs
        }
        return $c
    } -ArgumentList $OrdersUrl, $endTime, ($sleepMs * 4)
}

$jobs | Wait-Job | Out-Null
$total = ($jobs | Receive-Job | Measure-Object -Sum).Sum
$jobs | Remove-Job

Write-Host "sent ~$total requests across 4 parallel workers." -ForegroundColor Green
Write-Host "Check throughput in Prometheus: http://localhost:9090/graph?g0.expr=sum(rate(http_requests_total%7Bservice%3D%22orders%22%7D%5B5m%5D))" -ForegroundColor Cyan
