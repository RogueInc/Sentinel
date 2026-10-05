<#
scripts/chaos/report.ps1
Pulls the numbers the Phase 7 results table wants: recent incidents,
their status, mttr, and how many alerts collapsed into each one (the
dedupe ratio). Run this after a batch of chaos scenarios.
#>
docker compose exec postgres psql -U atlas -d atlas -c "
SELECT
    service,
    severity,
    status,
    alert_count,
    mttr_seconds,
    title,
    first_seen
FROM incidents
ORDER BY first_seen DESC
LIMIT 15;
"

Write-Host ""
Write-Host "Dedupe ratio (alerts collapsed per incident, last 15):" -ForegroundColor Cyan
docker compose exec postgres psql -U atlas -d atlas -c "
SELECT
    ROUND(AVG(alert_count), 2) AS avg_alerts_per_incident,
    SUM(alert_count) AS total_alerts,
    COUNT(*) AS total_incidents
FROM (SELECT alert_count FROM incidents ORDER BY first_seen DESC LIMIT 15) sub;
"
