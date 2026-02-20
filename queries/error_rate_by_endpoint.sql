-- Error counts by endpoint and day
-- Useful for identifying error spikes across endpoints
-- Params: $start_dt, $end_dt
SELECT dt, endpoint, COUNT(*) as error_count
FROM telemetry.error_logs
WHERE dt BETWEEN '$start_dt' AND '$end_dt'
GROUP BY dt, endpoint
ORDER BY error_count DESC
LIMIT 50
