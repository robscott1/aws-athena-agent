-- Error breakdown by minute for a specific endpoint on a given day
-- Useful for pinpointing the exact time window of an incident
-- Params: $dt, $endpoint
SELECT
  SUBSTR(timestamp, 12, 5) as time_hhmm,
  error_type,
  message,
  COUNT(*) as count
FROM telemetry.error_logs
WHERE dt = '$dt'
  AND endpoint = '$endpoint'
GROUP BY SUBSTR(timestamp, 12, 5), error_type, message
ORDER BY time_hhmm
LIMIT 50
