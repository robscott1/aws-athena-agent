-- Count affected accounts, users, and total failures for an endpoint incident
-- Params: $dt, $endpoint, $status_code, $start_time, $end_time
SELECT
  COUNT(DISTINCT account_id) as affected_accounts,
  COUNT(*) as total_failures,
  COUNT(DISTINCT user_id) as affected_users
FROM telemetry.api_requests
WHERE dt = '$dt'
  AND endpoint = '$endpoint'
  AND status_code = $status_code
  AND timestamp >= '$start_time'
  AND timestamp < '$end_time'
