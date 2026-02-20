# Triage Exercise Prompts

Synthetic telemetry data across 5 tables: `accounts`, `users`, `sessions`, `api_requests`, `error_logs`.
Date range: 2026-01-13 to 2026-01-15.

---

## 1. Failed Report Exports

Multiple customers are reporting that report exports are failing. The issue appears to have started on the afternoon of Jan 14. Identify the scope, affected endpoint, and time window.

## 2. Drop in Enterprise Account Activity

The usage dashboard is showing a significant drop in API activity for Company 42 (enterprise) starting Jan 15. Investigate what happened and whether this is account-wide or limited to specific users.

## 3. Slow API Responses

Company 3 (enterprise) has opened a P1 ticket reporting that API response times have degraded significantly. They say it was fine earlier in the week. Confirm the issue, determine when it started, and assess the severity.

## 4. Unexpected Activity on Churned Account

An internal alert fired showing API activity on Company 7, which is marked as churned. Investigate what activity occurred, when, and by whom.

## 5. Rate Limiting Ineffective

A free-tier account appears to be significantly exceeding its monthly request limit of 1,000. Determine which account, how they are bypassing the rate limit, and the scale of the overage.