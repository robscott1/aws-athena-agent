#!/usr/bin/env python3
"""
Generate synthetic SaaS telemetry data for an Athena triage exercise.

Produces ~5000 rows across 5 partitioned parquet tables with 5 planted
investigation scenarios. Run with: poetry run python sample_data/generate.py

Output: sample_data/data/<table>/dt=<date>/*.parquet

Flags:
  1. Bad deployment   - /api/v1/reports/export returns 500s for ~30 min on Jan 14
  2. Silent dropout   - Enterprise acct_042 goes completely silent on Jan 15
  3. Latency spike    - Enterprise acct_003 response times jump 5-10x on Jan 15
  4. Insider exfil    - usr_034 bulk exports from churned acct_007 at 2 AM
  5. Rate limit bypass - 5 puppet users on free-tier acct_019 share one IP
"""

import os
import random
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42
random.seed(SEED)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
DATES = ["2026-01-13", "2026-01-14", "2026-01-15"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ts(date_str: str, hour: int = 0, minute: int = 0, second: int = 0) -> str:
    """Return an ISO-8601 timestamp string for the given date and time."""
    dt = datetime.fromisoformat(date_str)
    dt = dt.replace(hour=hour, minute=minute, second=second)
    return dt.isoformat() + "Z"


def rand_ts(date_str: str) -> str:
    """Random timestamp within a given date (business-ish hours weighted)."""
    hour = random.choices(range(24), weights=[1]*6 + [3]*12 + [2]*6, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return ts(date_str, hour, minute, second)


def make_id(prefix: str, n: int) -> str:
    return f"{prefix}_{n:03d}"


# ---------------------------------------------------------------------------
# Static reference data
# ---------------------------------------------------------------------------

PLANS = ["free", "starter", "pro", "enterprise"]
PLAN_LIMITS = {"free": 1000, "starter": 5000, "pro": 25000, "enterprise": 100000}
COUNTRIES = ["US", "GB", "DE", "CA", "AU", "FR", "JP", "BR", "IN", "SG"]
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "okhttp/4.12.0",
    "python-requests/2.31.0",
]
ENDPOINTS = [
    ("GET", "/api/v1/users"),
    ("GET", "/api/v1/accounts"),
    ("POST", "/api/v1/users"),
    ("PUT", "/api/v1/users/{id}"),
    ("GET", "/api/v1/reports"),
    ("POST", "/api/v1/reports/export"),
    ("GET", "/api/v1/billing"),
    ("POST", "/auth/login"),
    ("POST", "/auth/logout"),
    ("GET", "/api/v1/settings"),
    ("PUT", "/api/v1/settings"),
    ("GET", "/api/v1/integrations"),
    ("POST", "/api/v1/integrations"),
    ("DELETE", "/api/v1/integrations/{id}"),
    ("GET", "/api/v1/audit-log"),
]

# ---------------------------------------------------------------------------
# Generate accounts
# ---------------------------------------------------------------------------

def generate_accounts() -> list[dict]:
    accounts = []
    for i in range(1, 101):
        acct_id = make_id("acct", i)
        plan = random.choice(PLANS)
        status = "active"

        # Flag 3 (latency spike): acct_003 is a large enterprise account
        if acct_id == "acct_003":
            plan = "enterprise"

        # Flag 4 (insider exfil): acct_007 is churned
        elif acct_id == "acct_007":
            plan = "pro"
            status = "churned"

        # Flag 5 (rate limit bypass): acct_019 is free tier
        elif acct_id == "acct_019":
            plan = "free"

        # Flag 2 (silent dropout): acct_042 is an active enterprise account
        elif acct_id == "acct_042":
            plan = "enterprise"

        # Some natural churn
        elif random.random() < 0.05:
            status = "churned"

        created = ts("2025-06-01", random.randint(0, 23), random.randint(0, 59))
        accounts.append({
            "account_id": acct_id,
            "name": f"Company {i}",
            "plan": plan,
            "monthly_request_limit": PLAN_LIMITS[plan],
            "status": status,
            "created_at": created,
            "dt": random.choice(DATES),
        })
    return accounts


# ---------------------------------------------------------------------------
# Generate users
# ---------------------------------------------------------------------------

def generate_users(accounts: list[dict]) -> list[dict]:
    users = []
    user_counter = 1

    for acct in accounts:
        acct_id = acct["account_id"]

        # Flag 5: acct_019 gets exactly 5 puppet users created at the same minute
        if acct_id == "acct_019":
            n_users = 5
            puppet_created = ts("2026-01-14", 3, 22, 0)
            for j in range(n_users):
                uid = make_id("usr", user_counter)
                users.append({
                    "user_id": uid,
                    "account_id": acct_id,
                    "email": f"user{user_counter}@company19.com",
                    "role": "member",
                    "status": "active",
                    "created_at": puppet_created,
                    "dt": "2026-01-14",
                })
                user_counter += 1
            continue

        # Normal accounts get 1-6 users
        n_users = random.randint(1, 6)
        for j in range(n_users):
            uid = make_id("usr", user_counter)
            role = "admin" if j == 0 else "member"
            status = "active"

            # Flag 4: usr_034 is admin on acct_007
            if uid == "usr_034":
                role = "admin"

            created = ts("2025-07-01", random.randint(0, 23), random.randint(0, 59))
            users.append({
                "user_id": uid,
                "account_id": acct_id,
                "email": f"user{user_counter}@company{acct_id.split('_')[1]}.com",
                "role": role,
                "status": status,
                "created_at": created,
                "dt": random.choice(DATES),
            })
            user_counter += 1

    return users


# ---------------------------------------------------------------------------
# Generate sessions
# ---------------------------------------------------------------------------

def generate_sessions(users: list[dict]) -> list[dict]:
    sessions = []
    sess_counter = 1

    # Flag 5: puppet users on acct_019 share the same IP and user-agent
    puppet_ip = "203.0.113.77"
    puppet_ua = "python-requests/2.31.0"
    puppet_user_ids = [u["user_id"] for u in users if u["account_id"] == "acct_019"]

    # Flag 2: users on acct_042 for selective session generation
    dropout_user_ids = [u["user_id"] for u in users if u["account_id"] == "acct_042"]

    for user in users:
        uid = user["user_id"]

        # Flag 5: puppet sessions
        if uid in puppet_user_ids:
            for dt in DATES:
                sessions.append({
                    "session_id": make_id("sess", sess_counter),
                    "user_id": uid,
                    "account_id": user["account_id"],
                    "ip_address": puppet_ip,
                    "country": "US",
                    "user_agent": puppet_ua,
                    "started_at": rand_ts(dt),
                    "duration_seconds": random.randint(60, 7200),
                    "dt": dt,
                })
                sess_counter += 1
            continue

        # Flag 4: usr_034 has a session from unusual country on Jan 15
        if uid == "usr_034":
            sessions.append({
                "session_id": make_id("sess", sess_counter),
                "user_id": uid,
                "account_id": user["account_id"],
                "ip_address": "185.220.101.33",
                "country": "RO",
                "user_agent": random.choice(USER_AGENTS[:3]),
                "started_at": ts("2026-01-15", 2, 5, 0),
                "duration_seconds": 3600,
                "dt": "2026-01-15",
            })
            sess_counter += 1

        # Flag 2: acct_042 users only have sessions on Jan 13 and 14 (not 15)
        if uid in dropout_user_ids:
            for dt in ["2026-01-13", "2026-01-14"]:
                sessions.append({
                    "session_id": make_id("sess", sess_counter),
                    "user_id": uid,
                    "account_id": user["account_id"],
                    "ip_address": f"{random.randint(10,199)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
                    "country": "US",
                    "user_agent": random.choice(USER_AGENTS[:3]),
                    "started_at": rand_ts(dt),
                    "duration_seconds": random.randint(300, 7200),
                    "dt": dt,
                })
                sess_counter += 1
            continue

        # Normal sessions: 1-3 per user spread across dates
        n_sess = random.randint(1, 3)
        for _ in range(n_sess):
            dt = random.choice(DATES)
            ip = f"{random.randint(10,199)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            sessions.append({
                "session_id": make_id("sess", sess_counter),
                "user_id": uid,
                "account_id": user["account_id"],
                "ip_address": ip,
                "country": random.choice(COUNTRIES),
                "user_agent": random.choice(USER_AGENTS),
                "started_at": rand_ts(dt),
                "duration_seconds": random.randint(30, 7200),
                "dt": dt,
            })
            sess_counter += 1

    return sessions


# ---------------------------------------------------------------------------
# Generate API requests
# ---------------------------------------------------------------------------

def generate_api_requests(users: list[dict], accounts: list[dict]) -> list[dict]:
    requests = []
    req_counter = 1

    user_acct = {u["user_id"]: u["account_id"] for u in users}
    acct_plan = {a["account_id"]: a["plan"] for a in accounts}
    puppet_user_ids = [u["user_id"] for u in users if u["account_id"] == "acct_019"]
    dropout_user_ids = [u["user_id"] for u in users if u["account_id"] == "acct_042"]

    # --- Normal traffic (~2400 requests) ---
    normal_users = [u for u in users
                    if u["account_id"] not in ("acct_019", "acct_007", "acct_042", "acct_003")]
    for _ in range(2400):
        user = random.choice(normal_users)
        uid = user["user_id"]
        acct_id = user["account_id"]
        dt = random.choice(DATES)
        method, endpoint = random.choice(ENDPOINTS)
        status_code = random.choices([200, 201, 400, 401, 403, 404, 500],
                                     weights=[60, 10, 8, 5, 3, 8, 6], k=1)[0]
        requests.append({
            "request_id": make_id("req", req_counter),
            "account_id": acct_id,
            "user_id": uid,
            "method": method,
            "endpoint": endpoint,
            "status_code": status_code,
            "response_time_ms": random.randint(15, 800),
            "ip_address": f"{random.randint(10,199)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "user_agent": random.choice(USER_AGENTS),
            "timestamp": rand_ts(dt),
            "dt": dt,
        })
        req_counter += 1

    # --- Flag 1: Bad deployment ---
    # On Jan 14 from 14:00-14:30, /api/v1/reports/export returns 500 for everyone.
    # Normal requests to that endpoint outside the window still succeed.
    # Generate ~150 failed requests in the bad window across many accounts.
    all_acct_ids = [a["account_id"] for a in accounts if a["status"] == "active"]
    all_user_map = {}
    for u in users:
        all_user_map.setdefault(u["account_id"], []).append(u["user_id"])

    for i in range(150):
        acct_id = random.choice(all_acct_ids)
        uid = random.choice(all_user_map.get(acct_id, ["usr_001"]))
        minute = random.randint(0, 29)
        second = random.randint(0, 59)
        requests.append({
            "request_id": make_id("req", req_counter),
            "account_id": acct_id,
            "user_id": uid,
            "method": "POST",
            "endpoint": "/api/v1/reports/export",
            "status_code": 500,
            "response_time_ms": random.randint(5000, 15000),
            "ip_address": f"{random.randint(10,199)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "user_agent": random.choice(USER_AGENTS),
            "timestamp": ts("2026-01-14", 14, minute, second),
            "dt": "2026-01-14",
        })
        req_counter += 1

    # --- Flag 2: Silent account dropout ---
    # acct_042 has healthy traffic on Jan 13 and 14, then nothing on Jan 15.
    acct042_users = [u["user_id"] for u in users if u["account_id"] == "acct_042"]
    for dt in ["2026-01-13", "2026-01-14"]:
        for _ in range(random.randint(40, 60)):
            uid = random.choice(acct042_users)
            method, endpoint = random.choice(ENDPOINTS)
            requests.append({
                "request_id": make_id("req", req_counter),
                "account_id": "acct_042",
                "user_id": uid,
                "method": method,
                "endpoint": endpoint,
                "status_code": random.choices([200, 201], weights=[85, 15], k=1)[0],
                "response_time_ms": random.randint(20, 300),
                "ip_address": f"{random.randint(10,199)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
                "user_agent": random.choice(USER_AGENTS[:3]),
                "timestamp": rand_ts(dt),
                "dt": dt,
            })
            req_counter += 1
    # Deliberately NO requests for acct_042 on Jan 15

    # --- Flag 3: Latency degradation ---
    # acct_003 (enterprise) has normal latency on Jan 13-14, but 5-10x on Jan 15.
    acct003_users = [u["user_id"] for u in users if u["account_id"] == "acct_003"]
    for dt in DATES:
        n_reqs = random.randint(40, 60)
        for _ in range(n_reqs):
            uid = random.choice(acct003_users)
            method, endpoint = random.choice(ENDPOINTS)
            # Normal latency on Jan 13-14, degraded on Jan 15
            if dt == "2026-01-15":
                response_time = random.randint(2000, 8000)
            else:
                response_time = random.randint(20, 300)
            requests.append({
                "request_id": make_id("req", req_counter),
                "account_id": "acct_003",
                "user_id": uid,
                "method": method,
                "endpoint": endpoint,
                "status_code": 200,
                "response_time_ms": response_time,
                "ip_address": f"{random.randint(10,199)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
                "user_agent": random.choice(USER_AGENTS[:3]),
                "timestamp": rand_ts(dt),
                "dt": dt,
            })
            req_counter += 1

    # --- Flag 4: Insider exfiltration - usr_034 bulk exports at 2 AM ---
    export_endpoints = [
        ("POST", "/api/v1/reports/export"),
        ("GET", "/api/v1/reports"),
        ("GET", "/api/v1/users"),
        ("GET", "/api/v1/audit-log"),
    ]
    for i in range(35):
        method, endpoint = random.choice(export_endpoints)
        requests.append({
            "request_id": make_id("req", req_counter),
            "account_id": "acct_007",
            "user_id": "usr_034",
            "method": method,
            "endpoint": endpoint,
            "status_code": 200,
            "response_time_ms": random.randint(200, 2000),
            "ip_address": "185.220.101.33",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "timestamp": ts("2026-01-15", 2, random.randint(0, 45), random.randint(0, 59)),
            "dt": "2026-01-15",
        })
        req_counter += 1

    # --- Flag 5: Rate limit bypass - acct_019 puppet users generate ~600 reqs each ---
    for uid in puppet_user_ids:
        for dt in DATES:
            n_reqs = random.randint(180, 220)
            for _ in range(n_reqs):
                method, endpoint = random.choice(ENDPOINTS[:7])  # only data endpoints
                requests.append({
                    "request_id": make_id("req", req_counter),
                    "account_id": "acct_019",
                    "user_id": uid,
                    "method": method,
                    "endpoint": endpoint,
                    "status_code": random.choices([200, 201, 429], weights=[80, 10, 10], k=1)[0],
                    "response_time_ms": random.randint(20, 300),
                    "ip_address": "203.0.113.77",
                    "user_agent": "python-requests/2.31.0",
                    "timestamp": rand_ts(dt),
                    "dt": dt,
                })
                req_counter += 1

    return requests


# ---------------------------------------------------------------------------
# Generate error logs
# ---------------------------------------------------------------------------

def generate_error_logs(api_requests: list[dict]) -> list[dict]:
    errors = []
    err_counter = 1

    # Errors from failed requests
    failed = [r for r in api_requests if r["status_code"] >= 400]
    # Sample ~500 errors from failed requests
    sampled = random.sample(failed, min(500, len(failed)))

    # Also include ALL 500s from the bad deploy window (Flag 1) to make it discoverable
    bad_deploy = [r for r in api_requests
                  if r["status_code"] == 500
                  and r["endpoint"] == "/api/v1/reports/export"
                  and r["dt"] == "2026-01-14"]
    # Merge, deduplicating by request_id
    seen_req_ids = {r["request_id"] for r in sampled}
    for r in bad_deploy:
        if r["request_id"] not in seen_req_ids:
            sampled.append(r)
            seen_req_ids.add(r["request_id"])

    for req in sampled:
        sc = req["status_code"]
        if sc == 401:
            error_type = "AUTH_FAILED"
            message = "Invalid credentials"
        elif sc == 403:
            error_type = "PERMISSION_DENIED"
            message = "Insufficient permissions for this resource"
        elif sc == 404:
            error_type = "NOT_FOUND"
            message = "Resource not found"
        elif sc == 429:
            error_type = "RATE_LIMIT_EXCEEDED"
            message = f"Rate limit exceeded for account {req['account_id']}"
        elif sc == 400:
            error_type = "VALIDATION_ERROR"
            message = "Invalid request parameters"
        elif sc == 500:
            error_type = "INTERNAL_ERROR"
            # Flag 1: bad deploy errors get a specific message
            if req["endpoint"] == "/api/v1/reports/export" and req["dt"] == "2026-01-14":
                message = "NullPointerException in ReportExportService.generateReport()"
            else:
                message = "Unexpected server error"
        else:
            error_type = "INTERNAL_ERROR"
            message = "Unexpected server error"

        errors.append({
            "error_id": make_id("err", err_counter),
            "request_id": req["request_id"],
            "account_id": req["account_id"],
            "user_id": req["user_id"],
            "error_type": error_type,
            "message": message,
            "endpoint": req["endpoint"],
            "ip_address": req["ip_address"],
            "timestamp": req["timestamp"],
            "dt": req["dt"],
        })
        err_counter += 1

    return errors


# ---------------------------------------------------------------------------
# Write partitioned parquet
# ---------------------------------------------------------------------------

def write_partitioned(table_name: str, rows: list[dict]) -> int:
    """Write rows as partitioned parquet under OUTPUT_DIR/table_name/dt=.../."""
    if not rows:
        return 0

    # Group by dt
    by_dt: dict[str, list[dict]] = {}
    for row in rows:
        dt = row["dt"]
        by_dt.setdefault(dt, []).append(row)

    # Infer schema from first row (excluding dt)
    sample = {k: v for k, v in rows[0].items() if k != "dt"}
    fields = []
    for k, v in sample.items():
        if isinstance(v, int):
            fields.append(pa.field(k, pa.int64()))
        elif isinstance(v, float):
            fields.append(pa.field(k, pa.float64()))
        elif isinstance(v, bool):
            fields.append(pa.field(k, pa.bool_()))
        else:
            fields.append(pa.field(k, pa.string()))
    schema = pa.schema(fields)

    total = 0
    for dt, dt_rows in by_dt.items():
        part_dir = os.path.join(OUTPUT_DIR, table_name, f"dt={dt}")
        os.makedirs(part_dir, exist_ok=True)

        # Build columnar arrays (excluding dt)
        columns = {}
        for key in sample.keys():
            columns[key] = [row[key] for row in dt_rows]

        table = pa.table(columns, schema=schema)
        out_path = os.path.join(part_dir, "data.parquet")
        pq.write_table(table, out_path)
        total += len(dt_rows)

    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Generating synthetic SaaS telemetry data...")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Date range: {DATES[0]} to {DATES[-1]}")
    print()

    accounts = generate_accounts()
    users = generate_users(accounts)
    sessions = generate_sessions(users)
    api_requests = generate_api_requests(users, accounts)
    error_logs = generate_error_logs(api_requests)

    tables = {
        "accounts": accounts,
        "users": users,
        "sessions": sessions,
        "api_requests": api_requests,
        "error_logs": error_logs,
    }

    grand_total = 0
    for name, rows in tables.items():
        n = write_partitioned(name, rows)
        print(f"  {name}: {n} rows")
        grand_total += n

    print(f"\nTotal: {grand_total} rows")
    print()

    # --- Verify flags ---
    print("Flag verification:")

    # Flag 1: bad deployment
    bad_deploy = [r for r in api_requests
                  if r["endpoint"] == "/api/v1/reports/export"
                  and r["status_code"] == 500
                  and r["dt"] == "2026-01-14"]
    print(f"  Flag 1 (Bad Deployment): {len(bad_deploy)} 500s on /api/v1/reports/export "
          f"on Jan 14 between 14:00-14:30")

    # Flag 2: silent dropout
    acct042_by_day = {}
    for r in api_requests:
        if r["account_id"] == "acct_042":
            acct042_by_day.setdefault(r["dt"], []).append(r)
    for dt in DATES:
        count = len(acct042_by_day.get(dt, []))
        print(f"  Flag 2 (Silent Dropout): acct_042 on {dt} = {count} requests")

    # Flag 3: latency degradation
    acct003_by_day = {}
    for r in api_requests:
        if r["account_id"] == "acct_003":
            acct003_by_day.setdefault(r["dt"], []).append(r)
    for dt in DATES:
        reqs = acct003_by_day.get(dt, [])
        if reqs:
            avg_ms = sum(r["response_time_ms"] for r in reqs) / len(reqs)
            print(f"  Flag 3 (Latency Spike): acct_003 on {dt} = avg {avg_ms:.0f}ms "
                  f"({len(reqs)} requests)")

    # Flag 4: insider exfiltration
    insider = [r for r in api_requests
               if r["user_id"] == "usr_034" and r["account_id"] == "acct_007"]
    print(f"  Flag 4 (Insider Exfiltration): {len(insider)} requests by usr_034 on "
          f"churned acct_007 at 2 AM")

    # Flag 5: rate limit bypass
    puppet_reqs = [r for r in api_requests if r["account_id"] == "acct_019"]
    puppet_users = set(r["user_id"] for r in puppet_reqs)
    print(f"  Flag 5 (Rate Limit Bypass): {len(puppet_reqs)} requests from "
          f"{len(puppet_users)} puppet users on free-tier acct_019")

    print("\nDone!")


if __name__ == "__main__":
    main()