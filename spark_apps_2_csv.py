#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.6.8 compatible.
Queries the Spark History Server REST API to list applications and produces
a dependency_access CSV with minimal fields for the dependency model.

Spark 2.4 SHS does NOT support offset-based pagination.  This script uses
date-window iteration (day by day) to collect all apps across a date range
without hitting server-side result caps.
"""

from __future__ import print_function

import argparse
import csv
import datetime as dt
import os
import sys
import time

try:
    import requests
except ImportError:
    requests = None

try:
    from requests_kerberos import HTTPKerberosAuth, OPTIONAL as KRB_OPTIONAL
    HAS_KRB = True
except ImportError:
    HAS_KRB = False


ACCESS_FIELDS = [
    "window_start", "service", "user", "do_as", "client_ip",
    "app_name", "op", "object_type", "object_id", "cnt",
]


def mk_session(auth_mode, user, password, verify_tls):
    s = requests.Session()
    s.verify = verify_tls
    if auth_mode == "basic":
        if user:
            s.auth = (user, password)
    elif auth_mode == "kerberos":
        if not HAS_KRB:
            print("ERROR: requests-kerberos not installed.", file=sys.stderr)
            sys.exit(1)
        s.auth = HTTPKerberosAuth(mutual_authentication=KRB_OPTIONAL)
    return s


def ts_to_iso(ts_str):
    """Convert Spark History Server timestamp to ISO format."""
    if not ts_str:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fGMT", "%Y-%m-%dT%H:%M:%SGMT",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = dt.datetime.strptime(ts_str, fmt)
            return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return ts_str[:19] + "Z" if len(ts_str) >= 19 else ts_str


def fetch_one_window(session, url, min_date, max_date, limit):
    """Fetch apps for a single date window. Returns list of app dicts."""
    params = {"limit": limit}
    if min_date:
        params["minDate"] = min_date
    if max_date:
        params["maxDate"] = max_date

    resp = session.get(url, params=params, timeout=120)
    if not resp.ok:
        print("[spark] ERROR: HTTP {}: {}".format(resp.status_code, resp.text[:500]), file=sys.stderr)
        return []

    apps = resp.json()
    if not isinstance(apps, list):
        return []
    return apps


def fetch_apps(session, base_url, since_date, until_date, limit, sleep_ms):
    """Fetch applications using day-by-day date windowing.

    Spark 2.4 SHS ignores the 'start' offset parameter, so we iterate
    one day at a time to ensure we collect all apps.
    """
    url = "{}/api/v1/applications".format(base_url.rstrip("/"))
    seen_ids = set()
    all_apps = []

    if not since_date:
        apps = fetch_one_window(session, url, None, until_date, limit)
        for app in apps:
            app_id = app.get("id")
            if app_id and app_id not in seen_ids:
                seen_ids.add(app_id)
                all_apps.append(app)
        print("[spark] Fetched {} apps (no date range)".format(len(all_apps)), file=sys.stderr)
        return all_apps

    start = dt.datetime.strptime(since_date, "%Y-%m-%d")
    if until_date:
        end = dt.datetime.strptime(until_date, "%Y-%m-%d")
    else:
        end = dt.datetime.utcnow()

    day = start
    day_count = 0
    while day <= end:
        day_str = day.strftime("%Y-%m-%d")
        next_day_str = (day + dt.timedelta(days=1)).strftime("%Y-%m-%d")

        apps = fetch_one_window(session, url, day_str, next_day_str, limit)

        new_count = 0
        for app in apps:
            app_id = app.get("id")
            if app_id and app_id not in seen_ids:
                seen_ids.add(app_id)
                all_apps.append(app)
                new_count += 1

        day_count += 1
        if new_count > 0 or day_count % 7 == 0:
            print("[spark] {} fetched={} new={} total={}".format(
                day_str, len(apps), new_count, len(all_apps)), file=sys.stderr)

        if len(apps) >= limit:
            print("[spark] WARNING: {} returned {} apps (= limit). "
                  "Some apps may be missing for this day. "
                  "Increase --limit if needed.".format(day_str, len(apps)), file=sys.stderr)

        day += dt.timedelta(days=1)
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    return all_apps


def main():
    ap = argparse.ArgumentParser(
        description="Export Spark History Server applications to dependency_access CSV."
    )
    ap.add_argument("--shs-url", required=True,
                    help="Spark History Server URL (e.g. http://shs-host:18088)")
    ap.add_argument("--auth-mode", choices=["basic", "kerberos", "none"], default="none",
                    help="Authentication mode (default: none)")
    ap.add_argument("--user", default="", help="Username for basic auth")
    ap.add_argument("--password", default="", help="Password for basic auth")
    ap.add_argument("--verify-tls", action="store_true", default=False,
                    help="Verify TLS certificates")
    ap.add_argument("--since", default=None,
                    help="Start date YYYYMMDD for minDate filter")
    ap.add_argument("--until", default=None,
                    help="End date YYYYMMDD for maxDate filter")
    ap.add_argument("--last-months", type=int, default=3,
                    help="If --since/--until not provided, fetch last N months (default: 3)")
    ap.add_argument("--limit", type=int, default=50000,
                    help="Max apps per API request (default: 50000). "
                         "Set high since SHS returns all matching apps in one call.")
    ap.add_argument("--sleep-ms", type=int, default=100,
                    help="Sleep between daily requests in ms (default: 100)")
    ap.add_argument("--out-access", default="spark_access.csv",
                    help="Output CSV path (default: spark_access.csv)")
    args = ap.parse_args()

    if requests is None:
        print("ERROR: requests not installed. Run: pip install requests", file=sys.stderr)
        sys.exit(1)

    session = mk_session(args.auth_mode, args.user, args.password, args.verify_tls)

    since_date = None
    until_date = None
    if args.since and args.until:
        since_date = "{}-{}-{}".format(args.since[:4], args.since[4:6], args.since[6:8])
        until_date = "{}-{}-{}".format(args.until[:4], args.until[4:6], args.until[6:8])
    elif not args.since and not args.until:
        now = dt.datetime.utcnow()
        since_dt = now - dt.timedelta(days=30 * args.last_months)
        since_date = since_dt.strftime("%Y-%m-%d")
    elif args.since or args.until:
        print("ERROR: Provide both --since and --until, or neither.", file=sys.stderr)
        sys.exit(1)

    print("[spark] Date range: {} to {}".format(
        since_date or "open", until_date or "open"
    ), file=sys.stderr)

    apps = fetch_apps(session, args.shs_url, since_date, until_date,
                      args.limit, args.sleep_ms)
    print("[spark] Total unique apps fetched: {}".format(len(apps)), file=sys.stderr)

    out_dir = os.path.dirname(os.path.abspath(args.out_access))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    with open(args.out_access, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=ACCESS_FIELDS)
        writer.writeheader()
        for app in apps:
            attempts = app.get("attempts") or [{}]
            last_attempt = attempts[-1] if attempts else {}
            start_time = last_attempt.get("startTime") or ""
            spark_user = last_attempt.get("sparkUser") or ""

            writer.writerow({
                "window_start": ts_to_iso(start_time),
                "service": "spark",
                "user": spark_user,
                "do_as": "",
                "client_ip": "",
                "app_name": app.get("name") or "",
                "op": "SUBMIT",
                "object_type": "application",
                "object_id": app.get("id") or "",
                "cnt": 1,
            })

    print("[spark] Wrote {} rows to {}".format(len(apps), args.out_access), file=sys.stderr)


if __name__ == "__main__":
    main()
