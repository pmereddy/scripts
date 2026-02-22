#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.6.8 compatible.
Queries the YARN ResourceManager REST API to list applications and produces
a dependency_access CSV with minimal fields for the dependency model:
  window_start, service, user, do_as, client_ip, app_name, op, object_type, object_id, cnt
"""

from __future__ import print_function

import argparse
import calendar
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


def utc_date_to_epoch_ms(date_str):
    """Convert YYYYMMDD string to epoch milliseconds (UTC)."""
    t = dt.datetime.strptime(date_str, "%Y%m%d")
    return calendar.timegm(t.timetuple()) * 1000


def ts_to_iso(epoch_ms):
    if not epoch_ms:
        return ""
    try:
        return dt.datetime.utcfromtimestamp(int(epoch_ms) / 1000.0).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def ms_to_date_str(epoch_ms):
    """Convert epoch ms to YYYY-MM-DD for progress display."""
    try:
        return dt.datetime.utcfromtimestamp(epoch_ms / 1000.0).strftime("%Y-%m-%d")
    except Exception:
        return "?"


def fetch_apps(session, base_url, states, started_begin, started_end, limit, sleep_ms):
    """Paginate through RM apps endpoint using time-cursor advancement.

    The YARN RM API does not guarantee result ordering or provide a cursor.
    We sort each batch by startedTime, advance the cursor past the max
    timestamp seen (+1 ms), and deduplicate by application ID.
    """
    seen_ids = set()
    all_apps = []
    url = "{}/ws/v1/cluster/apps".format(base_url.rstrip("/"))
    cursor = started_begin

    page = 0
    while True:
        params = {"limit": limit}
        if states:
            params["states"] = states
        if cursor:
            params["startedTimeBegin"] = cursor
        if started_end:
            params["startedTimeEnd"] = started_end

        resp = session.get(url, params=params, timeout=120)
        if not resp.ok:
            print("ERROR: HTTP {}: {}".format(resp.status_code, resp.text[:2000]), file=sys.stderr)
            sys.exit(1)

        data = resp.json()
        apps_wrapper = data.get("apps") or {}
        apps = apps_wrapper.get("app") or []
        if not apps:
            break

        new_count = 0
        max_ts = None
        for app in apps:
            app_id = app.get("id")
            if app_id and app_id not in seen_ids:
                seen_ids.add(app_id)
                all_apps.append(app)
                new_count += 1
            ts = app.get("startedTime")
            if ts and (max_ts is None or ts > max_ts):
                max_ts = ts

        page += 1
        print("[yarn] page {} fetched={} new={} total={} cursor={}".format(
            page, len(apps), new_count, len(all_apps),
            ms_to_date_str(cursor) if cursor else "start"
        ), file=sys.stderr)

        if len(apps) < limit:
            break

        if max_ts is None:
            break

        next_cursor = max_ts + 1
        if cursor is not None and next_cursor <= cursor:
            print("[yarn] WARNING: cursor did not advance (all apps at same ms). "
                  "Increasing limit for this window.", file=sys.stderr)
            cursor = None
            params_retry = {"limit": limit * 10}
            if states:
                params_retry["states"] = states
            params_retry["startedTimeBegin"] = max_ts
            if started_end:
                params_retry["startedTimeEnd"] = max_ts + 1000
            resp2 = session.get(url, params=params_retry, timeout=120)
            if resp2.ok:
                data2 = resp2.json()
                apps2 = (data2.get("apps") or {}).get("app") or []
                for app in apps2:
                    app_id = app.get("id")
                    if app_id and app_id not in seen_ids:
                        seen_ids.add(app_id)
                        all_apps.append(app)
            next_cursor = max_ts + 1001

        cursor = next_cursor
        if started_end and cursor >= started_end:
            break

        time.sleep(sleep_ms / 1000.0)

    return all_apps


def main():
    ap = argparse.ArgumentParser(
        description="Export YARN applications to dependency_access CSV."
    )
    ap.add_argument("--rm-url", required=True,
                    help="YARN ResourceManager URL (e.g. http://rm-host:8088)")
    ap.add_argument("--auth-mode", choices=["basic", "kerberos", "none"], default="none",
                    help="Authentication mode (default: none)")
    ap.add_argument("--user", default="", help="Username for basic auth")
    ap.add_argument("--password", default="", help="Password for basic auth")
    ap.add_argument("--verify-tls", action="store_true", default=False,
                    help="Verify TLS certificates")
    ap.add_argument("--states", default="FINISHED,KILLED,FAILED,RUNNING",
                    help="Comma-separated app states to fetch (default: FINISHED,KILLED,FAILED,RUNNING)")
    ap.add_argument("--since", default=None,
                    help="Start date YYYYMMDD for startedTimeBegin filter")
    ap.add_argument("--until", default=None,
                    help="End date YYYYMMDD for startedTimeEnd filter")
    ap.add_argument("--last-months", type=int, default=3,
                    help="If --since/--until not provided, fetch last N months (default: 3)")
    ap.add_argument("--limit", type=int, default=1000,
                    help="Max apps per API request (default: 1000)")
    ap.add_argument("--sleep-ms", type=int, default=100,
                    help="Sleep between paginated requests in ms (default: 100)")
    ap.add_argument("--out-access", default="yarn_access.csv",
                    help="Output CSV path (default: yarn_access.csv)")
    args = ap.parse_args()

    if requests is None:
        print("ERROR: requests not installed. Run: pip install requests", file=sys.stderr)
        sys.exit(1)

    session = mk_session(args.auth_mode, args.user, args.password, args.verify_tls)

    started_begin = None
    started_end = None
    if args.since and args.until:
        started_begin = utc_date_to_epoch_ms(args.since)
        started_end = utc_date_to_epoch_ms(args.until)
    elif not args.since and not args.until:
        now = dt.datetime.utcnow()
        since = now - dt.timedelta(days=30 * args.last_months)
        started_begin = calendar.timegm(since.timetuple()) * 1000
    elif args.since or args.until:
        print("ERROR: Provide both --since and --until, or neither.", file=sys.stderr)
        sys.exit(1)

    print("[yarn] Date range: {} to {}".format(
        ms_to_date_str(started_begin) if started_begin else "open",
        ms_to_date_str(started_end) if started_end else "open"
    ), file=sys.stderr)

    apps = fetch_apps(session, args.rm_url, args.states, started_begin, started_end,
                      args.limit, args.sleep_ms)
    print("[yarn] Total unique apps fetched: {}".format(len(apps)), file=sys.stderr)

    out_dir = os.path.dirname(os.path.abspath(args.out_access))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    with open(args.out_access, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=ACCESS_FIELDS)
        writer.writeheader()
        for app in apps:
            start_iso = ts_to_iso(app.get("startedTime"))
            writer.writerow({
                "window_start": start_iso,
                "service": "yarn",
                "user": app.get("user") or "",
                "do_as": "",
                "client_ip": "",
                "app_name": app.get("name") or "",
                "op": "SUBMIT",
                "object_type": "application",
                "object_id": app.get("id") or "",
                "cnt": 1,
            })

    print("[yarn] Wrote {} rows to {}".format(len(apps), args.out_access), file=sys.stderr)


if __name__ == "__main__":
    main()
