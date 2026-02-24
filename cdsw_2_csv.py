#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.6.8 compatible.
Queries the CDSW Admin API (v1) to enumerate projects, jobs, sessions,
models, and applications, and produces two CSVs compatible with load_neo4j.py:

  - dependency_objects CSV  (projects as CDPObjects)
  - dependency_access  CSV  (jobs/sessions/models as access events)

Usage examples:

  # Collect everything (API key auth)
  python3 cdsw_2_csv.py \\
      --cdsw-url https://cdsw.example.com \\
      --api-key <ADMIN_API_KEY> \\
      --out-objects cdsw_objects.csv \\
      --out-access  cdsw_access.csv

  # Basic auth, limit to 50 projects
  python3 cdsw_2_csv.py \\
      --cdsw-url https://cdsw.example.com \\
      --auth-mode basic --user admin --password secret \\
      --max-projects 50

  # Skip sessions (can be very large)
  python3 cdsw_2_csv.py \\
      --cdsw-url https://cdsw.example.com \\
      --api-key <KEY> --skip-sessions
"""

from __future__ import print_function

import argparse
import csv
import os
import sys
import time

try:
    import requests
except ImportError:
    requests = None

OBJECTS_FIELDS = [
    "service", "object_type", "object_id", "owner", "group", "extra",
]

ACCESS_FIELDS = [
    "window_start", "service", "user", "do_as", "client_ip",
    "app_name", "op", "object_type", "object_id", "cnt",
]


def mk_session(auth_mode, api_key, user, password, verify_tls):
    s = requests.Session()
    s.verify = verify_tls
    if api_key:
        s.headers["Authorization"] = "Bearer {}".format(api_key)
    elif auth_mode == "basic" and user:
        s.auth = (user, password)
    return s


def paginate(session, url, key, page_size=100, sleep_ms=100, max_items=0):
    """Generic paginated GET. Yields items from response[key]."""
    offset = 0
    total = 0
    while True:
        params = {"pageSize": page_size, "pageToken": offset}
        try:
            resp = session.get(url, params=params, timeout=60)
        except Exception as e:
            print("[cdsw] ERROR fetching {}: {}".format(url, e), file=sys.stderr)
            break
        if not resp.ok:
            print("[cdsw] HTTP {}: {} (url={})".format(
                resp.status_code, resp.text[:300], url), file=sys.stderr)
            break

        data = resp.json()
        items = data if isinstance(data, list) else data.get(key) or []
        if not items:
            break

        for item in items:
            yield item
            total += 1
            if 0 < max_items <= total:
                return

        if isinstance(data, list) or len(items) < page_size:
            break
        offset += page_size
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)


def fetch_projects(session, base_url, page_size, sleep_ms, max_projects):
    url = "{}/api/v1/projects".format(base_url)
    projects = list(paginate(session, url, "projects",
                             page_size=page_size, sleep_ms=sleep_ms,
                             max_items=max_projects))
    print("[cdsw] Fetched {} projects".format(len(projects)), file=sys.stderr)
    return projects


def fetch_jobs(session, base_url, owner, project, page_size, sleep_ms):
    url = "{}/api/v1/projects/{}/{}/jobs".format(base_url, owner, project)
    return list(paginate(session, url, "jobs",
                         page_size=page_size, sleep_ms=sleep_ms))


def fetch_sessions(session, base_url, owner, project, page_size, sleep_ms):
    url = "{}/api/v1/projects/{}/{}/sessions".format(base_url, owner, project)
    return list(paginate(session, url, "sessions",
                         page_size=page_size, sleep_ms=sleep_ms))


def fetch_models(session, base_url, owner, project, page_size, sleep_ms):
    url = "{}/api/v1/projects/{}/{}/models".format(base_url, owner, project)
    return list(paginate(session, url, "models",
                         page_size=page_size, sleep_ms=sleep_ms))


def fetch_applications(session, base_url, owner, project, page_size, sleep_ms):
    url = "{}/api/v1/projects/{}/{}/applications".format(base_url, owner, project)
    return list(paginate(session, url, "applications",
                         page_size=page_size, sleep_ms=sleep_ms))


def safe_ts(val):
    """Return ISO timestamp or empty string."""
    if not val:
        return ""
    s = str(val)
    if "T" in s:
        return s[:19] + "Z" if len(s) >= 19 else s
    return s


def project_id(proj):
    """Build a stable project identifier: owner/name."""
    owner = proj.get("owner", {})
    owner_name = owner.get("username") or owner.get("name") or "unknown"
    proj_name = proj.get("name") or proj.get("slug") or "unnamed"
    return "{}/{}".format(owner_name, proj_name)


def project_owner(proj):
    owner = proj.get("owner", {})
    return owner.get("username") or owner.get("name") or ""


def project_extra(proj):
    """Build pipe-delimited extra metadata."""
    parts = []
    desc = (proj.get("description") or "").replace("|", " ").replace("\n", " ")[:120]
    if desc:
        parts.append("desc={}".format(desc))
    engine = proj.get("default_engine_type") or ""
    if engine:
        parts.append("engine={}".format(engine))
    created = safe_ts(proj.get("created_at") or proj.get("createdAt"))
    if created:
        parts.append("created={}".format(created))
    return "|".join(parts)


def main():
    ap = argparse.ArgumentParser(
        description="Export CDSW projects, jobs, sessions, models to CSV."
    )
    ap.add_argument("--cdsw-url", required=True,
                    help="CDSW base URL (e.g. https://cdsw.example.com)")
    ap.add_argument("--api-key", default="",
                    help="CDSW API key (admin key sees all projects)")
    ap.add_argument("--auth-mode", choices=["basic", "none"], default="none",
                    help="Auth mode if not using API key (default: none)")
    ap.add_argument("--user", default="", help="Username for basic auth")
    ap.add_argument("--password", default="", help="Password for basic auth")
    ap.add_argument("--verify-tls", action="store_true", default=False,
                    help="Verify TLS certificates (default: skip)")
    ap.add_argument("--page-size", type=int, default=100,
                    help="API page size (default: 100)")
    ap.add_argument("--sleep-ms", type=int, default=100,
                    help="Sleep between API calls in ms (default: 100)")
    ap.add_argument("--max-projects", type=int, default=0,
                    help="Max projects to enumerate; 0 = all (default: 0)")
    ap.add_argument("--skip-sessions", action="store_true", default=False,
                    help="Skip collecting sessions (can be very large)")
    ap.add_argument("--skip-models", action="store_true", default=False,
                    help="Skip collecting models")
    ap.add_argument("--skip-applications", action="store_true", default=False,
                    help="Skip collecting CDSW applications (web apps)")
    ap.add_argument("--out-objects", default="cdsw_objects.csv",
                    help="Output objects CSV (default: cdsw_objects.csv)")
    ap.add_argument("--out-access", default="cdsw_access.csv",
                    help="Output access CSV (default: cdsw_access.csv)")
    args = ap.parse_args()

    if requests is None:
        print("ERROR: requests not installed. Run: pip install requests",
              file=sys.stderr)
        sys.exit(1)

    base_url = args.cdsw_url.rstrip("/")
    session = mk_session(args.auth_mode, args.api_key,
                         args.user, args.password, args.verify_tls)

    projects = fetch_projects(session, base_url, args.page_size,
                              args.sleep_ms, args.max_projects)
    if not projects:
        print("[cdsw] No projects found. Check URL and credentials.",
              file=sys.stderr)
        sys.exit(1)

    for d in (args.out_objects, args.out_access):
        out_dir = os.path.dirname(os.path.abspath(d))
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir)

    obj_rows = []
    access_rows = []

    for idx, proj in enumerate(projects):
        pid = project_id(proj)
        owner = project_owner(proj)

        obj_rows.append({
            "service": "cdsw",
            "object_type": "cdsw_project",
            "object_id": pid,
            "owner": owner,
            "group": "",
            "extra": project_extra(proj),
        })

        # --- Jobs (scheduled / manual) ---
        jobs = fetch_jobs(session, base_url, owner,
                          proj.get("slug") or proj.get("name") or "",
                          args.page_size, args.sleep_ms)
        for job in jobs:
            job_name = job.get("name") or job.get("title") or ""
            script = job.get("script") or ""
            kernel = job.get("kernel") or job.get("engine_type") or ""
            schedule = job.get("schedule") or ""

            extra_parts = []
            if script:
                extra_parts.append("script={}".format(script))
            if kernel:
                extra_parts.append("engine={}".format(kernel))
            if schedule:
                extra_parts.append("schedule={}".format(
                    schedule.replace("|", " ")))

            access_rows.append({
                "window_start": safe_ts(
                    job.get("latest", {}).get("started_at") or
                    job.get("updated_at") or job.get("created_at")),
                "service": "cdsw",
                "user": owner,
                "do_as": "",
                "client_ip": "",
                "app_name": job_name,
                "op": "JOB",
                "object_type": "cdsw_project",
                "object_id": pid,
                "cnt": 1,
            })

            obj_rows.append({
                "service": "cdsw",
                "object_type": "cdsw_job",
                "object_id": "{}/{}".format(pid, job_name or job.get("id", "")),
                "owner": owner,
                "group": "",
                "extra": "|".join(extra_parts),
            })

        # --- Sessions (interactive) ---
        if not args.skip_sessions:
            sessions = fetch_sessions(session, base_url, owner,
                                      proj.get("slug") or proj.get("name") or "",
                                      args.page_size, args.sleep_ms)
            for sess in sessions:
                sess_user = (sess.get("owner", {}).get("username") or
                             sess.get("owner", {}).get("name") or owner)
                kernel = sess.get("kernel") or sess.get("engine_type") or ""
                access_rows.append({
                    "window_start": safe_ts(
                        sess.get("created_at") or sess.get("createdAt")),
                    "service": "cdsw",
                    "user": sess_user,
                    "do_as": "",
                    "client_ip": "",
                    "app_name": "session:{}".format(kernel),
                    "op": "SESSION",
                    "object_type": "cdsw_project",
                    "object_id": pid,
                    "cnt": 1,
                })

        # --- Models ---
        if not args.skip_models:
            models = fetch_models(session, base_url, owner,
                                  proj.get("slug") or proj.get("name") or "",
                                  args.page_size, args.sleep_ms)
            for model in models:
                model_name = model.get("name") or ""
                access_rows.append({
                    "window_start": safe_ts(
                        model.get("updated_at") or model.get("created_at")),
                    "service": "cdsw",
                    "user": owner,
                    "do_as": "",
                    "client_ip": "",
                    "app_name": model_name,
                    "op": "MODEL",
                    "object_type": "cdsw_project",
                    "object_id": pid,
                    "cnt": 1,
                })

                obj_rows.append({
                    "service": "cdsw",
                    "object_type": "cdsw_model",
                    "object_id": "{}/model:{}".format(pid, model_name or model.get("id", "")),
                    "owner": owner,
                    "group": "",
                    "extra": "",
                })

        # --- Applications (web apps) ---
        if not args.skip_applications:
            cdsw_apps = fetch_applications(
                session, base_url, owner,
                proj.get("slug") or proj.get("name") or "",
                args.page_size, args.sleep_ms)
            for app in cdsw_apps:
                app_name = app.get("name") or ""
                access_rows.append({
                    "window_start": safe_ts(
                        app.get("updated_at") or app.get("created_at")),
                    "service": "cdsw",
                    "user": owner,
                    "do_as": "",
                    "client_ip": "",
                    "app_name": app_name,
                    "op": "APPLICATION",
                    "object_type": "cdsw_project",
                    "object_id": pid,
                    "cnt": 1,
                })

        if (idx + 1) % 10 == 0 or idx + 1 == len(projects):
            print("[cdsw] Processed {}/{} projects ({} objects, {} access rows)".format(
                idx + 1, len(projects), len(obj_rows), len(access_rows)),
                file=sys.stderr)

    # --- Write objects CSV ---
    with open(args.out_objects, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=OBJECTS_FIELDS)
        writer.writeheader()
        for row in obj_rows:
            writer.writerow(row)
    print("[cdsw] Wrote {} object rows to {}".format(
        len(obj_rows), args.out_objects), file=sys.stderr)

    # --- Write access CSV ---
    with open(args.out_access, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=ACCESS_FIELDS)
        writer.writeheader()
        for row in access_rows:
            writer.writerow(row)
    print("[cdsw] Wrote {} access rows to {}".format(
        len(access_rows), args.out_access), file=sys.stderr)


if __name__ == "__main__":
    main()
