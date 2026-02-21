#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Python 3.6 compatible.
Reads Ranger audit logs directly from HDFS (Kerberos ticket required), filters noise,
reduces file path to directory level (one level above), and produces:
  (a) events CSV (all non-filtered events)
  (b) summary CSV (graph-ready, 30-min dedup window by default)

Supports directory layouts:
  A) /ranger/audit/<service>/<YYYYMMDD>/...
  B) /ranger/audit/<service>/<service>/<YYYYMMDD>/...

Date filtering is done at DIRECTORY level to avoid scanning years of data.
"""

from __future__ import print_function

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import queue
import re
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


# -------------------------
# Noise filtering patterns
# -------------------------

NOISE_SUBSTRINGS = [
    "/_temporary/",
    "/_spark_metadata/",
    "/_success",
    "/.trash/",
    "/.staging/",
    "/tmp/",
    "_spark_metadata",
    "_temporary",
]

NOISE_REGEX = [
    re.compile(r"(?i)\.crc$"),
    re.compile(r"(?i)/_committed_.*"),
    re.compile(r"(?i)\b_committed_.*"),
    re.compile(r"(?i)/user/[^/]+/\.sparkstaging/"),
    re.compile(r"(?i)\.sparkstaging"),
]

NOISE_FILENAMES = set(["_SUCCESS"])

DATE_DIR_RE = re.compile(r"/(\d{8})(?:/)?$")


# -------------------------
# HDFS helpers
# -------------------------

def run_cmd(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    out, err = p.communicate()
    return p.returncode, out, err


def hdfs_ls(path):
    """
    Return parsed `hdfs dfs -ls` lines (excluding header).
    Each element is the raw line string.
    If path doesn't exist or fails, returns [].
    """
    rc, out, err = run_cmd(["hdfs", "dfs", "-ls", path])
    if rc != 0:
        return []
    lines = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        lines.append(line)
    return lines


def hdfs_find_files(hdfs_root):
    rc, out, err = run_cmd(["hdfs", "dfs", "-find", hdfs_root, "-type", "f"])
    if rc != 0:
        raise RuntimeError("Failed to list HDFS files under {}\n{}".format(hdfs_root, err.strip()))
    return [line.strip() for line in out.splitlines() if line.strip()]


def hdfs_stream_text(file_path):
    """
    Stream content using `hdfs dfs -text`, fallback to `-cat`.
    """
    p = subprocess.Popen(["hdfs", "dfs", "-text", file_path],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    for line in p.stdout:
        yield line
    _, err = p.communicate()
    if p.returncode == 0:
        return

    p2 = subprocess.Popen(["hdfs", "dfs", "-cat", file_path],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    for line in p2.stdout:
        yield line
    _, err2 = p2.communicate()
    if p2.returncode != 0:
        raise RuntimeError("Failed to read {}\n-text err: {}\n-cat err: {}".format(
            file_path, err.strip(), err2.strip()
        ))


# -------------------------
# Date folder pruning
# -------------------------

def parse_yyyymmdd(s):
    return dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def list_service_dirs(audit_root, services_filter):
    """
    Expect /ranger/audit/<service> directories.
    """
    lines = hdfs_ls(audit_root)
    dirs = []
    wanted = None
    if services_filter:
        wanted = set([x.strip() for x in services_filter.split(",") if x.strip()])

    for line in lines:
        parts = line.split()
        if len(parts) < 8:
            continue
        perms = parts[0]
        path = parts[-1]
        if not perms.startswith("d"):
            continue
        base = os.path.basename(path.rstrip("/"))
        if wanted is None or base in wanted:
            dirs.append(path)

    return dirs


def list_date_dirs_for_service(service_dir, since, until):
    """
    Support both layouts:
      A) /ranger/audit/<service>/<YYYYMMDD>/
      B) /ranger/audit/<service>/<service>/<YYYYMMDD>/
    """
    # list children of /ranger/audit/<service>
    lines = hdfs_ls(service_dir)
    child_dirs = []
    for line in lines:
        parts = line.split()
        if len(parts) < 8 or not parts[0].startswith("d"):
            continue
        child_dirs.append(parts[-1])

    # Try layout A first
    keep = []
    for p in child_dirs:
        m = DATE_DIR_RE.search(p)
        if not m:
            continue
        d = parse_yyyymmdd(m.group(1))
        if since <= d <= until:
            keep.append(p)
    if keep:
        return keep

    # Fallback to layout B: find inner folder matching service name
    outer = os.path.basename(service_dir.rstrip("/"))
    inner_dir = None
    for p in child_dirs:
        if os.path.basename(p.rstrip("/")) == outer:
            inner_dir = p
            break
    if not inner_dir:
        return []

    inner_lines = hdfs_ls(inner_dir)
    keep2 = []
    for line in inner_lines:
        parts = line.split()
        if len(parts) < 8 or not parts[0].startswith("d"):
            continue
        p = parts[-1]
        m = DATE_DIR_RE.search(p)
        if not m:
            continue
        d = parse_yyyymmdd(m.group(1))
        if since <= d <= until:
            keep2.append(p)

    return keep2


def collect_files_in_range(audit_root, since, until, services_filter):
    svc_dirs = list_service_dirs(audit_root, services_filter)
    all_files = []
    for svc_dir in sorted(svc_dirs):
        date_dirs = list_date_dirs_for_service(svc_dir, since, until)
        for dd in sorted(date_dirs):
            all_files.extend(hdfs_find_files(dd))
    return all_files


# -------------------------
# Parsing + normalization
# -------------------------

WRITE_HINTS = set(["write", "update", "create", "delete", "alter", "drop", "truncate", "append",
                   "rename", "setpermission", "setowner", "mkdir", "rmdir", "move"])
READ_HINTS = set(["read", "open", "list", "get", "getfileinfo", "getfilestatus", "liststatus",
                  "stat", "content", "execute"])


def sha1_hex(s):
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()


def safe_str(x, max_len=4000):
    if x is None:
        return ""
    s = str(x)
    if len(s) > max_len:
        return s[:max_len] + "…"
    return s


def extract_first(d, keys):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def to_iso_from_evtTime(evt_time):
    if evt_time is None:
        return ""
    try:
        ms = int(evt_time)
        if ms < 10000000000:
            ts = dt.datetime.utcfromtimestamp(ms)
        else:
            ts = dt.datetime.utcfromtimestamp(ms / 1000.0)
        return ts.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    except Exception:
        return safe_str(evt_time, 64)


def infer_op(access_type, action):
    s = (access_type or "").lower().strip()
    a = (action or "").lower().strip()
    if s in WRITE_HINTS or a in WRITE_HINTS:
        return "WRITE"
    if s in READ_HINTS or a in READ_HINTS:
        return "READ"
    if any(w in s for w in ["write", "create", "delete", "append", "rename", "alter", "drop"]):
        return "WRITE"
    return "READ"


def is_noise_path(path):
    if not path:
        return False
    p = path.strip()
    pl = p.lower()
    base = os.path.basename(p)

    if base in NOISE_FILENAMES:
        return True
    if base.lower().endswith(".crc"):
        return True

    for sub in NOISE_SUBSTRINGS:
        if sub in pl:
            return True

    for rx in NOISE_REGEX:
        if rx.search(p):
            return True

    return False


def reduce_to_directory_level(path):
    if not path:
        return ""
    p = path.strip()
    if p.endswith("/"):
        return p
    parent = os.path.dirname(p)
    if parent and not parent.endswith("/"):
        parent += "/"
    return parent or ""


def normalize_result(result):
    if result is None:
        return ""
    if isinstance(result, bool):
        return "ALLOWED" if result else "DENIED"
    rs = str(result).strip()
    if rs in ("1", "true", "True", "allowed", "ALLOW"):
        return "ALLOWED"
    if rs in ("0", "false", "False", "denied", "DENY"):
        return "DENIED"
    return rs[:64]


def parse_audit(obj, source_file):
    evt_time = extract_first(obj, ["evtTime", "eventTime", "time", "accessTime"])
    event_ts = to_iso_from_evtTime(evt_time)

    service = safe_str(extract_first(obj, ["repoName", "repositoryName", "serviceName", "repo"]), 256)
    user = safe_str(extract_first(obj, ["user", "reqUser", "accessUser"]), 256)

    # Keep doAs + ugi
    do_as = safe_str(extract_first(obj, ["doAsUser", "proxyUser", "impersonator", "proxy"]), 256)
    ugi = safe_str(extract_first(obj, ["ugi", "userGroupInformation"]), 512)

    client_ip = safe_str(extract_first(obj, ["clientIP", "cliIP", "client_ip", "ip", "remoteIP"]), 128)
    client_host = safe_str(extract_first(obj, ["clientHost", "clientHostname", "host", "remoteHost"]), 256)

    access_type = safe_str(extract_first(obj, ["accessType", "access", "access_type"]), 128)
    action = safe_str(extract_first(obj, ["action", "event", "auditType"]), 128)

    result_str = normalize_result(extract_first(obj, ["result", "accessResult", "isAllowed", "allowed"]))
    policy_id = safe_str(extract_first(obj, ["policyId", "policy_id", "policy"]), 64)

    resource = safe_str(extract_first(obj, ["resourcePath", "resourceName", "resource", "resource_path"]), 8000)
    if resource and is_noise_path(resource):
        return None

    resource_dir = reduce_to_directory_level(resource)
    if resource_dir and is_noise_path(resource_dir):
        return None

    request_data = safe_str(extract_first(obj, ["requestData", "request", "queryString", "sql"]), 16000)
    request_hash = sha1_hex(request_data) if request_data else ""

    app_name = safe_str(extract_first(obj, ["app", "application", "applicationName", "appName", "yarnAppName",
                                            "sparkAppName", "oozieJobId", "workflowId", "sessionId"]), 256)

    op = infer_op(access_type, action)
    resource_norm = resource_dir
    resource_hash = sha1_hex(resource_norm) if resource_norm else ""

    return {
        "event_ts": event_ts,
        "service": service,
        "user": user,
        "do_as": do_as,
        "ugi": ugi,
        "client_ip": client_ip,
        "client_host": client_host,
        "access_type": access_type,
        "action": action,
        "op": op,
        "result": result_str,
        "policy_id": policy_id,
        "resource": resource,
        "resource_dir": resource_dir,
        "resource_norm": resource_norm,
        "resource_hash": resource_hash,
        "app_name": app_name,
        "request_data": request_data,
        "request_hash": request_hash,
        "source_file": source_file,
    }


def parse_ts_utc_z(ts):
    """
    Parse "YYYY-mm-ddTHH:MM:SSZ" into datetime, or None.
    Python 3.6: no fromisoformat.
    """
    if not ts:
        return None
    try:
        return dt.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def floor_time_to_window(ts, minutes):
    # ts is datetime (naive UTC), floor to window
    discard = ts.minute % minutes
    return ts.replace(minute=ts.minute - discard, second=0, microsecond=0)


# -------------------------
# Summary aggregation
# -------------------------

def make_summary_key(window_start, rec):
    return (
        window_start,
        rec.get("service", ""),
        rec.get("user", ""),
        rec.get("do_as", ""),
        rec.get("ugi", ""),
        rec.get("client_ip", ""),
        rec.get("op", ""),
        rec.get("resource_norm", ""),
    )


def update_summary(summary_dict, rec, dedup_minutes):
    ts = parse_ts_utc_z(rec.get("event_ts", ""))
    window_start = ""
    if ts is not None:
        window_start = floor_time_to_window(ts, dedup_minutes).strftime("%Y-%m-%dT%H:%M:%SZ")

    k = make_summary_key(window_start, rec)
    agg = summary_dict.get(k)
    if agg is None:
        summary_dict[k] = {
            "first_seen": rec.get("event_ts", ""),
            "last_seen": rec.get("event_ts", ""),
            "cnt": 1
        }
    else:
        et = rec.get("event_ts", "")
        if et and (not agg["first_seen"] or et < agg["first_seen"]):
            agg["first_seen"] = et
        if et and (not agg["last_seen"] or et > agg["last_seen"]):
            agg["last_seen"] = et
        agg["cnt"] += 1


# -------------------------
# Writer + workers
# -------------------------

EVENT_FIELDS = [
    "event_ts", "service", "user", "do_as", "ugi", "client_ip", "client_host",
    "access_type", "action", "op", "result", "policy_id",
    "resource", "resource_dir", "resource_norm", "resource_hash",
    "app_name", "request_data", "request_hash", "source_file",
]

SUMMARY_FIELDS = [
    "window_start", "service", "user", "do_as", "ugi", "client_ip", "op", "resource_norm",
    "first_seen", "last_seen", "cnt",
]


def writer_thread_fn(q, out_events_path, stop_evt):
    with open(out_events_path, "w") as f:
        w = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
        w.writeheader()
        while not stop_evt.is_set():
            item = q.get()
            if item is None:
                q.task_done()
                break
            w.writerow(item)
            q.task_done()


def process_file(file_path, event_q, summary_global, summary_lock,
                 dedup_minutes, progress, progress_lock, skip_nonjson):
    local_summary = {}

    for line in hdfs_stream_text(file_path):
        line_s = line.strip()
        if not line_s:
            continue
        if skip_nonjson and not (line_s.startswith("{") and line_s.endswith("}")):
            continue

        try:
            obj = json.loads(line_s)
        except Exception:
            with progress_lock:
                progress["bad_json"] += 1
            continue

        rec = parse_audit(obj, file_path)
        if rec is None:
            with progress_lock:
                progress["filtered"] += 1
            continue

        event_q.put(rec)
        update_summary(local_summary, rec, dedup_minutes)

        with progress_lock:
            progress["events"] += 1
            if progress["events"] % progress["progress_every"] == 0:
                print("events={:,} filtered={:,} bad_json={:,}".format(
                    progress["events"], progress["filtered"], progress["bad_json"]
                ), file=sys.stderr)

    with summary_lock:
        for k, agg in local_summary.items():
            g = summary_global.get(k)
            if g is None:
                summary_global[k] = agg
            else:
                if agg["first_seen"] and (not g["first_seen"] or agg["first_seen"] < g["first_seen"]):
                    g["first_seen"] = agg["first_seen"]
                if agg["last_seen"] and (not g["last_seen"] or agg["last_seen"] > g["last_seen"]):
                    g["last_seen"] = agg["last_seen"]
                g["cnt"] += agg["cnt"]


def write_summary_csv(summary, out_summary_path):
    with open(out_summary_path, "w") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS)
        w.writeheader()

        # sort keys for stable output
        for k in sorted(summary.keys()):
            agg = summary[k]
            w.writerow({
                "window_start": k[0],
                "service": k[1],
                "user": k[2],
                "do_as": k[3],
                "ugi": k[4],
                "client_ip": k[5],
                "op": k[6],
                "resource_norm": k[7],
                "first_seen": agg.get("first_seen", ""),
                "last_seen": agg.get("last_seen", ""),
                "cnt": agg.get("cnt", 0),
            })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hdfs-root", required=True, help="HDFS audit root, e.g. hdfs:///ranger/audit")
    ap.add_argument("--out-events", required=True, help="Local output CSV path for per-event records")
    ap.add_argument("--out-summary", required=True, help="Local output CSV path for dedup summary")
    ap.add_argument("--threads", type=int, default=8, help="Parallel threads across HDFS files")
    ap.add_argument("--dedup-minutes", type=int, default=30, help="Dedup window minutes for summary tier")

    # Date control (folder-based)
    ap.add_argument("--since", default=None, help="Start date folder inclusive: YYYYMMDD")
    ap.add_argument("--until", default=None, help="End date folder inclusive: YYYYMMDD")
    ap.add_argument("--last-months", type=int, default=3, help="If --since/--until not provided, use last N months (default 3)")

    # Scope
    ap.add_argument("--services", default=None, help="Comma-separated services under audit root (e.g. hdfs,hive)")
    ap.add_argument("--max-files", type=int, default=None, help="Optional cap on number of files to process")
    ap.add_argument("--progress-every", type=int, default=200000, help="Print progress every N events")
    ap.add_argument("--skip-nonjson", action="store_true", default=True, help="Skip lines that are not JSON objects")
    args = ap.parse_args()

    # Kerberos sanity
    rc, _, err = run_cmd(["hdfs", "dfs", "-ls", args.hdfs_root])
    if rc != 0:
        raise RuntimeError("Cannot access {}. Ensure kinit done.\n{}".format(args.hdfs_root, err.strip()))

    # Determine date range for directory pruning
    if args.since or args.until:
        if not (args.since and args.until):
            raise SystemExit("If specifying date range, provide BOTH --since and --until in YYYYMMDD.")
        since = parse_yyyymmdd(args.since)
        until = parse_yyyymmdd(args.until)
    else:
        today = dt.date.today()
        since = today - dt.timedelta(days=30 * args.last_months)
        until = today

    if since > until:
        raise SystemExit("Invalid range: --since is after --until")

    print("Selecting date-folders from {} to {} (inclusive)".format(since.isoformat(), until.isoformat()), file=sys.stderr)

    files = collect_files_in_range(args.hdfs_root, since, until, args.services)
    if args.max_files is not None:
        files = files[:args.max_files]

    if not files:
        print("No files found in selected date range/services.", file=sys.stderr)
        sys.exit(2)

    # ensure output dirs exist
    out_events_dir = os.path.dirname(os.path.abspath(args.out_events)) or "."
    out_summary_dir = os.path.dirname(os.path.abspath(args.out_summary)) or "."
    if not os.path.exists(out_events_dir):
        os.makedirs(out_events_dir)
    if not os.path.exists(out_summary_dir):
        os.makedirs(out_summary_dir)

    # Shared summary dict + locks
    summary_global = {}
    summary_lock = threading.Lock()

    # Writer queue + thread
    event_q = queue.Queue(maxsize=50000)
    stop_evt = threading.Event()
    wt = threading.Thread(target=writer_thread_fn, args=(event_q, args.out_events, stop_evt))
    wt.daemon = True
    wt.start()

    progress = {"events": 0, "filtered": 0, "bad_json": 0, "progress_every": args.progress_every}
    progress_lock = threading.Lock()

    print("Processing {} files with threads={}".format(len(files), args.threads), file=sys.stderr)

    try:
        with ThreadPoolExecutor(max_workers=args.threads) as ex:
            futures = []
            for fp in files:
                futures.append(ex.submit(
                    process_file,
                    fp,
                    event_q,
                    summary_global,
                    summary_lock,
                    args.dedup_minutes,
                    progress,
                    progress_lock,
                    args.skip_nonjson
                ))
            for fut in as_completed(futures):
                fut.result()

        # finish writer
        event_q.put(None)
        event_q.join()
        stop_evt.set()
        wt.join(60)

        print("Writing summary CSV to {} (rows={:,})".format(args.out_summary, len(summary_global)), file=sys.stderr)
        write_summary_csv(summary_global, args.out_summary)

        print("Done.\n  files={}\n  events={:,}\n  filtered={:,}\n  bad_json={:,}\n  events_csv={}\n  summary_csv={}".format(
            len(files), progress["events"], progress["filtered"], progress["bad_json"], args.out_events, args.out_summary
        ), file=sys.stderr)

    finally:
        try:
            stop_evt.set()
            try:
                event_q.put_nowait(None)
            except Exception:
                pass
        except Exception:
            pass


if __name__ == "__main__":
    main()
