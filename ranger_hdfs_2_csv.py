#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Python 3.6 compatible.
Reads Ranger audit logs directly from HDFS (Kerberos ticket required), filters noise,
reduces file path to directory level (one level above), and produces:
  (a) events CSV   -- all non-filtered events (optional; skip with --skip-events)
  (b) summary CSV  -- graph-ready, 30-min dedup window by default
  (c) dependency CSV -- compact common-schema output for dependency modeling
       with configurable resource depth, time window, and key dimensions

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

FILTER_OPS_DEFAULT = "getfileinfo,liststatus,getfilestatus"


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
    lines = hdfs_ls(service_dir)
    child_dirs = []
    for line in lines:
        parts = line.split()
        if len(parts) < 8 or not parts[0].startswith("d"):
            continue
        child_dirs.append(parts[-1])

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
        return s[:max_len]
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


def truncate_path(file_path, depth):
    """Truncate path to the given directory depth.
    depth=3: /user/spark/data/2024/01/file.parquet -> /user/spark/data/
    """
    if not file_path:
        return ""
    parts = file_path.strip("/").split("/")
    kept = parts[:depth]
    if not kept:
        return "/"
    return "/" + "/".join(kept) + "/"


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
    discard = ts.minute % minutes
    return ts.replace(minute=ts.minute - discard, second=0, microsecond=0)


def floor_dep_window(ts, dep_window):
    """Floor a datetime to the dependency time window granularity."""
    if dep_window == "none":
        return ""
    if dep_window == "daily":
        return ts.strftime("%Y-%m-%d")
    if dep_window == "monthly":
        return ts.strftime("%Y-%m")
    if dep_window == "1h":
        return ts.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    if dep_window == "6h":
        h = ts.hour - (ts.hour % 6)
        return ts.replace(hour=h, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


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


def make_dep_key(window_start, rec, resource_depth, include_ip, include_app):
    object_id = rec.get("resource_norm", "")
    if resource_depth is not None:
        object_id = truncate_path(object_id, resource_depth)

    key = [
        window_start,
        rec.get("service", ""),
        rec.get("user", ""),
        rec.get("do_as", ""),
        rec.get("op", ""),
        object_id,
    ]
    if include_ip:
        key.append(rec.get("client_ip", ""))
    if include_app:
        key.append(rec.get("app_name", ""))
    return tuple(key)


def _update_agg(agg_dict, key, event_ts):
    agg = agg_dict.get(key)
    if agg is None:
        agg_dict[key] = {"first_seen": event_ts, "last_seen": event_ts, "cnt": 1}
    else:
        if event_ts and (not agg["first_seen"] or event_ts < agg["first_seen"]):
            agg["first_seen"] = event_ts
        if event_ts and (not agg["last_seen"] or event_ts > agg["last_seen"]):
            agg["last_seen"] = event_ts
        agg["cnt"] += 1


def _update_dep_agg(agg_dict, key, event_ts, rec, include_ip, include_app):
    """Like _update_agg but also tracks sample values for non-key fields."""
    agg = agg_dict.get(key)
    if agg is None:
        agg_dict[key] = {
            "first_seen": event_ts,
            "last_seen": event_ts,
            "cnt": 1,
            "sample_ip": rec.get("client_ip", "") if not include_ip else "",
            "sample_app": rec.get("app_name", "") if not include_app else "",
        }
    else:
        if event_ts and (not agg["first_seen"] or event_ts < agg["first_seen"]):
            agg["first_seen"] = event_ts
        if event_ts and (not agg["last_seen"] or event_ts > agg["last_seen"]):
            agg["last_seen"] = event_ts
        agg["cnt"] += 1


def update_summary(summary_dict, rec, dedup_minutes,
                   dep_dict=None, dep_window="daily", resource_depth=None,
                   include_ip=False, include_app=False, filter_ops_set=None):
    ts = parse_ts_utc_z(rec.get("event_ts", ""))
    window_start = ""
    if ts is not None:
        window_start = floor_time_to_window(ts, dedup_minutes).strftime("%Y-%m-%dT%H:%M:%SZ")

    k = make_summary_key(window_start, rec)
    _update_agg(summary_dict, k, rec.get("event_ts", ""))

    if dep_dict is not None:
        if filter_ops_set and rec.get("access_type", "").lower().strip() in filter_ops_set:
            return

        dep_ws = ""
        if ts is not None:
            dep_ws = floor_dep_window(ts, dep_window)

        dk = make_dep_key(dep_ws, rec, resource_depth, include_ip, include_app)
        _update_dep_agg(dep_dict, dk, rec.get("event_ts", ""), rec, include_ip, include_app)


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

DEPENDENCY_ACCESS_FIELDS = [
    "window_start", "service", "user", "do_as", "client_ip",
    "app_name", "op", "object_type", "object_id", "cnt",
]

SERVICE_TO_OBJECT_TYPE = {
    "hdfs": "hdfs_path",
    "hive": "hive_table",
    "kudu": "kudu_table",
    "impala": "hive_table",
    "yarn": "application",
    "spark": "application",
}


def infer_object_type(service_name):
    svc = (service_name or "").lower()
    for key, otype in SERVICE_TO_OBJECT_TYPE.items():
        if key in svc:
            return otype
    return "unknown"


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


def _merge_agg_dicts(target, source):
    for k, agg in source.items():
        g = target.get(k)
        if g is None:
            target[k] = agg
        else:
            if agg["first_seen"] and (not g["first_seen"] or agg["first_seen"] < g["first_seen"]):
                g["first_seen"] = agg["first_seen"]
            if agg["last_seen"] and (not g["last_seen"] or agg["last_seen"] > g["last_seen"]):
                g["last_seen"] = agg["last_seen"]
            g["cnt"] += agg["cnt"]


def process_file(file_path, event_q, summary_global, summary_lock,
                 dedup_minutes, progress, progress_lock, skip_nonjson,
                 dep_global=None, dep_window="daily", resource_depth=None,
                 include_ip=False, include_app=False, filter_ops_set=None,
                 skip_events=False):
    local_summary = {}
    local_dep = {} if dep_global is not None else None

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

        if not skip_events:
            event_q.put(rec)

        update_summary(local_summary, rec, dedup_minutes,
                       dep_dict=local_dep, dep_window=dep_window,
                       resource_depth=resource_depth,
                       include_ip=include_ip, include_app=include_app,
                       filter_ops_set=filter_ops_set)

        with progress_lock:
            progress["events"] += 1
            if progress["events"] % progress["progress_every"] == 0:
                print("events={:,} filtered={:,} bad_json={:,}".format(
                    progress["events"], progress["filtered"], progress["bad_json"]
                ), file=sys.stderr)

    with summary_lock:
        _merge_agg_dicts(summary_global, local_summary)
        if dep_global is not None and local_dep is not None:
            _merge_agg_dicts(dep_global, local_dep)


def write_summary_csv(summary, out_summary_path):
    with open(out_summary_path, "w") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS)
        w.writeheader()

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


def write_dependency_csv(dep_dict, out_path, include_ip, include_app):
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=DEPENDENCY_ACCESS_FIELDS)
        w.writeheader()
        for k in sorted(dep_dict.keys()):
            agg = dep_dict[k]
            idx = 0
            window_start = k[idx]; idx += 1
            service_raw  = k[idx]; idx += 1
            user         = k[idx]; idx += 1
            do_as        = k[idx]; idx += 1
            op           = k[idx]; idx += 1
            object_id    = k[idx]; idx += 1

            if include_ip:
                client_ip = k[idx]; idx += 1
            else:
                client_ip = agg.get("sample_ip", "")

            if include_app:
                app_name = k[idx]; idx += 1
            else:
                app_name = agg.get("sample_app", "")

            w.writerow({
                "window_start": window_start,
                "service": service_raw,
                "user": user,
                "do_as": do_as,
                "client_ip": client_ip,
                "app_name": app_name,
                "op": op,
                "object_type": infer_object_type(service_raw),
                "object_id": object_id,
                "cnt": agg.get("cnt", 0),
            })


def main():
    ap = argparse.ArgumentParser(
        description="Process Ranger audit logs from HDFS into events, summary, and dependency CSVs."
    )
    ap.add_argument("--hdfs-root", required=True, help="HDFS audit root, e.g. hdfs:///ranger/audit")
    ap.add_argument("--out-events", default=None,
                    help="Output CSV for per-event records (omit or use --skip-events to skip)")
    ap.add_argument("--out-summary", required=True, help="Local output CSV path for dedup summary")
    ap.add_argument("--out-dependency", default=None,
                    help="Output CSV path for common-schema dependency_access (minimal fields)")
    ap.add_argument("--skip-events", action="store_true", default=False,
                    help="Skip writing the per-event CSV entirely (saves disk and I/O)")
    ap.add_argument("--threads", type=int, default=8,
                    help="Parallel threads across HDFS files (production: start with 2-4)")
    ap.add_argument("--dedup-minutes", type=int, default=30,
                    help="Dedup window minutes for summary tier (default: 30)")

    dep_group = ap.add_argument_group("dependency output controls",
                                      "Tune dependency CSV size and granularity")
    dep_group.add_argument("--resource-depth", type=int, default=3,
                           help="Truncate resource paths to this directory depth for dependency "
                                "output (default: 3; e.g. /user/spark/data/). "
                                "Set to 0 to use full parent directory (original behavior).")
    dep_group.add_argument("--dep-window", default="daily",
                           choices=["30m", "1h", "6h", "daily", "monthly", "none"],
                           help="Time window granularity for dependency CSV "
                                "(default: daily). 'none' = no time dimension, "
                                "just first_seen/last_seen over entire range.")
    dep_group.add_argument("--dep-include-ip", action="store_true", default=False,
                           help="Include client_ip as a key dimension in dependency output "
                                "(default: excluded to reduce cardinality)")
    dep_group.add_argument("--dep-include-app", action="store_true", default=False,
                           help="Include app_name as a key dimension in dependency output "
                                "(default: excluded to reduce cardinality)")
    dep_group.add_argument("--filter-ops", default=FILTER_OPS_DEFAULT,
                           help="Comma-separated access types to EXCLUDE from dependency output "
                                "(default: {}). These metadata operations inflate HDFS/Impala "
                                "audit logs without adding dependency value. "
                                "Set to empty string to disable filtering.".format(FILTER_OPS_DEFAULT))

    date_group = ap.add_argument_group("date control (folder-based)")
    date_group.add_argument("--since", default=None, help="Start date folder inclusive: YYYYMMDD")
    date_group.add_argument("--until", default=None, help="End date folder inclusive: YYYYMMDD")
    date_group.add_argument("--last-months", type=int, default=3,
                            help="If --since/--until not provided, use last N months (default: 3)")

    scope_group = ap.add_argument_group("scope")
    scope_group.add_argument("--services", default=None,
                             help="Comma-separated services under audit root (e.g. hdfs,hive)")
    scope_group.add_argument("--max-files", type=int, default=None,
                             help="Optional cap on number of files to process")
    scope_group.add_argument("--progress-every", type=int, default=200000,
                             help="Print progress every N events")
    scope_group.add_argument("--skip-nonjson", action="store_true", default=True,
                             help="Skip lines that are not JSON objects")
    args = ap.parse_args()

    skip_events = args.skip_events or (args.out_events is None)

    if not skip_events and args.out_events is None:
        ap.error("--out-events is required unless --skip-events is set")

    resource_depth = args.resource_depth if args.resource_depth > 0 else None

    filter_ops_set = None
    if args.filter_ops and args.filter_ops.strip():
        filter_ops_set = set(op.strip().lower() for op in args.filter_ops.split(",") if op.strip())

    # Kerberos sanity
    rc, _, err = run_cmd(["hdfs", "dfs", "-ls", args.hdfs_root])
    if rc != 0:
        raise RuntimeError("Cannot access {}. Ensure kinit done.\n{}".format(args.hdfs_root, err.strip()))

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
    for out_path in [args.out_events, args.out_summary, args.out_dependency]:
        if out_path:
            out_dir = os.path.dirname(os.path.abspath(out_path))
            if out_dir and not os.path.exists(out_dir):
                os.makedirs(out_dir)

    summary_global = {}
    dep_global = {} if args.out_dependency else None
    summary_lock = threading.Lock()

    event_q = queue.Queue(maxsize=50000)
    stop_evt = threading.Event()
    wt = None

    if not skip_events:
        wt = threading.Thread(target=writer_thread_fn, args=(event_q, args.out_events, stop_evt))
        wt.daemon = True
        wt.start()

    progress = {"events": 0, "filtered": 0, "bad_json": 0, "progress_every": args.progress_every}
    progress_lock = threading.Lock()

    print("Processing {} files with threads={} skip_events={} dep_window={} resource_depth={} filter_ops={}".format(
        len(files), args.threads, skip_events, args.dep_window,
        resource_depth or "parent-dir", args.filter_ops or "none"
    ), file=sys.stderr)

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
                    args.skip_nonjson,
                    dep_global,
                    args.dep_window,
                    resource_depth,
                    args.dep_include_ip,
                    args.dep_include_app,
                    filter_ops_set,
                    skip_events,
                ))
            for fut in as_completed(futures):
                fut.result()

        if not skip_events and wt is not None:
            event_q.put(None)
            event_q.join()
            stop_evt.set()
            wt.join(60)

        print("Writing summary CSV to {} (rows={:,})".format(args.out_summary, len(summary_global)), file=sys.stderr)
        write_summary_csv(summary_global, args.out_summary)

        if args.out_dependency and dep_global is not None:
            print("Writing dependency CSV to {} (rows={:,})".format(
                args.out_dependency, len(dep_global)), file=sys.stderr)
            write_dependency_csv(dep_global, args.out_dependency,
                                args.dep_include_ip, args.dep_include_app)

        parts = [
            "Done.",
            "  files={}".format(len(files)),
            "  events={:,}".format(progress["events"]),
            "  filtered={:,}".format(progress["filtered"]),
            "  bad_json={:,}".format(progress["bad_json"]),
            "  summary_csv={} ({:,} rows)".format(args.out_summary, len(summary_global)),
        ]
        if not skip_events:
            parts.append("  events_csv={}".format(args.out_events))
        if args.out_dependency and dep_global is not None:
            parts.append("  dependency_csv={} ({:,} rows)".format(args.out_dependency, len(dep_global)))
        print("\n".join(parts), file=sys.stderr)

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
