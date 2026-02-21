#!/usr/bin/env python3
"""
Ranger audit on HDFS -> two-tier CSV outputs with:
  1) file path reduced to directory level (one level above)
  2) noise filtering (_SUCCESS, .crc, _temporary, _spark_metadata, _committed_*, /.Trash/, /.staging/, /tmp/, /user/*/.sparkStaging/)
  3) keep ugi + doAs/proxy info
  4) two outputs:
        a) graph-ready summary (dedup window, default 30 min)
        b) events CSV for all non-filtered events
  5) parallel threads across HDFS files
  6) DIRECTORY-LEVEL DATE FILTERING:
        /ranger/audit/<service>/<YYYYMMDD>/...  (e.g. 20260218)
     Use --since/--until or --last-months (default 3).

Assumes:
  - You have a valid Kerberos ticket (kinit already done)
  - Ranger audits on HDFS are JSON-per-line
"""

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
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


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

NOISE_FILENAMES = {"_SUCCESS"}


# -------------------------
# HDFS subprocess helpers
# -------------------------

def run_cmd(cmd: List[str]) -> Tuple[int, str, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    return p.returncode, out, err


def hdfs_ls(path: str) -> List[str]:
    """
    Return `hdfs dfs -ls` lines (excluding header).
    """
    rc, out, err = run_cmd(["hdfs", "dfs", "-ls", path])
    if rc != 0:
        # if path doesn't exist, treat as empty
        return []
    lines = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        lines.append(line)
    return lines


def hdfs_find_files(hdfs_root: str) -> List[str]:
    rc, out, err = run_cmd(["hdfs", "dfs", "-find", hdfs_root, "-type", "f"])
    if rc != 0:
        raise RuntimeError(f"Failed to list HDFS files under {hdfs_root}\n{err.strip()}")
    return [line.strip() for line in out.splitlines() if line.strip()]


def hdfs_stream_text(file_path: str) -> Iterable[str]:
    """
    Stream file content from HDFS using `hdfs dfs -text` (often handles compressed files).
    Falls back to `hdfs dfs -cat` if -text fails.
    """
    p = subprocess.Popen(["hdfs", "dfs", "-text", file_path],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert p.stdout is not None
    for line in p.stdout:
        yield line
    _, err = p.communicate()
    if p.returncode == 0:
        return

    p2 = subprocess.Popen(["hdfs", "dfs", "-cat", file_path],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert p2.stdout is not None
    for line in p2.stdout:
        yield line
    _, err2 = p2.communicate()
    if p2.returncode != 0:
        raise RuntimeError(
            f"Failed to read {file_path}\n"
            f"-text err: {err.strip()}\n"
            f"-cat err: {err2.strip()}"
        )


# -------------------------
# Date folder pruning
# -------------------------

DATE_DIR_RE = re.compile(r"/(\d{8})(?:/)?$")

def parse_yyyymmdd(s: str) -> dt.date:
    return dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def list_service_dirs(audit_root: str) -> List[str]:
    """
    Expect /ranger/audit/<service> directories.
    """
    lines = hdfs_ls(audit_root)
    dirs = []
    for line in lines:
        parts = line.split()
        # last column is path
        if len(parts) < 8:
            continue
        p = parts[-1]
        # 'd' means directory
        if parts[0].startswith("d"):
            dirs.append(p)
    return dirs


def list_date_dirs(service_dir: str, since: dt.date, until: dt.date) -> List[str]:
    """
    Support both layouts:
      A) /ranger/audit/<service>/<YYYYMMDD>/
      B) /ranger/audit/<service>/<service>/<YYYYMMDD>/
    """
    # First, list direct children of /ranger/audit/<service>
    lines = hdfs_ls(service_dir)

    # Collect child directories
    child_dirs = []
    for line in lines:
        parts = line.split()
        if len(parts) < 8 or not parts[0].startswith("d"):
            continue
        child_dirs.append(parts[-1])

    # 1) Try layout A: date dirs directly under service_dir
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

    # 2) Fall back to layout B: there is an inner <service> folder
    # Find exactly one inner folder whose basename matches the outer service basename
    outer = os.path.basename(service_dir.rstrip("/"))
    inner_dir = None
    for p in child_dirs:
        if os.path.basename(p.rstrip("/")) == outer:
            inner_dir = p
            break

    if not inner_dir:
        return []

    inner_lines = hdfs_ls(inner_dir)
    inner_child_dirs = []
    for line in inner_lines:
        parts = line.split()
        if len(parts) < 8 or not parts[0].startswith("d"):
            continue
        inner_child_dirs.append(parts[-1])

    keep2 = []
    for p in inner_child_dirs:
        m = DATE_DIR_RE.search(p)
        if not m:
            continue
        d = parse_yyyymmdd(m.group(1))
        if since <= d <= until:
            keep2.append(p)

    return keep2


def collect_files_in_date_dirs(audit_root: str, since: dt.date, until: dt.date, services: Optional[List[str]] = None) -> List[str]:
    """
    Build list of files to process by:
      - listing service dirs under audit_root
      - for each service dir, list date dirs and keep within range
      - find files under those kept date dirs
    """
    svc_dirs = list_service_dirs(audit_root)

    if services:
        wanted = set(s.strip() for s in services if s.strip())
        svc_dirs = [d for d in svc_dirs if os.path.basename(d) in wanted]

    all_files: List[str] = []
    for svc_dir in sorted(svc_dirs):
        date_dirs = list_date_dirs(svc_dir, since, until)
        for dd in sorted(date_dirs):
            all_files.extend(hdfs_find_files(dd))
    return all_files


# -------------------------
# Parsing + normalization
# -------------------------

WRITE_HINTS = {"write", "update", "create", "delete", "alter", "drop", "truncate", "append", "rename", "setpermission", "setowner", "mkdir", "rmdir", "move"}
READ_HINTS = {"read", "open", "list", "get", "getfileinfo", "getfilestatus", "liststatus", "stat", "content", "execute"}

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def safe_str(x: Any, max_len: int = 4000) -> str:
    if x is None:
        return ""
    s = str(x)
    return s if len(s) <= max_len else (s[:max_len] + "…")

def extract_first(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def to_iso_from_evtTime(evt_time: Any) -> str:
    if evt_time is None:
        return ""
    try:
        ms = int(evt_time)
        if ms < 10_000_000_000:
            ts = dt.datetime.utcfromtimestamp(ms)
        else:
            ts = dt.datetime.utcfromtimestamp(ms / 1000.0)
        return ts.replace(tzinfo=dt.timezone.utc).isoformat()
    except Exception:
        return safe_str(evt_time, 64)

def infer_op(access_type: str, action: str) -> str:
    s = (access_type or "").lower().strip()
    a = (action or "").lower().strip()
    if s in WRITE_HINTS or a in WRITE_HINTS:
        return "WRITE"
    if s in READ_HINTS or a in READ_HINTS:
        return "READ"
    if any(w in s for w in ("write", "create", "delete", "append", "rename", "alter", "drop")):
        return "WRITE"
    return "READ"

def is_noise_path(path: str) -> bool:
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

def reduce_to_directory_level(path: str) -> str:
    if not path:
        return ""
    p = path.strip()
    if p.endswith("/"):
        return p
    parent = os.path.dirname(p)
    if parent and not parent.endswith("/"):
        parent += "/"
    return parent or ""

def parse_audit(obj: Dict[str, Any], source_file: str) -> Optional[Dict[str, Any]]:
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

    result = extract_first(obj, ["result", "accessResult", "isAllowed", "allowed"])
    if result is None:
        result_str = ""
    elif isinstance(result, bool):
        result_str = "ALLOWED" if result else "DENIED"
    else:
        rs = str(result).strip()
        if rs in ("1", "true", "True", "allowed", "ALLOW"):
            result_str = "ALLOWED"
        elif rs in ("0", "false", "False", "denied", "DENY"):
            result_str = "DENIED"
        else:
            result_str = rs[:64]

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


def parse_date_prefix(iso_ts: str) -> Optional[dt.datetime]:
    if not iso_ts:
        return None
    try:
        return dt.datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    except Exception:
        try:
            return dt.datetime.fromisoformat(iso_ts[:19])
        except Exception:
            return None

def floor_time_to_window(ts: dt.datetime, minutes: int) -> dt.datetime:
    discard = ts.minute % minutes
    return ts.replace(minute=ts.minute - discard, second=0, microsecond=0)


# -------------------------
# Two-tier aggregation model
# -------------------------

@dataclass(frozen=True)
class SummaryKey:
    window_start: str
    service: str
    user: str
    do_as: str
    ugi: str
    client_ip: str
    op: str
    resource_norm: str

@dataclass
class SummaryAgg:
    first_seen: str
    last_seen: str
    cnt: int

def update_summary(summary: Dict[SummaryKey, SummaryAgg], rec: Dict[str, Any], dedup_minutes: int) -> None:
    ts = parse_date_prefix(rec.get("event_ts", ""))
    window_start = ""
    if ts is not None:
        window_start = floor_time_to_window(ts, dedup_minutes).isoformat()

    key = SummaryKey(
        window_start=window_start,
        service=rec.get("service", ""),
        user=rec.get("user", ""),
        do_as=rec.get("do_as", ""),
        ugi=rec.get("ugi", ""),
        client_ip=rec.get("client_ip", ""),
        op=rec.get("op", ""),
        resource_norm=rec.get("resource_norm", ""),
    )

    agg = summary.get(key)
    if agg is None:
        summary[key] = SummaryAgg(first_seen=rec.get("event_ts", ""), last_seen=rec.get("event_ts", ""), cnt=1)
    else:
        et = rec.get("event_ts", "")
        if et and (not agg.first_seen or et < agg.first_seen):
            agg.first_seen = et
        if et and (not agg.last_seen or et > agg.last_seen):
            agg.last_seen = et
        agg.cnt += 1


# -------------------------
# Writer + parallel workers
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

def writer_thread_fn(q: "queue.Queue[Optional[Dict[str, Any]]]", out_events_path: str, stop_evt: threading.Event):
    with open(out_events_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
        w.writeheader()
        while not stop_evt.is_set():
            item = q.get()
            if item is None:
                q.task_done()
                break
            w.writerow(item)
            q.task_done()

def process_file(
    file_path: str,
    event_q: "queue.Queue[Optional[Dict[str, Any]]]",
    summary_global: Dict[SummaryKey, SummaryAgg],
    summary_lock: threading.Lock,
    dedup_minutes: int,
    progress: Dict[str, int],
    progress_lock: threading.Lock,
    skip_nonjson: bool = True,
) -> None:
    local_summary: Dict[SummaryKey, SummaryAgg] = {}

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
                print(
                    f"events={progress['events']:,} filtered={progress['filtered']:,} bad_json={progress['bad_json']:,}",
                    file=sys.stderr
                )

    with summary_lock:
        for k, agg in local_summary.items():
            g = summary_global.get(k)
            if g is None:
                summary_global[k] = SummaryAgg(first_seen=agg.first_seen, last_seen=agg.last_seen, cnt=agg.cnt)
            else:
                if agg.first_seen and (not g.first_seen or agg.first_seen < g.first_seen):
                    g.first_seen = agg.first_seen
                if agg.last_seen and (not g.last_seen or agg.last_seen > g.last_seen):
                    g.last_seen = agg.last_seen
                g.cnt += agg.cnt

def write_summary_csv(summary: Dict[SummaryKey, SummaryAgg], out_summary_path: str):
    with open(out_summary_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS)
        w.writeheader()
        for k, agg in sorted(summary.items(), key=lambda kv: (kv[0].window_start, kv[0].service, kv[0].user, kv[0].client_ip, kv[0].op, kv[0].resource_norm)):
            w.writerow({
                "window_start": k.window_start,
                "service": k.service,
                "user": k.user,
                "do_as": k.do_as,
                "ugi": k.ugi,
                "client_ip": k.client_ip,
                "op": k.op,
                "resource_norm": k.resource_norm,
                "first_seen": agg.first_seen,
                "last_seen": agg.last_seen,
                "cnt": agg.cnt,
            })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hdfs-root", required=True, help="HDFS audit root, e.g. hdfs:///ranger/audit")
    ap.add_argument("--out-events", required=True, help="Local output CSV path for per-event records")
    ap.add_argument("--out-summary", required=True, help="Local output CSV path for dedup summary")
    ap.add_argument("--threads", type=int, default=8, help="Parallel threads across HDFS files")
    ap.add_argument("--dedup-minutes", type=int, default=30, help="Dedup window minutes for summary tier")

    # Date control
    ap.add_argument("--since", default=None, help="Start date folder inclusive: YYYYMMDD (e.g. 20260201)")
    ap.add_argument("--until", default=None, help="End date folder inclusive: YYYYMMDD (e.g. 20260220)")
    ap.add_argument("--last-months", type=int, default=1, help="If --since/--until not provided, use last N months (default 1)")

    # Scope
    ap.add_argument("--services", default=None, help="Comma-separated services under audit root (e.g. hdfs,hive). If omitted, all.")
    ap.add_argument("--max-files", type=int, default=None, help="Optional hard cap on number of files to process")
    ap.add_argument("--progress-every", type=int, default=200000, help="Print progress every N events")
    ap.add_argument("--skip-nonjson", action="store_true", default=True, help="Skip lines that are not JSON objects")
    args = ap.parse_args()

    # Kerberos sanity
    rc, _, err = run_cmd(["hdfs", "dfs", "-ls", args.hdfs_root])
    if rc != 0:
        raise RuntimeError(f"Cannot access {args.hdfs_root}. Ensure kinit done.\n{err.strip()}")

    # Determine date range
    if args.since or args.until:
        if not (args.since and args.until):
            raise SystemExit("If specifying date range, provide BOTH --since and --until in YYYYMMDD.")
        since = parse_yyyymmdd(args.since)
        until = parse_yyyymmdd(args.until)
    else:
        today = dt.date.today()
        # approximate "last N months" as 30*N days (fast & good enough for folder pruning)
        since = today - dt.timedelta(days=30 * args.last_months)
        until = today

    if since > until:
        raise SystemExit("Invalid range: --since is after --until")

    services = [s.strip() for s in args.services.split(",")] if args.services else None

    print(f"Selecting date-folders from {since.isoformat()} to {until.isoformat()} (inclusive)", file=sys.stderr)
    files = collect_files_in_date_dirs(args.hdfs_root, since, until, services=services)

    if args.max_files is not None:
        files = files[:args.max_files]

    if not files:
        print("No files found in selected date range/services.", file=sys.stderr)
        sys.exit(2)

    os.makedirs(os.path.dirname(os.path.abspath(args.out_events)) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(args.out_summary)) or ".", exist_ok=True)

    summary_global: Dict[SummaryKey, SummaryAgg] = {}
    summary_lock = threading.Lock()

    event_q: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(maxsize=50000)
    stop_evt = threading.Event()
    writer_thread = threading.Thread(target=writer_thread_fn, args=(event_q, args.out_events, stop_evt), daemon=True)
    writer_thread.start()

    progress = {"events": 0, "filtered": 0, "bad_json": 0, "progress_every": args.progress_every}
    progress_lock = threading.Lock()

    print(f"Processing {len(files)} files with threads={args.threads}", file=sys.stderr)

    try:
        with ThreadPoolExecutor(max_workers=args.threads) as ex:
            futures = [
                ex.submit(
                    process_file,
                    fp,
                    event_q,
                    summary_global,
                    summary_lock,
                    args.dedup_minutes,
                    progress,
                    progress_lock,
                    args.skip_nonjson,
                )
                for fp in files
            ]
            for fut in as_completed(futures):
                fut.result()

        event_q.put(None)
        event_q.join()
        stop_evt.set()
        writer_thread.join(timeout=60)

        print(f"Writing summary CSV to {args.out_summary} (rows={len(summary_global):,})", file=sys.stderr)
        write_summary_csv(summary_global, args.out_summary)

        print(
            f"Done.\n"
            f"  files={len(files)}\n"
            f"  events={progress['events']:,}\n"
            f"  filtered={progress['filtered']:,}\n"
            f"  bad_json={progress['bad_json']:,}\n"
            f"  events_csv={args.out_events}\n"
            f"  summary_csv={args.out_summary}",
            file=sys.stderr
        )
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
