#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.6.8 compatible.
Lists all Hive/Impala databases and tables with storage-type metadata
using impala-shell or beeline, and produces a dependency_objects CSV
enriched with storage format (kudu, parquet, orc, text, avro, etc.).

This lets you distinguish Kudu-backed tables from HDFS-backed ones and
accurately attribute access dependencies from Ranger audit logs.
"""

from __future__ import print_function

import argparse
import csv
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_impala(impala_bin, host, port, query, timeout=120, use_ssl=False, kerberos=False):
    """Run a query via impala-shell and return stdout lines."""
    cmd = [impala_bin, "-i", "{}:{}".format(host, port),
           "-B", "--delimited", "--output_delimiter=\t",
           "-q", query]
    if use_ssl:
        cmd.append("--ssl")
    if kerberos:
        cmd.extend(["-k"])
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True)
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        p.communicate()
        return None, "timeout after {}s".format(timeout)
    if p.returncode != 0:
        return None, err.strip()
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    return lines, None


def run_beeline(beeline_bin, jdbc_url, query, timeout=120):
    """Run a query via beeline and return stdout lines."""
    cmd = [beeline_bin, "-u", jdbc_url,
           "--outputformat=tsv2", "--silent=true", "--showHeader=false",
           "-e", query]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True)
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        p.communicate()
        return None, "timeout after {}s".format(timeout)
    if p.returncode != 0:
        return None, err.strip()
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    return lines, None


def run_query(args, query, timeout=120):
    """Run a query using the configured backend."""
    if args.backend == "impala":
        return run_impala(args.impala_bin, args.host, args.port, query,
                          timeout=timeout, use_ssl=args.ssl, kerberos=args.kerberos)
    else:
        return run_beeline(args.beeline_bin, args.jdbc_url, query, timeout=timeout)


def list_databases(args):
    lines, err = run_query(args, "SHOW DATABASES")
    if lines is None:
        print("ERROR listing databases: {}".format(err), file=sys.stderr)
        sys.exit(1)
    dbs = []
    for line in lines:
        parts = line.split("\t")
        db = parts[0].strip()
        if db and db.lower() not in ("name", "database_name", ""):
            dbs.append(db)
    return dbs


def list_tables(args, database):
    lines, err = run_query(args, "SHOW TABLES IN `{}`".format(database))
    if lines is None:
        print("  WARN: cannot list tables in {}: {}".format(database, err), file=sys.stderr)
        return []
    tables = []
    for line in lines:
        parts = line.split("\t")
        tbl = parts[0].strip()
        if tbl and tbl.lower() not in ("name", "tab_name", ""):
            tables.append(tbl)
    return tables


INPUT_FORMAT_MAP = {
    "kudu": "kudu",
    "parquet": "parquet",
    "orc": "orc",
    "text": "textfile",
    "rcfile": "rcfile",
    "avro": "avro",
    "sequencefile": "sequencefile",
    "json": "json",
}


def infer_storage(input_format, serde, tbl_params_lower):
    """Infer storage format from InputFormat, SerDe, and table parameters."""
    if "kudu" in tbl_params_lower or "kudu" in (input_format or "").lower():
        return "kudu"
    fmt_lower = (input_format or "").lower()
    for key, val in INPUT_FORMAT_MAP.items():
        if key in fmt_lower:
            return val
    serde_lower = (serde or "").lower()
    for key, val in INPUT_FORMAT_MAP.items():
        if key in serde_lower:
            return val
    return "unknown"


def describe_table(args, database, table, timeout=60):
    """Run DESCRIBE FORMATTED and parse metadata."""
    fqn = "`{}`.`{}`".format(database, table)
    lines, err = run_query(args, "DESCRIBE FORMATTED {}".format(fqn), timeout=timeout)
    if lines is None:
        return {"error": err}

    meta = {
        "table_type": "",
        "owner": "",
        "input_format": "",
        "serde": "",
        "location": "",
        "tbl_params": "",
        "storage_format": "",
    }

    all_text = "\n".join(lines).lower()

    for line in lines:
        parts = [p.strip() for p in line.split("\t")]
        if len(parts) < 2:
            parts = [p.strip() for p in re.split(r"\s{2,}", line)]
        if len(parts) < 2:
            continue
        key = parts[0].rstrip(":").strip().lower()
        val = parts[1].strip()

        if key in ("table type", "tabletype"):
            meta["table_type"] = val
        elif key == "owner":
            meta["owner"] = val
        elif key in ("inputformat", "input format"):
            meta["input_format"] = val
        elif key in ("serdelib", "serde library", "serdelibrary"):
            meta["serde"] = val
        elif key == "location":
            meta["location"] = val
        elif key == "storage_handler":
            meta["tbl_params"] += " " + val

    meta["tbl_params"] += " " + all_text
    meta["storage_format"] = infer_storage(
        meta["input_format"], meta["serde"], meta["tbl_params"]
    )
    return meta


def describe_one(args, database, table, sleep_ms):
    """Describe a single table with optional sleep."""
    meta = describe_table(args, database, table)
    if sleep_ms > 0:
        time.sleep(sleep_ms / 1000.0)
    return database, table, meta


OBJECTS_FIELDS = ["service", "object_type", "object_id", "owner", "group", "extra"]


def main():
    ap = argparse.ArgumentParser(
        description="List Hive/Impala databases and tables with storage type metadata. "
                    "Produces a dependency_objects CSV enriched with table_type and storage_format."
    )

    backend = ap.add_argument_group("backend (choose one)")
    backend.add_argument("--backend", choices=["impala", "beeline"], default="impala",
                         help="Query backend (default: impala)")
    backend.add_argument("--host", default="localhost",
                         help="Impala daemon host (default: localhost)")
    backend.add_argument("--port", default="21000",
                         help="Impala daemon port (default: 21000)")
    backend.add_argument("--impala-bin", default="impala-shell",
                         help="Path to impala-shell binary (default: impala-shell)")
    backend.add_argument("--ssl", action="store_true", default=False,
                         help="Use SSL for impala-shell")
    backend.add_argument("--kerberos", action="store_true", default=False,
                         help="Use Kerberos auth for impala-shell")
    backend.add_argument("--beeline-bin", default="beeline",
                         help="Path to beeline binary (default: beeline)")
    backend.add_argument("--jdbc-url", default="",
                         help="JDBC URL for beeline (e.g. jdbc:hive2://host:10000/default)")

    scope = ap.add_argument_group("scope")
    scope.add_argument("--databases", default=None,
                       help="Comma-separated list of databases to scan (default: all)")
    scope.add_argument("--exclude-databases", default="sys,information_schema,_impala_builtins",
                       help="Comma-separated databases to skip "
                            "(default: sys,information_schema,_impala_builtins)")
    scope.add_argument("--max-databases", type=int, default=None,
                       help="Cap on number of databases to scan (for testing)")
    scope.add_argument("--max-tables-per-db", type=int, default=None,
                       help="Cap on tables per database (for testing)")
    scope.add_argument("--skip-describe", action="store_true", default=False,
                       help="Only list databases/tables; skip DESCRIBE FORMATTED "
                            "(fast, but no storage type info)")

    perf = ap.add_argument_group("performance")
    perf.add_argument("--workers", type=int, default=4,
                      help="Parallel workers for DESCRIBE calls (default: 4)")
    perf.add_argument("--sleep-ms", type=int, default=0,
                      help="Sleep between DESCRIBE calls per worker in ms (default: 0)")
    perf.add_argument("--timeout", type=int, default=60,
                      help="Timeout per CLI call in seconds (default: 60)")

    output = ap.add_argument_group("output")
    output.add_argument("--out-objects", default="hive_tables.csv",
                        help="Output CSV path (default: hive_tables.csv)")

    args = ap.parse_args()

    if args.backend == "beeline" and not args.jdbc_url:
        ap.error("--jdbc-url is required when using --backend beeline")

    exclude_set = set()
    if args.exclude_databases:
        exclude_set = set(d.strip().lower() for d in args.exclude_databases.split(",") if d.strip())

    # List databases
    if args.databases:
        databases = [d.strip() for d in args.databases.split(",") if d.strip()]
    else:
        print("[hive_tables] Listing databases...", file=sys.stderr)
        databases = list_databases(args)

    databases = [d for d in databases if d.lower() not in exclude_set]
    if args.max_databases:
        databases = databases[:args.max_databases]

    print("[hive_tables] Found {} databases".format(len(databases)), file=sys.stderr)

    # List tables per database
    all_tables = []
    for i, db in enumerate(databases):
        tables = list_tables(args, db)
        if args.max_tables_per_db:
            tables = tables[:args.max_tables_per_db]
        for t in tables:
            all_tables.append((db, t))
        if (i + 1) % 50 == 0 or (i + 1) == len(databases):
            print("[hive_tables] Listed {}/{} databases, {} tables so far".format(
                i + 1, len(databases), len(all_tables)), file=sys.stderr)

    print("[hive_tables] Total tables found: {}".format(len(all_tables)), file=sys.stderr)

    out_dir = os.path.dirname(os.path.abspath(args.out_objects))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if args.skip_describe:
        print("[hive_tables] Writing table list (no DESCRIBE)...", file=sys.stderr)
        with open(args.out_objects, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=OBJECTS_FIELDS)
            w.writeheader()
            for db, tbl in all_tables:
                w.writerow({
                    "service": "impala",
                    "object_type": "hive_table",
                    "object_id": "{}.{}".format(db, tbl),
                    "owner": "",
                    "group": "",
                    "extra": "",
                })
        print("[hive_tables] Wrote {} rows to {}".format(len(all_tables), args.out_objects),
              file=sys.stderr)
        return

    # Describe tables in parallel
    print("[hive_tables] Describing {} tables with {} workers...".format(
        len(all_tables), args.workers), file=sys.stderr)

    results = []
    done = 0
    errors = 0
    storage_counts = {}

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {}
        for db, tbl in all_tables:
            fut = ex.submit(describe_one, args, db, tbl, args.sleep_ms)
            futures[fut] = (db, tbl)

        for fut in as_completed(futures):
            db, tbl = futures[fut]
            try:
                _, _, meta = fut.result()
            except Exception as e:
                meta = {"error": str(e)}

            if "error" in meta:
                errors += 1
                results.append((db, tbl, {
                    "table_type": "", "owner": "", "storage_format": "error",
                    "location": "", "input_format": "",
                }))
            else:
                sf = meta.get("storage_format", "unknown")
                storage_counts[sf] = storage_counts.get(sf, 0) + 1
                results.append((db, tbl, meta))

            done += 1
            if done % 500 == 0 or done == len(all_tables):
                print("[hive_tables] Described {}/{} tables (errors={})".format(
                    done, len(all_tables), errors), file=sys.stderr)

    # Write output
    with open(args.out_objects, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OBJECTS_FIELDS)
        w.writeheader()
        for db, tbl, meta in sorted(results):
            table_type = meta.get("table_type", "")
            storage = meta.get("storage_format", "")
            location = meta.get("location", "")
            extra_parts = []
            if table_type:
                extra_parts.append("type={}".format(table_type))
            if storage:
                extra_parts.append("storage={}".format(storage))
            if location:
                extra_parts.append("location={}".format(location))

            w.writerow({
                "service": "impala",
                "object_type": "hive_table",
                "object_id": "{}.{}".format(db, tbl),
                "owner": meta.get("owner", ""),
                "group": "",
                "extra": "|".join(extra_parts),
            })

    print("[hive_tables] Wrote {} rows to {}".format(len(results), args.out_objects),
          file=sys.stderr)

    print("[hive_tables] Storage format distribution:", file=sys.stderr)
    for sf, cnt in sorted(storage_counts.items(), key=lambda x: -x[1]):
        print("  {}: {}".format(sf, cnt), file=sys.stderr)


if __name__ == "__main__":
    main()
