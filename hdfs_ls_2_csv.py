#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.6.8 compatible.
Processes the output of `hdfs dfs -ls -R /` and produces a dependency_objects CSV.

Can operate in two modes:
  1. Run the command directly:  --run-cmd (requires hdfs CLI + Kerberos ticket)
  2. Read from a saved file:   --input-file listing.txt
     (Produce the file with: hdfs dfs -ls -R / > listing.txt)

Normalizes paths to a configurable directory depth (default 3) and deduplicates
to one row per unique directory with owner/group/permissions from the first entry seen.

Output schema (dependency_objects):
  service, object_type, object_id, owner, group, extra
"""

from __future__ import print_function

import argparse
import csv
import subprocess
import sys


OBJECT_FIELDS = [
    "service", "object_type", "object_id", "owner", "group", "extra",
]

# hdfs dfs -ls output format:
#   drwxr-xr-x   - hdfs supergroup          0 2025-01-01 12:00 /user
#   -rw-r--r--   3 spark spark      12345678 2025-01-01 12:00 /user/spark/file.parquet
# Fields (space-separated, 8 tokens minimum):
#   [0] permissions  [1] replication  [2] owner  [3] group
#   [4] size         [5] date         [6] time   [7..] path


def parse_ls_line(line):
    """Parse one line of `hdfs dfs -ls` output.

    Returns (path, owner, group, permissions) or None if unparseable.
    """
    line = line.rstrip("\n").rstrip("\r")
    if not line or line.startswith("Found "):
        return None

    parts = line.split(None, 7)
    if len(parts) < 8:
        return None

    permissions = parts[0]
    owner = parts[2]
    group = parts[3]
    path = parts[7]

    if not path.startswith("/"):
        return None

    return path, owner, group, permissions


def truncate_path(file_path, depth):
    """Truncate an HDFS path to the given directory depth.

    depth=2: /user/spark/data/file.parquet  ->  /user/spark/
    depth=3: /user/spark/data/file.parquet  ->  /user/spark/data/
    """
    if not file_path:
        return ""
    parts = file_path.strip("/").split("/")
    kept = parts[:depth]
    if not kept:
        return "/"
    return "/" + "/".join(kept) + "/"


def stream_from_command(hdfs_bin, hdfs_path):
    """Run `hdfs dfs -ls -R <path>` and stream stdout lines."""
    cmd = [hdfs_bin, "dfs", "-ls", "-R", hdfs_path]
    print("[hdfs_ls_2_csv] Running: {}".format(" ".join(cmd)), file=sys.stderr)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True, bufsize=1)
    for line in p.stdout:
        yield line
    _, err = p.communicate()
    if p.returncode != 0:
        print("WARNING: hdfs command exited with code {}: {}".format(
            p.returncode, err.strip()[:500]), file=sys.stderr)


def stream_from_file(input_path):
    """Read a saved `hdfs dfs -ls -R` output file line by line."""
    print("[hdfs_ls_2_csv] Reading from file: {}".format(input_path), file=sys.stderr)
    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            yield line


def main():
    ap = argparse.ArgumentParser(
        description=(
            "Process 'hdfs dfs -ls -R' output into dependency_objects CSV. "
            "Either run the command directly (--run-cmd) or read a saved file (--input-file)."
        )
    )

    source = ap.add_mutually_exclusive_group(required=True)
    source.add_argument("--run-cmd", action="store_true", default=False,
                        help="Run 'hdfs dfs -ls -R <path>' directly (requires hdfs CLI + kinit)")
    source.add_argument("--input-file", default=None,
                        help="Path to saved output of 'hdfs dfs -ls -R /' (one per line)")

    ap.add_argument("--hdfs-path", default="/",
                    help="HDFS path to list when using --run-cmd (default: /)")
    ap.add_argument("--hdfs-bin", default="hdfs",
                    help="Path to hdfs CLI binary (default: hdfs)")
    ap.add_argument("--depth", type=int, default=3,
                    help="Truncate HDFS path to this directory depth (default: 3)")
    ap.add_argument("--out-objects", default="hdfs_objects.csv",
                    help="Output CSV path (default: hdfs_objects.csv)")
    args = ap.parse_args()

    if args.run_cmd:
        lines = stream_from_command(args.hdfs_bin, args.hdfs_path)
    else:
        lines = stream_from_file(args.input_file)

    seen_dirs = {}
    line_count = 0

    for line in lines:
        parsed = parse_ls_line(line)
        if parsed is None:
            continue

        path, owner, group, permissions = parsed
        dir_path = truncate_path(path, args.depth)
        if not dir_path or dir_path == "/":
            continue

        line_count += 1
        if line_count % 1000000 == 0:
            print("[hdfs_ls_2_csv] Processed {:,} lines, {:,} unique dirs".format(
                line_count, len(seen_dirs)), file=sys.stderr)

        if dir_path not in seen_dirs:
            seen_dirs[dir_path] = {
                "owner": owner,
                "group": group,
                "permissions": permissions,
            }

    print("[hdfs_ls_2_csv] Processed {:,} entries -> {:,} unique dirs at depth={}".format(
        line_count, len(seen_dirs), args.depth), file=sys.stderr)

    with open(args.out_objects, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=OBJECT_FIELDS)
        writer.writeheader()
        for dir_path in sorted(seen_dirs):
            info = seen_dirs[dir_path]
            writer.writerow({
                "service": "hdfs",
                "object_type": "hdfs_path",
                "object_id": dir_path,
                "owner": info["owner"],
                "group": info["group"],
                "extra": info["permissions"],
            })

    print("[hdfs_ls_2_csv] Wrote {} dirs to {}".format(len(seen_dirs), args.out_objects), file=sys.stderr)


if __name__ == "__main__":
    main()
