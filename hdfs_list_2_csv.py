#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.6.8 compatible.
Reads HDFS file listing from PostgreSQL (cargill_drona_hdfs database),
normalizes paths to a configurable directory depth, and produces
a dependency_objects CSV.

Only minimal fields needed for the dependency model are emitted:
  service, object_type, object_id, owner, group, extra
"""

from __future__ import print_function

import argparse
import csv
import sys

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None


OBJECT_FIELDS = [
    "service", "object_type", "object_id", "owner", "group", "extra",
]


def truncate_path(file_path, depth):
    """Truncate an HDFS path to the given directory depth.

    depth=2 means keep the first 2 components after the leading /.
    Example: /user/spark/data/file.parquet  ->  /user/spark/
    """
    if not file_path:
        return ""
    parts = file_path.strip("/").split("/")
    kept = parts[:depth]
    if not kept:
        return "/"
    return "/" + "/".join(kept) + "/"


def main():
    ap = argparse.ArgumentParser(
        description="Export HDFS listing from PostgreSQL to dependency_objects CSV."
    )
    ap.add_argument("--db-host", default="localhost",
                    help="PostgreSQL host (default: localhost)")
    ap.add_argument("--db-port", type=int, default=5432,
                    help="PostgreSQL port (default: 5432)")
    ap.add_argument("--db-name", default="cargill_drona_hdfs",
                    help="Database name (default: cargill_drona_hdfs)")
    ap.add_argument("--db-user", default="postgres",
                    help="Database user (default: postgres)")
    ap.add_argument("--db-password", default="",
                    help="Database password (default: empty)")
    ap.add_argument("--table", default="list",
                    help="Table name containing HDFS listing (default: list)")
    ap.add_argument("--path-column", default="file_path",
                    help="Column name for file path (default: file_path)")
    ap.add_argument("--owner-column", default="owner",
                    help="Column name for owner (default: owner)")
    ap.add_argument("--group-column", default="group",
                    help='Column name for group (default: group)')
    ap.add_argument("--permissions-column", default="permissions",
                    help="Column name for permissions (default: permissions)")
    ap.add_argument("--depth", type=int, default=3,
                    help="Truncate HDFS path to this directory depth (default: 3)")
    ap.add_argument("--batch-size", type=int, default=10000,
                    help="Rows to fetch per DB cursor batch (controls memory; default: 10000)")
    ap.add_argument("--out-objects", default="hdfs_objects.csv",
                    help="Output CSV path (default: hdfs_objects.csv)")
    args = ap.parse_args()

    if psycopg2 is None:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    conn_params = {
        "host": args.db_host,
        "port": args.db_port,
        "dbname": args.db_name,
        "user": args.db_user,
    }
    if args.db_password:
        conn_params["password"] = args.db_password

    print("[hdfs_list_2_csv] Connecting to PostgreSQL {}:{}/{}".format(
        args.db_host, args.db_port, args.db_name), file=sys.stderr)

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor(name="hdfs_listing_cursor")
    cursor.itersize = args.batch_size

    # Quote the table name since "list" is a reserved word in some contexts
    query = 'SELECT "{path}", "{owner}", "{group}", "{perms}" FROM "{table}"'.format(
        path=args.path_column,
        owner=args.owner_column,
        group=args.group_column,
        perms=args.permissions_column,
        table=args.table,
    )

    print("[hdfs_list_2_csv] Running query (batch_size={}): {}".format(
        args.batch_size, query), file=sys.stderr)
    cursor.execute(query)

    seen_dirs = {}
    row_count = 0

    for row in cursor:
        file_path, owner, group, permissions = row
        dir_path = truncate_path(file_path, args.depth)
        if not dir_path or dir_path == "/":
            continue

        row_count += 1
        if row_count % 500000 == 0:
            print("[hdfs_list_2_csv] Processed {:,} rows, {:,} unique dirs".format(
                row_count, len(seen_dirs)), file=sys.stderr)

        if dir_path not in seen_dirs:
            seen_dirs[dir_path] = {
                "owner": owner or "",
                "group": group or "",
                "permissions": permissions or "",
            }

    cursor.close()
    conn.close()

    print("[hdfs_list_2_csv] Processed {:,} total rows -> {:,} unique dirs at depth={}".format(
        row_count, len(seen_dirs), args.depth), file=sys.stderr)

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

    print("[hdfs_list_2_csv] Wrote {} to {}".format(len(seen_dirs), args.out_objects), file=sys.stderr)


if __name__ == "__main__":
    main()
