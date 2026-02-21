#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python 3.6.8 compatible.
Reads Atlas JSONL export (from atlas_export.py) and produces:
  (a) dependency_objects CSV  -- one row per entity (hive_table, hdfs_path, etc.)
  (b) dependency_lineage CSV  -- one row per collapsed lineage edge

Only minimal fields needed for the dependency model are emitted.
"""

from __future__ import print_function

import argparse
import csv
import json
import os
import sys


OBJECT_FIELDS = [
    "service", "object_type", "object_id", "owner", "group", "extra",
]

LINEAGE_FIELDS = [
    "from_object_type", "from_object_id",
    "to_object_type", "to_object_id",
    "relation",
]

TYPE_TO_SERVICE = {
    "hive_db": "hive",
    "hive_table": "hive",
    "hive_column": "hive",
    "hive_storagedesc": "hive",
    "hdfs_path": "hdfs",
    "kudu_table": "kudu",
    "impala_table": "impala",
    "impala_db": "impala",
    "spark_application": "spark",
}


def pick_qualified_name(ent):
    attrs = ent.get("attributes") or {}
    return (
        attrs.get("qualifiedName")
        or (ent.get("uniqueAttributes") or {}).get("qualifiedName")
        or ""
    )


def pick_owner(ent):
    attrs = ent.get("attributes") or {}
    return attrs.get("owner") or attrs.get("createdBy") or ""


def entity_to_object_row(ent):
    type_name = ent.get("typeName") or ""
    qn = pick_qualified_name(ent)
    if not qn:
        return None
    service = TYPE_TO_SERVICE.get(type_name, type_name.split("_")[0] if "_" in type_name else "unknown")
    return {
        "service": service,
        "object_type": type_name,
        "object_id": qn,
        "owner": pick_owner(ent),
        "group": "",
        "extra": "",
    }


def parse_lineage_edges(lineage_payload, guid_qn_map):
    """Collapse entity->process->entity into direct edges with qualified names."""
    lin = lineage_payload.get("lineage") or {}
    guid_entity = lin.get("guidEntityMap") or {}
    relations = lin.get("relations") or []

    def is_process(guid):
        e = guid_entity.get(guid) or {}
        return "process" in (e.get("typeName") or "").lower()

    def qn_for(guid):
        if guid in guid_qn_map:
            return guid_qn_map[guid]
        e = guid_entity.get(guid) or {}
        attrs = e.get("attributes") or {}
        return attrs.get("qualifiedName") or (e.get("uniqueAttributes") or {}).get("qualifiedName") or ""

    def type_for(guid):
        e = guid_entity.get(guid) or {}
        return e.get("typeName") or ""

    out_adj = {}
    in_adj = {}
    for rel in relations:
        f = rel.get("fromEntityId")
        t = rel.get("toEntityId")
        if not f or not t:
            continue
        out_adj.setdefault(f, []).append(t)
        in_adj.setdefault(t, []).append(f)

    seen = set()
    rows = []
    for mid in guid_entity:
        if not is_process(mid):
            continue
        for src in in_adj.get(mid, []):
            for dst in out_adj.get(mid, []):
                if src == dst:
                    continue
                src_qn = qn_for(src)
                dst_qn = qn_for(dst)
                if not src_qn or not dst_qn:
                    continue
                key = (src_qn, dst_qn)
                if key not in seen:
                    seen.add(key)
                    rows.append({
                        "from_object_type": type_for(src),
                        "from_object_id": src_qn,
                        "to_object_type": type_for(dst),
                        "to_object_id": dst_qn,
                        "relation": "WRITES_TO",
                    })
                rev_key = (dst_qn, src_qn)
                if rev_key not in seen:
                    seen.add(rev_key)
                    rows.append({
                        "from_object_type": type_for(dst),
                        "from_object_id": dst_qn,
                        "to_object_type": type_for(src),
                        "to_object_id": src_qn,
                        "relation": "READS_FROM",
                    })
    return rows


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def main():
    ap = argparse.ArgumentParser(
        description="Convert Atlas JSONL export to minimal dependency CSVs."
    )
    ap.add_argument("--atlas-dump", required=True,
                    help="Directory created by atlas_export.py (contains entities/ and lineage/ subdirs)")
    ap.add_argument("--out-objects", default="atlas_objects.csv",
                    help="Output CSV for dependency objects (default: atlas_objects.csv)")
    ap.add_argument("--out-lineage", default="atlas_lineage.csv",
                    help="Output CSV for lineage edges (default: atlas_lineage.csv)")
    ap.add_argument("--batch-size", type=int, default=5000,
                    help="Rows to buffer before flushing to CSV (controls memory; default: 5000)")
    args = ap.parse_args()

    entities_dir = os.path.join(args.atlas_dump, "entities")
    lineage_dir = os.path.join(args.atlas_dump, "lineage")

    if not os.path.isdir(entities_dir):
        print("ERROR: entities/ dir not found under {}".format(args.atlas_dump), file=sys.stderr)
        sys.exit(1)

    entity_files = sorted([
        os.path.join(entities_dir, f) for f in os.listdir(entities_dir) if f.endswith(".jsonl")
    ])
    print("[atlas_2_csv] Found {} entity files".format(len(entity_files)), file=sys.stderr)

    guid_qn_map = {}
    obj_count = 0

    with open(args.out_objects, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=OBJECT_FIELDS)
        writer.writeheader()
        buf = []

        for ef in entity_files:
            for ent in read_jsonl(ef):
                guid = ent.get("guid")
                qn = pick_qualified_name(ent)
                if guid and qn:
                    guid_qn_map[guid] = qn

                row = entity_to_object_row(ent)
                if row is None:
                    continue
                buf.append(row)
                obj_count += 1

                if len(buf) >= args.batch_size:
                    writer.writerows(buf)
                    buf = []

        if buf:
            writer.writerows(buf)

    print("[atlas_2_csv] Wrote {} objects to {}".format(obj_count, args.out_objects), file=sys.stderr)

    if not os.path.isdir(lineage_dir):
        print("[atlas_2_csv] No lineage/ dir found; skipping lineage CSV", file=sys.stderr)
        return

    lineage_files = sorted([
        os.path.join(lineage_dir, f) for f in os.listdir(lineage_dir) if f.endswith(".jsonl")
    ])
    print("[atlas_2_csv] Found {} lineage files".format(len(lineage_files)), file=sys.stderr)

    edge_count = 0
    with open(args.out_lineage, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=LINEAGE_FIELDS)
        writer.writeheader()
        buf = []

        for lf in lineage_files:
            for payload in read_jsonl(lf):
                edges = parse_lineage_edges(payload, guid_qn_map)
                for e in edges:
                    buf.append(e)
                    edge_count += 1
                    if len(buf) >= args.batch_size:
                        writer.writerows(buf)
                        buf = []

        if buf:
            writer.writerows(buf)

    print("[atlas_2_csv] Wrote {} lineage edges to {}".format(edge_count, args.out_lineage), file=sys.stderr)
    print("[atlas_2_csv] Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
