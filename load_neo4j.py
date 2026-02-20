#!/usr/bin/env python3
import os
import json
import glob
import argparse
from typing import Dict, Any, List, Iterable, Tuple
from neo4j import GraphDatabase


def read_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def batch(iterable, n=1000):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def pick_name(ent: Dict[str, Any]) -> Tuple[str, str]:
    attrs = ent.get("attributes") or {}
    qn = attrs.get("qualifiedName") or ent.get("uniqueAttributes", {}).get("qualifiedName") or ""
    name = attrs.get("name") or attrs.get("displayText") or qn or ent.get("guid")
    return qn, name


def entity_row(ent: Dict[str, Any]) -> Dict[str, Any]:
    qn, name = pick_name(ent)
    return {
        "guid": ent.get("guid"),
        "typeName": ent.get("typeName"),
        "qualifiedName": qn,
        "name": name,
        # Keep a trimmed JSON payload for later enrichment/debug
        "raw": json.dumps(ent, ensure_ascii=False)[:65000],
    }


def parse_lineage_edges(lineage_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Atlas lineage response contains:
      - "guidEntityMap": guid -> entity summary
      - "relations": edges between nodes (including process nodes)
    We'll collapse to entity->entity edges:
      inputs -> outputs (writes), and outputs -> inputs (reads) depending on edge direction.
    """
    lin = (lineage_payload.get("lineage") or {})
    guid_entity = lin.get("guidEntityMap") or {}
    relations = lin.get("relations") or []

    # Helper: identify entity vs process
    def is_process(guid: str) -> bool:
        e = guid_entity.get(guid) or {}
        t = e.get("typeName", "")
        return "process" in t.lower()

    edges = []
    for rel in relations:
        f = rel.get("fromEntityId")
        t = rel.get("toEntityId")
        if not f or not t:
            continue

        # Common pattern: entity -> process -> entity
        # We collapse by later stitching; but for simplicity, record process edges then collapse in a second pass.
        edges.append({"from": f, "to": t})

    # Build adjacency for 2-hop collapse: entity -> process -> entity
    out_adj = {}
    in_adj = {}
    for e in edges:
        out_adj.setdefault(e["from"], []).append(e["to"])
        in_adj.setdefault(e["to"], []).append(e["from"])

    collapsed = []
    for mid in list(guid_entity.keys()):
        if not is_process(mid):
            continue
        ins = in_adj.get(mid, [])
        outs = out_adj.get(mid, [])
        for i in ins:
            for o in outs:
                # i reads into process, process writes o
                # So: o READS_FROM i, and i WRITES_TO o are both meaningful.
                collapsed.append({"src": o, "dst": i, "rel": "READS_FROM"})
                collapsed.append({"src": i, "dst": o, "rel": "WRITES_TO"})

    # De-dup
    uniq = {(c["src"], c["dst"], c["rel"]) for c in collapsed if c["src"] != c["dst"]}
    return [{"src": s, "dst": d, "rel": r} for (s, d, r) in uniq]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--atlas-dump", required=True, help="Directory created by atlas_export.py")
    ap.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    ap.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    ap.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", "neo4j"))
    ap.add_argument("--batch-size", type=int, default=1000)
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))

    with driver.session() as session:
        # Constraints / indexes
        session.run("CREATE CONSTRAINT atlas_guid IF NOT EXISTS FOR (n:AtlasEntity) REQUIRE n.guid IS UNIQUE")
        session.run("CREATE INDEX atlas_qn IF NOT EXISTS FOR (n:AtlasEntity) ON (n.qualifiedName)")
        session.run("CREATE INDEX atlas_type IF NOT EXISTS FOR (n:AtlasEntity) ON (n.typeName)")

    # 1) Load entity nodes
    entity_files = glob.glob(os.path.join(args.atlas_dump, "entities", "*.jsonl"))
    print(f"[Neo4j] Found entity files: {len(entity_files)}")

    def load_entities(rows: List[Dict[str, Any]]):
        q = """
        UNWIND $rows AS row
        MERGE (n:AtlasEntity {guid: row.guid})
        SET n.typeName = row.typeName,
            n.qualifiedName = row.qualifiedName,
            n.name = row.name,
            n.raw = row.raw
        """
        with driver.session() as session:
            session.run(q, rows=rows)

    for f in entity_files:
        print(f"[Neo4j] Loading entities from {f}")
        rows_iter = (entity_row(ent) for ent in read_jsonl(f))
        for b in batch(rows_iter, args.batch_size):
            load_entities(b)

    # 2) Load lineage edges
    lineage_files = glob.glob(os.path.join(args.atlas_dump, "lineage", "*.jsonl"))
    print(f"[Neo4j] Found lineage files: {len(lineage_files)}")

    def load_edges(edges: List[Dict[str, Any]]):
        q = """
        UNWIND $edges AS e
        MATCH (a:AtlasEntity {guid: e.src})
        MATCH (b:AtlasEntity {guid: e.dst})
        CALL {
          WITH a,b,e
          WITH a,b,e WHERE e.rel = 'READS_FROM'
          MERGE (a)-[:READS_FROM]->(b)
          RETURN 0
          UNION
          WITH a,b,e
          WITH a,b,e WHERE e.rel = 'WRITES_TO'
          MERGE (a)-[:WRITES_TO]->(b)
          RETURN 0
        }
        RETURN count(*)
        """
        with driver.session() as session:
            session.run(q, edges=edges)

    for f in lineage_files:
        print(f"[Neo4j] Loading lineage from {f}")
        all_edges_iter = (
            edge
            for rec in read_jsonl(f)
            for edge in parse_lineage_edges(rec)
        )
        for b in batch(all_edges_iter, args.batch_size):
            load_edges(b)

    driver.close()
    print("[Done] Neo4j load completed.")


if __name__ == "__main__":
    main()
