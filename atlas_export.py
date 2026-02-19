#!/usr/bin/env python3
import os
import json
import time
import argparse
from typing import Dict, Any, List, Optional
import requests

# Optional Kerberos support:
#   pip install requests-kerberos
try:
    from requests_kerberos import HTTPKerberosAuth, OPTIONAL as KRB_OPTIONAL
    HAS_KRB = True
except Exception:
    HAS_KRB = False


def mk_session(auth_mode: str, user: str, password: str, verify_tls: bool) -> requests.Session:
    s = requests.Session()
    s.verify = verify_tls
    if auth_mode == "basic":
        s.auth = (user, password)
    elif auth_mode == "kerberos":
        if not HAS_KRB:
            raise RuntimeError("requests-kerberos not installed. pip install requests-kerberos")
        s.auth = HTTPKerberosAuth(mutual_authentication=KRB_OPTIONAL)
    else:
        raise ValueError("auth_mode must be 'basic' or 'kerberos'")
    s.headers.update({"Content-Type": "application/json"})
    return s


def rj(resp: requests.Response) -> Dict[str, Any]:
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:5000]}")
    return resp.json()


def atlas_get(s: requests.Session, base: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return rj(s.get(f"{base}{path}", params=params, timeout=60))


def atlas_post(s: requests.Session, base: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return rj(s.post(f"{base}{path}", data=json.dumps(payload), timeout=120))


def write_jsonl(path: str, rows: List[Dict[str, Any]]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def list_typedefs(s: requests.Session, base: str) -> Dict[str, Any]:
    # Atlas v2 typedefs endpoint
    return atlas_get(s, base, "/api/atlas/v2/types/typedefs")


def search_entities_basic(
    s: requests.Session,
    base: str,
    type_name: str,
    limit: int,
    offset: int,
    classification: Optional[str] = None,
    query: Optional[str] = None
) -> Dict[str, Any]:
    # Basic Search supports typeName/classification/query/limit/offset. :contentReference[oaicite:1]{index=1}
    payload = {
        "typeName": type_name,
        "limit": limit,
        "offset": offset
    }
    if classification:
        payload["classification"] = classification
    if query:
        payload["query"] = query
    return atlas_post(s, base, "/api/atlas/v2/search/basic", payload)


def get_entity_bulk(s: requests.Session, base: str, guids: List[str]) -> Dict[str, Any]:
    # Bulk entity fetch by GUIDs (commonly available in Atlas v2). :contentReference[oaicite:2]{index=2}
    payload = {"guids": guids}
    return atlas_post(s, base, "/api/atlas/v2/entity/bulk", payload)


def get_lineage(s: requests.Session, base: str, guid: str, direction: str, depth: int) -> Dict[str, Any]:
    # Lineage REST endpoint. :contentReference[oaicite:3]{index=3}
    params = {"direction": direction, "depth": depth}
    return atlas_get(s, base, f"/api/atlas/v2/lineage/{guid}", params=params)


def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True, help="e.g. https://atlas-host:21000")
    ap.add_argument("--auth-mode", choices=["basic", "kerberos"], default=os.getenv("ATLAS_AUTH_MODE", "basic"))
    ap.add_argument("--user", default=os.getenv("ATLAS_USER", "admin"))
    ap.add_argument("--password", default=os.getenv("ATLAS_PASSWORD", "admin"))
    ap.add_argument("--outdir", default="atlas_export_out")
    ap.add_argument("--verify-tls", action="store_true", default=False, help="Enable TLS cert verification")
    ap.add_argument("--types", default="hive_db,hive_table,hive_column,hdfs_path,process",
                    help="Comma-separated list of Atlas entity types to export")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--bulk-guid-batch", type=int, default=50)
    ap.add_argument("--export-full-entity", action="store_true", default=True)
    ap.add_argument("--export-lineage", action="store_true", default=True)
    ap.add_argument("--lineage-depth", type=int, default=3)
    ap.add_argument("--lineage-direction", choices=["INPUT", "OUTPUT", "BOTH"], default="BOTH")
    ap.add_argument("--sleep-ms", type=int, default=50, help="Small throttle between requests")
    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    s = mk_session(args.auth_mode, args.user, args.password, args.verify_tls)

    os.makedirs(args.outdir, exist_ok=True)

    # 1) Export typedefs
    typedefs = list_typedefs(s, base)
    with open(os.path.join(args.outdir, "typedefs.json"), "w", encoding="utf-8") as f:
        json.dump(typedefs, f, indent=2)

    # 2) Export entities by type
    types = [t.strip() for t in args.types.split(",") if t.strip()]
    all_guids_by_type: Dict[str, List[str]] = {}

    for t in types:
        print(f"[Atlas] Exporting type={t}")
        offset = 0
        total = None
        type_guids: List[str] = []

        while True:
            res = search_entities_basic(s, base, t, limit=args.limit, offset=offset)
            entities = res.get("entities") or []
            approx_count = res.get("approximateCount")
            if total is None and approx_count is not None:
                total = approx_count

            guids = [e.get("guid") for e in entities if e.get("guid")]
            if not guids:
                break

            # Save lightweight search results
            write_jsonl(os.path.join(args.outdir, "search", f"{t}.jsonl"), entities)

            type_guids.extend(guids)
            offset += args.limit

            if total is not None and offset >= total:
                break

            time.sleep(args.sleep_ms / 1000.0)

        all_guids_by_type[t] = list(dict.fromkeys(type_guids))
        print(f"  -> {len(all_guids_by_type[t])} GUIDs")

        # 3) Optionally export full entity payloads
        if args.export_full_entity and all_guids_by_type[t]:
            for batch in chunked(all_guids_by_type[t], args.bulk_guid_batch):
                bulk = get_entity_bulk(s, base, batch)
                rows = bulk.get("entities") or []
                write_jsonl(os.path.join(args.outdir, "entities", f"{t}.jsonl"), rows)
                time.sleep(args.sleep_ms / 1000.0)

        # 4) Optionally export lineage per GUID
        if args.export_lineage and all_guids_by_type[t]:
            for guid in all_guids_by_type[t]:
                try:
                    lin = get_lineage(s, base, guid, args.lineage_direction, args.lineage_depth)
                    write_jsonl(os.path.join(args.outdir, "lineage", f"{t}.jsonl"), [{
                        "seedGuid": guid,
                        "typeName": t,
                        "lineage": lin
                    }])
                except Exception as e:
                    write_jsonl(os.path.join(args.outdir, "errors", "lineage_errors.jsonl"), [{
                        "seedGuid": guid,
                        "typeName": t,
                        "error": str(e)
                    }])
                time.sleep(args.sleep_ms / 1000.0)

    with open(os.path.join(args.outdir, "guids_by_type.json"), "w", encoding="utf-8") as f:
        json.dump(all_guids_by_type, f, indent=2)

    print(f"[Done] Output in: {args.outdir}")


if __name__ == "__main__":
    main()
