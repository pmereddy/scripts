#!/usr/bin/env bash
#
# Lists all Hive/Impala databases and tables with storage type metadata
# using impala-shell. Produces a dependency_objects CSV compatible with
# load_neo4j.py --objects-csv.
#
# Usage:
#   # List only (fast, no DESCRIBE):
#   bash hive_tables_2_csv.sh -H impalad-host -o hive_tables.csv
#
#   # With storage type detection:
#   bash hive_tables_2_csv.sh -H impalad-host -d -o hive_tables.csv
#
#   # Specific databases, parallel describe:
#   bash hive_tables_2_csv.sh -H impalad-host -d -w 8 -D "db1,db2" -o hive_tables.csv
#
#   # Using beeline instead:
#   bash hive_tables_2_csv.sh -b "jdbc:hive2://host:10000/default" -d -o hive_tables.csv
#
set -euo pipefail

# ---------- defaults ----------
IMPALA_HOST="localhost"
IMPALA_PORT="21000"
USE_SSL=""
USE_KERBEROS=""
BACKEND="impala"        # impala or beeline
BEELINE_URL=""
OUT_FILE="hive_tables.csv"
DO_DESCRIBE=0
WORKERS=4
DATABASES=""
EXCLUDE_DBS="sys,information_schema,_impala_builtins"
MAX_DBS=0
MAX_TABLES_PER_DB=0
TIMEOUT=120

usage() {
    cat <<'EOF'
hive_tables_2_csv.sh -- list Hive/Impala tables with storage type

OPTIONS:
  -H HOST        Impala daemon host (default: localhost)
  -P PORT        Impala daemon port (default: 21000)
  -k             Use Kerberos (-k flag for impala-shell)
  -s             Use SSL (--ssl flag for impala-shell)
  -b URL         Use beeline with this JDBC URL instead of impala-shell
  -o FILE        Output CSV path (default: hive_tables.csv)
  -d             Run DESCRIBE FORMATTED to detect storage type
  -w N           Parallel workers for DESCRIBE (default: 4)
  -D "db1,db2"   Only scan these databases (comma-separated)
  -E "db1,db2"   Exclude these databases (default: sys,information_schema,_impala_builtins)
  -m N           Max databases to scan (0=all)
  -t N           Max tables per database (0=all)
  -T SECS        Timeout per query in seconds (default: 120)
  -h             Show this help
EOF
    exit 0
}

while getopts "H:P:ksb:o:dw:D:E:m:t:T:h" opt; do
    case "$opt" in
        H) IMPALA_HOST="$OPTARG" ;;
        P) IMPALA_PORT="$OPTARG" ;;
        k) USE_KERBEROS="-k" ;;
        s) USE_SSL="--ssl" ;;
        b) BACKEND="beeline"; BEELINE_URL="$OPTARG" ;;
        o) OUT_FILE="$OPTARG" ;;
        d) DO_DESCRIBE=1 ;;
        w) WORKERS="$OPTARG" ;;
        D) DATABASES="$OPTARG" ;;
        E) EXCLUDE_DBS="$OPTARG" ;;
        m) MAX_DBS="$OPTARG" ;;
        t) MAX_TABLES_PER_DB="$OPTARG" ;;
        T) TIMEOUT="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# ---------- query helpers ----------

run_query() {
    local query="$1"
    local tmout="${2:-$TIMEOUT}"
    if [ "$BACKEND" = "beeline" ]; then
        timeout "$tmout" beeline -u "$BEELINE_URL" \
            --outputformat=tsv2 --silent=true --showHeader=false \
            -e "$query" 2>/dev/null
    else
        timeout "$tmout" impala-shell \
            -i "${IMPALA_HOST}:${IMPALA_PORT}" \
            -B --delimited --output_delimiter=$'\t' \
            $USE_SSL $USE_KERBEROS \
            -q "$query" 2>/dev/null
    fi
}

# ---------- build exclude set ----------

declare -A EXCLUDE_SET
IFS=',' read -ra _exc <<< "$EXCLUDE_DBS"
for _e in "${_exc[@]}"; do
    _e_lower=$(echo "$_e" | tr '[:upper:]' '[:lower:]' | xargs)
    EXCLUDE_SET["$_e_lower"]=1
done

is_excluded() {
    local db_lower
    db_lower=$(echo "$1" | tr '[:upper:]' '[:lower:]')
    [[ -n "${EXCLUDE_SET[$db_lower]+x}" ]]
}

# ---------- list databases ----------

echo "[hive_tables] Listing databases..." >&2

if [ -n "$DATABASES" ]; then
    IFS=',' read -ra DB_LIST <<< "$DATABASES"
else
    mapfile -t DB_LIST < <(run_query "SHOW DATABASES" | awk -F'\t' '{print $1}' | grep -v -i '^name$' | grep -v '^$')
fi

FILTERED_DBS=()
for db in "${DB_LIST[@]}"; do
    db=$(echo "$db" | xargs)
    [ -z "$db" ] && continue
    if is_excluded "$db"; then
        continue
    fi
    FILTERED_DBS+=("$db")
done

if [ "$MAX_DBS" -gt 0 ] && [ "${#FILTERED_DBS[@]}" -gt "$MAX_DBS" ]; then
    FILTERED_DBS=("${FILTERED_DBS[@]:0:$MAX_DBS}")
fi

echo "[hive_tables] Found ${#FILTERED_DBS[@]} databases" >&2

# ---------- list tables per database ----------

TMPDIR_WORK=$(mktemp -d)
trap 'rm -rf "$TMPDIR_WORK"' EXIT

TABLE_LIST_FILE="$TMPDIR_WORK/all_tables.txt"
> "$TABLE_LIST_FILE"

db_count=0
table_count=0
for db in "${FILTERED_DBS[@]}"; do
    tables=$(run_query "SHOW TABLES IN \`${db}\`" 2>/dev/null | awk -F'\t' '{print $1}' | grep -v -i '^name$' | grep -v '^$') || true

    count=0
    while IFS= read -r tbl; do
        tbl=$(echo "$tbl" | xargs)
        [ -z "$tbl" ] && continue
        echo "${db}	${tbl}" >> "$TABLE_LIST_FILE"
        count=$((count + 1))
        if [ "$MAX_TABLES_PER_DB" -gt 0 ] && [ "$count" -ge "$MAX_TABLES_PER_DB" ]; then
            break
        fi
    done <<< "$tables"

    table_count=$((table_count + count))
    db_count=$((db_count + 1))
    if [ $((db_count % 50)) -eq 0 ] || [ "$db_count" -eq "${#FILTERED_DBS[@]}" ]; then
        echo "[hive_tables] Listed ${db_count}/${#FILTERED_DBS[@]} databases, ${table_count} tables" >&2
    fi
done

echo "[hive_tables] Total tables: ${table_count}" >&2

# ---------- write CSV header ----------

OUT_DIR=$(dirname "$OUT_FILE")
[ -n "$OUT_DIR" ] && [ "$OUT_DIR" != "." ] && mkdir -p "$OUT_DIR"

echo "service,object_type,object_id,owner,group,extra" > "$OUT_FILE"

# ---------- list-only mode (no describe) ----------

if [ "$DO_DESCRIBE" -eq 0 ]; then
    while IFS=$'\t' read -r db tbl; do
        echo "impala,hive_table,${db}.${tbl},,," >> "$OUT_FILE"
    done < "$TABLE_LIST_FILE"
    echo "[hive_tables] Wrote ${table_count} rows to ${OUT_FILE}" >&2
    exit 0
fi

# ---------- describe mode ----------

RESULTS_DIR="$TMPDIR_WORK/results"
mkdir -p "$RESULTS_DIR"

describe_table() {
    local db="$1"
    local tbl="$2"
    local out_file="$3"

    local raw
    raw=$(run_query "DESCRIBE FORMATTED \`${db}\`.\`${tbl}\`" 60 2>/dev/null) || {
        echo "impala,hive_table,${db}.${tbl},,,storage=error" > "$out_file"
        return
    }

    local table_type="" owner="" storage="" location="" input_format=""

    while IFS=$'\t' read -r col1 col2 rest; do
        col1=$(echo "$col1" | sed 's/:$//' | xargs | tr '[:upper:]' '[:lower:]')
        col2=$(echo "$col2" | xargs)
        case "$col1" in
            "table type"|"tabletype")   table_type="$col2" ;;
            "owner")                    owner="$col2" ;;
            "inputformat"|"input format") input_format="$col2" ;;
            "location")                 location="$col2" ;;
            "storage_handler")
                if echo "$col2" | grep -qi "kudu"; then
                    storage="kudu"
                fi
                ;;
        esac
    done <<< "$raw"

    if [ -z "$storage" ]; then
        local fmt_lower
        fmt_lower=$(echo "$input_format" | tr '[:upper:]' '[:lower:]')
        case "$fmt_lower" in
            *kudu*)       storage="kudu" ;;
            *parquet*)    storage="parquet" ;;
            *orc*)        storage="orc" ;;
            *text*)       storage="textfile" ;;
            *avro*)       storage="avro" ;;
            *rcfile*)     storage="rcfile" ;;
            *sequence*)   storage="sequencefile" ;;
            *json*)       storage="json" ;;
            *)            storage="unknown" ;;
        esac
    fi

    # Build extra field, escaping commas in location
    local extra=""
    [ -n "$table_type" ] && extra="type=${table_type}"
    [ -n "$storage" ]    && extra="${extra:+${extra}|}storage=${storage}"
    [ -n "$location" ]   && extra="${extra:+${extra}|}location=${location}"

    # Escape commas in fields for CSV safety
    owner=$(echo "$owner" | tr ',' ';')
    extra=$(echo "$extra" | tr ',' ';')

    echo "impala,hive_table,${db}.${tbl},${owner},,${extra}" > "$out_file"
}

export -f describe_table run_query
export BACKEND BEELINE_URL IMPALA_HOST IMPALA_PORT USE_SSL USE_KERBEROS TIMEOUT

echo "[hive_tables] Describing ${table_count} tables with ${WORKERS} workers..." >&2

# Use xargs for parallelism
done_count=0
total=$(wc -l < "$TABLE_LIST_FILE" | xargs)

cat "$TABLE_LIST_FILE" | while IFS=$'\t' read -r db tbl; do
    done_count=$((done_count + 1))
    echo "${db}	${tbl}	${RESULTS_DIR}/${done_count}.csv"
done > "$TMPDIR_WORK/work_items.txt"

cat "$TMPDIR_WORK/work_items.txt" | \
    xargs -P "$WORKERS" -I {} bash -c '
        IFS=$'"'"'\t'"'"' read -r db tbl out_file <<< "{}"
        describe_table "$db" "$tbl" "$out_file"
    '

echo "[hive_tables] Describe complete. Writing output..." >&2

# Merge results
errors=0
declare -A STORAGE_COUNTS
for f in "$RESULTS_DIR"/*.csv; do
    [ -f "$f" ] || continue
    while IFS= read -r line; do
        echo "$line" >> "$OUT_FILE"
        # Count storage types
        storage=$(echo "$line" | grep -o 'storage=[^|,]*' | head -1 | sed 's/storage=//')
        if [ -n "$storage" ]; then
            STORAGE_COUNTS["$storage"]=$(( ${STORAGE_COUNTS["$storage"]:-0} + 1 ))
        fi
        if echo "$line" | grep -q 'storage=error'; then
            errors=$((errors + 1))
        fi
    done < "$f"
done

final_count=$(( $(wc -l < "$OUT_FILE" | xargs) - 1 ))
echo "[hive_tables] Wrote ${final_count} rows to ${OUT_FILE} (errors=${errors})" >&2

echo "[hive_tables] Storage format distribution:" >&2
for sf in "${!STORAGE_COUNTS[@]}"; do
    echo "  ${sf}: ${STORAGE_COUNTS[$sf]}" >&2
done | sort -t: -k2 -rn >&2
