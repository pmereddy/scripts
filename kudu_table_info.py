#!/bin/bash

# ---- CONFIG ----
KUDU_MASTER="kudu-master.example.com:7051"
OUTPUT_DIR="./kudu_table_descriptions"
KUDU_BIN=$(which kudu)

# ---- SETUP ----
mkdir -p "$OUTPUT_DIR"

echo "Fetching list of all tables from $KUDU_MASTER..."
TABLES=$($KUDU_BIN table list $KUDU_MASTER)

if [ $? -ne 0 ]; then
  echo "Failed to connect to Kudu master at $KUDU_MASTER"
  exit 1
fi

echo "Total tables: $(echo "$TABLES" | wc -l)"

# ---- PROCESS EACH TABLE ----
for table in $TABLES; do
  safe_name=$(echo "$table" | tr '/' '_' | tr ':' '_')  # sanitize for filename
  outfile="${OUTPUT_DIR}/${safe_name}.json"

  echo "Describing table: $table"
  $KUDU_BIN table describe $KUDU_MASTER "$table" --format json > "$outfile"

  if [ $? -ne 0 ]; then
    echo "⚠️ Failed to describe table: $table" >&2
  else
    echo "✅ Output written to $outfile"
  fi
done

echo "✅ All tables processed. Output directory: $OUTPUT_DIR"
