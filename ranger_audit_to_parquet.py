#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input path (HDFS or local). Can be glob. e.g. hdfs:///ranger/audit/*/*")
    ap.add_argument("--out", required=True, help="Output base path for parquet, e.g. hdfs:///datalake/ranger_audit_parquet")
    ap.add_argument("--service", default=None, help="Optional: set service name if not present in data/path")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("ranger-audit-to-parquet").getOrCreate()

    # Ranger audit is often JSON-per-line. :contentReference[oaicite:9]{index=9}
    df = spark.read.json(args.input)

    # Normalize columns defensively (schemas vary by plugin/version)
    # Common useful fields (often present): evtTime, accessType, action, result, resourcePath,
    # repoName (service), user, clientIP, requestData, policyId, tags, etc.
    def col_or_null(name, dtype="string"):
        return F.col(name) if name in df.columns else F.lit(None).cast(dtype)

    norm = (
        df
        .withColumn("event_ts", (col_or_null("evtTime", "long")/1000).cast("timestamp"))
        .withColumn("service", F.coalesce(col_or_null("repoName"), F.lit(args.service)))
        .withColumn("user", F.coalesce(col_or_null("user"), col_or_null("reqUser")))
        .withColumn("client_ip", F.coalesce(col_or_null("clientIP"), col_or_null("cliIP")))
        .withColumn("access_type", F.coalesce(col_or_null("accessType"), col_or_null("access")))
        .withColumn("result", F.coalesce(col_or_null("result"), col_or_null("accessResult")))
        .withColumn("resource", F.coalesce(col_or_null("resourcePath"), col_or_null("resourceName")))
        .withColumn("request_data", col_or_null("requestData"))
    )

    # Add partitions
    norm = norm.withColumn("ds", F.date_format(F.col("event_ts"), "yyyy-MM-dd"))

    # Write normalized audit table
    norm.select(
        "ds","event_ts","service","user","client_ip","access_type","result","resource","request_data"
    ).write.mode("append").partitionBy("ds","service").parquet(args.out.rstrip("/") + "/events")

    # Create daily edge aggregates for dependency graph:
    # user/ip -> resource (READ-like) and (WRITE-like) based on access_type/action
    write_types = ["write", "update", "create", "delete", "alter", "drop", "put", "add"]
    edges = (
        norm
        .withColumn("op",
            F.when(F.lower(F.col("access_type")).isin([x.lower() for x in write_types]), F.lit("WRITE"))
             .otherwise(F.lit("READ"))
        )
        .groupBy("ds","service","user","client_ip","resource","op")
        .agg(
            F.count("*").alias("cnt"),
            F.max("event_ts").alias("last_seen"),
            F.min("event_ts").alias("first_seen")
        )
    )

    edges.write.mode("append").partitionBy("ds","service").parquet(args.out.rstrip("/") + "/edges")

    spark.stop()

if __name__ == "__main__":
    main()
