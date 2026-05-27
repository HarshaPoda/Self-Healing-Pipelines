"""
Remediation Engine

Receives anomalies and executes appropriate fixes
Ex:   
  CAST_COLUMN       → update Glue table schema + re-run Athena repair
  UPDATE_GLUE_SCHEMA → add new column to Glue catalog
  FILTER_NULLS      → rewrite the S3 file with nulls removed + flag downstream
  FLAG_AND_MONITOR  → write a flag file to S3, no structural change
Then invokes the audit logger with outcome.
"""

import os, json, boto3, time
from datetime import datetime, timezone

BUCKET        = os.environ["BUCKET_NAME"]
REGION        = os.environ["AWS_REGION_NAME"]
PROJECT_NAME  = "self-healing-pipeline"
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
ATHENA_WG     = os.environ["ATHENA_WORKGROUP"]

s3            = boto3.client("s3",     region_name=REGION)
glue          = boto3.client("glue",   region_name=REGION)
athena        = boto3.client("athena", region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)

GLUE_TABLE    = "github_events_raw"

def get_glue_table() -> dict | None:
    try:
        resp = glue.get_table(DatabaseName=GLUE_DATABASE, Name=GLUE_TABLE)
        return resp["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None

def ensure_glue_table_exists(current_fp: dict):
    """Create the Glue table if it doesn't exist yet."""
    if get_glue_table():
        return

    type_map = {
        "integer":   "bigint",
        "float":     "double",
        "boolean":   "boolean",
        "timestamp": "string",
        "string":    "string",
        "array":     "string",
        "object":    "string",
        "null":      "string",
        "unknown":   "string",
    }

    columns = [
        {"Name": col, "Type": type_map.get(info["type"], "string")}
        for col, info in current_fp.get("columns", {}).items()
    ]

    glue.create_table(
        DatabaseName=GLUE_DATABASE,
        TableInput={
            "Name": GLUE_TABLE,
            "StorageDescriptor": {
                "Columns":           columns,
                "Location":          f"s3://{BUCKET}/raw/github-events/",
                "InputFormat":       "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat":      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    "Parameters": {"serialization.format": "1"},
                },
            },
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "json"},
        },
    )
    print(f"[remediation] Created Glue table {GLUE_DATABASE}.{GLUE_TABLE}")


def cast_glue_column(column: str, new_type: str):
    """Update a column's type in the Glue catalog."""
    table = get_glue_table()
    if not table:
        print(f"[remediation] CAST_COLUMN: Glue table not found — skipping")
        return False

    cols = table["StorageDescriptor"]["Columns"]
    updated = False
    for col in cols:
        if col["Name"] == column:
            old_type = col["Type"]
            col["Type"] = new_type
            updated = True
            print(f"[remediation] CAST_COLUMN: {column} {old_type} → {new_type}")
            break

    if not updated:
        print(f"[remediation] CAST_COLUMN: column {column} not found in Glue")
        return False

    table_input = {k: v for k, v in table.items()
                   if k not in ("DatabaseName", "CreateTime", "UpdateTime",
                                "CreatedBy", "IsRegisteredWithLakeFormation",
                                "CatalogId", "VersionId")}
    glue.update_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
    return True


def add_glue_column(column: str, col_type: str):
    """Add a new column to the Glue catalog."""
    table = get_glue_table()
    if not table:
        print(f"[remediation] UPDATE_GLUE_SCHEMA: Glue table not found — skipping")
        return False

    existing = {c["Name"] for c in table["StorageDescriptor"]["Columns"]}
    if column in existing:
        print(f"[remediation] Column {column} already in Glue schema")
        return True

    table["StorageDescriptor"]["Columns"].append({"Name": column, "Type": col_type})
    table_input = {k: v for k, v in table.items()
                   if k not in ("DatabaseName", "CreateTime", "UpdateTime",
                                "CreatedBy", "IsRegisteredWithLakeFormation",
                                "CatalogId", "VersionId")}
    glue.update_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
    print(f"[remediation] ADD_COLUMN: {column} ({col_type}) added to Glue schema")
    return True


def run_athena_repair():
    """Run MSCK REPAIR TABLE to pick up new S3 partitions."""
    resp = athena.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {GLUE_DATABASE}.{GLUE_TABLE}",
        WorkGroup=ATHENA_WG,
    )
    qid = resp["QueryExecutionId"]
    for _ in range(15):
        time.sleep(2)
        status = athena.get_query_execution(QueryExecutionId=qid)
        state  = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            print(f"[remediation] Athena REPAIR TABLE: {state}")
            return state == "SUCCEEDED"
    return False


def action_cast_column(anomaly: dict, current_fp: dict) -> dict:
    column   = anomaly["column"]
    cast_to  = anomaly["action_params"].get("cast_to", "varchar")
    success  = cast_glue_column(column, cast_to)
    if success:
        run_athena_repair()
    return {
        "action":  "CAST_COLUMN",
        "column":  column,
        "cast_to": cast_to,
        "success": success,
    }


def action_update_glue_schema(anomaly: dict, current_fp: dict) -> dict:
    column   = anomaly["column"]
    new_type = anomaly.get("new_type", "string")

    glue_type = {"integer": "bigint", "float": "double"}.get(new_type, "string")
    success   = add_glue_column(column, glue_type)
    if success:
        run_athena_repair()
    return {
        "action":    "UPDATE_GLUE_SCHEMA",
        "column":    column,
        "glue_type": glue_type,
        "success":   success,
    }


def action_filter_nulls(anomaly: dict, s3_key: str) -> dict:
    """Rewrite the S3 file with null rows removed for the affected column."""
    column = anomaly["column"]

    obj    = s3.get_object(Bucket=BUCKET, Key=s3_key)
    events = json.loads(obj["Body"].read().decode())

    before_count = len(events)
    filtered     = [e for e in events if e.get(column) is not None]
    after_count  = len(filtered)
    dropped      = before_count - after_count

    s3.put_object(
        Bucket=BUCKET, Key=s3_key,
        Body=json.dumps(filtered, default=str),
        ContentType="application/json",
    )

    flag_key = f"audit-log/null-filter-flags/{s3_key.split('/')[-1]}.flag.json"
    s3.put_object(
        Bucket=BUCKET, Key=flag_key,
        Body=json.dumps({
            "column":       column,
            "rows_dropped": dropped,
            "source_key":   s3_key,
            "flagged_at":   datetime.now(timezone.utc).isoformat(),
        }),
        ContentType="application/json",
    )

    print(f"[remediation] FILTER_NULLS: {column} — dropped {dropped}/{before_count} rows")
    return {
        "action":        "FILTER_NULLS",
        "column":        column,
        "rows_before":   before_count,
        "rows_after":    after_count,
        "rows_dropped":  dropped,
        "flag_key":      flag_key,
        "success":       True,
    }


def action_flag_and_monitor(anomaly: dict, run_id: str) -> dict:
    flag_key = f"audit-log/monitor-flags/{run_id}.flag.json"
    s3.put_object(
        Bucket=BUCKET, Key=flag_key,
        Body=json.dumps({
            "anomaly":   anomaly,
            "flagged_at": datetime.now(timezone.utc).isoformat(),
            "note":      "Monitoring next run — no structural change made",
        }),
        ContentType="application/json",
    )
    print(f"[remediation] FLAG_AND_MONITOR: {anomaly['anomaly_type']}")
    return {"action": "FLAG_AND_MONITOR", "flag_key": flag_key, "success": True}


def lambda_handler(event, context):
    run_id     = event["run_id"]
    s3_key     = event["s3_key"]
    anomalies  = event["anomalies"]
    current_fp = event.get("current_fp", {})

    print(f"[remediation] run_id={run_id} anomalies={len(anomalies)}")

    # Ensure Glue table exists before any schema operations
    ensure_glue_table_exists(current_fp)

    outcomes = []
    for anomaly in anomalies:
        action  = anomaly.get("action")
        outcome = {"anomaly": anomaly, "remediated_at": datetime.now(timezone.utc).isoformat()}

        try:
            if action == "CAST_COLUMN":
                outcome["result"] = action_cast_column(anomaly, current_fp)
            elif action == "UPDATE_GLUE_SCHEMA":
                outcome["result"] = action_update_glue_schema(anomaly, current_fp)
            elif action == "FILTER_NULLS":
                outcome["result"] = action_filter_nulls(anomaly, s3_key)
            elif action == "FLAG_AND_MONITOR":
                outcome["result"] = action_flag_and_monitor(anomaly, run_id)
            else:
                outcome["result"] = {"action": action, "success": False, "error": "Unknown action"}

            outcome["success"] = outcome["result"].get("success", False)

        except Exception as e:
            print(f"[remediation] ERROR on {action}: {e}")
            outcome["success"] = False
            outcome["error"]   = str(e)

        outcomes.append(outcome)

    # Send outcomes to audit logger
    lambda_client.invoke(
        FunctionName=f"{PROJECT_NAME}-audit_logger",
        InvocationType="Event",
        Payload=json.dumps({
            "run_id":   run_id,
            "s3_key":  s3_key,
            "stage":   "remediation_engine",
            "outcomes": outcomes,
        }),
    )

    successful = sum(1 for o in outcomes if o.get("success"))
    print(f"[remediation] Complete: {successful}/{len(outcomes)} succeeded")

    return {
        "run_id":     run_id,
        "total":      len(outcomes),
        "successful": successful,
        "failed":     len(outcomes) - successful,
    }
