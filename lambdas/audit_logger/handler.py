"""
Audit Logger
Every anomaly detection + remediation is logged and written
    to DynamoDB
"""

import os, json, boto3
from datetime import datetime, timezone

REGION         = os.environ["AWS_REGION_NAME"]
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table    = dynamodb.Table(DYNAMODB_TABLE)

# 90 days TTL
TTL_SECONDS = 90 * 24 * 60 * 60

def write_entry(pipeline_id: str, entry: dict):
    now = datetime.now(timezone.utc)
    event_time = now.isoformat()
    expires = int(now.timestamp()) + TTL_SECONDS

    table.put_items(Item={
        "pipeline_id":   pipeline_id,
        "event_time":    event_time,
        "expires_at":    expires,
        "anomaly_type":  entry.get("anomaly_type", "SYSTEM"),
        **{k: _serialize(v) for k, v in entry.items()},
    })

def _serialize(value):
    if isinstance(value, float):
        return str(round(value, 6))
    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize(i) for i in value]
    return value

def lambda_handler(event, context):
    run_id = event.get("run_id", "unknown")
    stage  = event.get("stage", "unknown")
    s3_key = event.get("s3_key", "")

    print(f"[audit_logger] run_id={run_id} stage={stage}")

    base = {
        "run_id":   run_id,
        "stage":    stage,
        "s3_key":   s3_key,
        "logged_at": datetime.now(timezone.utc).isoformat(),
    }

    # anomaly_detector sends classified anomalies
    if "classified" in event:
        for anomaly in event["classified"]:
            write_entry(run_id, {**base, **anomaly})
        print(f"[audit_logger] Wrote {len(event['classified'])} anomaly entries")

    # remediation_engine sends outcomes
    if "outcomes" in event:
        for outcome in event["outcomes"]:
            entry = {
                **base,
                "anomaly_type":  outcome.get("anomaly", {}).get("anomaly_type", "REMEDIATION"),
                "action":        outcome.get("anomaly", {}).get("action"),
                "action_result": json.dumps(outcome.get("result", {})),
                "success":       outcome.get("success", False),
                "remediated_at": outcome.get("remediated_at"),
            }
            write_entry(run_id, entry)
        print(f"[audit_logger] Wrote {len(event['outcomes'])} remediation entries")

    return {"status": "ok", "run_id": run_id}
