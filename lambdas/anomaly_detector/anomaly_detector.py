"""
Anomaly Detector Lambda

Receives a grouped, scored alert envelope from the Drift Engine Lambda


1. Deduplication 
    Suppress repeat alerts within a 1-hr window
2. Classification
    Match every raw anomaly against REMEDIATION_RULES
3. Routing
    Auto-fixable -> remediation engine
4. Alert Persistence 
    Save grouped alert to S3
5. Structured Log
    JSON log line for CloudWatch
6. Audit Logger
    Invoke Audit Logger

"""

import os, json, boto3
from datetime import datetime, timezone

BUCKET        = os.environ["BUCKET_NAME"]
REGION        = os.environ["AWS_REGION_NAME"]
PROJECT_NAME  = "self-healing-pipeline"
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

s3            = boto3.client("s3",     region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)
sns_client    = boto3.client("sns",    region_name=REGION)

ALERT_WINDOW_SECONDS = 3600 

REMEDIATION_RULES = [
    # Safe type widenings → auto-cast
    {
        "match":  {"anomaly_type": "SCHEMA_DRIFT", "subtype": "TYPE_CHANGED",
                   "old_type": "integer", "new_type": "float"},
        "action": "CAST_COLUMN",
        "params": {"cast_to": "double", "safe": True},
    },
    {
        "match":  {"anomaly_type": "SCHEMA_DRIFT", "subtype": "TYPE_CHANGED",
                   "old_type": "integer", "new_type": "string"},
        "action": "CAST_COLUMN",
        "params": {"cast_to": "varchar", "safe": True},
    },
    {
        "match":  {"anomaly_type": "SCHEMA_DRIFT", "subtype": "TYPE_CHANGED",
                   "old_type": "float", "new_type": "string"},
        "action": "CAST_COLUMN",
        "params": {"cast_to": "varchar", "safe": True},
    },
    # Column added → update Glue schema (safe, no page needed)
    {
        "match":  {"anomaly_type": "SCHEMA_DRIFT", "subtype": "COLUMN_ADDED"},
        "action": "UPDATE_GLUE_SCHEMA",
        "params": {"safe": True},
    },
    # Column removed → always escalate (breaks downstream models)
    {
        "match":  {"anomaly_type": "SCHEMA_DRIFT", "subtype": "COLUMN_REMOVED"},
        "action": "ESCALATE",
        "params": {"reason": "Column removal may break downstream models"},
    },
    # Mixed types appeared → escalate; can't auto-cast heterogeneous data
    {
        "match":  {"anomaly_type": "SCHEMA_DRIFT", "subtype": "INFERRED_MIXED"},
        "action": "ESCALATE",
        "params": {"reason": "Column values became heterogeneous — upstream data contract broken"},
    },
    # Cardinality explosion/collapse → escalate; possible PII leak or enum collapse
    {
        "match":  {"anomaly_type": "SCHEMA_DRIFT", "subtype": "CARDINALITY_SHIFT"},
        "action": "ESCALATE",
        "params": {"reason": "Cardinality changed drastically — possible PII leak or categorical collapse"},
    },
    # Null spike → filter nulls + flag downstream consumers
    {
        "match":  {"anomaly_type": "NULL_SPIKE"},
        "action": "FILTER_NULLS",
        "params": {"flag_downstream": True},
    },
    # Volume z-score extreme (|z| > 3) → flag and monitor
    {
        "match":  {"anomaly_type": "VOLUME_ZSCORE"},
        "action": "FLAG_AND_MONITOR",
        "params": {"reason": "Volume exceeded ±3σ vs 30-day baseline — check upstream pipeline"},
    },
    # Volume z-score moderate (2 < |z| ≤ 3) → monitor only, do not page
    {
        "match":  {"anomaly_type": "VOLUME_ZSCORE_MOD"},
        "action": "FLAG_AND_MONITOR",
        "params": {"reason": "Volume deviated 2–3σ — watching next run before escalating"},
    },
    # Distribution KL divergence → flag for review
    {
        "match":  {"anomaly_type": "DISTRIBUTION_KL"},
        "action": "FLAG_AND_MONITOR",
        "params": {"reason": "Event distribution shifted — possible behaviour change or upstream skew"},
    },
    # Row drop — backwards compat with direct fingerprinter invocations
    {
        "match":  {"anomaly_type": "ROW_DROP"},
        "action": "FLAG_AND_MONITOR",
        "params": {"reason": "Row count dropped >50% vs previous run — monitoring next batch"},
    },
]

def _match_rule(anomaly: dict) -> dict | None:
    for rule in REMEDIATION_RULES:
        if all(anomaly.get(k) == v for k, v in rule["match"].items()):
            return rule
    return None

def classify_anomaly(anomaly: dict) -> dict:
    rule = _match_rule(anomaly)
    if rule is None:
        return {
            **anomaly,
            "action":   "ESCALATE",
            "action_params" : {"reason" : "No matching remediation rule - novel failure"},
            "auto_remediable": False,
            "classified_at": datetime.now(timezone.utc).isoformat()
        }
    return {
        **anomaly,
        "action":   rule["action"],
        "action_params": rule["params"],
        "auto_remediable": rule["action"] != "ESCALATE",
        "classified_at": datetime.now(timezone.utc).isoformat()
    }

def _dedup_key(dataset_id: str, category: str) -> str:
    return f"alerts/dedup/{dataset_id}/{category}.json"

def _load_dedup_state(dataset_id: str, category: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=_dedup_key(dataset_id, category))
        return json.loads(obj["Body"].read().decode())
    except Exception:
        return None
    
def _save_dedup_state(dataset_id: str, category: str, score: int, run_id:str):
    s3.put_object(
        Bucket=BUCKET,
        Key=_dedup_key(dataset_id, category),
        Body=json.dumps({
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "score":     score,
            "run_id":    run_id
        }),
        ContentType = "application/json"
    )

def is_suppressed(dataset_id: str, category: str, current_score: int) -> bool:

    state = _load_dedup_state(dataset_id, category)
    if not state:
        return False

    last_seen = datetime.fromisoformat(state["last_seen"])
    age_secs  = (datetime.now(timezone.utc) - last_seen).total_seconds()

    if age_secs > ALERT_WINDOW_SECONDS:
        return False

    prev_score = state.get("score", 0)
    if prev_score > 0 and current_score >= prev_score * 2:
        print(f"[anomaly_detector] Suppression override — score escalated {prev_score} → {current_score}")
        return False

    print(f"[anomaly_detector] Suppressed: {category} alerted {age_secs:.0f}s ago (window={ALERT_WINDOW_SECONDS}s)")
    return True

def _escalate(run_id: str, anomaly: dict, current_fp: dict,
              severity: str, total_score: int):
    subject = (
        f"[{severity.upper()}] self-healing-pipeline — "
        f"{anomaly['anomaly_type']} — run {run_id}"
    )[:100]

    # Pull volume/schema fields from either new or old profile shape
    volume    = current_fp.get("volume", {})
    schema    = current_fp.get("schema", {})
    row_count = volume.get("row_count",       current_fp.get("row_count",    "unknown"))
    zscore    = volume.get("row_count_zscore", "n/a")
    s_hash    = schema.get("hash",             current_fp.get("schema_hash", "unknown"))
    col_count = len(schema.get("columns",      current_fp.get("columns", {})))

    body = f"""
SELF-HEALING PIPELINE — HUMAN ESCALATION REQUIRED
==================================================
Run ID:        {run_id}
Time:          {datetime.now(timezone.utc).isoformat()}
Anomaly type:  {anomaly['anomaly_type']}
Severity:      {severity}
Total score:   {total_score}
Reason:        {anomaly.get('action_params', {}).get('reason', 'No matching rule')}

ANOMALY DETAIL
--------------
{json.dumps(anomaly, indent=2, default=str)}

CURRENT SCHEMA STATE
--------------------
Row count:    {row_count}
Row z-score:  {zscore}
Schema hash:  {s_hash}
Columns:      {col_count}

SUGGESTED NEXT STEPS
--------------------
1. Review the anomaly detail above
2. Check s3://{BUCKET}/schema-snapshots/latest.json
3. Check s3://{BUCKET}/alerts/grouped/ for recent grouped history
4. If safe to remediate, add a rule to REMEDIATION_RULES in anomaly_detector/handler.py
5. Re-deploy the anomaly_detector Lambda
"""

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=body,
    )
    print(f"[anomaly_detector] Escalated → SNS: {anomaly['anomaly_type']}")


def _save_alert(payload: dict, run_id: str):
    ts  = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"alerts/grouped/{ts}/{run_id}.json"
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(payload, default=str),
        ContentType="application/json",
    )


def lambda_handler(event, context):

    run_id     = event["run_id"]
    dataset_id = event.get("dataset_id", "events")
    severity   = event.get("severity", "unknown")
    score      = event.get("total_score", 0)
    groups     = event.get("groups", [])
    current_fp = event.get("current_fp", {})

    # Backwards compat: old format used top-level "anomalies" + "s3_key"
    s3_key        = event.get("s3_key", current_fp.get("s3_key", "unknown"))
    raw_anomalies = event.get("raw_anomalies", event.get("anomalies", []))

    print(f"[anomaly_detector] run_id={run_id}  severity={severity}  "
          f"score={score}  anomalies={len(raw_anomalies)}")

    active_groups = []
    for group in groups:
        category = group["category"]
        if is_suppressed(dataset_id, category, score):
            continue
        active_groups.append(group)
        _save_dedup_state(dataset_id, category, score, run_id)

    if groups and not active_groups:
        print("[anomaly_detector] All groups suppressed — skipping alerts")
        lambda_client.invoke(
            FunctionName=f"{PROJECT_NAME}-audit_logger",
            InvocationType="Event",
            Payload=json.dumps({
                "run_id": run_id, "s3_key": s3_key,
                "stage":  "anomaly_detector", "status": "suppressed",
                "score":  score,
            }),
        )
        return {"run_id": run_id, "status": "suppressed"}

    classified = [classify_anomaly(a) for a in raw_anomalies]
    auto_fix   = [a for a in classified if     a["auto_remediable"]]
    escalate   = [a for a in classified if not a["auto_remediable"]]

    print(f"[anomaly_detector] auto_fix={len(auto_fix)}  escalate={len(escalate)}")

    if auto_fix:
        lambda_client.invoke(
            FunctionName=f"{PROJECT_NAME}-remediation_engine",
            InvocationType="Event",
            Payload=json.dumps({
                "run_id":     run_id,
                "s3_key":     s3_key,
                "anomalies":  auto_fix,
                "current_fp": current_fp,
                "severity":   severity,
                "score":      score,
            }, default=str),
        )

    for anomaly in escalate:
        _escalate(run_id, anomaly, current_fp, severity, score)

    _save_alert({**event, "groups": active_groups, "classified": classified}, run_id)

    print(json.dumps({
        "event":          "ANOMALY_ALERT",
        "run_id":         run_id,
        "dataset_id":     dataset_id,
        "severity":       severity,
        "total_score":    score,
        "anomaly_count":  len(raw_anomalies),
        "auto_fix_count": len(auto_fix),
        "escalate_count": len(escalate),
        "active_groups":  [g["category"] for g in active_groups],
        "timestamp":      event.get("timestamp", datetime.now(timezone.utc).isoformat()),
    }))

    lambda_client.invoke(
        FunctionName=f"{PROJECT_NAME}-audit_logger",
        InvocationType="Event",
        Payload=json.dumps({
            "run_id":     run_id,
            "s3_key":     s3_key,
            "stage":      "anomaly_detector",
            "classified": classified,
            "score":      score,
            "severity":   severity,
        }, default=str),
    )

    return {
        "run_id":        run_id,
        "total":         len(classified),
        "auto_fix":      len(auto_fix),
        "escalated":     len(escalate),
        "groups_active": len(active_groups),
        "score":         score,
        "severity":      severity,
        "status":        "alerted",
    }



