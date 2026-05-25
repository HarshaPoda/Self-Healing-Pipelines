"""
Fingerprinter Lambda

Reads the batch just written to S3, computes an "observability" profile:
    - schema fingerprint (column names + types)
        per-column type distribution, inferred type, null rate, 
        cardinality, uniqueness, semantic schema hash
    - volume
        row count + z-score vs rolling baseline
    - distribution
        event type proportions (for KL divergence downstream)
    - baseline management
        loads/updates 30-day rolling stats in s3

"""

import os, json, boto3, hashlib, math
from datetime import datetime, timezone
from collections import Counter, defaultdict
from typing import Any

BUCKET        = os.environ["BUCKET"]
REGION        = os.environ["AWS_REGION_NAME"]
PROJECT_NAME  = "self-healing-pipeline"

s3            = boto3.client("s3",     region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)

SNAPSHOT_KEY  = "schema-snapshots/latest.json"
HISTORY_PREFIX= "schema-snapshots/history/"
BASELINE_KEY  = "schema-snapshots/baseline/stats.json"

BASELINE_WINDOW = 30


##################### TYPE INFERENCE ###################
def infer_type(value: Any) -> str:
    if value is None:               return "null"
    if isinstance(value, bool):     return "boolean"
    if isinstance(value, int):      return "integer"
    if isinstance(value, float):    return "float"
    if isinstance(value, list):     return "array"
    if isinstance(value, dict):     return "object"
    if isinstance(value, str):
        s = value.strip()
        if len(s) >= 10 and "T" in s and (s.endswith("Z") or "+" in s):
            return "timestamp"
        # loose numeric check
        try:
            int(s);   return "numeric_string"
        except ValueError:
            pass
        try:
            float(s); return "numeric_string"
        except ValueError:
            pass
        return "string"
    return "unknown"


def type_distribution(values: list) -> dict[str, float]:
    non_null = [infer_type(v) for v in values if v is not None]
    if not non_null:
        return {"null": 1.0}
    total = len(non_null)
    counts = Counter(non_null)
    return {t: round(c/total, 4) for t, c in counts.most_common()}

def dominant_type(type_dist: dict) -> str:
    if not type_dist:
        return "null"
    top_type, top_frac = max(type_dist.items(), key=lambda x: x[1])
    if top_frac < 0.80 and len(type_dist) > 1:
        return "mixed"
    return top_type

def cardinality(values: list) -> int:
    return len(set(str(v) for v in values if v is not None))

def uniqueness_ratio(values: list) -> float:
    non_null = [v for v in values if v is not None]
    if not non_null:
        return 0.0
    return round(cardinality(non_null)/len(non_null), 4)

################# Fingerprint ###########################
def compute_fingerprint(events: list, run_id: str, s3_key: str) -> dict:
    
    if not events:
        return {"error": "empty_batch", "run_id": run_id}
    
    total = len(events)

    field_values: dict[str, list] = defaultdict(list)
    for event in events:
        for key, val in event.items():
            field_values[key].append(val)
    
    # Per-field stats
    columns = {}
    for field, values in field_values.items():
        null_cnt   = sum(1 for v in values if v is None)
        t_dist     = type_distribution(values)
        inf_type   = dominant_type(t_dist)
        card       = cardinality(values)
        uniq       = uniqueness_ratio(values)

        columns[field] = {
            "type_dist":     t_dist,
            "inferred_type": inf_type,
            "null_rate":     round(null_cnt / total, 4),
            "null_count":    null_cnt,
            "cardinality":   card,
            "uniqueness":    uniq,
            "coverage":      round((total - null_cnt) / total, 4),
        }

    # Semantic schema hash ──────────────────────────────────────────────────
    # Hash on (field → inferred_type + high_cardinality flag + nullable flag)
    # so we catch type drift AND cardinality / structural drift

    schema_repr = {
        k: {
            "type":             v["inferred_type"],
            "nullable":         v["null_rate"] > 0,
            "high_cardinality": v["cardinality"] > 1000,
        } for k, v in sorted(columns.items())}
    
    schema_hash = hashlib.md5(json.dumps(schema_repr, sort_keys=True).encode()).hexdigest()

    type_counts = Counter(e.get("event_type", "unknown") for e in events)
    type_dist = {k: round(v/total, 6) for k,v in type_counts.items()}

    return {
        "dataset_id":  "events",
        "run_id":      run_id,
        "s3_key":      s3_key,
        "timestamp":   datetime.now(timezone.utc).isoformat(),

        "schema": {
            "hash":     schema_hash,
            "columns":  columns,
            "semantic": schema_repr,          # used by drift engine for structural diff
        },

        "volume": {
            "row_count":       total,
            "row_count_zscore": None,      # filled by _attach_volume_zscore()
        },

        "distribution": {
            "event_type": type_dist,
        },
    }

def load_baseline() -> dict | None:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=BASELINE_KEY)
        return json.loads(obj["Body"].read().decode())
    except Exception:
        return None


def update_baseline(baseline: dict | None, current_row_count: int) -> dict:
    history: list[int] = (baseline or {}).get("row_count_history", [])
    history.append(current_row_count)
    history = history[-BASELINE_WINDOW:]

    n = len(history)
    mean = sum(history)/n
    variance = sum((x - mean) ** 2 for x in history) / max(n-1,1)
    std = math.sqrt(variance)

    sorted_h = sorted(history)
    p95_idx = min(int(0.95*n), n-1)
    p95 = sorted_h[p95_idx]

    return {
        "row_count_history": history,
        "baseline_mean":     round(mean, 2),
        "baseline_std":      round(std,  2),
        "baseline_p95":      p95,
        "updated_at":        datetime.now(timezone.utc).isoformat(),
        "window":            BASELINE_WINDOW,
    }

def save_baseline(baseline: dict):
    s3.put_object(Bucket=BUCKET, Key=BASELINE_KEY,
                  Body=json.dumps(baseline, defauly=str),
                  ContentType = "application/json"
    )

def attach_volume_zscore(profile: dict, baseline:dict | None):
    if baseline and baseline.get("baseline_std", 0) > 0:
        mean = baseline["baseline_mean"]
        std  = baseline["baseline_std"]
        rc   = profile["volume"]["row_count"]
        profile["volume"]["row_count_zscore"] = round((rc - mean) / std, 4)
        profile["volume"]["baseline_mean"]    = mean
        profile["volume"]["baseline_std"]     = std
        profile["volume"]["baseline_p95"]     = baseline.get("baseline_p95")
    else:
        profile["volume"]["row_count_zscore"] = None   # insufficient history


def load_latest_snapshot() -> dict | None:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)
        return json.loads(obj["Body"].read().decode())
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"[fingerprinter] Could not load snapshot: {e}")
        return None


def save_snapshot(snapshot: dict, run_id: str):
    s3.put_object(
        Bucket=BUCKET, Key=SNAPSHOT_KEY,
        Body=json.dumps(snapshot, default=str),
        ContentType="application/json"
    )

    ts  = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"{HISTORY_PREFIX}{ts}/{run_id}.json"
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(snapshot, default=str),
        ContentType="application/json"
    )


# Old method of tracking just basic schema changes
# def diff_schemas(old: dict, new: dict) -> list[dict]:
#     anomalies = []

#     old_cols = old.get("columns", {})
#     new_cols = new.get("columns", {})

#     # Schema drift: type changes, added/removed columns
#     added   = set(new_cols) - set(old_cols)
#     removed = set(old_cols) - set(new_cols)

#     for col in added:
#         anomalies.append({
#             "anomaly_type": "SCHEMA_DRIFT",
#             "subtype":      "COLUMN_ADDED",
#             "column":       col,
#             "old_type":     None,
#             "new_type":     new_cols[col]["type"],
#             "severity":     "medium",
#         })

#     for col in removed:
#         anomalies.append({
#             "anomaly_type": "SCHEMA_DRIFT",
#             "subtype":      "COLUMN_REMOVED",
#             "column":       col,
#             "old_type":     old_cols[col]["type"],
#             "new_type":     None,
#             "severity":     "high",   # removal is always high — downstream breaks
#         })

#     for col in set(old_cols) & set(new_cols):
#         old_type = old_cols[col]["type"]
#         new_type = new_cols[col]["type"]
#         if old_type != new_type:
#             anomalies.append({
#                 "anomaly_type": "SCHEMA_DRIFT",
#                 "subtype":      "TYPE_CHANGED",
#                 "column":       col,
#                 "old_type":     old_type,
#                 "new_type":     new_type,
#                 "severity":     _type_change_severity(old_type, new_type),
#             })

#         # Null spike: null rate jumped by more than 20 percentage points
#         old_null = old_cols[col]["null_rate"]
#         new_null = new_cols[col]["null_rate"]
#         if new_null - old_null > 0.20 and new_null > 0.10:
#             anomalies.append({
#                 "anomaly_type": "NULL_SPIKE",
#                 "column":       col,
#                 "old_null_rate": old_null,
#                 "new_null_rate": new_null,
#                 "delta":         round(new_null - old_null, 4),
#                 "severity":      "high" if new_null > 0.50 else "medium",
#             })

#     # Row count drop: less than 50% of previous batch
#     old_rows = old.get("row_count", 0)
#     new_rows = new.get("row_count", 0)
#     if old_rows > 0 and new_rows < old_rows * 0.50:
#         anomalies.append({
#             "anomaly_type": "ROW_DROP",
#             "old_row_count": old_rows,
#             "new_row_count": new_rows,
#             "drop_pct":      round(1 - new_rows / old_rows, 4),
#             "severity":      "high",
#         })

#     return anomalies


# def _type_change_severity(old_type: str, new_type: str) -> str:
#     safe_widenings = {
#         ("integer", "float"), ("integer", "string"),
#         ("float", "string"),  ("string", "string"),
#     }
#     if (old_type, new_type) in safe_widenings:
#         return "low"
#     return "high"


def lambda_handler(event, context):
    s3_key = event["s3_key"]
    run_id = event["run_id"]
    print(f"[fingerprinter] run_id={run_id}  s3_key={s3_key}")

    obj    = s3.get_object(Bucket=BUCKET, Key=s3_key)
    events = json.loads(obj["Body"].read().decode())

    profile = compute_fingerprint(events, run_id, s3_key)

    baseline = load_baseline()
    attach_volume_zscore(profile, baseline)
    new_baseline = update_baseline(baseline, profile["volume"]["row_count"])
    save_baseline(new_baseline)

    previous = load_latest_snapshot()

    save_snapshot(profile, run_id)

    print("[fingerprinter] invoking drift_engine")
    lambda_client.invoke(
        FunctionName=f"{PROJECT_NAME}-drift_engine",
        InvocationType="Event",
        Payload=json.dumps({
            "run_id":      run_id,
            "s3_key":      s3_key,
            "current_fp":  profile,
            "previous_fp": previous,   # None on first run
            "baseline":    new_baseline,
        }, default=str),
    )

    return {
        "run_id":      run_id,
        "schema_hash": profile["schema"]["hash"],
        "row_count":   profile["volume"]["row_count"],
        "zscore":      profile["volume"]["row_count_zscore"],
        "columns":     len(profile["schema"]["columns"]),
    }
