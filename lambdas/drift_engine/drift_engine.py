"""
Drift Engine 

Receives observability profile fingerprint
 and runs a multi=layer statistical drift analysis

Layer 1: Schema drift       -> structural diff on semantic schema
Layer 2: Statistical drift  -> z-score (volume), KL divergence (distribution)
Layer 3: Null Spike         -> per-column null-rate change
Layer 4: Scoring engine     -> weighted anomaly score -> severity label
Layer 5: Alert grouping     -> groups related anomalies before forwarding 
"""

import os, json, math, boto3
from datetime import datetime, timezone
from typing import Any

BUCKET       = os.environ["BUCKET_NAME"]
REGION       = os.environ["AWS_REGION_NAME"]
PROJECT_NAME = "self-healing-pipeline"

lambda_client = boto3.client("lambda", region_name=REGION)

# Starting Scoring Weights
WEIGHTS = {
    # schema drift
    "COLUMN_REMOVED":    30,
    "TYPE_CHANGED":      25,
    "COLUMN_ADDED":      10,
    "CARDINALITY_SHIFT": 15,
    "INFERRED_MIXED":    10,

    # statistical drift
    "VOLUME_ZSCORE":     30,   # applied when |z| > 3
    "VOLUME_ZSCORE_MOD": 15,   # applied when 2 < |z| <= 3
    "DISTRIBUTION_KL":   20,   # applied when KL > threshold

    # data quality
    "NULL_SPIKE":        20,
}

def score_to_severity(score: int) -> str:
    if score > 60: return "critical"
    if score > 30: return "high"
    if score > 10: return "medium"
    return "low"

def detect_schema_drift(old_fp: dict, new_fp: dict) -> list[dict]:
    anomalies = []
    old_cols = old_fp.get("schema", {}).get("columns", {})
    new_cols = new_fp.get("schema", {}).get("columns", {})
    old_sem  = old_fp.get("schema", {}).get("semantic", {})
    new_sem  = new_fp.get("schema", {}).get("semantic", {})

    added   = set(new_cols) - set(old_cols)
    removed = set(old_cols) - set(new_cols)

    for col in added:
        anomalies.append({
            "anomaly_type" : "SCHEMA_DRIFT",
            "subtype" :      "COLUMN_ADDED",
            "column"  :      col,
            "new_type":      new_cols[col]["inferred_type"],
            "detail"  :      "Column did not exist in previous run"
        })

    for col in removed:
        anomalies.append({
            "anomaly_type": "SCHEMA_DRIFT",
            "subtype":      "COLUMN_REMOVED",
            "column":       col,
            "old_type":     old_cols[col]["inferred_type"],
            "detail":       "Column present in previous run is now missing",
        })

    for col in set(old_cols) & set(new_cols):
        old_type = old_cols[col]["inferred_type"]
        new_type = new_cols[col]["inferred_type"]

        if old_type != new_type:
            anomalies.append({
                "anomaly_type": "SCHEMA_DRIFT",
                "subtype":      "TYPE_CHANGED",
                "column":       col,
                "old_type":     old_type,
                "new_type":     new_type,
                "detail":       f"Inferred type changed: {old_type} → {new_type}",
            })

        # Newly mixed (heterogeneous values appeared)
        if new_type == "mixed" and old_type != "mixed":
            anomalies.append({
                "anomaly_type": "SCHEMA_DRIFT",
                "subtype":      "INFERRED_MIXED",
                "column":       col,
                "detail":       "Column values became heterogeneous (mixed types)",
                "type_dist":    new_cols[col].get("type_dist", {}),
            })

        old_card = old_cols[col].get("cardinality", 0)
        new_card = new_cols[col].get("cardinality", 0)
        if old_card > 0:
            ratio = new_card / old_card
            if ratio > 3.0 or ratio < 0.33:
                anomalies.append({
                    "anomaly_type": "SCHEMA_DRIFT",
                    "subtype":      "CARDINALITY_SHIFT",
                    "column":       col,
                    "old_cardinality": old_card,
                    "new_cardinality": new_card,
                    "ratio":        round(ratio, 3),
                    "detail":       f"Cardinality changed by {ratio:.1f}× (possible PII leak or enum collapse)",
                })

    return anomalies

def kl_divergence(p: dict, q: dict) -> float:
    epsilon = 1e-9
    keys = set(p) | set(q)
    kl = 0.0
    for k in keys:
        p_k = p.get(k, epsilon)
        q_k = q.get(k, epsilon)
        kl += p_k*math.log(p_k, q_k)
    return round(kl, 6)

def detect_statistical_drift(old_fp: dict, new_fp: dict) -> list[dict]:
    anomalies = []

    zscore = new_fp.get("volume", {}).get("row_count_zscore")
    if zscore is not None:
        abs_z = abs(zscore)
        if abs_z > 3.0:
            anomalies.append({
                "anomaly_type": "VOLUME_ZSCORE",
                "zscore":       zscore,
                "row_count":    new_fp["volume"]["row_count"],
                "baseline_mean": new_fp["volume"].get("baseline_mean"),
                "baseline_std":  new_fp["volume"].get("baseline_std"),
                "detail":       f"Volume z-score={zscore:.2f} exceeds ±3σ threshold (extreme outlier vs 30-day baseline)",
            })
        elif abs_z > 2.0:
            anomalies.append({
                "anomaly_type": "VOLUME_ZSCORE_MOD",
                "zscore":       zscore,
                "row_count":    new_fp["volume"]["row_count"],
                "baseline_mean": new_fp["volume"].get("baseline_mean"),
                "detail":       f"Volume z-score={zscore:.2f} — moderate deviation (2–3σ), monitoring recommended",
            })
    
    # Distribution KL divergence -> Compute KL for every distribution
    old_dists = old_fp.get("distribution", {})
    new_dists = new_fp.get("distribution", {})

    KL_THRESHOLD = 0.1  # bits; tune per data

    for dist_name in set(old_dists) & set(new_dists):
        old_d = old_dists[dist_name]
        new_d = new_dists[dist_name]
        kl = kl_divergence(new_d, old_d)

        if kl > KL_THRESHOLD:
            shifted = _top_shifted_categories(old_d, new_d, top_n=3)
            anomalies.append({
                "anomaly_type":  "DISTRIBUTION_KL",
                "distribution":  dist_name,
                "kl_divergence": kl,
                "threshold":     KL_THRESHOLD,
                "top_shifts":    shifted,
                "detail":        f"KL({dist_name})={kl:.4f} > {KL_THRESHOLD} — distribution has shifted significantly",
            })

    return anomalies


def _top_shifted_categories(old_d: dict, new_d: dict, top_n: int = 3) -> list[dict]:
    all_keys = set(old_d) | set(new_d)
    deltas = [
        {
            "category": k, 
            "old_pct": round(old_d.get(k, 0)*100, 2),
            "new_pct": round(new_d.get(k, 0)*100, 2),
            "delta": round((new_d.get(k,0) - old_d.get(k,0))*100, 2)
        }
        for k in all_keys
    ]
    return sorted(deltas, key=lambda x: abs(x["delta"]), reverse=True)[:top_n]

def detect_null_spikes(old_fp: dict, new_fp: dict) -> list[dict]:
    anomalies = []
    old_cols = old_fp.get("schema", {}).get("columns", {})
    new_cols = new_fp.get("schema", {}).get("columns", {})

    for col in set(old_cols) & set(new_cols):
        old_null = old_cols[col].get("null_rate", 0)
        new_null = new_cols[col].get("null_rate", 0)
        delta    = new_null - old_null

        if delta > 0.20 and new_null > 0.10:
            anomalies.append({
                "anomaly_type":  "NULL_SPIKE",
                "column":        col,
                "old_null_rate": old_null,
                "new_null_rate": new_null,
                "delta":         round(delta, 4),
                "detail":        f"Null rate for '{col}' jumped by {delta*100:.1f}pp",
            })

    return anomalies

def score_anomalies(anomalies: list[dict]) -> int:
    score = 0
    for a in anomalies:
        atype = a.get("anomaly_type", "")
        sub = a.get("subtype", "")
        key = sub if sub in WEIGHTS else atype
        score += WEIGHTS.get(key, 5)
    return score

def group_anomalies(anomalies: list[dict], score: int, 
                    current_fp: dict, run_id: str) -> dict:
    schema_anomalies = [a for a in anomalies if a["anomaly_type"] == "SCHEMA_DRIFT"]
    stat_anomalies   = [a for a in anomalies if a["anomaly_type"] in ("VOLUME_ZSCORE", "VOLUME_ZSCORE_MOD", "DISTRIBUTION_KL")]
    quality_anomalies= [a for a in anomalies if a["anomaly_type"] == "NULL_SPIKE"]

    groups = []
    if schema_anomalies:
        groups.append({
            "group_id":   f"schema_drift_{run_id}",
            "category":   "schema",
            "anomalies":  schema_anomalies,
            "summary":    f"{len(schema_anomalies)} schema change(s) detected",
        })
    if stat_anomalies:
        groups.append({
            "group_id":   f"statistical_drift_{run_id}",
            "category":   "statistical",
            "anomalies":  stat_anomalies,
            "summary":    f"{len(stat_anomalies)} statistical drift signal(s)",
        })
    if quality_anomalies:
        groups.append({
            "group_id":   f"data_quality_{run_id}",
            "category":   "data_quality",
            "anomalies":  quality_anomalies,
            "summary":    f"{len(quality_anomalies)} null spike(s) detected",
        })

    severity = score_to_severity(score)

    return {
        "run_id":        run_id,
        "dataset_id":    current_fp.get("dataset_id", "events"),
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "total_score":   score,
        "severity":      severity,
        "anomaly_count": len(anomalies),
        "groups":        groups,
        "raw_anomalies": anomalies,   # kept for downstream debugging
        "current_fp":    current_fp,
    }
    

def lambda_handler(event, context):
    run_id = event["run_id"]
    s3_key = event["s3_key"]
    current_fp = event["current_fp"]
    previous_fp = event.get("previous_fp")
    baseline    = event.get("baseline", {})

    print(f"[drift engine] run_id={run_id}")

    if not previous_fp:
        print("[drift_engine] No previous snapshot — establishing baseline, skipping drift checks")
        return {"run_id": run_id, "status": "baseline_established", "anomaly_count": 0}
    
    # Run all detection layers
    schema_anomalies = detect_schema_drift(previous_fp, current_fp)
    stat_anomalies = detect_statistical_drift(previous_fp, current_fp)
    null_anomalies = detect_null_spikes(previous_fp, current_fp)

    all_anomalies = schema_anomalies + stat_anomalies + null_anomalies
    if not all_anomalies:
        print("[drift_engine] No anomalies detected - pipeline healthy")
        return {"run_id": run_id, "status": "healthy", "anomaly_count": 0}
    
    score = score_anomalies(all_anomalies)
    grouped = group_anomalies(all_anomalies, score, current_fp, run_id)

    print(
        f"[drift engine] score={score} severity={grouped['severity']}"
        f"  anomalies={len(all_anomalies)}"
    )

    lambda_client.invoke(
        FunctionName=f"{PROJECT_NAME}-anomaly_detector",
        InvocationType="Event",
        Payload=json.dumps(grouped, default=str)
    )

    return {
        "run_id":           run_id,
        "score":            score,
        "severity":         grouped["severity"],
        "anomaly_count":    len(all_anomalies),
        "groups":           len(grouped["groups"])
    }
