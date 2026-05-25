"""
Ingestion Lambda

Pulls data from APIs, cleans for PII and flattens 
writes raw JSON to S3, then invokes the fingerprinter lambda.
"""
import os, json, hashlib, boto3, urllib.request, urllib.error
from datetime import datetime, timezone

BUCKET        = os.environ["BUCKET"]
REGION        = os.environ["AWS_REGION_NAME"]
PROJECT_NAME  = "self-healing-pipeline"

s3            = boto3.client("s3",      region_name=REGION)
lambda_client = boto3.client("lambda",  region_name=REGION)


def clean_events(event: dict) -> dict:
    actor   = event.get("actor", {})
    repo    = event.get("repo", {})
    payload = event.get("payload", {})

    return {
        "event_id":    event.get("id"),
        "event_type":  event.get("type"),
        "actor_id":    actor.get("id"),
        "actor_hash":  _hash(actor.get("login", "")),
        "repo_id":     repo.get("id"),
        "repo_hash":   _hash(repo.get("name", "")),
        "is_public":   event.get("public", True),
        "created_at":  event.get("created_at"),
        "ingest_time": datetime.now(timezone.utc).isoformat(),
        **flatten_payload(event.get("type", ""), payload),
    }


def flatten_payload(event_type: str, payload: dict) -> dict:
    base = {"payload_keys": sorted(payload.keys())}

    if event_type == "PushEvent":
        return {**base,
            "push_ref":      payload.get("ref"),
            "push_size":     payload.get("size"),
            "push_distinct": payload.get("distinct_size"),
        }
    if event_type == "WatchEvent":
        return {**base, "watch_action": payload.get("action")}

    if event_type == "ForkEvent":
        forkee = payload.get("forkee", {})
        return {**base,
            "fork_is_private": forkee.get("private"),
            "fork_has_issues": forkee.get("has_issues"),
        }
    if event_type == "IssuesEvent":
        issue = payload.get("issue", {})
        return {**base,
            "issue_action":      payload.get("action"),
            "issue_number":      issue.get("number"),
            "issue_state":       issue.get("state"),
            "issue_comments":    issue.get("comments"),
            "issue_has_body":    issue.get("body") is not None,
            "issue_label_count": len(issue.get("labels", [])),
        }
    if event_type == "PullRequestEvent":
        pr = payload.get("pull_request", {})
        return {**base,
            "pr_action":        payload.get("action"),
            "pr_number":        pr.get("number"),
            "pr_state":         pr.get("state"),
            "pr_merged":        pr.get("merged"),
            "pr_commit_count":  pr.get("commits"),
            "pr_changed_files": pr.get("changed_files"),
        }

    return {**base, "payload_action": payload.get("action")}

# hash to remove PII type data
def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16] if value else None


def fetch_github_events() -> list:
    url = "https://api.github.com/events?per_page=100"
    req = urllib.request.Request(url, headers={
        "Accept":     "application/vnd.github.v3+json",
        "User-Agent": "self-healing-pipeline/1.0",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"[ingestor] GitHub API error: {e.code} {e.reason}")
        return []
    except Exception as e:
        print(f"[ingestor] Fetch error: {e}")
        return []


def write_to_s3(events: list, run_id: str) -> str:
    now = datetime.now(timezone.utc)
    key = f"raw/github-events/{now.strftime('%Y/%m/%d/%H')}/{run_id}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(events, default=str),
        ContentType="application/json",
    )
    return key


def lambda_handler(event, context):
    run_id = (context.aws_request_id
              if context else datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))
    print(f"[ingestor] Starting run_id={run_id}")

    raw_events = fetch_github_events()
    if not raw_events:
        print("[ingestor] No events from GitHub — exiting")
        return {"status": "no_events"}

    cleaned = [clean_events(e) for e in raw_events]
    s3_key    = write_to_s3(raw_events, run_id)
    print(f"[ingestor] {len(cleaned)} events → s3://{BUCKET}/{s3_key}")

    # Hand off to fingerprinter (async — don't block)
    lambda_client.invoke(
        FunctionName=f"{PROJECT_NAME}-fingerprint",
        InvocationType="Event",
        Payload=json.dumps({
            "s3_key":       s3_key,
            "run_id":       run_id,
            "event_count":  len(cleaned),
        }),
    )

    return {"status": "ok", "events_ingested": len(raw_events), "s3_key": s3_key}
