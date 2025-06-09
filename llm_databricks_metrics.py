# backend/app/services/metrics.py

import os
import logging
import json
from datetime import datetime, timedelta

from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError

# use the same constants you already have in utilities/constants.py
from utilities.constants import (
    AZURE_COSMOSDB_ENDPOINT,
    AZURE_COSMOSDB_ACCOUNT_KEY,
    AZURE_COSMOSDB_DATABASE,
    AZURE_COSMOSDB_CONVERSATIONS_CONTAINER,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_conversations_container():
    """
    Initialize and return the Cosmos DB container for conversation logs.
    """
    client = CosmosClient(AZURE_COSMOSDB_ENDPOINT, AZURE_COSMOSDB_ACCOUNT_KEY)
    db = client.get_database_client(AZURE_COSMOSDB_DATABASE)
    return db.get_container_client(AZURE_COSMOSDB_CONVERSATIONS_CONTAINER)


def fetch_weekly_logs(container, start: datetime, end: datetime):
    """
    Pull all conversation-log items in the given time window.
    """
    start_iso = start.isoformat()
    end_iso = end.isoformat()
    query = f"""
      SELECT c.id, c.timestamp_query_asked, c.timestamp_query_executed, c.status, c.error
      FROM c
      WHERE c.timestamp_query_asked >= '{start_iso}'
        AND c.timestamp_query_asked <  '{end_iso}'
    """
    logging.info("Querying Cosmos for logs between %s and %s", start_iso, end_iso)
    try:
        return list(container.query_items(query, enable_cross_partition_query=True))
    except CosmosHttpResponseError as e:
        logging.error("Failed to query Cosmos DB: %s", e)
        return []


def compute_metrics(items):
    """
    Given a list of log dicts, compute summary metrics.
    """
    total = len(items)
    successes = sum(1 for i in items if i.get("status") == "success")
    failures = total - successes

    # compute avg response time in seconds
    durations = []
    for i in items:
        t0 = i.get("timestamp_query_asked")
        t1 = i.get("timestamp_query_executed")
        if t0 and t1:
            dt0 = datetime.fromisoformat(t0)
            dt1 = datetime.fromisoformat(t1)
            durations.append((dt1 - dt0).total_seconds())
    avg_latency = sum(durations) / len(durations) if durations else None

    error_messages = {}
    for i in items:
        err = i.get("error")
        if err:
            error_messages[err] = error_messages.get(err, 0) + 1

    return {
        "period_start": items[0].get("timestamp_query_asked") if items else None,
        "period_end": items[-1].get("timestamp_query_asked") if items else None,
        "total_requests": total,
        "successful_requests": successes,
        "failed_requests": failures,
        "average_latency_seconds": avg_latency,
        "errors_breakdown": error_messages,
    }


def main():
    # define “last week”
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)

    container = get_conversations_container()
    logs = fetch_weekly_logs(container, start=week_ago, end=now)
    metrics = compute_metrics(logs)

    # pretty‐print JSON to stdout (or write to file, email, etc.)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
