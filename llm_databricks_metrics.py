import os
from datetime import datetime, timedelta
from azure.cosmos import CosmosClient
from utilities.constants import (
    AZURE_COSMOSDB_ENDPOINT,
    AZURE_COSMOSDB_ACCOUNT_KEY,
    AZURE_COSMOSDB_DATABASE,
    AZURE_COSMOSDB_CONVERSATIONS_CONTAINER,
)

def fetch_weekly_metrics():
    # instantiate Cosmos client
    client = CosmosClient(AZURE_COSMOSDB_ENDPOINT, AZURE_COSMOSDB_ACCOUNT_KEY)
    db = client.get_database_client(AZURE_COSMOSDB_DATABASE)
    container = db.get_container_client(AZURE_COSMOSDB_CONVERSATIONS_CONTAINER)

    # define time window
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)

    # parameterized query to avoid injection
    query = """
        SELECT c.status, c.timestamp_query_asked, c.timestamp_query_executed
        FROM c
        WHERE c.timestamp_query_asked >= @start
    """
    params = [{"name": "@start", "value": week_ago.isoformat() + "Z"}]

    items = list(container.query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=True
    ))

    total = len(items)
    successes = [i for i in items if i.get("status") == "success"]
    failures  = [i for i in items if i.get("status") != "success"]

    # compute average latency (in seconds)
    latencies = []
    for itm in successes:
        try:
            t_asked   = datetime.fromisoformat(itm["timestamp_query_asked"].rstrip("Z"))
            t_executed= datetime.fromisoformat(itm["timestamp_query_executed"].rstrip("Z"))
            latencies.append((t_executed - t_asked).total_seconds())
        except Exception:
            continue

    avg_latency = sum(latencies) / len(latencies) if latencies else None

    return {
        "period_start": week_ago.isoformat() + "Z",
        "period_end"  : now.isoformat() + "Z",
        "total_queries"     : total,
        "successful_queries": len(successes),
        "failed_queries"    : len(failures),
        "success_rate"      : round(len(successes)/total*100, 1) if total else 0,
        "avg_latency_sec"   : round(avg_latency, 2) if avg_latency is not None else None
    }

if __name__ == "__main__":
    metrics = fetch_weekly_metrics()
    print("\n===== Weekly Usage Metrics =====")
    for k, v in metrics.items():
        print(f"{k:20s}: {v}")
    print("================================\n")
