# backend/app/services/metrics.py

import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from azure.cosmos import CosmosClient

from utilities.constants import (
    AZURE_COSMOSDB_ENDPOINT,
    AZURE_COSMOSDB_ACCOUNT_KEY,
    AZURE_COSMOSDB_DATABASE,
    AZURE_COSMOSDB_CONVERSATIONS_CONTAINER,
)

def fetch_weekly_items():
    """
    Pull all conversation-history documents from the last 7 days with
    the fields needed for metrics.
    """
    client    = CosmosClient(AZURE_COSMOSDB_ENDPOINT, AZURE_COSMOSDB_ACCOUNT_KEY)
    database  = client.get_database_client(AZURE_COSMOSDB_DATABASE)
    container = database.get_container_client(AZURE_COSMOSDB_CONVERSATIONS_CONTAINER)

    now      = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    start_iso = week_ago.isoformat()

    query = """
      SELECT
        c.id,
        c.userId,
        c.user_context,
        c.user_question,
        c.generated_sql_query,
        c.database_response,
        c.error,
        c.status,
        c.timestamp_query_asked,
        c.timestamp_query_generated,
        c.timestamp_query_executed
      FROM c
      WHERE c.timestamp_query_asked >= @start
    """
    params = [{"name": "@start", "value": start_iso}]

    return list(container.query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=True
    ))


def compute_metrics(items):
    """
    Compute summary metrics and per-user counts from fetched documents.
    """
    total     = len(items)
    successes = sum(1 for d in items if d.get('status') == 'Success')
    failures  = total - successes
    success_rate = round(successes / total * 100, 2) if total else 0

    llm_times = []
    db_times  = []

    for d in items:
        try:
            t0 = datetime.fromisoformat(d['timestamp_query_asked'])
            t1 = datetime.fromisoformat(d['timestamp_query_generated'])
            t2 = datetime.fromisoformat(d['timestamp_query_executed'])
            llm_times.append((t1 - t0).total_seconds() * 1000)
            db_times.append((t2 - t1).total_seconds() * 1000)
        except Exception:
            continue

    avg_llm = round(np.mean(llm_times), 2) if llm_times else None
    avg_db  = round(np.mean(db_times),  2) if db_times  else None

    # per-user query counts
    users = [d.get('userId', '') for d in items]
    user_counts = pd.Series(users).value_counts().to_dict()

    return {
        'total_queries': total,
        'successful_queries': successes,
        'failed_queries': failures,
        'success_rate_pct': success_rate,
        'avg_llm_latency_ms': avg_llm,
        'avg_db_latency_ms': avg_db,
        'user_counts': user_counts,
    }


def generate_report():
    """
    Fetches data, computes metrics, prints to console, and writes Excel.
    """
    now     = datetime.now(timezone.utc)
    items   = fetch_weekly_items()
    metrics = compute_metrics(items)

    # Build terminal summary
    print("\n===== Weekly Metrics Summary =====")
    print(f"Date Range          : {(now - timedelta(days=7)).date()} → {now.date()}")
    print(f"Total Queries       : {metrics['total_queries']}")
    print(f"Successful Queries  : {metrics['successful_queries']}")
    print(f"Failed Queries      : {metrics['failed_queries']}")
    print(f"Success Rate (%)    : {metrics['success_rate_pct']}")
    print(f"Avg LLM Latency (ms): {metrics['avg_llm_latency_ms']}")
    print(f"Avg DB Latency (ms) : {metrics['avg_db_latency_ms']}")
    print("===================================\n")

    # Queries per user
    print("===== Queries per User =====")
    for user, count in metrics['user_counts'].items():
        print(f"{user:30s} : {count}")
    print()

    # Full query details with user_context, latencies, error
    print("===== All Query Details =====")
    detailed_rows = []
    for d in items:
        # calculate latencies per record
        try:
            t0 = datetime.fromisoformat(d['timestamp_query_asked'])
            t1 = datetime.fromisoformat(d['timestamp_query_generated'])
            t2 = datetime.fromisoformat(d['timestamp_query_executed'])
            llm_ms = round((t1 - t0).total_seconds() * 1000, 2)
            db_ms  = round((t2 - t1).total_seconds() * 1000, 2)
        except Exception:
            llm_ms = db_ms = None

        detailed_rows.append({
            'UserId': d.get('userId'),
            'UserContext': d.get('user_context'),
            'UserQuestion': d.get('user_question'),
            'GeneratedSQL': d.get('generated_sql_query'),
            'LLM Latency (ms)': llm_ms,
            'DB Latency (ms)': db_ms,
            'DatabaseResponse': json.dumps(d.get('database_response'), ensure_ascii=False),
            'Error': d.get('error'),
            'Status': d.get('status'),
        })

        # print to console
        print(f"UserId          : {d.get('userId')}")
        print(f"UserContext     : {d.get('user_context')}")
        print(f"Question        : {d.get('user_question')}")
        print(f"GeneratedSQL    : {d.get('generated_sql_query')}")
        print(f"LLM Latency (ms): {llm_ms}")
        print(f"DB Latency (ms) : {db_ms}")
        print(f"Status / Error  : {d.get('status')} / {d.get('error')}")
        print(f"Response        : {json.dumps(d.get('database_response'), ensure_ascii=False)}")
        print("-" * 60)

    # Prepare DataFrames for Excel
    summary_df = pd.DataFrame([
        ("Date Range",          f"{(now - timedelta(days=7)).date()} → {now.date()}"),
        ("Total Queries",       metrics['total_queries']),
        ("Successful Queries",  metrics['successful_queries']),
        ("Failed Queries",      metrics['failed_queries']),
        ("Success Rate (%)",    metrics['success_rate_pct']),
        ("Avg LLM Latency (ms)",metrics['avg_llm_latency_ms']),
        ("Avg DB Latency (ms)", metrics['avg_db_latency_ms']),
    ], columns=["Metric", "Value"])

    user_counts_df = pd.DataFrame(
        list(metrics['user_counts'].items()),
        columns=["UserId", "Queries"]
    )

    details_df = pd.DataFrame(detailed_rows)

    # Write to Excel
    excel_path = "weekly_metrics_detailed.xlsx"
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        user_counts_df.to_excel(writer, sheet_name="UserCounts", index=False)
        details_df.to_excel(writer, sheet_name="AllQueries", index=False)

    print(f"\nExcel report saved to: {excel_path}\n")


if __name__ == "__main__":
    generate_report()
