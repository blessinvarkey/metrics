import os
from datetime import datetime, timedelta, timezone
from azure.cosmos import CosmosClient
import numpy as np
import pandas as pd
from utilities.constants import (
    AZURE_COSMOSDB_ENDPOINT,
    AZURE_COSMOSDB_ACCOUNT_KEY,
    AZURE_COSMOSDB_DATABASE,
    AZURE_COSMOSDB_CONVERSATIONS_CONTAINER,
)

def fetch_weekly_items():
    client    = CosmosClient(AZURE_COSMOSDB_ENDPOINT, AZURE_COSMOSDB_ACCOUNT_KEY)
    db        = client.get_database_client(AZURE_COSMOSDB_DATABASE)
    container = db.get_container_client(AZURE_COSMOSDB_CONVERSATIONS_CONTAINER)

    now      = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    start_iso = week_ago.isoformat()

    query = """
      SELECT c.id, c.userId, c.sql_type, c.status, c.error,
             c.timestamp_query_asked, c.timestamp_query_generated,
             c.timestamp_query_executed
      FROM c
      WHERE c.timestamp_query_asked >= @start
    """
    params = [{"name":"@start","value":start_iso}]

    return list(container.query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=True
    ))

def compute_metrics(items):
    total     = len(items)
    successes = sum(1 for d in items if d.get('status')=='success')
    failures  = total - successes

    llm = []
    db  = []

    for d in items:
        try:
            t0 = datetime.fromisoformat(d['timestamp_query_asked'])
            t1 = datetime.fromisoformat(d['timestamp_query_generated'])
            t2 = datetime.fromisoformat(d['timestamp_query_executed'])
            llm.append((t1 - t0).total_seconds()*1000)
            db.append((t2 - t1).total_seconds()*1000)
        except:
            pass

    return {
        'total_queries': total,
        'successful_queries': successes,
        'failed_queries': failures,
        'success_rate_pct': round(successes/total*100,2) if total else None,
        'avg_llm_latency_ms': round(np.mean(llm),2) if llm else None,
        'avg_db_latency_ms' : round(np.mean(db),2) if db  else None,
        'all_users': sorted({d.get('userId','') for d in items}),
        'all_queries': [d.get('user_question','') for d in items]
    }

def generate_report():
    now     = datetime.now(timezone.utc)
    metrics = compute_metrics(fetch_weekly_items())

    # build summary DataFrame
    summary = {
        'Date Range'          : f"{(now-timedelta(days=7)).date()} → {now.date()}",
        'Total Queries'       : metrics['total_queries'],
        'Successful Queries'  : metrics['successful_queries'],
        'Failed Queries'      : metrics['failed_queries'],
        'Success Rate (%)'    : metrics['success_rate_pct'],
        'Avg LLM Latency (ms)': metrics['avg_llm_latency_ms'],
        'Avg DB Latency (ms)' : metrics['avg_db_latency_ms'],
    }
    summary_df = pd.DataFrame(list(summary.items()), columns=['Metric','Value'])

    # all users & queries
    users_df   = pd.DataFrame(metrics['all_users'],   columns=['UserId'])
    queries_df = pd.DataFrame(metrics['all_queries'], columns=['UserQuestion'])

    # print to terminal
    print("\n===== Weekly Metrics Summary =====")
    print(summary_df.to_string(index=False))
    print("\n===== All Users =====")
    print("\n".join(metrics['all_users']))
    print("\n===== All Queries =====")
    for q in metrics['all_queries']:
        print(f"- {q}")
    print("====================================\n")

    # write to Excel
    excel_path = "weekly_metrics.xlsx"
    with pd.ExcelWriter(excel_path) as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        users_df.to_excel(writer, sheet_name='All Users', index=False)
        queries_df.to_excel(writer, sheet_name='All Queries', index=False)

    print(f"Excel report saved to: {excel_path}")

if __name__ == "__main__":
    generate_report()
