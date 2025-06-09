# backend/app/services/metrics.py

import os
from datetime import datetime, timedelta
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
    """
    Pull all conversation-history documents from the last 7 days.
    """
    client    = CosmosClient(AZURE_COSMOSDB_ENDPOINT, AZURE_COSMOSDB_ACCOUNT_KEY)
    database  = client.get_database_client(AZURE_COSMOSDB_DATABASE)
    container = database.get_container_client(AZURE_COSMOSDB_CONVERSATIONS_CONTAINER)

    now      = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    start_iso = week_ago.isoformat() + "Z"

    query = """
      SELECT
        c.id,
        c.userId,
        c.sql_type,
        c.status,
        c.error,
        c.timestamp_query_asked,
        c.timestamp_query_generated,
        c.timestamp_query_executed,
        ARRAY_LENGTH(c.database_response) AS rows_returned
      FROM c
      WHERE c.timestamp_query_asked >= @start
    """
    params = [{"name":"@start", "value": start_iso}]

    return list(container.query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=True
    ))

def compute_metrics(items):
    """
    Compute aggregate metrics from the fetched documents.
    """
    total = len(items)
    by_type, by_user = {}, {}
    llm_times, db_times, e2e_times, rows = [], [], [], []

    for doc in items:
        # Count by SQL type and user
        typ  = doc.get('sql_type', 'Unknown')
        user = doc.get('userId', 'Unknown')
        by_type[typ]    = by_type.get(typ, 0) + 1
        by_user[user]   = by_user.get(user, 0) + 1

        # Parse timestamps for latency
        try:
            t0 = datetime.fromisoformat(doc['timestamp_query_asked'].rstrip('Z'))
            t1 = datetime.fromisoformat(doc['timestamp_query_generated'].rstrip('Z'))
            t2 = datetime.fromisoformat(doc['timestamp_query_executed'].rstrip('Z'))
            llm_times.append((t1 - t0).total_seconds()*1000)
            db_times.append((t2 - t1).total_seconds()*1000)
            e2e_times.append((t2 - t0).total_seconds()*1000)
        except:
            pass

        # Rows returned
        rows.append(doc.get('rows_returned', 0))

    successes = len([d for d in items if d.get('status') == 'success'])
    failures  = total - successes

    def stats(arr):
        return {
            'avg': round(np.mean(arr), 2) if arr else None,
            'p90': round(np.percentile(arr, 90), 2) if arr else None,
            'max': int(np.max(arr)) if arr else None
        }

    return {
        'total_queries': total,
        'success_rate_pct': round(successes/total*100, 2) if total else None,
        'by_type': by_type,
        'by_user': by_user,
        'llm_latency_ms': stats(llm_times),
        'db_latency_ms': stats(db_times),
        'end_to_end_latency_ms': stats(e2e_times),
        'rows_returned': stats(rows),
    }

def generate_report():
    items   = fetch_weekly_items()
    metrics = compute_metrics(items)

    # Build the summary dictionary
    summary = {
        'Date Range': f"{(datetime.utcnow()-timedelta(days=7)).date()} to {datetime.utcnow().date()}",
        'Total Queries': metrics['total_queries'],
        'Success Rate (%)': metrics['success_rate_pct'],
    }
    # Add SQL type counts
    for typ, cnt in metrics['by_type'].items():
        summary[f"{typ} Queries"] = cnt
    # Add latency & rows stats
    for category in ['llm_latency_ms','db_latency_ms','end_to_end_latency_ms','rows_returned']:
        for k,v in metrics[category].items():
            name = category.replace('_',' ').title()
            summary[f"{name} {k.title()}"] = v

    # Convert to DataFrames
    summary_df     = pd.DataFrame(summary.items(), columns=['Metric','Value'])
    top_users_df   = pd.DataFrame(
        sorted(metrics['by_user'].items(), key=lambda x:x[1], reverse=True)[:10],
        columns=['User','Queries']
    )
    top_queries = sorted(
        [{
            'Query ID': d['id'],
            'User': d.get('userId'),
            'Rows': d.get('rows_returned'),
            'End-to-End Latency (ms)': round(
                (datetime.fromisoformat(d['timestamp_query_executed'].rstrip('Z')) -
                 datetime.fromisoformat(d['timestamp_query_asked'].rstrip('Z'))
                ).total_seconds()*1000, 2
            )
        } for d in items if d.get('timestamp_query_executed')],
        key=lambda x:x['End-to-End Latency (ms)'], reverse=True
    )[:10]
    top_queries_df = pd.DataFrame(top_queries)

    # Print to terminal
    print("\n===== Weekly Metrics Summary =====")
    print(summary_df.to_string(index=False))
    print("\n===== Top Users =====")
    print(top_users_df.to_string(index=False))
    print("\n===== Top Heavy Queries =====")
    print(top_queries_df.to_string(index=False))
    print("====================================\n")

    # Write to Excel
    excel_path = "weekly_metrics.xlsx"
    with pd.ExcelWriter(excel_path) as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        top_users_df.to_excel(writer, sheet_name='Top Users', index=False)
        top_queries_df.to_excel(writer, sheet_name='Top Queries', index=False)

    print(f"Excel report saved to: {excel_path}")

if __name__ == "__main__":
    generate_report()
