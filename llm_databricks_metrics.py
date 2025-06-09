# app/services/metrics.py

import json
import datetime
import requests
import numpy as np
from azure.cosmos import CosmosClient
from utilities.constants import (
    AZURE_COSMOSDB_ENDPOINT,
    AZURE_COSMOSDB_ACCOUNT_KEY,
    AZURE_COSMOSDB_DATABASE,
    AZURE_COSMOSDB_CONVERSATIONS_CONTAINER,
    API_HEALTH_URL,
)

class MetricsService:
    def __init__(self):
        # Initialize Cosmos DB client & container
        self.client = CosmosClient(
            AZURE_COSMOSDB_ENDPOINT,
            AZURE_COSMOSDB_ACCOUNT_KEY
        )
        self.container = (
            self.client
                .get_database_client(AZURE_COSMOSDB_DATABASE)
                .get_container_client(AZURE_COSMOSDB_CONVERSATIONS_CONTAINER)
        )
        # Health-check URL for uptime
        self.health_url = API_HEALTH_URL

    def _query_value(self, query: str, params: list) -> any:
        results = list(self.container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True
        ))
        return results[0] if results else None

    def get_usage(self, start: datetime.datetime, end: datetime.datetime) -> dict:
        p = [{"name":"@start","value":start.isoformat()},
             {"name":"@end","value":end.isoformat()}]
        total_q = self._query_value(
            "SELECT VALUE COUNT(1) FROM c WHERE c.timestamp_query_asked >= @start AND c.timestamp_query_asked <= @end",
            p
        ) or 0
        unique_u = self._query_value(
            "SELECT VALUE COUNT(DISTINCT c.userId) FROM c WHERE c.timestamp_query_asked >= @start AND c.timestamp_query_asked <= @end",
            p
        ) or 0
        return {"total_queries": int(total_q), "active_users": int(unique_u)}

    def get_performance(self, start: datetime.datetime, end: datetime.datetime) -> dict:
        p = [{"name":"@start","value":start.isoformat()},
             {"name":"@end","value":end.isoformat()}]
        items = list(self.container.query_items(
            """
            SELECT c.timestamp_query_asked, c.timestamp_query_executed
              FROM c
             WHERE c.timestamp_query_asked >= @start
               AND c.timestamp_query_executed != null
               AND c.timestamp_query_asked <= @end
            """, p, enable_cross_partition_query=True
        ))
        latencies = []
        for doc in items:
            t0 = datetime.datetime.fromisoformat(doc["timestamp_query_asked"])
            t1 = datetime.datetime.fromisoformat(doc["timestamp_query_executed"])
            latencies.append((t1 - t0).total_seconds())
        if not latencies:
            return {"avg_s": None, "p95_s": None}
        arr = np.array(latencies)
        return {"avg_s": float(arr.mean()), "p95_s": float(np.percentile(arr, 95))}

    def get_error_rate(self, start: datetime.datetime, end: datetime.datetime) -> float:
        usage = self.get_usage(start, end)["total_queries"]
        if usage == 0:
            return 0.0
        p = [{"name":"@start","value":start.isoformat()},
             {"name":"@end","value":end.isoformat()}]
        err = self._query_value(
            """
            SELECT VALUE COUNT(1)
              FROM c
             WHERE (c.sql_error_status != null OR c.error != null)
               AND c.timestamp_query_asked >= @start
               AND c.timestamp_query_asked <= @end
            """, p
        ) or 0
        return float(err) / usage

    def get_confidence(self, start: datetime.datetime, end: datetime.datetime) -> float:
        p = [{"name":"@start","value":start.isoformat()},
             {"name":"@end","value":end.isoformat()}]
        items = list(self.container.query_items(
            """
            SELECT VALUE c.refiner_confidence_score
              FROM c
             WHERE c.refiner_confidence_score != null
               AND c.timestamp_query_asked >= @start
               AND c.timestamp_query_asked <= @end
            """, p, enable_cross_partition_query=True
        ))
        scores = [float(x) for x in items]
        return float(np.mean(scores)) if scores else None

    def get_uptime(self) -> float:
        try:
            r = requests.get(self.health_url, timeout=5)
            return 100.0 if r.status_code == 200 else 0.0
        except Exception:
            return 0.0

    def collect_all(self, period_days: int = 7) -> dict:
        end   = datetime.datetime.utcnow()
        start = end - datetime.timedelta(days=period_days)
        usage   = self.get_usage(start, end)
        performance = self.get_performance(start, end)
        return {
            "period_start": start.isoformat(),
            "period_end":   end.isoformat(),
            **usage,
            **performance,
            "error_rate":     self.get_error_rate(start, end),
            "avg_confidence": self.get_confidence(start, end),
            "uptime_pct":     self.get_uptime(),
        }


def format_report(m: dict) -> str:
    return f"""
Weekly API Metrics
Period: {m['period_start']} → {m['period_end']}

Usage:
 • Total NL→SQL requests:   {m['total_queries']}
 • Active users:            {m['active_users']}

Performance:
 • Avg latency (s):         {m['avg_s']:.2f if m['avg_s'] is not None else 'N/A'}
 • 95th percentile (s):     {m['p95_s']:.2f if m['p95_s'] is not None else 'N/A'}

Quality & Reliability:
 • Error rate:              {m['error_rate']*100:.2f if m['error_rate'] is not None else 'N/A'}%
 • Avg. confidence:         {m['avg_confidence']:.2f if m['avg_confidence'] is not None else 'N/A'}
 • API uptime:              {m['uptime_pct']:.1f}%
"""


if __name__ == "__main__":
    svc = MetricsService()
    metrics = svc.collect_all(7)
    print(format_report(metrics))
