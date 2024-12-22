import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(project_root)
from src.db.postgres_client import PostgresClient
from src.config.settings import get_settings
import pandas as pd

def get_client():
    settings = get_settings()
    client = PostgresClient(settings.POSTGRES_URL)
    return client

def get_top_attack_types_by_severity():
    client = get_client()
    query = """
    SELECT 
        at.name as attack_type,
        COUNT(*) as total_attacks,
        SUM(te.killed) as total_killed,
        SUM(te.wounded) as total_wounded,
        SUM(te.killed * 2 + te.wounded) as severity_score
    FROM terror_events te
    JOIN attack_types at ON te.attack_type_id = at.id
    GROUP BY at.name
    ORDER BY severity_score DESC
    LIMIT 10
    """
    return client.execute_query(query)

if __name__ == "__main__":
    print("Top Attack Types by Severity:")
    results = get_top_attack_types_by_severity()
    print("\nResults:")
    print(results.to_string())  
    print("\nSummary Statistics:")
    print(results[['total_killed', 'total_wounded', 'severity_score']].describe())