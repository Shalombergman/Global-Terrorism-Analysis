import os
import sys
from pathlib import Path

# מציאת תיקיית הפרויקט הראשית
project_root = str(Path(__file__).parent.parent.parent.parent.parent)
sys.path.append(project_root)

import pandas as pd
import folium
from folium import plugins
from src.db.postgres_client import PostgresClient
from src.config.settings import get_settings

def get_client():
    settings = get_settings()
    client = PostgresClient(settings.POSTGRES_URL)
    return client

def get_region_severity_stats(top_n=None):
    """
    מחזיר סטטיסטיקות נפגעים לפי אזור, מחשב כל מדד בנפרד תוך התעלמות מ-NaN ספציפיים
    """
    client = get_client()
    query = """
    WITH base_stats AS (
        SELECT 
            r.name as region,
            r.latitude,
            r.longitude,
            COUNT(*) as total_attacks,
            SUM(te.killed) FILTER (WHERE te.killed IS NOT NULL) as total_killed,
            COUNT(te.killed) FILTER (WHERE te.killed IS NOT NULL) as killed_count,
            SUM(te.wounded) FILTER (WHERE te.wounded IS NOT NULL) as total_wounded,
            COUNT(te.wounded) FILTER (WHERE te.wounded IS NOT NULL) as wounded_count
        FROM terror_events te
        JOIN regions r ON te.region_id = r.id
        GROUP BY r.name, r.latitude, r.longitude
    )
    SELECT 
        region,
        latitude,
        longitude,
        total_attacks,
        total_killed,
        total_wounded,
        CASE 
            WHEN total_killed IS NOT NULL OR total_wounded IS NOT NULL 
            THEN COALESCE(total_killed * 2, 0) + COALESCE(total_wounded, 0)
        END as severity_score,
        CASE 
            WHEN killed_count > 0 OR wounded_count > 0 
            THEN (COALESCE(total_killed * 2, 0) + COALESCE(total_wounded, 0))::FLOAT / 
                 GREATEST(killed_count, wounded_count)
        END::NUMERIC(10,2) as avg_severity_per_attack,
        CASE 
            WHEN killed_count > 0 OR wounded_count > 0 
            THEN ((COALESCE(total_killed * 2, 0) + COALESCE(total_wounded, 0))::FLOAT * 100 / 
                 GREATEST(killed_count, wounded_count))
        END::NUMERIC(10,2) as severity_percentage
    FROM base_stats
    ORDER BY severity_score DESC NULLS LAST
    """
    
    results = client.execute_query(query)
    if top_n:
        return results.head(top_n)
    return results

def create_severity_map(results_df, output_file='region_severity_map.html'):
    """
    יוצר מפה אינטראקטיבית של חומרת הפיגועים לפי אזור
    """
    m = folium.Map(location=[0, 0], zoom_start=2)
    
    # מחשב max_severity רק מערכים תקינים
    max_severity = results_df['severity_score'].max()
    
    for idx, row in results_df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            # מחשב צבע רק אם יש ציון חומרה
            if pd.notna(row['severity_score']) and max_severity > 0:
                severity_ratio = row['severity_score'] / max_severity
                color = f'#{int((1 - severity_ratio) * 255):02x}0000'
            else:
                color = '#808080'  # אפור לאזורים ללא נתוני חומרה
            
            popup_text = f"""
            Region: {row['region']}<br>
            Total Attacks: {int(row['total_attacks'])}<br>
            """
            
            if pd.notna(row['severity_score']):
                popup_text += f"Severity Score: {row['severity_score']:.2f}<br>"
            if pd.notna(row['avg_severity_per_attack']):
                popup_text += f"Avg Severity per Attack: {row['avg_severity_per_attack']:.2f}"
            
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=10,
                popup=popup_text,
                color=color,
                fill=True,
                fill_color=color
            ).add_to(m)
    
    m.save(output_file)
    return m

def print_analysis_results(results_df, show_top_n=5):
    """
    מדפיס את תוצאות הניתוח בפורמט קריא
    """
    print("\nRegion Severity Analysis:")
    if show_top_n:
        print(f"\nTop {show_top_n} Most Severe Regions:")
        print(results_df.head(show_top_n).to_string())
    else:
        print("\nAll Regions:")
        print(results_df.to_string())

def main():
    # קבלת כל התוצאות
    all_results = get_region_severity_stats()
    
    # הדפסת התוצאות
    print_analysis_results(all_results, show_top_n=5)
    
    # יצירת המפה
    create_severity_map(all_results)
    print("\nMap has been saved to 'region_severity_map.html'")

if __name__ == "__main__":
    main()