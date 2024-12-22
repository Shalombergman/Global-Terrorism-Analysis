import pandas as pd
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import time

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.data_etl.load.init_db import init_db
from src.config.settings import get_settings
from src.data_etl.transform.transformer import DataTransformer

def run_etl():
    load_dotenv()
    settings = get_settings()
    
    print("Loading data...")
    csv_path = os.path.join(project_root, 'csv_data', 'globalterrorismdb_clean.csv')
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from CSV")
    
    print("\nCreating tables...")
    engine = create_engine(settings.POSTGRES_URL)
    session = Session(engine)
    init_db(engine)
    
    print("\nStarting transformation and loading...")
    transformer = DataTransformer(df, session)
    
    try:
        start_time = time.time()
        
        print("\nPreparing regions...")
        print(f"Found {len(df[['Region', 'Country', 'State', 'City']].drop_duplicates())} unique regions")
        
        print("\nPreparing groups...")
        print(f"Found {len(df['Group'].unique())} unique groups")
        
        print("\nPreparing attack types...")
        print(f"Found {len(df['AttackType'].unique())} unique attack types")
        
        print("\nPreparing targets...")
        print(f"Found {len(df[['Target', 'Target_type']].drop_duplicates())} unique targets")
        
        print("\nPreparing weapon types...")
        print(f"Found {len(df['Weapon_type'].unique())} unique weapon types")
        
        print("\nStarting database operations...")
        result = transformer.prepare_for_postgres()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print("\nLoading completed!")
        print(f"Loaded {len(result['regions'])} regions")
        print(f"Loaded {len(result['groups'])} groups")
        print(f"Loaded {len(result['attack_types'])} attack types")
        print(f"Loaded {len(result['targets'])} targets")
        print(f"Loaded {len(result['weapon_types'])} weapon types")
        print(f"Loaded {len(result['terror_events'])} terror events")
        print(f"\nTotal time: {duration:.2f} seconds")
        
    except Exception as e:
        print(f"\nError during ETL process: {str(e)}")
        print("Rolling back changes...")
        session.rollback()
        raise
    finally:
        print("\nClosing database connection...")
        session.close()
    
    print("\nETL process completed!")

if __name__ == "__main__":
    run_etl()