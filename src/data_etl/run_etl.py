import pandas as pd
import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

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
    
    print("Creating tables...")
    engine = create_engine(settings.POSTGRES_URL)
    init_db(engine)
    
    print("Transforming data...")
    transformer = DataTransformer(df)
    transformed_data = transformer.prepare_for_postgres()
    
    print("Loading to database...")
    table_order = ['regions', 'groups', 'attack_types', 'targets', 'weapon_types', 'terror_events']
    
    with engine.begin() as conn:
        for table_name in table_order:
            if table_name in transformed_data:
                print(f"Loading {table_name}...")
                transformed_data[table_name].to_sql(
                    table_name, 
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
    
    print("ETL process completed!")

if __name__ == "__main__":
    run_etl()