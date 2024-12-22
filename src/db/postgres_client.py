from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd
from typing import Optional

class PostgresClient:
    def __init__(self, connection_string: str):
        self.engine: Optional[Engine] = None
        self.connection_string = connection_string
        
    def connect(self) -> None:
        self.engine = create_engine(self.connection_string)
        
    def load_events(self, df: pd.DataFrame) -> None:
        if not self.engine:
            self.connect()
            
        df.to_sql('terror_events', self.engine, 
                  if_exists='replace', index=False)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        if not self.engine:
            self.connect()
        return pd.read_sql(query, self.engine) 