from neo4j import GraphDatabase
from typing import Dict, Any
import pandas as pd

class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def create_constraints(self) -> None:
        with self.driver.session() as session:
            session.run("""
                CREATE CONSTRAINT group_name IF NOT EXISTS 
                FOR (g:Group) REQUIRE g.name IS UNIQUE
            """)
            session.run("""
                CREATE CONSTRAINT region_name IF NOT EXISTS 
                FOR (r:Region) REQUIRE r.name IS UNIQUE
            """)
    
    def load_entities(self, entities: Dict[str, pd.DataFrame]) -> None:
        with self.driver.session() as session:
            for _, row in entities['groups'].iterrows():
                session.run("""
                    MERGE (g:Group {name: $name})
                """, name=row['Group'])
            
            for _, row in entities['regions'].iterrows():
                session.run("""
                    MERGE (r:Region {
                        name: $region,
                        country: $country,
                        state: $state,
                        city: $city,
                        lat: $lat,
                        lon: $lon
                    })
                """, 
                region=row['Region'],
                country=row['Country'],
                state=row['state'],
                city=row['city'],
                lat=float(row['latitude']) if pd.notna(row['latitude']) else None,
                lon=float(row['longitude']) if pd.notna(row['longitude']) else None
                )
    
    def load_relationships(self, relationships: pd.DataFrame) -> None:
        with self.driver.session() as session:
            for _, row in relationships.iterrows():
                session.run("""
                    MATCH (g:Group {name: $group})
                    MATCH (r:Region {name: $region})
                    MERGE (g)-[a:ATTACKED {
                        event_id: $event_id,
                        attack_type: $attack_type,
                        target: $target
                    }]->(r)
                """, 
                group=row['Group'],
                region=row['Region'],
                event_id=row['eventid'],
                attack_type=row['AttackType'],
                target=row['Target']
                )
    
    def close(self) -> None:
        self.driver.close() 