import pandas as pd
from typing import Dict, Any

class DataTransformer:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
    def prepare_for_databases(self) -> Dict[str, Any]:
        """הכנת הנתונים למבנה המתאים לשני בסיסי הנתונים"""
        
        # נתונים לפוסטגרס
        postgres_df = self.df.copy()
        postgres_df['severity_score'] = postgres_df['Killed'] * 2 + postgres_df['Wounded']
        
        # נתונים לנאו4ג'יי
        entities = {
            'groups': self.df[['Group']].drop_duplicates(),
            'regions': self.df[['Region', 'Country', 'state', 'city', 
                              'latitude', 'longitude']].drop_duplicates(),
            'attack_types': self.df[['AttackType']].drop_duplicates(),
            'targets': self.df[['Target', 'Target_type']].drop_duplicates()
        }
        
        relationships = self.df[['Group', 'eventid', 'AttackType', 
                               'Target', 'Region']].copy()
        
        return {
            'postgres': postgres_df,
            'neo4j_entities': entities,
            'neo4j_relationships': relationships
        } 

    def prepare_for_postgres(self) -> Dict[str, pd.DataFrame]:
        """הכנת הנתונים לטבלאות המנורמלות"""
        
        # טבלאות lookup
        regions = self.df[['Region', 'Country', 'state', 'city', 
                          'latitude', 'longitude']].drop_duplicates()
        groups = self.df[['Group']].drop_duplicates()
        attack_types = self.df[['AttackType']].drop_duplicates()
        targets = self.df[['Target', 'Target_type']].drop_duplicates()
        weapon_types = self.df[['Weapon_type']].drop_duplicates()
        
        # הוספת מזהים
        regions['id'] = range(1, len(regions) + 1)
        groups['id'] = range(1, len(groups) + 1)
        attack_types['id'] = range(1, len(attack_types) + 1)
        targets['id'] = range(1, len(targets) + 1)
        weapon_types['id'] = range(1, len(weapon_types) + 1)
        
        # מיזוג המזהים לטבלת האירועים
        events = self.df.merge(regions, on=['Region', 'Country', 'state', 'city'], 
                              how='left', suffixes=('', '_region'))
        events = events.merge(groups, on='Group', how='left', suffixes=('', '_group'))
        events = events.merge(attack_types, on='AttackType', how='left', suffixes=('', '_attack'))
        events = events.merge(targets, on=['Target', 'Target_type'], how='left', suffixes=('', '_target'))
        events = events.merge(weapon_types, on='Weapon_type', how='left', suffixes=('', '_weapon'))
        
        return {
            'regions': regions,
            'groups': groups,
            'attack_types': attack_types,
            'targets': targets,
            'weapon_types': weapon_types,
            'events': events
        } 