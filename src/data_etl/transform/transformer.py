import pandas as pd
from typing import Dict, Any

class DataTransformer:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
    def prepare_for_databases(self) -> Dict[str, Any]:
        postgres_df = self.df.copy()
        
        entities = {
            'groups': self.df[['Group']].drop_duplicates(),
            'regions': self.df[['Region', 'Country', 'State', 'City', 
                              'Latitude', 'Longitude']].drop_duplicates(),
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
        transformed_data = {}
        
        regions_df = self.df[['Region', 'Country', 'State', 'City', 'Latitude', 'Longitude']].drop_duplicates()
        regions_df.columns = ['name', 'country', 'state', 'city', 'latitude', 'longitude']
        regions_df['id'] = range(1, len(regions_df) + 1)
        transformed_data['regions'] = regions_df
        
        groups_df = self.df[['Group']].drop_duplicates()
        groups_df.columns = ['name']
        groups_df['id'] = range(1, len(groups_df) + 1)
        transformed_data['groups'] = groups_df
        
        attack_types_df = self.df[['AttackType']].drop_duplicates()
        attack_types_df.columns = ['name']
        attack_types_df['id'] = range(1, len(attack_types_df) + 1)
        transformed_data['attack_types'] = attack_types_df
        
        targets_df = self.df[['Target', 'Target_type']].drop_duplicates()
        targets_df.columns = ['name', 'type']
        targets_df['id'] = range(1, len(targets_df) + 1)
        transformed_data['targets'] = targets_df
        
        weapon_types_df = self.df[['Weapon_type']].drop_duplicates()
        weapon_types_df.columns = ['name']
        weapon_types_df['id'] = range(1, len(weapon_types_df) + 1)
        transformed_data['weapon_types'] = weapon_types_df
        
        events_df = self.df.merge(regions_df, 
            left_on=['Region', 'Country', 'State', 'City'], 
            right_on=['name', 'country', 'state', 'city'], 
            how='left',
            suffixes=('', '_region')
        ).merge(groups_df,
            left_on='Group',
            right_on='name',
            how='left',
            suffixes=('', '_group')
        ).merge(attack_types_df,
            left_on='AttackType',
            right_on='name',
            how='left',
            suffixes=('', '_attack')
        ).merge(targets_df,
            left_on=['Target', 'Target_type'],
            right_on=['name', 'type'],
            how='left',
            suffixes=('', '_target')
        ).merge(weapon_types_df,
            left_on='Weapon_type',
            right_on='name',
            how='left',
            suffixes=('', '_weapon')
        )

        # בדיקת וטיפול בכפילויות
        print(f"מספר שורות לפני הסרת כפילויות: {len(events_df)}")
        events_df = events_df.drop_duplicates(subset=['eventid'])
        print(f"מספר שורות אחרי הסרת כפילויות: {len(events_df)}")

        date_columns = ['Month', 'Day']
        for col in date_columns:
            events_df.loc[events_df[col] == 0, col] = 1
        events_df['date'] = pd.to_datetime(
            {
                'year': events_df['Year'],
                'month': events_df['Month'],
                'day': events_df['Day']
            }
        )
        
        events_df = events_df[[
            'eventid', 'date', 'id', 'id_group', 'id_attack', 'id_target', 'id_weapon',
            'Killed', 'Wounded',
            'Summary', 'Motive', 'Nperps'
        ]]
        
        events_df.columns = [
            'event_id', 'date', 'region_id', 'group_id', 'attack_type_id', 'target_id', 'weapon_type_id',
            'killed', 'wounded',
            'summary', 'motive','num_perpetrators'
        ]
        transformed_data['terror_events'] = events_df
        
        return transformed_data