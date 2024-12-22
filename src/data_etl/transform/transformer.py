from sqlalchemy.orm import Session
import numpy as np
from src.db.models.regions import Region
from src.db.models.groups import Group
from src.db.models.attack_types import AttackType
from src.db.models.targets import Target
from src.db.models.weapon_types import WeaponType
from src.db.models.terror_events import TerrorEvent

class DataTransformer:
    def __init__(self, df, session: Session):
        self.df = df
        self.session = session
        
    def prepare_for_databases(self):
        postgres_df = self.df.copy() 
        return {
            'postgres': postgres_df
        } 

    def prepare_for_postgres(self):
        self.session.query(TerrorEvent).delete()
        self.session.query(Region).delete()
        self.session.query(Group).delete()
        self.session.query(AttackType).delete()
        self.session.query(Target).delete()
        self.session.query(WeaponType).delete()
        self.session.commit()

        print("\nProcessing regions...")
        regions = []
        seen_regions = set()
        for _, row in self.df[['Region', 'Country', 'State', 'City', 'Latitude', 'Longitude']].drop_duplicates().iterrows():
            key = (row['Region'], row['Country'], row['State'], row['City'])
            if key not in seen_regions:
                regions.append(Region(
                    name=row['Region'],
                    country=row['Country'],
                    state=row['State'],
                    city=row['City'],
                    latitude=row['Latitude'],
                    longitude=row['Longitude']
                ))
                seen_regions.add(key)
        
        print("Processing groups...")
        groups = [Group(name=name) for name in set(self.df['Group'].unique())]
        
        print("Processing attack types...")
        attack_types = [AttackType(name=name) for name in set(self.df['AttackType'].unique())]
        
        print("Processing targets...")
        seen_targets = set()
        targets = []
        for _, row in self.df[['Target', 'Target_type']].drop_duplicates().iterrows():
            key = (row['Target'], row['Target_type'])
            if key not in seen_targets:
                targets.append(Target(name=row['Target'], type=row['Target_type']))
                seen_targets.add(key)
        
        print("Processing weapon types...")
        weapon_types = [WeaponType(name=name) for name in set(self.df['Weapon_type'].unique())]

        print("Saving base entities...")
        self.session.add_all(regions)
        self.session.add_all(groups)
        self.session.add_all(attack_types)
        self.session.add_all(targets)
        self.session.add_all(weapon_types)
        self.session.commit()

        print("Creating ID mappings...")
        region_map = {
            (r.name, r.country, r.state, r.city): r.id 
            for r in self.session.query(Region).all()
        }
        group_map = {g.name: g.id for g in self.session.query(Group).all()}
        attack_type_map = {a.name: a.id for a in self.session.query(AttackType).all()}
        target_map = {(t.name, t.type): t.id for t in self.session.query(Target).all()}
        weapon_type_map = {w.name: w.id for w in self.session.query(WeaponType).all()}
        
        print("Processing terror events...")
        terror_events = []
        for _, row in self.df.iterrows():
            month = row['Month'] if row['Month'] != 0 else 1
            day = row['Day'] if row['Day'] != 0 else 1
            
            # טיפול בערכים חסרים ובטווחי מספרים
            killed = int(row['Killed']) if not np.isnan(row['Killed']) else None
            wounded = int(row['Wounded']) if not np.isnan(row['Wounded']) else None
            num_perpetrators = int(row['Nperps']) if not np.isnan(row['Nperps']) else None
            
            event = TerrorEvent(
                event_id=str(row['eventid']),  # המרה למחרוזת
                date=f"{row['Year']}-{month}-{day}",
                region_id=region_map.get((row['Region'], row['Country'], row['State'], row['City'])),
                group_id=group_map.get(row['Group']),
                attack_type_id=attack_type_map.get(row['AttackType']),
                target_id=target_map.get((row['Target'], row['Target_type'])),
                weapon_type_id=weapon_type_map.get(row['Weapon_type']),
                killed=killed,
                wounded=wounded,
                summary=row['Summary'],
                motive=row['Motive'],
                num_perpetrators=num_perpetrators
            )
            terror_events.append(event)

        print("Saving terror events...")
        batch_size = 500
        for i in range(0, len(terror_events), batch_size):
            self.session.add_all(terror_events[i:i + batch_size])
            self.session.commit()

        return {
            'regions': regions,
            'groups': groups,
            'attack_types': attack_types,
            'targets': targets,
            'weapon_types': weapon_types,
            'terror_events': terror_events
        }