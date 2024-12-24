from sqlalchemy.orm import Session
import pandas as pd
from uuid import uuid4
from src.db.models.terror_events import TerrorEvent
from src.db.models.regions import Region
from src.db.models.groups import Group
from src.db.models.weapon_types import WeaponType
from src.db.models.attack_types import AttackType

def merge_rand_data(session: Session, rand_csv_path: str):
    df = pd.read_csv(rand_csv_path, encoding='latin1')
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%y', errors='coerce')
    df['eventid'] = [f"RAND_{str(uuid4())[:8]}" for _ in range(len(df))]
    
    existing_regions = {(r.country, r.city): r.id for r in session.query(Region).all()}
    existing_groups = {g.name: g.id for g in session.query(Group).all()}
    existing_weapons = {w.name: w.id for w in session.query(WeaponType).all()}
    existing_attack_types = {a.name: a.id for a in session.query(AttackType).all()}
    
    weapon_mapping = {
        'Firearms': 'Firearms',
        'Explosives': 'Explosives',
        'Fire or Firebomb': 'Incendiary',
        'Unknown': 'Unknown'
    }
    
    attack_type_mapping = {
        'Firearms': 'Armed Assault',
        'Explosives': 'Bombing/Explosion',
        'Fire or Firebomb': 'Facility/Infrastructure Attack',
        'Unknown': 'Unknown'
    }

    for _, row in df.iterrows():
        region_key = (row['Country'], row['City'])
        if region_key not in existing_regions:
            new_region = Region(
                country=row['Country'],
                city=row['City'],
                name=f"{row['Country']} - {row['City']}",
                state='Unknown',
                latitude=None,
                longitude=None
            )
            session.add(new_region)
            session.flush()
            existing_regions[region_key] = new_region.id
            
        if row['Perpetrator'] not in existing_groups:
            new_group = Group(name=row['Perpetrator'])
            session.add(new_group)
            session.flush()
            existing_groups[row['Perpetrator']] = new_group.id
            
        weapon_type = weapon_mapping.get(row['Weapon'], 'Unknown')
        if weapon_type not in existing_weapons:
            new_weapon = WeaponType(name=weapon_type)
            session.add(new_weapon)
            session.flush()
            existing_weapons[weapon_type] = new_weapon.id
            
        attack_type = attack_type_mapping.get(row['Weapon'], 'Unknown')
        if attack_type not in existing_attack_types:
            new_attack_type = AttackType(name=attack_type)
            session.add(new_attack_type)
            session.flush()
            existing_attack_types[attack_type] = new_attack_type.id
    
    new_events = []
    print(f"Adding {len(df)} new events")
    for _, row in df.iterrows():
        weapon_type = weapon_mapping.get(row['Weapon'], 'Unknown')
        attack_type = attack_type_mapping.get(row['Weapon'], 'Unknown')
        
        event = TerrorEvent(
            event_id=row['eventid'],
            date=row['Date'],
            region_id=existing_regions[(row['Country'], row['City'])],
            group_id=existing_groups[row['Perpetrator']],
            weapon_type_id=existing_weapons[weapon_type],
            attack_type_id=existing_attack_types[attack_type],
            killed=row['Fatalities'] if pd.notna(row['Fatalities']) else None,
            wounded=row['Injuries'] if pd.notna(row['Injuries']) else None,
            summary=row['Description'],
            target_id=None,
            motive=None,
            num_perpetrators=None
        )
        new_events.append(event)
        
        if len(new_events) >= 500:
            session.add_all(new_events)
            session.commit()
            new_events = []
    
    if new_events:
        session.add_all(new_events)
        session.commit()

    print(f"Added {len(df)} new events")
    return len(df)

if __name__ == "__main__":
    from src.config.settings import get_settings
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    settings = get_settings()
    engine = create_engine(settings.POSTGRES_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    merge_rand_data(session, '/Users/shalom_bergman/kodcode2/Data_engineering_course/Data_course_final_exam/Global_Terrorism_Analysis/csv_data/RAND_Database_of_Worldwide_Terrorism_Incidents.csv')