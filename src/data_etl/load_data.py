# import pandas as pd
# from sqlalchemy.orm import Session
# from sqlalchemy import create_engine
# from src.db.models.terror_events import TerrorEvent
# from src.db.models.groups import Group
# from src.db.models.regions import Region
# from src.db.models.attack_types import AttackType
# from src.db.models.targets import Target
# from src.db.models.weapon_types import WeaponType
# from src.config.settings import get_settings
# from datetime import datetime

# def load_data():
#     settings = get_settings()
#     engine = create_engine(settings.POSTGRES_URL)
    
#     df = pd.read_csv('csv_data/globalterrorismdb_clean.csv')
#     with Session(engine) as session:
#         groups = {name: Group(name=name) for name in df['Group'].unique()}
#         regions = {name: Region(name=name) for name in df['Region'].unique()}
#         attack_types = {name: AttackType(name=name) for name in df['AttackType'].unique()}
#         targets = {name: Target(name=name) for name in df['Target'].unique()}
#         weapon_types = {name: WeaponType(name=name) for name in df['Weapon_type'].unique()}
#         targets = {name: Target(name=name) for name in df['Target'].unique()}
#         session.add_all(groups.values())
#         session.add_all(regions.values())
#         session.add_all(attack_types.values())
#         session.add_all(targets.values())
#         session.add_all(weapon_types.values())
#         session.commit()
#         session.add_all(targets.values())
#         events = []
#         for _, row in df.iterrows():
#             date = datetime(int(row['Year']), int(row['Month'] or 1), int(row['Day'] or 1))
#             event = TerrorEvent(
#                 event_id=row['eventid'],
#                 date=date,
#                 region=regions[row['Region']],
#                 group=groups.get(row['Group']),
#                 attack_type=attack_types[row['AttackType']],
#                 target=targets.get(row['Target']),
#                 weapon_type=weapon_types.get(row['Weapon_type']),
#                 killed=row['Killed'],
#                 wounded=row['Wounded'],
#                 casualties=row['casualties'],
#                 severity_score=row['severity'],
#                 summary=row['Summary'],
#                 motive=row['Motive'],
#                 success=bool(row['success']),
#                 num_perpetrators=row['nperps']
#             )
#             events.append(event)

# if __name__ == "__main__":
#     load_data()