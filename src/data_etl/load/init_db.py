from sqlalchemy import create_engine
from src.db.models.base import Base
from src.db.models.attack_types import AttackType
from src.db.models.groups import Group
from src.db.models.regions import Region
from src.db.models.targets import Target
from src.db.models.terror_events import TerrorEvent
from src.db.models.weapon_types import WeaponType

def init_db(engine):
    Base.metadata.create_all(bind=engine)