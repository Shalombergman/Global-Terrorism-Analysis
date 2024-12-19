from sqlalchemy import create_engine
from db.models.base import Base
from db.models.terror_events import TerrorEvent
from db.models.groups import Group
from db.models.regions import Region
from db.models.attack_types import AttackType
from db.models.targets import Target
from db.models.weapon_types import WeaponType
from config.settings import get_settings

def init_db():
    settings = get_settings()
    engine = create_engine(settings.POSTGRES_URL)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    init_db()