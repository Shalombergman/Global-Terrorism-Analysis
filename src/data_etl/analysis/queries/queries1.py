from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from src.db.models.attack_types import AttackType
from src.db.models.groups import Group
from src.db.models.regions import Region
from src.db.models.terror_events import TerrorEvent

def get_top_attack_types(session, limit=None) -> list:
    query = (
        session.query(
            AttackType.name,
            func.count(TerrorEvent.event_id).label('total_attacks'),
            func.sum(TerrorEvent.killed).label('total_killed'),
            func.sum(TerrorEvent.wounded).label('total_wounded'),
            (func.sum(TerrorEvent.killed) * 2 + func.sum(TerrorEvent.wounded)).label('severity_score')
        )
        .join(TerrorEvent)
        .group_by(AttackType.name)
        .order_by(desc('severity_score'))
    )

    if limit:
        query = query.limit(limit)

    return query.all()


def get_region_severity_stats(session, limit=None):
    severity_score = (func.coalesce(TerrorEvent.killed, 0) * 2 + 
                     func.coalesce(TerrorEvent.wounded, 0))
    
    query = (
        session.query(
            Region.name,
            func.count(TerrorEvent.event_id).label('total_attacks'),
            func.avg(severity_score).label('avg_severity_score_per_event')
        )
        .join(TerrorEvent)
        .group_by(Region.name)
        .order_by(func.avg(severity_score).desc())
    )

    if limit:
        query = query.limit(limit)

    return query.all()

def get_deadliest_groups(session, limit=5):
    severity_score = (func.coalesce(TerrorEvent.killed, 0) * 2 + 
                     func.coalesce(TerrorEvent.wounded, 0))
    query = (
        session.query(
            Group.name.label('group_name'),
            func.count(TerrorEvent.event_id).label('total_attacks'),
            func.sum(severity_score).label('total_casualties')
        )
        .join(TerrorEvent)
        .group_by(Group.name)
        .order_by(desc('total_casualties'))

    )
    if limit:
        query = query.limit(limit)
    
    return query.all()


if __name__ == "__main__":
    from src.config.settings import get_settings
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    settings = get_settings()
    engine = create_engine(settings.POSTGRES_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    results = get_top_attack_types(session, limit=5)
    print("\nTop 5 Most Severe Attack Types:")
    print("--------------------------------")
    for r in results:
        print(f"Attack Type: {r.name}")
        print(f"Total Attacks: {r.total_attacks}")
        print(f"Total Killed: {r.total_killed or 'No Data'}")
        print(f"Total Wounded: {r.total_wounded or 'No Data'}")
        print(f"Severity Score: {r.severity_score}")
        print("--------------------------------")

    avg_results = get_region_severity_stats(session, limit=5)
    print("\nTop Locations by Average Severity:")
    print("------------------------------------")
    for r in avg_results:
        print(f"Region: {r.name}")
        print(f"Total Attacks: {r.total_attacks}")
        print(f"Avg Severity Score: {r.avg_severity_score_per_event:.2f}")
        print("--------------------------------")

    results = get_deadliest_groups(session, limit=5)
    print("\nTop 5 Deadliest Terror Groups:")
    print("--------------------------------")
    for r in results:
        print(f"Group: {r.group_name}")
        print(f"Total Attacks: {r.total_attacks}")
        print(f"Total Casualties: {r.total_casualties}")
        print("--------------------------------")

    session.close()
