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
def get_most_active_groups_by_region(session, region_name=None, limit=5):
    query = (
        session.query(
            Region.name.label('region_name'),
            Group.name.label('group_name'),
            func.count(TerrorEvent.event_id).label('attack_count'),
            func.max(Region.latitude).label('latitude'),
            func.max(Region.longitude).label('longitude'),
            func.sum(TerrorEvent.killed).label('total_killed'),
            func.sum(TerrorEvent.wounded).label('total_wounded')
        )
        .join(TerrorEvent, Region.events)  # שינוי סדר ה-joins
        .join(Group, TerrorEvent.group)
        .filter(
            Group.id.isnot(None),
            Group.name != 'Unknown',
            Region.latitude.isnot(None),
            Region.longitude.isnot(None)
        )
        .group_by(Region.name, Group.name)
        .having(func.count(TerrorEvent.event_id) >= 5)
        .order_by(Region.name, desc('attack_count'))
    )

    results = query.all()

    grouped_results = {}
    for r in results:
        if r.region_name not in grouped_results:
            grouped_results[r.region_name] = {
                'top_groups': [],
                'location': {'lat': r.latitude, 'lng': r.longitude}
            }
        if len(grouped_results[r.region_name]['top_groups']) < limit:
            grouped_results[r.region_name]['top_groups'].append({
                'group_name': r.group_name,
                'attack_count': r.attack_count
            })

    return grouped_results

def get_region_correlation_stats(session, region_name=None):
    query = (
        session.query(
            Region.name.label('region_name'),
            Region.latitude.label('latitude'),
            Region.longitude.label('longitude'),
            func.count(TerrorEvent.event_id).label('total_events'),
            func.sum(
                func.coalesce(TerrorEvent.killed, 0) +
                func.coalesce(TerrorEvent.wounded, 0)
            ).label('total_casualties'),
            (func.sum(
                func.coalesce(TerrorEvent.killed, 0) +
                func.coalesce(TerrorEvent.wounded, 0)
            ) / func.count(TerrorEvent.event_id)).label('avg_casualties_per_event')
        )
        .join(TerrorEvent)
        .filter(
            Region.latitude.isnot(None),
            Region.longitude.isnot(None)
        )
        .group_by(Region.name, Region.latitude, Region.longitude)
    )

    if region_name:
        query = query.filter(Region.name == region_name)

    results = query.all()

    correlation_data = {
        r.region_name: {
            'location': {'lat': r.latitude, 'lng': r.longitude},
            'stats': {
                'total_events': r.total_events,
                'total_casualties': r.total_casualties,
                'avg_casualties': r.avg_casualties_per_event,
                'correlation_score': (r.total_casualties / r.total_events)
                                   if r.total_events > 0 else 0
            }
        }
        for r in results
    }

    return correlation_data


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

    results = get_most_active_groups_by_region(session)

    middle_east_results = get_most_active_groups_by_region(session, region_name="Middle East & North Africa")

    for region, data in results.items():
        print(f"\nRegion: {region}")
        print(f"Location: ({data['location']['lat']}, {data['location']['lng']})")
        print("Top Active Groups:")
        for group in data['top_groups']:
            print(f"- {group['group_name']}: {group['attack_count']} attacks")


    all_correlations = get_region_correlation_stats(session)

    print("\nRegion Correlation Stats:")
    print("--------------------------------")
    for region, data in all_correlations.items():
        print(f"Region: {region}")
        print(f"Total Events: {data['stats']['total_events']}")
        print(f"Total Casualties: {data['stats']['total_casualties']}")
        print(f"Avg Casualties per Event: {data['stats']['avg_casualties']:.2f}")
        print(f"Correlation Score: {data['stats']['correlation_score']:.2f}")
        print("--------------------------------")

    

    # groups_results = get_most_active_groups_by_region(session)
    # groups_map = map_manager.create_active_groups_map(groups_results)
    # groups_map.save('active_groups_map.html')

    # correlation_results = get_region_correlation_stats(session)
    # correlation_map = map_manager.create_correlation_map(correlation_results)
    # correlation_map.save('correlation_map.html')

    session.close()
