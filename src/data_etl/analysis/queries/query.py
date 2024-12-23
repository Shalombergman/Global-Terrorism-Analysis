from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from src.db.models.attack_types import AttackType
from src.db.models.groups import Group
from src.db.models.regions import Region
from src.db.models.terror_events import TerrorEvent
from src.data_etl.analysis.visualizations.map_utils import (
    create_severity_map,
    create_active_groups_map,
    create_correlation_map
)
from dataclasses import dataclass
from typing import Optional

@dataclass
class RegionStats:
    name: str
    latitude: float
    longitude: float
    total_attacks: int
    avg_severity_score_per_event: float

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
    REGION_COORDINATES = {
        'East Asia': (35.8617, 104.1954),
        'Sub-Saharan Africa': (-8.7832, 34.5085),
        'Middle East & North Africa': (26.8206, 30.8025),
        'North America': (40.7128, -74.0060),
        'South Asia': (20.5937, 78.9629),
        'Central Asia': (41.2044, 74.7661),
        'Central America & Caribbean': (15.7835, -90.2308),
        'Eastern Europe': (50.4501, 30.5234),
        'Southeast Asia': (13.7563, 100.5018),
        'South America': (-15.7975, -47.8919),
        'Australasia & Oceania': (-25.2744, 133.7751),
        'Western Europe': (48.8566, 2.3522)
    }

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

    results = []
    for r in query.all():
        lat, lng = REGION_COORDINATES.get(r.name, (None, None))
        results.append(RegionStats(
            name=r.name,
            latitude=lat,
            longitude=lng,
            total_attacks=r.total_attacks,
            avg_severity_score_per_event=r.avg_severity_score_per_event
        ))

    return results

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
    REGION_COORDINATES = {
        'East Asia': (35.8617, 104.1954),
        'Sub-Saharan Africa': (-8.7832, 34.5085),
        'Middle East & North Africa': (26.8206, 30.8025),
        'North America': (40.7128, -74.0060),
        'South Asia': (20.5937, 78.9629),
        'Central Asia': (41.2044, 74.7661),
        'Central America & Caribbean': (15.7835, -90.2308),
        'Eastern Europe': (50.4501, 30.5234),
        'Southeast Asia': (13.7563, 100.5018),
        'South America': (-15.7975, -47.8919),
        'Australasia & Oceania': (-25.2744, 133.7751),
        'Western Europe': (48.8566, 2.3522)
    }

    query = (
        session.query(
            Region.name.label('region_name'),
            Group.name.label('group_name'),
            func.count(TerrorEvent.event_id).label('attack_count')
        )
        .join(TerrorEvent, Region.events)
        .join(Group, TerrorEvent.group)
        .filter(
            Group.id.isnot(None),
            Group.name != 'Unknown'
        )
        .group_by(Region.name, Group.name)
        .order_by(Region.name, desc('attack_count'))
    )

    results = query.all()

    grouped_results = {}
    for r in results:
        if r.region_name not in grouped_results:
            lat, lng = REGION_COORDINATES.get(r.region_name, (None, None))
            grouped_results[r.region_name] = {
                'top_groups': [],
                'location': {'lat': lat, 'lng': lng}
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


# if __name__ == "__main__":
#     from src.config.settings import get_settings
#     from sqlalchemy import create_engine
#     from sqlalchemy.orm import sessionmaker
#
#     settings = get_settings()
#     engine = create_engine(settings.POSTGRES_URL)
#     Session = sessionmaker(bind=engine)
#     session = Session()
#
#     results = get_top_attack_types(session, limit=5)
#     print("\nTop 5 Most Severe Attack Types:")
#     print("--------------------------------")
#     for r in results:
#         print(f"Attack Type: {r.name}")
#         print(f"Total Attacks: {r.total_attacks}")
#         print(f"Total Killed: {r.total_killed or 'No Data'}")
#         print(f"Total Wounded: {r.total_wounded or 'No Data'}")
#         print(f"Severity Score: {r.severity_score}")
#         print("--------------------------------")
#
#     avg_results = get_region_severity_stats(session, limit=5)
#     print("\nTop Locations by Average Severity:")
#     print("------------------------------------")
#     for r in avg_results:
#         print(f"Region: {r.name}")
#         print(f"Total Attacks: {r.total_attacks}")
#         print(f"Avg Severity Score: {r.avg_severity_score_per_event:.2f}")
#         print("--------------------------------")
#
#     results = get_deadliest_groups(session, limit=5)
#     print("\nTop 5 Deadliest Terror Groups:")
#     print("--------------------------------")
#     for r in results:
#         print(f"Group: {r.group_name}")
#         print(f"Total Attacks: {r.total_attacks}")
#         print(f"Total Casualties: {r.total_casualties}")
#         print("--------------------------------")
#
#     results = get_most_active_groups_by_region(session)
#
#     middle_east_results = get_most_active_groups_by_region(session, region_name="Middle East & North Africa")
#
#     for region, data in results.items():
#         print(f"\nRegion: {region}")
#         print(f"Location: ({data['location']['lat']}, {data['location']['lng']})")
#         print("Top Active Groups:")
#         for group in data['top_groups']:
#             print(f"- {group['group_name']}: {group['attack_count']} attacks")
#
#
#     all_correlations = get_region_correlation_stats(session)
#
#     print("\nRegion Correlation Stats:")
#     print("--------------------------------")
#     for region, data in all_correlations.items():
#         print(f"Region: {region}")
#         print(f"Total Events: {data['stats']['total_events']}")
#         print(f"Total Casualties: {data['stats']['total_casualties']}")
#         print(f"Avg Casualties per Event: {data['stats']['avg_casualties']:.2f}")
#         print(f"Correlation Score: {data['stats']['correlation_score']:.2f}")
#         print("--------------------------------")
#
#
#
#     print("\nCreating maps...")
#
#     # מפת חומרה
#     severity_results = get_region_severity_stats(session)
#     severity_map = create_severity_map(severity_results)
#     severity_map.save('severity_map.html')
#     print("Severity map saved as 'severity_map.html'")
#
#     groups_results = get_most_active_groups_by_region(session)
#     groups_map = create_active_groups_map(groups_results)
#     groups_map.save('active_groups_map.html')
#     print("Active groups map saved as 'active_groups_map.html'")
#
#     correlation_results = get_region_correlation_stats(session)
#     correlation_map = create_correlation_map(correlation_results)
#     correlation_map.save('correlation_map.html')
#     print("Correlation map saved as 'correlation_map.html'")
#
#     session.close()
