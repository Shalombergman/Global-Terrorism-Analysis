from sqlalchemy import func, desc
from src.db.models.attack_types import AttackType
from src.db.models.groups import Group
from src.db.models.regions import Region
from src.db.models.terror_events import TerrorEvent

from dataclasses import dataclass
import numpy as np


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
            func.count(TerrorEvent.event_id).label('total_events'),
            func.sum(
                func.coalesce(TerrorEvent.killed, 0) +
                func.coalesce(TerrorEvent.wounded, 0)
            ).label('total_casualties'),
            func.array_agg(
                func.coalesce(TerrorEvent.killed, 0) +
                func.coalesce(TerrorEvent.wounded, 0)
            ).label('casualties_per_event')
        )
        .join(TerrorEvent)
        .group_by(Region.name)
    )

    if region_name:
        query = query.filter(Region.name == region_name)

    results = query.all()
    correlation_data = {}

    for r in results:
        lat, lng = REGION_COORDINATES.get(r.region_name, (None, None))
        if lat and lng:
            event_numbers = list(range(len(r.casualties_per_event)))
            casualties = [float(x) for x in r.casualties_per_event]

            try:
                correlation = np.corrcoef(event_numbers, casualties)[0, 1]
            except:
                correlation = 0

            avg_casualties = r.total_casualties / r.total_events if r.total_events > 0 else 0

            correlation_data[r.region_name] = {
                'location': {'lat': lat, 'lng': lng},
                'stats': {
                    'total_events': r.total_events,
                    'total_casualties': r.total_casualties,
                    'avg_casualties': avg_casualties,
                    'correlation_score': float(correlation) if not np.isnan(correlation) else 0
                }
            }

    return correlation_data
