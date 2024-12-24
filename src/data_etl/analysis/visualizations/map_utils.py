import folium
from branca.colormap import LinearColormap
from typing import Dict, Any, List
import math

def create_severity_map(severity_results: List) -> folium.Map:
    m = folium.Map(location=[20, 0], zoom_start=2)
    values = []
    for r in severity_results:
        try:
            if r.avg_severity_score_per_event is not None:
                values.append(float(r.avg_severity_score_per_event))
        except (ValueError, AttributeError) as e:
            print(f"Error with {r.name}: {e}")
            continue
    print("Valid values:", len(values))
    
    if not values:
        print("No valid values found!")
        return m
        
    colormap = LinearColormap(
        colors=['green', 'yellow', 'red'],
        vmin=min(values),
        vmax=max(values)
    )
    
    markers_added = 0
    for r in severity_results:
        try:
            if r.latitude is not None and r.longitude is not None:
                lat, lng = float(r.latitude), float(r.longitude)
                if not (math.isnan(lat) or math.isnan(lng)):
                    folium.Marker(
                        location=[lat, lng],
                        popup=f"""
                            <b>{r.name}</b><br>
                            Total Attacks: {r.total_attacks}<br>
                            Severity Score: {float(r.avg_severity_score_per_event):.2f}
                        """,
                        icon=folium.Icon(color=get_color(r.avg_severity_score_per_event, values),
                                       icon='info-sign')
                    ).add_to(m)
                    markers_added += 1
        except (ValueError, AttributeError) as e:
            print(f"Error adding marker for {r.name}: {e}")
            continue
    
    colormap.add_to(m)
    return m

def create_active_groups_map(groups_data: Dict[str, Any]) -> folium.Map:
    m = folium.Map(location=[20, 0], zoom_start=2)
    markers_added = 0
    for region, data in groups_data.items():
        try:
            lat = float(data['location']['lat'])
            lng = float(data['location']['lng'])
            
            if not (math.isnan(lat) or math.isnan(lng)):
                popup_html = f"<b>{region}</b><br>Top Groups:<br>"
                for group in data['top_groups']:
                    popup_html += f"- {group['group_name']}: {group['attack_count']} attacks<br>"
                
                folium.Marker(
                    location=[lat, lng],
                    popup=popup_html,
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
                markers_added += 1
        except (ValueError, TypeError, KeyError) as e:
            print(f"Error adding marker for {region}: {e}")
            continue
    
    return m

def create_correlation_map(correlation_data: Dict[str, Any]) -> folium.Map:
    m = folium.Map(location=[20, 0], zoom_start=2)
    values = []
    for region, data in correlation_data.items():
        try:
            score = data['stats']['correlation_score']
            if not math.isnan(score):
                values.append(score)
        except (KeyError, TypeError) as e:
            print(f"Error processing {region}: {e}")
            continue
    
    print("Valid correlation scores:", len(values))
    
    if not values:
        print("No valid correlation scores found!")
        return m
    
    markers_added = 0
    for region, data in correlation_data.items():
        try:
            lat = float(data['location']['lat'])
            lng = float(data['location']['lng'])
            
            if not (math.isnan(lat) or math.isnan(lng)):
                popup_html = f"""
                    <b>{region}</b><br>
                    Events: {data['stats']['total_events']}<br>
                    Casualties: {data['stats']['total_casualties']}<br>
                    Avg Casualties/Event: {data['stats']['avg_casualties']:.2f}<br>
                    Correlation Score: {data['stats']['correlation_score']:.2f}
                """
                
                folium.Marker(
                    location=[lat, lng],
                    popup=popup_html,
                    icon=folium.Icon(
                        color=get_color(data['stats']['correlation_score'], values),
                        icon='info-sign'
                    )
                ).add_to(m)
                markers_added += 1
        except (ValueError, TypeError, KeyError) as e:
            print(f"Error adding marker for {region}: {e}")
            continue
    
    print(f"Total markers added: {markers_added}")
    return m

def get_color(value: float, values: List[float]) -> str:
    min_val, max_val = min(values), max(values)
    third = (max_val - min_val) / 3
    
    if value <= min_val + third:
        return 'green'
    elif value <= min_val + 2 * third:
        return 'orange'
    else:
        return 'red'