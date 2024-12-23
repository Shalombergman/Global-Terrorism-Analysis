from flask import Blueprint, jsonify, send_file
from src.data_etl.analysis.queries.query import (
    get_top_attack_types,
    get_region_severity_stats,
    get_deadliest_groups,
    get_most_active_groups_by_region,
    get_region_correlation_stats,
    create_severity_map,
    create_active_groups_map,
    create_correlation_map
)
from src.api.database import Session
import webbrowser
import os

analysis_bp = Blueprint('analysis', __name__)

@analysis_bp.route('/api/top-attack-types', methods=['GET'])
def top_attack_types():
    with Session() as session:
        results = get_top_attack_types(session)
        return jsonify(results)

@analysis_bp.route('/api/deadliest-groups', methods=['GET'])
def deadliest_groups():
    with Session() as session:
        results = get_deadliest_groups(session)
        return jsonify([{
            'group_name': row.group_name,
            'total_kills': row.total_kills,
            'total_attacks': row.total_attacks,
            'avg_kills_per_attack': row.avg_kills_per_attack
        } for row in results])

@analysis_bp.route('/api/active-groups/<region>', methods=['GET'])
def active_groups_by_region(region):
    with Session() as session:
        results = get_most_active_groups_by_region(session, region_name=region)
        return jsonify(results)

@analysis_bp.route('/api/correlation-stats', methods=['GET'])
def correlation_stats():
    with Session() as session:
        results = get_region_correlation_stats(session)
        return jsonify(results)

@analysis_bp.route('/api/severity-stats', methods=['GET'])
def severity_stats():
    with Session() as session:
        results = get_region_severity_stats(session)
        return jsonify(results)

@analysis_bp.route('/api/maps/severity', methods=['GET'])
def severity_map():
    with Session() as session:
        results = get_region_severity_stats(session)
        map_obj = create_severity_map(results)
        file_path = 'temp_severity_map.html'
        map_obj.save(file_path)
        webbrowser.open('file://' + os.path.abspath(file_path))
        return send_file(file_path)

@analysis_bp.route('/api/maps/active-groups', methods=['GET'])
def active_groups_map():
    with Session() as session:
        results = get_most_active_groups_by_region(session)
        map_obj = create_active_groups_map(results)
        file_path = 'temp_active_groups_map.html'
        map_obj.save(file_path)
        webbrowser.open('file://' + os.path.abspath(file_path))
        return send_file(file_path)

@analysis_bp.route('/api/maps/correlation', methods=['GET'])
def correlation_map():
    with Session() as session:
        results = get_region_correlation_stats(session)
        map_obj = create_correlation_map(results)
        file_path = 'temp_correlation_map.html'
        map_obj.save(file_path)
        webbrowser.open('file://' + os.path.abspath(file_path))
        return send_file(file_path)
