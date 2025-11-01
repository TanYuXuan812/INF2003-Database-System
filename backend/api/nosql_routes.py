"""
NoSQL/MongoDB API routes
"""
from flask import Blueprint, request, jsonify
import logging
from datetime import datetime
from queries.mongo_query import (
    log_event,
    get_events,
    get_event_funnel,
    get_top_viewed_movies,
    get_hourly_activity,
    get_user_engagement,
    get_conversion_funnel,
    get_trending_movies,
    get_multi_dimensional_analytics,
    get_user_profile,
    update_user_profile,
    get_collection_stats,
    get_pipeline_definition
)

logger = logging.getLogger(__name__)

nosql_bp = Blueprint('nosql', __name__)


# ============================================================================
# Events
# ============================================================================

@nosql_bp.route('/events', methods=['POST'])
def create_event():
    """Log a new event"""
    try:
        data = request.json

        # Validate required fields
        if 'type' not in data:
            return jsonify({'error': 'type is required'}), 400

        # Add timestamp if not provided
        if 'timestamp' not in data:
            data['timestamp'] = datetime.utcnow()

        event_id = log_event(data)

        return jsonify({'event_id': event_id, 'status': 'logged'}), 201

    except Exception as e:
        logger.error(f'Error logging event: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/events', methods=['GET'])
def list_events():
    """Get events with optional filters"""
    try:
        # Pagination
        limit = min(int(request.args.get('limit', 100)), 1000)
        skip = int(request.args.get('skip', 0))

        # Filters
        filters = {}
        event_type = request.args.get('type')
        user_id = request.args.get('user_id')
        movie_id = request.args.get('movie_id')

        if event_type:
            filters['type'] = event_type
        if user_id:
            filters['user_id'] = int(user_id)
        if movie_id:
            filters['movie_id'] = int(movie_id)

        events = get_events(filters, limit, skip)

        return jsonify({
            'data': events,
            'count': len(events),
            'limit': limit,
            'skip': skip
        })

    except Exception as e:
        logger.error(f'Error getting events: {e}')
        return jsonify({'error': str(e)}), 500


# ============================================================================
# Analytics
# ============================================================================

@nosql_bp.route('/analytics/funnel', methods=['GET'])
def event_funnel():
    """Get event funnel analysis"""
    try:
        days = int(request.args.get('days', 30))
        result = get_event_funnel(days)

        return jsonify({
            'data': result,
            'period_days': days
        })

    except Exception as e:
        logger.error(f'Error getting event funnel: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/analytics/top-viewed', methods=['GET'])
def top_viewed():
    """Get top viewed movies"""
    try:
        days = int(request.args.get('days', 30))
        limit = min(int(request.args.get('limit', 10)), 50)

        result = get_top_viewed_movies(days, limit)

        return jsonify({
            'data': result,
            'period_days': days
        })

    except Exception as e:
        logger.error(f'Error getting top viewed movies: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/analytics/hourly-activity', methods=['GET'])
def hourly_activity():
    """Get hourly activity pattern"""
    try:
        days = int(request.args.get('days', 7))
        result = get_hourly_activity(days)

        return jsonify({
            'data': result,
            'period_days': days
        })

    except Exception as e:
        logger.error(f'Error getting hourly activity: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/analytics/user-engagement', methods=['GET'])
def user_engagement():
    """Get user engagement scores"""
    try:
        days = int(request.args.get('days', 30))
        limit = min(int(request.args.get('limit', 20)), 100)

        result = get_user_engagement(days, limit)

        return jsonify({
            'data': result,
            'period_days': days
        })

    except Exception as e:
        logger.error(f'Error getting user engagement: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/analytics/conversion-funnel', methods=['GET'])
def conversion_funnel():
    """Get conversion funnel metrics"""
    try:
        days = int(request.args.get('days', 30))
        result = get_conversion_funnel(days)

        return jsonify({
            'data': result,
            'period_days': days
        })

    except Exception as e:
        logger.error(f'Error getting conversion funnel: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/analytics/trending', methods=['GET'])
def trending():
    """Get trending movies"""
    try:
        days = int(request.args.get('days', 7))
        limit = min(int(request.args.get('limit', 20)), 50)

        result = get_trending_movies(days, limit)

        return jsonify({
            'data': result,
            'period_days': days
        })

    except Exception as e:
        logger.error(f'Error getting trending movies: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/analytics/multi-dimensional', methods=['GET'])
def multi_dimensional():
    """Get multi-dimensional analytics using $facet"""
    try:
        days = int(request.args.get('days', 30))
        result = get_multi_dimensional_analytics(days)

        return jsonify({
            'data': result,
            'period_days': days
        })

    except Exception as e:
        logger.error(f'Error getting multi-dimensional analytics: {e}')
        return jsonify({'error': str(e)}), 500


# ============================================================================
# User Profiles
# ============================================================================

@nosql_bp.route('/profiles/<int:user_id>', methods=['GET'])
def get_profile(user_id):
    """Get user profile"""
    try:
        profile = get_user_profile(user_id)

        if not profile:
            return jsonify({'error': 'Profile not found'}), 404

        return jsonify(profile)

    except Exception as e:
        logger.error(f'Error getting user profile: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/profiles/<int:user_id>', methods=['PUT'])
def update_profile(user_id):
    """Update user profile"""
    try:
        data = request.json
        success = update_user_profile(user_id, data)

        if success:
            return jsonify({'status': 'updated'})
        else:
            return jsonify({'error': 'Failed to update profile'}), 500

    except Exception as e:
        logger.error(f'Error updating user profile: {e}')
        return jsonify({'error': str(e)}), 500


# ============================================================================
# Admin / Diagnostics
# ============================================================================

@nosql_bp.route('/admin/stats', methods=['GET'])
def collection_stats():
    """Get collection statistics"""
    try:
        stats = get_collection_stats()
        return jsonify(stats)

    except Exception as e:
        logger.error(f'Error getting collection stats: {e}')
        return jsonify({'error': str(e)}), 500


@nosql_bp.route('/admin/pipeline', methods=['GET'])
def pipeline_info():
    """Get aggregation pipeline definition"""
    try:
        pipeline_name = request.args.get('name', 'funnel')
        pipeline_def = get_pipeline_definition(pipeline_name)

        if not pipeline_def:
            return jsonify({'error': 'Pipeline not found'}), 404

        return jsonify(pipeline_def)

    except Exception as e:
        logger.error(f'Error getting pipeline definition: {e}')
        return jsonify({'error': str(e)}), 500
