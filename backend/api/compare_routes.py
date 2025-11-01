"""
Comparison API routes - SQL vs NoSQL performance and feature comparison
"""
from flask import Blueprint, request, jsonify, current_app
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import time
from queries.mongo_query import get_top_viewed_movies, get_trending_movies

logger = logging.getLogger(__name__)

compare_bp = Blueprint('compare', __name__)


def get_db_connection():
    """Get PostgreSQL database connection"""
    return psycopg2.connect(current_app.config['DATABASE_URL'])


# ============================================================================
# Comparison Endpoints
# ============================================================================

@compare_bp.route('/top-movies', methods=['GET'])
def compare_top_movies():
    """
    Compare top movies query between SQL and NoSQL

    SQL: Uses GROUP BY, AVG, COUNT on ratings table
    NoSQL: Uses aggregation pipeline on events collection
    """
    try:
        limit = min(int(request.args.get('limit', 10)), 50)
        days = int(request.args.get('days', 30))

        results = {
            'sql': {'query': '', 'results': [], 'time_ms': 0, 'method': ''},
            'nosql': {'query': '', 'results': [], 'time_ms': 0, 'method': ''}
        }

        # SQL Query
        sql_query = """
            SELECT
                m.movie_id,
                m.title,
                m.poster_path,
                AVG(r.rating) AS avg_rating,
                COUNT(r.rating_id) AS rating_count
            FROM movies m
            INNER JOIN ratings r ON m.movie_id = r.movie_id
            WHERE r.timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            GROUP BY m.movie_id, m.title, m.poster_path
            HAVING COUNT(r.rating_id) >= 5
            ORDER BY avg_rating DESC, rating_count DESC
            LIMIT %s
        """

        start_time = time.time()
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql_query, (days, limit))
        sql_results = cur.fetchall()
        cur.close()
        conn.close()
        sql_time = (time.time() - start_time) * 1000

        results['sql'] = {
            'query': sql_query.strip(),
            'results': sql_results,
            'time_ms': round(sql_time, 2),
            'method': 'PostgreSQL: GROUP BY with AVG aggregation on indexed foreign key',
            'row_count': len(sql_results)
        }

        # NoSQL Query
        nosql_description = f"""
MongoDB Aggregation Pipeline:
[
  {{ $match: {{ type: 'page_view', movie_id: {{ $ne: null }}, timestamp: {{ $gte: <{days} days ago> }} }} }},
  {{ $group: {{ _id: '$movie_id', view_count: {{ $sum: 1 }}, unique_viewers: {{ $addToSet: '$user_id' }} }} }},
  {{ $project: {{ movie_id: '$_id', view_count: 1, unique_viewer_count: {{ $size: '$unique_viewers' }} }} }},
  {{ $sort: {{ view_count: -1 }} }},
  {{ $limit: {limit} }}
]
        """

        start_time = time.time()
        nosql_results = get_top_viewed_movies(days, limit)
        nosql_time = (time.time() - start_time) * 1000

        results['nosql'] = {
            'query': nosql_description.strip(),
            'results': nosql_results,
            'time_ms': round(nosql_time, 2),
            'method': 'MongoDB: Aggregation pipeline with $group, $addToSet, $size operators',
            'row_count': len(nosql_results)
        }

        # Comparison summary
        faster = 'sql' if sql_time < nosql_time else 'nosql'
        speedup = max(sql_time, nosql_time) / min(sql_time, nosql_time)

        results['comparison'] = {
            'faster': faster,
            'speedup_factor': round(speedup, 2),
            'difference_ms': round(abs(sql_time - nosql_time), 2),
            'analysis': f"{faster.upper()} was {speedup:.2f}x faster for this query"
        }

        return jsonify(results)

    except Exception as e:
        logger.error(f'Error comparing top movies: {e}')
        return jsonify({'error': str(e)}), 500


@compare_bp.route('/trending', methods=['GET'])
def compare_trending():
    """
    Compare trending movies calculation

    SQL: Complex query with CASE WHEN and date calculations
    NoSQL: Aggregation pipeline with weighted scoring
    """
    try:
        limit = min(int(request.args.get('limit', 10)), 50)
        days = int(request.args.get('days', 7))

        results = {
            'sql': {'query': '', 'results': [], 'time_ms': 0, 'method': ''},
            'nosql': {'query': '', 'results': [], 'time_ms': 0, 'method': ''}
        }

        # SQL Query - calculate trending score
        sql_query = """
            WITH recent_activity AS (
                SELECT
                    movie_id,
                    COUNT(*) AS total_ratings,
                    AVG(rating) AS avg_rating,
                    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(timestamp))) / 86400 AS days_since_last_rating
                FROM ratings
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                GROUP BY movie_id
            )
            SELECT
                m.movie_id,
                m.title,
                m.poster_path,
                ra.total_ratings,
                ra.avg_rating,
                ROUND(
                    (ra.total_ratings * ra.avg_rating * EXP(-0.1 * ra.days_since_last_rating))::numeric,
                    2
                ) AS trending_score
            FROM movies m
            INNER JOIN recent_activity ra ON m.movie_id = ra.movie_id
            ORDER BY trending_score DESC
            LIMIT %s
        """

        start_time = time.time()
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql_query, (days, limit))
        sql_results = cur.fetchall()
        cur.close()
        conn.close()
        sql_time = (time.time() - start_time) * 1000

        results['sql'] = {
            'query': sql_query.strip(),
            'results': sql_results,
            'time_ms': round(sql_time, 2),
            'method': 'PostgreSQL: CTE with exponential decay calculation using EXP function',
            'row_count': len(sql_results)
        }

        # NoSQL Query
        nosql_description = """
MongoDB Aggregation Pipeline with weighted scoring:
- $match: Filter events by type and date
- $addFields: Calculate days_ago and weighted_score with exponential decay
- $group: Aggregate by movie_id
- $sort: Order by trending_score
        """

        start_time = time.time()
        nosql_results = get_trending_movies(days, limit)
        nosql_time = (time.time() - start_time) * 1000

        results['nosql'] = {
            'query': nosql_description.strip(),
            'results': nosql_results,
            'time_ms': round(nosql_time, 2),
            'method': 'MongoDB: Pipeline with $exp, $multiply for time-weighted scoring',
            'row_count': len(nosql_results)
        }

        # Comparison
        faster = 'sql' if sql_time < nosql_time else 'nosql'
        speedup = max(sql_time, nosql_time) / min(sql_time, nosql_time)

        results['comparison'] = {
            'faster': faster,
            'speedup_factor': round(speedup, 2),
            'difference_ms': round(abs(sql_time - nosql_time), 2),
            'analysis': f"{faster.upper()} was {speedup:.2f}x faster. Trending score uses exponential time decay."
        }

        return jsonify(results)

    except Exception as e:
        logger.error(f'Error comparing trending: {e}')
        return jsonify({'error': str(e)}), 500


@compare_bp.route('/search', methods=['GET'])
def compare_search():
    """
    Compare search capabilities

    SQL: ILIKE with trigram indexes for fuzzy search
    NoSQL: Text index search (if available) or regex
    """
    try:
        search_term = request.args.get('q', 'star')
        limit = min(int(request.args.get('limit', 20)), 100)

        results = {
            'sql': {'query': '', 'results': [], 'time_ms': 0, 'method': ''},
            'nosql': {'query': '', 'results': [], 'time_ms': 0, 'method': ''}
        }

        # SQL Query with ILIKE
        sql_query = """
            SELECT
                m.movie_id,
                m.title,
                m.poster_path,
                m.released_date,
                m.popularity,
                SIMILARITY(m.title, %s) AS similarity_score
            FROM movies m
            WHERE m.title ILIKE %s
            ORDER BY similarity_score DESC, m.popularity DESC
            LIMIT %s
        """

        start_time = time.time()
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql_query, (search_term, f'%{search_term}%', limit))
        sql_results = cur.fetchall()
        cur.close()
        conn.close()
        sql_time = (time.time() - start_time) * 1000

        results['sql'] = {
            'query': sql_query.strip(),
            'results': sql_results,
            'time_ms': round(sql_time, 2),
            'method': 'PostgreSQL: ILIKE with pg_trgm extension for fuzzy matching',
            'row_count': len(sql_results)
        }

        # NoSQL Query (simulated - would query MongoDB events for search terms)
        nosql_description = f"""
MongoDB: Text search or regex matching on indexed fields
{{ title: {{ $regex: '{search_term}', $options: 'i' }} }}
Note: In real implementation, would use full-text search index
        """

        # For demo, we'll show the approach but note MongoDB is optimized for events, not movie catalog
        results['nosql'] = {
            'query': nosql_description.strip(),
            'results': [],
            'time_ms': 0,
            'method': 'MongoDB: Better suited for event/log search; movie catalog search is SQL strength',
            'row_count': 0,
            'note': 'MongoDB excels at flexible document queries and aggregations, but structured relational queries like catalog search are better suited for PostgreSQL'
        }

        results['comparison'] = {
            'analysis': 'SQL is better suited for structured catalog search with indexes. MongoDB excels at flexible event aggregations and real-time analytics.'
        }

        return jsonify(results)

    except Exception as e:
        logger.error(f'Error comparing search: {e}')
        return jsonify({'error': str(e)}), 500


@compare_bp.route('/benchmark', methods=['POST'])
def benchmark():
    """
    Run multiple iterations of a query to get performance statistics

    Accepts: { "query_type": "top_movies", "iterations": 10, "params": {...} }
    Returns: Latency statistics (p50, p95, p99, min, max, avg)
    """
    try:
        data = request.json
        query_type = data.get('query_type', 'top_movies')
        iterations = min(int(data.get('iterations', 10)), 100)
        params = data.get('params', {})

        if query_type not in ['top_movies', 'trending']:
            return jsonify({'error': 'Invalid query_type. Choose: top_movies, trending'}), 400

        sql_times = []
        nosql_times = []

        # Run iterations
        for i in range(iterations):
            if query_type == 'top_movies':
                limit = params.get('limit', 10)
                days = params.get('days', 30)

                # SQL
                start = time.time()
                conn = get_db_connection()
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute("""
                    SELECT m.movie_id, AVG(r.rating) AS avg_rating
                    FROM movies m
                    INNER JOIN ratings r ON m.movie_id = r.movie_id
                    GROUP BY m.movie_id
                    ORDER BY avg_rating DESC
                    LIMIT %s
                """, (limit,))
                cur.fetchall()
                cur.close()
                conn.close()
                sql_times.append((time.time() - start) * 1000)

                # NoSQL
                start = time.time()
                get_top_viewed_movies(days, limit)
                nosql_times.append((time.time() - start) * 1000)

        # Calculate statistics
        def calculate_percentile(data, percentile):
            sorted_data = sorted(data)
            index = int(len(sorted_data) * percentile / 100)
            return sorted_data[min(index, len(sorted_data) - 1)]

        sql_stats = {
            'min': round(min(sql_times), 2),
            'max': round(max(sql_times), 2),
            'avg': round(sum(sql_times) / len(sql_times), 2),
            'p50': round(calculate_percentile(sql_times, 50), 2),
            'p95': round(calculate_percentile(sql_times, 95), 2),
            'p99': round(calculate_percentile(sql_times, 99), 2)
        }

        nosql_stats = {
            'min': round(min(nosql_times), 2),
            'max': round(max(nosql_times), 2),
            'avg': round(sum(nosql_times) / len(nosql_times), 2),
            'p50': round(calculate_percentile(nosql_times, 50), 2),
            'p95': round(calculate_percentile(nosql_times, 95), 2),
            'p99': round(calculate_percentile(nosql_times, 99), 2)
        }

        return jsonify({
            'query_type': query_type,
            'iterations': iterations,
            'params': params,
            'sql': sql_stats,
            'nosql': nosql_stats,
            'comparison': {
                'sql_faster_on_avg': sql_stats['avg'] < nosql_stats['avg'],
                'avg_difference_ms': round(abs(sql_stats['avg'] - nosql_stats['avg']), 2),
                'sql_more_consistent': (sql_stats['max'] - sql_stats['min']) < (nosql_stats['max'] - nosql_stats['min'])
            }
        })

    except Exception as e:
        logger.error(f'Error running benchmark: {e}')
        return jsonify({'error': str(e)}), 500
