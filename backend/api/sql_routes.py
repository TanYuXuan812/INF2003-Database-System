"""
SQL/PostgreSQL API routes
"""
from flask import Blueprint, request, jsonify, current_app
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import time

logger = logging.getLogger(__name__)

sql_bp = Blueprint('sql', __name__)


def get_db_connection():
    """Get PostgreSQL database connection"""
    return psycopg2.connect(current_app.config['DATABASE_URL'])


# ============================================================================
# Movies CRUD
# ============================================================================

@sql_bp.route('/movies', methods=['GET'])
def get_movies():
    """Get movies with pagination and filtering"""
    try:
        page = int(request.args.get('page', 1))
        page_size = min(int(request.args.get('page_size', 20)), current_app.config['MAX_PAGE_SIZE'])
        search = request.args.get('search', '')
        genre = request.args.get('genre', '')
        min_rating = request.args.get('min_rating', None)

        offset = (page - 1) * page_size

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Build query
        where_clauses = []
        params = []

        if search:
            where_clauses.append("m.title ILIKE %s")
            params.append(f'%{search}%')

        if genre:
            where_clauses.append("g.genre_name = %s")
            params.append(genre)

        if min_rating:
            where_clauses.append("AVG(r.rating) >= %s")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Count total
        count_query = f"""
            SELECT COUNT(DISTINCT m.movie_id)
            FROM movies m
            LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
            LEFT JOIN genres g ON mg.genre_id = g.genre_id
            LEFT JOIN ratings r ON m.movie_id = r.movie_id
            {where_sql}
        """
        cur.execute(count_query, params if not min_rating else params[:-1])
        total = cur.fetchone()['count']

        # Get movies
        query = f"""
            SELECT m.movie_id, m.title, m.released_date, m.popularity, m.poster_path,
                   COALESCE(AVG(r.rating), 0) AS avg_rating,
                   COUNT(DISTINCT r.rating_id) AS rating_count,
                   STRING_AGG(DISTINCT g.genre_name, ', ') AS genres
            FROM movies m
            LEFT JOIN ratings r ON m.movie_id = r.movie_id
            LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
            LEFT JOIN genres g ON mg.genre_id = g.genre_id
            {where_sql}
            GROUP BY m.movie_id, m.title, m.released_date, m.popularity, m.poster_path
            {"HAVING AVG(r.rating) >= %s" if min_rating else ""}
            ORDER BY m.popularity DESC NULLS LAST
            LIMIT %s OFFSET %s
        """

        if min_rating:
            params.append(float(min_rating))
        params.extend([page_size, offset])

        cur.execute(query, params)
        movies = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify({
            'data': movies,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total': total,
                'total_pages': (total + page_size - 1) // page_size
            }
        })

    except Exception as e:
        logger.error(f'Error getting movies: {e}')
        return jsonify({'error': str(e)}), 500


@sql_bp.route('/movies/<int:movie_id>', methods=['GET'])
def get_movie(movie_id):
    """Get single movie with details"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT m.*,
                   COALESCE(AVG(r.rating), 0) AS avg_rating,
                   COUNT(DISTINCT r.rating_id) AS rating_count,
                   STRING_AGG(DISTINCT g.genre_name, ', ') AS genres
            FROM movies m
            LEFT JOIN ratings r ON m.movie_id = r.movie_id
            LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
            LEFT JOIN genres g ON mg.genre_id = g.genre_id
            WHERE m.movie_id = %s
            GROUP BY m.movie_id
        """

        cur.execute(query, (movie_id,))
        movie = cur.fetchone()

        if not movie:
            cur.close()
            conn.close()
            return jsonify({'error': 'Movie not found'}), 404

        # Get cast
        cur.execute("""
            SELECT p.person_id, p.name, mc.character_name, mc.cast_order
            FROM movie_cast mc
            JOIN people p ON mc.person_id = p.person_id
            WHERE mc.movie_id = %s
            ORDER BY mc.cast_order NULLS LAST
            LIMIT 10
        """, (movie_id,))
        movie['cast'] = cur.fetchall()

        # Get crew (directors)
        cur.execute("""
            SELECT p.person_id, p.name, mcr.job
            FROM movie_crew mcr
            JOIN people p ON mcr.person_id = p.person_id
            WHERE mcr.movie_id = %s AND mcr.job = 'Director'
        """, (movie_id,))
        movie['directors'] = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(movie)

    except Exception as e:
        logger.error(f'Error getting movie {movie_id}: {e}')
        return jsonify({'error': str(e)}), 500


# ============================================================================
# Analytics
# ============================================================================

@sql_bp.route('/analytics/movie-stats', methods=['GET'])
def get_movie_stats():
    """Get movie statistics from view"""
    try:
        limit = min(int(request.args.get('limit', 20)), 100)

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT * FROM v_movie_stats
            WHERE rating_count >= 5
            ORDER BY avg_rating DESC, rating_count DESC
            LIMIT %s
        """, (limit,))

        stats = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(stats)

    except Exception as e:
        logger.error(f'Error getting movie stats: {e}')
        return jsonify({'error': str(e)}), 500


@sql_bp.route('/analytics/top-rated', methods=['GET'])
def get_top_rated():
    """Get top-rated movies"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT * FROM v_top_rated_movies LIMIT 50")
        movies = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(movies)

    except Exception as e:
        logger.error(f'Error getting top-rated movies: {e}')
        return jsonify({'error': str(e)}), 500


@sql_bp.route('/analytics/genre-stats', methods=['GET'])
def get_genre_stats():
    """Get genre statistics"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT * FROM v_genre_stats ORDER BY movie_count DESC")
        stats = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(stats)

    except Exception as e:
        logger.error(f'Error getting genre stats: {e}')
        return jsonify({'error': str(e)}), 500


@sql_bp.route('/analytics/top-per-genre', methods=['GET'])
def get_top_per_genre():
    """Get top movies per genre using window functions"""
    try:
        limit = min(int(request.args.get('limit', 10)), 20)

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(f"""
            SELECT * FROM v_top_movies_per_genre
            WHERE genre_rank <= %s
            ORDER BY genre_name, genre_rank
        """, (limit,))

        movies = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(movies)

    except Exception as e:
        logger.error(f'Error getting top per genre: {e}')
        return jsonify({'error': str(e)}), 500


# ============================================================================
# Admin / Diagnostics
# ============================================================================

@sql_bp.route('/admin/explain', methods=['GET'])
def explain_query():
    """Get EXPLAIN ANALYZE for named query"""
    try:
        query_name = request.args.get('name', 'movie_stats')

        # Define available queries
        queries = {
            'movie_stats': """
                SELECT m.movie_id, m.title, AVG(r.rating) AS avg_rating
                FROM movies m
                LEFT JOIN ratings r ON m.movie_id = r.movie_id
                GROUP BY m.movie_id, m.title
                ORDER BY avg_rating DESC NULLS LAST
                LIMIT 20
            """,
            'genre_distribution': """
                SELECT g.genre_name, COUNT(mg.movie_id) AS movie_count
                FROM genres g
                LEFT JOIN movie_genres mg ON g.genre_id = mg.genre_id
                GROUP BY g.genre_name
                ORDER BY movie_count DESC
            """,
            'top_rated_with_cast': """
                SELECT m.movie_id, m.title, AVG(r.rating) AS avg_rating,
                       COUNT(DISTINCT mc.id) AS cast_count
                FROM movies m
                INNER JOIN ratings r ON m.movie_id = r.movie_id
                LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
                GROUP BY m.movie_id, m.title
                HAVING AVG(r.rating) >= 4.0
                ORDER BY avg_rating DESC
                LIMIT 10
            """
        }

        if query_name not in queries:
            return jsonify({'error': 'Query not found'}), 404

        conn = get_db_connection()
        cur = conn.cursor()

        # Get EXPLAIN ANALYZE
        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {queries[query_name]}"
        cur.execute(explain_query)
        explain_result = cur.fetchone()[0]

        cur.close()
        conn.close()

        return jsonify({
            'query_name': query_name,
            'sql': queries[query_name],
            'explain': explain_result
        })

    except Exception as e:
        logger.error(f'Error explaining query: {e}')
        return jsonify({'error': str(e)}), 500


@sql_bp.route('/admin/indexes', methods=['GET'])
def get_indexes():
    """Get list of indexes"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)

        indexes = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(indexes)

    except Exception as e:
        logger.error(f'Error getting indexes: {e}')
        return jsonify({'error': str(e)}), 500


# ============================================================================
# Genres
# ============================================================================

@sql_bp.route('/genres', methods=['GET'])
def get_genres():
    """Get all genres"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT g.genre_id, g.genre_name, COUNT(mg.movie_id) AS movie_count
            FROM genres g
            LEFT JOIN movie_genres mg ON g.genre_id = mg.genre_id
            GROUP BY g.genre_id, g.genre_name
            ORDER BY g.genre_name
        """)

        genres = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(genres)

    except Exception as e:
        logger.error(f'Error getting genres: {e}')
        return jsonify({'error': str(e)}), 500


# ============================================================================
# Orders (Transactional Demo)
# ============================================================================

@sql_bp.route('/orders', methods=['POST'])
def create_order():
    """Create order with items (transactional)"""
    try:
        data = request.json
        user_id = data.get('user_id')
        items = data.get('items', [])
        payment_method = data.get('payment_method', 'credit_card')
        shipping_address = data.get('shipping_address', '')

        if not user_id or not items:
            return jsonify({'error': 'user_id and items are required'}), 400

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Use stored function for transactional order creation
        import json
        items_json = json.dumps(items)

        cur.execute("""
            SELECT create_order_with_items(%s, %s::jsonb, %s, %s) AS order_id
        """, (user_id, items_json, payment_method, shipping_address))

        result = cur.fetchone()
        order_id = result['order_id']

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({'order_id': order_id, 'status': 'created'}), 201

    except Exception as e:
        logger.error(f'Error creating order: {e}')
        return jsonify({'error': str(e)}), 500


@sql_bp.route('/orders/<int:user_id>', methods=['GET'])
def get_user_orders(user_id):
    """Get user's orders"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT o.*, COUNT(oi.order_item_id) AS item_count
            FROM orders o
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.user_id = %s
            GROUP BY o.order_id
            ORDER BY o.order_date DESC
        """, (user_id,))

        orders = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(orders)

    except Exception as e:
        logger.error(f'Error getting user orders: {e}')
        return jsonify({'error': str(e)}), 500
