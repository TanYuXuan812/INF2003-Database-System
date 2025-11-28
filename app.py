from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import psycopg2
import os
import time
import re
import statistics
from dotenv import load_dotenv
from datetime import datetime
from admin_query import (
    admin_create_movie,
    admin_get_movie,
    admin_search_movies_by_title,
    admin_update_movie,
    admin_delete_movie,
    admin_create_genre,
    admin_get_genre,
    admin_search_genres_by_name,
    admin_read_genres,
    admin_update_genre,
    admin_delete_genre,
    admin_create_company,
    admin_get_company,
    admin_search_companies_by_name,
    admin_read_companies,
    admin_update_company,
    admin_delete_company,
    admin_get_company_with_movies,
    admin_create_person,
    admin_get_person,
    admin_search_people_by_name,
    admin_read_people,
    admin_update_person,
    admin_delete_person,
    admin_get_rating,
    admin_read_ratings,
    admin_update_rating,
    admin_delete_rating,
    # Movie-Genre junction functions
    admin_create_movie_genre,
    admin_get_movie_genre,
    admin_read_movie_genres,
    admin_delete_movie_genre,
    # Movie-Company junction functions
    admin_create_movie_company,
    admin_get_movie_company,
    admin_read_movie_companies,
    admin_delete_movie_company,
    # Movie Cast functions
    admin_create_movie_cast,
    admin_get_movie_cast,
    admin_read_movie_casts,
    admin_update_movie_cast,
    admin_delete_movie_cast,
    # Movie Crew functions
    admin_create_movie_crew,
    admin_get_movie_crew,
    admin_read_movie_crews,
    admin_update_movie_crew,
    admin_delete_movie_crew,
    conn
)
# Import user query functions
from user_query import (
    search_movies_by_title,
    search_movies_by_genre,
    search_movies_by_production_company,
    search_movies_by_rating,
    search_movies_by_actor,
    search_movies_by_crew,
    get_movie_full_details,
    search_movies_by_title_detailed
)
# Import MongoDB connection module
from mongo_connection import get_mongo_connection, close_mongo_connection

# Import MongoDB admin functions
from mongo_admin_query import (
    admin_create_movie as mongo_admin_create_movie,
    admin_get_movie as mongo_admin_get_movie,
    admin_update_movie as mongo_admin_update_movie,
    admin_delete_movie as mongo_admin_delete_movie,
    admin_search_movies_by_title as mongo_admin_search_movies_by_title,
    admin_read_genres as mongo_admin_read_genres,
    admin_read_companies as mongo_admin_read_companies,
    mongo_admin_read_movie_cast,
    mongo_admin_read_movie_crew,
    mongo_admin_create_person_cast,
    mongo_admin_get_person_cast,
    mongo_admin_update_person_cast,
    mongo_admin_delete_person_cast,
    mongo_admin_create_person_crew,
    mongo_admin_get_person_crew,
    mongo_admin_update_person_crew,
    mongo_admin_delete_person_crew,
    admin_create_genre as mongo_admin_create_genre,
    admin_get_genre as mongo_admin_get_genre,
    admin_read_genres as mongo_admin_read_genres,
    admin_update_genre as mongo_admin_update_genre,
    admin_delete_genre as mongo_admin_delete_genre,
    admin_create_company as mongo_admin_create_company,
    admin_get_company as mongo_admin_get_company,
    admin_update_company as mongo_admin_update_company,
    admin_delete_company as mongo_admin_delete_company,
    admin_search_companies_by_name as mongo_admin_search_companies_by_name,
    mongo_admin_get_all_genres,
    mongo_admin_get_all_companies,
    admin_get_rating as mongo_admin_get_rating,
    admin_read_ratings as mongo_admin_read_ratings,
    admin_update_rating as mongo_admin_update_rating,
    admin_delete_rating as mongo_admin_delete_rating,
    movies_col as mongo_movies_col,
    credits_col as mongo_credits_col,
    ratings_col as mongo_ratings_col
)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')

from admin_query import conn

# Initialize with default DB type from environment
DEFAULT_DB_TYPE = os.getenv('DB_TYPE', 'postgres').lower()

# MongoDB connection cache
_mongo_conn = None

GENDER_OPTIONS = [
    {"value": 0, "label": "Unknown"},
    {"value": 1, "label": "Female"},
    {"value": 2, "label": "Male"},
    {"value": 3, "label": "Non-binary"}
]

def get_current_db():
    """Get database cursor based on current session - for simple version"""
    return conn.cursor()

def get_mongo_db():
    """Get MongoDB connection"""
    global _mongo_conn
    if _mongo_conn is None:
        _mongo_conn = get_mongo_connection()
    return _mongo_conn

def get_db_display_name():
    """Get friendly display name for current database"""
    db_type = session.get('db_type', DEFAULT_DB_TYPE)
    return 'PostgreSQL' if db_type in ['postgres', 'postgresql'] else 'MongoDB'

def is_using_mongodb():
    """Check if currently using MongoDB"""
    db_type = session.get('db_type', DEFAULT_DB_TYPE)
    return db_type in ['mongodb', 'mongo']

@app.context_processor
def inject_db_info():
    """Make database info available to all templates"""
    return {
        'current_db_type': get_db_display_name(),
        'current_db_raw': session.get('db_type', DEFAULT_DB_TYPE)
    }

@app.route('/switch-database', methods=['POST'])
def switch_database():
    """Switch between PostgreSQL and MongoDB"""
    new_db_type = request.form.get('db_type', DEFAULT_DB_TYPE).lower()

    if new_db_type not in ['postgres', 'postgresql', 'mongodb', 'mongo']:
        flash('Invalid database type', 'danger')
        return redirect(url_for('index'))

    if new_db_type in ['postgres', 'postgresql']:
        new_db_type = 'postgres'
    elif new_db_type in ['mongodb', 'mongo']:
        new_db_type = 'mongodb'

    old_db = session.get('db_type', DEFAULT_DB_TYPE)
    session['db_type'] = new_db_type

    db_name = 'PostgreSQL' if new_db_type == 'postgres' else 'MongoDB'
    flash(f'Successfully switched to {db_name}!', 'info')

    # Always redirect to index (movies list) when switching databases
    return redirect(url_for('index'))

def get_db_cursor():
    """Get a new database cursor"""
    return conn.cursor()

# ==================== COMBINED SEARCH HELPER FUNCTIONS ====================

def search_postgres(cur, genre=None, actor=None, year_start=None, year_end=None, sort_by='popularity', limit=100, title=None):
    """
    Combined search for PostgreSQL with direct ratings join.

    Uses direct LEFT JOIN on movie_id (no links bridge needed).
    Shows all movies including those without ratings.

    Args:
        cur: Database cursor
        genre: Genre name to filter by (optional, partial match)
        actor: Actor name to search (optional, partial match)
        year_start: Start year for release date range (optional)
        year_end: End year for release date range (optional)
        sort_by: 'popularity' or 'rating' (default: 'popularity')
        limit: Max results to return (default: 100)
        title: Movie title to search (optional, partial match)

    Returns:
        List of movie dictionaries with ratings (0 for movies without ratings).
    """
    # Build dynamic query parts
    joins = []
    where_conditions = ["1=1"]
    params = []

    # Title filter
    if title:
        where_conditions.append("m.title ILIKE %s")
        params.append(f"%{title}%")

    # Genre filter
    if genre:
        joins.append("JOIN movie_genres mg ON m.movie_id = mg.movie_id")
        joins.append("JOIN genres g ON mg.genre_id = g.genre_id")
        where_conditions.append("g.genre_name ILIKE %s")
        params.append(f"%{genre}%")

    # Actor filter
    if actor:
        joins.append("JOIN movie_cast mc ON m.movie_id = mc.movie_id")
        joins.append("JOIN people p ON mc.person_id = p.person_id")
        where_conditions.append("p.name ILIKE %s")
        params.append(f"%{actor}%")

    # Year range filter
    if year_start and year_end:
        where_conditions.append("EXTRACT(YEAR FROM m.released_date) BETWEEN %s AND %s")
        params.append(int(year_start))
        params.append(int(year_end))

    # Build ORDER BY
    if sort_by == 'rating':
        order_by = "ORDER BY avg_rating DESC NULLS LAST, m.popularity DESC NULLS LAST"
    else:
        order_by = "ORDER BY m.popularity DESC NULLS LAST"

    # Add limit param
    params.append(limit)

    # Build the full query
    joins_sql = "\n".join(joins)
    where_sql = " AND ".join(where_conditions)

    query = f"""
        SELECT
            m.movie_id,
            m.title,
            m.released_date,
            m.popularity,
            COALESCE(AVG(r.rating), 0) as avg_rating,
            COUNT(DISTINCT r.rating_id) as num_ratings,
            STRING_AGG(DISTINCT g2.genre_name, ', ' ORDER BY g2.genre_name) as genres
        FROM movies m
        -- Direct LEFT JOIN - includes all movies
        LEFT JOIN ratings r ON m.movie_id = r.movie_id
        -- Get all genres for display
        LEFT JOIN movie_genres mg2 ON m.movie_id = mg2.movie_id
        LEFT JOIN genres g2 ON mg2.genre_id = g2.genre_id
        {joins_sql}
        WHERE {where_sql}
        GROUP BY m.movie_id, m.title, m.released_date, m.popularity
        {order_by}
        LIMIT %s
    """

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    # Convert to list of dictionaries
    movies = []
    for row in rows:
        movies.append({
            "movie_id": row[0],
            "title": row[1],
            "released_date": row[2],
            "popularity": float(row[3]) if row[3] else 0,
            "avg_rating": float(row[4]) if row[4] else 0,
            "num_ratings": row[5] or 0,
            "genres": row[6] or ""
        })

    return movies


def search_mongo(genre=None, actor=None, year_start=None, year_end=None, sort_by='popularity', limit=100, title=None):
    """
    Combined search for MongoDB with direct ratings lookup.

    Uses direct $lookup on movie_id (no links bridge needed).
    Shows all movies including those without ratings.

    Args:
        genre: Genre name to filter by (optional, partial match)
        actor: Actor name to search (optional, partial match)
        year_start: Start year for release date range (optional)
        year_end: End year for release date range (optional)
        sort_by: 'popularity' or 'rating' (default: 'popularity')
        limit: Max results to return (default: 100)
        title: Movie title to search (optional, partial match)

    Returns:
        List of movie dictionaries with ratings (0 for movies without ratings).
    """
    from mongo_connection import get_mongo_connection

    mongo = get_mongo_connection()
    db = mongo.db

    # Step 1: If actor filter, get TMDB IDs from credits collection first
    tmdb_ids_from_actor = None
    if actor:
        actor_regex = {"$regex": actor, "$options": "i"}
        # Check if cast is array or string
        sample_credit = mongo_credits_col.find_one({"cast": {"$exists": True}})
        if sample_credit and isinstance(sample_credit.get("cast"), list):
            credits_query = {"cast.name": actor_regex}
        else:
            credits_query = {"cast": actor_regex}

        credit_results = list(mongo_credits_col.find(credits_query, {"_id": 0, "id": 1}).limit(500))
        tmdb_ids_from_actor = [c["id"] for c in credit_results if "id" in c]

        # If no movies found for this actor, return empty
        if not tmdb_ids_from_actor:
            return []

    # Step 2: Build aggregation pipeline
    pipeline = []

    # Match stage for filters
    match_conditions = {}

    if title:
        match_conditions["$or"] = [
            {"title": {"$regex": title, "$options": "i"}},
            {"original_title": {"$regex": title, "$options": "i"}}
        ]

    if genre:
        match_conditions["genres"] = {"$elemMatch": {"name": {"$regex": genre, "$options": "i"}}}

    if year_start and year_end:
        match_conditions["release_date"] = {
            "$gte": f"{year_start}-01-01",
            "$lte": f"{year_end}-12-31"
        }

    if tmdb_ids_from_actor:
        match_conditions["id"] = {"$in": tmdb_ids_from_actor}

    if match_conditions:
        pipeline.append({"$match": match_conditions})

    # Direct lookup of ratings using movie id
    pipeline.append({
        "$lookup": {
            "from": "ratings",
            "localField": "id",
            "foreignField": "movieId",
            "as": "ratings"
        }
    })

    # Calculate average rating and count
    pipeline.append({
        "$addFields": {
            "avg_rating": {"$ifNull": [{"$avg": "$ratings.rating"}, 0]},
            "num_ratings": {"$size": "$ratings"}
        }
    })

    # Clean up - remove the ratings array from output
    pipeline.append({
        "$project": {
            "ratings": 0,
            "_id": 0
        }
    })

    # Sort stage
    if sort_by == 'rating':
        pipeline.append({"$sort": {"avg_rating": -1, "popularity": -1}})
    else:
        pipeline.append({"$sort": {"popularity": -1}})

    # Limit
    pipeline.append({"$limit": limit})

    # Execute aggregation
    results = list(mongo_movies_col.aggregate(pipeline))

    # Convert to standardized format
    movies = []
    for doc in results:
        # Extract genre names from embedded array
        genres_list = doc.get("genres", [])
        if isinstance(genres_list, list):
            genre_names = ", ".join([g.get("name", "") for g in genres_list if isinstance(g, dict)])
        else:
            genre_names = ""

        movies.append({
            "movie_id": doc.get("id"),
            "title": doc.get("original_title") or doc.get("title", ""),
            "released_date": doc.get("release_date"),
            "popularity": float(doc.get("popularity", 0)) if doc.get("popularity") else 0,
            "avg_rating": float(doc.get("avg_rating", 0)),
            "num_ratings": doc.get("num_ratings", 0),
            "genres": genre_names
        })

    return movies


# ==================== ROUTES ====================

@app.route('/')
def index():
    """Home page - redirects to movies list"""
    return redirect(url_for('list_movies'))

# ==================== USER ROUTES (READ-ONLY) ====================

@app.route('/user')
def user_home():
    """User home page"""
    return render_template('user.html')

@app.route('/user/advanced-search')
def user_advanced_search():
    """
    User view - Advanced Search (read-only)
    Filter by Title, Genre, Actor, Year Range with Sort by Popularity or Rating
    Always uses PostgreSQL regardless of admin database selection
    """
    # Get filter parameters from query string
    title = request.args.get('title', '').strip()
    genre = request.args.get('genre', '').strip()
    actor = request.args.get('actor', '').strip()
    year_start = request.args.get('year_start', '').strip()
    year_end = request.args.get('year_end', '').strip()
    sort_by = request.args.get('sort_by', 'popularity').strip()

    # Validate sort_by
    if sort_by not in ['popularity', 'rating']:
        sort_by = 'popularity'

    movies = []
    error = None
    # Perform search if form was submitted (any query params present)
    search_performed = len(request.args) > 0

    # Only execute search if form was submitted
    if search_performed:
        # Convert year values to integers if provided
        year_start_int = int(year_start) if year_start else None
        year_end_int = int(year_end) if year_end else None

        # User view always uses PostgreSQL
        try:
            cur = get_db_cursor()
            try:
                movies = search_postgres(
                    cur,
                    title=title if title else None,
                    genre=genre if genre else None,
                    actor=actor if actor else None,
                    year_start=year_start_int,
                    year_end=year_end_int,
                    sort_by=sort_by,
                    limit=100
                )
            finally:
                cur.close()
        except Exception as e:
            error = str(e)
            flash(f'Search error: {error}', 'danger')

    # Get genres list for dropdown (always from PostgreSQL for user view)
    genres_list = []
    try:
        genres_list = admin_read_genres(conn.cursor())
    except Exception as e:
        pass  # Genres dropdown will be empty if this fails

    return render_template('user_queries/user_advanced_search.html',
                           movies=movies,
                           genres_list=genres_list,
                           title=title,
                           genre=genre,
                           actor=actor,
                           year_start=year_start,
                           year_end=year_end,
                           sort_by=sort_by,
                           search_performed=search_performed,
                           error=error)

@app.route('/user/movies')
def user_movies():
    """User view - List movies (read-only)"""
    search_query = request.args.get('search', '').strip()

    cur = None
    try:
        cur = get_db_cursor()

        if search_query:
            # Search by title using user_query function
            movies = search_movies_by_title(cur, search_query, limit=100, offset=0)
        else:
            # Get all movies by searching with empty string
            movies = search_movies_by_title(cur, '', limit=100, offset=0)

        return render_template('user_queries/user_movies.html', movies=movies, search_query=search_query)

    except Exception as e:
        flash(f'Error loading movies: {str(e)}', 'danger')
        return render_template('user_queries/user_movies.html', movies=[], search_query=search_query)
    finally:
        if cur:
            cur.close()

@app.route('/user/crew')
def user_crew():
    """User view - Search movies by crew member (read-only)"""
    search_query = request.args.get('search', '').strip()
    cur = None

    try:
        cur = get_db_cursor()
        movies = []

        if search_query:
            rows = search_movies_by_crew(cur, search_query, job=None, limit=100, offset=0)
            movies = [{
                "movie_id": r[0],
                "title": r[1],
                "released_date": r[2],
                "job": r[3],
                "department": r[4]
            } for r in rows]

        return render_template('user_queries/user_crew.html', movies=movies, search_query=search_query, job_filter='')

    except Exception as e:
        flash(f'Error searching crew: {str(e)}', 'danger')
        return render_template('user_queries/user_crew.html', movies=[], search_query=search_query, job_filter='')
    finally:
        if cur:
            cur.close()

@app.route('/api/user/crew')
def api_user_crew():
    """Live search API for movies by crew member"""
    search_query = request.args.get('search', '').strip()
    cur = None

    try:
        cur = get_db_cursor()
        movies = []
        if search_query:
            rows = search_movies_by_crew(cur, search_query, job=None, limit=50, offset=0)
            movies = [{
                "movie_id": r[0],
                "title": r[1],
                "released_date": r[2].strftime('%Y-%m-%d') if r[2] else None,
                "job": r[3],
                "department": r[4]
            } for r in rows]

        return jsonify({"movies": movies, "search": search_query})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            cur.close()

@app.route('/user/cast')
def user_cast():
    """User view - Search movies by actor (read-only)"""
    search_query = request.args.get('search', '').strip()
    cur = None

    try:
        cur = get_db_cursor()

        movies = []
        if search_query:
            rows = search_movies_by_actor(cur, search_query, limit=100, offset=0)
            movies = [{
                "movie_id": r[0],
                "title": r[1],
                "released_date": r[2],
                "character": r[3],
                "avg_rating": float(r[4]) if r[4] else None
            } for r in rows]

        return render_template('user_queries/user_cast.html', movies=movies, search_query=search_query)

    except Exception as e:
        flash(f'Error searching cast: {str(e)}', 'danger')
        return render_template('user_queries/user_cast.html', movies=[], search_query=search_query)
    finally:
        if cur:
            cur.close()

@app.route('/api/user/cast')
def api_user_cast():
    """Live search API for movies by actor"""
    search_query = request.args.get('search', '').strip()
    cur = None

    try:
        cur = get_db_cursor()

        movies = []
        if search_query:
            rows = search_movies_by_actor(cur, search_query, limit=50, offset=0)
            movies = [{
                "movie_id": r[0],
                "title": r[1],
                "released_date": r[2].strftime('%Y-%m-%d') if r[2] else None,
                "character": r[3],
                "avg_rating": float(r[4]) if r[4] else None
            } for r in rows]

        return jsonify({"movies": movies, "search": search_query})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            cur.close()

@app.route('/user/genres')
def user_genres():
    """User view - List genres (read-only)"""
    search_query = request.args.get('search', '').strip()

    cur = None
    try:
        cur = get_db_cursor()

        if search_query:
            # Search by genre name using admin_query function
            genres = admin_search_genres_by_name(cur, search_query)
        else:
            # Get all genres using admin_query function
            genres = admin_read_genres(cur)

        return render_template('user_queries/user_genres.html', genres=genres, search_query=search_query)

    except Exception as e:
        flash(f'Error loading genres: {str(e)}', 'danger')
        return render_template('user_queries/user_genres.html', genres=[], search_query=search_query)
    finally:
        if cur:
            cur.close()

@app.route('/user/genres/<int:genre_id>')
def user_view_genre(genre_id):
    """User view - View detailed genre information (read-only)"""
    movie_search = request.args.get('movie_search', '').strip()
    cur = None

    try:
        cur = get_db_cursor()

        genre = admin_get_genre(cur, genre_id)
        if not genre:
            flash('Genre not found', 'warning')
            return redirect(url_for('user_genres'))

        # Build query based on whether we're searching or not
        if movie_search:
            # Search for movies by title within this genre
            cur.execute("""
                SELECT m.movie_id, m.title, m.released_date
                FROM movie_genres mg
                JOIN movies m ON mg.movie_id = m.movie_id
                WHERE mg.genre_id = %s AND m.title ILIKE %s
                ORDER BY m.title
                LIMIT 100
            """, (genre_id, f"%{movie_search}%"))
        else:
            # Show top 100 movies in this genre
            cur.execute("""
                SELECT m.movie_id, m.title, m.released_date
                FROM movie_genres mg
                JOIN movies m ON mg.movie_id = m.movie_id
                WHERE mg.genre_id = %s
                ORDER BY m.title
                LIMIT 100
            """, (genre_id,))

        movies = [
            {"movie_id": r[0], "title": r[1], "released_date": r[2]}
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT COUNT(*)
            FROM movie_genres
            WHERE genre_id = %s
        """, (genre_id,))
        movie_count = cur.fetchone()[0]

        return render_template('user_queries/user_genres.html', genre=genre, movies=movies, movie_count=movie_count, genres=None, search_query='', movie_search=movie_search)
    except Exception as e:
        flash(f'Error loading genre: {str(e)}', 'danger')
        return redirect(url_for('user_genres'))
    finally:
        if cur:
            cur.close()

@app.route('/api/user/genres/<int:genre_id>/movies')
def api_search_genre_movies(genre_id):
    """API endpoint for live search of movies in a genre"""
    search_query = request.args.get('search', '').strip()
    cur = None

    try:
        cur = get_db_cursor()

        if search_query:
            # Search for movies by title within this genre
            cur.execute("""
                SELECT m.movie_id, m.title, m.released_date
                FROM movie_genres mg
                JOIN movies m ON mg.movie_id = m.movie_id
                WHERE mg.genre_id = %s AND m.title ILIKE %s
                ORDER BY m.title
                LIMIT 100
            """, (genre_id, f"%{search_query}%"))
        else:
            # Return top 100 movies in this genre
            cur.execute("""
                SELECT m.movie_id, m.title, m.released_date
                FROM movie_genres mg
                JOIN movies m ON mg.movie_id = m.movie_id
                WHERE mg.genre_id = %s
                ORDER BY m.title
                LIMIT 100
            """, (genre_id,))

        movies = [
            {
                "movie_id": r[0],
                "title": r[1],
                "released_date": r[2].strftime('%Y-%m-%d') if r[2] else None
            }
            for r in cur.fetchall()
        ]

        return jsonify({"movies": movies, "search": search_query})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            cur.close()

@app.route('/user/movies/<int:movie_id>')
def user_view_movie(movie_id):
    """User view - View detailed movie information (read-only)"""

    # Use PostgreSQL - use user_query functions (read-only)
    cur = None

    try:
        cur = get_db_cursor()
        # Use get_movie_full_details which includes all movie info, genres, cast, crew, and production companies
        movie_data = get_movie_full_details(cur, movie_id)
        if not movie_data:
            flash('Movie not found', 'warning')
            return redirect(url_for('user_movies'))

        # Extract genres from the movie_data (already included in get_movie_full_details)
        movie_genres = movie_data.get('genres', [])

        # Extract cast, crew, and production companies (for potential future use)
        cast = movie_data.get('cast', [])
        crew = movie_data.get('crew', [])
        production_companies = movie_data.get('production_companies', [])

        return render_template('user_queries/user_movies.html',
                               movie=movie_data,
                               movie_genres=movie_genres,
                               cast=cast,
                               crew=crew,
                               production_companies=production_companies,
                               movies=None,
                               search_query='')
    except Exception as e:
        flash(f'Error loading movie: {str(e)}', 'danger')
        return redirect(url_for('user_movies'))
    finally:
        if cur:
            cur.close()

@app.route('/user/companies')
def user_companies():
    """User view - List production companies (read-only)"""
    search_query = request.args.get('search', '').strip()

    cur = None
    try:
        cur = get_db_cursor()

        if search_query:
            companies = admin_search_companies_by_name(cur, search_query)
        else:
            companies = admin_read_companies(cur)

        return render_template('user_queries/user_companies.html', companies=companies, search_query=search_query)

    except Exception as e:
        flash(f'Error loading companies: {str(e)}', 'danger')
        return render_template('user_queries/user_companies.html', companies=[], search_query=search_query)
    finally:
        if cur:
            cur.close()

@app.route('/user/companies/<int:company_id>')
def user_view_company(company_id):
    """User view - View detailed company information and movies (read-only)"""
    cur = None

    try:
        cur = get_db_cursor()

        company = admin_get_company(cur, company_id)
        if not company:
            flash('Company not found', 'warning')
            return redirect(url_for('user_companies'))

        # Get movies by this company using company_id directly
        cur.execute("""
            SELECT
                m.movie_id,
                m.title,
                m.released_date,
                m.popularity,
                (SELECT AVG(r2.rating) FROM ratings r2 WHERE r2.movie_id = m.movie_id) AS avg_rating
            FROM movies m
            JOIN movie_production_companies mpc ON m.movie_id = mpc.movie_id
            WHERE mpc.company_id = %s
            ORDER BY m.popularity DESC
            LIMIT 10
        """, (company_id,))

        movies = []
        for r in cur.fetchall():
            movies.append({
                "movie_id": r[0],
                "title": r[1],
                "released_date": r[2],
                "popularity": float(r[3]) if r[3] else None,
                "avg_rating": float(r[4]) if r[4] else None
            })

        # Get total movie count
        cur.execute("""
            SELECT COUNT(*)
            FROM movie_production_companies
            WHERE company_id = %s
        """, (company_id,))
        movie_count = cur.fetchone()[0]

        return render_template('user_queries/user_companies.html', company=company, movies=movies, movie_count=movie_count, companies=None, search_query='')
    except Exception as e:
        flash(f'Error loading company: {str(e)}', 'danger')
        return redirect(url_for('user_companies'))
    finally:
        if cur:
            cur.close()

@app.route('/user/ratings')
def user_ratings():
    """User view - Browse movies by ratings with filters (read-only)"""
    cur = None

    try:
        cur = get_db_cursor()

        # Get filter parameters
        rating_filter = request.args.get('rating_filter', 'all')
        sort_by = request.args.get('sort_by', 'random')
        movie_id_search = request.args.get('movie_id', '').strip()
        user_id_search = request.args.get('user_id', '').strip()

        # Build the WHERE and HAVING clauses based on filters
        where_conditions = []
        having_conditions = []
        params = []

        # Movie ID filter (goes in WHERE)
        if movie_id_search:
            where_conditions.append("m.movie_id = %s")
            params.append(int(movie_id_search))

        # User ID filter - only show movies rated by this user
        user_id_join = ""
        if user_id_search:
            user_id_join = "JOIN ratings r2 ON m.movie_id = r2.movie_id AND r2.user_id = %s"
            params.append(int(user_id_search))

        # Build WHERE clause
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        # Rating range filter (goes in HAVING after GROUP BY)
        having_conditions.append("COUNT(DISTINCT r.rating_id) > 0")  # Always ensure movies have ratings

        if rating_filter == 'excellent':
            having_conditions.append("AVG(r.rating) >= 4.0")
        elif rating_filter == 'high':
            having_conditions.append("AVG(r.rating) >= 3.0 AND AVG(r.rating) < 4.0")
        elif rating_filter == 'medium':
            having_conditions.append("AVG(r.rating) >= 2.0 AND AVG(r.rating) < 3.0")
        elif rating_filter == 'low':
            having_conditions.append("AVG(r.rating) < 2.0")

        # Build HAVING clause
        having_clause = "HAVING " + " AND ".join(having_conditions)

        # Build ORDER BY clause
        if sort_by == 'rating_desc':
            order_by = "ORDER BY avg_rating DESC NULLS LAST"
        elif sort_by == 'rating_asc':
            order_by = "ORDER BY avg_rating ASC NULLS LAST"
        elif sort_by == 'popularity_desc':
            order_by = "ORDER BY m.popularity DESC NULLS LAST"
        elif sort_by == 'title':
            order_by = "ORDER BY m.title"
        elif sort_by == 'release_date':
            order_by = "ORDER BY m.released_date DESC NULLS LAST"
        else:  # random
            order_by = "ORDER BY RANDOM()"

        # Build main query
        query = f"""
            SELECT
                m.movie_id,
                m.title,
                m.released_date,
                m.popularity,
                AVG(r.rating) AS avg_rating,
                COUNT(DISTINCT r.rating_id) AS num_ratings,
                STRING_AGG(DISTINCT g.genre_name, ', ' ORDER BY g.genre_name) AS genres
            FROM movies m
            {user_id_join}
            JOIN ratings r ON m.movie_id = r.movie_id
            LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
            LEFT JOIN genres g ON mg.genre_id = g.genre_id
            {where_clause}
            GROUP BY m.movie_id, m.title, m.released_date, m.popularity
            {having_clause}
            {order_by}
            LIMIT 100
        """

        cur.execute(query, tuple(params))

        movies = []
        for r in cur.fetchall():
            movies.append({
                "movie_id": r[0],
                "title": r[1],
                "released_date": r[2],
                "popularity": float(r[3]) if r[3] else None,
                "avg_rating": float(r[4]) if r[4] else None,
                "num_ratings": r[5],
                "genres": r[6]
            })

        # Get total count of movies with ratings (using subquery to handle HAVING)
        count_query = f"""
            SELECT COUNT(*)
            FROM (
                SELECT m.movie_id
                FROM movies m
                {user_id_join}
                JOIN ratings r ON m.movie_id = r.movie_id
                {where_clause}
                GROUP BY m.movie_id
                {having_clause}
            ) AS filtered_movies
        """
        cur.execute(count_query, tuple(params))
        total_movies = cur.fetchone()[0]

        return render_template('user_queries/user_ratings.html',
                             movies=movies,
                             total_movies=total_movies,
                             rating_filter=rating_filter,
                             sort_by=sort_by,
                             movie_id_search=movie_id_search,
                             user_id_search=user_id_search)
    except Exception as e:
        if cur:
            cur.connection.rollback()  # Rollback the failed transaction
        flash(f'Error loading ratings: {str(e)}', 'danger')
        return redirect(url_for('user_home'))
    finally:
        if cur:
            cur.close()

# ==================== ADMIN ROUTES ====================

@app.route('/movies')
def list_movies():
    """List all movies with search functionality"""
    search_query = request.args.get('search', '').strip()

    # Check if using MongoDB
    if is_using_mongodb():
        try:
            mongo = get_mongo_db()

            if search_query:
                # Search movies in MongoDB
                mongo_movies = mongo.search_movies(search_query, limit=100)

                # Also search by ID if query is numeric
                if search_query.isdigit():
                    movie_by_id = mongo.get_movie_by_id(int(search_query))
                    if movie_by_id:
                        # Check if not already in results
                        existing_ids = {m.get('id') for m in mongo_movies}
                        if movie_by_id.get('id') not in existing_ids:
                            mongo_movies.insert(0, movie_by_id)

                # Convert MongoDB format to template format
                movies = []
                for m in mongo_movies:
                    movies.append({
                        "movie_id": m.get('id'),
                        "title": m.get('original_title', 'N/A'),
                        "released_date": m.get('release_date') or m.get('releaseDate'),
                        "popularity": m.get('popularity')
                    })
            else:
                # Get all movies
                mongo_movies = mongo.get_movies(limit=100)
                movies = []
                for m in mongo_movies:
                    movies.append({
                        "movie_id": m.get('id'),
                        "title": m.get('original_title', 'N/A'),
                        "released_date": m.get('release_date') or m.get('releaseDate'),
                        "popularity": m.get('popularity')
                    })

            return render_template('mongo_movies/list.html', movies=movies, search_query=search_query)
        except Exception as e:
            flash(f'Error loading movies from MongoDB: {str(e)}', 'danger')
            return render_template('mongo_movies/list.html', movies=[], search_query=search_query)
    else:
        # Use PostgreSQL
        cur = get_db_cursor()

        try:
            if search_query:
                movies = admin_search_movies_by_title(cur, search_query, limit=100)
                seen_ids = {movie['movie_id'] for movie in movies}

                # Allow searching directly by numeric movie ID
                if search_query.isdigit():
                    movie_by_id = admin_get_movie(cur, int(search_query))
                    if movie_by_id and movie_by_id['movie_id'] not in seen_ids:
                        movies.insert(0, {
                            "movie_id": movie_by_id['movie_id'],
                            "title": movie_by_id['title'],
                            "released_date": movie_by_id.get('released_date'),
                            "popularity": movie_by_id.get('popularity')
                        })
            else:
                cur.execute("""
                    SELECT movie_id, title, released_date, popularity
                    FROM movies
                    ORDER BY created_at DESC
                    LIMIT 100
                """)
                rows = cur.fetchall()
                movies = [
                    {
                        "movie_id": r[0],
                        "title": r[1],
                        "released_date": r[2],
                        "popularity": float(r[3]) if r[3] else None
                    }
                    for r in rows
                ]

            return render_template('movies/list.html', movies=movies, search_query=search_query)
        except Exception as e:
            flash(f'Error loading movies: {str(e)}', 'danger')
            return render_template('movies/list.html', movies=[], search_query=search_query)
        finally:
            cur.close()

@app.route('/api/movies/search')
def search_movies_api():
    """AJAX endpoint to search movies by title for autocomplete widgets"""
    query = request.args.get('q', '').strip()
    if len(query) < 2:
        return jsonify({"movies": []})

    cur = get_db_cursor()
    try:
        movies = admin_search_movies_by_title(cur, query, limit=20)
        formatted = []
        for movie in movies:
            released_date = movie.get("released_date")
            formatted.append({
                "movie_id": movie.get("movie_id"),
                "title": movie.get("title"),
                "released_date": released_date.isoformat() if released_date else None
            })
        return jsonify({"movies": formatted})
    except Exception as e:
        return jsonify({"movies": [], "error": str(e)}), 500
    finally:
        cur.close()

@app.route('/api/mongo/movies/search')
def search_mongo_movies_api():
    """AJAX endpoint to search MongoDB movies by title for autocomplete widgets"""
    query = request.args.get('q', '').strip()
    if len(query) < 2:
        return jsonify({"movies": []})

    try:
        movies = mongo_admin_search_movies_by_title(query, limit=20)
        formatted = []
        for movie in movies:
            released_date = movie.get("released_date")
            formatted.append({
                "movie_id": movie.get("movie_id"),
                "title": movie.get("title"),
                "released_date": released_date if released_date else None
            })
        return jsonify({"movies": formatted})
    except Exception as e:
        return jsonify({"movies": [], "error": str(e)}), 500

@app.route('/api/people/search')
def search_people_api():
    """AJAX endpoint to search people by name for autocomplete widgets"""
    query = request.args.get('q', '').strip()
    if len(query) < 2:
        return jsonify({"people": []})

    cur = get_db_cursor()
    try:
        people = admin_search_people_by_name(cur, query, limit=20)
        return jsonify({"people": people})
    except Exception as e:
        return jsonify({"people": [], "error": str(e)}), 500
    finally:
        cur.close()

@app.route('/movies/<int:movie_id>')
def view_movie(movie_id):
    """View detailed movie information with genres"""

    # Check if using MongoDB
    if is_using_mongodb():
        try:
            mongo = get_mongo_db()
            mongo_movie = mongo.get_movie_by_id(movie_id)

            if not mongo_movie:
                flash('Movie not found', 'warning')
                return redirect(url_for('list_movies'))

            # Convert MongoDB format to template format
            movie = {
                "movie_id": mongo_movie.get('id'),
                "title": mongo_movie.get('original_title', 'N/A'),
                "adult": mongo_movie.get('adult', False),
                "overview": mongo_movie.get('overview'),
                "language": mongo_movie.get('original_language'),
                "popularity": mongo_movie.get('popularity'),
                "released_date": mongo_movie.get('release_date'),
                "runtime": mongo_movie.get('runtime'),
                "poster_path": mongo_movie.get('poster_path'),
                "tagline": mongo_movie.get('tagline'),
                "created_at": mongo_movie.get('created_at'),
                "updated_at": mongo_movie.get('updated_at')
            }

            # Extract genres from embedded array
            movie_genres = []
            if 'genres' in mongo_movie and isinstance(mongo_movie['genres'], list):
                for genre in mongo_movie['genres']:
                    if isinstance(genre, dict):
                        movie_genres.append({
                            "genre_id": genre.get('id', 0),
                            "genre_name": genre.get('name', 'Unknown')
                        })

            return render_template('movies/view.html', movie=movie, movie_genres=movie_genres)
        except Exception as e:
            flash(f'Error loading movie from MongoDB: {str(e)}', 'danger')
            return redirect(url_for('list_movies'))
    else:
        # Use PostgreSQL
        cur = get_db_cursor()

        try:
            movie = admin_get_movie(cur, movie_id)
            if not movie:
                flash('Movie not found', 'warning')
                return redirect(url_for('list_movies'))

            # Get associated genres for this movie
            cur.execute("""
                SELECT g.genre_id, g.genre_name
                FROM movie_genres mg
                JOIN genres g ON mg.genre_id = g.genre_id
                WHERE mg.movie_id = %s
                ORDER BY g.genre_name
            """, (movie_id,))

            movie_genres = [
                {"genre_id": r[0], "genre_name": r[1]}
                for r in cur.fetchall()
            ]

            return render_template('movies/view.html', movie=movie, movie_genres=movie_genres)
        except Exception as e:
            flash(f'Error loading movie: {str(e)}', 'danger')
            return redirect(url_for('list_movies'))
        finally:
            cur.close()

@app.route('/movies/create', methods=['GET', 'POST'])
def create_movie():
    """Create a new movie"""
    if request.method == 'POST':
        try:
            movie_data = {
                'id': int(request.form['movie_id']),
                'original_title': request.form['title'],
                'adult': request.form.get('adult') == 'on',
                'overview': request.form.get('overview', '').strip() or None,
                'original_language': request.form.get('language', '').strip() or None,
                'popularity': float(request.form['popularity']) if request.form.get('popularity') else None,
                'release_date': request.form.get('released_date') or None,
                'runtime': int(request.form['runtime']) if request.form.get('runtime') else None,
                'poster_path': request.form.get('poster_path', '').strip() or None,
                'tagline': request.form.get('tagline', '').strip() or None
            }

            # Check if using MongoDB
            if is_using_mongodb():
                # Use MongoDB admin function
                new_movie_id = mongo_admin_create_movie(movie_data)
                flash(f'Movie "{movie_data["original_title"]}" created successfully in MongoDB!', 'success')
            else:
                # Use PostgreSQL admin function
                cur = get_db_cursor()
                try:
                    # Convert MongoDB field names to PostgreSQL field names
                    pg_movie_data = {
                        'movie_id': movie_data['id'],
                        'title': movie_data['original_title'],
                        'adult': movie_data['adult'],
                        'overview': movie_data['overview'],
                        'language': movie_data['original_language'],
                        'popularity': movie_data['popularity'],
                        'released_date': movie_data['release_date'],
                        'runtime': movie_data['runtime'],
                        'poster_path': movie_data['poster_path'],
                        'tagline': movie_data['tagline']
                    }
                    new_movie_id = admin_create_movie(cur, pg_movie_data)
                    conn.commit()
                    flash(f'Movie "{movie_data["original_title"]}" created successfully in PostgreSQL!', 'success')
                except Exception as e:
                    conn.rollback()
                    raise e
                finally:
                    cur.close()

            return redirect(url_for('view_movie', movie_id=new_movie_id))

        except ValueError as e:
            flash(f'Invalid input: {str(e)}', 'danger')
        except Exception as e:
            flash(f'Error creating movie: {str(e)}', 'danger')

    # Use different templates based on database type
    if is_using_mongodb():
        return render_template('mongo_movies/create.html')
    else:
        return render_template('movies/create.html')

@app.route('/movies/<int:movie_id>/edit', methods=['GET', 'POST'])
def edit_movie(movie_id):
    """Edit an existing movie"""
    try:
        # Check if using MongoDB
        if is_using_mongodb():
            # Get movie from MongoDB
            mongo_movie = mongo_admin_get_movie(movie_id)
            if not mongo_movie:
                flash('Movie not found', 'warning')
                return redirect(url_for('list_movies'))

            if request.method == 'POST':
                update_data = {}

                if request.form.get('title'):
                    update_data['original_title'] = request.form['title']

                update_data['adult'] = request.form.get('adult') == 'on'

                if 'overview' in request.form:
                    update_data['overview'] = request.form['overview'].strip() or None

                if 'language' in request.form:
                    update_data['original_language'] = request.form['language'].strip() or None

                if request.form.get('popularity'):
                    update_data['popularity'] = float(request.form['popularity'])

                if 'released_date' in request.form:
                    update_data['release_date'] = request.form['released_date'] or None

                if request.form.get('runtime'):
                    update_data['runtime'] = int(request.form['runtime'])

                if 'poster_path' in request.form:
                    update_data['poster_path'] = request.form['poster_path'].strip() or None

                if 'tagline' in request.form:
                    update_data['tagline'] = request.form['tagline'].strip() or None

                mongo_admin_update_movie(movie_id, update_data)

                flash(f'Movie "{update_data.get("original_title", mongo_movie.get("original_title"))}" updated successfully in MongoDB!', 'success')
                return redirect(url_for('view_movie', movie_id=movie_id))

            # Normalize MongoDB field names to match template expectations
            movie = {
                "movie_id": mongo_movie.get('id'),
                "title": mongo_movie.get('original_title', ''),
                "adult": mongo_movie.get('adult', False),
                "overview": mongo_movie.get('overview'),
                "language": mongo_movie.get('original_language'),
                "popularity": mongo_movie.get('popularity'),
                "released_date": mongo_movie.get('release_date'),
                "runtime": mongo_movie.get('runtime'),
                "poster_path": mongo_movie.get('poster_path'),
                "tagline": mongo_movie.get('tagline')
            }

            return render_template('mongo_movies/edit.html', movie=movie)
        else:
            # Use PostgreSQL
            cur = get_db_cursor()
            try:
                movie = admin_get_movie(cur, movie_id)
                if not movie:
                    flash('Movie not found', 'warning')
                    return redirect(url_for('list_movies'))

                if request.method == 'POST':
                    update_data = {}

                    if request.form.get('title'):
                        update_data['title'] = request.form['title']

                    update_data['adult'] = request.form.get('adult') == 'on'

                    if 'overview' in request.form:
                        update_data['overview'] = request.form['overview'].strip() or None

                    if 'language' in request.form:
                        update_data['language'] = request.form['language'].strip() or None

                    if request.form.get('popularity'):
                        update_data['popularity'] = float(request.form['popularity'])

                    if 'released_date' in request.form:
                        update_data['released_date'] = request.form['released_date'] or None

                    if request.form.get('runtime'):
                        update_data['runtime'] = int(request.form['runtime'])

                    if 'poster_path' in request.form:
                        update_data['poster_path'] = request.form['poster_path'].strip() or None

                    if 'tagline' in request.form:
                        update_data['tagline'] = request.form['tagline'].strip() or None

                    admin_update_movie(cur, movie_id, update_data)
                    conn.commit()

                    flash(f'Movie "{update_data.get("title", movie["title"])}" updated successfully in PostgreSQL!', 'success')
                    return redirect(url_for('view_movie', movie_id=movie_id))

                return render_template('movies/edit.html', movie=movie)
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cur.close()

    except ValueError as e:
        flash(f'Invalid input: {str(e)}', 'danger')
        return redirect(url_for('edit_movie', movie_id=movie_id))
    except Exception as e:
        flash(f'Error updating movie: {str(e)}', 'danger')
        return redirect(url_for('edit_movie', movie_id=movie_id))

@app.route('/movies/<int:movie_id>/delete', methods=['POST'])
def delete_movie(movie_id):
    """Delete a movie"""
    try:
        # Check if using MongoDB
        if is_using_mongodb():
            # Get movie from MongoDB
            movie = mongo_admin_get_movie(movie_id)
            if not movie:
                flash('Movie not found', 'warning')
                return redirect(url_for('list_movies'))

            mongo_admin_delete_movie(movie_id)

            flash(f'Movie "{movie.get("original_title", "Unknown")}" deleted successfully from MongoDB!', 'success')
            return redirect(url_for('list_movies'))
        else:
            # Use PostgreSQL
            cur = get_db_cursor()
            try:
                movie = admin_get_movie(cur, movie_id)
                if not movie:
                    flash('Movie not found', 'warning')
                    return redirect(url_for('list_movies'))

                admin_delete_movie(cur, movie_id)
                conn.commit()

                flash(f'Movie "{movie["title"]}" deleted successfully from PostgreSQL!', 'success')
                return redirect(url_for('list_movies'))
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cur.close()

    except Exception as e:
        flash(f'Error deleting movie: {str(e)}', 'danger')
        return redirect(url_for('view_movie', movie_id=movie_id))

# ==================== GENRE ROUTES ====================

@app.route('/genres')
def list_genres():
    """List all genres with search functionality"""
    search_query = request.args.get('search', '')

    # Check if using MongoDB
    if is_using_mongodb():
        try:
            # Get all unique genres using the admin function
            genres = mongo_admin_get_all_genres()

            # Apply search filter if provided
            if search_query:
                genres = [
                    genre for genre in genres
                    if search_query.lower() in genre.get('genre_name', '').lower()
                ]

            print(f"[DEBUG] MongoDB returned {len(genres)} unique genres")
            if genres:
                print(f"[DEBUG] Sample genre: {genres[0]}")

            return render_template('mongo_genres/list.html', genres=genres, search_query=search_query)
        except Exception as e:
            flash(f'Error loading genres from MongoDB: {str(e)}', 'danger')
            return render_template('mongo_genres/list.html', genres=[], search_query=search_query)
    else:
        # Use PostgreSQL
        cur = get_db_cursor()

        try:
            if search_query:
                genres = admin_search_genres_by_name(cur, search_query)
            else:
                genres = admin_read_genres(cur)

            return render_template('genres/list.html', genres=genres, search_query=search_query)
        except Exception as e:
            flash(f'Error loading genres: {str(e)}', 'danger')
            return render_template('genres/list.html', genres=[], search_query=search_query)
        finally:
            cur.close()

@app.route('/genres/<int:genre_id>')
def view_genre(genre_id):
    """View detailed genre information"""
    cur = get_db_cursor()
    
    try:
        genre = admin_get_genre(cur, genre_id)
        if not genre:
            flash('Genre not found', 'warning')
            return redirect(url_for('list_genres'))
        
        cur.execute("""
            SELECT m.movie_id, m.title, m.released_date
            FROM movie_genres mg
            JOIN movies m ON mg.movie_id = m.movie_id
            WHERE mg.genre_id = %s
            ORDER BY m.title
            LIMIT 10
        """, (genre_id,))
        
        movies = [
            {"movie_id": r[0], "title": r[1], "released_date": r[2]}
            for r in cur.fetchall()
        ]
        
        cur.execute("""
            SELECT COUNT(*)
            FROM movie_genres
            WHERE genre_id = %s
        """, (genre_id,))
        movie_count = cur.fetchone()[0]
        
        return render_template('genres/view.html', genre=genre, movies=movies, movie_count=movie_count)
    except Exception as e:
        flash(f'Error loading genre: {str(e)}', 'danger')
        return redirect(url_for('list_genres'))
    finally:
        cur.close()

@app.route('/genres/create', methods=['GET', 'POST'])
def create_genre():
    """Create a new genre"""
    if request.method == 'POST':
        try:
            # Check if using MongoDB
            if is_using_mongodb():
                # MongoDB requires movie_id, genre_id, and genre_name
                movie_id = int(request.form['movie_id'])
                genre_id = int(request.form['genre_id'])
                genre_name = request.form['genre_name'].strip()

                if not genre_name:
                    flash('Genre name is required', 'danger')
                    return render_template('mongo_genres/create.html')

                genre_data = {
                    'id': genre_id,
                    'name': genre_name
                }

                result = mongo_admin_create_genre(movie_id, genre_data)

                if result:
                    flash(f'Genre "{genre_name}" added to movie {movie_id} successfully in MongoDB!', 'success')
                    return redirect(url_for('list_genres'))
                else:
                    flash(f'Genre already exists in movie {movie_id}', 'warning')
                    return render_template('mongo_genres/create.html')
            else:
                # PostgreSQL - standalone genre
                cur = get_db_cursor()
                try:
                    genre_name = request.form['genre_name'].strip()

                    if not genre_name:
                        flash('Genre name is required', 'danger')
                        return render_template('genres/create.html')

                    new_genre_id = admin_create_genre(cur, genre_name)
                    conn.commit()

                    flash(f'Genre "{genre_name}" created successfully!', 'success')
                    return redirect(url_for('view_genre', genre_id=new_genre_id))
                except psycopg2.IntegrityError:
                    conn.rollback()
                    flash(f'Genre "{genre_name}" already exists', 'danger')
                except Exception as e:
                    conn.rollback()
                    raise e
                finally:
                    cur.close()

        except ValueError as e:
            flash(f'Invalid input: {str(e)}', 'danger')
        except Exception as e:
            flash(f'Error creating genre: {str(e)}', 'danger')

    # Use different templates based on database type
    if is_using_mongodb():
        return render_template('mongo_genres/create.html')
    else:
        return render_template('genres/create.html')

@app.route('/genres/<int:genre_id>/edit', methods=['GET', 'POST'])
def edit_genre(genre_id):
    """Edit an existing genre"""
    cur = get_db_cursor()
    
    try:
        genre = admin_get_genre(cur, genre_id)
        if not genre:
            flash('Genre not found', 'warning')
            return redirect(url_for('list_genres'))
        
        if request.method == 'POST':
            new_name = request.form['genre_name'].strip()
            
            if not new_name:
                flash('Genre name is required', 'danger')
                return render_template('genres/edit.html', genre=genre)
            
            admin_update_genre(cur, genre_id, new_name)
            conn.commit()
            
            flash(f'Genre updated to "{new_name}" successfully!', 'success')
            return redirect(url_for('view_genre', genre_id=genre_id))
        
        return render_template('genres/edit.html', genre=genre)
        
    except psycopg2.IntegrityError:
        conn.rollback()
        flash('A genre with that name already exists', 'danger')
        return redirect(url_for('edit_genre', genre_id=genre_id))
    except Exception as e:
        conn.rollback()
        flash(f'Error updating genre: {str(e)}', 'danger')
        return redirect(url_for('edit_genre', genre_id=genre_id))
    finally:
        cur.close()

@app.route('/genres/<int:genre_id>/delete', methods=['POST'])
def delete_genre(genre_id):
    """Delete a genre"""
    cur = get_db_cursor()
    
    try:
        genre = admin_get_genre(cur, genre_id)
        if not genre:
            flash('Genre not found', 'warning')
            return redirect(url_for('list_genres'))
        
        admin_delete_genre(cur, genre_id)
        conn.commit()
        
        flash(f'Genre "{genre["genre_name"]}" deleted successfully!', 'success')
        return redirect(url_for('list_genres'))
        
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting genre: {str(e)}', 'danger')
        return redirect(url_for('view_genre', genre_id=genre_id))
    finally:
        cur.close()

# MongoDB-specific genre routes (require movie_id and genre_id)
@app.route('/genres/<movie_id>/<genre_id>')
def view_mongo_genre(movie_id, genre_id):
    """View genre from a specific movie (MongoDB)"""
    try:
        # Resolve genre and a valid movie_id containing it
        def _to_int(val):
            try:
                return int(val)
            except Exception:
                return None

        movie_id_int = _to_int(movie_id)
        genre_id_int = _to_int(genre_id)

        mongo = get_mongo_db()
        # Try to locate a movie containing this genre (int or string)
        genre_doc = mongo.db.movies.find_one(
            {"genres.id": {"$in": [genre_id_int, genre_id, str(genre_id)]}},
            {"_id": 0, "id": 1, "genres": 1}
        )
        if not genre_doc:
            flash('Genre not found', 'warning')
            return redirect(url_for('list_genres'))

        # Extract the genre name from the first matching movie
        genre_name = None
        for g in genre_doc.get("genres", []):
            gid = g.get("id")
            if gid == genre_id_int or gid == genre_id or str(gid) == str(genre_id):
                genre_name = g.get("name")
                break

        if not genre_name:
            flash('Genre not found', 'warning')
            return redirect(url_for('list_genres'))

        resolved_movie_id = genre_doc.get("id")

        # Build movies list and count for this genre
        movies_cursor = mongo.db.movies.find(
            {"genres.id": {"$in": [genre_id_int, genre_id, str(genre_id)]}},
            {"_id": 0, "id": 1, "original_title": 1, "release_date": 1}
        ).sort("popularity", -1).limit(50)
        movies = [{
            "movie_id": m.get("id"),
            "title": m.get("original_title") or m.get("title"),
            "released_date": m.get("release_date")
        } for m in movies_cursor]
        movie_count = mongo.db.movies.count_documents(
            {"genres.id": {"$in": [genre_id_int, genre_id, str(genre_id)]}}
        )

        genre = {"genre_id": genre_id_int or genre_id, "genre_name": genre_name, "movie_id": resolved_movie_id}

        return render_template('mongo_genres/view.html',
                               genre=genre,
                               movies=movies,
                               movie_count=movie_count)
    except Exception as e:
        flash(f'Error loading genre: {str(e)}', 'danger')
        return redirect(url_for('list_genres'))

@app.route('/genres/<movie_id>/<genre_id>/edit', methods=['GET', 'POST'])
def edit_mongo_genre(movie_id, genre_id):
    """Edit genre from a specific movie (MongoDB)"""
    try:
        def _to_int(val):
            try:
                return int(val)
            except Exception:
                return None

        genre = mongo_admin_get_genre(movie_id, genre_id)
        if not genre:
            mongo = get_mongo_db()
            alt = mongo.db.movies.find_one(
                {"genres.id": {"$in": [_to_int(genre_id), genre_id, str(genre_id)]}},
                {"id": 1}
            )
            if alt:
                movie_id = alt.get("id", movie_id)
                genre = mongo_admin_get_genre(movie_id, genre_id)

        if not genre:
            flash('Genre not found', 'warning')
            return redirect(url_for('list_genres'))

        if request.method == 'POST':
            new_name = request.form['genre_name'].strip()

            if not new_name:
                flash('Genre name is required', 'danger')
                genre['movie_id'] = movie_id
                return render_template('mongo_genres/edit.html', genre=genre)

            result = mongo_admin_update_genre(movie_id, genre_id, new_name)

            if result:
                flash(f'Genre updated to "{new_name}" successfully in MongoDB!', 'success')
                return redirect(url_for('view_mongo_genre', movie_id=movie_id, genre_id=genre_id))
            else:
                flash('Genre not found or not updated', 'warning')
                return redirect(url_for('list_genres'))

        # Add movie_id to genre dict for template
        genre['movie_id'] = movie_id
        return render_template('mongo_genres/edit.html', genre=genre)

    except Exception as e:
        flash(f'Error updating genre: {str(e)}', 'danger')
        return redirect(url_for('list_genres'))

@app.route('/genres/<movie_id>/<genre_id>/delete', methods=['POST'])
def delete_mongo_genre(movie_id, genre_id):
    """Delete genre from a specific movie (MongoDB)"""
    try:
        def _to_int(val):
            try:
                return int(val)
            except Exception:
                return None

        genre = mongo_admin_get_genre(movie_id, genre_id)
        if not genre:
            mongo = get_mongo_db()
            alt = mongo.db.movies.find_one(
                {"genres.id": {"$in": [_to_int(genre_id), genre_id, str(genre_id)]}},
                {"id": 1}
            )
            if alt:
                movie_id = alt.get("id", movie_id)
                genre = mongo_admin_get_genre(movie_id, genre_id)

        if not genre:
            flash('Genre not found', 'warning')
            return redirect(url_for('list_genres'))

        result = mongo_admin_delete_genre(movie_id, genre_id)

        if result:
            flash(f'Genre "{genre.get("genre_name", "Unknown")}" deleted successfully from MongoDB!', 'success')
        else:
            flash('Genre not found or not deleted', 'warning')

        return redirect(url_for('list_genres'))

    except Exception as e:
        flash(f'Error deleting genre: {str(e)}', 'danger')
        return redirect(url_for('list_genres'))

# ==================== MONGO COMPANY ROUTES ====================

@app.route('/companies/<movie_id>/<company_id>')
def view_mongo_company(movie_id, company_id):
    """View company from a specific movie (MongoDB)"""
    import ast

    try:
        # Convert to int
        try:
            company_id_int = int(company_id)
        except:
            company_id_int = company_id

        try:
            movie_id_int = int(movie_id)
        except:
            movie_id_int = movie_id

        # Get company details using the admin function
        company_data = mongo_admin_get_company(movie_id_int, company_id_int)

        if not company_data:
            flash('Company not found', 'warning')
            return redirect(url_for('list_companies'))

        # Find all movies with this company (parse stringified production_companies)
        from mongo_admin_query import movies_col
        all_movies = movies_col.find(
            {"production_companies": {"$exists": True, "$ne": None, "$ne": ""}},
            {"_id": 0, "id": 1, "original_title": 1, "release_date": 1, "production_companies": 1}
        )

        matching_movies = []
        movie_count = 0

        for movie in all_movies:
            companies_data = movie.get("production_companies", "")
            if not companies_data or companies_data == "[]":
                continue

            # Handle both stringified and actual array formats
            companies_list = None

            # If it's already a list (new format)
            if isinstance(companies_data, list):
                companies_list = companies_data
            # If it's a string (old format)
            elif isinstance(companies_data, str):
                try:
                    # Parse stringified list
                    companies_list = ast.literal_eval(companies_data)
                except (ValueError, SyntaxError):
                    continue

            if not isinstance(companies_list, list):
                continue

            # Check if this movie has the company
            for company in companies_list:
                if isinstance(company, dict) and company.get("id") == company_id_int:
                    movie_count += 1
                    if len(matching_movies) < 50:  # Limit to 50 movies displayed
                        matching_movies.append({
                            "movie_id": movie.get("id"),
                            "title": movie.get("original_title", ""),
                            "released_date": movie.get("release_date")
                        })
                    break

        company = {
            "company_id": company_id_int,
            "name": company_data["name"],
            "movie_id": movie_id_int
        }

        return render_template('mongo_companies/view.html',
                               company=company,
                               movies=matching_movies,
                               movie_count=movie_count)
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f'Error loading company: {str(e)}', 'danger')
        return redirect(url_for('list_companies'))

@app.route('/companies/<movie_id>/<company_id>/edit', methods=['GET', 'POST'])
def edit_mongo_company(movie_id, company_id):
    """Edit company from a specific movie (MongoDB)"""
    try:
        # Convert to int
        try:
            movie_id_int = int(movie_id)
        except:
            movie_id_int = movie_id

        try:
            company_id_int = int(company_id)
        except:
            company_id_int = company_id

        # Get company details
        company_data = mongo_admin_get_company(movie_id_int, company_id_int)

        if not company_data:
            flash('Company not found', 'warning')
            return redirect(url_for('list_companies'))

        # Add movie_id to company data for template
        company = {
            "company_id": company_data["company_id"],
            "name": company_data["name"],
            "movie_id": movie_id_int
        }

        if request.method == 'POST':
            new_name = request.form.get('company_name', '').strip()
            if not new_name:
                flash('Company name is required', 'danger')
                return render_template('mongo_companies/edit.html', company=company)

            result = mongo_admin_update_company(movie_id_int, company_id_int, new_name)

            if result:
                flash(f'Company updated to "{new_name}" successfully!', 'success')
                return redirect(url_for('view_mongo_company', movie_id=movie_id_int, company_id=company_id_int))
            else:
                flash('Company not found or not updated', 'warning')

        return render_template('mongo_companies/edit.html', company=company)

    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f'Error editing company: {str(e)}', 'danger')
        return redirect(url_for('list_companies'))

@app.route('/companies/<movie_id>/<company_id>/delete', methods=['POST'])
def delete_mongo_company(movie_id, company_id):
    """Delete company from a specific movie (MongoDB)"""
    try:
        # Convert to int
        try:
            movie_id_int = int(movie_id)
        except:
            movie_id_int = movie_id

        try:
            company_id_int = int(company_id)
        except:
            company_id_int = company_id

        # Get company details before deleting
        company_data = mongo_admin_get_company(movie_id_int, company_id_int)

        if not company_data:
            flash('Company not found', 'warning')
            return redirect(url_for('list_companies'))

        company_name = company_data.get("name", "Unknown")

        result = mongo_admin_delete_company(movie_id_int, company_id_int)

        if result:
            flash(f'Company "{company_name}" deleted successfully!', 'success')
        else:
            flash('Company not found or not deleted', 'warning')

        return redirect(url_for('list_companies'))

    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f'Error deleting company: {str(e)}', 'danger')
        return redirect(url_for('list_companies'))

# ==================== MOVIE-GENRE ROUTES ====================

@app.route('/movie-genres')
def list_movie_genres():
    """List all movie-genre associations with optional filtering"""
    movie_id_filter = request.args.get('movie_id', type=int)
    genre_id_filter = request.args.get('genre_id', type=int)
    
    cur = get_db_cursor()
    
    try:
        query = """
            SELECT mg.movie_id, mg.genre_id, m.title as movie_title, g.genre_name
            FROM movie_genres mg
            LEFT JOIN movies m ON mg.movie_id = m.movie_id
            LEFT JOIN genres g ON mg.genre_id = g.genre_id
        """
        
        conditions = []
        params = []
        
        if movie_id_filter:
            conditions.append("mg.movie_id = %s")
            params.append(movie_id_filter)
        
        if genre_id_filter:
            conditions.append("mg.genre_id = %s")
            params.append(genre_id_filter)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY mg.movie_id, mg.genre_id LIMIT 100"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        movie_genres = [
            {
                "movie_id": r[0],
                "genre_id": r[1],
                "movie_title": r[2],
                "genre_name": r[3]
            }
            for r in rows
        ]
        
        movie_info = None
        if movie_id_filter:
            movie_info = admin_get_movie(cur, movie_id_filter)
        
        unique_movies = set(mg['movie_id'] for mg in movie_genres)
        unique_genres = set(mg['genre_id'] for mg in movie_genres)
        
        return render_template('movie_genres/list.html',
                             movie_genres=movie_genres,
                             movie_info=movie_info,
                             unique_movies=unique_movies,
                             unique_genres=unique_genres)
    
    except Exception as e:
        flash(f'Error loading movie-genre associations: {str(e)}', 'danger')
        return render_template('movie_genres/list.html', 
                             movie_genres=[], 
                             movie_info=None,
                             unique_movies=set(),
                             unique_genres=set())
    finally:
        cur.close()


@app.route('/movie-genres/create', methods=['GET', 'POST'])
def create_movie_genre():
    """Add a genre to a movie - TEXT INPUT VERSION"""
    
    if request.method == 'POST':
        cur = get_db_cursor()
        try:
            movie_id_raw = request.form.get('movie_id', '').strip()
            if not movie_id_raw:
                flash('Please select a movie from the search results before submitting.', 'danger')
                cur.close()
                return redirect(url_for('create_movie_genre'))

            movie_id = int(movie_id_raw)
            genre_id = int(request.form['genre_id'])
            
            # Check if movie exists
            movie = admin_get_movie(cur, movie_id)
            if not movie:
                flash(f'Movie with ID {movie_id} not found in database. Please check the Movie ID and try again.', 'danger')
                cur.close()
                return redirect(url_for('create_movie_genre'))
            
            # Check if genre exists
            genre = admin_get_genre(cur, genre_id)
            if not genre:
                flash(f'Genre with ID {genre_id} not found. Please select a valid genre.', 'danger')
                cur.close()
                return redirect(url_for('create_movie_genre'))
            
            # Check if association already exists
            existing = admin_get_movie_genre(cur, movie_id, genre_id)
            if existing:
                flash(f'Movie "{movie["title"]}" already has the genre "{genre["genre_name"]}".', 'warning')
                cur.close()
                return redirect(url_for('list_movie_genres'))
            
            # Create the association
            result = admin_create_movie_genre(cur, movie_id, genre_id)
            conn.commit()
            
            if result:
                flash(f'✓ Successfully added genre "{genre["genre_name"]}" to movie "{movie["title"]}" (ID: {movie_id})', 'success')
            else:
                flash('Association already exists (no changes made)', 'info')
            
            cur.close()
            return redirect(url_for('list_movie_genres'))
            
        except ValueError as e:
            conn.rollback()
            flash(f'Invalid input: Please enter valid numbers for Movie ID and Genre ID.', 'danger')
            if 'cur' in locals():
                cur.close()
            return redirect(url_for('create_movie_genre'))
        except Exception as e:
            conn.rollback()
            flash(f'Error creating association: {str(e)}', 'danger')
            if 'cur' in locals():
                cur.close()
            return redirect(url_for('create_movie_genre'))
    
    # GET request - show form (only need genres, user enters movie ID)
    cur = get_db_cursor()
    
    try:
        cur.execute("""
            SELECT genre_id, genre_name
            FROM genres
            ORDER BY genre_name
        """)
        genres = [
            {"genre_id": r[0], "genre_name": r[1]}
            for r in cur.fetchall()
        ]
        
        # Pass empty movies list since we're using text input
        return render_template('movie_genres/create.html', movies=[], genres=genres)
        
    except Exception as e:
        flash(f'Error loading form: {str(e)}', 'danger')
        return redirect(url_for('list_movie_genres'))
    finally:
        cur.close()


@app.route('/movie-genres/<int:movie_id>/<int:genre_id>')
def view_movie_genre(movie_id, genre_id):
    """View a specific movie-genre association"""
    cur = get_db_cursor()
    
    try:
        association = admin_get_movie_genre(cur, movie_id, genre_id)
        if not association:
            flash('Movie-genre association not found', 'warning')
            return redirect(url_for('list_movie_genres'))
        
        movie = admin_get_movie(cur, movie_id)
        if not movie:
            flash('Movie not found', 'warning')
            return redirect(url_for('list_movie_genres'))
        
        genre = admin_get_genre(cur, genre_id)
        if not genre:
            flash('Genre not found', 'warning')
            return redirect(url_for('list_movie_genres'))
        
        cur.execute("""
            SELECT g.genre_id, g.genre_name
            FROM movie_genres mg
            JOIN genres g ON mg.genre_id = g.genre_id
            WHERE mg.movie_id = %s AND mg.genre_id != %s
            ORDER BY g.genre_name
        """, (movie_id, genre_id))
        
        other_genres = [
            {"genre_id": r[0], "genre_name": r[1]}
            for r in cur.fetchall()
        ]
        
        return render_template('movie_genres/view.html',
                             movie=movie,
                             genre=genre,
                             other_genres=other_genres)
    
    except Exception as e:
        flash(f'Error loading association: {str(e)}', 'danger')
        return redirect(url_for('list_movie_genres'))
    finally:
        cur.close()


@app.route('/movie-genres/<int:movie_id>/<int:genre_id>/delete', methods=['POST'])
def delete_movie_genre(movie_id, genre_id):
    """Remove a genre from a movie"""
    cur = get_db_cursor()
    
    try:
        association = admin_get_movie_genre(cur, movie_id, genre_id)
        if not association:
            flash('Movie-genre association not found', 'warning')
            return redirect(url_for('list_movie_genres'))
        
        movie = admin_get_movie(cur, movie_id)
        genre = admin_get_genre(cur, genre_id)
        
        result = admin_delete_movie_genre(cur, movie_id, genre_id)
        conn.commit()
        
        if result:
            movie_title = movie['title'] if movie else f'Movie {movie_id}'
            genre_name = genre['genre_name'] if genre else f'Genre {genre_id}'
            flash(f'Successfully removed genre "{genre_name}" from movie "{movie_title}"', 'success')
        else:
            flash('Association not found (may have been already deleted)', 'warning')
        
        return redirect(url_for('list_movie_genres'))
    
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting association: {str(e)}', 'danger')
        return redirect(url_for('list_movie_genres'))
    finally:
        cur.close()


@app.route('/movies/<int:movie_id>/genres')
def movie_genres_for_movie(movie_id):
    """Quick route to view all genres for a specific movie"""
    return redirect(url_for('list_movie_genres', movie_id=movie_id))

# ==================== MOVIE-COMPANY ROUTES ====================

@app.route('/movie-companies')
def list_movie_companies():
    """List all movie-company associations with optional filtering"""
    movie_id_filter = request.args.get('movie_id', type=int)
    company_id_filter = request.args.get('company_id', type=int)

    cur = get_db_cursor()

    try:
        query = """
            SELECT mpc.movie_id, mpc.company_id, m.title as movie_title, pc.name as company_name
            FROM movie_production_companies mpc
            LEFT JOIN movies m ON mpc.movie_id = m.movie_id
            LEFT JOIN production_companies pc ON mpc.company_id = pc.company_id
        """

        conditions = []
        params = []

        if movie_id_filter:
            conditions.append("mpc.movie_id = %s")
            params.append(movie_id_filter)

        if company_id_filter:
            conditions.append("mpc.company_id = %s")
            params.append(company_id_filter)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY mpc.movie_id, mpc.company_id LIMIT 100"

        cur.execute(query, params)
        rows = cur.fetchall()

        movie_companies = [
            {
                "movie_id": r[0],
                "company_id": r[1],
                "movie_title": r[2],
                "company_name": r[3]
            }
            for r in rows
        ]

        movie_info = admin_get_movie(cur, movie_id_filter) if movie_id_filter else None
        company_info = admin_get_company(cur, company_id_filter) if company_id_filter else None

        unique_movies = set(mc['movie_id'] for mc in movie_companies)
        unique_companies = set(mc['company_id'] for mc in movie_companies)

        return render_template('movie_companies/list.html',
                               movie_companies=movie_companies,
                               movie_info=movie_info,
                               company_info=company_info,
                               unique_movies=unique_movies,
                               unique_companies=unique_companies)

    except Exception as e:
        flash(f'Error loading movie-company associations: {str(e)}', 'danger')
        return render_template('movie_companies/list.html',
                               movie_companies=[],
                               movie_info=None,
                               company_info=None,
                               unique_movies=set(),
                               unique_companies=set())
    finally:
        cur.close()


@app.route('/movie-companies/create', methods=['GET', 'POST'])
def create_movie_company():
    """Add a production company to a movie"""

    if request.method == 'POST':
        cur = get_db_cursor()
        try:
            movie_id_raw = request.form.get('movie_id', '').strip()
            if not movie_id_raw:
                flash('Please select a movie from the search results before submitting.', 'danger')
                cur.close()
                return redirect(url_for('create_movie_company'))

            movie_id = int(movie_id_raw)
            company_id = int(request.form['company_id'])

            movie = admin_get_movie(cur, movie_id)
            if not movie:
                flash(f'Movie with ID {movie_id} not found in database. Please check and try again.', 'danger')
                cur.close()
                return redirect(url_for('create_movie_company'))

            company = admin_get_company(cur, company_id)
            if not company:
                flash(f'Company with ID {company_id} not found. Please select a valid company.', 'danger')
                cur.close()
                return redirect(url_for('create_movie_company'))

            existing = admin_get_movie_company(cur, movie_id, company_id)
            if existing:
                flash(f'Movie "{movie["title"]}" already lists company "{company["name"]}".', 'warning')
                cur.close()
                return redirect(url_for('list_movie_companies'))

            result = admin_create_movie_company(cur, movie_id, company_id)
            conn.commit()

            if result:
                flash(f'Successfully associated "{company["name"]}" with movie "{movie["title"]}" (ID: {movie_id}).', 'success')
            else:
                flash('Association already exists (no changes made).', 'info')

            cur.close()
            return redirect(url_for('list_movie_companies'))

        except ValueError:
            conn.rollback()
            flash('Invalid input: Movie ID and Company ID must be valid numbers.', 'danger')
            if 'cur' in locals():
                cur.close()
            return redirect(url_for('create_movie_company'))
        except Exception as e:
            conn.rollback()
            flash(f'Error creating association: {str(e)}', 'danger')
            if 'cur' in locals():
                cur.close()
            return redirect(url_for('create_movie_company'))

    cur = get_db_cursor()
    try:
        cur.execute("""
            SELECT company_id, name
            FROM production_companies
            ORDER BY name
        """)
        companies = [
            {"company_id": r[0], "name": r[1]}
            for r in cur.fetchall()
        ]

        return render_template('movie_companies/create.html', companies=companies)
    except Exception as e:
        flash(f'Error loading form: {str(e)}', 'danger')
        return redirect(url_for('list_movie_companies'))
    finally:
        cur.close()


@app.route('/movie-companies/<int:movie_id>/<int:company_id>')
def view_movie_company(movie_id, company_id):
    """View a specific movie-company association"""
    cur = get_db_cursor()

    try:
        association = admin_get_movie_company(cur, movie_id, company_id)
        if not association:
            flash('Movie-company association not found', 'warning')
            return redirect(url_for('list_movie_companies'))

        movie = admin_get_movie(cur, movie_id)
        if not movie:
            flash('Movie not found', 'warning')
            return redirect(url_for('list_movie_companies'))

        company = admin_get_company(cur, company_id)
        if not company:
            flash('Company not found', 'warning')
            return redirect(url_for('list_movie_companies'))

        cur.execute("""
            SELECT pc.company_id, pc.name
            FROM movie_production_companies mpc
            JOIN production_companies pc ON mpc.company_id = pc.company_id
            WHERE mpc.movie_id = %s AND mpc.company_id != %s
            ORDER BY pc.name
        """, (movie_id, company_id))

        other_companies = [
            {"company_id": r[0], "name": r[1]}
            for r in cur.fetchall()
        ]

        return render_template('movie_companies/view.html',
                               movie=movie,
                               company=company,
                               other_companies=other_companies)
    except Exception as e:
        flash(f'Error loading association: {str(e)}', 'danger')
        return redirect(url_for('list_movie_companies'))
    finally:
        cur.close()


@app.route('/movie-companies/<int:movie_id>/<int:company_id>/delete', methods=['POST'])
def delete_movie_company(movie_id, company_id):
    """Remove a production company from a movie"""
    cur = get_db_cursor()

    try:
        association = admin_get_movie_company(cur, movie_id, company_id)
        if not association:
            flash('Movie-company association not found', 'warning')
            return redirect(url_for('list_movie_companies'))

        movie = admin_get_movie(cur, movie_id)
        company = admin_get_company(cur, company_id)

        result = admin_delete_movie_company(cur, movie_id, company_id)
        conn.commit()

        if result:
            movie_title = movie['title'] if movie else f'Movie {movie_id}'
            company_name = company['name'] if company else f'Company {company_id}'
            flash(f'Successfully removed "{company_name}" from movie "{movie_title}".', 'success')
        else:
            flash('Association not found (may have been already deleted).', 'warning')

        return redirect(url_for('list_movie_companies'))
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting association: {str(e)}', 'danger')
        return redirect(url_for('list_movie_companies'))
    finally:
        cur.close()

# ==================== PEOPLE ROUTES ====================

@app.route('/people')
def list_people():
    """List all people with optional name search"""
    search_query = request.args.get('search', '').strip()
    cur = get_db_cursor()

    try:
        if search_query:
            people = admin_search_people_by_name(cur, search_query, limit=100)
        else:
            people = admin_read_people(cur, limit=100)

        return render_template('people/list.html',
                               people=people,
                               search_query=search_query)
    except Exception as e:
        flash(f'Error loading people: {str(e)}', 'danger')
        return render_template('people/list.html', people=[], search_query=search_query)
    finally:
        cur.close()


@app.route('/people/create', methods=['GET', 'POST'])
def create_person():
    """Create a new person record"""
    if request.method == 'POST':
        cur = get_db_cursor()
        try:
            name = request.form['name'].strip()
            if not name:
                raise ValueError('Name is required.')

            tmdb_id_raw = request.form.get('tmdb_id', '').strip()
            tmdb_id = int(tmdb_id_raw) if tmdb_id_raw else None

            gender_raw = request.form.get('gender')
            gender = int(gender_raw) if gender_raw not in (None, '') else None

            profile_path = request.form.get('profile_path', '').strip() or None

            new_person_id = admin_create_person(cur, tmdb_id, name, gender, profile_path)
            conn.commit()

            flash(f'Person "{name}" created successfully!', 'success')
            return redirect(url_for('view_person', person_id=new_person_id))
        except ValueError as e:
            conn.rollback()
            flash(str(e), 'danger')
        except Exception as e:
            conn.rollback()
            flash(f'Error creating person: {str(e)}', 'danger')
        finally:
            cur.close()

    return render_template('people/create.html', gender_options=GENDER_OPTIONS)


@app.route('/people/<int:person_id>')
def view_person(person_id):
    """View detailed information about a person"""
    cur = get_db_cursor()

    try:
        person = admin_get_person(cur, person_id)
        if not person:
            flash('Person not found', 'warning')
            return redirect(url_for('list_people'))

        cur.execute("SELECT COUNT(*) FROM movie_cast WHERE person_id = %s", (person_id,))
        cast_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM movie_crew WHERE person_id = %s", (person_id,))
        crew_count = cur.fetchone()[0]

        cur.execute("""
            SELECT m.movie_id, m.title, mc.character, mc.cast_order, m.released_date
            FROM movie_cast mc
            JOIN movies m ON mc.movie_id = m.movie_id
            WHERE mc.person_id = %s
            ORDER BY mc.cast_order NULLS LAST, m.released_date DESC NULLS LAST
            LIMIT 25
        """, (person_id,))
        cast_roles = [
            {
                "movie_id": r[0],
                "title": r[1],
                "character": r[2],
                "cast_order": r[3],
                "released_date": r[4]
            }
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT m.movie_id, m.title, mc.department, mc.job, m.released_date
            FROM movie_crew mc
            JOIN movies m ON mc.movie_id = m.movie_id
            WHERE mc.person_id = %s
            ORDER BY m.released_date DESC NULLS LAST, mc.department, mc.job
            LIMIT 25
        """, (person_id,))
        crew_roles = [
            {
                "movie_id": r[0],
                "title": r[1],
                "department": r[2],
                "job": r[3],
                "released_date": r[4]
            }
            for r in cur.fetchall()
        ]

        return render_template('people/view.html',
                               person=person,
                               cast_count=cast_count,
                               crew_count=crew_count,
                               cast_roles=cast_roles,
                               crew_roles=crew_roles)
    except Exception as e:
        flash(f'Error loading person: {str(e)}', 'danger')
        return redirect(url_for('list_people'))
    finally:
        cur.close()


@app.route('/people/<int:person_id>/edit', methods=['GET', 'POST'])
def edit_person(person_id):
    """Edit existing person details"""
    cur = get_db_cursor()

    try:
        person = admin_get_person(cur, person_id)
        if not person:
            flash('Person not found', 'warning')
            return redirect(url_for('list_people'))

        if request.method == 'POST':
            try:
                name = request.form['name'].strip()
                if not name:
                    raise ValueError('Name is required.')

                tmdb_id_raw = request.form.get('tmdb_id', '').strip()
                tmdb_id = int(tmdb_id_raw) if tmdb_id_raw else None

                gender_raw = request.form.get('gender')
                gender = int(gender_raw) if gender_raw not in (None, '') else None

                profile_path = request.form.get('profile_path', '').strip() or None

                update_data = {
                    "name": name,
                    "tmdb_id": tmdb_id,
                    "gender": gender,
                    "profile_path": profile_path
                }

                admin_update_person(cur, person_id, update_data)
                conn.commit()

                flash(f'Person "{name}" updated successfully!', 'success')
                return redirect(url_for('view_person', person_id=person_id))
            except ValueError as e:
                conn.rollback()
                flash(str(e), 'danger')
            except Exception as e:
                conn.rollback()
                flash(f'Error updating person: {str(e)}', 'danger')

        return render_template('people/edit.html',
                               person=person,
                               gender_options=GENDER_OPTIONS)
    finally:
        cur.close()


@app.route('/people/<int:person_id>/delete', methods=['POST'])
def delete_person(person_id):
    """Delete a person if they have no cast/crew associations"""
    cur = get_db_cursor()

    try:
        person = admin_get_person(cur, person_id)
        if not person:
            flash('Person not found', 'warning')
            return redirect(url_for('list_people'))

        cur.execute("SELECT COUNT(*) FROM movie_cast WHERE person_id = %s", (person_id,))
        cast_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM movie_crew WHERE person_id = %s", (person_id,))
        crew_count = cur.fetchone()[0]

        if cast_count > 0 or crew_count > 0:
            flash('Cannot delete person with existing cast or crew associations.', 'warning')
            return redirect(url_for('view_person', person_id=person_id))

        admin_delete_person(cur, person_id)
        conn.commit()
        flash(f'Person "{person["name"]}" deleted successfully.', 'success')
        return redirect(url_for('list_people'))
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting person: {str(e)}', 'danger')
        return redirect(url_for('view_person', person_id=person_id))
    finally:
        cur.close()


# ==================== MOVIE CAST ROUTES ====================

@app.route('/movie-cast')
def list_movie_casts():
    """List all movie cast entries with optional filtering"""
    movie_id = request.args.get('movie_id', type=int)
    person_id = request.args.get('person_id', type=int)
    search_query = request.args.get('search', '').strip()

    cur = get_db_cursor()

    try:
        if movie_id or person_id:
            # Filter by movie or person
            casts = admin_read_movie_casts(cur, movie_id=movie_id)
            # Get detailed info with joins
            cast_list = []
            for cast in casts:
                if person_id and cast[1] != person_id:
                    continue
                cur.execute("""
                    SELECT m.title, p.name
                    FROM movies m, people p
                    WHERE m.movie_id = %s AND p.person_id = %s
                """, (cast[0], cast[1]))
                movie_person = cur.fetchone()
                cast_list.append({
                    "movie_id": cast[0],
                    "person_id": cast[1],
                    "movie_title": movie_person[0] if movie_person else "Unknown",
                    "person_name": movie_person[1] if movie_person else "Unknown",
                    "character": cast[2],
                    "cast_order": cast[3],
                    "credit_id": cast[4]
                })
        elif search_query:
            # Search by movie title or person name
            cur.execute("""
                SELECT mc.movie_id, mc.person_id, m.title, p.name,
                       mc.character, mc.cast_order, mc.credit_id
                FROM movie_cast mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE m.title ILIKE %s OR p.name ILIKE %s
                ORDER BY m.title, mc.cast_order NULLS LAST
                LIMIT 100
            """, (f"%{search_query}%", f"%{search_query}%"))

            cast_list = [
                {
                    "movie_id": r[0],
                    "person_id": r[1],
                    "movie_title": r[2],
                    "person_name": r[3],
                    "character": r[4],
                    "cast_order": r[5],
                    "credit_id": r[6]
                }
                for r in cur.fetchall()
            ]
        else:
            # List all with pagination
            cur.execute("""
                SELECT mc.movie_id, mc.person_id, m.title, p.name,
                       mc.character, mc.cast_order, mc.credit_id
                FROM movie_cast mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                ORDER BY m.title, mc.cast_order NULLS LAST
                LIMIT 100
            """)

            cast_list = [
                {
                    "movie_id": r[0],
                    "person_id": r[1],
                    "movie_title": r[2],
                    "person_name": r[3],
                    "character": r[4],
                    "cast_order": r[5],
                    "credit_id": r[6]
                }
                for r in cur.fetchall()
            ]

        return render_template('movie_cast/list.html',
                               casts=cast_list,
                               search_query=search_query)
    except Exception as e:
        flash(f'Error loading cast entries: {str(e)}', 'danger')
        return render_template('movie_cast/list.html', casts=[], search_query=search_query)
    finally:
        cur.close()


@app.route('/movie-cast/<int:movie_id>/<int:person_id>')
def view_movie_cast(movie_id, person_id):
    """View detailed information about a specific cast entry"""
    cur = get_db_cursor()

    try:
        cast_entry = admin_get_movie_cast(cur, movie_id, person_id)
        if not cast_entry:
            flash('Cast entry not found', 'warning')
            return redirect(url_for('list_movie_casts'))

        # Get movie and person details
        movie = admin_get_movie(cur, movie_id)
        person = admin_get_person(cur, person_id)

        if not movie or not person:
            flash('Associated movie or person not found', 'warning')
            return redirect(url_for('list_movie_casts'))

        cast_data = {
            "movie_id": cast_entry[0],
            "person_id": cast_entry[1],
            "character": cast_entry[2],
            "cast_order": cast_entry[3],
            "credit_id": cast_entry[4]
        }

        return render_template('movie_cast/view.html',
                               cast=cast_data,
                               movie=movie,
                               person=person)
    except Exception as e:
        flash(f'Error loading cast entry: {str(e)}', 'danger')
        return redirect(url_for('list_movie_casts'))
    finally:
        cur.close()


@app.route('/movie-cast/create', methods=['GET', 'POST'])
def create_movie_cast():
    """Create a new movie cast entry"""
    if request.method == 'POST':
        cur = get_db_cursor()
        try:
            movie_id = int(request.form['movie_id'])
            person_id = int(request.form['person_id'])
            character = request.form.get('character', '').strip() or None

            cast_order_raw = request.form.get('cast_order', '').strip()
            cast_order = int(cast_order_raw) if cast_order_raw else None

            credit_id = request.form.get('credit_id', '').strip() or None

            result = admin_create_movie_cast(cur, movie_id, person_id, character, cast_order, credit_id)

            if result:
                conn.commit()
                flash('Cast entry created successfully!', 'success')
                return redirect(url_for('view_movie_cast', movie_id=movie_id, person_id=person_id))
            else:
                conn.rollback()
                flash('Cast entry already exists for this movie and person.', 'warning')
        except ValueError as e:
            conn.rollback()
            flash(f'Invalid input: {str(e)}', 'danger')
        except Exception as e:
            conn.rollback()
            flash(f'Error creating cast entry: {str(e)}', 'danger')
        finally:
            cur.close()

    # For GET request, just render the form (no data needed with search API)
    return render_template('movie_cast/create.html')


@app.route('/movie-cast/<int:movie_id>/<int:person_id>/edit', methods=['GET', 'POST'])
def edit_movie_cast(movie_id, person_id):
    """Edit existing movie cast entry"""
    cur = get_db_cursor()

    try:
        cast_entry = admin_get_movie_cast(cur, movie_id, person_id)
        if not cast_entry:
            flash('Cast entry not found', 'warning')
            return redirect(url_for('list_movie_casts'))

        # Get movie and person details
        movie = admin_get_movie(cur, movie_id)
        person = admin_get_person(cur, person_id)

        if request.method == 'POST':
            try:
                character = request.form.get('character', '').strip() or None

                cast_order_raw = request.form.get('cast_order', '').strip()
                cast_order = int(cast_order_raw) if cast_order_raw else None

                credit_id = request.form.get('credit_id', '').strip() or None

                update_data = {
                    "character": character,
                    "cast_order": cast_order,
                    "credit_id": credit_id
                }

                admin_update_movie_cast(cur, movie_id, person_id, update_data)
                conn.commit()

                flash('Cast entry updated successfully!', 'success')
                return redirect(url_for('view_movie_cast', movie_id=movie_id, person_id=person_id))
            except Exception as e:
                conn.rollback()
                flash(f'Error updating cast entry: {str(e)}', 'danger')

        cast_data = {
            "movie_id": cast_entry[0],
            "person_id": cast_entry[1],
            "character": cast_entry[2],
            "cast_order": cast_entry[3],
            "credit_id": cast_entry[4]
        }

        return render_template('movie_cast/edit.html',
                               cast=cast_data,
                               movie=movie,
                               person=person)
    finally:
        cur.close()


@app.route('/movie-cast/<int:movie_id>/<int:person_id>/delete', methods=['POST'])
def delete_movie_cast(movie_id, person_id):
    """Delete a movie cast entry"""
    cur = get_db_cursor()

    try:
        result = admin_delete_movie_cast(cur, movie_id, person_id)
        if result:
            conn.commit()
            flash('Cast entry deleted successfully.', 'success')
        else:
            flash('Cast entry not found.', 'warning')

        return redirect(url_for('list_movie_casts'))
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting cast entry: {str(e)}', 'danger')
        return redirect(url_for('list_movie_casts'))
    finally:
        cur.close()


# ==================== MOVIE CREW ROUTES ====================

@app.route('/movie-crew')
def list_movie_crews():
    """List all movie crew entries with optional filtering"""
    movie_id = request.args.get('movie_id', type=int)
    person_id = request.args.get('person_id', type=int)
    search_query = request.args.get('search', '').strip()

    cur = get_db_cursor()

    try:
        if movie_id or person_id:
            # Filter by movie or person
            crews = admin_read_movie_crews(cur, movie_id=movie_id)
            # Get detailed info with joins
            crew_list = []
            for crew in crews:
                if person_id and crew[1] != person_id:
                    continue
                cur.execute("""
                    SELECT m.title, p.name
                    FROM movies m, people p
                    WHERE m.movie_id = %s AND p.person_id = %s
                """, (crew[0], crew[1]))
                movie_person = cur.fetchone()
                crew_list.append({
                    "movie_id": crew[0],
                    "person_id": crew[1],
                    "movie_title": movie_person[0] if movie_person else "Unknown",
                    "person_name": movie_person[1] if movie_person else "Unknown",
                    "department": crew[2],
                    "job": crew[3],
                    "credit_id": crew[4]
                })
        elif search_query:
            # Search by movie title or person name
            cur.execute("""
                SELECT mc.movie_id, mc.person_id, m.title, p.name,
                       mc.department, mc.job, mc.credit_id
                FROM movie_crew mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE m.title ILIKE %s OR p.name ILIKE %s
                ORDER BY m.title, mc.department, mc.job
                LIMIT 100
            """, (f"%{search_query}%", f"%{search_query}%"))

            crew_list = [
                {
                    "movie_id": r[0],
                    "person_id": r[1],
                    "movie_title": r[2],
                    "person_name": r[3],
                    "department": r[4],
                    "job": r[5],
                    "credit_id": r[6]
                }
                for r in cur.fetchall()
            ]
        else:
            # List all with pagination
            cur.execute("""
                SELECT mc.movie_id, mc.person_id, m.title, p.name,
                       mc.department, mc.job, mc.credit_id
                FROM movie_crew mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                ORDER BY m.title, mc.department, mc.job
                LIMIT 100
            """)

            crew_list = [
                {
                    "movie_id": r[0],
                    "person_id": r[1],
                    "movie_title": r[2],
                    "person_name": r[3],
                    "department": r[4],
                    "job": r[5],
                    "credit_id": r[6]
                }
                for r in cur.fetchall()
            ]

        return render_template('movie_crew/list.html',
                               crews=crew_list,
                               search_query=search_query)
    except Exception as e:
        flash(f'Error loading crew entries: {str(e)}', 'danger')
        return render_template('movie_crew/list.html', crews=[], search_query=search_query)
    finally:
        cur.close()


@app.route('/movie-crew/<int:movie_id>/<int:person_id>/<job>')
def view_movie_crew(movie_id, person_id, job):
    """View detailed information about a specific crew entry"""
    cur = get_db_cursor()

    try:
        crew_entry = admin_get_movie_crew(cur, movie_id, person_id, job)
        if not crew_entry:
            flash('Crew entry not found', 'warning')
            return redirect(url_for('list_movie_crews'))

        # Get movie and person details
        movie = admin_get_movie(cur, movie_id)
        person = admin_get_person(cur, person_id)

        if not movie or not person:
            flash('Associated movie or person not found', 'warning')
            return redirect(url_for('list_movie_crews'))

        crew_data = {
            "movie_id": crew_entry[0],
            "person_id": crew_entry[1],
            "department": crew_entry[2],
            "job": crew_entry[3],
            "credit_id": crew_entry[4]
        }

        return render_template('movie_crew/view.html',
                               crew=crew_data,
                               movie=movie,
                               person=person)
    except Exception as e:
        flash(f'Error loading crew entry: {str(e)}', 'danger')
        return redirect(url_for('list_movie_crews'))
    finally:
        cur.close()

# ==================== MONGO MOVIE VIEW ====================
@app.route('/mongo/movies/<movie_id>')
def view_mongo_movie(movie_id):
    """View MongoDB movie details"""
    try:
        # If not in Mongo mode, redirect to Postgres view
        if not is_using_mongodb():
            return redirect(url_for('view_movie', movie_id=movie_id))

        # Coerce movie_id to int when possible
        try:
            movie_id_int = int(movie_id)
        except Exception:
            movie_id_int = movie_id

        # Fetch movie
        movie = mongo_admin_get_movie(movie_id_int)
        if not movie:
            flash('Movie not found', 'warning')
            return redirect(url_for('list_movies'))

        # Defensive normalization in case the helpers return unexpected shapes
        def _safe_list(val):
            return val if isinstance(val, list) else []

        genres = _safe_list(mongo_admin_read_genres(movie_id_int))
        companies = _safe_list(mongo_admin_read_companies(movie_id_int))
        cast = _safe_list(mongo_admin_read_movie_cast(movie_id_int))
        crew = _safe_list(mongo_admin_read_movie_crew(movie_id_int))

        return render_template('mongo_movies/view.html',
                               movie=movie,
                               genres=genres,
                               companies=companies,
                               cast=cast,
                               crew=crew)
    except Exception as e:
        flash(f'Error loading movie: {str(e)}', 'danger')
        return redirect(url_for('list_movies'))


@app.route('/movie-crew/create', methods=['GET', 'POST'])
def create_movie_crew():
    """Create a new movie crew entry"""
    if request.method == 'POST':
        cur = get_db_cursor()
        try:
            movie_id = int(request.form['movie_id'])
            person_id = int(request.form['person_id'])
            department = request.form.get('department', '').strip()
            job = request.form.get('job', '').strip()

            if not department:
                raise ValueError('Department is required.')
            if not job:
                raise ValueError('Job is required.')

            credit_id = request.form.get('credit_id', '').strip() or None

            result = admin_create_movie_crew(cur, movie_id, person_id, department, job, credit_id)

            if result:
                conn.commit()
                flash('Crew entry created successfully!', 'success')
                # URL encode the job parameter
                from urllib.parse import quote
                return redirect(url_for('view_movie_crew', movie_id=movie_id, person_id=person_id, job=quote(job)))
            else:
                conn.rollback()
                flash('Crew entry already exists for this movie, person, and job combination.', 'warning')
        except ValueError as e:
            conn.rollback()
            flash(f'Invalid input: {str(e)}', 'danger')
        except Exception as e:
            conn.rollback()
            flash(f'Error creating crew entry: {str(e)}', 'danger')
        finally:
            cur.close()

    # For GET request, just render the form (no data needed with search API)
    return render_template('movie_crew/create.html')


@app.route('/movie-crew/<int:movie_id>/<int:person_id>/<job>/edit', methods=['GET', 'POST'])
def edit_movie_crew(movie_id, person_id, job):
    """Edit existing movie crew entry"""
    cur = get_db_cursor()

    try:
        crew_entry = admin_get_movie_crew(cur, movie_id, person_id, job)
        if not crew_entry:
            flash('Crew entry not found', 'warning')
            return redirect(url_for('list_movie_crews'))

        # Get movie and person details
        movie = admin_get_movie(cur, movie_id)
        person = admin_get_person(cur, person_id)

        if request.method == 'POST':
            try:
                department = request.form.get('department', '').strip()
                new_job = request.form.get('job', '').strip()

                if not department:
                    raise ValueError('Department is required.')
                if not new_job:
                    raise ValueError('Job is required.')

                credit_id = request.form.get('credit_id', '').strip() or None

                update_data = {
                    "department": department,
                    "job": new_job,
                    "credit_id": credit_id
                }

                admin_update_movie_crew(cur, movie_id, person_id, job, update_data)
                conn.commit()

                flash('Crew entry updated successfully!', 'success')
                # URL encode the new job parameter
                from urllib.parse import quote
                return redirect(url_for('view_movie_crew', movie_id=movie_id, person_id=person_id, job=quote(new_job)))
            except Exception as e:
                conn.rollback()
                flash(f'Error updating crew entry: {str(e)}', 'danger')

        crew_data = {
            "movie_id": crew_entry[0],
            "person_id": crew_entry[1],
            "department": crew_entry[2],
            "job": crew_entry[3],
            "credit_id": crew_entry[4]
        }

        return render_template('movie_crew/edit.html',
                               crew=crew_data,
                               movie=movie,
                               person=person)
    finally:
        cur.close()


@app.route('/movie-crew/<int:movie_id>/<int:person_id>/<job>/delete', methods=['POST'])
def delete_movie_crew(movie_id, person_id, job):
    """Delete a movie crew entry"""
    cur = get_db_cursor()

    try:
        result = admin_delete_movie_crew(cur, movie_id, person_id, job)
        if result:
            conn.commit()
            flash('Crew entry deleted successfully.', 'success')
        else:
            flash('Crew entry not found.', 'warning')

        return redirect(url_for('list_movie_crews'))
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting crew entry: {str(e)}', 'danger')
        return redirect(url_for('list_movie_crews'))
    finally:
        cur.close()


# ==================== COMPANY ROUTES ====================

@app.route('/companies')
def list_companies():
    """List all companies with search functionality"""
    search_query = request.args.get('search', '')

    # Check if using MongoDB
    if is_using_mongodb():
        try:
            if search_query:
                companies = mongo_admin_search_companies_by_name(search_query, limit=200)
            else:
                companies = mongo_admin_get_all_companies()

            return render_template('mongo_companies/list.html', companies=companies, search_query=search_query)
        except Exception as e:
            print(f"[ERROR] Error loading companies from MongoDB: {str(e)}")
            import traceback
            traceback.print_exc()
            flash(f'Error loading companies from MongoDB: {str(e)}', 'danger')
            return render_template('mongo_companies/list.html', companies=[], search_query=search_query)
    else:
        # PostgreSQL mode
        cur = get_db_cursor()

        try:
            if search_query:
                companies = admin_search_companies_by_name(cur, search_query)
            else:
                companies = admin_read_companies(cur)

            return render_template('companies/list.html', companies=companies, search_query=search_query)
        except Exception as e:
            flash(f'Error loading companies: {str(e)}', 'danger')
            return render_template('companies/list.html', companies=[], search_query=search_query)
        finally:
            cur.close()

@app.route('/companies/<int:company_id>')
def view_company(company_id):
    """View detailed company information"""
    cur = get_db_cursor()
    
    try:
        company_details = admin_get_company_with_movies(cur, company_id)
        if not company_details:
            flash('Company not found', 'warning')
            return redirect(url_for('list_companies'))

        company = {
            "company_id": company_details["company_id"],
            "name": company_details["name"]
        }
        movie_count = company_details.get("movie_count", 0)

        cur.execute("""
            SELECT m.movie_id, m.title, m.released_date
            FROM movie_production_companies mpc
            JOIN movies m ON mpc.movie_id = m.movie_id
            WHERE mpc.company_id = %s
            ORDER BY m.released_date DESC NULLS LAST
            LIMIT 50
        """, (company_id,))

        movies = [
            {"movie_id": r[0], "title": r[1], "released_date": r[2]}
            for r in cur.fetchall()
        ]

        return render_template('companies/view.html',
                               company=company,
                               movies=movies,
                               movie_count=movie_count)
    except Exception as e:
        flash(f'Error loading company: {str(e)}', 'danger')
        return redirect(url_for('list_companies'))
    finally:
        cur.close()

@app.route('/companies/create', methods=['GET', 'POST'])
def create_company():
    """Create a new company"""
    if request.method == 'POST':
        # Check if using MongoDB
        if is_using_mongodb():
            try:
                movie_id = int(request.form['movie_id'])
                company_id = int(request.form['company_id'])
                company_name = request.form['company_name'].strip()

                if not company_name:
                    flash('Company name is required', 'danger')
                    return render_template('mongo_companies/create.html')

                company_data = {
                    "id": company_id,
                    "name": company_name
                }

                result = mongo_admin_create_company(movie_id, company_data)

                if result:
                    flash(f'Company "{company_name}" added to movie successfully in MongoDB!', 'success')
                    return redirect(url_for('view_mongo_company', movie_id=movie_id, company_id=company_id))
                else:
                    flash('Company already exists in this movie', 'warning')
                    return render_template('mongo_companies/create.html')

            except ValueError as e:
                flash(f'Invalid input: {str(e)}', 'danger')
                return render_template('mongo_companies/create.html')
            except Exception as e:
                flash(f'Error creating company: {str(e)}', 'danger')
                return render_template('mongo_companies/create.html')
        else:
            # PostgreSQL mode
            cur = get_db_cursor()

            try:
                company_name = request.form['company_name'].strip()

                if not company_name:
                    flash('Company name is required', 'danger')
                    return render_template('companies/create.html')

                new_company_id = admin_create_company(cur, company_name)
                conn.commit()

                flash(f'Company "{company_name}" created successfully!', 'success')
                return redirect(url_for('view_company', company_id=new_company_id))

            except psycopg2.IntegrityError:
                conn.rollback()
                flash(f'Company "{company_name}" already exists', 'danger')
            except Exception as e:
                conn.rollback()
                flash(f'Error creating company: {str(e)}', 'danger')
            finally:
                cur.close()

    # GET request - render appropriate template
    if is_using_mongodb():
        return render_template('mongo_companies/create.html')
    else:
        return render_template('companies/create.html')

@app.route('/companies/<int:company_id>/edit', methods=['GET', 'POST'])
def edit_company(company_id):
    """Edit an existing company"""
    cur = get_db_cursor()
    
    try:
        company = admin_get_company(cur, company_id)
        if not company:
            flash('Company not found', 'warning')
            return redirect(url_for('list_companies'))
        
        if request.method == 'POST':
            new_name = request.form['company_name'].strip()
            
            if not new_name:
                flash('Company name is required', 'danger')
                return render_template('companies/edit.html', company=company)
            
            admin_update_company(cur, company_id, new_name)
            conn.commit()
            
            flash(f'Company updated to "{new_name}" successfully!', 'success')
            return redirect(url_for('view_company', company_id=company_id))
        
        return render_template('companies/edit.html', company=company)
        
    except psycopg2.IntegrityError:
        conn.rollback()
        flash('A company with that name already exists', 'danger')
        return redirect(url_for('edit_company', company_id=company_id))
    except Exception as e:
        conn.rollback()
        flash(f'Error updating company: {str(e)}', 'danger')
        return redirect(url_for('edit_company', company_id=company_id))
    finally:
        cur.close()

@app.route('/companies/<int:company_id>/delete', methods=['POST'])
def delete_company(company_id):
    """Delete a company"""
    cur = get_db_cursor()
    
    try:
        company = admin_get_company(cur, company_id)
        if not company:
            flash('Company not found', 'warning')
            return redirect(url_for('list_companies'))
        
        admin_delete_company(cur, company_id)
        conn.commit()
        
        # admin_get_company returns {"company_id": ..., "name": ...}
        flash(f'Company "{company["name"]}" deleted successfully!', 'success')
        return redirect(url_for('list_companies'))
        
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting company: {str(e)}', 'danger')
        return redirect(url_for('view_company', company_id=company_id))
    finally:
        cur.close()

# ==================== RATING ROUTES ====================

@app.route('/ratings')
def list_ratings():
    """List all ratings with filtering and sorting"""
    movie_id = request.args.get('movie_id')
    user_id = request.args.get('user_id')
    rating_filter = request.args.get('rating_filter', 'all')
    sort_by = request.args.get('sort_by', 'created_desc')
    limit = request.args.get('limit', 100, type=int)

    cur = get_db_cursor()

    try:
        where_conditions = []
        params = []

        if movie_id:
            try:
                movie_id_int = int(movie_id)
                where_conditions.append("movie_id = %s")
                params.append(movie_id_int)
            except ValueError:
                flash('Invalid movie ID', 'warning')

        if user_id:
            try:
                user_id_int = int(user_id)
                where_conditions.append("user_id = %s")
                params.append(user_id_int)
            except ValueError:
                flash('Invalid user ID', 'warning')

        # Rating range filter
        if rating_filter == 'excellent':
            where_conditions.append("rating >= 4.0 AND rating <= 5.0")
        elif rating_filter == 'high':
            where_conditions.append("rating >= 3.0 AND rating < 4.0")
        elif rating_filter == 'medium':
            where_conditions.append("rating >= 2.0 AND rating < 3.0")
        elif rating_filter == 'low':
            where_conditions.append("rating < 2.0")

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        order_by_clause = "ORDER BY created_at DESC"
        if sort_by == 'created_asc':
            order_by_clause = "ORDER BY created_at ASC"
        elif sort_by == 'rating_desc':
            order_by_clause = "ORDER BY rating DESC"
        elif sort_by == 'rating_asc':
            order_by_clause = "ORDER BY rating ASC"
        elif sort_by == 'movie_id':
            order_by_clause = "ORDER BY movie_id"
        elif sort_by == 'user_id':
            order_by_clause = "ORDER BY user_id"
        
        cur.execute("SELECT COUNT(*) FROM ratings")
        total_ratings = cur.fetchone()[0]
        
        count_query = f"SELECT COUNT(*) FROM ratings {where_clause}"
        if params:
            cur.execute(count_query, params)
        else:
            cur.execute(count_query)
        total_filtered = cur.fetchone()[0]
        
        query = f"""
            SELECT rating_id, user_id, movie_id, rating, created_at, updated_at
            FROM ratings
            {where_clause}
            {order_by_clause}
            LIMIT %s
        """
        params_with_limit = params + [limit]
        cur.execute(query, params_with_limit)
        
        rows = cur.fetchall()
        filtered_ratings = [{
            "rating_id": r[0],
            "user_id": r[1],
            "movie_id": r[2],
            "rating": float(r[3]) if r[3] is not None else None,
            "created_at": r[4],
            "updated_at": r[5]
        } for r in rows]
        
        return render_template('ratings/list.html',
                             ratings=filtered_ratings,
                             total_ratings=total_ratings,
                             total_filtered=total_filtered,
                             current_limit=limit)
    except Exception as e:
        if cur:
            cur.connection.rollback()  # Rollback the failed transaction
        flash(f'Error loading ratings: {str(e)}', 'danger')
        return render_template('ratings/list.html', ratings=[], total_ratings=0, total_filtered=0, current_limit=100)
    finally:
        if cur:
            cur.close()


@app.route('/ratings/<int:rating_id>')
def view_rating(rating_id):
    """View detailed rating information"""
    cur = get_db_cursor()
    
    try:
        rating = admin_get_rating(cur, rating_id)
        if not rating:
            flash('Rating not found', 'warning')
            return redirect(url_for('list_ratings'))
        
        return render_template('ratings/view.html', rating=rating)
    except Exception as e:
        flash(f'Error loading rating: {str(e)}', 'danger')
        return redirect(url_for('list_ratings'))
    finally:
        cur.close()


@app.route('/ratings/<int:rating_id>/edit', methods=['GET', 'POST'])
def edit_rating(rating_id):
    """Edit an existing rating - Admin moderation"""
    cur = get_db_cursor()
    
    try:
        rating = admin_get_rating(cur, rating_id)
        if not rating:
            flash('Rating not found', 'warning')
            return redirect(url_for('list_ratings'))
        
        if request.method == 'POST':
            new_rating = float(request.form['rating'])
            
            if new_rating < 0 or new_rating > 10:
                flash('Rating must be between 0 and 10', 'danger')
                return render_template('ratings/edit.html', rating=rating)
            
            admin_update_rating(cur, rating_id, new_rating)
            conn.commit()
            
            flash(f'Rating updated successfully to {new_rating}/5.0!', 'success')
            return redirect(url_for('view_rating', rating_id=rating_id))
        
        return render_template('ratings/edit.html', rating=rating)
        
    except ValueError as e:
        conn.rollback()
        flash(f'Invalid input: {str(e)}', 'danger')
        return redirect(url_for('edit_rating', rating_id=rating_id))
    except Exception as e:
        conn.rollback()
        flash(f'Error updating rating: {str(e)}', 'danger')
        return redirect(url_for('edit_rating', rating_id=rating_id))
    finally:
        cur.close()


@app.route('/ratings/<int:rating_id>/delete', methods=['POST'])
def delete_rating(rating_id):
    """Delete a rating - Admin moderation"""
    cur = get_db_cursor()

    try:
        rating = admin_get_rating(cur, rating_id)
        if not rating:
            flash('Rating not found', 'warning')
            return redirect(url_for('list_ratings'))

        admin_delete_rating(cur, rating_id)
        conn.commit()

        flash(f'Rating #{rating_id} deleted successfully!', 'success')
        return redirect(url_for('list_ratings'))

    except Exception as e:
        conn.rollback()
        flash(f'Error deleting rating: {str(e)}', 'danger')
        return redirect(url_for('view_rating', rating_id=rating_id))
    finally:
        cur.close()

# ==================== MONGODB RATING ROUTES ====================

@app.route('/mongo/ratings')
def mongo_list_ratings():
    """List all ratings (MongoDB) with filtering and search"""
    try:
        # Get filter parameters
        rating_filter = request.args.get('rating_filter', 'all')
        sort_by = request.args.get('sort_by', 'created_desc')
        movie_id = request.args.get('movie_id', type=int)
        user_id = request.args.get('user_id', type=int)
        limit = request.args.get('limit', 200, type=int)

        # Get all ratings from MongoDB for total count
        all_ratings = mongo_admin_read_ratings(limit=10000, offset=0)
        total_ratings = len(all_ratings)

        # Apply filters
        filtered_ratings = all_ratings

        # Filter by movie_id
        if movie_id:
            filtered_ratings = [r for r in filtered_ratings if r.get('movieId') == movie_id]

        # Filter by user_id
        if user_id:
            filtered_ratings = [r for r in filtered_ratings if r.get('userId') == user_id]

        # Filter by rating range (0-5 scale, matching PostgreSQL)
        if rating_filter == 'excellent':
            filtered_ratings = [r for r in filtered_ratings if r.get('rating', 0) >= 4.0 and r.get('rating', 0) <= 5.0]
        elif rating_filter == 'high':
            filtered_ratings = [r for r in filtered_ratings if r.get('rating', 0) >= 3.0 and r.get('rating', 0) < 4.0]
        elif rating_filter == 'medium':
            filtered_ratings = [r for r in filtered_ratings if r.get('rating', 0) >= 2.0 and r.get('rating', 0) < 3.0]
        elif rating_filter == 'low':
            filtered_ratings = [r for r in filtered_ratings if r.get('rating', 0) < 2.0]

        # Sort ratings
        if sort_by == 'created_desc':
            filtered_ratings.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
        elif sort_by == 'created_asc':
            filtered_ratings.sort(key=lambda x: x.get('timestamp', 0))
        elif sort_by == 'rating_desc':
            filtered_ratings.sort(key=lambda x: x.get('rating', 0), reverse=True)
        elif sort_by == 'rating_asc':
            filtered_ratings.sort(key=lambda x: x.get('rating', 0))
        elif sort_by == 'movie_id':
            filtered_ratings.sort(key=lambda x: x.get('movieId', 0))
        elif sort_by == 'user_id':
            filtered_ratings.sort(key=lambda x: x.get('userId', 0))

        # Count filtered results before limiting
        total_filtered = len(filtered_ratings)

        # Limit results
        ratings = filtered_ratings[:limit]

        return render_template(
            'mongo_ratings/list.html',
            ratings=ratings,
            total_ratings=total_ratings,
            total_filtered=total_filtered,
            current_limit=limit
        )

    except Exception as e:
        print(f"[ERROR] Error loading MongoDB ratings: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading ratings: {str(e)}', 'danger')
        return render_template('mongo_ratings/list.html', ratings=[], total_ratings=0, total_filtered=0, current_limit=200)


@app.route('/mongo/ratings/<int:user_id>/<int:movie_id>')
def mongo_view_rating(user_id, movie_id):
    """View a specific rating (MongoDB)"""
    try:
        rating = mongo_admin_get_rating(user_id, movie_id)

        if not rating:
            flash('Rating not found', 'warning')
            return redirect(url_for('mongo_list_ratings'))

        return render_template('mongo_ratings/view.html', rating=rating)

    except Exception as e:
        print(f"[ERROR] Error loading rating: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading rating: {str(e)}', 'danger')
        return redirect(url_for('mongo_list_ratings'))


@app.route('/mongo/ratings/<int:user_id>/<int:movie_id>/edit', methods=['GET', 'POST'])
def mongo_edit_rating(user_id, movie_id):
    """Edit a rating (MongoDB)"""
    try:
        rating = mongo_admin_get_rating(user_id, movie_id)
        if not rating:
            flash('Rating not found', 'warning')
            return redirect(url_for('mongo_list_ratings'))

        if request.method == 'POST':
            try:
                new_rating = float(request.form['rating'])

                # Validate rating range (0-5 scale, matching PostgreSQL)
                if new_rating < 0 or new_rating > 5:
                    flash('Rating must be between 0 and 5', 'danger')
                    return render_template('mongo_ratings/edit.html', rating=rating)

                # Update rating
                result = mongo_admin_update_rating(user_id, movie_id, new_rating)

                if result:
                    flash(f'Rating updated successfully to {new_rating}/5.0!', 'success')
                    return redirect(url_for('mongo_view_rating', user_id=user_id, movie_id=movie_id))
                else:
                    flash('Rating not found or not updated', 'warning')
                    return redirect(url_for('mongo_list_ratings'))

            except ValueError as e:
                flash(f'Invalid input: {str(e)}', 'danger')
                return redirect(url_for('mongo_edit_rating', user_id=user_id, movie_id=movie_id))
            except Exception as e:
                print(f"[ERROR] Error updating rating: {str(e)}")
                import traceback
                traceback.print_exc()
                flash(f'Error updating rating: {str(e)}', 'danger')
                return redirect(url_for('mongo_edit_rating', user_id=user_id, movie_id=movie_id))

        # GET request - show edit form
        return render_template('mongo_ratings/edit.html', rating=rating)

    except Exception as e:
        print(f"[ERROR] Error loading rating: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading rating: {str(e)}', 'danger')
        return redirect(url_for('mongo_list_ratings'))


@app.route('/mongo/ratings/<int:user_id>/<int:movie_id>/delete', methods=['POST'])
def mongo_delete_rating(user_id, movie_id):
    """Delete a rating (MongoDB)"""
    try:
        # Check if rating exists
        rating = mongo_admin_get_rating(user_id, movie_id)
        if not rating:
            flash('Rating not found', 'warning')
            return redirect(url_for('mongo_list_ratings'))

        # Delete rating
        result = mongo_admin_delete_rating(user_id, movie_id)

        if result:
            flash(f'Rating deleted successfully!', 'success')
        else:
            flash('Rating not found or not deleted', 'warning')

        return redirect(url_for('mongo_list_ratings'))

    except Exception as e:
        print(f"[ERROR] Error deleting rating: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error deleting rating: {str(e)}', 'danger')
        return redirect(url_for('mongo_list_ratings'))

# ==================== MONGODB CAST ROUTES ====================

@app.route('/mongo/cast')
def mongo_list_cast():
    """List all cast members (MongoDB) with filtering and search"""
    try:
        # Get search query
        search_query = request.args.get('search', '')

        # Import credits and movies collections
        from mongo_admin_query import credits_col, movies_col

        # If searching, load all credits; otherwise limit for performance
        if search_query:
            # Search entire database
            all_credits = list(credits_col.find({}, {"_id": 0, "id": 1, "cast": 1}))
        else:
            # Only load first 50 movies for initial page load (performance)
            all_credits = list(credits_col.find({}, {"_id": 0, "id": 1, "cast": 1}).limit(50))

        # Get movie IDs from credits
        movie_ids = [credit["id"] for credit in all_credits]

        # Get only the movies we need
        all_movies = {m["id"]: m.get("original_title", "Unknown")
                      for m in movies_col.find({"id": {"$in": movie_ids}}, {"_id": 0, "id": 1, "original_title": 1})}

        # Flatten cast members and add movie titles
        all_cast_members = []
        for credit in all_credits:
            if "cast" in credit and isinstance(credit["cast"], list):
                for cast_member in credit["cast"]:
                    if isinstance(cast_member, dict):
                        # Create a copy to avoid modifying original
                        cast_copy = cast_member.copy()
                        cast_copy["movie_id"] = credit["id"]
                        cast_copy["movie_title"] = all_movies.get(credit["id"], f"Movie #{credit['id']}")
                        cast_copy["person_name"] = cast_member.get("name", "Unknown")
                        all_cast_members.append(cast_copy)

                        # Early exit if we have enough results and no search (performance optimization)
                        if not search_query and len(all_cast_members) >= 100:
                            break

            # Early exit at movie level if not searching
            if not search_query and len(all_cast_members) >= 100:
                break

        # Apply search filter
        filtered_cast = all_cast_members
        if search_query:
            search_lower = search_query.lower()
            filtered_cast = [
                c for c in filtered_cast
                if search_lower in str(c.get('movie_title', '')).lower() or
                   search_lower in str(c.get('person_name', '')).lower() or
                   search_lower in str(c.get('character', '')).lower()
            ]

        # Limit results to 100
        casts = filtered_cast[:100]

        return render_template(
            'mongo_movie_cast/list.html',
            casts=casts,
            search_query=search_query
        )

    except Exception as e:
        print(f"[ERROR] Error loading MongoDB cast: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading cast members: {str(e)}', 'danger')
        return render_template('mongo_movie_cast/list.html', casts=[], search_query='')


@app.route('/mongo/cast/<int:movie_id>/<int:person_id>')
def mongo_view_cast(movie_id, person_id):
    """View a specific cast member (MongoDB)"""
    try:
        cast = mongo_admin_get_person_cast(movie_id, person_id)

        if not cast:
            flash('Cast member not found', 'warning')
            return redirect(url_for('mongo_list_cast'))

        # Get movie details
        movie_data = mongo_admin_get_movie(movie_id)
        if movie_data:
            movie = {
                'movie_id': movie_data.get('id'),
                'title': movie_data.get('original_title', f'Movie #{movie_id}'),
                'released_date': movie_data.get('release_date')
            }
        else:
            movie = {
                'movie_id': movie_id,
                'title': f'Movie #{movie_id}',
                'released_date': None
            }

        return render_template('mongo_movie_cast/view.html', cast=cast, movie=movie, movie_id=movie_id)

    except Exception as e:
        print(f"[ERROR] Error loading cast member: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading cast member: {str(e)}', 'danger')
        return redirect(url_for('mongo_list_cast'))


@app.route('/mongo/cast/create', methods=['GET', 'POST'])
def mongo_create_cast():
    """Create a new cast member (MongoDB)"""
    try:
        if request.method == 'POST':
            # Get form data
            movie_id = int(request.form['movie_id'])
            person_id = int(request.form['person_id'])
            name = request.form['name']

            # Build cast data
            cast_data = {
                'id': person_id,
                'name': name,
            }

            # Optional fields
            if request.form.get('character'):
                cast_data['character'] = request.form['character']
            if request.form.get('cast_id'):
                cast_data['cast_id'] = request.form['cast_id']
            if request.form.get('credit_id'):
                cast_data['credit_id'] = request.form['credit_id']
            if request.form.get('order'):
                cast_data['order'] = int(request.form['order'])
            if request.form.get('gender'):
                cast_data['gender'] = int(request.form['gender'])
            if request.form.get('profile_path'):
                cast_data['profile_path'] = request.form['profile_path']

            # Create cast member
            result = mongo_admin_create_person_cast(movie_id, cast_data)

            if result:
                flash(f'Cast member "{name}" added successfully!', 'success')
                return redirect(url_for('mongo_view_cast', movie_id=movie_id, person_id=person_id))
            else:
                flash('Cast member already exists or could not be added', 'warning')
                return redirect(url_for('mongo_create_cast'))

        # GET request - show create form
        return render_template('mongo_movie_cast/create.html')

    except ValueError as e:
        flash(f'Invalid input: {str(e)}', 'danger')
        return redirect(url_for('mongo_create_cast'))
    except Exception as e:
        print(f"[ERROR] Error creating cast member: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error creating cast member: {str(e)}', 'danger')
        return redirect(url_for('mongo_create_cast'))


@app.route('/mongo/cast/<int:movie_id>/<int:person_id>/edit', methods=['GET', 'POST'])
def mongo_edit_cast(movie_id, person_id):
    """Edit a cast member (MongoDB)"""
    try:
        cast = mongo_admin_get_person_cast(movie_id, person_id)
        if not cast:
            flash('Cast member not found', 'warning')
            return redirect(url_for('mongo_list_cast'))

        if request.method == 'POST':
            # Build update data
            update_data = {}

            if request.form.get('name'):
                update_data['name'] = request.form['name']
            if 'character' in request.form:
                update_data['character'] = request.form['character']
            if 'cast_id' in request.form:
                update_data['cast_id'] = request.form['cast_id']
            if 'credit_id' in request.form:
                update_data['credit_id'] = request.form['credit_id']
            if request.form.get('order'):
                update_data['order'] = int(request.form['order'])
            if request.form.get('gender'):
                update_data['gender'] = int(request.form['gender'])
            if 'profile_path' in request.form:
                update_data['profile_path'] = request.form['profile_path']

            # Update cast member
            result = mongo_admin_update_person_cast(movie_id, person_id, update_data)

            if result:
                flash('Cast member updated successfully!', 'success')
                return redirect(url_for('mongo_view_cast', movie_id=movie_id, person_id=person_id))
            else:
                flash('Cast member not found or not updated', 'warning')
                return redirect(url_for('mongo_list_cast'))

        # GET request - show edit form
        # Get movie details
        movie_data = mongo_admin_get_movie(movie_id)
        if movie_data:
            movie = {
                'movie_id': movie_data.get('id'),
                'title': movie_data.get('original_title', f'Movie #{movie_id}')
            }
        else:
            movie = {
                'movie_id': movie_id,
                'title': f'Movie #{movie_id}'
            }

        return render_template('mongo_movie_cast/edit.html', cast=cast, movie=movie, movie_id=movie_id)

    except ValueError as e:
        flash(f'Invalid input: {str(e)}', 'danger')
        return redirect(url_for('mongo_edit_cast', movie_id=movie_id, person_id=person_id))
    except Exception as e:
        print(f"[ERROR] Error updating cast member: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error updating cast member: {str(e)}', 'danger')
        return redirect(url_for('mongo_edit_cast', movie_id=movie_id, person_id=person_id))


@app.route('/mongo/cast/<int:movie_id>/<int:person_id>/delete', methods=['POST'])
def mongo_delete_cast(movie_id, person_id):
    """Delete a cast member (MongoDB)"""
    try:
        # Check if cast member exists
        cast = mongo_admin_get_person_cast(movie_id, person_id)
        if not cast:
            flash('Cast member not found', 'warning')
            return redirect(url_for('mongo_list_cast'))

        # Delete cast member
        result = mongo_admin_delete_person_cast(movie_id, person_id)

        if result:
            flash('Cast member deleted successfully!', 'success')
        else:
            flash('Cast member not found or not deleted', 'warning')

        return redirect(url_for('mongo_list_cast'))

    except Exception as e:
        print(f"[ERROR] Error deleting cast member: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error deleting cast member: {str(e)}', 'danger')
        return redirect(url_for('mongo_list_cast'))


# ==================== MONGODB CREW CRUD ====================

@app.route('/mongo/crew')
def mongo_list_crew():
    """List all crew members (MongoDB) with filtering and search"""
    try:
        # Get search query
        search_query = request.args.get('search', '')

        # Import credits and movies collections
        from mongo_admin_query import credits_col, movies_col

        # If searching, load all credits; otherwise limit for performance
        if search_query:
            # Search entire database
            all_credits = list(credits_col.find({}, {"_id": 0, "id": 1, "crew": 1}))
        else:
            # Only load first 50 movies for initial page load (performance)
            all_credits = list(credits_col.find({}, {"_id": 0, "id": 1, "crew": 1}).limit(50))

        # Get movie IDs from credits
        movie_ids = [credit["id"] for credit in all_credits]

        # Get only the movies we need
        all_movies = {m["id"]: m.get("original_title", "Unknown")
                      for m in movies_col.find({"id": {"$in": movie_ids}}, {"_id": 0, "id": 1, "original_title": 1})}

        # Flatten crew members and add movie titles
        all_crew_members = []
        for credit in all_credits:
            if "crew" in credit and isinstance(credit["crew"], list):
                for crew_member in credit["crew"]:
                    if isinstance(crew_member, dict):
                        # Create a copy to avoid modifying original
                        crew_copy = crew_member.copy()
                        crew_copy["movie_id"] = credit["id"]
                        crew_copy["movie_title"] = all_movies.get(credit["id"], f"Movie #{credit['id']}")
                        crew_copy["person_name"] = crew_member.get("name", "Unknown")
                        all_crew_members.append(crew_copy)

                        # Early exit if we have enough results and no search (performance optimization)
                        if not search_query and len(all_crew_members) >= 100:
                            break

            # Early exit at movie level if not searching
            if not search_query and len(all_crew_members) >= 100:
                break

        # Apply search filter
        filtered_crew = all_crew_members
        if search_query:
            search_lower = search_query.lower()
            filtered_crew = [
                c for c in filtered_crew
                if search_lower in str(c.get('movie_title', '')).lower() or
                   search_lower in str(c.get('person_name', '')).lower() or
                   search_lower in str(c.get('job', '')).lower() or
                   search_lower in str(c.get('department', '')).lower()
            ]

        # Limit results to 100
        crew = filtered_crew[:100]

        return render_template(
            'mongo_movie_crew/list.html',
            crew=crew,
            search_query=search_query
        )

    except Exception as e:
        print(f"[ERROR] Error loading MongoDB crew: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading crew members: {str(e)}', 'danger')
        return render_template('mongo_movie_crew/list.html', crew=[], search_query='')


@app.route('/mongo/crew/<int:movie_id>/<int:person_id>/<path:job>')
def mongo_view_crew(movie_id, person_id, job):
    """View a specific crew member (MongoDB)"""
    try:
        crew = mongo_admin_get_person_crew(movie_id, person_id, job)

        if not crew:
            flash('Crew member not found', 'warning')
            return redirect(url_for('mongo_list_crew'))

        # Get movie details
        movie_data = mongo_admin_get_movie(movie_id)
        if movie_data:
            movie = {
                'movie_id': movie_data.get('id'),
                'title': movie_data.get('original_title', f'Movie #{movie_id}'),
                'released_date': movie_data.get('release_date')
            }
        else:
            movie = {
                'movie_id': movie_id,
                'title': f'Movie #{movie_id}',
                'released_date': None
            }

        return render_template('mongo_movie_crew/view.html', crew=crew, movie=movie, movie_id=movie_id)

    except Exception as e:
        print(f"[ERROR] Error loading crew member: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading crew member: {str(e)}', 'danger')
        return redirect(url_for('mongo_list_crew'))


@app.route('/mongo/crew/create', methods=['GET', 'POST'])
def mongo_create_crew():
    """Create a new crew member (MongoDB)"""
    try:
        if request.method == 'POST':
            # Get form data
            movie_id = int(request.form['movie_id'])
            person_id = int(request.form['person_id'])
            name = request.form['name']
            job = request.form['job']

            # Build crew data
            crew_data = {
                'id': person_id,
                'name': name,
                'job': job
            }

            # Optional fields
            if request.form.get('department'):
                crew_data['department'] = request.form['department']
            if request.form.get('credit_id'):
                crew_data['credit_id'] = request.form['credit_id']
            if request.form.get('gender'):
                crew_data['gender'] = int(request.form['gender'])
            if request.form.get('profile_path'):
                crew_data['profile_path'] = request.form['profile_path']

            # Create crew member
            result = mongo_admin_create_person_crew(movie_id, crew_data)

            if result:
                flash(f'Crew member "{name}" added successfully!', 'success')
                return redirect(url_for('mongo_view_crew', movie_id=movie_id, person_id=person_id, job=job))
            else:
                flash('Crew member already exists or could not be added', 'warning')
                return redirect(url_for('mongo_create_crew'))

        # GET request - show create form
        return render_template('mongo_movie_crew/create.html')

    except ValueError as e:
        flash(f'Invalid input: {str(e)}', 'danger')
        return redirect(url_for('mongo_create_crew'))
    except Exception as e:
        print(f"[ERROR] Error creating crew member: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error creating crew member: {str(e)}', 'danger')
        return redirect(url_for('mongo_create_crew'))


@app.route('/mongo/crew/<int:movie_id>/<int:person_id>/<path:job>/edit', methods=['GET', 'POST'])
def mongo_edit_crew(movie_id, person_id, job):
    """Edit a crew member (MongoDB)"""
    try:
        crew = mongo_admin_get_person_crew(movie_id, person_id, job)
        if not crew:
            flash('Crew member not found', 'warning')
            return redirect(url_for('mongo_list_crew'))

        # Store original job for later use
        original_job = job

        if request.method == 'POST':
            # Build update data
            update_data = {}

            if request.form.get('name'):
                update_data['name'] = request.form['name']
            if 'job' in request.form:
                update_data['job'] = request.form['job']
            if 'department' in request.form:
                update_data['department'] = request.form['department']
            if 'credit_id' in request.form:
                update_data['credit_id'] = request.form['credit_id']
            if request.form.get('gender'):
                update_data['gender'] = int(request.form['gender'])
            if 'profile_path' in request.form:
                update_data['profile_path'] = request.form['profile_path']

            # Update crew member
            result = mongo_admin_update_person_crew(movie_id, person_id, original_job, update_data)

            if result:
                flash('Crew member updated successfully!', 'success')
                # If job changed, redirect to new URL
                if 'job' in update_data and update_data['job'] != original_job:
                    return redirect(url_for('mongo_view_crew', movie_id=movie_id, person_id=person_id, job=update_data['job']))
                else:
                    return redirect(url_for('mongo_view_crew', movie_id=movie_id, person_id=person_id, job=original_job))
            else:
                flash('Crew member not found or not updated', 'warning')
                return redirect(url_for('mongo_list_crew'))

        # GET request - show edit form
        # Get movie details
        movie_data = mongo_admin_get_movie(movie_id)
        if movie_data:
            movie = {
                'movie_id': movie_data.get('id'),
                'title': movie_data.get('original_title', f'Movie #{movie_id}')
            }
        else:
            movie = {
                'movie_id': movie_id,
                'title': f'Movie #{movie_id}'
            }

        return render_template('mongo_movie_crew/edit.html', crew=crew, movie=movie, movie_id=movie_id, original_job=original_job)

    except ValueError as e:
        flash(f'Invalid input: {str(e)}', 'danger')
        return redirect(url_for('mongo_edit_crew', movie_id=movie_id, person_id=person_id, job=job))
    except Exception as e:
        print(f"[ERROR] Error updating crew member: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error updating crew member: {str(e)}', 'danger')
        return redirect(url_for('mongo_edit_crew', movie_id=movie_id, person_id=person_id, job=job))


@app.route('/mongo/crew/<int:movie_id>/<int:person_id>/<path:job>/delete', methods=['POST'])
def mongo_delete_crew(movie_id, person_id, job):
    """Delete a crew member (MongoDB)"""
    try:
        # Check if crew member exists
        crew = mongo_admin_get_person_crew(movie_id, person_id, job)
        if not crew:
            flash('Crew member not found', 'warning')
            return redirect(url_for('mongo_list_crew'))

        # Delete crew member
        result = mongo_admin_delete_person_crew(movie_id, person_id, job)

        if result:
            flash('Crew member deleted successfully!', 'success')
        else:
            flash('Crew member not found or not deleted', 'warning')

        return redirect(url_for('mongo_list_crew'))

    except Exception as e:
        print(f"[ERROR] Error deleting crew member: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error deleting crew member: {str(e)}', 'danger')
        return redirect(url_for('mongo_list_crew'))


# ==================== ADVANCED SEARCH ROUTE ====================

@app.route('/combined-search')
def combined_search():
    """
    Advanced Search page - Filter by Title, Genre, Actor, Year Range with Sort by Popularity or Rating.
    Works with both PostgreSQL and MongoDB based on current session database.
    """
    # Get filter parameters from query string
    title = request.args.get('title', '').strip()
    genre = request.args.get('genre', '').strip()
    actor = request.args.get('actor', '').strip()
    year_start = request.args.get('year_start', '').strip()
    year_end = request.args.get('year_end', '').strip()
    sort_by = request.args.get('sort_by', 'popularity').strip()

    # Validate sort_by
    if sort_by not in ['popularity', 'rating']:
        sort_by = 'popularity'

    movies = []
    error = None
    # Perform search if form was submitted (any query params present)
    search_performed = len(request.args) > 0

    # Only execute search if form was submitted
    if search_performed:
        # Convert year values to integers if provided
        year_start_int = int(year_start) if year_start else None
        year_end_int = int(year_end) if year_end else None

        # Determine which database to use
        db_type = session.get('db_type', DEFAULT_DB_TYPE)

        try:
            if db_type == 'mongodb':
                movies = search_mongo(
                    title=title if title else None,
                    genre=genre if genre else None,
                    actor=actor if actor else None,
                    year_start=year_start_int,
                    year_end=year_end_int,
                    sort_by=sort_by,
                    limit=100
                )
            else:
                cur = get_db_cursor()
                try:
                    movies = search_postgres(
                        cur,
                        title=title if title else None,
                        genre=genre if genre else None,
                        actor=actor if actor else None,
                        year_start=year_start_int,
                        year_end=year_end_int,
                        sort_by=sort_by,
                        limit=100
                    )
                finally:
                    cur.close()
        except Exception as e:
            error = str(e)
            flash(f'Search error: {error}', 'danger')

    # Get genres list for dropdown (from current database)
    genres_list = []
    db_type = session.get('db_type', DEFAULT_DB_TYPE)
    try:
        if db_type == 'mongodb':
            genres_list = mongo_admin_read_genres()
        else:
            genres_list = admin_read_genres(conn.cursor())
    except Exception as e:
        pass  # Genres dropdown will be empty if this fails

    return render_template('combined_search.html',
                           movies=movies,
                           genres_list=genres_list,
                           title=title,
                           genre=genre,
                           actor=actor,
                           year_start=year_start,
                           year_end=year_end,
                           sort_by=sort_by,
                           search_performed=search_performed,
                           error=error)


# ==================== BENCHMARK ROUTES ====================

@app.route('/benchmark')
def benchmark_page():
    """Benchmark page - Compare PostgreSQL vs MongoDB performance"""
    return render_template('benchmark.html')


@app.route('/benchmark', methods=['POST'])
def benchmark_run():
    """
    Run benchmark tests comparing PostgreSQL vs MongoDB query performance.
    Returns JSON with latency metrics and ops/sec for both databases.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        test_type = data.get('test_type', 'title')
        term = data.get('term', 'Toy Story')
        genre_term = data.get('genre_term', 'Animation')
        actor_term = data.get('actor_term', 'Tom Hanks')
        year_from = int(data.get('year_from', 1990))
        year_to = int(data.get('year_to', 2000))
        movie_id = data.get('movie_id')
        runs = max(5, min(200, int(data.get('runs', 50))))
        limit = max(1, min(200, int(data.get('limit', 50))))
        include_explain = data.get('include_explain', False)

        warmup_runs = 3
        notes = []
        notes.append(f"Warm-up: {warmup_runs} runs excluded from measurements")
        notes.append(f"Ops/sec: Sequential single-thread throughput (not concurrent)")

        # Escape text inputs for regex safety
        escaped_term = re.escape(term) if term else ''
        escaped_genre = re.escape(genre_term) if genre_term else ''
        escaped_actor = re.escape(actor_term) if actor_term else ''

        # Get database connections outside the loop
        pg_cursor = conn.cursor()

        # Results storage
        pg_latencies = []
        mongo_latencies = []
        pg_result_count = 0
        mongo_result_count = 0
        pg_explain = None
        mongo_explain = None

        # ==================== TEST IMPLEMENTATIONS ====================

        if test_type == 'title':
            # PostgreSQL: Title search
            pg_query = """
                SELECT movie_id, title, released_date
                FROM movies
                WHERE title ILIKE %s
                LIMIT %s
            """
            pg_params = (f"%{escaped_term}%", limit)

            # Warm-up PostgreSQL
            for _ in range(warmup_runs):
                pg_cursor.execute(pg_query, pg_params)
                _ = pg_cursor.fetchall()

            # Measure PostgreSQL
            for _ in range(runs):
                start = time.perf_counter()
                pg_cursor.execute(pg_query, pg_params)
                results = pg_cursor.fetchall()
                end = time.perf_counter()
                pg_latencies.append((end - start) * 1000)
                pg_result_count = len(results)

            # MongoDB: Title search
            mongo_query = {"original_title": {"$regex": escaped_term, "$options": "i"}}
            mongo_projection = {"_id": 0, "id": 1, "original_title": 1, "release_date": 1}

            # Warm-up MongoDB
            for _ in range(warmup_runs):
                _ = list(mongo_movies_col.find(mongo_query, mongo_projection).limit(limit))

            # Measure MongoDB
            for _ in range(runs):
                start = time.perf_counter()
                results = list(mongo_movies_col.find(mongo_query, mongo_projection).limit(limit))
                end = time.perf_counter()
                mongo_latencies.append((end - start) * 1000)
                mongo_result_count = len(results)

            # Optional explain
            if include_explain:
                pg_cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {pg_query.replace('%s', '%s')}", pg_params)
                pg_explain = '\n'.join([row[0] for row in pg_cursor.fetchall()])
                mongo_explain_result = mongo_movies_col.find(mongo_query, mongo_projection).limit(limit).explain()
                mongo_explain = _format_mongo_explain(mongo_explain_result)

        elif test_type == 'genre':
            # PostgreSQL: Genre search with joins
            pg_query = """
                SELECT m.movie_id, m.title, m.released_date
                FROM movies m
                JOIN movie_genres mg ON m.movie_id = mg.movie_id
                JOIN genres g ON mg.genre_id = g.genre_id
                WHERE g.genre_name ILIKE %s
                LIMIT %s
            """
            pg_params = (f"%{escaped_genre}%", limit)

            # Warm-up PostgreSQL
            for _ in range(warmup_runs):
                pg_cursor.execute(pg_query, pg_params)
                _ = pg_cursor.fetchall()

            # Measure PostgreSQL
            for _ in range(runs):
                start = time.perf_counter()
                pg_cursor.execute(pg_query, pg_params)
                results = pg_cursor.fetchall()
                end = time.perf_counter()
                pg_latencies.append((end - start) * 1000)
                pg_result_count = len(results)

            # MongoDB: Genre search (embedded genres array)
            mongo_query = {"genres": {"$elemMatch": {"name": {"$regex": escaped_genre, "$options": "i"}}}}
            mongo_projection = {"_id": 0, "id": 1, "original_title": 1, "release_date": 1}

            # Warm-up MongoDB
            for _ in range(warmup_runs):
                _ = list(mongo_movies_col.find(mongo_query, mongo_projection).limit(limit))

            # Measure MongoDB
            for _ in range(runs):
                start = time.perf_counter()
                results = list(mongo_movies_col.find(mongo_query, mongo_projection).limit(limit))
                end = time.perf_counter()
                mongo_latencies.append((end - start) * 1000)
                mongo_result_count = len(results)

            # Optional explain
            if include_explain:
                pg_cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT m.movie_id, m.title, m.released_date FROM movies m JOIN movie_genres mg ON m.movie_id = mg.movie_id JOIN genres g ON mg.genre_id = g.genre_id WHERE g.genre_name ILIKE %s LIMIT %s", pg_params)
                pg_explain = '\n'.join([row[0] for row in pg_cursor.fetchall()])
                mongo_explain_result = mongo_movies_col.find(mongo_query, mongo_projection).limit(limit).explain()
                mongo_explain = _format_mongo_explain(mongo_explain_result)

        elif test_type == 'year_range':
            # PostgreSQL: Year range search
            pg_query = """
                SELECT movie_id, title, released_date
                FROM movies
                WHERE EXTRACT(YEAR FROM released_date) BETWEEN %s AND %s
                LIMIT %s
            """
            pg_params = (year_from, year_to, limit)

            # Warm-up PostgreSQL
            for _ in range(warmup_runs):
                pg_cursor.execute(pg_query, pg_params)
                _ = pg_cursor.fetchall()

            # Measure PostgreSQL
            for _ in range(runs):
                start = time.perf_counter()
                pg_cursor.execute(pg_query, pg_params)
                results = pg_cursor.fetchall()
                end = time.perf_counter()
                pg_latencies.append((end - start) * 1000)
                pg_result_count = len(results)

            # MongoDB: Year range search using ISO date strings
            date_from = f"{year_from}-01-01"
            date_to = f"{year_to}-12-31"
            mongo_query = {
                "release_date": {"$gte": date_from, "$lte": date_to, "$exists": True, "$ne": None, "$ne": ""}
            }
            mongo_projection = {"_id": 0, "id": 1, "original_title": 1, "release_date": 1}

            # Warm-up MongoDB
            for _ in range(warmup_runs):
                _ = list(mongo_movies_col.find(mongo_query, mongo_projection).limit(limit))

            # Measure MongoDB
            for _ in range(runs):
                start = time.perf_counter()
                results = list(mongo_movies_col.find(mongo_query, mongo_projection).limit(limit))
                end = time.perf_counter()
                mongo_latencies.append((end - start) * 1000)
                mongo_result_count = len(results)

            # Optional explain
            if include_explain:
                pg_cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT movie_id, title, released_date FROM movies WHERE EXTRACT(YEAR FROM released_date) BETWEEN %s AND %s LIMIT %s", pg_params)
                pg_explain = '\n'.join([row[0] for row in pg_cursor.fetchall()])
                mongo_explain_result = mongo_movies_col.find(mongo_query, mongo_projection).limit(limit).explain()
                mongo_explain = _format_mongo_explain(mongo_explain_result)

        elif test_type == 'actor':
            # PostgreSQL: Actor search with joins
            pg_query = """
                SELECT m.movie_id, m.title, m.released_date, mc.character
                FROM movies m
                JOIN movie_cast mc ON m.movie_id = mc.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE p.name ILIKE %s
                LIMIT %s
            """
            pg_params = (f"%{escaped_actor}%", limit)

            # Warm-up PostgreSQL
            for _ in range(warmup_runs):
                pg_cursor.execute(pg_query, pg_params)
                _ = pg_cursor.fetchall()

            # Measure PostgreSQL
            for _ in range(runs):
                start = time.perf_counter()
                pg_cursor.execute(pg_query, pg_params)
                results = pg_cursor.fetchall()
                end = time.perf_counter()
                pg_latencies.append((end - start) * 1000)
                pg_result_count = len(results)

            # MongoDB: Actor search in credits collection
            # Check if cast is array of objects or string
            sample_credit = mongo_credits_col.find_one({"cast": {"$exists": True}})
            cast_is_array = sample_credit and isinstance(sample_credit.get("cast"), list)

            if cast_is_array:
                mongo_query = {"cast.name": {"$regex": escaped_actor, "$options": "i"}}
                notes.append("MongoDB: cast field is array of objects (optimal)")
            else:
                mongo_query = {"cast": {"$regex": escaped_actor, "$options": "i"}}
                notes.append("MongoDB: cast field is string (fallback regex on entire field)")

            mongo_projection = {"_id": 0, "id": 1}

            # Warm-up MongoDB
            for _ in range(warmup_runs):
                _ = list(mongo_credits_col.find(mongo_query, mongo_projection).limit(limit))

            # Measure MongoDB
            for _ in range(runs):
                start = time.perf_counter()
                results = list(mongo_credits_col.find(mongo_query, mongo_projection).limit(limit))
                end = time.perf_counter()
                mongo_latencies.append((end - start) * 1000)
                mongo_result_count = len(results)

            # Optional explain
            if include_explain:
                pg_cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT m.movie_id, m.title, m.released_date, mc.character FROM movies m JOIN movie_cast mc ON m.movie_id = mc.movie_id JOIN people p ON mc.person_id = p.person_id WHERE p.name ILIKE %s LIMIT %s", pg_params)
                pg_explain = '\n'.join([row[0] for row in pg_cursor.fetchall()])
                mongo_explain_result = mongo_credits_col.find(mongo_query, mongo_projection).limit(limit).explain()
                mongo_explain = _format_mongo_explain(mongo_explain_result)

        elif test_type == 'combined':
            # Combined search using helper functions with FAIR benchmark
            # Both PostgreSQL and MongoDB now use the links bridge to properly join ratings
            # This ensures a fair comparison where both databases do equivalent work
            sort_by = data.get('sort_by', 'popularity')  # 'popularity' or 'rating'

            notes.append("Using links bridge for fair rating comparison (TMDB ID <-> MovieLens ID)")
            notes.append("Both databases calculate avg_rating from ratings table/collection")
            notes.append(f"Sort by: {sort_by}")

            # Warm-up PostgreSQL
            for _ in range(warmup_runs):
                _ = search_postgres(
                    pg_cursor,
                    genre=genre_term if genre_term else None,
                    actor=actor_term if actor_term else None,
                    year_start=year_from,
                    year_end=year_to,
                    sort_by=sort_by,
                    limit=limit
                )

            # Measure PostgreSQL
            for _ in range(runs):
                start = time.perf_counter()
                results = search_postgres(
                    pg_cursor,
                    genre=genre_term if genre_term else None,
                    actor=actor_term if actor_term else None,
                    year_start=year_from,
                    year_end=year_to,
                    sort_by=sort_by,
                    limit=limit
                )
                end = time.perf_counter()
                pg_latencies.append((end - start) * 1000)
                pg_result_count = len(results)

            # Warm-up MongoDB
            for _ in range(warmup_runs):
                _ = search_mongo(
                    genre=genre_term if genre_term else None,
                    actor=actor_term if actor_term else None,
                    year_start=year_from,
                    year_end=year_to,
                    sort_by=sort_by,
                    limit=limit
                )

            # Measure MongoDB
            for _ in range(runs):
                start = time.perf_counter()
                results = search_mongo(
                    genre=genre_term if genre_term else None,
                    actor=actor_term if actor_term else None,
                    year_start=year_from,
                    year_end=year_to,
                    sort_by=sort_by,
                    limit=limit
                )
                end = time.perf_counter()
                mongo_latencies.append((end - start) * 1000)
                mongo_result_count = len(results)

            # Optional explain (simplified - explain would need separate execution)
            if include_explain:
                pg_explain = "Combined search uses search_postgres() helper with links bridge.\nQuery joins: movies -> links -> ratings for fair benchmark."
                mongo_explain = "Combined search uses search_mongo() helper with $lookup aggregation.\nPipeline: $match -> $lookup links -> $lookup ratings -> $sort -> $limit"

        elif test_type == 'deep_fetch':
            # Resolve movie_id if not provided
            if not movie_id:
                pg_cursor.execute("SELECT movie_id FROM movies ORDER BY movie_id LIMIT 1")
                row = pg_cursor.fetchone()
                if row:
                    movie_id = row[0]
                else:
                    return jsonify({"error": "No movies found in PostgreSQL"}), 400

                mongo_first = mongo_movies_col.find({}, {"id": 1}).sort("id", 1).limit(1)
                mongo_first_list = list(mongo_first)
                if not mongo_first_list:
                    return jsonify({"error": "No movies found in MongoDB"}), 400
                notes.append(f"Using movie_id={movie_id} (first available)")

            movie_id = int(movie_id)

            # PostgreSQL: Deep fetch with aggregated arrays
            pg_query = """
                SELECT
                    m.movie_id, m.title, m.released_date, m.overview, m.runtime,
                    COALESCE(
                        (SELECT json_agg(json_build_object('id', g.genre_id, 'name', g.genre_name))
                         FROM genres g
                         JOIN movie_genres mg ON g.genre_id = mg.genre_id
                         WHERE mg.movie_id = m.movie_id),
                        '[]'::json
                    ) AS genres,
                    COALESCE(
                        (SELECT json_agg(json_build_object('id', pc.company_id, 'name', pc.name))
                         FROM production_companies pc
                         JOIN movie_production_companies mpc ON pc.company_id = mpc.company_id
                         WHERE mpc.movie_id = m.movie_id),
                        '[]'::json
                    ) AS production_companies
                FROM movies m
                WHERE m.movie_id = %s
            """
            pg_params = (movie_id,)

            # Warm-up PostgreSQL
            for _ in range(warmup_runs):
                pg_cursor.execute(pg_query, pg_params)
                _ = pg_cursor.fetchone()

            # Measure PostgreSQL
            for _ in range(runs):
                start = time.perf_counter()
                pg_cursor.execute(pg_query, pg_params)
                result = pg_cursor.fetchone()
                end = time.perf_counter()
                pg_latencies.append((end - start) * 1000)
                pg_result_count = 1 if result else 0

            # MongoDB: Deep fetch with projection
            mongo_projection = {
                "_id": 0,
                "id": 1,
                "original_title": 1,
                "release_date": 1,
                "overview": 1,
                "runtime": 1,
                "genres": 1,
                "production_companies": 1
            }

            # Warm-up MongoDB
            for _ in range(warmup_runs):
                result = mongo_movies_col.find_one({"id": movie_id}, mongo_projection)
                if result:
                    _ = result.get("id")  # Force field access

            # Measure MongoDB
            for _ in range(runs):
                start = time.perf_counter()
                result = mongo_movies_col.find_one({"id": movie_id}, mongo_projection)
                if result:
                    _ = result.get("id")  # Force field access
                end = time.perf_counter()
                mongo_latencies.append((end - start) * 1000)
                mongo_result_count = 1 if result else 0

            # Optional explain
            if include_explain:
                pg_cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {pg_query}", pg_params)
                pg_explain = '\n'.join([row[0] for row in pg_cursor.fetchall()])
                mongo_explain_result = mongo_movies_col.find({"id": movie_id}, mongo_projection).explain()
                mongo_explain = _format_mongo_explain(mongo_explain_result)

        else:
            pg_cursor.close()
            return jsonify({"error": f"Unknown test_type: {test_type}"}), 400

        pg_cursor.close()

        # ==================== CALCULATE METRICS ====================
        def calc_metrics(latencies, result_count):
            if not latencies:
                return None
            total_time = sum(latencies)
            return {
                "avg_latency_ms": round(statistics.mean(latencies), 3),
                "median_latency_ms": round(statistics.median(latencies), 3),
                "p95_latency_ms": round(sorted(latencies)[int(len(latencies) * 0.95)], 3) if len(latencies) >= 20 else round(max(latencies), 3),
                "min_latency_ms": round(min(latencies), 3),
                "max_latency_ms": round(max(latencies), 3),
                "total_time_ms": round(total_time, 3),
                "ops_per_sec": round(len(latencies) / (total_time / 1000), 2) if total_time > 0 else 0,
                "runs": len(latencies),
                "warmup": warmup_runs,
                "result_count": result_count
            }

        response = {
            "test_type": test_type,
            "postgres": calc_metrics(pg_latencies, pg_result_count),
            "mongodb": calc_metrics(mongo_latencies, mongo_result_count),
            "notes": notes
        }

        if include_explain:
            response["postgres_explain"] = pg_explain
            response["mongodb_explain"] = mongo_explain

        return jsonify(response)

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


def _format_mongo_explain(explain_result):
    """Format MongoDB explain output to key fields"""
    try:
        execution_stats = explain_result.get("executionStats", {})
        query_planner = explain_result.get("queryPlanner", {})

        formatted = []
        formatted.append(f"Execution Time: {execution_stats.get('executionTimeMillis', 'N/A')} ms")
        formatted.append(f"Total Docs Examined: {execution_stats.get('totalDocsExamined', 'N/A')}")
        formatted.append(f"Total Keys Examined: {execution_stats.get('totalKeysExamined', 'N/A')}")
        formatted.append(f"Docs Returned: {execution_stats.get('nReturned', 'N/A')}")

        winning_plan = query_planner.get("winningPlan", {})
        if winning_plan:
            formatted.append(f"Winning Plan Stage: {winning_plan.get('stage', 'N/A')}")
            if "inputStage" in winning_plan:
                formatted.append(f"  Input Stage: {winning_plan['inputStage'].get('stage', 'N/A')}")

        return '\n'.join(formatted)
    except Exception:
        return str(explain_result)[:2000]


# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def not_found(e):
    return render_template('errors/404.html'), 404

@app.errorhandler(500)
def server_error(e):
    return render_template('errors/500.html'), 500

# ==================== TEMPLATE FILTERS ====================

@app.template_filter('format_date')
def format_date(value):
    """Format date for display"""
    if value is None:
        return 'N/A'
    if isinstance(value, str):
        return value
    return value.strftime('%Y-%m-%d')

@app.template_filter('format_popularity')
def format_popularity(value):
    """Format popularity score"""
    if value is None:
        return 'N/A'
    return f'{value:.2f}'

@app.template_filter('format_datetime')
def format_datetime(value):
    """Format datetime for display"""
    if value is None:
        return 'N/A'
    if isinstance(value, str):
        return value
    return value.strftime('%Y-%m-%d %H:%M:%S')

# ==================== HELPER FUNCTIONS ====================

@app.context_processor
def utility_processor():
    def get_rating_color(rating):
        """Return color based on rating value"""
        if rating >= 9:
            return '#28a745'
        elif rating >= 7:
            return '#ffc107'
        elif rating >= 5:
            return '#fd7e14'
        else:
            return '#dc3545'
    
    return dict(get_rating_color=get_rating_color)

# ==================== RUN APP ====================

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
