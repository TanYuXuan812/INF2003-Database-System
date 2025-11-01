import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# =================================
# Database connection
# =================================
conn = psycopg2.connect(
    dbname=os.getenv("DATABASE"),
    user=os.getenv("DB_USER"),
    password=os.getenv("PASSWORD"),
    host=os.getenv("HOST"),
    port=os.getenv("PORT")
)
conn.autocommit = False  # we'll manage commits

# =================================
# Query functions for User Searches
# =================================

def search_movies_by_title(cur, title_text, limit=20, offset=0):
    query = """
    SELECT 
        m.movie_id,
        m.title,
        m.released_date,
        m.popularity,
        m.poster_path,
        AVG(r.rating) AS avg_rating,
        COUNT(r.rating) AS num_ratings,
        STRING_AGG(DISTINCT g.genre_name, ', ') AS genres
    FROM movies m
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
    LEFT JOIN genres g ON mg.genre_id = g.genre_id
    WHERE m.title ILIKE %s
    GROUP BY m.movie_id, m.title, m.released_date, m.popularity, m.poster_path
    ORDER BY avg_rating DESC NULLS LAST
    LIMIT %s OFFSET %s
    """
    
    cur.execute(query, (f"%{title_text}%", limit, offset))
    return cur.fetchall()

def search_movies_by_genre(cur, genre_name, limit=20, offset=0):
    query = """
    SELECT m.movie_id, m.title
    FROM movies m
    JOIN movie_genres mg ON m.movie_id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.genre_id
    WHERE g.genre_name = %s
    ORDER BY m.popularity DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (genre_name, limit, offset))
    return cur.fetchall()

def search_movies_by_production_company(cur, company_name, limit=20, offset=0):
    query = """
    SELECT m.movie_id, m.title
    FROM movies m
    JOIN movie_production_companies mp ON m.movie_id = mp.movie_id
    JOIN production_companies pc ON mp.company_id = pc.company_id
    WHERE pc.name = %s
    ORDER BY m.popularity DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (company_name, limit, offset))
    return cur.fetchall()

def search_movies_by_date_range(cur, start_date, end_date, limit=20, offset=0):
    query = """
    SELECT movie_id, title, released_date
    FROM movies
    WHERE released_date BETWEEN %s AND %s
    ORDER BY released_date DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (start_date, end_date, limit, offset))
    return cur.fetchall()

def search_movies_by_rating(cur, min_rating, limit=20, offset=0):
    query = """
    SELECT m.movie_id, m.title, AVG(r.rating) AS avg_rating
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
    HAVING AVG(r.rating) >= %s
    ORDER BY avg_rating DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (min_rating, limit, offset))
    return cur.fetchall()

def search_movies_by_actor(cur, actor_name, limit=20, offset=0):
    query = """
    SELECT m.movie_id, m.title
    FROM movies m
    JOIN movie_cast mc ON m.movie_id = mc.movie_id
    JOIN people p ON mc.person_id = p.person_id
    WHERE p.name ILIKE %s
    ORDER BY m.released_date DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (f"%{actor_name}%", limit, offset))
    return cur.fetchall()

def search_movies_by_actor(cur, actor_name, limit=20, offset=0):
    query = """
    SELECT m.movie_id, m.title
    FROM movies m
    JOIN movie_cast mc ON m.movie_id = mc.movie_id
    JOIN people p ON mc.person_id = p.person_id
    WHERE p.name ILIKE %s
    ORDER BY m.released_date DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (f"%{actor_name}%", limit, offset))
    return cur.fetchall()

def search_movies_by_crew(cur, crew_name, job=None, limit=20, offset=0):
    query = """
    SELECT m.movie_id, m.title
    FROM movies m
    JOIN movie_crew mc ON m.movie_id = mc.movie_id
    JOIN people p ON mc.person_id = p.person_id
    WHERE p.name ILIKE %s
    """
    params = [f"%{crew_name}%"]
    if job:
        query += " AND mc.job = %s"
        params.append(job)
    query += " ORDER BY m.released_date DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    cur.execute(query, params)
    return cur.fetchall()

def search_movies_by_keyword(cur, keyword_text, limit=20, offset=0):
    query = """
    SELECT m.movie_id, m.title
    FROM movies m
    JOIN movie_keywords mk ON m.movie_id = mk.movie_id
    JOIN keywords k ON mk.keyword_id = k.keyword_id
    WHERE k.keyword_name ILIKE %s
    ORDER BY m.popularity DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (f"%{keyword_text}%", limit, offset))
    return cur.fetchall()

def search_movies_combined(cur, title=None, genre=None, min_rating=None, limit=20, offset=0):
    query = """
    SELECT m.movie_id, m.title, AVG(r.rating) AS avg_rating
    FROM movies m
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
    LEFT JOIN genres g ON mg.genre_id = g.genre_id
    WHERE 1=1
    """
    params = []
    if title:
        query += " AND m.title ILIKE %s"
        params.append(f"%{title}%")
    if genre:
        query += " AND g.genre_name = %s"
        params.append(genre)
    query += " GROUP BY m.movie_id, m.title"
    if min_rating:
        query += " HAVING AVG(r.rating) >= %s"
        params.append(min_rating)
    query += " ORDER BY avg_rating DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    cur.execute(query, params)
    return cur.fetchall()

# ======================================================
# CRUD Admin Functions
# ======================================================

def admin_create_movie(cur, movie_data):
    query = """
    INSERT INTO movies (movie_id, title, adult, overview, language, popularity, 
                       released_date, runtime, poster_path, tagline)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING movie_id
    """
    cur.execute(query, (
        movie_data['movie_id'], movie_data['title'], movie_data['adult'],
        movie_data['overview'], movie_data['language'], movie_data['popularity'],
        movie_data['released_date'], movie_data['runtime'], 
        movie_data['poster_path'], movie_data['tagline']
    ))
    return cur.fetchone()[0]

def admin_update_movie(cur, movie_id, update_data):
    set_clauses = []
    params = []
    for field, value in update_data.items():
        set_clauses.append(f"{field} = %s")
        params.append(value)
    params.append(movie_id)
    
    query = f"""
    UPDATE movies 
    SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
    WHERE movie_id = %s
    RETURNING movie_id
    """
    cur.execute(query, params)
    return cur.fetchone()[0] if cur.rowcount > 0 else None

def admin_delete_movie(cur, movie_id):
    query = "DELETE FROM movies WHERE movie_id = %s RETURNING movie_id"
    cur.execute(query, (movie_id,))
    return cur.fetchone()[0] if cur.rowcount > 0 else None

def admin_create_genre(cur, genre_name):
    query = "INSERT INTO genres (genre_name) VALUES (%s) RETURNING genre_id"
    cur.execute(query, (genre_name,))
    return cur.fetchone()[0]

def admin_assign_genre_to_movie(cur, movie_id, genre_id):
    query = """
    INSERT INTO movie_genres (movie_id, genre_id) 
    VALUES (%s, %s)
    ON CONFLICT (movie_id, genre_id) DO NOTHING
    RETURNING movie_id
    """
    cur.execute(query, (movie_id, genre_id))
    return cur.fetchone()[0] if cur.rowcount > 0 else None

def admin_remove_genre_from_movie(cur, movie_id, genre_id):
    query = "DELETE FROM movie_genres WHERE movie_id = %s AND genre_id = %s RETURNING movie_id"
    cur.execute(query, (movie_id, genre_id))
    return cur.fetchone()[0] if cur.rowcount > 0 else None


cur = conn.cursor()
# results = search_movies_combined(cur, title="Star", genre="Science Fiction", min_rating=1, limit=100)
# for row in results:
#     print(row)
results = search_movies_by_title(cur, title_text="Star", limit=20, offset=0)
for rows in results:
    print(rows)
cur.close()

