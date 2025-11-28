# ======================================================
# CRUD Admin Functions
# ======================================================

"""
admin_db.py
Database access layer for admin CRUD operations.
All functions expect a psycopg2 cursor (cur) as the first argument.
They use parameterized queries to avoid SQL injection.
"""
import psycopg2
import os
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional, Tuple
from datetime import date


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

# -------------------------
# Movies
# -------------------------

def admin_create_movie(cur, movie_data: Dict[str, Any]) -> int:
    """
    Insert a movie. movie_data must contain:
      movie_id, title, adult, overview, language, popularity,
      released_date (date or None), runtime, poster_path, tagline
    Returns the inserted movie_id.
    """
    query = """
    INSERT INTO movies (movie_id, title, adult, overview, language, popularity, 
                        released_date, runtime, poster_path, tagline)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING movie_id
    """
    cur.execute(query, (
        movie_data['movie_id'], movie_data['title'], movie_data.get('adult', False),
        movie_data.get('overview'), movie_data.get('language'), movie_data.get('popularity'),
        movie_data.get('released_date'), movie_data.get('runtime'),
        movie_data.get('poster_path'), movie_data.get('tagline')
    ))
    return cur.fetchone()[0]


def admin_get_movie(cur, movie_id: int) -> Optional[Dict[str, Any]]:
    """
    Simple movie read without aggregates or related data.
    Returns a dict with basic movie fields only.
    """
    query = """
    SELECT movie_id, title, adult, overview, language, popularity,
           released_date, runtime, poster_path, tagline, created_at, updated_at
    FROM movies
    WHERE movie_id = %s
    """
    cur.execute(query, (movie_id,))
    row = cur.fetchone()
    if not row:
        return None

    (movie_id, title, adult, overview, language, popularity,
     released_date, runtime, poster_path, tagline, created_at, updated_at) = row

    return {
        "movie_id": movie_id,
        "title": title,
        "adult": adult,
        "overview": overview,
        "language": language,
        "popularity": float(popularity) if popularity is not None else None,
        "released_date": released_date,
        "runtime": runtime,
        "poster_path": poster_path,
        "tagline": tagline,
        "created_at": created_at,
        "updated_at": updated_at
    }


def admin_search_movies_by_title(cur, title: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Search movies by title (fuzzy match).
    Returns list of basic movie dicts.
    """
    query = """
    SELECT movie_id, title, released_date
    FROM movies
    WHERE title ILIKE %s
    ORDER BY title
    LIMIT %s
    """
    cur.execute(query, (f"%{title}%", limit))
    rows = cur.fetchall()
    return [
        {"movie_id": r[0], "title": r[1], "released_date": r[2]}
        for r in rows
    ]


def admin_update_movie(cur, movie_id: int, update_data: Dict[str, Any]) -> Optional[int]:
    """
    Update movie fields specified in update_data dict.
    Returns the movie_id if updated, else None.
    """
    if not update_data:
        return None
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


def admin_delete_movie(cur, movie_id: int) -> Optional[int]:
    """
    Delete a movie (cascades to related tables because of FK ON DELETE CASCADE).
    Returns deleted movie_id or None.
    """
    query = "DELETE FROM movies WHERE movie_id = %s RETURNING movie_id"
    cur.execute(query, (movie_id,))
    return cur.fetchone()[0] if cur.rowcount > 0 else None

# -------------------------
# Genres CRUD
# -------------------------

def admin_create_genre(cur, genre_name: str) -> Optional[int]:
    """
    Create a new genre. Returns genre_id if created, None if genre already exists.
    """
    if not genre_name or not genre_name.strip():
        raise ValueError("Genre name cannot be empty")
    
    clean_name = genre_name.strip()
    query = """
    INSERT INTO genres (genre_name) 
    VALUES (%s) 
    ON CONFLICT (genre_name) DO NOTHING
    RETURNING genre_id
    """
    cur.execute(query, (clean_name,))
    result = cur.fetchone()
    return result[0] if result else None


def admin_get_genre(cur, genre_id: int) -> Optional[Dict[str, Any]]:
    """
    Get genre by ID. Returns dict with genre_id and genre_name.
    """
    query = "SELECT genre_id, genre_name FROM genres WHERE genre_id = %s"
    cur.execute(query, (genre_id,))
    row = cur.fetchone()
    return {"genre_id": row[0], "genre_name": row[1]} if row else None


def admin_search_genres_by_name(cur, genre_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Search genres by name (fuzzy match).
    Returns list of genre dicts.
    """
    query = """
    SELECT genre_id, genre_name
    FROM genres
    WHERE genre_name ILIKE %s
    ORDER BY genre_name
    LIMIT %s
    """
    cur.execute(query, (f"%{genre_name}%", limit))
    rows = cur.fetchall()
    return [{"genre_id": r[0], "genre_name": r[1]} for r in rows]


def admin_read_genres(cur) -> List[Dict[str, Any]]:
    """
    Get all genres ordered by name.
    Returns list of genre dicts.
    """
    cur.execute("SELECT genre_id, genre_name FROM genres ORDER BY genre_name")
    return [{"genre_id": r[0], "genre_name": r[1]} for r in cur.fetchall()]


def admin_update_genre(cur, genre_id: int, new_name: str) -> Optional[int]:
    """
    Update genre name. Returns genre_id if updated, None if not found.
    """
    if not new_name or not new_name.strip():
        raise ValueError("Genre name cannot be empty")
    
    clean_name = new_name.strip()
    query = """
    UPDATE genres 
    SET genre_name = %s 
    WHERE genre_id = %s 
    RETURNING genre_id
    """
    cur.execute(query, (clean_name, genre_id))
    return cur.fetchone()[0] if cur.rowcount > 0 else None


def admin_delete_genre(cur, genre_id: int) -> Optional[int]:
    """
    Delete genre if not used by any movies.
    Returns genre_id if deleted, None if not found.
    Raises exception if genre is in use.
    """
    # Check if genre is used by any movies
    check_query = "SELECT COUNT(*) FROM movie_genres WHERE genre_id = %s"
    cur.execute(check_query, (genre_id,))
    movie_count = cur.fetchone()[0]
    
    if movie_count > 0:
        raise Exception(f"Cannot delete genre: used by {movie_count} movies")
    
    query = "DELETE FROM genres WHERE genre_id = %s RETURNING genre_id"
    cur.execute(query, (genre_id,))
    return cur.fetchone()[0] if cur.rowcount > 0 else None

# -------------------------
# Production Companies CRUD
# -------------------------

def admin_create_company(cur, name: str) -> Optional[int]:
    """
    Create a new production company. 
    Returns company_id if created, None if company already exists.
    """
    if not name or not name.strip():
        raise ValueError("Company name cannot be empty")
    
    clean_name = name.strip()
    query = """
    INSERT INTO production_companies (name) 
    VALUES (%s) 
    ON CONFLICT (name) DO NOTHING
    RETURNING company_id
    """
    cur.execute(query, (clean_name,))
    result = cur.fetchone()
    return result[0] if result else None


def admin_get_company(cur, company_id: int) -> Optional[Dict[str, Any]]:
    """
    Get company by ID. Returns dict with company_id and name.
    """
    query = "SELECT company_id, name FROM production_companies WHERE company_id = %s"
    cur.execute(query, (company_id,))
    row = cur.fetchone()
    return {"company_id": row[0], "name": row[1]} if row else None


def admin_search_companies_by_name(cur, name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Search production companies by name (fuzzy match).
    Returns list of company dicts.
    """
    query = """
    SELECT company_id, name
    FROM production_companies
    WHERE name ILIKE %s
    ORDER BY name
    LIMIT %s
    """
    cur.execute(query, (f"%{name}%", limit))
    rows = cur.fetchall()
    return [{"company_id": r[0], "name": r[1]} for r in rows]


def admin_read_companies(cur, limit: int = 100) -> List[Dict[str, Any]]:
    
    if limit is None:
        query = "SELECT company_id, name FROM production_companies ORDER BY name"
        cur.execute(query)
    else:
        query = "SELECT company_id, name FROM production_companies ORDER BY name LIMIT %s"
        cur.execute(query, (limit,))
    
    return [{"company_id": r[0], "name": r[1]} for r in cur.fetchall()]


def admin_update_company(cur, company_id: int, new_name: str) -> Optional[int]:
    """
    Update company name. Returns company_id if updated, None if not found.
    """
    if not new_name or not new_name.strip():
        raise ValueError("Company name cannot be empty")
    
    clean_name = new_name.strip()
    query = """
    UPDATE production_companies 
    SET name = %s 
    WHERE company_id = %s 
    RETURNING company_id
    """
    cur.execute(query, (clean_name, company_id))
    return cur.fetchone()[0] if cur.rowcount > 0 else None


def admin_delete_company(cur, company_id: int) -> Optional[int]:
    """
    Delete company if not associated with any movies.
    Returns company_id if deleted, None if not found.
    Raises exception if company has movie associations.
    """
    # Check if company is associated with any movies
    check_query = "SELECT COUNT(*) FROM movie_production_companies WHERE company_id = %s"
    cur.execute(check_query, (company_id,))
    movie_count = cur.fetchone()[0]
    
    if movie_count > 0:
        raise Exception(f"Cannot delete company: associated with {movie_count} movies")
    
    query = "DELETE FROM production_companies WHERE company_id = %s RETURNING company_id"
    cur.execute(query, (company_id,))
    return cur.fetchone()[0] if cur.rowcount > 0 else None

def admin_get_company_with_movies(cur, company_id: int) -> Optional[Dict[str, Any]]:
    """
    Get company details with movie count and sample movies.
    """
    query = """
    SELECT 
        pc.company_id,
        pc.name,
        COUNT(mpc.movie_id) as movie_count,
        ARRAY_AGG(m.title) FILTER (WHERE m.title IS NOT NULL) as sample_movies
    FROM production_companies pc
    LEFT JOIN movie_production_companies mpc ON pc.company_id = mpc.company_id
    LEFT JOIN movies m ON mpc.movie_id = m.movie_id
    WHERE pc.company_id = %s
    GROUP BY pc.company_id, pc.name
    """
    cur.execute(query, (company_id,))
    row = cur.fetchone()
    
    if not row:
        return None
    
    return {
        "company_id": row[0],
        "name": row[1],
        "movie_count": row[2],
        "sample_movies": row[3][:5] if row[3] else []  # First 5 movies
    }

# def admin_get_companies_with_stats(cur, limit: int = 100) -> List[Dict[str, Any]]:
#     """
#     Get all companies with movie counts, ordered by most active.
#     """
#     query = """
#     SELECT 
#         pc.company_id,
#         pc.name,
#         COUNT(mpc.movie_id) as movie_count,
#         MIN(m.released_date) as first_movie_date,
#         MAX(m.released_date) as last_movie_date
#     FROM production_companies pc
#     LEFT JOIN movie_production_companies mpc ON pc.company_id = mpc.company_id
#     LEFT JOIN movies m ON mpc.movie_id = m.movie_id
#     GROUP BY pc.company_id, pc.name
#     ORDER BY movie_count DESC, pc.name
#     LIMIT %s
#     """
#     cur.execute(query, (limit,))
#     rows = cur.fetchall()
    
#     return [{
#         "company_id": r[0],
#         "name": r[1],
#         "movie_count": r[2],
#         "first_movie_date": r[3],
#         "last_movie_date": r[4]
#     } for r in rows]

# -------------------------
# Ratings CRUD
# -------------------------

def admin_get_rating(cur, rating_id: int) -> Optional[Dict[str, Any]]:
    """
    Get rating by ID. Returns dict with rating details.
    """
    query = """
    SELECT rating_id, user_id, movie_id, rating, created_at, updated_at 
    FROM ratings 
    WHERE rating_id = %s
    """
    cur.execute(query, (rating_id,))
    row = cur.fetchone()
    
    if not row:
        return None
    
    return {
        "rating_id": row[0],
        "user_id": row[1],
        "movie_id": row[2],
        "rating": float(row[3]) if row[3] is not None else None,
        "created_at": row[4],
        "updated_at": row[5]
    }


def admin_read_ratings(cur, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Get ratings with pagination. Returns list of rating dicts.
    """
    cur.execute("""
        SELECT rating_id, user_id, movie_id, rating, created_at, updated_at
        FROM ratings
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))
    
    rows = cur.fetchall()
    return [{
        "rating_id": r[0],
        "user_id": r[1],
        "movie_id": r[2],
        "rating": float(r[3]) if r[3] is not None else None,
        "created_at": r[4],
        "updated_at": r[5]
    } for r in rows]


def admin_update_rating(cur, rating_id: int, new_rating: float) -> Optional[int]:
    """
    Update rating value. Returns rating_id if updated, None if not found.
    """
    # Validate rating range
    if new_rating < 0 or new_rating > 10:
        raise ValueError("Rating must be between 0 and 10")
    
    cur.execute("""
        UPDATE ratings SET rating = %s, updated_at = CURRENT_TIMESTAMP
        WHERE rating_id = %s
        RETURNING rating_id
    """, (new_rating, rating_id))
    
    return cur.fetchone()[0] if cur.rowcount else None


def admin_delete_rating(cur, rating_id: int) -> Optional[int]:
    """
    Delete rating by ID. Returns rating_id if deleted, None if not found.
    """
    cur.execute("DELETE FROM ratings WHERE rating_id = %s RETURNING rating_id", (rating_id,))
    return cur.fetchone()[0] if cur.rowcount else None

# -------------------------
# Movie Genres CRUD (junction) add genre to movie
# -------------------------
def admin_create_movie_genre(cur, movie_id: int, genre_id: int) -> Optional[Tuple[int, int]]:
    query = """
    INSERT INTO movie_genres (movie_id, genre_id) 
    VALUES (%s, %s)
    ON CONFLICT DO NOTHING
    RETURNING movie_id, genre_id
    """
    cur.execute(query, (movie_id, genre_id))
    return cur.fetchone() if cur.rowcount > 0 else None


def admin_get_movie_genre(cur, movie_id: int, genre_id: int) -> Optional[Tuple[int, int]]:
    query = "SELECT movie_id, genre_id FROM movie_genres WHERE movie_id = %s AND genre_id = %s"
    cur.execute(query, (movie_id, genre_id))
    return cur.fetchone()


def admin_read_movie_genres(cur, movie_id: Optional[int] = None) -> List[Tuple[int, int]]:
    if movie_id is not None:
        cur.execute("SELECT movie_id, genre_id FROM movie_genres WHERE movie_id = %s", (movie_id,))
    else:
        cur.execute("SELECT movie_id, genre_id FROM movie_genres")
    return cur.fetchall()


def admin_delete_movie_genre(cur, movie_id: int, genre_id: int) -> Optional[int]:
    query = "DELETE FROM movie_genres WHERE movie_id = %s AND genre_id = %s RETURNING movie_id"
    cur.execute(query, (movie_id, genre_id))
    return cur.fetchone()[0] if cur.rowcount > 0 else None

# -------------------------
# Movie Production Companies CRUD (junction) add company to movie
# -------------------------

def admin_create_movie_company(cur, movie_id: int, company_id: int) -> Optional[Dict[str, Any]]:
    """
    Associate a production company with a movie.
    Returns dict with movie_id and company_id if created, None if duplicate.
    """
    # Validate existence
    cur.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
    if not cur.fetchone():
        raise ValueError(f"Movie ID {movie_id} does not exist")
    
    cur.execute("SELECT 1 FROM production_companies WHERE company_id = %s", (company_id,))
    if not cur.fetchone():
        raise ValueError(f"Company ID {company_id} does not exist")
    
    query = """
    INSERT INTO movie_production_companies (movie_id, company_id) 
    VALUES (%s, %s)
    ON CONFLICT DO NOTHING
    RETURNING movie_id, company_id
    """
    cur.execute(query, (movie_id, company_id))
    result = cur.fetchone()
    return {"movie_id": result[0], "company_id": result[1]} if result else None


def admin_get_movie_company(cur, movie_id: int, company_id: int) -> Optional[Dict[str, Any]]:
    """
    Check if a movie is associated with a production company.
    Returns dict with movie_id and company_id if found.
    """
    query = "SELECT movie_id, company_id FROM movie_production_companies WHERE movie_id = %s AND company_id = %s"
    cur.execute(query, (movie_id, company_id))
    result = cur.fetchone()
    return {"movie_id": result[0], "company_id": result[1]} if result else None


def admin_read_movie_companies(cur, movie_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Get movie-company associations.
    If movie_id provided, returns all companies for that movie.
    Otherwise returns all associations.
    """
    if movie_id is not None:
        cur.execute("SELECT movie_id, company_id FROM movie_production_companies WHERE movie_id = %s", (movie_id,))
    else:
        cur.execute("SELECT movie_id, company_id FROM movie_production_companies")
    
    rows = cur.fetchall()
    return [{"movie_id": r[0], "company_id": r[1]} for r in rows]


def admin_delete_movie_company(cur, movie_id: int, company_id: int) -> Optional[int]:
    """
    Remove association between movie and production company.
    Returns movie_id if deleted, None if not found.
    """
    query = "DELETE FROM movie_production_companies WHERE movie_id = %s AND company_id = %s RETURNING movie_id"
    cur.execute(query, (movie_id, company_id))
    result = cur.fetchone()
    return result[0] if result else None

# -------------------------
# Keywords CRUD
# -------------------------


# -------------------------
# Movie Keywords CRUD (junction)
# -------------------------



# -------------------------
# People CRUD
# -------------------------

# Gender constants
GENDER_UNKNOWN = 0
GENDER_FEMALE = 1
GENDER_MALE = 2
GENDER_NON_BINARY = 3

GENDER_MAP = {
    GENDER_UNKNOWN: "Unknown",
    GENDER_FEMALE: "Female", 
    GENDER_MALE: "Male",
    GENDER_NON_BINARY: "Non-binary"
}

REVERSE_GENDER_MAP = {
    "unknown": GENDER_UNKNOWN,
    "female": GENDER_FEMALE,
    "male": GENDER_MALE,
    "non_binary": GENDER_NON_BINARY
}

def validate_gender(gender: Optional[int]) -> int:
    """Validate and normalize gender value"""
    if gender is None:
        return GENDER_UNKNOWN
    if gender in [GENDER_UNKNOWN, GENDER_FEMALE, GENDER_MALE, GENDER_NON_BINARY]:
        return gender
    return GENDER_UNKNOWN

def get_gender_display(gender: Optional[int]) -> str:
    """Convert gender code to display name"""
    if gender is None:
        return "Unknown"
    return GENDER_MAP.get(gender, "Unknown")

def admin_create_person(cur, tmdb_id: Optional[int], name: str, gender: Optional[int] = None, profile_path: Optional[str] = None) -> int:
    """
    Create person. If tmdb_id exists, prefer to use uniqueness on tmdb_id.
    Returns person_id.
    """
    if not name or not name.strip():
        raise ValueError("Person name cannot be empty")
    
    clean_name = name.strip()
    normalized_gender = validate_gender(gender)
    
    if tmdb_id is not None:
        cur.execute("""
            INSERT INTO people (tmdb_id, name, gender, profile_path)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tmdb_id) DO UPDATE
              SET name = EXCLUDED.name, gender = EXCLUDED.gender, profile_path = EXCLUDED.profile_path
            RETURNING person_id
        """, (tmdb_id, clean_name, normalized_gender, profile_path))
        return cur.fetchone()[0]
    else:
        cur.execute("""
            INSERT INTO people (name, gender, profile_path)
            VALUES (%s, %s, %s)
            RETURNING person_id
        """, (clean_name, normalized_gender, profile_path))
        return cur.fetchone()[0]

def admin_get_person(cur, person_id: int) -> Optional[Dict[str, Any]]:
    """
    Get person by ID. Returns dict with person details including gender display.
    """
    query = "SELECT person_id, tmdb_id, name, gender, profile_path FROM people WHERE person_id = %s"
    cur.execute(query, (person_id,))
    row = cur.fetchone()
    
    if not row:
        return None
    
    return {
        "person_id": row[0],
        "tmdb_id": row[1],
        "name": row[2],
        "gender": row[3],
        "gender_display": get_gender_display(row[3]),
        "profile_path": row[4]
    }

def admin_search_people_by_name(cur, name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Search people by name (fuzzy match).
    Returns list of person dicts with gender display.
    """
    query = """
    SELECT person_id, name, tmdb_id, gender, profile_path
    FROM people
    WHERE name ILIKE %s
    ORDER BY name
    LIMIT %s
    """
    cur.execute(query, (f"%{name}%", limit))
    rows = cur.fetchall()
    
    return [{
        "person_id": r[0],
        "name": r[1],
        "tmdb_id": r[2],
        "gender": r[3],
        "gender_display": get_gender_display(r[3]),
        "profile_path": r[4]
    } for r in rows]

def admin_read_people(cur, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Get people with pagination. Returns list of person dicts with gender display.
    """
    cur.execute("""
        SELECT person_id, tmdb_id, name, gender, profile_path 
        FROM people 
        ORDER BY name 
        LIMIT %s OFFSET %s
    """, (limit, offset))
    
    rows = cur.fetchall()
    return [{
        "person_id": r[0],
        "tmdb_id": r[1],
        "name": r[2],
        "gender": r[3],
        "gender_display": get_gender_display(r[3]),
        "profile_path": r[4]
    } for r in rows]


def admin_update_person(cur, person_id: int, update_data: Dict[str, Any]) -> Optional[int]:
    """
    Update person fields. Returns person_id if updated, None if not found.
    """
    if not update_data:
        return None
    
    # Handle gender normalization if provided
    if 'gender' in update_data:
        update_data['gender'] = validate_gender(update_data['gender'])
    
    # Validate name if being updated
    if 'name' in update_data and (not update_data['name'] or not update_data['name'].strip()):
        raise ValueError("Person name cannot be empty")
    
    # Clean name if provided
    if 'name' in update_data:
        update_data['name'] = update_data['name'].strip()
    
    set_clauses = []
    params = []
    for field, value in update_data.items():
        set_clauses.append(f"{field} = %s")
        params.append(value)
    
    params.append(person_id)
    query = f"UPDATE people SET {', '.join(set_clauses)} WHERE person_id = %s RETURNING person_id"
    
    cur.execute(query, params)
    result = cur.fetchone()
    return result[0] if result else None


def admin_delete_person(cur, person_id: int) -> Optional[int]:
    """
    Delete person. Returns person_id if deleted, None if not found.
    """
    cur.execute("DELETE FROM people WHERE person_id = %s RETURNING person_id", (person_id,))
    result = cur.fetchone()
    return result[0] if result else None

# -------------------------
# Movie Cast CRUD (junction with extra fields)
# -------------------------

def admin_create_movie_cast(cur, movie_id: int, person_id: int, character: Optional[str], cast_order: Optional[int], credit_id: Optional[str]) -> Optional[Tuple]:
    query = """
    INSERT INTO movie_cast (movie_id, person_id, character, cast_order, credit_id)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (movie_id, person_id) DO NOTHING
    RETURNING movie_id, person_id
    """
    cur.execute(query, (movie_id, person_id, character, cast_order, credit_id))
    return cur.fetchone() if cur.rowcount > 0 else None


def admin_get_movie_cast(cur, movie_id: int, person_id: int) -> Optional[Tuple]:
    query = """
    SELECT movie_id, person_id, character, cast_order, credit_id
    FROM movie_cast
    WHERE movie_id = %s AND person_id = %s
    """
    cur.execute(query, (movie_id, person_id))
    return cur.fetchone()


def admin_read_movie_casts(cur, movie_id: Optional[int] = None) -> List[Tuple]:
    if movie_id is not None:
        cur.execute("""
            SELECT movie_id, person_id, character, cast_order, credit_id
            FROM movie_cast
            WHERE movie_id = %s
        """, (movie_id,))
    else:
        cur.execute("""
            SELECT movie_id, person_id, character, cast_order, credit_id
            FROM movie_cast
        """)
    return cur.fetchall()


def admin_update_movie_cast(cur, movie_id: int, person_id: int, update_data: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    if not update_data:
        return None
    set_clauses = []
    params = []
    for field, value in update_data.items():
        set_clauses.append(f"{field} = %s")
        params.append(value)
    params.extend([movie_id, person_id])
    query = f"UPDATE movie_cast SET {', '.join(set_clauses)} WHERE movie_id = %s AND person_id = %s RETURNING movie_id, person_id"
    cur.execute(query, params)
    return cur.fetchone() if cur.rowcount > 0 else None


def admin_delete_movie_cast(cur, movie_id: int, person_id: int) -> Optional[int]:
    cur.execute("DELETE FROM movie_cast WHERE movie_id = %s AND person_id = %s RETURNING movie_id", (movie_id, person_id))
    return cur.fetchone()[0] if cur.rowcount > 0 else None

# -------------------------
# Movie Crew CRUD (junction with extra fields)
# -------------------------

# -------------------------
# Movie Crew CRUD (junction with extra fields)
# -------------------------
def admin_create_movie_crew(cur, movie_id: int, person_id: int, department: str, job: str, credit_id: Optional[str]) -> Optional[Tuple]:
    # Add validation
    if len(department) > 100:
        raise ValueError("Department cannot exceed 100 characters")
    if len(job) > 100:
        raise ValueError("Job cannot exceed 100 characters")
    if credit_id and len(credit_id) > 50:
        raise ValueError("Credit ID cannot exceed 50 characters")

    query = """
    INSERT INTO movie_crew (movie_id, person_id, department, job, credit_id)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (movie_id, person_id, job) DO NOTHING
    RETURNING movie_id, person_id, job
    """
    try:
        cur.execute(query, (movie_id, person_id, department, job, credit_id))
        return cur.fetchone() if cur.rowcount > 0 else None
    except Exception as e:
        if "credit_id" in str(e) and "unique" in str(e):
            raise ValueError("Credit ID already exists") from e
        elif "foreign key" in str(e):
            raise ValueError("Movie or Person does not exist") from e
        raise


def admin_get_movie_crew(cur, movie_id: int, person_id: int, job: str) -> Optional[Tuple]:
    query = """
    SELECT movie_id, person_id, department, job, credit_id
    FROM movie_crew
    WHERE movie_id = %s AND person_id = %s AND job = %s
    """
    cur.execute(query, (movie_id, person_id, job))
    return cur.fetchone()


def admin_read_movie_crews(cur, movie_id: Optional[int] = None) -> List[Tuple]:
    if movie_id is not None:
        cur.execute("""
            SELECT movie_id, person_id, department, job, credit_id
            FROM movie_crew
            WHERE movie_id = %s
        """, (movie_id,))
    else:
        cur.execute("""
            SELECT movie_id, person_id, department, job, credit_id
            FROM movie_crew
        """)
    return cur.fetchall()


def admin_update_movie_crew(cur, movie_id: int, person_id: int, job: str, update_data: Dict[str, Any]) -> Optional[Tuple[int, int, str]]:
    if not update_data:
        return None
    
    # Add validation for update fields
    allowed_fields = {'department', 'job', 'credit_id'}
    for field in update_data.keys():
        if field not in allowed_fields:
            raise ValueError(f"Cannot update field: {field}")
    
    # Length validation
    if 'department' in update_data and len(update_data['department']) > 100:
        raise ValueError("Department cannot exceed 100 characters")
    if 'job' in update_data and len(update_data['job']) > 100:
        raise ValueError("Job cannot exceed 100 characters")
    if 'credit_id' in update_data and update_data['credit_id'] and len(update_data['credit_id']) > 50:
        raise ValueError("Credit ID cannot exceed 50 characters")
    
    set_clauses = []
    params = []
    for field, value in update_data.items():
        set_clauses.append(f"{field} = %s")
        params.append(value)
    params.extend([movie_id, person_id, job])
    
    query = f"UPDATE movie_crew SET {', '.join(set_clauses)} WHERE movie_id = %s AND person_id = %s AND job = %s RETURNING movie_id, person_id, job"
    
    try:
        cur.execute(query, params)
        return cur.fetchone() if cur.rowcount > 0 else None
    except Exception as e:
        if "credit_id" in str(e) and "unique" in str(e):
            raise ValueError("Credit ID already exists") from e
        raise


def admin_delete_movie_crew(cur, movie_id: int, person_id: int, job: str) -> Optional[int]:
    try:
        cur.execute("DELETE FROM movie_crew WHERE movie_id = %s AND person_id = %s AND job = %s RETURNING movie_id", (movie_id, person_id, job))
        return cur.fetchone()[0] if cur.rowcount > 0 else None
    except Exception as e:
        raise ValueError(f"Failed to delete crew member: {str(e)}")
# -------------------------
# Users CRUD
# -------------------------
