import psycopg2
import os
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any

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
conn.autocommit = False

# =================================
# Detailed Movie Information Functions
# =================================

def get_movie_full_details(cur, movie_id: int) -> Optional[Dict[str, Any]]:
    """
    Get complete information about a movie including:
    - Basic movie info
    - Genres
    - Cast
    - Crew
    - Production companies
    - Rating statistics
    """
    # Get basic movie info
    movie_query = """
    SELECT 
        m.movie_id,
        m.title,
        m.adult,
        m.overview,
        m.language,
        m.popularity,
        m.released_date,
        m.runtime,
        m.poster_path,
        m.tagline,
        m.created_at,
        m.updated_at,
        AVG(r.rating) AS avg_rating,
        COUNT(r.rating) AS num_ratings
    FROM movies m
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    WHERE m.movie_id = %s
    GROUP BY m.movie_id, m.title, m.adult, m.overview, m.language, 
             m.popularity, m.released_date, m.runtime, m.poster_path, 
             m.tagline, m.created_at, m.updated_at
    """
    cur.execute(movie_query, (movie_id,))
    movie_row = cur.fetchone()
    
    if not movie_row:
        return None
    
    # Get genres
    genres_query = """
    SELECT g.genre_id, g.genre_name
    FROM genres g
    JOIN movie_genres mg ON g.genre_id = mg.genre_id
    WHERE mg.movie_id = %s
    ORDER BY g.genre_name
    """
    cur.execute(genres_query, (movie_id,))
    genres = [{"genre_id": r[0], "genre_name": r[1]} for r in cur.fetchall()]
    
    # Get cast
    cast_query = """
    SELECT 
        p.person_id,
        p.name,
        p.gender,
        p.profile_path,
        mc.character,
        mc.cast_order
    FROM people p
    JOIN movie_cast mc ON p.person_id = mc.person_id
    WHERE mc.movie_id = %s
    ORDER BY mc.cast_order
    """
    cur.execute(cast_query, (movie_id,))
    cast = [{
        "person_id": r[0],
        "name": r[1],
        "gender": r[2],
        "profile_path": r[3],
        "character": r[4],
        "cast_order": r[5]
    } for r in cur.fetchall()]
    
    # Get crew
    crew_query = """
    SELECT 
        p.person_id,
        p.name,
        p.gender,
        p.profile_path,
        mc.department,
        mc.job
    FROM people p
    JOIN movie_crew mc ON p.person_id = mc.person_id
    WHERE mc.movie_id = %s
    ORDER BY mc.department, mc.job, p.name
    """
    cur.execute(crew_query, (movie_id,))
    crew = [{
        "person_id": r[0],
        "name": r[1],
        "gender": r[2],
        "profile_path": r[3],
        "department": r[4],
        "job": r[5]
    } for r in cur.fetchall()]
    
    # Get production companies
    companies_query = """
    SELECT pc.company_id, pc.name
    FROM production_companies pc
    JOIN movie_production_companies mpc ON pc.company_id = mpc.company_id
    WHERE mpc.movie_id = %s
    ORDER BY pc.name
    """
    cur.execute(companies_query, (movie_id,))
    companies = [{"company_id": r[0], "name": r[1]} for r in cur.fetchall()]
    
    # Build complete movie object
    movie = {
        "movie_id": movie_row[0],
        "title": movie_row[1],
        "adult": movie_row[2],
        "overview": movie_row[3],
        "language": movie_row[4],
        "popularity": float(movie_row[5]) if movie_row[5] else None,
        "released_date": movie_row[6],
        "runtime": movie_row[7],
        "poster_path": movie_row[8],
        "tagline": movie_row[9],
        "created_at": movie_row[10],
        "updated_at": movie_row[11],
        "avg_rating": float(movie_row[12]) if movie_row[12] else None,
        "num_ratings": movie_row[13],
        "genres": genres,
        "cast": cast,
        "crew": crew,
        "production_companies": companies
    }
    
    return movie


def search_movies_by_title_detailed(cur, title_text: str, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Search movies by title and return comprehensive information for each.
    """
    query = """
    SELECT 
        m.movie_id,
        m.title,
        m.released_date,
        m.popularity,
        m.poster_path,
        m.runtime,
        m.overview,
        m.tagline,
        AVG(r.rating) AS avg_rating,
        COUNT(DISTINCT r.rating_id) AS num_ratings,  -- CHANGED: Added DISTINCT
        STRING_AGG(DISTINCT g.genre_name, ', ' ORDER BY g.genre_name) AS genres
    FROM movies m
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
    LEFT JOIN genres g ON mg.genre_id = g.genre_id
    WHERE m.title ILIKE %s
    GROUP BY m.movie_id, m.title, m.released_date, m.popularity, m.poster_path, m.runtime, m.overview, m.tagline
    ORDER BY avg_rating DESC NULLS LAST, m.popularity DESC
    LIMIT %s OFFSET %s
    """
    
    cur.execute(query, (f"%{title_text}%", limit, offset))
    results = []
    
    for row in cur.fetchall():
        results.append({
            "movie_id": row[0],
            "title": row[1],
            "released_date": row[2],
            "popularity": float(row[3]) if row[3] else None,
            "poster_path": row[4],
            "runtime": row[5],
            "overview": row[6],
            "tagline": row[7],
            "avg_rating": float(row[8]) if row[8] else None,
            "num_ratings": row[9],
            "genres": row[10]
        })
    
    return results

# =================================
# Original Query Functions (Enhanced)
# =================================

def search_movies_by_title(cur, title_text, limit=20, offset=0):
    """Enhanced version with more details"""
    return search_movies_by_title_detailed(cur, title_text, limit, offset)


def search_movies_by_genre(cur, genre_name, limit=20, offset=0):
    query = """
    SELECT 
        m.movie_id, 
        m.title,
        m.released_date,
        m.popularity,
        AVG(r.rating) AS avg_rating,
        COUNT(r.rating) AS num_ratings
    FROM movies m
    JOIN movie_genres mg ON m.movie_id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.genre_id
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    WHERE g.genre_name = %s
    GROUP BY m.movie_id, m.title, m.released_date, m.popularity
    ORDER BY m.popularity DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (genre_name, limit, offset))
    return cur.fetchall()


def search_movies_by_production_company(cur, company_name, limit=20, offset=0):
    query = """
    SELECT 
        m.movie_id, 
        m.title,
        m.released_date,
        m.popularity,
        AVG(r.rating) AS avg_rating 
    FROM movies m
    JOIN movie_production_companies mp ON m.movie_id = mp.movie_id
    JOIN production_companies pc ON mp.company_id = pc.company_id
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    WHERE pc.name = %s
    GROUP BY m.movie_id, m.title, m.released_date, m.popularity
    ORDER BY m.popularity DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (company_name, limit, offset))
    return cur.fetchall()


def search_movies_by_date_range(cur, start_date, end_date, limit=20, offset=0):
    query = """
    SELECT 
        movie_id, 
        title, 
        released_date,
        popularity,
        runtime
    FROM movies
    WHERE released_date BETWEEN %s AND %s
    ORDER BY released_date DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (start_date, end_date, limit, offset))
    return cur.fetchall()


def search_movies_by_rating(cur, min_rating, limit=20, offset=0):
    query = """
    SELECT 
        m.movie_id, 
        m.title, 
        m.released_date,
        AVG(r.rating) AS avg_rating,
        COUNT(r.rating) AS num_ratings
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title, m.released_date
    HAVING AVG(r.rating) >= %s
    ORDER BY avg_rating DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (min_rating, limit, offset))
    return cur.fetchall()


def search_movies_by_actor(cur, actor_name, limit=20, offset=0):
    query = """
    SELECT 
        m.movie_id, 
        m.title,
        m.released_date,
        mc.character,
        AVG(r.rating) AS avg_rating
    FROM movies m
    JOIN movie_cast mc ON m.movie_id = mc.movie_id
    JOIN people p ON mc.person_id = p.person_id
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    WHERE p.name ILIKE %s
    GROUP BY m.movie_id, m.title, m.released_date, mc.character
    ORDER BY m.released_date DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (f"%{actor_name}%", limit, offset))
    return cur.fetchall()


def search_movies_by_crew(cur, crew_name, job=None, limit=20, offset=0):
    query = """
    SELECT 
        m.movie_id, 
        m.title,
        m.released_date,
        mc.job,
        mc.department
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
    SELECT 
        m.movie_id, 
        m.title,
        m.released_date,
        m.popularity
    FROM movies m
    JOIN movie_keywords mk ON m.movie_id = mk.movie_id
    JOIN keywords k ON mk.keyword_id = k.keyword_id
    WHERE k.keyword_name ILIKE %s
    ORDER BY m.popularity DESC
    LIMIT %s OFFSET %s
    """
    cur.execute(query, (f"%{keyword_text}%", limit, offset))
    return cur.fetchall()


# def search_movies_combined(cur, title=None, genre=None, min_rating=None, limit=20, offset=0):
#     query = """
#     SELECT 
#         m.movie_id, 
#         m.title, 
#         m.released_date,
#         m.popularity,
#         AVG(r.rating) AS avg_rating,
#         COUNT(r.rating) AS num_ratings,
#         STRING_AGG(DISTINCT g.genre_name, ', ') AS genres
#     FROM movies m
#     LEFT JOIN ratings r ON m.movie_id = r.movie_id
#     LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
#     LEFT JOIN genres g ON mg.genre_id = g.genre_id
#     WHERE 1=1
#     """
#     params = []
#     if title:
#         query += " AND m.title ILIKE %s"
#         params.append(f"%{title}%")
#     if genre:
#         query += " AND g.genre_name = %s"
#         params.append(genre)
#     query += " GROUP BY m.movie_id, m.title, m.released_date, m.popularity"
#     if min_rating:
#         query += " HAVING AVG(r.rating) >= %s"
#         params.append(min_rating)
#     query += " ORDER BY avg_rating DESC NULLS LAST LIMIT %s OFFSET %s"
#     params.extend([limit, offset])

#     cur.execute(query, params)
#     return cur.fetchall()


# # =================================
# # Utility Functions for Display
# # =================================

# def print_movie_summary(movie: Dict[str, Any]) -> None:
#     """Pretty print a movie summary"""
#     print(f"\n{'='*80}")
#     print(f"Title: {movie['title']}")
#     print(f"Movie ID: {movie['movie_id']}")
#     print(f"Released: {movie.get('released_date', 'N/A')}")
#     print(f"Runtime: {movie.get('runtime', 'N/A')} minutes")
#     print(f"Popularity: {movie.get('popularity', 'N/A')}")
#     print(f"Rating: {movie.get('avg_rating', 'N/A')}/10 ({movie.get('num_ratings', 0)} ratings)")
#     if movie.get('genres'):
#         print(f"Genres: {movie['genres']}")
#     print(f"{'='*80}")


# def print_movie_full_details(movie: Dict[str, Any]) -> None:
#     """Pretty print complete movie details"""
#     print(f"\n{'='*80}")
#     print(f"MOVIE: {movie['title']}")
#     print(f"{'='*80}")
    
#     # Basic Info
#     print(f"\nID: {movie['movie_id']}")
#     print(f"Released: {movie.get('released_date', 'N/A')}")
#     print(f"Runtime: {movie.get('runtime', 'N/A')} minutes")
#     print(f"Language: {movie.get('language', 'N/A')}")
#     print(f"Popularity: {movie.get('popularity', 'N/A')}")
#     print(f"Rating: {movie.get('avg_rating', 'N/A')}/10 ({movie.get('num_ratings', 0)} ratings)")
#     print(f"Adult: {movie.get('adult', False)}")
    
#     if movie.get('tagline'):
#         print(f"\nTagline: {movie['tagline']}")
    
#     if movie.get('overview'):
#         print(f"\nOverview:\n{movie['overview']}")
    
#     # Genres
#     if movie.get('genres'):
#         print(f"\nGenres:")
#         for genre in movie['genres']:
#             print(f"  - {genre['genre_name']}")
    
#     # Production Companies
#     if movie.get('production_companies'):
#         print(f"\nProduction Companies:")
#         for company in movie['production_companies']:
#             print(f"  - {company['name']}")
    
#     # Cast
#     if movie.get('cast'):
#         print(f"\nCast ({len(movie['cast'])} members):")
#         for i, member in enumerate(movie['cast'][:10]):  # Show first 10
#             print(f"  {i+1}. {member['name']} as {member.get('character', 'N/A')}")
#         if len(movie['cast']) > 10:
#             print(f"  ... and {len(movie['cast']) - 10} more")
    
#     # Crew (grouped by department)
#     if movie.get('crew'):
#         print(f"\nCrew ({len(movie['crew'])} members):")
#         departments = {}
#         for member in movie['crew']:
#             dept = member['department']
#             if dept not in departments:
#                 departments[dept] = []
#             departments[dept].append(member)
        
#         for dept, members in sorted(departments.items()):
#             print(f"\n  {dept}:")
#             for member in members[:5]:  # Show first 5 per department
#                 print(f"    - {member['name']} ({member['job']})")
#             if len(members) > 5:
#                 print(f"    ... and {len(members) - 5} more")
    
#     print(f"\n{'='*80}\n")


# # =================================
# # Example Usage
# # =================================

# if __name__ == "__main__":
#     cur = conn.cursor()
    
#     # Example 1: Search movies by title
#     print("\n=== Searching for movies with 'Star' in title ===")
#     results = search_movies_by_title(cur, title_text="Star", limit=5, offset=0)
#     for movie in results:
#         print_movie_summary(movie)
    
#     # Example 2: Get full details for a specific movie
#     if results:
#         movie_id = results[0]['movie_id']
#         print(f"\n\n=== Full Details for Movie ID {movie_id} ===")
#         full_movie = get_movie_full_details(cur, movie_id)
#         if full_movie:
#             print_movie_full_details(full_movie)
    
#     # Example 3: Combined search
#     print("\n=== Combined Search: Science Fiction movies with 'Star' ===")
#     combined_results = search_movies_combined(
#         cur, 
#         title="Star", 
#         genre="Science Fiction", 
#         min_rating=6.0, 
#         limit=5
#     )
#     for row in combined_results:
#         print(f"\nID: {row[0]}, Title: {row[1]}, Rating: {row[4]}, Genres: {row[6]}")
    
#     cur.close()
#     conn.close()
