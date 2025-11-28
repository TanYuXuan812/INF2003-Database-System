import csv
import psycopg2
import json
import os
import ast
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# -------------------------------
# Database connection
# -------------------------------
conn = psycopg2.connect(
    dbname=os.getenv("DATABASE"),
    user=os.getenv("DB_USER"),
    password=os.getenv("PASSWORD"),
    host=os.getenv("HOST"),
    port=os.getenv("PORT")


)
conn.autocommit = False  # we'll manage commits


def get_cursor():
    return conn.cursor()


# -------------------------------
# Parsing helpers
# -------------------------------
def parse_json_field(field_str):
    """Safely parse Python-style string representations"""
    if not field_str or field_str.strip() == '':
        return []
    try:
        # This handles {'name': "O'Neal"} correctly
        return ast.literal_eval(field_str)
    except (ValueError, SyntaxError):
        try:
            return json.loads(field_str)
        except:
            return []


def parse_date(date_str):
    """Parse date in format YYYY-MM-DD or DD/MM/YYYY"""
    if not date_str or str(date_str).strip() == '':
        return None
    for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(date_str, fmt).date()
        except:
            continue
    return None


def parse_timestamp(timestamp_str):
    """Parse unix timestamp (seconds) to datetime, if possible"""
    try:
        timestamp = float(timestamp_str)
        return datetime.fromtimestamp(timestamp)
    except:
        return None


# -------------------------------
# Insert helpers (normalized)
# -------------------------------
def insert_genres(cur, genres_list, movie_id):
    for genre in genres_list:
        genre_name = genre.get('name')
        if not genre_name:
            continue
        cur.execute("""
            INSERT INTO genres (genre_name)
            VALUES (%s)
            ON CONFLICT (genre_name) DO NOTHING
        """, (genre_name,))
        cur.execute(
            "SELECT genre_id FROM genres WHERE genre_name = %s", (genre_name,))
        res = cur.fetchone()
        if res:
            genre_id = res[0]
            cur.execute("""
                INSERT INTO movie_genres (movie_id, genre_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (movie_id, genre_id))


def insert_production_companies(cur, companies_list, movie_id):
    for comp in companies_list:
        name = comp.get('name')
        if not name:
            continue
        cur.execute("""
            INSERT INTO production_companies (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, (name,))
        cur.execute(
            "SELECT company_id FROM production_companies WHERE name = %s", (name,))
        res = cur.fetchone()
        if res:
            company_id = res[0]
            cur.execute("""
                INSERT INTO movie_production_companies (movie_id, company_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (movie_id, company_id))


def insert_keywords(cur, keywords_list, movie_id):
    for kw in keywords_list:
        name = kw.get('name')
        if not name:
            continue
        cur.execute("""
            INSERT INTO keywords (keyword_name)
            VALUES (%s)
            ON CONFLICT (keyword_name) DO NOTHING
        """, (name,))
        cur.execute(
            "SELECT keyword_id FROM keywords WHERE keyword_name = %s", (name,))
        res = cur.fetchone()
        if res:
            keyword_id = res[0]
            cur.execute("""
                INSERT INTO movie_keywords (movie_id, keyword_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (movie_id, keyword_id))


def upsert_person_return_id(cur, tmdb_id, name, gender, profile_path):
    """
    Insert a person into people table or update existing, returning person_id.
    Note: tmdb_id can be NULL; in that case we try to avoid relying on tmdb_id uniqueness.
    """
    if tmdb_id is not None:
        cur.execute("""
            INSERT INTO people (tmdb_id, name, gender, profile_path)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tmdb_id) DO UPDATE
              SET name = EXCLUDED.name,
                  gender = EXCLUDED.gender,
                  profile_path = EXCLUDED.profile_path
            RETURNING person_id
        """, (tmdb_id, name, gender, profile_path))
        res = cur.fetchone()
        return res[0] if res else None
    else:
        # fallback: try to find an existing person by exact name + profile_path (best-effort)
        cur.execute("SELECT person_id FROM people WHERE name = %s AND profile_path IS NOT DISTINCT FROM %s LIMIT 1",
                    (name, profile_path))
        res = cur.fetchone()
        if res:
            return res[0]
        # otherwise insert new with NULL tmdb_id
        cur.execute("""
            INSERT INTO people (tmdb_id, name, gender, profile_path)
            VALUES (NULL, %s, %s, %s)
            RETURNING person_id
        """, (name, gender, profile_path))
        return cur.fetchone()[0]


def insert_people_and_cast(cur, cast_list, movie_id):
    """
    For each cast member:
      - upsert into people (by tmdb_id if present)
      - insert into movie_cast junction table
    """
    for member in cast_list:
        try:
            tmdb_id = member.get('id')
            name = member.get('name')
            gender = member.get('gender')
            profile_path = member.get('profile_path')
            credit_id = member.get('credit_id')
            character = member.get('character')
            order = member.get('order')

            person_id = upsert_person_return_id(
                cur, tmdb_id, name, gender, profile_path)
            if not person_id:
                # safety: skip if we couldn't determine a person id
                continue

            cur.execute("""
                INSERT INTO movie_cast (movie_id, person_id, character, cast_order, credit_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (movie_id, person_id) DO NOTHING
            """, (movie_id, person_id, character, order, credit_id))
        except Exception as e:
            print(f"  Error inserting cast member {member.get('name')}: {e}")
            raise


def insert_people_and_crew(cur, crew_list, movie_id):
    """
    For each crew member:
      - upsert into people (by tmdb_id if present)
      - insert into movie_crew junction table
    """
    for member in crew_list:
        try:
            tmdb_id = member.get('id')
            name = member.get('name')
            gender = member.get('gender')
            profile_path = member.get('profile_path')
            credit_id = member.get('credit_id')
            department = member.get('department')
            job = member.get('job')

            person_id = upsert_person_return_id(
                cur, tmdb_id, name, gender, profile_path)
            if not person_id:
                continue

            cur.execute("""
                INSERT INTO movie_crew (movie_id, person_id, department, job, credit_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (movie_id, person_id, job) DO NOTHING
            """, (movie_id, person_id, department, job, credit_id))
        except Exception as e:
            print(f"  Error inserting crew member {member.get('name')}: {e}")
            raise


# -------------------------------
# PROCESS MOVIES CSV
# -------------------------------
print("Processing Movies CSV...")
movie_count = 0
movie_errors = 0

with open('data/movies_metadata.csv', 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    header = next(reader, None)
    for row in reader:
        cur = get_cursor()
        try:
            # adjust indexes according to your CSV header layout
            movie_id = int(row[2])
            adult = row[0].upper() == 'TRUE' if row[0] else False
            genres = parse_json_field(row[1]) if len(row) > 1 else []
            language = row[3] if len(row) > 3 else None
            title = row[4] if len(row) > 4 else None
            overview = row[5] if len(row) > 5 else None
            popularity = float(row[6]) if (len(row) > 6 and row[6]) else None
            poster_path = row[7] if len(row) > 7 else None
            production_companies = parse_json_field(row[8]) if len(row) > 8 else []
            released_date = parse_date(row[9]) if len(row) > 9 else None
            runtime = int(float(row[11])) if (len(row) > 11 and row[11]) else None
            tagline = row[12] if len(row) > 12 else None

            cur.execute("""
                INSERT INTO movies (
                    movie_id, title, adult, overview, language,
                    popularity, released_date, runtime, poster_path, tagline
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (movie_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    adult = EXCLUDED.adult,
                    overview = EXCLUDED.overview,
                    language = EXCLUDED.language,
                    popularity = EXCLUDED.popularity,
                    released_date = EXCLUDED.released_date,
                    runtime = EXCLUDED.runtime,
                    poster_path = EXCLUDED.poster_path,
                    tagline = EXCLUDED.tagline,
                    updated_at = CURRENT_TIMESTAMP
            """, (movie_id, title, adult, overview, language,
                  popularity, released_date, runtime, poster_path, tagline))

            # related tables
            insert_genres(cur, genres, movie_id)
            insert_production_companies(cur, production_companies, movie_id)

            conn.commit()
            movie_count += 1
            print(f"Movie inserted/updated: {title} (ID {movie_id})")
        except Exception as e:
            conn.rollback()
            movie_errors += 1
            print(
                f"Error processing movie_id {row[0] if row else 'unknown'}: {e}")
        finally:
            cur.close()

print(f"\nMovies processed: {movie_count}, errors: {movie_errors}")
print("=" * 50)


# -------------------------------
# PROCESS KEYWORDS CSV
# -------------------------------
print("Processing Keywords CSV...")
keyword_count = 0
keyword_errors = 0
with open('data/keywords.csv', 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    header = next(reader, None)
    for row in reader:
        cur = get_cursor()
        try:
            movie_id = int(row[0])
            keywords = parse_json_field(row[1]) if len(row) > 1 else []
            insert_keywords(cur, keywords, movie_id)
            conn.commit()
            keyword_count += 1
            print(
                f"Keywords inserted for movie {movie_id} ({len(keywords)})")
        except Exception as e:
            conn.rollback()
            keyword_errors += 1
            print(
                f"Error inserting keywords for movie {row[0] if row else 'unknown'}: {e}")
        finally:
            cur.close()

print(f"\nKeywords processed: {keyword_count}, errors: {keyword_errors}")
print("=" * 50)


# -------------------------------
# PROCESS CREDITS CSV (people, movie_cast, movie_crew)
# -------------------------------
print("Processing Credits CSV...")
credit_count = 0
credit_errors = 0
with open('data/credits.csv', 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    header = next(reader, None)
    for row in reader:
        cur = get_cursor()
        try:
            # expected columns: cast, crew, id
            cast_json = parse_json_field(row[0]) if len(row) > 0 else []
            crew_json = parse_json_field(row[1]) if len(row) > 1 else []
            movie_id = int(row[2]) if len(row) > 2 and row[2] else None
            if not movie_id:
                # skip if movie_id missing
                conn.rollback()
                cur.close()
                credit_errors += 1
                print("Skipping credits row with missing movie id")
                continue

            # Insert people and relationships
            insert_people_and_cast(cur, cast_json, movie_id)
            insert_people_and_crew(cur, crew_json, movie_id)

            conn.commit()
            credit_count += 1
            print(
                f"Credits processed for movie {movie_id} ({len(cast_json)} cast, {len(crew_json)} crew)")
        except Exception as e:
            conn.rollback()
            credit_errors += 1
            print(
                f"Error processing credits for movie {row[2] if len(row) > 2 else 'unknown'}: {e}")
        finally:
            cur.close()

print(f"\nCredits processed: {credit_count}, errors: {credit_errors}")
print("=" * 50)


# -------------------------------
# PROCESS RATINGS CSV (ratings)
# -------------------------------
print("Processing Ratings CSV...")
rating_count = 0
rating_errors = 0

with open('data/ratings_small.csv', 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    header = next(reader, None)
    for row in reader:
        cur = get_cursor()
        try:
            # expected columns: user_id, movie_id, rating, timestamp
            user_id = int(row[0])
            movie_id = int(row[1])
            rating = float(row[2])
            ts = parse_timestamp(row[3]) if len(row) > 3 else None

            cur.execute("""
                INSERT INTO ratings (user_id, movie_id, rating, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, movie_id) DO UPDATE
                  SET rating = EXCLUDED.rating,
                      updated_at = CURRENT_TIMESTAMP
            """, (user_id, movie_id, rating, ts))
            conn.commit()
            rating_count += 1
            print(f"Rating inserted for movie {movie_id} (user {user_id})")
        except Exception as e:
            conn.rollback()
            rating_errors += 1
            print(
                f"Error processing rating for movie {row[1] if len(row) > 1 else 'unknown'}: {e}")
        finally:
            cur.close()

print(f"\nRatings processed: {rating_count}, errors: {rating_errors}")
print("=" * 50)

# -------------------------------
# INSERT synthetic USERS
# -------------------------------

def create_synthetic_users_from_ratings():
    """Extract unique user_ids from ratings and create synthetic user records"""
    print("Creating synthetic users from ratings CSV...")
    user_ids = set()

    # First pass: collect all unique user_ids
    with open('data/ratings_small.csv', 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            try:
                user_id = int(row[0])
                user_ids.add(user_id)
            except:
                continue

    print(f"Found {len(user_ids)} unique users in ratings")

    # Insert synthetic users
    cur = get_cursor()
    inserted_count = 0
    error_count = 0

    try:
        for user_id in user_ids:
            try:
                cur.execute("""
                    INSERT INTO users (user_id, is_synthetic)
                    VALUES (%s, TRUE)
                    ON CONFLICT (user_id) DO NOTHING
                """, (user_id,))
                inserted_count += 1
            except Exception as e:
                error_count += 1
                print(f" Error inserting user {user_id}: {e}")

        conn.commit()
        print(
            f" Synthetic users created: {inserted_count} (errors: {error_count})")
    except Exception as e:
        conn.rollback()
        print(f" Error in batch user creation: {e}")
    finally:
        cur.close()

    print("=" * 50)

create_synthetic_users_from_ratings()


# -------------------------------
# Close connection & summary
# -------------------------------
conn.close()
print("\n All data import run finished")
print("=" * 50)
print("Summary:")
print(f"Movies inserted/updated: {movie_count} (errors: {movie_errors})")
print(f"Keywords processed: {keyword_count} (errors: {keyword_errors})")
print(f"Credits processed: {credit_count} (errors: {credit_errors})")
print(f"Ratings processed: {rating_count} (errors: {rating_errors})")
print("=" * 50)