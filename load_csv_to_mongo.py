import os
import ast
import json
import math
from typing import Any, Dict, List, Optional

import pandas as pd
from pymongo import MongoClient, UpdateOne


# ----------------------------
# CONFIG
# ----------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "moviedb")

MOVIES_CSV = os.getenv("MOVIES_CSV", "data/movies_metadata.csv")
CREDITS_CSV = os.getenv("CREDITS_CSV", "data/credits.csv")
KEYWORDS_CSV = os.getenv("KEYWORDS_CSV", "data/keywords.csv")
RATINGS_CSV = os.getenv("RATINGS_CSV", "data/ratings_small.csv")

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))


# ----------------------------
# HELPERS
# ----------------------------
def is_nan(x: Any) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))


def to_int(x: Any) -> Optional[int]:
    if is_nan(x):
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def to_float(x: Any) -> Optional[float]:
    if is_nan(x):
        return None
    try:
        return float(x)
    except Exception:
        return None


def to_bool(x: Any) -> Optional[bool]:
    if is_nan(x):
        return None
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return None


def safe_parse_jsonish(x: Any) -> Any:
    """
    Kaggle TMDB CSV fields often look like:
      "[{'id': 18, 'name': 'Drama'}]"   (python-literal-ish)
    Sometimes true JSON, sometimes not.
    """
    if is_nan(x):
        return None
    if isinstance(x, (dict, list)):
        return x
    s = str(x).strip()
    if s == "" or s.lower() in ("none", "nan"):
        return None

    # try json
    try:
        return json.loads(s)
    except Exception:
        pass

    # try python literal
    try:
        return ast.literal_eval(s)
    except Exception:
        return s  # fallback to raw string


def bulk_upsert(col, docs: List[Dict[str, Any]], key_field: str):
    ops = []
    for d in docs:
        key = d.get(key_field)
        if key is None:
            continue
        ops.append(UpdateOne({key_field: key}, {"$set": d}, upsert=True))
    if ops:
        col.bulk_write(ops, ordered=False)


# ----------------------------
# LOADERS
# ----------------------------
def load_movies(db):
    col = db["movies"]
    col.create_index("id", unique=True)

    print(f"[movies] Loading {MOVIES_CSV}")
    for chunk in pd.read_csv(MOVIES_CSV, chunksize=CHUNK_SIZE, low_memory=False):
        docs = []
        for _, r in chunk.iterrows():
            tmdb_id = to_int(r.get("id"))
            if tmdb_id is None:
                continue

            doc = {
                "id": tmdb_id,
                "imdb_id": None if is_nan(r.get("imdb_id")) else str(r.get("imdb_id")),
                "title": None if is_nan(r.get("title")) else str(r.get("title")),
                "original_title": None if is_nan(r.get("original_title")) else str(r.get("original_title")),
                "original_language": None if is_nan(r.get("original_language")) else str(r.get("original_language")),
                "overview": None if is_nan(r.get("overview")) else str(r.get("overview")),
                "tagline": None if is_nan(r.get("tagline")) else str(r.get("tagline")),
                "homepage": None if is_nan(r.get("homepage")) else str(r.get("homepage")),
                "release_date": None if is_nan(r.get("release_date")) else str(r.get("release_date")),
                "status": None if is_nan(r.get("status")) else str(r.get("status")),
                "adult": to_bool(r.get("adult")),
                "video": to_bool(r.get("video")),
                "budget": to_int(r.get("budget")),
                "revenue": to_int(r.get("revenue")),
                "runtime": to_float(r.get("runtime")),
                "popularity": to_float(r.get("popularity")),
                "vote_average": to_float(r.get("vote_average")),
                "vote_count": to_int(r.get("vote_count")),

                # Embedded/complex fields (parsed)
                "belongs_to_collection": safe_parse_jsonish(r.get("belongs_to_collection")),
                "genres": safe_parse_jsonish(r.get("genres")),
                "production_companies": safe_parse_jsonish(r.get("production_companies")),
                "production_countries": safe_parse_jsonish(r.get("production_countries")),
                "spoken_languages": safe_parse_jsonish(r.get("spoken_languages")),
            }
            docs.append(doc)

        bulk_upsert(col, docs, "id")

    print("[movies] Done.")


def load_credits(db):
    col = db["credits"]
    col.create_index("id", unique=True)

    print(f"[credits] Loading {CREDITS_CSV}")
    for chunk in pd.read_csv(CREDITS_CSV, chunksize=CHUNK_SIZE, low_memory=False):
        docs = []
        for _, r in chunk.iterrows():
            tmdb_id = to_int(r.get("id"))
            if tmdb_id is None:
                continue

            cast_val = safe_parse_jsonish(r.get("cast"))
            crew_val = safe_parse_jsonish(r.get("crew"))

            doc = {
                "id": tmdb_id,
                "cast": cast_val,  # may be list or raw string if parsing failed
                "crew": crew_val,
            }
            docs.append(doc)

        bulk_upsert(col, docs, "id")

    print("[credits] Done.")


def load_keywords(db):
    col = db["keywords"]
    col.create_index("id", unique=True)

    print(f"[keywords] Loading {KEYWORDS_CSV}")
    df = pd.read_csv(KEYWORDS_CSV, low_memory=False)
    docs = []
    for _, r in df.iterrows():
        tmdb_id = to_int(r.get("id"))
        if tmdb_id is None:
            continue
        kws = safe_parse_jsonish(r.get("keywords"))
        docs.append({"id": tmdb_id, "keywords": kws})
    bulk_upsert(col, docs, "id")

    print("[keywords] Done.")


def load_ratings(db):
    col = db["ratings"]
    # Ratings can be huge in other datasets; ratings_small is manageable.
    # We'll drop+insert to avoid duplicates across runs.
    print(f"[ratings] Loading {RATINGS_CSV} (drop then insert)")
    col.drop()
    col.create_index("userId")
    col.create_index("movieId")

    df = pd.read_csv(RATINGS_CSV, low_memory=False)
    batch = []
    inserted = 0

    for _, r in df.iterrows():
        user_id = to_int(r.get("userId"))
        movie_id = to_int(r.get("movieId"))
        rating = to_float(r.get("rating"))
        ts = to_int(r.get("timestamp"))
        if user_id is None or movie_id is None:
            continue

        batch.append({
            "userId": user_id,
            "movieId": movie_id,
            "rating": rating,
            "timestamp": ts,
        })

        if len(batch) >= 5000:
            col.insert_many(batch, ordered=False)
            inserted += len(batch)
            batch = []

    if batch:
        col.insert_many(batch, ordered=False)
        inserted += len(batch)

    print(f"[ratings] Done. Inserted {inserted} docs.")


def create_indexes(db):
    # For benchmark queries
    db["movies"].create_index("original_title")
    db["movies"].create_index("release_date")
    db["movies"].create_index("genres.name")

    # credits cast.name index only helps if cast is stored as real array of objects
    db["credits"].create_index("cast.name")

    print("[indexes] Done.")


def main():
    for f in [MOVIES_CSV, CREDITS_CSV, KEYWORDS_CSV, RATINGS_CSV]:
        if not os.path.exists(f):
            raise FileNotFoundError(f"Missing file in current folder: {f}")

    print(f"Connecting: {MONGO_URI}  DB: {MONGO_DB}")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    load_movies(db)
    load_credits(db)
    load_keywords(db)
    load_ratings(db)
    create_indexes(db)

    print("All done.")


if __name__ == "__main__":
    main()
