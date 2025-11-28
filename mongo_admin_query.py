"""
MongoDB Admin CRUD Operations
Complete CRUD layer matching admin_query.py functionality
Adapted for MongoDB document structure
"""

from pymongo import MongoClient
from typing import Optional, Dict, Any, List
from datetime import datetime
import re
import ast
import json

# =======================================
# MongoDB Connection
# =======================================
client = MongoClient("mongodb://localhost:27017/")
db = client["moviedb"]

movies_col = db["movies"]
credits_col = db["credits"]
keywords_col = db["keywords"]
ratings_col = db["ratings"]
users_col = db["users"]

# =======================================
# DATA CONVERSION UTILITIES
# =======================================

def parse_json_string(json_str: str) -> List[Dict]:
    """
    Convert JSON string to Python list/dict.
    Handles both single quotes (Python) and double quotes (JSON).
    """
    if not json_str or not isinstance(json_str, str):
        return []
    
    try:
        # Try ast.literal_eval first (for Python-style strings with single quotes)
        return ast.literal_eval(json_str)
    except:
        try:
            # Try json.loads (for proper JSON with double quotes)
            return json.loads(json_str)
        except:
            return []


def convert_credits_strings_to_arrays():
    """
    ONE-TIME CONVERSION: Convert all cast/crew string fields to proper arrays.
    Run this once to fix your database.
    """
    print("Converting credits collection from strings to arrays...")
    
    credits = credits_col.find({})
    converted_count = 0
    
    for credit in credits:
        updates = {}
        
        # Convert cast if it's a string
        if "cast" in credit and isinstance(credit["cast"], str):
            cast_array = parse_json_string(credit["cast"])
            updates["cast"] = cast_array
        
        # Convert crew if it's a string
        if "crew" in credit and isinstance(credit["crew"], str):
            crew_array = parse_json_string(credit["crew"])
            updates["crew"] = crew_array
        
        # Update document if we have conversions
        if updates:
            credits_col.update_one(
                {"_id": credit["_id"]},
                {"$set": updates}
            )
            converted_count += 1
    
    print(f"Converted {converted_count} documents")
    return converted_count


def convert_keywords_strings_to_arrays():
    """
    ONE-TIME CONVERSION: Convert keywords string fields to proper arrays.
    """
    print("Converting keywords collection from strings to arrays...")
    
    keywords = keywords_col.find({})
    converted_count = 0
    
    for keyword_doc in keywords:
        if "keywords" in keyword_doc and isinstance(keyword_doc["keywords"], str):
            keywords_array = parse_json_string(keyword_doc["keywords"])
            keywords_col.update_one(
                {"_id": keyword_doc["_id"]},
                {"$set": {"keywords": keywords_array}}
            )
            converted_count += 1
    
    print(f"Converted {converted_count} documents")
    return converted_count


# =========================================================
# MOVIES CRUD
# =========================================================

def admin_create_movie(movie_data: Dict[str, Any]) -> int:
    """
    Create a new movie document.
    Returns the movie id.
    """
    if "id" not in movie_data:
        raise ValueError("Movie must have an 'id' field")
    
    # Add timestamps
    movie_data["created_at"] = datetime.utcnow()
    movie_data["updated_at"] = datetime.utcnow()
    
    result = movies_col.insert_one(movie_data)
    return movie_data["id"]


def admin_get_movie(movie_id: int) -> Optional[Dict[str, Any]]:
    """
    Get movie by ID. Returns dict with movie details.
    """
    movie = movies_col.find_one({"id": movie_id}, {"_id": 0})
    return movie


def admin_search_movies_by_title(title: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Search movies by title (fuzzy match).
    Returns list of basic movie dicts.
    """
    results = movies_col.find(
        {"original_title": {"$regex": title, "$options": "i"}},
        {"_id": 0, "id": 1, "original_title": 1, "release_date": 1}
    ).limit(limit).sort("original_title", 1)
    
    return [
        {
            "movie_id": r["id"],
            "title": r.get("original_title", ""),
            "released_date": r.get("release_date")
        }
        for r in results
    ]


def admin_update_movie(movie_id: int, update_data: Dict[str, Any]) -> Optional[int]:
    """
    Update movie fields specified in update_data dict.
    Returns the movie_id if updated, else None.
    """
    if not update_data:
        return None
    
    # Add updated timestamp
    update_data["updated_at"] = datetime.utcnow()
    
    result = movies_col.update_one(
        {"id": movie_id},
        {"$set": update_data}
    )
    
    return movie_id if result.modified_count > 0 else None


def admin_delete_movie(movie_id: int) -> Optional[int]:
    """
    Delete a movie and its related data (credits, keywords, ratings).
    Returns deleted movie_id or None.
    """
    # Delete related data
    credits_col.delete_one({"id": movie_id})
    keywords_col.delete_one({"id": movie_id})
    ratings_col.delete_many({"movieId": movie_id})
    
    # Delete movie
    result = movies_col.delete_one({"id": movie_id})
    return movie_id if result.deleted_count > 0 else None


# =========================================================
# GENRES CRUD (Embedded in Movies)
# =========================================================

def admin_create_genre(movie_id: int, genre_data: Dict[str, Any]) -> Optional[int]:
    """
    Add a genre to a movie. Returns genre_id if created, None if already exists.
    """
    if "id" not in genre_data or "name" not in genre_data:
        raise ValueError("Genre must have 'id' and 'name' fields")
    
    # Check if genre already exists in movie
    movie = movies_col.find_one({"id": movie_id, "genres.id": genre_data["id"]})
    if movie:
        return None
    
    result = movies_col.update_one(
        {"id": movie_id},
        {"$push": {"genres": genre_data}}
    )
    
    return genre_data["id"] if result.modified_count > 0 else None


def admin_get_genre(movie_id: int, genre_id: int) -> Optional[Dict[str, Any]]:
    """
    Get genre by ID from a specific movie.
    """
    movie_id_int = None
    genre_id_int = None
    try:
        movie_id_int = int(movie_id)
    except Exception:
        pass
    try:
        genre_id_int = int(genre_id)
    except Exception:
        pass

    movie = movies_col.find_one(
        {
            "id": {"$in": [movie_id_int, movie_id, str(movie_id)]},
            "genres.id": {"$in": [genre_id_int, genre_id, str(genre_id)]}
        },
        {"_id": 0, "genres.$": 1}
    )
    
    if movie and "genres" in movie and len(movie["genres"]) > 0:
        genre = movie["genres"][0]
        return {"genre_id": genre["id"], "genre_name": genre["name"]}
    return None


def admin_read_genres(movie_id: int) -> List[Dict[str, Any]]:
    """
    Get all genres for a movie.
    """
    movie = movies_col.find_one({"id": movie_id}, {"_id": 0, "genres": 1})

    if not movie or "genres" not in movie:
        return []

    genres = movie.get("genres", [])
    if not isinstance(genres, list):
        return []

    result = []
    for g in genres:
        if isinstance(g, dict) and "id" in g and "name" in g:
            result.append({"genre_id": g["id"], "genre_name": g["name"]})
    return result


def admin_update_genre(movie_id: int, genre_id: int, new_name: str) -> Optional[int]:
    """
    Update genre name in a movie. Returns genre_id if updated, None if not found.
    """
    if not new_name or not new_name.strip():
        raise ValueError("Genre name cannot be empty")

    movie_id_int = None
    genre_id_int = None
    try:
        movie_id_int = int(movie_id)
    except Exception:
        pass
    try:
        genre_id_int = int(genre_id)
    except Exception:
        pass
    
    result = movies_col.update_one(
        {"id": {"$in": [movie_id_int, movie_id, str(movie_id)]}},
        {"$set": {"genres.$[g].name": new_name.strip()}},
        array_filters=[{"g.id": {"$in": [genre_id_int, genre_id, str(genre_id)]}}]
    )
    
    return genre_id if result.modified_count > 0 else None


def admin_delete_genre(movie_id: int, genre_id: int) -> Optional[int]:
    """
    Remove genre from a movie. Returns genre_id if deleted, None if not found.
    """
    movie_id_int = None
    genre_id_int = None
    try:
        movie_id_int = int(movie_id)
    except Exception:
        pass
    try:
        genre_id_int = int(genre_id)
    except Exception:
        pass

    result = movies_col.update_one(
        {"id": {"$in": [movie_id_int, movie_id, str(movie_id)]}},
        {"$pull": {"genres": {"id": {"$in": [genre_id_int, genre_id, str(genre_id)]}}}}
    )
    
    return genre_id if result.modified_count > 0 else None

def mongo_admin_get_all_genres() -> List[Dict[str, Any]]:
    """
    Get all unique genres across all movies.
    Returns list of unique genres sorted by name.
    Includes a sample movie_id to enable view/edit links in the UI.
    """
    pipeline = [
        {"$unwind": "$genres"},
        {"$match": {
            "genres.id": {"$exists": True, "$ne": None},
            "genres.name": {"$exists": True, "$ne": None, "$ne": ""}
        }},
        {"$group": {
            "_id": "$genres.id",
            "name": {"$first": "$genres.name"},
            "movie_id": {"$first": "$id"}
        }},
        {"$sort": {"name": 1}},
        {"$project": {
            "_id": 0,
            "genre_id": "$_id",
            "genre_name": "$name",
            "movie_id": "$movie_id"
        }}
    ]
    
    results = movies_col.aggregate(pipeline)
    return list(results)

# =========================================================
# PRODUCTION COMPANIES CRUD (Embedded in Movies)
# =========================================================

def admin_create_company(movie_id: int, company_data: Dict[str, Any]) -> Optional[int]:
    """
    Add a production company to a movie.
    Returns company_id if created, None if already exists.
    """
    if "id" not in company_data or "name" not in company_data:
        raise ValueError("Company must have 'id' and 'name' fields")
    
    # Check if company already exists in movie
    movie = movies_col.find_one({"id": movie_id, "production_companies.id": company_data["id"]})
    if movie:
        return None
    
    result = movies_col.update_one(
        {"id": movie_id},
        {"$push": {"production_companies": company_data}}
    )
    
    return company_data["id"] if result.modified_count > 0 else None


def admin_get_company(movie_id: int, company_id: int) -> Optional[Dict[str, Any]]:
    """
    Get company by ID from a specific movie.

    NOTE: production_companies is stored as a stringified list in MongoDB,
    so we need to parse it manually.
    """
    import ast

    movie = movies_col.find_one(
        {"id": movie_id},
        {"_id": 0, "production_companies": 1}
    )

    if not movie or "production_companies" not in movie:
        return None

    companies_str = movie.get("production_companies", "")

    # Handle empty or null values
    if not companies_str or companies_str == "[]":
        return None

    # If it's already a list (shouldn't happen but just in case)
    if isinstance(companies_str, list):
        for company in companies_str:
            if isinstance(company, dict) and company.get("id") == company_id:
                return {"company_id": company["id"], "name": company["name"]}
        return None

    # Parse stringified list
    try:
        companies = ast.literal_eval(companies_str)
        if not isinstance(companies, list):
            return None

        for company in companies:
            if isinstance(company, dict) and company.get("id") == company_id:
                return {"company_id": company["id"], "name": company["name"]}

        return None
    except (ValueError, SyntaxError):
        return None


def admin_read_companies(movie_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Get all production companies. If movie_id provided, returns companies for that movie.

    NOTE: production_companies is stored as a stringified list in MongoDB,
    so we need to parse it manually.
    """
    import ast

    if movie_id is not None:
        movie = movies_col.find_one({"id": movie_id}, {"_id": 0, "production_companies": 1})

        if not movie or "production_companies" not in movie:
            return []

        companies_str = movie.get("production_companies", "")

        # Handle empty or null values
        if not companies_str or companies_str == "[]":
            return []

        # If it's already a list (shouldn't happen but just in case)
        if isinstance(companies_str, list):
            result = []
            for c in companies_str:
                if isinstance(c, dict) and "id" in c and "name" in c:
                    result.append({"company_id": c["id"], "name": c["name"]})
            return result

        # Parse stringified list
        try:
            companies = ast.literal_eval(companies_str)
            if not isinstance(companies, list):
                return []

            result = []
            for c in companies:
                if isinstance(c, dict) and "id" in c and "name" in c:
                    result.append({"company_id": c["id"], "name": c["name"]})
            return result
        except (ValueError, SyntaxError):
            return []
    else:
        # Get all unique companies - use the dedicated function
        all_companies = mongo_admin_get_all_companies()
        return [{"company_id": c["company_id"], "name": c["name"]} for c in all_companies]


def admin_update_company(movie_id: int, company_id: int, new_name: str) -> Optional[int]:
    """
    Update company name in a movie. Returns company_id if updated, None if not found.

    NOTE: production_companies is stored as a stringified list in MongoDB,
    so we need to parse it, update it, and stringify it back.
    """
    import ast

    if not new_name or not new_name.strip():
        raise ValueError("Company name cannot be empty")

    # Get the movie
    movie = movies_col.find_one({"id": movie_id}, {"_id": 0, "production_companies": 1})

    if not movie or "production_companies" not in movie:
        return None

    companies_str = movie.get("production_companies", "")

    # Handle empty or null values
    if not companies_str or companies_str == "[]":
        return None

    # Parse stringified list
    try:
        companies = ast.literal_eval(companies_str)
        if not isinstance(companies, list):
            return None

        # Find and update the company
        updated = False
        for company in companies:
            if isinstance(company, dict) and company.get("id") == company_id:
                company["name"] = new_name.strip()
                updated = True
                break

        if not updated:
            return None

        # Convert back to string and save
        updated_companies_str = str(companies)
        result = movies_col.update_one(
            {"id": movie_id},
            {"$set": {"production_companies": updated_companies_str}}
        )

        return company_id if result.modified_count > 0 else None
    except (ValueError, SyntaxError):
        return None


def admin_delete_company(movie_id: int, company_id: int) -> Optional[int]:
    """
    Remove company from a movie. Returns company_id if deleted, None if not found.

    NOTE: production_companies is stored as a stringified list in MongoDB,
    so we need to parse it, remove the company, and stringify it back.
    """
    import ast

    # Get the movie
    movie = movies_col.find_one({"id": movie_id}, {"_id": 0, "production_companies": 1})

    if not movie or "production_companies" not in movie:
        return None

    companies_str = movie.get("production_companies", "")

    # Handle empty or null values
    if not companies_str or companies_str == "[]":
        return None

    # Parse stringified list
    try:
        companies = ast.literal_eval(companies_str)
        if not isinstance(companies, list):
            return None

        # Find and remove the company
        original_length = len(companies)
        companies = [c for c in companies if not (isinstance(c, dict) and c.get("id") == company_id)]

        # Check if anything was removed
        if len(companies) == original_length:
            return None

        # Convert back to string and save
        updated_companies_str = str(companies)
        result = movies_col.update_one(
            {"id": movie_id},
            {"$set": {"production_companies": updated_companies_str}}
        )

        return company_id if result.modified_count > 0 else None
    except (ValueError, SyntaxError):
        return None


def mongo_admin_get_all_companies() -> List[Dict[str, Any]]:
    """
    Get all unique production companies across all movies.
    Returns list of unique companies sorted by name.
    Includes a sample movie_id to enable view/edit links in the UI.

    NOTE: production_companies is stored as a stringified list in MongoDB,
    so we need to parse it manually.
    """
    import ast

    # Get all movies with production_companies field
    movies = movies_col.find(
        {"production_companies": {"$exists": True, "$ne": None, "$ne": ""}},
        {"_id": 0, "id": 1, "production_companies": 1}
    )

    # Build unique companies dictionary
    companies_dict = {}

    for movie in movies:
        companies_data = movie.get("production_companies", "")
        if not companies_data or companies_data == "[]":
            continue

        # Handle both stringified and actual array formats
        companies_list = None

        # If it's already a list (new format from admin_create_company)
        if isinstance(companies_data, list):
            companies_list = companies_data
        # If it's a string (old format in database)
        elif isinstance(companies_data, str):
            try:
                # Parse the stringified list
                companies_list = ast.literal_eval(companies_data)
            except (ValueError, SyntaxError):
                # Skip malformed data
                continue

        if not isinstance(companies_list, list):
            continue

        for company in companies_list:
            if isinstance(company, dict) and "id" in company and "name" in company:
                company_id = company["id"]
                company_name = company["name"]

                # Store first occurrence (for movie_id reference)
                if company_id not in companies_dict:
                    companies_dict[company_id] = {
                        "company_id": company_id,
                        "name": company_name,
                        "movie_id": movie["id"]
                    }

    # Convert to list and sort by name
    companies_list = list(companies_dict.values())
    companies_list.sort(key=lambda x: x["name"])

    # Limit to 100 companies
    return companies_list[:100]


def admin_search_companies_by_name(name: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Search unique production companies by name (case-insensitive) across all movies.
    Handles both list and stringified production_companies formats.
    """
    import ast
    if not name:
        return mongo_admin_get_all_companies()

    name_lower = name.lower()
    companies_dict = {}

    movies = movies_col.find(
        {"production_companies": {"$exists": True, "$ne": None, "$ne": ""}},
        {"_id": 0, "id": 1, "production_companies": 1}
    )

    for movie in movies:
        companies_data = movie.get("production_companies", "")
        if not companies_data or companies_data == "[]":
            continue

        companies_list = None
        if isinstance(companies_data, list):
            companies_list = companies_data
        elif isinstance(companies_data, str):
            try:
                companies_list = ast.literal_eval(companies_data)
            except (ValueError, SyntaxError):
                continue

        if not isinstance(companies_list, list):
            continue

        for company in companies_list:
            if isinstance(company, dict) and "id" in company and "name" in company:
                company_id = company["id"]
                company_name = company["name"]
                if company_name and name_lower in company_name.lower():
                    if company_id not in companies_dict:
                        companies_dict[company_id] = {
                            "company_id": company_id,
                            "name": company_name,
                            "movie_id": movie["id"]
                        }

    companies_list = list(companies_dict.values())
    companies_list.sort(key=lambda x: x["name"])
    return companies_list[:limit]


# =========================================================
# RATINGS CRUD
# =========================================================

def admin_create_rating(rating_data: Dict[str, Any]) -> str:
    """
    Create a new rating. Returns the inserted MongoDB _id as string.
    """
    if "userId" not in rating_data or "movieId" not in rating_data or "rating" not in rating_data:
        raise ValueError("Rating must have userId, movieId, and rating fields")
    
    # Validate rating range
    if rating_data["rating"] < 0 or rating_data["rating"] > 10:
        raise ValueError("Rating must be between 0 and 10")
    
    rating_data["created_at"] = datetime.utcnow()
    rating_data["updated_at"] = datetime.utcnow()
    
    result = ratings_col.insert_one(rating_data)
    return str(result.inserted_id)


def admin_get_rating(user_id: int, movie_id: int) -> Optional[Dict[str, Any]]:
    """
    Get rating by user_id and movie_id.
    """
    rating = ratings_col.find_one(
        {"userId": user_id, "movieId": movie_id},
        {"_id": 0}
    )
    return rating


def admin_read_ratings(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Get ratings with pagination. Returns list of rating dicts.
    """
    results = ratings_col.find(
        {},
        {"_id": 0}
    ).sort("timestamp", -1).skip(offset).limit(limit)
    
    return list(results)


def admin_update_rating(user_id: int, movie_id: int, new_rating: float) -> Optional[int]:
    """
    Update rating value. Returns 1 if updated, None if not found.
    """
    if new_rating < 0 or new_rating > 10:
        raise ValueError("Rating must be between 0 and 10")
    
    result = ratings_col.update_one(
        {"userId": user_id, "movieId": movie_id},
        {"$set": {"rating": new_rating, "updated_at": datetime.utcnow()}}
    )
    
    return 1 if result.modified_count > 0 else None


def admin_delete_rating(user_id: int, movie_id: int) -> Optional[int]:
    """
    Delete rating. Returns 1 if deleted, None if not found.
    """
    result = ratings_col.delete_one({"userId": user_id, "movieId": movie_id})
    return 1 if result.deleted_count > 0 else None


# =========================================================
# CAST CRUD - FIXED TO WORK WITH PROPER ARRAYS
# =========================================================

def mongo_admin_create_person_cast(movie_id: int, cast_data: Dict[str, Any]) -> Optional[int]:
    """
    Add a cast member to a movie's credits.
    NOW WORKS - requires cast to be a proper array (use convert_credits_strings_to_arrays first).
    """
    required_fields = ["id", "name"]
    if not all(field in cast_data for field in required_fields):
        raise ValueError("Cast member must have 'id' and 'name' fields")
    
    # Check if cast member already exists
    credits = credits_col.find_one({"id": movie_id, "cast.id": cast_data["id"]})
    if credits:
        return None
    
    result = credits_col.update_one(
        {"id": movie_id},
        {"$push": {"cast": cast_data}},
        upsert=True
    )
    
    return cast_data["id"] if result.modified_count > 0 or result.upserted_id else None


def mongo_admin_get_person_cast(movie_id: int, person_id: int) -> Optional[Dict[str, Any]]:
    """Get cast member by person_id from a specific movie."""
    credits = credits_col.find_one(
        {"id": movie_id, "cast.id": person_id},
        {"_id": 0, "cast.$": 1}
    )
    
    if credits and "cast" in credits and len(credits["cast"]) > 0:
        return credits["cast"][0]
    return None

    
def mongo_admin_read_movie_cast(movie_id: int) -> List[Dict[str, Any]]:
    """Get all cast members for a movie."""
    credits = credits_col.find_one({"id": movie_id}, {"_id": 0, "cast": 1})
    
    if not credits or "cast" not in credits:
        return []
    
    cast = credits.get("cast", [])
    
    # If cast is still a string (not converted yet), return empty
    if isinstance(cast, str):
        print(f"WARNING: Cast for movie {movie_id} is still a string. Run convert_credits_strings_to_arrays().")
        return []
    
    return cast


def mongo_admin_update_person_cast(movie_id: int, person_id: int, update_data: Dict[str, Any]) -> Optional[int]:
    """Update cast member details."""
    if not update_data:
        return None
    
    # Build update fields with positional operator
    set_fields = {f"cast.$.{k}": v for k, v in update_data.items()}
    
    result = credits_col.update_one(
        {"id": movie_id, "cast.id": person_id},
        {"$set": set_fields}
    )
    
    return person_id if result.modified_count > 0 else None


def mongo_admin_delete_person_cast(movie_id: int, person_id: int) -> Optional[int]:
    """Remove cast member from a movie."""
    result = credits_col.update_one(
        {"id": movie_id},
        {"$pull": {"cast": {"id": person_id}}}
    )
    
    return person_id if result.modified_count > 0 else None


# =========================================================
# CREW CRUD - FIXED
# =========================================================

def mongo_admin_create_person_crew(movie_id: int, crew_data: Dict[str, Any]) -> Optional[int]:
    """Add a crew member to a movie's credits."""
    required_fields = ["id", "name", "job"]
    if not all(field in crew_data for field in required_fields):
        raise ValueError("Crew member must have 'id', 'name', and 'job' fields")
    
    # Check if crew member with same id and job already exists
    credits = credits_col.find_one({
        "id": movie_id,
        "crew": {"$elemMatch": {"id": crew_data["id"], "job": crew_data["job"]}}
    })
    
    if credits:
        return None
    
    result = credits_col.update_one(
        {"id": movie_id},
        {"$push": {"crew": crew_data}},
        upsert=True
    )
    
    return crew_data["id"] if result.modified_count > 0 or result.upserted_id else None


def mongo_admin_get_person_crew(movie_id: int, person_id: int, job: str) -> Optional[Dict[str, Any]]:
    """Get crew member by person_id and job from a specific movie."""
    credits = credits_col.find_one(
        {"id": movie_id, "crew": {"$elemMatch": {"id": person_id, "job": job}}},
        {"_id": 0, "crew.$": 1}
    )
    
    if credits and "crew" in credits and len(credits["crew"]) > 0:
        return credits["crew"][0]
    return None


def mongo_admin_read_movie_crew(movie_id: int) -> List[Dict[str, Any]]:
    """Get all crew members for a movie."""
    credits = credits_col.find_one({"id": movie_id}, {"_id": 0, "crew": 1})
    
    if not credits or "crew" not in credits:
        return []
    
    crew = credits.get("crew", [])
    
    # If crew is still a string, return empty
    if isinstance(crew, str):
        print(f"WARNING: Crew for movie {movie_id} is still a string. Run convert_credits_strings_to_arrays().")
        return []
    
    return crew


def mongo_admin_update_person_crew(movie_id: int, person_id: int, job: str, update_data: Dict[str, Any]) -> Optional[int]:
    """Update crew member details."""
    if not update_data:
        return None
    
    # Find the array index first
    credits = credits_col.find_one({"id": movie_id})
    if not credits or "crew" not in credits:
        return None
    
    crew = credits["crew"]
    if isinstance(crew, str):
        print(f"WARNING: Crew for movie {movie_id} is still a string. Run convert_credits_strings_to_arrays().")
        return None
    
    crew_index = None
    for idx, member in enumerate(crew):
        if member.get("id") == person_id and member.get("job") == job:
            crew_index = idx
            break
    
    if crew_index is None:
        return None
    
    # Update specific array element
    set_fields = {f"crew.{crew_index}.{k}": v for k, v in update_data.items()}
    
    result = credits_col.update_one(
        {"id": movie_id},
        {"$set": set_fields}
    )
    
    return person_id if result.modified_count > 0 else None


def mongo_admin_delete_person_crew(movie_id: int, person_id: int, job: str) -> Optional[int]:
    """Remove crew member from a movie."""
    result = credits_col.update_one(
        {"id": movie_id},
        {"$pull": {"crew": {"id": person_id, "job": job}}}
    )
    
    return person_id if result.modified_count > 0 else None

# =========================================================
# USERS CRUD
# =========================================================

def admin_create_user(user_data: Dict[str, Any]) -> int:
    """
    Create a new user. Returns user_id.
    """
    if "userId" not in user_data:
        raise ValueError("User must have 'userId' field")
    
    user_data["created_at"] = datetime.utcnow()
    
    result = users_col.insert_one(user_data)
    return user_data["userId"]


def admin_get_user(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Get user by ID.
    """
    return users_col.find_one({"userId": user_id}, {"_id": 0})


def admin_update_user(user_id: int, update_data: Dict[str, Any]) -> Optional[int]:
    """
    Update user fields. Returns user_id if updated, None if not found.
    """
    if not update_data:
        return None
    
    result = users_col.update_one(
        {"userId": user_id},
        {"$set": update_data}
    )
    
    return user_id if result.modified_count > 0 else None


def admin_delete_user(user_id: int) -> Optional[int]:
    """
    Delete user and their ratings. Returns user_id if deleted, None if not found.
    """
    # Delete user's ratings
    ratings_col.delete_many({"userId": user_id})
    
    # Delete user
    result = users_col.delete_one({"userId": user_id})
    return user_id if result.deleted_count > 0 else None


# =========================================================
# TESTING
# =========================================================

# if __name__ == "__main__":
#     print("\n=== TESTING MONGODB ADMIN CRUD ===")
    
#     test_movie_id = 862  # Toy Story
    
#     print("\n--- TEST: Movie Operations ---")
#     movie = admin_get_movie(test_movie_id)
#     if movie:
#         print(f"Found movie: {movie.get('original_title')}")
    
#     print("\n--- TEST: Search Movies ---")
#     results = admin_search_movies_by_title("Toy", limit=3)
#     for r in results:
#         print(f"  {r['movie_id']}: {r['title']}")
    
#     print("\n--- TEST: Genre Operations ---")
#     print(f"Add genre: {admin_create_genre(test_movie_id, {'id': 999, 'name': 'Test Genre'})}")
#     genres = admin_read_genres(test_movie_id)
#     print(f"Movie has {len(genres)} genres")
#     print(f"Delete genre: {admin_delete_genre(test_movie_id, 999)}")
    
#     print("\n--- TEST: Cast Operations ---")
#     cast_members = admin_read_movie_cast(test_movie_id)
#     print(f"Movie has {len(cast_members)} cast members")
#     if cast_members:
#         print(f"First cast: {cast_members[0].get('name')} as {cast_members[0].get('character')}")
    
#     print("\n--- TEST: Crew Operations ---")
#     crew_members = admin_read_movie_crew(test_movie_id)
#     print(f"Movie has {len(crew_members)} crew members")
#     if crew_members:
#         print(f"First crew: {crew_members[0].get('name')} - {crew_members[0].get('job')}")
    
#     print("\n--- TESTING COMPLETED ---")


# if __name__ == "__main__":
#     convert_credits_strings_to_arrays()
#     convert_keywords_strings_to_arrays()

