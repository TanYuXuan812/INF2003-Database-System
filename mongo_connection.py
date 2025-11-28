"""
MongoDB Connection Module
Handles MongoDB connection and provides movie data access
"""
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from typing import List, Dict, Any, Optional
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


class MongoDBConnection:
    """MongoDB connection handler for the movie database"""

    def __init__(self):
        self.client = None
        self.db = None
        self.connected = False
        self.connect()

    def connect(self):
        """Establish connection to MongoDB"""
        try:
            # Get MongoDB configuration from .env
            mongo_host = os.getenv('MONGO_HOST', 'localhost')
            mongo_port = int(os.getenv('MONGO_PORT', 27017))
            mongo_db = os.getenv('MONGO_DB', 'moviedb')

            # Create MongoDB connection string (no authentication)
            connection_string = f"mongodb://{mongo_host}:{mongo_port}/"

            # Connect to MongoDB
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)

            # Test connection
            self.client.admin.command('ping')

            # Select database
            self.db = self.client[mongo_db]

            # Initialize collections
            self.movies_collection = self.db.movies

            # Create indexes for better performance
            self._create_indexes()

            self.connected = True
            print(f"[OK] Connected to MongoDB: {mongo_db} at {mongo_host}:{mongo_port}")

        except ConnectionFailure as e:
            print(f"[ERROR] MongoDB Connection Failed: {e}")
            self.connected = False
            raise
        except Exception as e:
            print(f"[ERROR] MongoDB Error: {e}")
            self.connected = False
            raise

    def _create_indexes(self):
        """Create necessary indexes for the movies collection"""
        try:
            # Index on movie ID (unique)
            self.movies_collection.create_index([("id", ASCENDING)], unique=True, background=True)

            # Index on title for searching
            self.movies_collection.create_index([("original_title", ASCENDING)], background=True)

            # Text index for full-text search on title and overview
            self.movies_collection.create_index(
                [("original_title", "text"), ("overview", "text")],
                name="text_search_idx",
                background=True
            )

            # Index on popularity for sorting
            self.movies_collection.create_index([("popularity", DESCENDING)], background=True)

            # Index on release date
            self.movies_collection.create_index([("release_date", DESCENDING)], background=True)

        except Exception as e:
            print(f"Warning: Could not create indexes: {e}")

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.connected = False
            print("[OK] MongoDB connection closed")

    def is_connected(self) -> bool:
        """Check if connected to MongoDB"""
        return self.connected

    # ==================== MOVIE OPERATIONS ====================

    def get_movies(self, limit: int = 100, skip: int = 0) -> List[Dict[str, Any]]:
        """Get all movies with pagination"""
        try:
            # Query movies and filter out invalid records
            movies = list(self.movies_collection.find(
                {"id": {"$type": "number"}},  # Only get documents where id is a number
                {"_id": 0}  # Exclude MongoDB's internal _id field
            ).sort("id", DESCENDING).limit(limit).skip(skip))
            return movies
        except Exception as e:
            print(f"Error fetching movies: {e}")
            return []

    def get_movie_by_id(self, movie_id: int) -> Optional[Dict[str, Any]]:
        """Get a single movie by ID"""
        try:
            # Convert movie_id to int if it's a string
            if isinstance(movie_id, str):
                movie_id = int(movie_id)

            movie = self.movies_collection.find_one(
                {"id": movie_id},
                {"_id": 0}
            )
            return movie
        except Exception as e:
            print(f"Error fetching movie {movie_id}: {e}")
            return None

    def search_movies(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Search movies by title (case-insensitive)"""
        try:
            # Use regex for case-insensitive search
            import re
            regex = re.compile(query, re.IGNORECASE)

            movies = list(self.movies_collection.find(
                {
                    "original_title": {"$regex": regex},
                    "id": {"$type": "number"}  # Ensure id is a number
                },
                {"_id": 0, "id": 1, "original_title": 1, "release_date": 1, "popularity": 1}
            ).sort("original_title", ASCENDING).limit(limit))

            return movies
        except Exception as e:
            print(f"Error searching movies: {e}")
            return []

    def create_movie(self, movie_data: Dict[str, Any]) -> Optional[int]:
        """Create a new movie document"""
        try:
            # Add timestamps
            movie_data['created_at'] = datetime.utcnow()
            movie_data['updated_at'] = datetime.utcnow()

            result = self.movies_collection.insert_one(movie_data)

            if result.inserted_id:
                return movie_data.get('id')
            return None
        except DuplicateKeyError:
            print(f"Movie with ID {movie_data.get('id')} already exists")
            return None
        except Exception as e:
            print(f"Error creating movie: {e}")
            return None

    def update_movie(self, movie_id: int, update_data: Dict[str, Any]) -> bool:
        """Update a movie document"""
        try:
            # Add updated timestamp
            update_data['updated_at'] = datetime.utcnow()

            result = self.movies_collection.update_one(
                {"id": movie_id},
                {"$set": update_data}
            )

            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating movie {movie_id}: {e}")
            return False

    def delete_movie(self, movie_id: int) -> bool:
        """Delete a movie document"""
        try:
            result = self.movies_collection.delete_one({"id": movie_id})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting movie {movie_id}: {e}")
            return False

    def get_movie_count(self) -> int:
        """Get total number of movies"""
        try:
            return self.movies_collection.count_documents({})
        except Exception as e:
            print(f"Error counting movies: {e}")
            return 0

    def get_movies_by_genre(self, genre_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get movies filtered by genre"""
        try:
            movies = list(self.movies_collection.find(
                {"genres.name": genre_name},
                {"_id": 0}
            ).sort("popularity", DESCENDING).limit(limit))
            return movies
        except Exception as e:
            print(f"Error fetching movies by genre: {e}")
            return []

    def get_popular_movies(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get most popular movies"""
        try:
            movies = list(self.movies_collection.find(
                {},
                {"_id": 0}
            ).sort("popularity", DESCENDING).limit(limit))
            return movies
        except Exception as e:
            print(f"Error fetching popular movies: {e}")
            return []


# Singleton instance
_mongo_connection = None


def get_mongo_connection() -> MongoDBConnection:
    """Get or create MongoDB connection singleton"""
    global _mongo_connection
    if _mongo_connection is None:
        _mongo_connection = MongoDBConnection()
    return _mongo_connection


def close_mongo_connection():
    """Close MongoDB connection"""
    global _mongo_connection
    if _mongo_connection:
        _mongo_connection.close()
        _mongo_connection = None


if __name__ == "__main__":
    # Test the connection
    print("Testing MongoDB connection...")
    mongo = get_mongo_connection()

    if mongo.is_connected():
        print(f"Total movies in database: {mongo.get_movie_count()}")

        # Test search
        results = mongo.search_movies("toy", limit=5)
        print(f"\nSearch results for 'toy': {len(results)} movies found")
        for movie in results:
            print(f"  - {movie.get('original_title')} ({movie.get('id')})")

    close_mongo_connection()
