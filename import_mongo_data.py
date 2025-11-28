"""
Import Movie Data into MongoDB
Imports movie data from JSON files in scripts/mongo_seed/ directory
"""
import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


def import_movies_to_mongodb():
    """Import movies from JSON file to MongoDB"""

    # MongoDB connection
    mongo_host = os.getenv('MONGO_HOST', 'localhost')
    mongo_port = int(os.getenv('MONGO_PORT', 27017))
    mongo_db = os.getenv('MONGO_DB', 'moviedb')

    print(f"Connecting to MongoDB at {mongo_host}:{mongo_port}...")

    try:
        client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/", serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✓ Connected to MongoDB successfully!")
    except Exception as e:
        print(f"✗ Failed to connect to MongoDB: {e}")
        return

    db = client[mongo_db]
    movies_collection = db.movies

    # Path to the movies JSON file
    movies_json_path = os.path.join("scripts", "mongo_seed", "movies.json")

    if not os.path.exists(movies_json_path):
        print(f"✗ Movies JSON file not found at: {movies_json_path}")
        print("Please ensure the file exists.")
        return

    print(f"\nReading movies from: {movies_json_path}")

    try:
        with open(movies_json_path, 'r', encoding='utf-8') as f:
            movies_data = json.load(f)

        if not isinstance(movies_data, list):
            print("✗ Invalid JSON format. Expected a list of movies.")
            return

        print(f"Found {len(movies_data)} movies in JSON file")

        # Clear existing movies (optional - comment out if you want to keep existing data)
        print("\nClearing existing movies from MongoDB...")
        movies_collection.delete_many({})

        # Create index on 'id' field for better performance
        print("Creating indexes...")
        movies_collection.create_index([("id", 1)], unique=True)
        movies_collection.create_index([("original_title", 1)])
        movies_collection.create_index([("popularity", -1)])

        # Insert movies in batches
        batch_size = 100
        total_inserted = 0
        failed_inserts = 0

        print(f"\nImporting movies (batch size: {batch_size})...")

        for i in range(0, len(movies_data), batch_size):
            batch = movies_data[i:i + batch_size]

            try:
                result = movies_collection.insert_many(batch, ordered=False)
                total_inserted += len(result.inserted_ids)
                print(f"  Imported batch {i // batch_size + 1}: {len(result.inserted_ids)} movies")
            except Exception as e:
                failed_inserts += batch_size - len(batch)
                print(f"  Warning: Some duplicates or errors in batch {i // batch_size + 1}")

        print(f"\n{'='*60}")
        print(f"Import Summary:")
        print(f"  Total movies in JSON: {len(movies_data)}")
        print(f"  Successfully imported: {total_inserted}")
        print(f"  Failed/Skipped: {failed_inserts}")
        print(f"{'='*60}")

        # Verify import
        count = movies_collection.count_documents({})
        print(f"\nTotal movies in MongoDB: {count}")

        # Show sample movies
        print("\nSample movies:")
        for movie in movies_collection.find().limit(3):
            print(f"  - {movie.get('original_title', 'N/A')} (ID: {movie.get('id', 'N/A')})")

        print("\n✓ MongoDB import completed successfully!")

    except json.JSONDecodeError as e:
        print(f"✗ Error parsing JSON file: {e}")
    except Exception as e:
        print(f"✗ Error during import: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    print("="*60)
    print("MongoDB Movie Data Import Tool")
    print("="*60)
    import_movies_to_mongodb()
