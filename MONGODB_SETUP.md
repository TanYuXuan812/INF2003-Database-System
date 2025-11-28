# MongoDB Setup Instructions

## Prerequisites

1. **MongoDB** installed and running on your system
   - Default connection: `localhost:27017`
   - No authentication required (as specified in .env)

2. **Python packages**:
   - `pymongo` (already installed - version 4.6.1)
   - `flask`
   - `psycopg2`

## Configuration

Your `.env` file is already configured with:
```
MONGO_DB=moviedb
MONGO_HOST=localhost
MONGO_PORT=27017
```

## Setup Steps

### 1. Start MongoDB Server

Make sure MongoDB is running on your system:

**Windows:**
```bash
# Start MongoDB service
net start MongoDB
```

**Or start manually:**
```bash
mongod --dbpath "C:\data\db"
```

### 2. Import Movie Data

Run the import script to load movie data into MongoDB:

```bash
python import_mongo_data.py
```

This will:
- Connect to MongoDB at `localhost:27017`
- Create the `moviedb` database
- Import movies from `scripts/mongo_seed/movies.json`
- Create necessary indexes
- Display import summary

### 3. Verify MongoDB Connection

Test the connection:

```bash
python mongo_connection.py
```

This will:
- Test MongoDB connection
- Display total movie count
- Show sample search results

### 4. Run the Flask Application

Start the Flask app:

```bash
python app.py
```

The application will be available at: `http://localhost:5000`

### 5. Switch to MongoDB

1. Open the application in your browser
2. Click on the **Database dropdown** in the navigation bar (shows current database)
3. Select **MongoDB** from the dropdown
4. The page will reload and display MongoDB data

## Features

### Database Switching
- Switch between PostgreSQL and MongoDB seamlessly
- Session-based database selection
- Real-time indicator in navigation bar

### MongoDB Support
The following features work with MongoDB:

✅ **Movies Tab**:
- List all movies
- Search movies by title
- View movie details
- View movie genres (embedded in movie documents)

⚠️ **Limited Support** (PostgreSQL only):
- Create/Edit/Delete movies
- Genres, Companies, People tabs
- Ratings, Movie Cast, Movie Crew tabs

### Data Structure

MongoDB movies use this schema:
```json
{
  "id": 862,
  "adult": false,
  "genres": [
    {
      "id": 16,
      "name": "Animation"
    }
  ],
  "original_language": "en",
  "original_title": "Toy Story",
  "overview": "Led by Woody, Andy's toys...",
  "popularity": 21.946943,
  "poster_path": "https://image.tmdb.org/t/p/w500/...",
  "release_date": "30/10/1995",
  "runtime": 81
}
```

## Troubleshooting

### MongoDB Connection Failed
- Ensure MongoDB is running: `mongo --eval "db.version()"`
- Check the port: default is `27017`
- Verify .env configuration

### No Movies Displayed
- Run the import script: `python import_mongo_data.py`
- Check if movies.json exists in `scripts/mongo_seed/`
- Verify database name in .env matches import script

### Import Script Errors
- Check JSON file format
- Ensure MongoDB is accessible
- Verify sufficient disk space

## MongoDB Commands

Useful MongoDB commands for debugging:

```bash
# Connect to MongoDB shell
mongo

# Switch to moviedb database
use moviedb

# Count movies
db.movies.countDocuments()

# Find a movie
db.movies.findOne({"id": 862})

# List all indexes
db.movies.getIndexes()

# Drop the database (if you want to start over)
db.dropDatabase()
```

## Architecture

### Files Added/Modified

**New Files:**
- `mongo_connection.py` - MongoDB connection handler
- `import_mongo_data.py` - Data import script
- `MONGODB_SETUP.md` - This file

**Modified Files:**
- `app.py` - Added MongoDB support for movies routes
  - `list_movies()` - Supports both databases
  - `view_movie()` - Supports both databases
  - `is_using_mongodb()` - Helper function
  - `get_mongo_db()` - Get MongoDB connection

**Unchanged:**
- `admin_query.py` - PostgreSQL functions
- `db_adapter.py` - Database adapter pattern (not currently used)
- Templates - Work with both databases

## Next Steps

To add full MongoDB CRUD support:

1. Extend `mongo_connection.py` with create/update/delete methods
2. Update app.py routes for create/edit/delete movies
3. Add MongoDB collections for genres, companies, people
4. Implement relationship handling for MongoDB

## Performance Notes

- MongoDB uses embedded genres (no joins needed)
- Indexes created for:
  - Movie ID (unique)
  - Title (search)
  - Full-text search (title + overview)
  - Popularity (sorting)
  - Release date (filtering)

## Data Sources

Movie data source: `scripts/mongo_seed/movies.json`
- Contains movie metadata from TMDB
- Includes embedded genre information
- Pre-formatted for MongoDB import
