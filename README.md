# Movie Database Management System

A comprehensive movie database application with dual-database architecture supporting both PostgreSQL and MongoDB. Built for INF2003 Database Systems module, this project demonstrates advanced database operations, performance benchmarking, and full-stack web development.

## Features

### Core Functionality
- **Dual Database Support**: Seamlessly switch between PostgreSQL (relational) and MongoDB (NoSQL)
- **CRUD Operations**: Complete Create, Read, Update, Delete functionality for movies, genres, companies, cast, crew, and ratings
- **Advanced Search**: Multi-filter search with title, genre, actor, year range, and sorting options
- **Performance Benchmark**: Real-time comparison of PostgreSQL vs MongoDB query performance
- **User & Admin Interfaces**: Separate interfaces for end-users and administrators

### Advanced Features
- Database-agnostic search architecture with unified helper functions
- Dynamic query construction for complex multi-table joins
- Rating aggregation with LEFT JOIN to display all movies
- Responsive Bootstrap 5 UI with modern design
- Real-time database switching without application restart

## Technology Stack

**Backend:**
- Python 3.12
- Flask (Web Framework)
- psycopg2 (PostgreSQL Driver)
- PyMongo (MongoDB Driver)

**Frontend:**
- HTML5, CSS3, JavaScript
- Bootstrap 5
- Bootstrap Icons
- Jinja2 Templating

**Databases:**
- PostgreSQL 14+
- MongoDB 6.0+

## Prerequisites

- Python 3.12 or higher
- PostgreSQL 14+ installed and running
- MongoDB 6.0+ installed and running
- pip (Python package manager)

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/INF2003-Database-System.git
cd INF2003-Database-System
```

### 2. Create Virtual Environment
```bash
python -m venv venv314

# Windows
venv314\Scripts\activate

# macOS/Linux
source venv314/bin/activate
```

### 3. Install Dependencies
```bash
pip install flask psycopg2 pymongo
```

### 4. Database Setup

#### PostgreSQL Setup
```bash
# Create database
createdb moviedb

# Run schema
psql -d moviedb -f scripts/schema.sql

# Import data (if using import script)
python import.py
```

#### MongoDB Setup
```bash
# MongoDB will auto-create database on first connection
# Import data using the import script
python import.py
```

### 5. Configure Database Connections

Update connection settings in `app.py`:

```python
# PostgreSQL Configuration
PG_CONFIG = {
    "host": "localhost",
    "database": "moviedb",
    "user": "postgres",
    "password": "your_password"
}

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "moviedb"
```

## Running the Application

### Start the Flask Server
```bash
python app.py
```

The application will be available at: `http://127.0.0.1:5000`

### Default Routes
- **Home (Admin)**: `http://127.0.0.1:5000/`
- **User Interface**: `http://127.0.0.1:5000/user`
- **Advanced Search**: `http://127.0.0.1:5000/combined-search`
- **Benchmark**: `http://127.0.0.1:5000/benchmark`

## Project Structure

```
Database Files/
├── app.py                          # Main Flask application
├── admin_query.py                  # PostgreSQL admin queries
├── mongo_admin_query.py            # MongoDB admin queries
├── user_query.py                   # User-facing queries
├── import.py                       # Data import script
├── requirements.txt                # Python dependencies
│
├── scripts/
│   └── schema.sql                  # PostgreSQL database schema
│
├── data/                           # CSV data files
│   ├── credits.csv
│   ├── keywords.csv
│   ├── movies_metadata.csv
│   └── ratings_small.csv
│
└── templates/                      # HTML templates
    ├── base.html                   # Base template with navigation
    ├── combined_search.html        # Advanced search interface
    ├── benchmark.html              # Performance benchmark page
    └── user_queries/               # User interface templates
```

## Key Features Explained

### Advanced Search
The Advanced Search feature provides comprehensive movie discovery with:
- **Multiple Filters**: Title (partial match), Genre, Actor, Year Range
- **Dual Sorting**: By Popularity or Rating
- **Database-Agnostic**: Identical functionality across PostgreSQL and MongoDB
- **Complete Results**: Shows all movies including those without ratings

**Implementation Highlights:**
- Helper functions `search_postgres()` and `search_mongo()` with identical parameters
- Direct LEFT JOIN on movie_id (no intermediate bridge tables)
- Dynamic SQL/aggregation pipeline construction based on active filters

### Performance Benchmark
Real-time performance comparison between PostgreSQL and MongoDB:
- **Fair Testing**: Uses identical search logic for both databases
- **High-Resolution Timing**: Python's `time.perf_counter()` for accurate measurements
- **Observable Metrics**: Response time comparison and percentage differences
- **Educational Value**: Demonstrates practical performance differences for relational vs NoSQL

**Typical Results:**
- PostgreSQL: ~100-150ms (optimized joins)
- MongoDB: ~800-1200ms (aggregation pipeline)
- PostgreSQL shows ~10x performance advantage for this relational schema

### Database Switching
Toggle between PostgreSQL and MongoDB through the UI:
- Click database badge in navigation bar
- Select desired database (PostgreSQL/MongoDB)
- Application maintains all functionality across both databases
- Session-based database preference

## Database Schema

### PostgreSQL (Normalized Schema)
- `movies` - Core movie information
- `genres` - Genre master list
- `movie_genres` - Many-to-many relationship
- `companies` - Production companies
- `movie_companies` - Many-to-many relationship
- `people` - Cast and crew members
- `movie_casts` - Movie-actor relationships
- `movie_crews` - Movie-crew relationships
- `ratings` - User ratings

### MongoDB (Document-Oriented)
- `movies` - Embedded genres, keywords, companies
- `credits` - Cast and crew information
- `ratings` - User ratings collection

## Development Team

Group 7 - INF2003 Database Systems
- [Team Member Names]

## License

This project is developed for educational purposes as part of the INF2003 Database Systems module.

## Acknowledgments

- MovieLens Dataset
- TMDB (The Movie Database) for movie metadata
- SIT School of Computing
