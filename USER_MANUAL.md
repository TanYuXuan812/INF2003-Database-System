# User Manual - INF2003 Movie Database System

## Table of Contents
1. [System Requirements](#system-requirements)
2. [Installation Guide](#installation-guide)
3. [Starting the Application](#starting-the-application)
4. [User Portal Guide](#user-portal-guide)
5. [Admin Portal Guide](#admin-portal-guide)
6. [API Usage Examples](#api-usage-examples)
7. [Database Management](#database-management)
8. [Troubleshooting](#troubleshooting)
9. [Advanced Features](#advanced-features)

---

## System Requirements

### Minimum Requirements
- **Operating System**: Windows 10/11, macOS 10.15+, or Linux (Ubuntu 20.04+)
- **RAM**: 8GB minimum, 16GB recommended
- **Disk Space**: 5GB free space
- **CPU**: Dual-core processor (quad-core recommended)
- **Internet**: Required for Docker image downloads

### Software Prerequisites
- **Docker Desktop** version 20.10 or higher
  - Download from: https://www.docker.com/products/docker-desktop
- **Docker Compose** version 2.0 or higher (included with Docker Desktop)
- **Web Browser**: Chrome, Firefox, Safari, or Edge (latest version)

### Port Requirements
The following ports must be available:
- `80` - Frontend (Nginx)
- `5000` - API (Flask)
- `5432` - PostgreSQL
- `8080` - Adminer (PostgreSQL Web UI)
- `8081` - Mongo Express (MongoDB Web UI)
- `27017` - MongoDB

---

## Installation Guide

### Step 1: Install Docker

#### Windows
1. Download Docker Desktop from https://www.docker.com/products/docker-desktop
2. Run the installer and follow the prompts
3. Restart your computer when prompted
4. Verify installation: Open PowerShell and run:
   ```powershell
   docker --version
   docker compose version
   ```

#### macOS
1. Download Docker Desktop for Mac
2. Open the .dmg file and drag Docker to Applications
3. Launch Docker from Applications
4. Verify installation in Terminal:
   ```bash
   docker --version
   docker compose version
   ```

#### Linux (Ubuntu/Debian)
```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Add your user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Install Docker Compose
sudo apt-get update
sudo apt-get install docker-compose-plugin

# Verify installation
docker --version
docker compose version
```

### Step 2: Clone the Repository

```bash
# Option 1: If you have the zip file
unzip INF2003-Database-System.zip
cd INF2003-Database-System

# Option 2: If using Git
git clone <repository-url>
cd INF2003-Database-System
```

### Step 3: Configure Environment

```bash
# Copy the environment template
cp .env.example .env

# Optional: Edit the .env file to change default passwords
# nano .env    (Linux/Mac)
# notepad .env (Windows)
```

**Default Configuration** (`.env`):
```env
POSTGRES_DB=moviedb
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123

MONGO_DB=moviedb_mongo
MONGO_USER=admin
MONGO_PASSWORD=admin123

SECRET_KEY=dev-secret-key-change-in-production
```

---

## Starting the Application

### Quick Start (Recommended)

Open a terminal in the project directory and run:

```bash
docker compose up --build
```

**What this does**:
1. Builds the Flask API Docker image
2. Downloads PostgreSQL, MongoDB, Nginx, Adminer, and Mongo Express images
3. Creates and starts all containers
4. Initializes databases with schemas
5. Starts the web servers

**Expected Output**:
```
[+] Building...
[+] Running 6/6
 ✔ Container moviedb_postgres       Healthy
 ✔ Container moviedb_mongodb        Healthy
 ✔ Container moviedb_api            Healthy
 ✔ Container moviedb_frontend       Started
 ✔ Container moviedb_adminer        Started
 ✔ Container moviedb_mongo_express  Started
```

**First-time startup takes ~2-3 minutes**. Subsequent starts are faster.

### Verify Services are Running

```bash
# Check all containers are running
docker compose ps

# Check API health
curl http://localhost:5000/health
```

Expected health check response:
```json
{
  "status": "healthy",
  "databases": {
    "postgresql": "connected",
    "mongodb": "connected"
  }
}
```

---

## User Portal Guide

### Accessing the User Portal

Open your web browser and navigate to:
```
http://localhost
```

### Features Overview

#### 1. Search Movies Tab
- **Purpose**: Search and browse movies from PostgreSQL database
- **How to use**:
  1. Enter a movie title in the search box (e.g., "Star Wars")
  2. Optionally select a genre from the dropdown
  3. Click "Search"
  4. Results show: title, rating, genres, popularity

**Behind the scenes**: Uses PostgreSQL query with ILIKE, JOINs, and aggregations

#### 2. Trending Tab
- **Purpose**: View trending movies based on recent activity
- **How to use**:
  1. Click the "🔥 Trending" tab
  2. Movies are ranked by trending score (computed from MongoDB events)

**Behind the scenes**: MongoDB aggregation pipeline with weighted scoring and exponential time decay

#### 3. Analytics Tab
- **Purpose**: Real-time analytics from MongoDB
- **Features**:
  - Event funnel statistics (page views, searches, clicks)
  - Top viewed movies (last 30 days)
  - User engagement metrics

**Behind the scenes**: Complex MongoDB aggregation pipelines ($match, $group, $facet)

#### 4. Compare Databases Tab
- **Purpose**: Side-by-side performance comparison of SQL vs NoSQL
- **How to use**:
  1. Click "⚡ Compare Databases"
  2. Click "Compare: Top Movies Query"
  3. View results showing:
     - Query execution times (milliseconds)
     - Number of results
     - Performance analysis

**Example Output**:
```
PostgreSQL: 45.23 ms (GROUP BY with AVG aggregation)
MongoDB: 32.18 ms (Aggregation pipeline)
Analysis: MongoDB was 1.4x faster for this query
```

---

## Admin Portal Guide

### Accessing the Admin Portal

Navigate to:
```
http://localhost/admin
```

### Dashboard Tab

**Overview metrics**:
- Movie counts by genre
- Database health status
- System statistics

### Movies Tab

**Manage movie database**:
1. Enter search term in the search box
2. Click "Search"
3. View table with:
   - Movie ID, Title, Release Date
   - Genres, Average Rating, Rating Count
   - Popularity score

### SQL Analytics Tab

**Explore advanced SQL features**:

#### Top Rated Movies
- View: `v_top_rated_movies`
- Shows movies with avg_rating >= 4.0
- Minimum 10 ratings required
- **Demonstrates**: Views, aggregations

#### Genre Statistics
- View: `v_genre_stats`
- Movie count per genre
- Average ratings and revenue
- **Demonstrates**: Multi-table JOINs, aggregations

#### Top Per Genre
- View: `v_top_movies_per_genre`
- Top 10 movies within each genre
- **Demonstrates**: Window functions (DENSE_RANK)

#### EXPLAIN Plans
- Click "EXPLAIN Plans" tab
- View PostgreSQL query execution plans
- See index usage and performance metrics
- **Demonstrates**: EXPLAIN ANALYZE, query optimization

### MongoDB Analytics Tab

**Explore NoSQL aggregation pipelines**:

#### Event Funnel
- Groups events by type
- Counts and unique user metrics
- **Pipeline**: $match, $group, $project

#### Trending Movies
- Weighted scoring algorithm
- Time decay factor
- **Pipeline**: $addFields, $exp, $multiply

#### Conversion Funnel
- Session-based analysis
- Search → View → Cart → Purchase
- Conversion rate calculations
- **Pipeline**: $in, $cond, session grouping

#### User Engagement
- Engagement scores by activity
- Page views, searches, purchases weighted
- **Pipeline**: $group, $addFields, calculated fields

#### Pipeline Viewer
- View raw aggregation pipeline JSON
- Understand MongoDB query structure

### Diagnostics Tab

**System information**:

#### PostgreSQL Indexes
- List all indexes
- Index definitions
- Table associations
- **Shows**: Composite, Partial, GIN indexes

#### MongoDB Collection Statistics
- Document counts
- Index information per collection
- TTL index status

---

## API Usage Examples

### Testing with cURL

#### 1. Search Movies (SQL)
```bash
curl "http://localhost:5000/api/sql/movies?search=matrix&page_size=5"
```

**Response**:
```json
{
  "data": [
    {
      "movie_id": 603,
      "title": "The Matrix",
      "released_date": "1999-03-30",
      "avg_rating": 4.2,
      "rating_count": 1234,
      "genres": "Action, Science Fiction",
      "popularity": 98.5
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 5,
    "total": 1,
    "total_pages": 1
  }
}
```

#### 2. Get Top Rated Movies (SQL View)
```bash
curl "http://localhost:5000/api/sql/analytics/top-rated"
```

#### 3. Log Event (MongoDB)
```bash
curl -X POST http://localhost:5000/api/nosql/events \
  -H "Content-Type: application/json" \
  -d '{
    "type": "page_view",
    "user_id": 1,
    "movie_id": 603,
    "session_id": "demo-session-123",
    "metadata": {"page": "movie_detail"}
  }'
```

**Response**:
```json
{
  "event_id": "507f1f77bcf86cd799439011",
  "status": "logged"
}
```

#### 4. Get Trending Movies (MongoDB)
```bash
curl "http://localhost:5000/api/nosql/analytics/trending?days=7&limit=10"
```

#### 5. Compare Performance
```bash
curl "http://localhost:5000/api/compare/top-movies?limit=10&days=30"
```

**Response**:
```json
{
  "sql": {
    "time_ms": 45.23,
    "row_count": 10,
    "method": "PostgreSQL: GROUP BY with AVG aggregation"
  },
  "nosql": {
    "time_ms": 32.18,
    "row_count": 10,
    "method": "MongoDB: Aggregation pipeline"
  },
  "comparison": {
    "faster": "nosql",
    "speedup_factor": 1.41,
    "difference_ms": 13.05,
    "analysis": "NOSQL was 1.41x faster for this query"
  }
}
```

#### 6. Run Performance Benchmark
```bash
curl -X POST http://localhost:5000/api/compare/benchmark \
  -H "Content-Type: application/json" \
  -d '{
    "query_type": "top_movies",
    "iterations": 20,
    "params": {"limit": 10}
  }'
```

**Response**:
```json
{
  "query_type": "top_movies",
  "iterations": 20,
  "sql": {
    "min": 42.15,
    "max": 58.92,
    "avg": 47.23,
    "p50": 46.18,
    "p95": 55.41,
    "p99": 58.02
  },
  "nosql": {
    "min": 28.34,
    "max": 41.27,
    "avg": 32.15,
    "p50": 31.45,
    "p95": 38.92,
    "p99": 40.55
  },
  "comparison": {
    "sql_faster_on_avg": false,
    "avg_difference_ms": 15.08,
    "sql_more_consistent": true
  }
}
```

---

## Database Management

### Accessing Database GUIs

#### Adminer (PostgreSQL)
```
URL: http://localhost:8080
System: PostgreSQL
Server: postgres
Username: admin
Password: admin123
Database: moviedb
```

**Features**:
- Browse tables and data
- Run SQL queries
- View schema and relationships
- Export data

#### Mongo Express (MongoDB)
```
URL: http://localhost:8081
Username: admin
Password: admin123
```

**Features**:
- Browse collections
- Run aggregation pipelines
- View indexes
- Edit documents

### Direct Database Access

#### PostgreSQL (psql)
```bash
docker exec -it moviedb_postgres psql -U admin -d moviedb
```

**Example queries**:
```sql
-- List all tables
\dt

-- View movie stats
SELECT * FROM v_movie_stats LIMIT 5;

-- Show indexes
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'public';

-- EXPLAIN a query
EXPLAIN ANALYZE SELECT * FROM movies WHERE title ILIKE '%star%';
```

#### MongoDB (mongosh)
```bash
docker exec -it moviedb_mongodb mongosh -u admin -p admin123
```

**Example commands**:
```javascript
// Switch to database
use moviedb_mongo

// List collections
show collections

// Query events
db.events.find({type: "page_view"}).limit(5)

// Run aggregation
db.events.aggregate([
  {$group: {_id: "$type", count: {$sum: 1}}},
  {$sort: {count: -1}}
])

// Show indexes
db.events.getIndexes()
```

---

## Troubleshooting

### Problem: Containers won't start

**Solution 1**: Check port conflicts
```bash
# Linux/Mac
sudo lsof -i :80 -i :5000 -i :5432 -i :27017

# Windows (PowerShell as Admin)
netstat -ano | findstr ":80 :5000 :5432 :27017"
```

**Solution 2**: Stop conflicting services
```bash
# Stop existing services using those ports
# Then try again
docker compose down
docker compose up --build
```

### Problem: "Error: Cannot connect to database"

**Check database health**:
```bash
# View logs
docker compose logs postgres
docker compose logs mongodb

# Restart specific service
docker compose restart postgres
docker compose restart mongodb
```

**Wait for healthchecks**:
- PostgreSQL and MongoDB need ~10-30 seconds to start
- Check status: `docker compose ps`
- Look for "healthy" status

### Problem: "404 Not Found" on API endpoints

**Verify API is running**:
```bash
# Check API health
curl http://localhost:5000/health

# View API logs
docker compose logs api

# Restart API
docker compose restart api
```

### Problem: Frontend shows blank page

**Solution**:
```bash
# Check frontend logs
docker compose logs frontend

# Verify Nginx is running
docker compose ps frontend

# Test direct API access
curl http://localhost:5000/
```

### Problem: Need to reset everything

**Fresh start**:
```bash
# Stop and remove all containers and volumes
docker compose down -v

# Remove all Docker data
docker system prune -a

# Start fresh
docker compose up --build
```

---

## Advanced Features

### Custom SQL Queries

Access PostgreSQL and run custom queries:

```sql
-- Get movies with above-average ratings
SELECT m.movie_id, m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
HAVING AVG(r.rating) > (SELECT AVG(rating) FROM ratings)
ORDER BY avg_rating DESC
LIMIT 20;

-- Window function: Rank movies by popularity per genre
SELECT
    m.title,
    g.genre_name,
    m.popularity,
    DENSE_RANK() OVER (PARTITION BY g.genre_name ORDER BY m.popularity DESC) AS rank
FROM movies m
JOIN movie_genres mg ON m.movie_id = mg.movie_id
JOIN genres g ON mg.genre_id = g.genre_id
WHERE m.popularity > 50
LIMIT 50;
```

### Custom MongoDB Aggregations

Access MongoDB and run custom pipelines:

```javascript
// Complex aggregation: User behavior analysis
db.events.aggregate([
  {
    $match: {
      timestamp: {$gte: new Date(Date.now() - 7*24*60*60*1000)}
    }
  },
  {
    $group: {
      _id: {user: "$user_id", movie: "$movie_id"},
      events: {$push: "$type"},
      first_event: {$min: "$timestamp"},
      last_event: {$max: "$timestamp"}
    }
  },
  {
    $addFields: {
      engagement_duration: {
        $subtract: ["$last_event", "$first_event"]
      },
      event_count: {$size: "$events"}
    }
  },
  {
    $sort: {engagement_duration: -1}
  },
  {
    $limit: 20
  }
])
```

### Performance Tuning

#### Create Custom Index (PostgreSQL)
```sql
-- Create index for specific query pattern
CREATE INDEX idx_custom ON movies(released_date, popularity DESC)
WHERE popularity > 50;
```

#### Create Custom Index (MongoDB)
```javascript
// Create compound index
db.events.createIndex(
  {user_id: 1, type: 1, timestamp: -1},
  {name: "idx_user_type_time"}
)
```

---

## Support and Resources

### Documentation Files
- **PLAN.md** - System architecture and design decisions
- **README.md** - Project overview and quick reference
- **scripts/schema.sql** - PostgreSQL schema with comments
- **scripts/views.sql** - SQL views and functions
- **scripts/mongo_schema.js** - MongoDB setup
- **scripts/mongo_aggregations.js** - Example pipelines

### Getting Help

1. Check logs: `docker compose logs [service-name]`
2. Review health status: `curl http://localhost:5000/health`
3. Consult PostgreSQL docs: https://www.postgresql.org/docs/
4. Consult MongoDB docs: https://docs.mongodb.com/

### Common Commands Reference

```bash
# Start system
docker compose up -d

# Stop system
docker compose down

# View logs
docker compose logs -f

# Restart specific service
docker compose restart [service-name]

# Fresh start
docker compose down -v && docker compose up --build

# Enter container shell
docker exec -it moviedb_api /bin/bash

# Run SQL commands
docker exec -it moviedb_postgres psql -U admin -d moviedb

# Run MongoDB commands
docker exec -it moviedb_mongodb mongosh -u admin -p admin123
```

---

**End of User Manual**

For technical details and implementation notes, refer to PLAN.md.
For API documentation, visit http://localhost:5000 after starting the system.
