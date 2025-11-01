# INF2003 Database System - Movie Database

A comprehensive dual-database system demonstrating advanced SQL and NoSQL features using PostgreSQL and MongoDB.

## 🎯 Project Overview

This project implements a full-stack movie database application with:
- **PostgreSQL** for relational data (movies, users, ratings, orders)
- **MongoDB** for non-relational data (events, analytics, user profiles)
- **Flask REST API** backend
- **Dual Web GUIs** (Admin Portal + User Portal)
- **Docker Compose** for one-command deployment

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose                           │
├──────────────┬──────────────┬──────────────┬────────────────┤
│  PostgreSQL  │   MongoDB    │  Flask API   │  Frontend      │
│  (port 5432) │  (port       │  (port 5000) │  (Nginx :80)   │
│              │   27017)     │              │                │
│  Movies      │  events      │  /api/sql/*  │  User GUI: /   │
│  Genres      │  profiles    │  /api/nosql/*│  Admin: /admin │
│  Users       │  sessions    │  /api/       │                │
│  Orders      │  analytics   │  compare/*   │                │
└──────────────┴──────────────┴──────────────┴────────────────┘
```

## ✨ Key Features

### Advanced SQL Features (PostgreSQL)
✅ **Constraints**: PK, FK with CASCADE, UNIQUE, NOT NULL, CHECK
✅ **Indexes**: Composite, Partial, GIN (JSONB), B-tree, pg_trgm
✅ **Views**: Regular and Materialized views
✅ **Window Functions**: DENSE_RANK, ROW_NUMBER, LAG
✅ **CTEs**: Common Table Expressions for complex queries
✅ **JSONB**: Flexible metadata storage with GIN indexing
✅ **Triggers**: Automatic timestamp updates
✅ **Stored Functions**: Transactional order placement

### Advanced NoSQL Features (MongoDB)
✅ **Aggregation Pipelines**: $match, $group, $sort, $project
✅ **Cross-Collection Joins**: $lookup operator
✅ **Multi-Dimensional Analytics**: $facet for parallel aggregations
✅ **Time-Series Analysis**: $bucket for hourly/daily grouping
✅ **Weighted Scoring**: $exp for exponential decay calculations
✅ **TTL Indexes**: Automatic document expiry
✅ **Compound Indexes**: Multi-field query optimization
✅ **Schema Validation**: BSON schema enforcement

### Application Features
✅ Movie search and browse with filters
✅ Real-time analytics and trending movies
✅ Event tracking and telemetry logging
✅ User engagement scoring
✅ Conversion funnel analysis
✅ Side-by-side SQL vs NoSQL performance comparison
✅ Admin portal with diagnostics and EXPLAIN plans
✅ Benchmark endpoint for performance testing

## 🚀 Quick Start

### Prerequisites
- **Docker** (v20.10+)
- **Docker Compose** (v2.0+)
- 8GB RAM minimum
- Ports 80, 5000, 5432, 8080, 8081, 27017 available

### One-Command Deployment

```bash
# Clone the repository
git clone <repository-url>
cd INF2003-Database-System

# Copy environment template
cp .env.example .env

# Optional: Edit .env to change passwords
# nano .env

# Start all services
docker compose up --build
```

### Access Points

After ~2 minutes of startup:

| Service | URL | Description |
|---------|-----|-------------|
| **User Portal** | http://localhost | Main application interface |
| **Admin Portal** | http://localhost/admin | Database management & analytics |
| **API Documentation** | http://localhost:5000 | REST API endpoints |
| **Health Check** | http://localhost:5000/health | System status |
| **Adminer (PostgreSQL)** | http://localhost:8080 | Database web UI |
| **Mongo Express** | http://localhost:8081 | MongoDB web UI |

**Default Credentials**: `admin` / `admin123` (change in `.env`)

## 📊 Database Schemas

### PostgreSQL Tables
- `movies` - Core movie data with JSONB metadata
- `genres` - Normalized genre reference
- `production_companies` - Production company details
- `people` - Cast and crew members
- `users` - Application users
- `ratings` - User movie ratings
- `orders` - Purchase orders (transactional demo)
- `order_items` - Order line items
- Junction tables: `movie_genres`, `movie_cast`, `movie_crew`, etc.

### MongoDB Collections
- `events` - User interaction telemetry (TTL: 30 days)
- `user_profiles` - Denormalized user preferences
- `movie_aggregates` - Pre-computed analytics
- `sessions` - Session tracking (TTL: 90 days)
- `recommendations` - Cached recommendations (TTL: 24 hours)

See [PLAN.md](PLAN.md) for detailed schema documentation.

## 🔧 API Endpoints

### SQL Endpoints (`/api/sql/`)
- `GET /movies` - List movies with pagination & filters
- `GET /movies/:id` - Get movie details
- `GET /genres` - List all genres
- `POST /orders` - Create order (transactional)
- `GET /analytics/top-rated` - Top-rated movies view
- `GET /analytics/genre-stats` - Genre statistics view
- `GET /analytics/top-per-genre` - Window function demo
- `GET /admin/explain?name=<query>` - EXPLAIN ANALYZE results
- `GET /admin/indexes` - List all indexes

### NoSQL Endpoints (`/api/nosql/`)
- `POST /events` - Log event
- `GET /events` - Query events
- `GET /analytics/funnel` - Event funnel analysis
- `GET /analytics/trending` - Trending movies (weighted)
- `GET /analytics/conversion-funnel` - Conversion metrics
- `GET /analytics/user-engagement` - User engagement scores
- `GET /admin/stats` - Collection statistics
- `GET /admin/pipeline?name=<pipeline>` - Pipeline definition

### Comparison Endpoints (`/api/compare/`)
- `GET /top-movies` - SQL vs NoSQL performance comparison
- `GET /trending` - Trending calculation comparison
- `POST /benchmark` - Run performance benchmarks

## 🧪 Testing the System

### 1. Search Movies (SQL)
```bash
curl "http://localhost:5000/api/sql/movies?search=star&page_size=5"
```

### 2. Log Events (MongoDB)
```bash
curl -X POST http://localhost:5000/api/nosql/events \
  -H "Content-Type: application/json" \
  -d '{
    "type": "page_view",
    "user_id": 1,
    "movie_id": 550,
    "session_id": "test-session",
    "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
  }'
```

### 3. Compare Performance
```bash
curl "http://localhost:5000/api/compare/top-movies?limit=10&days=30"
```

### 4. Run Benchmark
```bash
curl -X POST http://localhost:5000/api/compare/benchmark \
  -H "Content-Type: application/json" \
  -d '{
    "query_type": "top_movies",
    "iterations": 10,
    "params": {"limit": 10}
  }'
```

## 📈 Advanced SQL Demonstrations

### Window Functions (DENSE_RANK)
```sql
-- Top 10 movies per genre
SELECT * FROM v_top_movies_per_genre WHERE genre_rank <= 10;
```

### CTEs and Aggregations
```sql
-- Genre-based recommendations
SELECT * FROM v_genre_based_recommendations WHERE user_id = 1;
```

### EXPLAIN ANALYZE
```bash
curl "http://localhost:5000/api/sql/admin/explain?name=movie_stats"
```

## 📊 Advanced NoSQL Demonstrations

### Aggregation Pipeline (Trending Score)
```javascript
db.events.aggregate([
  { $match: { type: { $in: ['page_view', 'purchase'] } } },
  { $addFields: { weighted_score: { $multiply: [...] } } },
  { $group: { _id: '$movie_id', score: { $sum: '$weighted_score' } } },
  { $sort: { score: -1 } }
])
```

### Multi-Dimensional Analytics ($facet)
```bash
curl "http://localhost:5000/api/nosql/analytics/multi-dimensional?days=30"
```

## 🏆 INF2003 Rubric Alignment

### Data & Design (20%)
✅ ER diagram with 1-M, M-1, M-N relationships
✅ PostgreSQL: 12 tables, PK/FK, 10+ indexes (composite, partial, GIN)
✅ MongoDB: 5 collections with schema validation
✅ Views, materialized views, window functions
✅ Advanced aggregation pipelines ($lookup, $facet)

### Functionality (30%)
✅ Admin GUI with CRUD, pagination, diagnostics
✅ User GUI with search, analytics, comparison
✅ Event logging and telemetry
✅ Real-time analytics dashboards
✅ Performance comparison page

### Advanced Database Features (25%)
✅ **SQL**: CTEs, window functions, views, JSONB, triggers, stored functions
✅ **NoSQL**: Complex pipelines, $lookup, $facet, $bucket, weighted scoring
✅ Indexes: Composite, partial, GIN, TTL

### Performance & Analysis (15%)
✅ Benchmark endpoint with p50/p95/p99 metrics
✅ Side-by-side query comparison
✅ EXPLAIN ANALYZE integration

### Deliverables (10%)
✅ One-command Docker deployment
✅ Comprehensive README and USER_MANUAL
✅ Clean, documented code
✅ PLAN.md with architecture details

## 📚 Documentation

- **[PLAN.md](PLAN.md)** - Detailed architecture and implementation plan
- **[USER_MANUAL.md](USER_MANUAL.md)** - Step-by-step user guide
- **[docs/nosql_schema.md](docs/nosql_schema.md)** - MongoDB schema details
- **[scripts/schema.sql](scripts/schema.sql)** - PostgreSQL DDL
- **[scripts/views.sql](scripts/views.sql)** - SQL views and functions
- **[scripts/mongo_schema.js](scripts/mongo_schema.js)** - MongoDB setup
- **[scripts/mongo_aggregations.js](scripts/mongo_aggregations.js)** - Pipeline examples

## 🛠️ Development

### Project Structure
```
INF2003-Database-System/
├── backend/              # Flask API
│   ├── api/              # Route blueprints
│   ├── queries/          # Database query modules
│   ├── app.py            # Main application
│   └── config.py         # Configuration
├── frontend/             # Web interfaces
│   ├── index.html        # User portal
│   └── admin/            # Admin portal
├── scripts/              # Database scripts
│   ├── schema.sql        # PostgreSQL schema
│   ├── views.sql         # Views and functions
│   ├── mongo_schema.js   # MongoDB setup
│   └── seed_*.sql/js     # Seed data
├── docs/                 # Documentation
├── docker-compose.yml    # Multi-service orchestration
└── .env.example          # Environment template
```

### Stopping the System
```bash
docker compose down        # Stop services
docker compose down -v     # Stop and remove volumes (fresh start)
```

### Viewing Logs
```bash
docker compose logs -f api      # API logs
docker compose logs -f postgres # PostgreSQL logs
docker compose logs -f mongodb  # MongoDB logs
```

## 🐛 Troubleshooting

### Services Won't Start
```bash
# Check port conflicts
sudo lsof -i :80 -i :5000 -i :5432 -i :27017

# View all service logs
docker compose logs
```

### Database Connection Issues
```bash
# Check health status
curl http://localhost:5000/health

# Restart specific service
docker compose restart postgres
```

### Fresh Database Reset
```bash
docker compose down -v
docker compose up --build
```

## 👥 Team

[Add your team members here]

## 📄 License

MIT License - See [LICENSE](LICENSE) file

## 🙏 Acknowledgments

- MovieLens dataset for sample data
- TMDB API for movie metadata
- PostgreSQL and MongoDB communities
- Flask and Python ecosystem

---

**Note**: This is an academic project for INF2003 Database Systems. It demonstrates advanced database concepts and is not intended for production use without additional security hardening.

For detailed usage instructions, see [USER_MANUAL.md](USER_MANUAL.md).
