# INF2003 Database System - Implementation Plan

## Current Stack Analysis

### Existing Files
```
INF2003-Database-System/
├── app_gui.py          # Tkinter Admin GUI (158KB) - comprehensive CRUD interface
├── admin_query.py      # PostgreSQL CRUD functions (31KB)
├── user_query.py       # PostgreSQL search/query functions (8KB)
├── insert.py           # Data seeding script (16KB)
├── main.py             # Data cleaning/preprocessing (3KB)
├── README.md           # Minimal description
├── LICENSE             # MIT License
└── .env                # Database credentials (gitignored)
```

### Current Tech Stack
- **Language**: Python 3.x
- **GUI Framework**: Tkinter (desktop application)
- **Database**: PostgreSQL only
- **Data Source**: MovieLens + TMDB (The Movie Database)
- **Domain**: Movie database with entities:
  - Movies (title, overview, release date, runtime, etc.)
  - Genres (normalized M-N relationship)
  - Production Companies (normalized M-N relationship)
  - People (cast & crew)
  - Ratings (user ratings from MovieLens)
  - Keywords, Cast, Crew (junction tables)

### Current Features
✅ Admin CRUD operations for all entities
✅ Search queries (by title, genre, actor, date range, rating)
✅ Basic PostgreSQL schema with foreign keys
✅ Data import from CSV files
✅ Tkinter GUI with entity management

### Missing Requirements (INF2003 Rubric)
❌ MongoDB integration (NoSQL database)
❌ User-facing GUI (separate from admin)
❌ Docker Compose for one-command deployment
❌ Advanced SQL features (views, CTEs, window functions, complex indexes)
❌ NoSQL aggregation pipelines with $lookup, $facet, etc.
❌ Performance comparison (SQL vs NoSQL)
❌ Comprehensive documentation (ERD, user manual, slides)
❌ Benchmarking and performance analysis
❌ Web-based interface (Tkinter won't work in Docker without X11)

---

## Proposed Architecture (Minimal Churn)

### Architecture Decision: Web-Based Dual GUI

**Rationale**:
1. Docker containerization requires web-based GUIs (Tkinter needs X11, impractical)
2. Performance testing is easier with HTTP endpoints
3. "One-command demo" requirement strongly implies web architecture
4. Enables side-by-side SQL/NoSQL comparison in browser
5. Can reuse existing Python query logic with minimal changes

### New Tech Stack
```
┌─────────────────────────────────────────────────────────┐
│                    Docker Compose                        │
├──────────────┬──────────────┬──────────────┬────────────┤
│  PostgreSQL  │   MongoDB    │  Flask API   │  Frontend  │
│   (port      │  (port       │  (port 5000) │  (Nginx    │
│    5432)     │   27017)     │              │   port 80) │
│              │              │  Reuses:     │            │
│  - Movies    │  - events    │  - admin_    │  - Admin   │
│  - Genres    │  - profiles  │    query.py  │    GUI     │
│  - Ratings   │  - sessions  │  - user_     │  - User    │
│  - Cast      │  - analytics │    query.py  │    GUI     │
│  - Crew      │              │              │  - Compare │
└──────────────┴──────────────┴──────────────┴────────────┘
```

### Component Breakdown

#### 1. PostgreSQL (Relational - Enhanced)
**Purpose**: Core OLTP data (movies, users, orders, ratings)

**Tables** (keeping existing + adding new):
- `movies` - Core movie data with JSONB metadata field
- `genres` - Normalized genres
- `production_companies` - Normalized companies
- `people` - Cast and crew members
- `movie_genres` - Junction table (M-N)
- `movie_production_companies` - Junction table (M-N)
- `movie_cast` - Junction table with character names
- `movie_crew` - Junction table with jobs
- `ratings` - User ratings
- `keywords` - Movie keywords
- `movie_keywords` - Junction table
- **NEW**: `users` - App users for cart/checkout
- **NEW**: `orders` - Purchase orders (transactional demo)
- **NEW**: `order_items` - Order line items

**Advanced Features** (to satisfy rubric):
1. **Constraints**: PK, FK (CASCADE), UNIQUE, NOT NULL, CHECK
2. **Indexes**:
   - Composite: `(status, created_at)` on orders
   - Partial: `WHERE rating >= 4` on ratings
   - GIN: On JSONB metadata field
   - B-tree: On FKs and frequently queried fields
3. **Views**:
   - `v_movie_stats` - Aggregated movie statistics
   - `v_top_rated_movies` - Movies with avg rating > 4
   - `v_popular_genres` - Genre popularity metrics
4. **Advanced Queries**:
   - CTEs for recursive/hierarchical queries
   - Window functions: `DENSE_RANK()`, `ROW_NUMBER()`, `LAG()`
   - Aggregations: Complex GROUP BY with HAVING
   - Full-text search using `to_tsvector`
5. **Materialized View**: `mv_daily_kpis` for dashboard
6. **Optional**: LISTEN/NOTIFY for real-time order updates

#### 2. MongoDB (Non-Relational)
**Purpose**: Heterogeneous telemetry, denormalized profiles, analytics

**Collections**:
1. **`events`** - User interaction telemetry
   ```javascript
   {
     _id: ObjectId,
     type: "page_view" | "click" | "search" | "add_to_cart" | "checkout",
     user_id: int,
     movie_id: int (optional),
     timestamp: ISODate,
     metadata: { /* flexible fields */ },
     session_id: string,
     ip_address: string
   }
   ```
   - **Indexes**: Compound `(type, timestamp)`, TTL on timestamp (30 days)

2. **`user_profiles`** - Denormalized user preferences
   ```javascript
   {
     _id: ObjectId,
     user_id: int,
     preferences: {
       favorite_genres: [],
       viewing_history: [],
       recommendations: []
     },
     last_updated: ISODate
   }
   ```

3. **`movie_aggregates`** - Pre-computed analytics
   ```javascript
   {
     movie_id: int,
     hourly_views: { "2025-11-01T10:00": 150, ... },
     click_rate: 0.25,
     conversion_rate: 0.05
   }
   ```

**Aggregation Pipelines** (to satisfy rubric):
1. **Event Funnel Analysis**:
   ```javascript
   [
     { $match: { timestamp: { $gte: ISODate("...") } } },
     { $group: { _id: "$type", count: { $sum: 1 } } },
     { $sort: { count: -1 } }
   ]
   ```

2. **Top Movies by Views** (with $lookup to profiles):
   ```javascript
   [
     { $match: { type: "page_view" } },
     { $group: { _id: "$movie_id", views: { $sum: 1 } } },
     { $lookup: { from: "user_profiles", ... } },
     { $sort: { views: -1 } },
     { $limit: 10 }
   ]
   ```

3. **Time-Series Analytics** (using $bucket/$facet):
   ```javascript
   [
     { $facet: {
       hourly: [{ $bucket: { groupBy: "$timestamp", ... } }],
       by_type: [{ $group: { _id: "$type", ... } }]
     }}
   ]
   ```

4. **Text Search**: Text index on event metadata for flexible search

#### 3. Flask API Backend
**Purpose**: RESTful API exposing both SQL and NoSQL operations

**API Structure**:
```
/api/
├── /sql/
│   ├── /movies (GET, POST, PUT, DELETE)
│   ├── /genres (GET, POST, PUT, DELETE)
│   ├── /ratings (GET, POST, PUT, DELETE)
│   ├── /orders (POST - transactional)
│   ├── /analytics/
│   │   ├── /top-movies
│   │   ├── /genre-distribution
│   │   └── /revenue-by-month (window functions)
│   └── /admin/
│       ├── /explain?query=<name> (returns EXPLAIN ANALYZE)
│       └── /slow-queries (recent slow queries)
│
├── /nosql/
│   ├── /events (POST, GET)
│   ├── /profiles (GET, PUT)
│   ├── /analytics/
│   │   ├── /funnel
│   │   ├── /top-viewed-movies
│   │   └── /hourly-activity
│   └── /admin/
│       ├── /pipeline?name=<name> (returns pipeline JSON)
│       └── /indexes (list indexes)
│
├── /compare/
│   ├── /top-movies?window=30d (SQL vs NoSQL side-by-side)
│   ├── /search?q=<term> (FTS in SQL vs text index in Mongo)
│   └── /benchmark (run N iterations, return latency stats)
│
└── /health (DB connectivity check)
```

**Implementation Strategy**:
- Refactor `admin_query.py` into API endpoints (minimal changes)
- Add `mongo_query.py` with aggregation pipeline functions
- Add `compare_query.py` for side-by-side execution
- Use Flask-CORS for cross-origin requests
- Add request logging and slow query detection

#### 4. Frontend (Dual GUIs)

**Admin GUI** (enhanced from existing Tkinter):
- **Tech**: Vue.js 3 + Vite (lightweight, reactive)
- **Features**:
  - DataGrid with server-side pagination, filtering, sorting
  - CRUD forms with validation (aligned to SQL constraints)
  - "Explain Plan" modal showing SQL EXPLAIN output
  - "Show Pipeline" modal for MongoDB aggregations
  - Import/Export CSV/JSON
  - Slow query diagnostics tab
  - Database health dashboard
- **Routes**:
  - `/admin/movies`
  - `/admin/genres`
  - `/admin/ratings`
  - `/admin/diagnostics`

**User GUI** (new):
- **Tech**: Vue.js 3 + Vite (consistent stack)
- **Features**:
  - Movie search/browse with filters (genre, rating, date)
  - Movie detail page with recommendations
  - Shopping cart (demonstration of transactions)
  - Checkout flow (creates order + order_items transactionally)
  - "Compare Engines" page:
    - Run same query (e.g., "Top 10 movies") via SQL and Mongo
    - Display results side-by-side with execution time
    - Show query/pipeline JSON
  - User profile with viewing history (from Mongo)
- **Routes**:
  - `/` - Home/search
  - `/movie/:id` - Movie details
  - `/cart` - Shopping cart
  - `/checkout` - Checkout
  - `/compare` - SQL vs NoSQL comparison

**Shared Components**:
- API client utility
- Loading/error states
- Toast notifications
- Responsive layout

#### 5. Docker Compose Setup

**Services**:
```yaml
services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: moviedb
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - ./scripts/schema.sql:/docker-entrypoint-initdb.d/01-schema.sql
      - ./scripts/seed_postgres.sql:/docker-entrypoint-initdb.d/02-seed.sql
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  mongodb:
    image: mongo:7
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: ${MONGO_PASSWORD}
    volumes:
      - ./scripts/seed_mongo.js:/docker-entrypoint-initdb.d/seed.js
      - mongo_data:/data/db
    ports:
      - "27017:27017"

  api:
    build: ./backend
    environment:
      DATABASE_URL: postgresql://admin:${POSTGRES_PASSWORD}@postgres:5432/moviedb
      MONGO_URL: mongodb://admin:${MONGO_PASSWORD}@mongodb:27017
    depends_on:
      - postgres
      - mongodb
    ports:
      - "5000:5000"

  frontend:
    build: ./frontend
    depends_on:
      - api
    ports:
      - "80:80"

  # Optional: Database admin tools
  adminer:
    image: adminer
    ports:
      - "8080:8080"

  mongo-express:
    image: mongo-express
    environment:
      ME_CONFIG_MONGODB_URL: mongodb://admin:${MONGO_PASSWORD}@mongodb:27017
    ports:
      - "8081:8081"
```

**One-Command Start**:
```bash
docker compose up --build
```

---

## Implementation Phases

### Phase 1: Infrastructure Setup ✅ (Current)
- [x] Analyze existing codebase
- [x] Create PLAN.md
- [ ] Set up project structure (directories)
- [ ] Create .env.example

### Phase 2: Database Layer
**PostgreSQL**:
- [ ] Create `schema.sql` with all tables, constraints, indexes
- [ ] Create `views.sql` with views and materialized views
- [ ] Create `functions.sql` with stored procedures (optional)
- [ ] Create `seed_postgres.sql` using existing data

**MongoDB**:
- [ ] Create `mongo_schema.js` with collection definitions
- [ ] Create `mongo_indexes.js` with index creation
- [ ] Create `mongo_aggregations.js` with pipeline templates
- [ ] Create `seed_mongo.js` with sample data

### Phase 3: Backend API
- [ ] Set up Flask project structure
- [ ] Refactor `admin_query.py` → `/api/sql/*` endpoints
- [ ] Create `mongo_query.py` for NoSQL operations
- [ ] Implement `/api/compare/*` endpoints
- [ ] Add logging, error handling, CORS
- [ ] Create `requirements.txt`
- [ ] Create Dockerfile for backend

### Phase 4: Frontend Development
**Admin GUI**:
- [ ] Set up Vue.js project with Vite
- [ ] Create data grid component with pagination
- [ ] Implement CRUD forms for movies, genres, etc.
- [ ] Add "Explain Plan" and "Pipeline Viewer" modals
- [ ] Add diagnostics dashboard

**User GUI**:
- [ ] Create search/browse interface
- [ ] Implement movie detail page
- [ ] Build cart and checkout flow
- [ ] Create "Compare Engines" page
- [ ] Add recommendations section

**Both**:
- [ ] Create Dockerfile for frontend (Nginx)
- [ ] Build production assets

### Phase 5: Docker Integration
- [ ] Create `docker-compose.yml`
- [ ] Create `.env.example`
- [ ] Test one-command startup
- [ ] Add healthchecks
- [ ] Verify data seeding

### Phase 6: Documentation
- [ ] Create comprehensive README.md
- [ ] Create USER_MANUAL.md with step-by-step instructions
- [ ] Generate ERD diagram (using dbdiagram.io or Mermaid)
- [ ] Document NoSQL schema (nosql_schema.md)
- [ ] Create performance analysis (perf_results.md)
- [ ] Create slides deck outline (docs/slides/)

### Phase 7: Testing & Benchmarking
- [ ] Implement `/api/compare/benchmark` endpoint
- [ ] Run performance tests (SQL vs NoSQL)
- [ ] Document results with screenshots
- [ ] Add unit tests for critical functions
- [ ] End-to-end testing

---

## Rubric Alignment Checklist

### Data & Design (20%)
- [ ] ER diagram showing 1-M, M-1, M-N relationships
- [ ] PostgreSQL tables with PK, FK, UNIQUE, NOT NULL, CHECK constraints
- [ ] 3+ meaningful indexes (composite, partial, GIN)
- [ ] Views + advanced construct (CTE/window/JSONB/materialized view)
- [ ] MongoDB schema documented with 2+ collections
- [ ] 2+ non-trivial aggregation pipelines
- [ ] Compound + TTL indexes in MongoDB

### Functionality (30%)
- [ ] Admin GUI CRUD with validation + pagination
- [ ] "Explain Plan" viewer for SQL
- [ ] "Pipeline JSON" viewer for MongoDB
- [ ] User GUI with search, browse, cart, checkout
- [ ] Telemetry logging to MongoDB
- [ ] Recommendations feature
- [ ] Compare Engines page with time_ms display

### Advanced Database Features (25%)
**SQL**:
- [ ] Complex JOIN queries (3+ tables)
- [ ] Aggregations (GROUP BY, HAVING)
- [ ] Window functions (RANK, LAG, etc.)
- [ ] CTEs (Common Table Expressions)
- [ ] Views and materialized views
- [ ] Full-text search or JSONB queries
- [ ] Transaction handling (order placement)

**NoSQL**:
- [ ] $match, $group, $sort, $project
- [ ] $lookup (cross-collection join)
- [ ] $facet (multi-dimensional aggregation)
- [ ] $bucket (time-series bucketing)
- [ ] Compound indexes
- [ ] TTL indexes for auto-expiry
- [ ] Text search indexes

### Performance & Analysis (15% - Optional Extra Credit)
- [ ] Benchmark endpoint comparing SQL vs NoSQL
- [ ] perf_results.md with data and analysis
- [ ] Screenshots/CSV of performance metrics
- [ ] Discussion of trade-offs

### Deliverables (10%)
- [ ] `docker compose up --build` works on fresh machine
- [ ] README.md with quickstart guide
- [ ] USER_MANUAL.md with detailed instructions
- [ ] All team members can present (slides/video folder stub)
- [ ] Code is clean, commented, and organized

---

## File Structure (Final)

```
INF2003-Database-System/
├── backend/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── sql_routes.py       # Refactored from admin_query.py
│   │   ├── nosql_routes.py     # New MongoDB endpoints
│   │   └── compare_routes.py   # Comparison endpoints
│   ├── queries/
│   │   ├── admin_query.py      # (existing, minimal changes)
│   │   ├── user_query.py       # (existing, minimal changes)
│   │   ├── mongo_query.py      # New MongoDB functions
│   │   └── compare_query.py    # Side-by-side execution
│   ├── app.py                  # Flask application entry point
│   ├── config.py               # Configuration management
│   ├── requirements.txt        # Python dependencies
│   └── Dockerfile              # Backend Docker image
│
├── frontend/
│   ├── admin/                  # Admin GUI (Vue.js)
│   │   ├── src/
│   │   ├── public/
│   │   ├── index.html
│   │   ├── package.json
│   │   └── vite.config.js
│   ├── user/                   # User GUI (Vue.js)
│   │   ├── src/
│   │   ├── public/
│   │   ├── index.html
│   │   ├── package.json
│   │   └── vite.config.js
│   ├── nginx.conf              # Nginx routing config
│   └── Dockerfile              # Frontend Docker image
│
├── scripts/
│   ├── schema.sql              # PostgreSQL schema (tables, constraints)
│   ├── indexes.sql             # PostgreSQL indexes
│   ├── views.sql               # PostgreSQL views
│   ├── seed_postgres.sql       # PostgreSQL seed data
│   ├── mongo_schema.js         # MongoDB collection setup
│   ├── mongo_indexes.js        # MongoDB index creation
│   ├── seed_mongo.js           # MongoDB seed data
│   └── benchmark.sh            # Performance testing script
│
├── docs/
│   ├── ERD.png                 # Entity-Relationship Diagram
│   ├── nosql_schema.md         # MongoDB schema documentation
│   ├── perf_results.md         # Performance analysis
│   ├── architecture.md         # System architecture
│   └── slides/                 # Presentation deck
│       ├── 01-background.md
│       ├── 02-sql-implementation.md
│       ├── 03-nosql-implementation.md
│       ├── 04-comparison.md
│       └── 05-performance.md
│
├── legacy/                     # Original Tkinter code (archived)
│   ├── app_gui.py
│   └── ...
│
├── docker-compose.yml          # Multi-service orchestration
├── .env.example                # Environment template
├── .gitignore                  # Git ignore rules
├── README.md                   # Quickstart guide
├── USER_MANUAL.md              # Detailed user manual
├── PLAN.md                     # This file
└── LICENSE                     # MIT License

```

---

## Risk Mitigation

### Risk 1: Tight Timeline
**Mitigation**: Reuse existing query logic; use lightweight frameworks (Flask, Vue); minimal custom CSS (use Tailwind/Bootstrap).

### Risk 2: Docker Complexity
**Mitigation**: Use official images; simple compose file; document troubleshooting in USER_MANUAL.md.

### Risk 3: Performance Testing
**Mitigation**: Start simple (single query comparison); expand if time allows; use realistic dataset size.

### Risk 4: Learning Curve (MongoDB)
**Mitigation**: Start with simple aggregations; use MongoDB documentation; test in Mongo Express UI first.

### Risk 5: Data Seeding
**Mitigation**: Reuse existing CSV data; create smaller sample datasets for development; optimize bulk inserts.

---

## Success Criteria

1. ✅ `docker compose up --build` starts all services in <2 minutes
2. ✅ Admin GUI accessible at `http://localhost/admin` with full CRUD
3. ✅ User GUI accessible at `http://localhost/` with search, cart, checkout
4. ✅ Compare page shows SQL vs NoSQL results with execution times
5. ✅ PostgreSQL has 10+ tables, 3+ views, 5+ advanced queries
6. ✅ MongoDB has 3+ collections, 3+ aggregation pipelines
7. ✅ Documentation allows fresh machine setup in <10 minutes
8. ✅ All rubric requirements satisfied with evidence

---

## Next Steps

1. **Immediate**: Create project directory structure
2. **Day 1-2**: PostgreSQL schema + seed scripts
3. **Day 3-4**: MongoDB schema + aggregations + Flask API
4. **Day 5-6**: Admin GUI + User GUI frontends
5. **Day 7**: Docker integration + testing
6. **Day 8**: Documentation + benchmarking
7. **Day 9**: Final testing + polish
8. **Day 10**: Presentation prep

---

## Notes

- Existing Python query functions will be **wrapped** as API endpoints, not rewritten
- Tkinter GUI code will be **archived** in `legacy/` folder, not deleted
- CSV data files will be **converted** to SQL INSERT statements for seeding
- MongoDB will log telemetry from User GUI interactions (non-intrusive)
- Performance comparison will focus on one canonical query (e.g., "Top 10 movies by rating")
- Documentation will include screenshots and code snippets for clarity

---

**Status**: Plan approved. Ready to implement Phase 2 (Database Layer).
