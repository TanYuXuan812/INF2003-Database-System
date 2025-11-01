-- ============================================================================
-- INF2003 Database System - PostgreSQL Schema
-- Movie Database with Advanced Features
-- ============================================================================
-- This schema demonstrates:
-- - Primary Keys, Foreign Keys with CASCADE
-- - UNIQUE, NOT NULL, CHECK constraints
-- - Composite indexes, Partial indexes, GIN indexes
-- - JSONB for flexible metadata
-- - Timestamp tracking with triggers
-- ============================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- For similarity/fuzzy search

-- Drop tables if they exist (for idempotent execution)
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS ratings CASCADE;
DROP TABLE IF EXISTS movie_keywords CASCADE;
DROP TABLE IF EXISTS keywords CASCADE;
DROP TABLE IF EXISTS movie_crew CASCADE;
DROP TABLE IF EXISTS movie_cast CASCADE;
DROP TABLE IF EXISTS movie_production_companies CASCADE;
DROP TABLE IF EXISTS production_companies CASCADE;
DROP TABLE IF EXISTS movie_genres CASCADE;
DROP TABLE IF EXISTS genres CASCADE;
DROP TABLE IF EXISTS movies CASCADE;
DROP TABLE IF EXISTS people CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- ============================================================================
-- Core Tables
-- ============================================================================

-- Users table (for cart/checkout functionality)
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    -- Constraints
    CONSTRAINT check_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

-- Movies table (core entity)
CREATE TABLE movies (
    movie_id INTEGER PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    adult BOOLEAN DEFAULT FALSE,
    overview TEXT,
    language VARCHAR(10),
    popularity DECIMAL(10, 2) DEFAULT 0.00,
    released_date DATE,
    runtime INTEGER,
    poster_path TEXT,
    tagline TEXT,
    revenue BIGINT DEFAULT 0,
    budget BIGINT DEFAULT 0,
    -- JSONB for flexible metadata (advanced feature)
    metadata JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT check_runtime_positive CHECK (runtime IS NULL OR runtime > 0),
    CONSTRAINT check_popularity_positive CHECK (popularity >= 0),
    CONSTRAINT check_revenue_non_negative CHECK (revenue >= 0),
    CONSTRAINT check_budget_non_negative CHECK (budget >= 0)
);

-- Genres table (normalized)
CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Production Companies table (normalized)
CREATE TABLE production_companies (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(100),
    founded_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT check_founded_year CHECK (founded_year IS NULL OR founded_year BETWEEN 1800 AND 2100)
);

-- People table (actors, directors, crew)
CREATE TABLE people (
    person_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender SMALLINT DEFAULT 0,
    -- 0: Unknown, 1: Female, 2: Male, 3: Non-binary
    birthday DATE,
    death_day DATE,
    biography TEXT,
    profile_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT check_gender_value CHECK (gender IN (0, 1, 2, 3)),
    CONSTRAINT check_death_after_birth CHECK (death_day IS NULL OR death_day >= birthday)
);

-- Keywords table
CREATE TABLE keywords (
    keyword_id SERIAL PRIMARY KEY,
    keyword_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Junction Tables (Many-to-Many Relationships)
-- ============================================================================

-- Movie-Genre relationship
CREATE TABLE movie_genres (
    movie_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

-- Movie-Production Company relationship
CREATE TABLE movie_production_companies (
    movie_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (movie_id, company_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (company_id) REFERENCES production_companies(company_id) ON DELETE CASCADE
);

-- Movie-Cast relationship (actors)
CREATE TABLE movie_cast (
    id SERIAL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    character_name VARCHAR(500),
    cast_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (person_id) REFERENCES people(person_id) ON DELETE CASCADE,
    -- Unique constraint: same actor can't play same character twice in same movie
    UNIQUE (movie_id, person_id, character_name)
);

-- Movie-Crew relationship (directors, producers, etc.)
CREATE TABLE movie_crew (
    id SERIAL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    job VARCHAR(255) NOT NULL,
    -- e.g., "Director", "Producer", "Writer"
    department VARCHAR(255),
    -- e.g., "Directing", "Production", "Writing"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (person_id) REFERENCES people(person_id) ON DELETE CASCADE
);

-- Movie-Keywords relationship
CREATE TABLE movie_keywords (
    movie_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (movie_id, keyword_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id) ON DELETE CASCADE
);

-- Ratings table (user ratings)
CREATE TABLE ratings (
    rating_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating DECIMAL(2, 1) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    review TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    -- Unique constraint: one rating per user per movie
    UNIQUE (user_id, movie_id),
    -- Constraints
    CONSTRAINT check_rating_range CHECK (rating >= 0.5 AND rating <= 5.0)
);

-- ============================================================================
-- E-Commerce Tables (for transactional demo)
-- ============================================================================

-- Orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    -- pending, processing, completed, cancelled
    total_amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    -- Constraints
    CONSTRAINT check_status_value CHECK (status IN ('pending', 'processing', 'completed', 'cancelled', 'refunded')),
    CONSTRAINT check_total_amount_positive CHECK (total_amount >= 0)
);

-- Order Items table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE RESTRICT,
    -- Constraints
    CONSTRAINT check_quantity_positive CHECK (quantity > 0),
    CONSTRAINT check_unit_price_positive CHECK (unit_price >= 0),
    CONSTRAINT check_subtotal_match CHECK (subtotal = quantity * unit_price)
);

-- ============================================================================
-- Indexes (Advanced Features)
-- ============================================================================

-- B-tree indexes on foreign keys (performance)
CREATE INDEX idx_ratings_user_id ON ratings(user_id);
CREATE INDEX idx_ratings_movie_id ON ratings(movie_id);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_movie_cast_movie_id ON movie_cast(movie_id);
CREATE INDEX idx_movie_cast_person_id ON movie_cast(person_id);
CREATE INDEX idx_movie_crew_movie_id ON movie_crew(movie_id);
CREATE INDEX idx_movie_crew_person_id ON movie_crew(person_id);

-- Composite index for common query pattern (status + date filtering)
CREATE INDEX idx_orders_status_date ON orders(status, order_date DESC);

-- Partial index for high ratings only (space-efficient)
CREATE INDEX idx_ratings_high_ratings ON ratings(movie_id, rating)
WHERE rating >= 4.0;

-- GIN index on JSONB metadata for flexible queries
CREATE INDEX idx_movies_metadata_gin ON movies USING GIN (metadata);

-- Full-text search index on movie titles and overviews
CREATE INDEX idx_movies_title_trgm ON movies USING GIN (title gin_trgm_ops);
CREATE INDEX idx_movies_overview_trgm ON movies USING GIN (overview gin_trgm_ops);

-- Index for popularity-based sorting
CREATE INDEX idx_movies_popularity_desc ON movies(popularity DESC NULLS LAST);

-- Index for release date queries
CREATE INDEX idx_movies_released_date ON movies(released_date DESC NULLS LAST);

-- Index for people name search
CREATE INDEX idx_people_name_trgm ON people USING GIN (name gin_trgm_ops);

-- ============================================================================
-- Triggers for automatic timestamp updates
-- ============================================================================

-- Function to update 'updated_at' timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to relevant tables
CREATE TRIGGER update_movies_updated_at
    BEFORE UPDATE ON movies
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_people_updated_at
    BEFORE UPDATE ON people
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Comments for documentation
-- ============================================================================

COMMENT ON TABLE movies IS 'Core movie entity with JSONB metadata for flexible attributes';
COMMENT ON TABLE users IS 'Application users for authentication and order management';
COMMENT ON TABLE orders IS 'Purchase orders demonstrating transactional integrity';
COMMENT ON TABLE ratings IS 'User ratings with unique constraint per user-movie pair';
COMMENT ON COLUMN movies.metadata IS 'JSONB field for flexible movie attributes (awards, certifications, etc.)';
COMMENT ON INDEX idx_ratings_high_ratings IS 'Partial index optimized for high-rated movies queries';
COMMENT ON INDEX idx_movies_metadata_gin IS 'GIN index enabling fast JSONB queries on metadata';

-- ============================================================================
-- End of Schema
-- ============================================================================

-- Verify table creation
SELECT 'Schema created successfully. Total tables: ' || COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
