-- Enable pg_trgm extension for efficient ILIKE/fuzzy search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ================================================
-- DROP STATEMENTS
-- ================================================
DROP TABLE IF EXISTS movie_crew CASCADE;
DROP TABLE IF EXISTS movie_cast CASCADE;
DROP TABLE IF EXISTS movie_keywords CASCADE;
DROP TABLE IF EXISTS movie_production_companies CASCADE;
DROP TABLE IF EXISTS movie_genres CASCADE;
DROP TABLE IF EXISTS ratings CASCADE;
DROP TABLE IF EXISTS movies CASCADE;
DROP TABLE IF EXISTS keywords CASCADE;
DROP TABLE IF EXISTS genres CASCADE;
DROP TABLE IF EXISTS production_companies CASCADE;
DROP TABLE IF EXISTS people CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- ================================================
-- TABLE CREATION
-- ================================================

-- 1. Movies Table
CREATE TABLE IF NOT EXISTS movies(
 movie_id INT PRIMARY KEY CHECK (movie_id > 0),
 title VARCHAR(255) NOT NULL,
 adult BOOLEAN DEFAULT FALSE,
 overview TEXT,
 language VARCHAR(50),
 popularity DECIMAL(10,2),
 released_date DATE,
 runtime INTEGER,
 poster_path TEXT,
 tagline TEXT,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Genres Table
CREATE TABLE IF NOT EXISTS genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Production Companies Table
CREATE TABLE IF NOT EXISTS production_companies (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Ratings Table
CREATE TABLE IF NOT EXISTS ratings (
    rating_id SERIAL PRIMARY KEY,
    user_id INTEGER,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    rating DECIMAL(3,1) CHECK (rating >= 0 AND rating <= 10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, movie_id)
);

-- 5. Keywords Table
CREATE TABLE IF NOT EXISTS keywords (
    keyword_id SERIAL PRIMARY KEY,
    keyword_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. People Table
CREATE TABLE IF NOT EXISTS people (
    person_id SERIAL PRIMARY KEY,
    tmdb_id INT UNIQUE,
    name VARCHAR(255),
    gender INT,
    profile_path TEXT
);

-- 7. Users Table
CREATE TABLE IF NOT EXISTS users(
  user_id INTEGER PRIMARY KEY,
  username VARCHAR(50) UNIQUE,
  password_hash VARCHAR(255),
  role VARCHAR(20) DEFAULT 'user', -- Changed custom type to VARCHAR for portability
  is_synthetic BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================================
-- JUNCTION TABLES
-- ================================================

-- Movie Genres
CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INT REFERENCES movies(movie_id) ON DELETE CASCADE,
    genre_id INT REFERENCES genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY(movie_id, genre_id)
);

-- Movie Production Companies
CREATE TABLE IF NOT EXISTS movie_production_companies (
    movie_id INT REFERENCES movies(movie_id) ON DELETE CASCADE,
    company_id INT REFERENCES production_companies(company_id) ON DELETE CASCADE,
    PRIMARY KEY(movie_id, company_id)
);

-- Movie Keywords
CREATE TABLE IF NOT EXISTS movie_keywords (
    movie_id INT REFERENCES movies(movie_id) ON DELETE CASCADE,
    keyword_id INT REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, keyword_id)
);

-- Movie Cast
CREATE TABLE IF NOT EXISTS movie_cast (
    movie_id INT REFERENCES movies(movie_id) ON DELETE CASCADE,
    person_id INT REFERENCES people(person_id),
    character VARCHAR(255),
    cast_order INT,
    credit_id VARCHAR(50) UNIQUE,
    PRIMARY KEY (movie_id, person_id)
);

-- Movie Crew
CREATE TABLE IF NOT EXISTS movie_crew (
    movie_id INT REFERENCES movies(movie_id) ON DELETE CASCADE,
    person_id INT REFERENCES people(person_id),
    department VARCHAR(100),
    job VARCHAR(100),
    credit_id VARCHAR(50) UNIQUE,
    PRIMARY KEY (movie_id, person_id, job)
);

-- ================================================
-- PERFORMANCE INDEXES
-- ================================================

-- [MOVIES]
-- Support for: admin_search_movies_by_title (ILIKE search)
-- Support for: user_query.search_movies_by_title_detailed
CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops);

-- Support for: search_movies_by_date_range, sorting by release date
CREATE INDEX IF NOT EXISTS idx_movies_released_date ON movies(released_date);

-- Support for: sorting by popularity in user_query
CREATE INDEX IF NOT EXISTS idx_movies_popularity ON movies(popularity DESC);


-- [PEOPLE]
-- Support for: admin_search_people_by_name (ILIKE search)
-- Support for: search_movies_by_actor, search_movies_by_crew
CREATE INDEX IF NOT EXISTS idx_people_name_trgm ON people USING gin (name gin_trgm_ops);


-- [GENRES & COMPANIES & KEYWORDS]
-- Support for fuzzy searching in admin panel
CREATE INDEX IF NOT EXISTS idx_genres_name_trgm ON genres USING gin (genre_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_companies_name_trgm ON production_companies USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_keywords_name_trgm ON keywords USING gin (keyword_name gin_trgm_ops);


-- [RATINGS]
-- Support for: get_movie_full_details (AVG rating joins)
-- Support for: search_movies_by_rating
CREATE INDEX IF NOT EXISTS idx_ratings_movie_id ON ratings(movie_id);

-- Support for: admin_read_ratings (pagination by created_at)
CREATE INDEX IF NOT EXISTS idx_ratings_created_at ON ratings(created_at DESC);


-- [JUNCTION TABLE FOREIGN KEYS]
-- Note: Primary Keys (movie_id, other_id) automatically index the first column (movie_id),
-- but we usually need an index on the second column to search "Movies BY Genre" or "Movies BY Actor".

-- Support for: search_movies_by_genre
CREATE INDEX IF NOT EXISTS idx_movie_genres_genre_id ON movie_genres(genre_id);

-- Support for: search_movies_by_production_company
CREATE INDEX IF NOT EXISTS idx_movie_companies_company_id ON movie_production_companies(company_id);

-- Support for: search_movies_by_keyword
CREATE INDEX IF NOT EXISTS idx_movie_keywords_keyword_id ON movie_keywords(keyword_id);

-- Support for: search_movies_by_actor (Joining people -> movie_cast -> movies)
CREATE INDEX IF NOT EXISTS idx_movie_cast_person_id ON movie_cast(person_id);

-- Support for: search_movies_by_crew
CREATE INDEX IF NOT EXISTS idx_movie_crew_person_id ON movie_crew(person_id);