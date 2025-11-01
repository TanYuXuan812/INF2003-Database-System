-- ============================================================================
-- INF2003 Database System - Views and Advanced SQL Constructs
-- ============================================================================
-- This file demonstrates:
-- - Views for common aggregations
-- - Materialized views for performance
-- - CTEs (Common Table Expressions)
-- - Window functions (RANK, DENSE_RANK, LAG, ROW_NUMBER)
-- - Complex aggregations and JOINs
-- ============================================================================

-- ============================================================================
-- Regular Views
-- ============================================================================

-- View: Movie statistics with aggregated ratings and genre information
CREATE OR REPLACE VIEW v_movie_stats AS
SELECT
    m.movie_id,
    m.title,
    m.released_date,
    m.runtime,
    m.popularity,
    m.revenue,
    m.budget,
    COALESCE(AVG(r.rating), 0) AS avg_rating,
    COUNT(DISTINCT r.rating_id) AS rating_count,
    STRING_AGG(DISTINCT g.genre_name, ', ' ORDER BY g.genre_name) AS genres,
    COUNT(DISTINCT mc.id) AS cast_count,
    COUNT(DISTINCT mcr.id) AS crew_count
FROM movies m
LEFT JOIN ratings r ON m.movie_id = r.movie_id
LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
LEFT JOIN genres g ON mg.genre_id = g.genre_id
LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
LEFT JOIN movie_crew mcr ON m.movie_id = mcr.movie_id
GROUP BY m.movie_id, m.title, m.released_date, m.runtime, m.popularity, m.revenue, m.budget;

COMMENT ON VIEW v_movie_stats IS 'Comprehensive movie statistics including ratings, genres, and cast/crew counts';

-- View: Top-rated movies (ratings >= 4.0 with at least 10 ratings)
CREATE OR REPLACE VIEW v_top_rated_movies AS
SELECT
    m.movie_id,
    m.title,
    m.released_date,
    m.poster_path,
    AVG(r.rating) AS avg_rating,
    COUNT(r.rating_id) AS rating_count,
    STRING_AGG(DISTINCT g.genre_name, ', ' ORDER BY g.genre_name) AS genres
FROM movies m
INNER JOIN ratings r ON m.movie_id = r.movie_id
LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
LEFT JOIN genres g ON mg.genre_id = g.genre_id
GROUP BY m.movie_id, m.title, m.released_date, m.poster_path
HAVING AVG(r.rating) >= 4.0 AND COUNT(r.rating_id) >= 10
ORDER BY avg_rating DESC, rating_count DESC;

COMMENT ON VIEW v_top_rated_movies IS 'Movies with average rating >= 4.0 and at least 10 ratings';

-- View: Genre popularity and statistics
CREATE OR REPLACE VIEW v_genre_stats AS
SELECT
    g.genre_id,
    g.genre_name,
    COUNT(DISTINCT mg.movie_id) AS movie_count,
    AVG(m.popularity) AS avg_movie_popularity,
    COUNT(DISTINCT r.rating_id) AS total_ratings,
    COALESCE(AVG(r.rating), 0) AS avg_rating,
    SUM(m.revenue) AS total_revenue,
    SUM(m.budget) AS total_budget
FROM genres g
LEFT JOIN movie_genres mg ON g.genre_id = mg.genre_id
LEFT JOIN movies m ON mg.movie_id = m.movie_id
LEFT JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY g.genre_id, g.genre_name
ORDER BY movie_count DESC;

COMMENT ON VIEW v_genre_stats IS 'Genre popularity metrics including movie count, ratings, and revenue';

-- View: User activity summary
CREATE OR REPLACE VIEW v_user_activity AS
SELECT
    u.user_id,
    u.username,
    u.email,
    u.created_at AS joined_date,
    u.last_login,
    COUNT(DISTINCT r.rating_id) AS total_ratings,
    COALESCE(AVG(r.rating), 0) AS avg_rating_given,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_value
FROM users u
LEFT JOIN ratings r ON u.user_id = r.user_id
LEFT JOIN orders o ON u.user_id = o.user_id AND o.status = 'completed'
GROUP BY u.user_id, u.username, u.email, u.created_at, u.last_login;

COMMENT ON VIEW v_user_activity IS 'User engagement metrics including ratings and purchase history';

-- View: Production company performance
CREATE OR REPLACE VIEW v_company_stats AS
SELECT
    pc.company_id,
    pc.name AS company_name,
    pc.country,
    COUNT(DISTINCT mpc.movie_id) AS movie_count,
    SUM(m.revenue) AS total_revenue,
    SUM(m.budget) AS total_budget,
    CASE
        WHEN SUM(m.budget) > 0 THEN (SUM(m.revenue) - SUM(m.budget)) / SUM(m.budget) * 100
        ELSE 0
    END AS roi_percentage,
    AVG(m.popularity) AS avg_movie_popularity,
    COALESCE(AVG(r.rating), 0) AS avg_movie_rating
FROM production_companies pc
LEFT JOIN movie_production_companies mpc ON pc.company_id = mpc.company_id
LEFT JOIN movies m ON mpc.movie_id = m.movie_id
LEFT JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY pc.company_id, pc.name, pc.country
HAVING COUNT(DISTINCT mpc.movie_id) > 0
ORDER BY total_revenue DESC NULLS LAST;

COMMENT ON VIEW v_company_stats IS 'Production company performance metrics including ROI and ratings';

-- ============================================================================
-- Materialized Views (for performance)
-- ============================================================================

-- Materialized View: Daily KPIs dashboard
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_kpis AS
SELECT
    CURRENT_DATE AS report_date,
    -- User metrics
    (SELECT COUNT(*) FROM users) AS total_users,
    (SELECT COUNT(*) FROM users WHERE created_at::date = CURRENT_DATE) AS new_users_today,
    (SELECT COUNT(*) FROM users WHERE last_login::date = CURRENT_DATE) AS active_users_today,
    -- Movie metrics
    (SELECT COUNT(*) FROM movies) AS total_movies,
    (SELECT COUNT(*) FROM ratings) AS total_ratings,
    (SELECT ROUND(AVG(rating)::numeric, 2) FROM ratings) AS avg_rating_overall,
    -- Order metrics
    (SELECT COUNT(*) FROM orders) AS total_orders,
    (SELECT COUNT(*) FROM orders WHERE order_date::date = CURRENT_DATE) AS orders_today,
    (SELECT COALESCE(SUM(total_amount), 0) FROM orders WHERE status = 'completed') AS total_revenue,
    (SELECT COALESCE(SUM(total_amount), 0) FROM orders WHERE order_date::date = CURRENT_DATE AND status = 'completed') AS revenue_today;

CREATE UNIQUE INDEX ON mv_daily_kpis(report_date);

COMMENT ON MATERIALIZED VIEW mv_daily_kpis IS 'Daily dashboard KPIs (refresh periodically for performance)';

-- Materialized View: Monthly revenue analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_revenue AS
SELECT
    DATE_TRUNC('month', o.order_date) AS month,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT o.user_id) AS unique_customers,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    SUM(oi.quantity) AS total_items_sold
FROM orders o
INNER JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status = 'completed'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY month DESC;

CREATE UNIQUE INDEX ON mv_monthly_revenue(month);

COMMENT ON MATERIALIZED VIEW mv_monthly_revenue IS 'Monthly revenue and sales metrics (refresh monthly)';

-- ============================================================================
-- Sample Queries with Advanced SQL Features
-- ============================================================================

-- Sample 1: Top 10 movies per genre using window functions (DENSE_RANK)
CREATE OR REPLACE VIEW v_top_movies_per_genre AS
WITH movie_genre_ratings AS (
    SELECT
        m.movie_id,
        m.title,
        g.genre_name,
        AVG(r.rating) AS avg_rating,
        COUNT(r.rating_id) AS rating_count,
        DENSE_RANK() OVER (
            PARTITION BY g.genre_name
            ORDER BY AVG(r.rating) DESC, COUNT(r.rating_id) DESC
        ) AS genre_rank
    FROM movies m
    INNER JOIN movie_genres mg ON m.movie_id = mg.movie_id
    INNER JOIN genres g ON mg.genre_id = g.genre_id
    INNER JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title, g.genre_name
    HAVING COUNT(r.rating_id) >= 5
)
SELECT
    genre_name,
    movie_id,
    title,
    avg_rating,
    rating_count,
    genre_rank
FROM movie_genre_ratings
WHERE genre_rank <= 10
ORDER BY genre_name, genre_rank;

COMMENT ON VIEW v_top_movies_per_genre IS 'Top 10 movies per genre using DENSE_RANK window function';

-- Sample 2: User rating trends with LAG window function
CREATE OR REPLACE VIEW v_user_rating_trends AS
SELECT
    r.user_id,
    u.username,
    r.movie_id,
    m.title,
    r.rating,
    r.timestamp,
    LAG(r.rating, 1) OVER (PARTITION BY r.user_id ORDER BY r.timestamp) AS previous_rating,
    r.rating - LAG(r.rating, 1) OVER (PARTITION BY r.user_id ORDER BY r.timestamp) AS rating_change,
    ROW_NUMBER() OVER (PARTITION BY r.user_id ORDER BY r.timestamp DESC) AS recent_rank
FROM ratings r
INNER JOIN users u ON r.user_id = u.user_id
INNER JOIN movies m ON r.movie_id = m.movie_id
ORDER BY r.user_id, r.timestamp DESC;

COMMENT ON VIEW v_user_rating_trends IS 'User rating patterns showing changes over time using LAG window function';

-- Sample 3: Running total of revenue per user (cumulative sum)
CREATE OR REPLACE VIEW v_user_spending_cumulative AS
SELECT
    o.user_id,
    u.username,
    o.order_id,
    o.order_date,
    o.total_amount,
    SUM(o.total_amount) OVER (
        PARTITION BY o.user_id
        ORDER BY o.order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_spending
FROM orders o
INNER JOIN users u ON o.user_id = u.user_id
WHERE o.status = 'completed'
ORDER BY o.user_id, o.order_date;

COMMENT ON VIEW v_user_spending_cumulative IS 'Running total of user spending using window functions';

-- ============================================================================
-- Stored Procedure Example: Transactional Order Placement
-- ============================================================================

-- Function to create an order with items transactionally
CREATE OR REPLACE FUNCTION create_order_with_items(
    p_user_id INTEGER,
    p_items JSONB, -- Format: [{"movie_id": 123, "quantity": 1, "unit_price": 9.99}, ...]
    p_payment_method VARCHAR(50),
    p_shipping_address TEXT
)
RETURNS INTEGER AS $$
DECLARE
    v_order_id INTEGER;
    v_total_amount DECIMAL(10, 2) := 0;
    v_item JSONB;
BEGIN
    -- Start transaction (implicit in function)

    -- Create the order
    INSERT INTO orders (user_id, status, total_amount, payment_method, shipping_address)
    VALUES (p_user_id, 'pending', 0, p_payment_method, p_shipping_address)
    RETURNING order_id INTO v_order_id;

    -- Insert order items and calculate total
    FOR v_item IN SELECT * FROM jsonb_array_elements(p_items)
    LOOP
        DECLARE
            v_movie_id INTEGER := (v_item->>'movie_id')::INTEGER;
            v_quantity INTEGER := (v_item->>'quantity')::INTEGER;
            v_unit_price DECIMAL(10, 2) := (v_item->>'unit_price')::DECIMAL(10, 2);
            v_subtotal DECIMAL(10, 2) := v_quantity * v_unit_price;
        BEGIN
            INSERT INTO order_items (order_id, movie_id, quantity, unit_price, subtotal)
            VALUES (v_order_id, v_movie_id, v_quantity, v_unit_price, v_subtotal);

            v_total_amount := v_total_amount + v_subtotal;
        END;
    END LOOP;

    -- Update order total
    UPDATE orders SET total_amount = v_total_amount WHERE order_id = v_order_id;

    -- Return the created order ID
    RETURN v_order_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION create_order_with_items IS 'Transactionally create an order with multiple items';

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_kpis;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_revenue;
    RAISE NOTICE 'All materialized views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_all_materialized_views IS 'Refresh all materialized views (run via cron or manually)';

-- ============================================================================
-- Sample CTE Queries (for demonstration in API)
-- ============================================================================

-- CTE Example 1: Recursive genre recommendations
-- (Find movies in same genres as highly-rated movies by user)
CREATE OR REPLACE VIEW v_genre_based_recommendations AS
WITH user_favorite_genres AS (
    SELECT
        r.user_id,
        g.genre_id,
        g.genre_name,
        AVG(r.rating) AS avg_rating
    FROM ratings r
    INNER JOIN movies m ON r.movie_id = m.movie_id
    INNER JOIN movie_genres mg ON m.movie_id = mg.movie_id
    INNER JOIN genres g ON mg.genre_id = g.genre_id
    WHERE r.rating >= 4.0
    GROUP BY r.user_id, g.genre_id, g.genre_name
),
recommended_movies AS (
    SELECT
        ufg.user_id,
        m.movie_id,
        m.title,
        m.poster_path,
        ufg.genre_name,
        AVG(r.rating) AS avg_rating,
        COUNT(r.rating_id) AS rating_count
    FROM user_favorite_genres ufg
    INNER JOIN movie_genres mg ON ufg.genre_id = mg.genre_id
    INNER JOIN movies m ON mg.movie_id = m.movie_id
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    WHERE NOT EXISTS (
        SELECT 1 FROM ratings ur
        WHERE ur.user_id = ufg.user_id AND ur.movie_id = m.movie_id
    )
    GROUP BY ufg.user_id, m.movie_id, m.title, m.poster_path, ufg.genre_name
    HAVING COUNT(r.rating_id) >= 5
)
SELECT
    user_id,
    movie_id,
    title,
    poster_path,
    genre_name,
    avg_rating,
    rating_count,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY avg_rating DESC, rating_count DESC) AS recommendation_rank
FROM recommended_movies
ORDER BY user_id, recommendation_rank;

COMMENT ON VIEW v_genre_based_recommendations IS 'Genre-based movie recommendations using CTEs';

-- ============================================================================
-- End of Views
-- ============================================================================

SELECT 'Views and advanced SQL constructs created successfully.' AS status;
