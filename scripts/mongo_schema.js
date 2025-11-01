// ============================================================================
// INF2003 Database System - MongoDB Schema and Indexes
// ============================================================================
// This script demonstrates:
// - Multiple collections with flexible schemas
// - Compound indexes for query optimization
// - TTL indexes for automatic document expiry
// - Text indexes for full-text search
// - Aggregation pipeline examples
// ============================================================================

// Connect to database
db = db.getSiblingDB('moviedb_mongo');

print('Setting up MongoDB database: moviedb_mongo');

// ============================================================================
// Drop existing collections (for idempotent execution)
// ============================================================================

db.events.drop();
db.user_profiles.drop();
db.movie_aggregates.drop();
db.sessions.drop();
db.recommendations.drop();

print('Dropped existing collections');

// ============================================================================
// Collection 1: Events (Telemetry and User Interactions)
// ============================================================================

// Create events collection with schema validation
db.createCollection('events', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['type', 'timestamp'],
            properties: {
                type: {
                    bsonType: 'string',
                    enum: ['page_view', 'search', 'click', 'add_to_cart', 'remove_from_cart', 'checkout', 'purchase', 'rating_submitted'],
                    description: 'Event type must be one of the enum values'
                },
                user_id: {
                    bsonType: ['int', 'null'],
                    description: 'User ID from PostgreSQL (nullable for anonymous users)'
                },
                movie_id: {
                    bsonType: ['int', 'null'],
                    description: 'Movie ID from PostgreSQL (nullable for non-movie events)'
                },
                timestamp: {
                    bsonType: 'date',
                    description: 'Event timestamp'
                },
                session_id: {
                    bsonType: 'string',
                    description: 'Session identifier for grouping events'
                },
                metadata: {
                    bsonType: 'object',
                    description: 'Flexible metadata specific to event type'
                },
                ip_address: {
                    bsonType: 'string',
                    description: 'User IP address for analytics'
                },
                user_agent: {
                    bsonType: 'string',
                    description: 'Browser user agent string'
                }
            }
        }
    }
});

// Compound index for event type and timestamp queries
db.events.createIndex(
    { type: 1, timestamp: -1 },
    { name: 'idx_type_timestamp', background: true }
);

// Index for user-specific event queries
db.events.createIndex(
    { user_id: 1, timestamp: -1 },
    { name: 'idx_user_timestamp', background: true }
);

// Index for movie-specific event queries
db.events.createIndex(
    { movie_id: 1, type: 1 },
    { name: 'idx_movie_type', background: true }
);

// Index for session-based queries
db.events.createIndex(
    { session_id: 1, timestamp: 1 },
    { name: 'idx_session_timestamp', background: true }
);

// TTL index - automatically delete events older than 30 days
db.events.createIndex(
    { timestamp: 1 },
    { name: 'idx_ttl_timestamp', expireAfterSeconds: 2592000, background: true }
);

print('Created events collection with indexes (including TTL)');

// ============================================================================
// Collection 2: User Profiles (Denormalized User Preferences)
// ============================================================================

db.createCollection('user_profiles', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['user_id', 'last_updated'],
            properties: {
                user_id: {
                    bsonType: 'int',
                    description: 'User ID from PostgreSQL'
                },
                preferences: {
                    bsonType: 'object',
                    properties: {
                        favorite_genres: {
                            bsonType: 'array',
                            items: { bsonType: 'string' }
                        },
                        favorite_actors: {
                            bsonType: 'array',
                            items: { bsonType: 'int' }
                        },
                        viewing_history: {
                            bsonType: 'array',
                            items: {
                                bsonType: 'object',
                                properties: {
                                    movie_id: { bsonType: 'int' },
                                    viewed_at: { bsonType: 'date' },
                                    duration_seconds: { bsonType: 'int' }
                                }
                            }
                        },
                        language_preferences: {
                            bsonType: 'array',
                            items: { bsonType: 'string' }
                        }
                    }
                },
                recommendations: {
                    bsonType: 'array',
                    description: 'Pre-computed movie recommendations',
                    items: {
                        bsonType: 'object',
                        properties: {
                            movie_id: { bsonType: 'int' },
                            score: { bsonType: 'double' },
                            reason: { bsonType: 'string' }
                        }
                    }
                },
                activity_summary: {
                    bsonType: 'object',
                    properties: {
                        total_views: { bsonType: 'int' },
                        total_searches: { bsonType: 'int' },
                        total_ratings: { bsonType: 'int' },
                        total_purchases: { bsonType: 'int' },
                        last_active: { bsonType: 'date' }
                    }
                },
                last_updated: {
                    bsonType: 'date',
                    description: 'Last profile update timestamp'
                }
            }
        }
    }
});

// Unique index on user_id
db.user_profiles.createIndex(
    { user_id: 1 },
    { name: 'idx_user_id', unique: true, background: true }
);

// Index for last updated queries
db.user_profiles.createIndex(
    { last_updated: -1 },
    { name: 'idx_last_updated', background: true }
);

print('Created user_profiles collection with indexes');

// ============================================================================
// Collection 3: Movie Aggregates (Pre-computed Analytics)
// ============================================================================

db.createCollection('movie_aggregates', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['movie_id', 'computed_at'],
            properties: {
                movie_id: {
                    bsonType: 'int',
                    description: 'Movie ID from PostgreSQL'
                },
                views: {
                    bsonType: 'object',
                    properties: {
                        hourly: { bsonType: 'object' },
                        daily: { bsonType: 'object' },
                        total: { bsonType: 'int' }
                    }
                },
                engagement: {
                    bsonType: 'object',
                    properties: {
                        click_rate: { bsonType: 'double' },
                        add_to_cart_rate: { bsonType: 'double' },
                        conversion_rate: { bsonType: 'double' }
                    }
                },
                trending_score: {
                    bsonType: 'double',
                    description: 'Calculated trending score based on recent activity'
                },
                computed_at: {
                    bsonType: 'date',
                    description: 'When these metrics were last computed'
                }
            }
        }
    }
});

// Unique index on movie_id
db.movie_aggregates.createIndex(
    { movie_id: 1 },
    { name: 'idx_movie_id', unique: true, background: true }
);

// Index for trending queries
db.movie_aggregates.createIndex(
    { trending_score: -1, computed_at: -1 },
    { name: 'idx_trending', background: true }
);

print('Created movie_aggregates collection with indexes');

// ============================================================================
// Collection 4: Sessions (User Session Tracking)
// ============================================================================

db.createCollection('sessions', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['session_id', 'started_at'],
            properties: {
                session_id: {
                    bsonType: 'string',
                    description: 'Unique session identifier'
                },
                user_id: {
                    bsonType: ['int', 'null'],
                    description: 'User ID if logged in'
                },
                started_at: {
                    bsonType: 'date',
                    description: 'Session start timestamp'
                },
                ended_at: {
                    bsonType: ['date', 'null'],
                    description: 'Session end timestamp'
                },
                duration_seconds: {
                    bsonType: 'int',
                    description: 'Total session duration'
                },
                page_views: {
                    bsonType: 'int',
                    description: 'Number of page views in session'
                },
                device_info: {
                    bsonType: 'object',
                    properties: {
                        device_type: { bsonType: 'string' },
                        browser: { bsonType: 'string' },
                        os: { bsonType: 'string' }
                    }
                },
                referrer: {
                    bsonType: 'string',
                    description: 'How user arrived at the site'
                }
            }
        }
    }
});

// Unique index on session_id
db.sessions.createIndex(
    { session_id: 1 },
    { name: 'idx_session_id', unique: true, background: true }
);

// Index for user sessions
db.sessions.createIndex(
    { user_id: 1, started_at: -1 },
    { name: 'idx_user_started', background: true }
);

// TTL index - delete sessions older than 90 days
db.sessions.createIndex(
    { started_at: 1 },
    { name: 'idx_ttl_started', expireAfterSeconds: 7776000, background: true }
);

print('Created sessions collection with indexes (including TTL)');

// ============================================================================
// Collection 5: Recommendations Cache
// ============================================================================

db.createCollection('recommendations', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['user_id', 'movies', 'generated_at'],
            properties: {
                user_id: {
                    bsonType: 'int',
                    description: 'User ID from PostgreSQL'
                },
                movies: {
                    bsonType: 'array',
                    description: 'Array of recommended movies',
                    items: {
                        bsonType: 'object',
                        required: ['movie_id', 'score'],
                        properties: {
                            movie_id: { bsonType: 'int' },
                            score: { bsonType: 'double' },
                            reason: { bsonType: 'string' }
                        }
                    }
                },
                algorithm: {
                    bsonType: 'string',
                    description: 'Algorithm used for recommendations'
                },
                generated_at: {
                    bsonType: 'date',
                    description: 'When recommendations were generated'
                }
            }
        }
    }
});

// Index on user_id
db.recommendations.createIndex(
    { user_id: 1, generated_at: -1 },
    { name: 'idx_user_generated', background: true }
);

// TTL index - expire recommendations after 24 hours
db.recommendations.createIndex(
    { generated_at: 1 },
    { name: 'idx_ttl_generated', expireAfterSeconds: 86400, background: true }
);

print('Created recommendations collection with indexes');

// ============================================================================
// Text Search Indexes
// ============================================================================

// Text index on event metadata for flexible search
db.events.createIndex(
    { 'metadata.search_query': 'text', 'metadata.keywords': 'text' },
    { name: 'idx_event_text_search', background: true }
);

print('Created text search indexes');

// ============================================================================
// Sample Data (for testing)
// ============================================================================

// Insert sample events
db.events.insertMany([
    {
        type: 'page_view',
        user_id: 1,
        movie_id: 550,
        timestamp: new Date(),
        session_id: 'session_001',
        metadata: { page: 'movie_detail', duration_ms: 5000 },
        ip_address: '192.168.1.1',
        user_agent: 'Mozilla/5.0'
    },
    {
        type: 'search',
        user_id: 1,
        movie_id: null,
        timestamp: new Date(),
        session_id: 'session_001',
        metadata: { search_query: 'action movies', results_count: 50 },
        ip_address: '192.168.1.1',
        user_agent: 'Mozilla/5.0'
    },
    {
        type: 'add_to_cart',
        user_id: 1,
        movie_id: 550,
        timestamp: new Date(),
        session_id: 'session_001',
        metadata: { price: 9.99, quantity: 1 },
        ip_address: '192.168.1.1',
        user_agent: 'Mozilla/5.0'
    }
]);

print('Inserted sample event data');

// Insert sample user profile
db.user_profiles.insertOne({
    user_id: 1,
    preferences: {
        favorite_genres: ['Action', 'Science Fiction', 'Thriller'],
        favorite_actors: [2, 3, 4],
        viewing_history: [
            { movie_id: 550, viewed_at: new Date(), duration_seconds: 7200 }
        ],
        language_preferences: ['en', 'es']
    },
    recommendations: [
        { movie_id: 551, score: 0.95, reason: 'Similar to Fight Club' },
        { movie_id: 552, score: 0.89, reason: 'Top Action movie' }
    ],
    activity_summary: {
        total_views: 15,
        total_searches: 8,
        total_ratings: 5,
        total_purchases: 2,
        last_active: new Date()
    },
    last_updated: new Date()
});

print('Inserted sample user profile');

// Insert sample session
db.sessions.insertOne({
    session_id: 'session_001',
    user_id: 1,
    started_at: new Date(Date.now() - 3600000), // 1 hour ago
    ended_at: new Date(),
    duration_seconds: 3600,
    page_views: 12,
    device_info: {
        device_type: 'desktop',
        browser: 'Chrome',
        os: 'Windows 10'
    },
    referrer: 'google.com'
});

print('Inserted sample session');

// ============================================================================
// Aggregation Pipeline Templates (Saved as Views/Functions)
// ============================================================================

// Note: MongoDB doesn't have "stored procedures" like SQL, but we can
// document common aggregation pipelines here for use in the API

print('='.repeat(80));
print('MongoDB Schema Setup Complete');
print('='.repeat(80));
print('Collections created: events, user_profiles, movie_aggregates, sessions, recommendations');
print('Indexes created: Compound, TTL, Text, Unique');
print('Sample data inserted for testing');
print('='.repeat(80));

// Show collections and their indexes
print('\nCollections and Index Counts:');
db.getCollectionNames().forEach(function(collName) {
    var coll = db.getCollection(collName);
    var indexCount = coll.getIndexes().length;
    print('  ' + collName + ': ' + indexCount + ' indexes');
});
