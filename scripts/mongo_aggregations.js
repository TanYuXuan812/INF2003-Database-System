// ============================================================================
// INF2003 Database System - MongoDB Aggregation Pipelines
// ============================================================================
// This file contains advanced aggregation pipeline examples demonstrating:
// - $match, $group, $sort, $project
// - $lookup (cross-collection joins)
// - $facet (multi-dimensional aggregations)
// - $bucket (time-series bucketing)
// - $unwind (array expansion)
// - Complex analytics queries
// ============================================================================

db = db.getSiblingDB('moviedb_mongo');

print('MongoDB Aggregation Pipeline Examples\n');

// ============================================================================
// Pipeline 1: Event Funnel Analysis
// ============================================================================

print('Pipeline 1: Event Funnel Analysis');
print('-'.repeat(80));

const funnelPipeline = [
    // Stage 1: Filter events from last 30 days
    {
        $match: {
            timestamp: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }
        }
    },
    // Stage 2: Group by event type and count
    {
        $group: {
            _id: '$type',
            count: { $sum: 1 },
            unique_users: { $addToSet: '$user_id' },
            unique_movies: { $addToSet: '$movie_id' }
        }
    },
    // Stage 3: Project with calculated fields
    {
        $project: {
            event_type: '$_id',
            count: 1,
            unique_user_count: { $size: '$unique_users' },
            unique_movie_count: { $size: '$unique_movies' },
            _id: 0
        }
    },
    // Stage 4: Sort by count descending
    {
        $sort: { count: -1 }
    }
];

const funnelResult = db.events.aggregate(funnelPipeline).toArray();
print('Funnel Analysis Result:');
printjson(funnelResult);
print('');

// ============================================================================
// Pipeline 2: Top Movies by Page Views (with $lookup)
// ============================================================================

print('Pipeline 2: Top Movies by Page Views');
print('-'.repeat(80));

const topMoviesPipeline = [
    // Stage 1: Filter page_view events
    {
        $match: {
            type: 'page_view',
            movie_id: { $ne: null }
        }
    },
    // Stage 2: Group by movie and count views
    {
        $group: {
            _id: '$movie_id',
            view_count: { $sum: 1 },
            unique_viewers: { $addToSet: '$user_id' }
        }
    },
    // Stage 3: Calculate unique viewer count
    {
        $project: {
            movie_id: '$_id',
            view_count: 1,
            unique_viewer_count: { $size: '$unique_viewers' },
            _id: 0
        }
    },
    // Stage 4: Sort by view count
    {
        $sort: { view_count: -1 }
    },
    // Stage 5: Limit to top 10
    {
        $limit: 10
    }
];

const topMoviesResult = db.events.aggregate(topMoviesPipeline).toArray();
print('Top 10 Movies by Views:');
printjson(topMoviesResult);
print('');

// ============================================================================
// Pipeline 3: Hourly Activity Pattern (Time-Series with $bucket)
// ============================================================================

print('Pipeline 3: Hourly Activity Pattern');
print('-'.repeat(80));

const hourlyActivityPipeline = [
    // Stage 1: Filter last 7 days
    {
        $match: {
            timestamp: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) }
        }
    },
    // Stage 2: Add hour field
    {
        $addFields: {
            hour: { $hour: '$timestamp' }
        }
    },
    // Stage 3: Group by hour
    {
        $group: {
            _id: '$hour',
            event_count: { $sum: 1 },
            event_types: { $push: '$type' }
        }
    },
    // Stage 4: Sort by hour
    {
        $sort: { _id: 1 }
    },
    // Stage 5: Project with formatting
    {
        $project: {
            hour: '$_id',
            event_count: 1,
            hour_label: {
                $concat: [
                    { $toString: '$_id' },
                    ':00-',
                    { $toString: { $add: ['$_id', 1] } },
                    ':00'
                ]
            },
            _id: 0
        }
    }
];

const hourlyResult = db.events.aggregate(hourlyActivityPipeline).toArray();
print('Hourly Activity Pattern:');
printjson(hourlyResult);
print('');

// ============================================================================
// Pipeline 4: User Engagement Score with $lookup
// ============================================================================

print('Pipeline 4: User Engagement Score');
print('-'.repeat(80));

const engagementPipeline = [
    // Stage 1: Match recent events
    {
        $match: {
            user_id: { $ne: null },
            timestamp: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }
        }
    },
    // Stage 2: Group by user
    {
        $group: {
            _id: '$user_id',
            total_events: { $sum: 1 },
            page_views: {
                $sum: { $cond: [{ $eq: ['$type', 'page_view'] }, 1, 0] }
            },
            searches: {
                $sum: { $cond: [{ $eq: ['$type', 'search'] }, 1, 0] }
            },
            purchases: {
                $sum: { $cond: [{ $eq: ['$type', 'purchase'] }, 1, 0] }
            },
            last_activity: { $max: '$timestamp' }
        }
    },
    // Stage 3: Calculate engagement score
    {
        $addFields: {
            engagement_score: {
                $add: [
                    { $multiply: ['$page_views', 1] },
                    { $multiply: ['$searches', 2] },
                    { $multiply: ['$purchases', 10] }
                ]
            }
        }
    },
    // Stage 4: Lookup user profile
    {
        $lookup: {
            from: 'user_profiles',
            localField: '_id',
            foreignField: 'user_id',
            as: 'profile'
        }
    },
    // Stage 5: Unwind profile (optional - handle missing profiles)
    {
        $unwind: {
            path: '$profile',
            preserveNullAndEmptyArrays: true
        }
    },
    // Stage 6: Project final fields
    {
        $project: {
            user_id: '$_id',
            total_events: 1,
            page_views: 1,
            searches: 1,
            purchases: 1,
            engagement_score: 1,
            last_activity: 1,
            favorite_genres: '$profile.preferences.favorite_genres',
            _id: 0
        }
    },
    // Stage 7: Sort by engagement score
    {
        $sort: { engagement_score: -1 }
    },
    // Stage 8: Limit to top 20
    {
        $limit: 20
    }
];

const engagementResult = db.events.aggregate(engagementPipeline).toArray();
print('Top 20 Users by Engagement Score:');
printjson(engagementResult);
print('');

// ============================================================================
// Pipeline 5: Multi-Dimensional Analysis with $facet
// ============================================================================

print('Pipeline 5: Multi-Dimensional Analysis with $facet');
print('-'.repeat(80));

const facetPipeline = [
    // Stage 1: Filter last 30 days
    {
        $match: {
            timestamp: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }
        }
    },
    // Stage 2: Multiple aggregations in parallel
    {
        $facet: {
            // Dimension 1: By event type
            by_type: [
                {
                    $group: {
                        _id: '$type',
                        count: { $sum: 1 }
                    }
                },
                { $sort: { count: -1 } }
            ],
            // Dimension 2: By day of week
            by_day_of_week: [
                {
                    $addFields: {
                        day_of_week: { $dayOfWeek: '$timestamp' }
                    }
                },
                {
                    $group: {
                        _id: '$day_of_week',
                        count: { $sum: 1 }
                    }
                },
                {
                    $project: {
                        day: {
                            $switch: {
                                branches: [
                                    { case: { $eq: ['$_id', 1] }, then: 'Sunday' },
                                    { case: { $eq: ['$_id', 2] }, then: 'Monday' },
                                    { case: { $eq: ['$_id', 3] }, then: 'Tuesday' },
                                    { case: { $eq: ['$_id', 4] }, then: 'Wednesday' },
                                    { case: { $eq: ['$_id', 5] }, then: 'Thursday' },
                                    { case: { $eq: ['$_id', 6] }, then: 'Friday' },
                                    { case: { $eq: ['$_id', 7] }, then: 'Saturday' }
                                ],
                                default: 'Unknown'
                            }
                        },
                        count: 1,
                        _id: 0
                    }
                },
                { $sort: { _id: 1 } }
            ],
            // Dimension 3: Top movies
            top_movies: [
                {
                    $match: {
                        movie_id: { $ne: null }
                    }
                },
                {
                    $group: {
                        _id: '$movie_id',
                        events: { $sum: 1 }
                    }
                },
                { $sort: { events: -1 } },
                { $limit: 5 }
            ],
            // Dimension 4: Summary statistics
            summary: [
                {
                    $group: {
                        _id: null,
                        total_events: { $sum: 1 },
                        unique_users: { $addToSet: '$user_id' },
                        unique_sessions: { $addToSet: '$session_id' },
                        date_range: {
                            $push: '$timestamp'
                        }
                    }
                },
                {
                    $project: {
                        total_events: 1,
                        unique_user_count: { $size: '$unique_users' },
                        unique_session_count: { $size: '$unique_sessions' },
                        min_date: { $min: '$date_range' },
                        max_date: { $max: '$date_range' },
                        _id: 0
                    }
                }
            ]
        }
    }
];

const facetResult = db.events.aggregate(facetPipeline).toArray();
print('Multi-Dimensional Analysis Result:');
printjson(facetResult);
print('');

// ============================================================================
// Pipeline 6: Conversion Funnel (Search -> View -> Cart -> Purchase)
// ============================================================================

print('Pipeline 6: Conversion Funnel');
print('-'.repeat(80));

const conversionFunnelPipeline = [
    // Stage 1: Filter last 30 days
    {
        $match: {
            timestamp: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) },
            type: { $in: ['search', 'page_view', 'add_to_cart', 'purchase'] }
        }
    },
    // Stage 2: Group by session
    {
        $group: {
            _id: '$session_id',
            events: { $push: '$type' },
            user_id: { $first: '$user_id' }
        }
    },
    // Stage 3: Check which stages each session reached
    {
        $project: {
            session_id: '$_id',
            user_id: 1,
            had_search: { $in: ['search', '$events'] },
            had_view: { $in: ['page_view', '$events'] },
            had_cart: { $in: ['add_to_cart', '$events'] },
            had_purchase: { $in: ['purchase', '$events'] },
            _id: 0
        }
    },
    // Stage 4: Group and count
    {
        $group: {
            _id: null,
            total_sessions: { $sum: 1 },
            sessions_with_search: {
                $sum: { $cond: ['$had_search', 1, 0] }
            },
            sessions_with_view: {
                $sum: { $cond: ['$had_view', 1, 0] }
            },
            sessions_with_cart: {
                $sum: { $cond: ['$had_cart', 1, 0] }
            },
            sessions_with_purchase: {
                $sum: { $cond: ['$had_purchase', 1, 0] }
            }
        }
    },
    // Stage 5: Calculate conversion rates
    {
        $project: {
            total_sessions: 1,
            search_count: '$sessions_with_search',
            view_count: '$sessions_with_view',
            cart_count: '$sessions_with_cart',
            purchase_count: '$sessions_with_purchase',
            search_to_view_rate: {
                $multiply: [
                    { $divide: ['$sessions_with_view', '$sessions_with_search'] },
                    100
                ]
            },
            view_to_cart_rate: {
                $multiply: [
                    { $divide: ['$sessions_with_cart', '$sessions_with_view'] },
                    100
                ]
            },
            cart_to_purchase_rate: {
                $multiply: [
                    { $divide: ['$sessions_with_purchase', '$sessions_with_cart'] },
                    100
                ]
            },
            overall_conversion_rate: {
                $multiply: [
                    { $divide: ['$sessions_with_purchase', '$total_sessions'] },
                    100
                ]
            },
            _id: 0
        }
    }
];

const conversionResult = db.events.aggregate(conversionFunnelPipeline).toArray();
print('Conversion Funnel Analysis:');
printjson(conversionResult);
print('');

// ============================================================================
// Pipeline 7: Movie Trending Score Calculation
// ============================================================================

print('Pipeline 7: Movie Trending Score');
print('-'.repeat(80));

const trendingScorePipeline = [
    // Stage 1: Filter last 7 days
    {
        $match: {
            type: { $in: ['page_view', 'search', 'add_to_cart', 'purchase'] },
            movie_id: { $ne: null },
            timestamp: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) }
        }
    },
    // Stage 2: Add time weight (more recent = higher weight)
    {
        $addFields: {
            time_weight: {
                $divide: [
                    { $subtract: [new Date(), '$timestamp'] },
                    1000 * 60 * 60 * 24 // Convert to days
                ]
            }
        }
    },
    // Stage 3: Calculate weighted score by event type
    {
        $addFields: {
            weighted_score: {
                $multiply: [
                    {
                        $switch: {
                            branches: [
                                { case: { $eq: ['$type', 'page_view'] }, then: 1 },
                                { case: { $eq: ['$type', 'search'] }, then: 0.5 },
                                { case: { $eq: ['$type', 'add_to_cart'] }, then: 3 },
                                { case: { $eq: ['$type', 'purchase'] }, then: 10 }
                            ],
                            default: 0
                        }
                    },
                    {
                        $exp: {
                            $multiply: [-0.1, '$time_weight'] // Exponential decay
                        }
                    }
                ]
            }
        }
    },
    // Stage 4: Group by movie
    {
        $group: {
            _id: '$movie_id',
            trending_score: { $sum: '$weighted_score' },
            total_events: { $sum: 1 },
            unique_users: { $addToSet: '$user_id' }
        }
    },
    // Stage 5: Calculate final metrics
    {
        $project: {
            movie_id: '$_id',
            trending_score: { $round: ['$trending_score', 2] },
            total_events: 1,
            unique_user_count: { $size: '$unique_users' },
            _id: 0
        }
    },
    // Stage 6: Sort by trending score
    {
        $sort: { trending_score: -1 }
    },
    // Stage 7: Limit to top 20
    {
        $limit: 20
    }
];

const trendingResult = db.events.aggregate(trendingScorePipeline).toArray();
print('Top 20 Trending Movies:');
printjson(trendingResult);
print('');

// ============================================================================
// Summary
// ============================================================================

print('='.repeat(80));
print('MongoDB Aggregation Pipeline Examples Complete');
print('='.repeat(80));
print('Pipelines demonstrated:');
print('  1. Event Funnel Analysis ($match, $group, $sort, $project)');
print('  2. Top Movies by Views ($group, $addToSet, $size)');
print('  3. Hourly Activity Pattern ($addFields, $hour, time-series)');
print('  4. User Engagement Score ($lookup, $unwind, calculated fields)');
print('  5. Multi-Dimensional Analysis ($facet for parallel aggregations)');
print('  6. Conversion Funnel (session-based analysis)');
print('  7. Trending Score (weighted scoring with time decay)');
print('='.repeat(80));
