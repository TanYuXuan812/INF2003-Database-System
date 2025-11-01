"""
MongoDB query functions for telemetry, analytics, and aggregations
"""
from pymongo import MongoClient
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import os
from dotenv import load_dotenv

load_dotenv()


def get_mongo_client():
    """Get MongoDB client"""
    mongo_url = os.getenv('MONGO_URL', 'mongodb://admin:password@mongodb:27017')
    return MongoClient(mongo_url)


def get_db():
    """Get MongoDB database"""
    client = get_mongo_client()
    db_name = os.getenv('MONGO_DB', 'moviedb_mongo')
    return client[db_name]


# ============================================================================
# Event Logging
# ============================================================================

def log_event(event_data: Dict[str, Any]) -> str:
    """
    Log a user event to MongoDB

    Args:
        event_data: Event details (type, user_id, movie_id, metadata, etc.)

    Returns:
        Inserted event ID
    """
    db = get_db()

    # Ensure timestamp
    if 'timestamp' not in event_data:
        event_data['timestamp'] = datetime.utcnow()

    result = db.events.insert_one(event_data)
    return str(result.inserted_id)


def get_events(filters: Dict[str, Any] = None, limit: int = 100, skip: int = 0) -> List[Dict]:
    """
    Get events with optional filters

    Args:
        filters: MongoDB query filters
        limit: Maximum number of results
        skip: Number of results to skip

    Returns:
        List of events
    """
    db = get_db()

    if filters is None:
        filters = {}

    cursor = db.events.find(filters).sort('timestamp', -1).skip(skip).limit(limit)
    events = list(cursor)

    # Convert ObjectId to string
    for event in events:
        event['_id'] = str(event['_id'])

    return events


# ============================================================================
# Aggregation Pipelines
# ============================================================================

def get_event_funnel(days: int = 30) -> List[Dict]:
    """
    Event funnel analysis showing count by event type

    Args:
        days: Number of days to analyze

    Returns:
        List of event types with counts
    """
    db = get_db()

    since = datetime.utcnow() - timedelta(days=days)

    pipeline = [
        {
            '$match': {
                'timestamp': {'$gte': since}
            }
        },
        {
            '$group': {
                '_id': '$type',
                'count': {'$sum': 1},
                'unique_users': {'$addToSet': '$user_id'},
                'unique_movies': {'$addToSet': '$movie_id'}
            }
        },
        {
            '$project': {
                'event_type': '$_id',
                'count': 1,
                'unique_user_count': {'$size': '$unique_users'},
                'unique_movie_count': {'$size': '$unique_movies'},
                '_id': 0
            }
        },
        {
            '$sort': {'count': -1}
        }
    ]

    return list(db.events.aggregate(pipeline))


def get_top_viewed_movies(days: int = 30, limit: int = 10) -> List[Dict]:
    """
    Get top movies by page view count

    Args:
        days: Number of days to analyze
        limit: Maximum number of results

    Returns:
        List of movies with view counts
    """
    db = get_db()

    since = datetime.utcnow() - timedelta(days=days)

    pipeline = [
        {
            '$match': {
                'type': 'page_view',
                'movie_id': {'$ne': None},
                'timestamp': {'$gte': since}
            }
        },
        {
            '$group': {
                '_id': '$movie_id',
                'view_count': {'$sum': 1},
                'unique_viewers': {'$addToSet': '$user_id'}
            }
        },
        {
            '$project': {
                'movie_id': '$_id',
                'view_count': 1,
                'unique_viewer_count': {'$size': '$unique_viewers'},
                '_id': 0
            }
        },
        {
            '$sort': {'view_count': -1}
        },
        {
            '$limit': limit
        }
    ]

    return list(db.events.aggregate(pipeline))


def get_hourly_activity(days: int = 7) -> List[Dict]:
    """
    Get activity pattern by hour of day

    Args:
        days: Number of days to analyze

    Returns:
        List of hourly activity counts
    """
    db = get_db()

    since = datetime.utcnow() - timedelta(days=days)

    pipeline = [
        {
            '$match': {
                'timestamp': {'$gte': since}
            }
        },
        {
            '$addFields': {
                'hour': {'$hour': '$timestamp'}
            }
        },
        {
            '$group': {
                '_id': '$hour',
                'event_count': {'$sum': 1}
            }
        },
        {
            '$sort': {'_id': 1}
        },
        {
            '$project': {
                'hour': '$_id',
                'event_count': 1,
                '_id': 0
            }
        }
    ]

    return list(db.events.aggregate(pipeline))


def get_user_engagement(days: int = 30, limit: int = 20) -> List[Dict]:
    """
    Calculate user engagement scores

    Args:
        days: Number of days to analyze
        limit: Maximum number of users

    Returns:
        List of users with engagement scores
    """
    db = get_db()

    since = datetime.utcnow() - timedelta(days=days)

    pipeline = [
        {
            '$match': {
                'user_id': {'$ne': None},
                'timestamp': {'$gte': since}
            }
        },
        {
            '$group': {
                '_id': '$user_id',
                'total_events': {'$sum': 1},
                'page_views': {
                    '$sum': {'$cond': [{'$eq': ['$type', 'page_view']}, 1, 0]}
                },
                'searches': {
                    '$sum': {'$cond': [{'$eq': ['$type', 'search']}, 1, 0]}
                },
                'purchases': {
                    '$sum': {'$cond': [{'$eq': ['$type', 'purchase']}, 1, 0]}
                },
                'last_activity': {'$max': '$timestamp'}
            }
        },
        {
            '$addFields': {
                'engagement_score': {
                    '$add': [
                        '$page_views',
                        {'$multiply': ['$searches', 2]},
                        {'$multiply': ['$purchases', 10]}
                    ]
                }
            }
        },
        {
            '$project': {
                'user_id': '$_id',
                'total_events': 1,
                'page_views': 1,
                'searches': 1,
                'purchases': 1,
                'engagement_score': 1,
                'last_activity': 1,
                '_id': 0
            }
        },
        {
            '$sort': {'engagement_score': -1}
        },
        {
            '$limit': limit
        }
    ]

    return list(db.events.aggregate(pipeline))


def get_conversion_funnel(days: int = 30) -> Dict:
    """
    Calculate conversion funnel metrics

    Args:
        days: Number of days to analyze

    Returns:
        Dictionary with funnel metrics
    """
    db = get_db()

    since = datetime.utcnow() - timedelta(days=days)

    pipeline = [
        {
            '$match': {
                'timestamp': {'$gte': since},
                'type': {'$in': ['search', 'page_view', 'add_to_cart', 'purchase']}
            }
        },
        {
            '$group': {
                '_id': '$session_id',
                'events': {'$push': '$type'}
            }
        },
        {
            '$project': {
                'had_search': {'$in': ['search', '$events']},
                'had_view': {'$in': ['page_view', '$events']},
                'had_cart': {'$in': ['add_to_cart', '$events']},
                'had_purchase': {'$in': ['purchase', '$events']}
            }
        },
        {
            '$group': {
                '_id': None,
                'total_sessions': {'$sum': 1},
                'sessions_with_search': {
                    '$sum': {'$cond': ['$had_search', 1, 0]}
                },
                'sessions_with_view': {
                    '$sum': {'$cond': ['$had_view', 1, 0]}
                },
                'sessions_with_cart': {
                    '$sum': {'$cond': ['$had_cart', 1, 0]}
                },
                'sessions_with_purchase': {
                    '$sum': {'$cond': ['$had_purchase', 1, 0]}
                }
            }
        }
    ]

    result = list(db.events.aggregate(pipeline))

    if not result:
        return {
            'total_sessions': 0,
            'search_count': 0,
            'view_count': 0,
            'cart_count': 0,
            'purchase_count': 0,
            'search_to_view_rate': 0,
            'view_to_cart_rate': 0,
            'cart_to_purchase_rate': 0,
            'overall_conversion_rate': 0
        }

    data = result[0]
    total = data['total_sessions']

    return {
        'total_sessions': total,
        'search_count': data['sessions_with_search'],
        'view_count': data['sessions_with_view'],
        'cart_count': data['sessions_with_cart'],
        'purchase_count': data['sessions_with_purchase'],
        'search_to_view_rate': round((data['sessions_with_view'] / data['sessions_with_search'] * 100) if data['sessions_with_search'] > 0 else 0, 2),
        'view_to_cart_rate': round((data['sessions_with_cart'] / data['sessions_with_view'] * 100) if data['sessions_with_view'] > 0 else 0, 2),
        'cart_to_purchase_rate': round((data['sessions_with_purchase'] / data['sessions_with_cart'] * 100) if data['sessions_with_cart'] > 0 else 0, 2),
        'overall_conversion_rate': round((data['sessions_with_purchase'] / total * 100) if total > 0 else 0, 2)
    }


def get_trending_movies(days: int = 7, limit: int = 20) -> List[Dict]:
    """
    Calculate trending movies with weighted scoring

    Args:
        days: Number of days to analyze
        limit: Maximum number of results

    Returns:
        List of trending movies
    """
    db = get_db()

    since = datetime.utcnow() - timedelta(days=days)
    now = datetime.utcnow()

    pipeline = [
        {
            '$match': {
                'type': {'$in': ['page_view', 'search', 'add_to_cart', 'purchase']},
                'movie_id': {'$ne': None},
                'timestamp': {'$gte': since}
            }
        },
        {
            '$addFields': {
                'days_ago': {
                    '$divide': [
                        {'$subtract': [now, '$timestamp']},
                        1000 * 60 * 60 * 24  # Convert ms to days
                    ]
                }
            }
        },
        {
            '$addFields': {
                'weighted_score': {
                    '$multiply': [
                        {
                            '$switch': {
                                'branches': [
                                    {'case': {'$eq': ['$type', 'page_view']}, 'then': 1},
                                    {'case': {'$eq': ['$type', 'search']}, 'then': 0.5},
                                    {'case': {'$eq': ['$type', 'add_to_cart']}, 'then': 3},
                                    {'case': {'$eq': ['$type', 'purchase']}, 'then': 10}
                                ],
                                'default': 0
                            }
                        },
                        {
                            '$exp': {
                                '$multiply': [-0.1, '$days_ago']  # Exponential decay
                            }
                        }
                    ]
                }
            }
        },
        {
            '$group': {
                '_id': '$movie_id',
                'trending_score': {'$sum': '$weighted_score'},
                'total_events': {'$sum': 1},
                'unique_users': {'$addToSet': '$user_id'}
            }
        },
        {
            '$project': {
                'movie_id': '$_id',
                'trending_score': {'$round': ['$trending_score', 2]},
                'total_events': 1,
                'unique_user_count': {'$size': '$unique_users'},
                '_id': 0
            }
        },
        {
            '$sort': {'trending_score': -1}
        },
        {
            '$limit': limit
        }
    ]

    return list(db.events.aggregate(pipeline))


def get_multi_dimensional_analytics(days: int = 30) -> Dict:
    """
    Multi-dimensional analysis using $facet

    Args:
        days: Number of days to analyze

    Returns:
        Dictionary with multiple analytics dimensions
    """
    db = get_db()

    since = datetime.utcnow() - timedelta(days=days)

    pipeline = [
        {
            '$match': {
                'timestamp': {'$gte': since}
            }
        },
        {
            '$facet': {
                'by_type': [
                    {'$group': {'_id': '$type', 'count': {'$sum': 1}}},
                    {'$sort': {'count': -1}}
                ],
                'by_day_of_week': [
                    {'$addFields': {'day_of_week': {'$dayOfWeek': '$timestamp'}}},
                    {'$group': {'_id': '$day_of_week', 'count': {'$sum': 1}}},
                    {'$sort': {'_id': 1}}
                ],
                'top_movies': [
                    {'$match': {'movie_id': {'$ne': None}}},
                    {'$group': {'_id': '$movie_id', 'events': {'$sum': 1}}},
                    {'$sort': {'events': -1}},
                    {'$limit': 5}
                ],
                'summary': [
                    {
                        '$group': {
                            '_id': None,
                            'total_events': {'$sum': 1},
                            'unique_users': {'$addToSet': '$user_id'},
                            'unique_sessions': {'$addToSet': '$session_id'}
                        }
                    },
                    {
                        '$project': {
                            'total_events': 1,
                            'unique_user_count': {'$size': '$unique_users'},
                            'unique_session_count': {'$size': '$unique_sessions'},
                            '_id': 0
                        }
                    }
                ]
            }
        }
    ]

    result = list(db.events.aggregate(pipeline))
    return result[0] if result else {}


# ============================================================================
# User Profiles
# ============================================================================

def get_user_profile(user_id: int) -> Optional[Dict]:
    """Get user profile"""
    db = get_db()
    profile = db.user_profiles.find_one({'user_id': user_id})

    if profile:
        profile['_id'] = str(profile['_id'])

    return profile


def update_user_profile(user_id: int, profile_data: Dict) -> bool:
    """Update or create user profile"""
    db = get_db()

    profile_data['user_id'] = user_id
    profile_data['last_updated'] = datetime.utcnow()

    result = db.user_profiles.update_one(
        {'user_id': user_id},
        {'$set': profile_data},
        upsert=True
    )

    return result.acknowledged


# ============================================================================
# Admin / Diagnostics
# ============================================================================

def get_collection_stats() -> Dict:
    """Get statistics for all collections"""
    db = get_db()

    stats = {}
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        stats[collection_name] = {
            'count': collection.count_documents({}),
            'indexes': [idx['name'] for idx in collection.list_indexes()]
        }

    return stats


def get_pipeline_definition(pipeline_name: str) -> Optional[Dict]:
    """
    Get aggregation pipeline definition by name

    Args:
        pipeline_name: Name of the pipeline (e.g., 'funnel', 'trending')

    Returns:
        Dictionary with pipeline definition and description
    """
    pipelines = {
        'funnel': {
            'name': 'Event Funnel Analysis',
            'description': 'Groups events by type and counts occurrences',
            'pipeline': 'See get_event_funnel() in mongo_query.py'
        },
        'trending': {
            'name': 'Trending Movies',
            'description': 'Calculates trending score with time decay and weighted events',
            'pipeline': 'See get_trending_movies() in mongo_query.py'
        },
        'conversion': {
            'name': 'Conversion Funnel',
            'description': 'Tracks user journey from search to purchase',
            'pipeline': 'See get_conversion_funnel() in mongo_query.py'
        },
        'engagement': {
            'name': 'User Engagement',
            'description': 'Calculates user engagement scores based on activity',
            'pipeline': 'See get_user_engagement() in mongo_query.py'
        }
    }

    return pipelines.get(pipeline_name)
