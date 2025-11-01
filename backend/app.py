"""
Main Flask application entry point
"""
from flask import Flask, jsonify
from flask_cors import CORS
from config import get_config
import psycopg2
from pymongo import MongoClient
import logging

# Import route blueprints
from api.sql_routes import sql_bp
from api.nosql_routes import nosql_bp
from api.compare_routes import compare_bp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_app():
    """Application factory"""
    app = Flask(__name__)

    # Load configuration
    app.config.from_object(get_config())

    # Enable CORS
    CORS(app, origins=app.config['CORS_ORIGINS'])

    # Register blueprints
    app.register_blueprint(sql_bp, url_prefix='/api/sql')
    app.register_blueprint(nosql_bp, url_prefix='/api/nosql')
    app.register_blueprint(compare_bp, url_prefix='/api/compare')

    # Health check endpoint
    @app.route('/health', methods=['GET'])
    def health():
        """Health check endpoint"""
        try:
            # Check PostgreSQL connection
            pg_conn = psycopg2.connect(app.config['DATABASE_URL'])
            pg_conn.close()
            pg_status = 'connected'
        except Exception as e:
            pg_status = f'error: {str(e)}'
            logger.error(f'PostgreSQL health check failed: {e}')

        try:
            # Check MongoDB connection
            mongo_client = MongoClient(app.config['MONGO_URL'], serverSelectionTimeoutMS=2000)
            mongo_client.admin.command('ping')
            mongo_status = 'connected'
        except Exception as e:
            mongo_status = f'error: {str(e)}'
            logger.error(f'MongoDB health check failed: {e}')

        status_code = 200 if pg_status == 'connected' and mongo_status == 'connected' else 503

        return jsonify({
            'status': 'healthy' if status_code == 200 else 'unhealthy',
            'databases': {
                'postgresql': pg_status,
                'mongodb': mongo_status
            }
        }), status_code

    # Root endpoint
    @app.route('/', methods=['GET'])
    def root():
        """Root endpoint with API information"""
        return jsonify({
            'name': 'INF2003 Movie Database API',
            'version': '1.0.0',
            'description': 'Dual-database system with PostgreSQL and MongoDB',
            'endpoints': {
                'sql': '/api/sql/*',
                'nosql': '/api/nosql/*',
                'compare': '/api/compare/*',
                'health': '/health'
            },
            'documentation': 'See README.md for full API documentation'
        })

    # Error handlers
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({'error': 'Endpoint not found'}), 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f'Internal server error: {error}')
        return jsonify({'error': 'Internal server error'}), 500

    logger.info('Flask application initialized')

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(
        host=app.config['API_HOST'],
        port=app.config['API_PORT'],
        debug=app.config['DEBUG']
    )
