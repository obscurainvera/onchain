from config.Config import get_config
from flask import jsonify, Blueprint, request
from scheduler.TradingAttentionScheduler import TradingAttentionScheduler
from actions.TradingAttentionAction import TradingAttentionAction
from database.operations.PortfolioDB import PortfolioDB
from config.Security import COOKIE_MAP, isValidCookie
from logs.logger import get_logger

logger = get_logger(__name__)

tradingattention_bp = Blueprint('tradingattention', __name__)

@tradingattention_bp.route('/api/tradingattention/fetch-data-scheduled', methods=['POST', 'OPTIONS'])
def scheduleTradingAttentionDataFetch():
    """Execute the scheduler's execute_actions function for trading attention data"""
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        logger.info("Starting scheduled trading attention data fetch")
        scheduler = TradingAttentionScheduler()
        scheduler.handleTradingAttentionAnalysisFromAPI()
        
        logger.info("Successfully triggered scheduled trading attention data fetch")
        return jsonify({
            'status': 'success',
            'message': 'Successfully triggered scheduled trading attention data fetch'
        })

    except Exception as e:
        logger.error(f"API Error in scheduleTradingAttentionDataFetch: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Internal server error: {str(e)}'
        }), 500

# Add a new endpoint that matches the one used in the frontend
@tradingattention_bp.route('/api/tradingattention/fetch-data', methods=['POST', 'OPTIONS'])
def fetchTradingAttentionData():
    """Alias for scheduleTradingAttentionDataFetch to match frontend endpoint"""
    return scheduleTradingAttentionDataFetch()

@tradingattention_bp.route('/api/tradingattention/fetch', methods=['POST', 'OPTIONS'])
def fetchTradingAttentionDataAlias():
    """Alias for scheduleTradingAttentionDataFetch to match frontend endpoint"""
    return scheduleTradingAttentionDataFetch()

@tradingattention_bp.route('/api/tradingattention/data', methods=['GET', 'OPTIONS'])
def getTradingAttentionData():
    """Get top trading attention tokens"""
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        logger.info("Fetching trading attention data")
        
        # Get query parameters
        limit = request.args.get('limit', 100, type=int)
        
        # Initialize database
        db = PortfolioDB()
        
        # Get top trading attention tokens
        tokens = db.tradingattention.getTopTradingAttentionTokens(limit)
        
        logger.info(f"Successfully retrieved {len(tokens)} trading attention tokens")
        return jsonify({
            'status': 'success',
            'data': tokens,
            'count': len(tokens)
        })

    except Exception as e:
        logger.error(f"API Error in getTradingAttentionData: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Internal server error: {str(e)}'
        }), 500

@tradingattention_bp.route('/api/tradingattention/token/<token_id>', methods=['GET', 'OPTIONS'])
def getTradingAttentionTokenData(token_id: str):
    """Get trading attention data for a specific token"""
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        logger.info(f"Fetching trading attention data for token: {token_id}")
        
        # Initialize database
        db = PortfolioDB()
        
        # Get token data
        token_data = db.tradingattention.getTradingAttentionData(token_id)
        
        if not token_data:
            return jsonify({
                'status': 'error',
                'message': f'Token {token_id} not found'
            }), 404
        
        logger.info(f"Successfully retrieved trading attention data for token: {token_id}")
        return jsonify({
            'status': 'success',
            'data': token_data
        })

    except Exception as e:
        logger.error(f"API Error in getTradingAttentionTokenData: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Internal server error: {str(e)}'
        }), 500

@tradingattention_bp.route('/api/tradingattention/token/<token_id>/history', methods=['GET', 'OPTIONS'])
def getTradingAttentionTokenHistory(token_id: str):
    """Get trading attention history for a specific token"""
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        logger.info(f"Fetching trading attention history for token: {token_id}")
        
        # Get query parameters
        limit = request.args.get('limit', 100, type=int)
        
        # Initialize database
        db = PortfolioDB()
        
        # Get token history
        history = db.tradingattention.getTradingAttentionHistory(token_id, limit)
        
        logger.info(f"Successfully retrieved {len(history)} history records for token: {token_id}")
        return jsonify({
            'status': 'success',
            'data': history,
            'count': len(history)
        })

    except Exception as e:
        logger.error(f"API Error in getTradingAttentionTokenHistory: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Internal server error: {str(e)}'
        }), 500 