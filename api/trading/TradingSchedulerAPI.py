from config.Config import get_config
from flask import jsonify, Blueprint, request
from scheduler.TradingScheduler import TradingScheduler
from database.operations.PortfolioDB import PortfolioDB
from logs.logger import get_logger

logger = get_logger(__name__)

trading_scheduler_bp = Blueprint('trading_scheduler', __name__)

# Initialize components
db = PortfolioDB()
trading_scheduler = TradingScheduler(db)

@trading_scheduler_bp.route('/api/trading/fetch-data-scheduled', methods=['POST', 'OPTIONS'])
def scheduleTradingDataFetch():
    """
    Execute the trading scheduler's data fetch function
    This endpoint is called by the scheduler every 5 minutes
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        logger.info("Starting scheduled trading data fetch via API")
        result = trading_scheduler.handleTradingDataFromAPI()
        
        if result['success']:
            logger.info(f"Successfully completed scheduled trading data fetch: {result.get('message', 'No message')}")
            return jsonify({
                'status': 'success',
                'message': 'Successfully triggered scheduled trading data fetch',
                'results': result
            }), 200
        else:
            logger.error(f"Scheduled trading data fetch failed: {result.get('error', 'Unknown error')}")
            return jsonify({
                'status': 'error',
                'message': 'Scheduled trading data fetch failed',
                'error': result.get('error', 'Unknown error'),
                'results': result
            }), 500

    except Exception as e:
        logger.error(f"API Error in scheduleTradingDataFetch: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Internal server error: {str(e)}'
        }), 500


@trading_scheduler_bp.route('/api/trading/process-token', methods=['POST', 'OPTIONS'])
def processSpecificToken():
    """
    Process a specific token manually
    
    REQUEST BODY:
    {
        "tokenAddress": "So11111111111111111111111111111111111111112"
    }
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'No JSON data provided'
            }), 400
        
        token_address = data.get('tokenAddress', '').strip()
        
        if not token_address:
            return jsonify({
                'success': False,
                'error': 'Missing required field: tokenAddress'
            }), 400
        
        # Validate address format
        if len(token_address) != 44:
            return jsonify({
                'success': False,
                'error': 'Invalid tokenAddress format (must be 44 characters)'
            }), 400
        
        logger.info(f"Processing specific token: {token_address}")
        
        # Process the token
        from actions.TradingAction import TradingAction
        trading_action = TradingAction(db)
        success = trading_action.processSingleToken(token_address)
        
        if success:
            logger.info(f"Successfully processed token {token_address}")
            return jsonify({
                'success': True,
                'tokenAddress': token_address,
                'message': f'Token {token_address} processed successfully'
            }), 200
        else:
            logger.warning(f"Failed to process token {token_address}")
            return jsonify({
                'success': False,
                'tokenAddress': token_address,
                'error': f'Failed to process token {token_address}'
            }), 500
        
    except Exception as e:
        logger.error(f"Error in processSpecificToken API: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500


@trading_scheduler_bp.route('/api/trading/system-status', methods=['GET', 'OPTIONS'])
def getSystemStatus():
    """
    Get Trading System Status API - GET /api/trading/system-status
    
    Returns overall system health and processing statistics
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        logger.info("Getting trading system status")
        status = trading_scheduler.getSystemStatus()
        
        return jsonify(status), 200
        
    except Exception as e:
        logger.error(f"Error in getSystemStatus API: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500


@trading_scheduler_bp.route('/api/trading/fetch', methods=['POST', 'OPTIONS'])
def fetchTradingData():
    """
    Manual Trading Data Fetch API - POST /api/trading/fetch
    
    Alias for scheduleTradingDataFetch to match frontend patterns
    """
    return scheduleTradingDataFetch()


@trading_scheduler_bp.route('/api/trading/backfill', methods=['POST', 'OPTIONS'])
def triggerBackfill():
    """
    Trigger Backfill API - POST /api/trading/backfill
    
    REQUEST BODY:
    {
        "tokenAddress": "So11111111111111111111111111111111111111112",
        "hours": 168
    }
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'No JSON data provided'
            }), 400
        
        token_address = data.get('tokenAddress', '').strip()
        hours = data.get('hours', 168)  # Default 7 days
        
        if not token_address:
            return jsonify({
                'success': False,
                'error': 'Missing required field: tokenAddress'
            }), 400
        
        # Validate parameters
        if len(token_address) != 44:
            return jsonify({
                'success': False,
                'error': 'Invalid tokenAddress format (must be 44 characters)'
            }), 400
        
        if not isinstance(hours, int) or hours < 1 or hours > 8760:  # Max 1 year
            return jsonify({
                'success': False,
                'error': 'hours must be between 1 and 8760 (1 year)'
            }), 400
        
        # Get token info
        active_tokens = db.trading.getActiveTokens()
        token_info = None
        
        for token in active_tokens:
            if token['tokenaddress'] == token_address:
                token_info = token
                break
        
        if not token_info:
            return jsonify({
                'success': False,
                'error': f'Token {token_address} not found or not active'
            }), 404
        
        logger.info(f"Triggering backfill for {token_info['symbol']} ({token_address}): {hours} hours")
        
        # Execute backfill
        from actions.TradingAction import TradingAction
        trading_action = TradingAction(db)
        
        backfill_result = trading_action.executeBackfill(
            token_address=token_address,
            pair_address=token_info['pairaddress'],
            symbol=token_info['symbol'],
            name=token_info['name'],
            hours=hours
        )
        
        if backfill_result.success:
            logger.info(f"Backfill completed for {token_info['symbol']}: {backfill_result.candlesinserted} candles")
            return jsonify({
                'success': True,
                'tokenAddress': token_address,
                'symbol': token_info['symbol'],
                'hoursRequested': hours,
                'candlesInserted': backfill_result.candlesinserted,
                'candlesProcessed': backfill_result.totalcandlesprocessed,
                'creditsUsed': backfill_result.apicreditsused,
                'completedAt': backfill_result.timecomplete.isoformat() if backfill_result.timecomplete else None,
                'message': f'Backfill completed for {token_info["symbol"]}'
            }), 200
        else:
            logger.error(f"Backfill failed for {token_info['symbol']}: {backfill_result.errordetails}")
            return jsonify({
                'success': False,
                'tokenAddress': token_address,
                'symbol': token_info['symbol'],
                'error': backfill_result.errordetails,
                'message': f'Backfill failed for {token_info["symbol"]}'
            }), 500
        
    except Exception as e:
        logger.error(f"Error in triggerBackfill API: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500


@trading_scheduler_bp.route('/api/trading/candles/<token_address>', methods=['GET', 'OPTIONS'])
def getTokenCandles(token_address):
    """
    Get Token Candles API - GET /api/trading/candles/{tokenAddress}
    
    Query Parameters:
    - timeframe: "15m", "1h", "4h" (default: "15m")
    - limit: Number of candles to return (default: 100, max: 1000)
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        # Parse query parameters
        timeframe = request.args.get('timeframe', '15m').lower()
        limit = int(request.args.get('limit', 100))
        
        # Validate parameters
        if timeframe not in ['15m', '1h', '4h']:
            return jsonify({
                'success': False,
                'error': 'Invalid timeframe. Must be "15m", "1h", or "4h"'
            }), 400
        
        if limit < 1 or limit > 1000:
            return jsonify({
                'success': False,
                'error': 'Limit must be between 1 and 1000'
            }), 400
        
        if len(token_address) != 44:
            return jsonify({
                'success': False,
                'error': 'Invalid tokenAddress format (must be 44 characters)'
            }), 400
        
        # Get candles
        candles = db.trading.getLatestCandles(
            tokenAddress=token_address,
            timeframe=timeframe,
            limit=limit
        )
        
        # Format response
        formatted_candles = []
        for candle in candles:
            formatted_candle = {
                'id': candle['id'],
                'tokenAddress': candle['tokenaddress'],
                'pairAddress': candle['pairaddress'],
                'timeframe': candle['timeframe'],
                'unixTime': candle['unixtime'],
                'timestamp': candle['unixtime'] * 1000,  # JavaScript timestamp
                'open': float(candle['openprice']),
                'high': float(candle['highprice']),
                'low': float(candle['lowprice']),
                'close': float(candle['closeprice']),
                'volume': float(candle['volume']),
                'vwap': float(candle['vwapvalue']) if candle['vwapvalue'] else None,
                'ema21': float(candle['ema21value']) if candle['ema21value'] else None,
                'ema34': float(candle['ema34value']) if candle['ema34value'] else None,
                'isComplete': candle['iscomplete'],
                'dataSource': candle['datasource'],
                'createdAt': candle['createdat'].isoformat() if candle['createdat'] else None
            }
            formatted_candles.append(formatted_candle)
        
        return jsonify({
            'success': True,
            'tokenAddress': token_address,
            'timeframe': timeframe,
            'candleCount': len(formatted_candles),
            'candles': formatted_candles
        }), 200
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': f'Invalid parameter: {str(e)}'
        }), 400
    except Exception as e:
        logger.error(f"Error in getTokenCandles API: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500