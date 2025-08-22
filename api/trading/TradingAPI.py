
from config.Config import get_config
from flask import jsonify, Blueprint, request
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler
from database.trading.TradingModels import TrackedToken, BackfillRequest
from actions.TradingAction import TradingAction
from logs.logger import get_logger
import re
from datetime import datetime, timedelta
import pytz
import time


logger = get_logger(__name__)


trading_bp = Blueprint('trading', __name__)


# Initialize database connection
db = PortfolioDB()
trading_handler = TradingHandler(db.conn_manager)




@trading_bp.route('/api/tokens/add', methods=['POST', 'OPTIONS'])
def addToken():
   """
   Add Token API - POST /api/tokens/add
  
   REQUEST BODY (New tokens - no EMA required):
   {
       "tokenAddress": "So11111111111111111111111111111111111111112",
       "pairAddress": "4w2cysotX6czaUGmmWg13hDpY4QEMG2CzeKYEQyK9Ama"
   }
  
   REQUEST BODY (Old tokens - EMA required):
   {
       "tokenAddress": "So11111111111111111111111111111111111111112",
       "pairAddress": "4w2cysotX6czaUGmmWg13hDpY4QEMG2CzeKYEQyK9Ama",
       "ema21": {"15m": 1.25, "1h": 1.28, "4h": 1.30},
       "ema34": {"15m": 1.22, "1h": 1.24, "4h": 1.26},
       "referenceUnixTime": 1745173826
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
      
       # Extract and validate required fields
       token_address = data.get('tokenAddress', '').strip()
       pair_address = data.get('pairAddress', '').strip()
       ema21_values = data.get('ema21')
       ema34_values = data.get('ema34')
       reference_unix_time = data.get('referenceUnixTime')
       added_by = data.get('addedBy', 'api_user')
      
       # Validate basic required fields
       if not all([token_address, pair_address]):
           return jsonify({
               'success': False,
               'error': 'Missing required fields: tokenAddress, pairAddress'
           }), 400
      
       # Check token age first to determine if EMA values are required
       from actions.DexscrennerAction import DexScreenerAction
       dex_action = DexScreenerAction()
       token_info = dex_action.getTokenPrice(token_address)
      
       if not token_info:
           return jsonify({
               'success': False,
               'error': 'Token not found on DexScreener or no valid trading pairs available'
           }), 404
      
       # Calculate pair age
       current_time = int(time.time())
       pair_created_time = token_info.pairCreatedAt // 1000  # ms to seconds
       pair_age_days = (current_time - pair_created_time) / 86400
      
       # For old tokens (>7 days), require EMA values
       if pair_age_days > 7:
           if not all([ema21_values, ema34_values, reference_unix_time]):
               return jsonify({
                   'success': False,
                   'error': f'Token is {pair_age_days:.1f} days old. For old tokens, please provide: ema21, ema34, referenceUnixTime',
                   'tokenAge': round(pair_age_days, 1),
                   'requiresEMA': True
               }), 400
          
           # Validate EMA values for required timeframes
           required_timeframes = ['15m', '1h', '4h']
           for timeframe in required_timeframes:
               if timeframe not in ema21_values or not isinstance(ema21_values[timeframe], (int, float)):
                   return jsonify({
                       'success': False,
                       'error': f'Missing or invalid ema21 value for {timeframe} timeframe'
                   }), 400
                  
               if timeframe not in ema34_values or not isinstance(ema34_values[timeframe], (int, float)):
                   return jsonify({
                       'success': False,
                       'error': f'Missing or invalid ema34 value for {timeframe} timeframe'
                   }), 400
          
           # Validate reference unix time
           if not isinstance(reference_unix_time, int) or reference_unix_time <= 0:
               return jsonify({
                   'success': False,
                   'error': 'referenceUnixTime must be a positive integer'
               }), 400
      
       # Check if token already exists
       existing_tokens = trading_handler.getActiveTokens()
       for existing in existing_tokens:
           if existing['tokenaddress'] == token_address:
               return jsonify({
                   'success': False,
                   'error': f'Token {token_address} is already being tracked',
                   'conflictType': 'ALREADY_ACTIVE'
               }), 409
      
       # Use TradingAction for manual token addition with enhanced flow
       logger.info(f"Adding token: {token_address} (age: {pair_age_days:.1f} days)")
      
       try:
           trading_action = TradingAction(db)
           result = trading_action.addTokenManual(
               tokenAddress=token_address,
               pairAddress=pair_address,
               ema21Values=ema21_values,
               ema34Values=ema34_values,
               referenceUnixTime=reference_unix_time,
               addedBy=added_by
           )
          
           if result['success']:
               return jsonify({
                   'success': True,
                   'tokenId': result['tokenId'],
                   'tokenAddress': token_address,
                   'pairAddress': pair_address,
                   'tokenAge': round(pair_age_days, 1),
                   'mode': result['mode'],
                   'candlesInserted': result.get('candlesInserted', 0),
                   'creditsUsed': result.get('creditsUsed', 0),
                   'symbol': token_info.symbol,
                   'name': token_info.name,
                   'message': f'Token added successfully ({result["mode"]})'
               }), 201
           else:
               return jsonify({
                   'success': False,
                   'error': result.get('error', 'Unknown error occurred')
               }), 500
              
       except Exception as e:
           logger.error(f"Error in manual token addition: {e}")
           return jsonify({
               'success': False,
               'error': f'Failed to add token: {str(e)}'
           }), 500
      
   except Exception as e:
       logger.error(f"Error in addToken API: {str(e)}", exc_info=True)
       return jsonify({
           'success': False,
           'error': f'Internal server error: {str(e)}'
       }), 500




@trading_bp.route('/api/tokens/disable', methods=['POST', 'OPTIONS'])
def disableToken():
   """
   Disable Token API - POST /api/tokens/disable
  
   REQUEST BODY:
   {
       "tokenAddress": "So11111111111111111111111111111111111111112",
       "reason": "Low volume",
       "disabledBy": "admin@example.com"
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
      
       # Extract and validate required fields
       token_address = data.get('tokenAddress', '').strip()
       reason = data.get('reason', '').strip()
       disabled_by = data.get('disabledBy', 'api_user')
      
       # Validate required fields
       if not token_address:
           return jsonify({
               'success': False,
               'error': 'Missing required field: tokenAddress'
           }), 400
      
      
       # Check if token exists and is active
       existing_tokens = trading_handler.getActiveTokens()
       token_found = None
       for existing in existing_tokens:
           if existing['tokenaddress'] == token_address:
               token_found = existing
               break
      
       if not token_found:
           return jsonify({
               'success': False,
               'error': f'Token {token_address} not found or already disabled'
           }), 404
      
       # Disable the token
       logger.info(f"Disabling token: {token_found['symbol']} ({token_address}) - Reason: {reason}")
      
       success = trading_handler.disableToken(
           tokenAddress=token_address,
           disabledBy=disabled_by,
           reason=reason
       )
      
       if not success:
           return jsonify({
               'success': False,
               'error': 'Failed to disable token'
           }), 500
      
       logger.info(f"Successfully disabled token {token_found['symbol']}")
      
       return jsonify({
           'success': True,
           'tokenAddress': token_address,
           'symbol': token_found['symbol'],
           'name': token_found['name'],
           'reason': reason,
           'disabledBy': disabled_by,
           'message': f'Token {token_found["symbol"]} disabled successfully'
       }), 200
      
   except Exception as e:
       logger.error(f"Error in disableToken API: {str(e)}", exc_info=True)
       return jsonify({
           'success': False,
           'error': f'Internal server error: {str(e)}'
       }), 500




@trading_bp.route('/api/tokens/list', methods=['GET', 'OPTIONS'])
def listTokens():
   """
   List Tokens API - GET /api/tokens/list
  
   Query Parameters:
   - status: "active", "disabled", "all" (default: "active")
   - limit: Number of tokens to return (default: 100)
   - offset: Offset for pagination (default: 0)
   """
   if request.method == 'OPTIONS':
       return jsonify({}), 200
  
   try:
       # Parse query parameters
       status = request.args.get('status', 'active').lower()
       limit = int(request.args.get('limit', 100))
       offset = int(request.args.get('offset', 0))
      
       # Validate parameters
       if status not in ['active', 'disabled', 'all']:
           return jsonify({
               'success': False,
               'error': 'Invalid status. Must be "active", "disabled", or "all"'
           }), 400
      
       if limit < 1 or limit > 1000:
           return jsonify({
               'success': False,
               'error': 'Limit must be between 1 and 1000'
           }), 400
      
       if offset < 0:
           return jsonify({
               'success': False,
               'error': 'Offset must be non-negative'
           }), 400
      
       # Get tokens based on status
       if status == 'active':
           tokens = trading_handler.getActiveTokens()
       else:
           # For now, we'll just return active tokens
           # In a full implementation, you'd add methods to get disabled/all tokens
           tokens = trading_handler.getActiveTokens()
      
       # Apply pagination
       total_count = len(tokens)
       paginated_tokens = tokens[offset:offset + limit]
      
       # Format response
       formatted_tokens = []
       for token in paginated_tokens:
           formatted_token = {
               'tokenId': token['trackedtokenid'],
               'tokenAddress': token['tokenaddress'],
               'symbol': token['symbol'],
               'name': token['name'],
               'pairAddress': token['pairaddress'],
               'status': 'active' if token['status'] == 1 else 'disabled',
               'enabledAt': token['enabledat'].isoformat() if token['enabledat'] else None,
               'disabledAt': token['disabledat'].isoformat() if token['disabledat'] else None,
               'createdAt': token['createdat'].isoformat() if token['createdat'] else None,
               'lastUpdatedAt': token['lastupdatedat'].isoformat() if token['lastupdatedat'] else None,
               'addedBy': token['addedby'],
               'disabledBy': token['disabledby'],
               'activeTimeframes': token.get('active_timeframes', 0),
               'metadata': token['metadata']
           }
           formatted_tokens.append(formatted_token)
      
       return jsonify({
           'success': True,
           'tokens': formatted_tokens,
           'pagination': {
               'total': total_count,
               'limit': limit,
               'offset': offset,
               'hasMore': offset + limit < total_count
           }
       }), 200
      
   except ValueError as e:
       return jsonify({
           'success': False,
           'error': f'Invalid parameter: {str(e)}'
       }), 400
   except Exception as e:
       logger.error(f"Error in listTokens API: {str(e)}", exc_info=True)
       return jsonify({
           'success': False,
           'error': f'Internal server error: {str(e)}'
       }), 500




@trading_bp.route('/api/tokens/<token_address>/status', methods=['GET', 'OPTIONS'])
def getTokenStatus(token_address):
   """
   Get Token Status API - GET /api/tokens/{tokenAddress}/status
  
   Returns detailed status information for a specific token
   """
   if request.method == 'OPTIONS':
       return jsonify({}), 200
  
   try:
       # Get token information
       active_tokens = trading_handler.getActiveTokens()
       token_info = None
      
       for token in active_tokens:
           if token['tokenaddress'] == token_address:
               token_info = token
               break
      
       if not token_info:
           return jsonify({
               'success': False,
               'error': f'Token {token_address} not found'
           }), 404
      
       # Get timeframe metadata
       tokens_due = trading_handler.getTokensDueForFetch(limit=1000)
       timeframe_status = {}
      
       for token_due in tokens_due:
           if token_due['tokenaddress'] == token_address:
               timeframe_status[token_due['timeframe']] = {
                   'nextFetchAt': token_due['nextfetchat'].isoformat(),
                   'lastFetchedAt': token_due['lastfetchedat'].isoformat() if token_due['lastfetchedat'] else None,
                   'lastSuccessfulFetch': token_due['lastsuccessfullfetchat'].isoformat() if token_due['lastsuccessfullfetchat'] else None,
                   'consecutiveFailures': token_due['consecutivefailures'],
                   'isActive': token_due['isactive']
               }
      
       # Get latest candles count
       latest_candles = {}
       for timeframe in ['15m', '1h', '4h']:
           candles = trading_handler.getLatestCandles(token_address, timeframe, limit=1)
           latest_candles[timeframe] = {
               'latestCandle': candles[0] if candles else None,
               'candleCount': len(candles)
           }
      
       return jsonify({
           'success': True,
           'tokenInfo': {
               'tokenId': token_info['trackedtokenid'],
               'tokenAddress': token_info['tokenaddress'],
               'symbol': token_info['symbol'],
               'name': token_info['name'],
               'pairAddress': token_info['pairaddress'],
               'status': 'active' if token_info['status'] == 1 else 'disabled',
               'enabledAt': token_info['enabledat'].isoformat() if token_info['enabledat'] else None,
               'createdAt': token_info['createdat'].isoformat() if token_info['createdat'] else None,
               'addedBy': token_info['addedby'],
               'metadata': token_info['metadata']
           },
           'timeframeStatus': timeframe_status,
           'latestCandles': latest_candles
       }), 200
      
   except Exception as e:
       logger.error(f"Error in getTokenStatus API: {str(e)}", exc_info=True)
       return jsonify({
           'success': False,
           'error': f'Internal server error: {str(e)}'
       }), 500