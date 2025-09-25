from config.Config import get_config
from flask import jsonify, Blueprint, request
from constants.TradingAPIConstants import TradingAPIConstants
from constants.TradingHandlerConstants import TradingHandlerConstants
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler
from actions.TradingActionEnhanced import TradingActionEnhanced
from logs.logger import get_logger
from actions.DexscrennerAction import DexScreenerAction
from api.trading.TradingAPIUtil import TradingAPIUtil
from constants.TradingConstants import TimeframeConstants, TokenFlowConstants, ValidationMessages
import time

from scheduler.CredentialResetScheduler import CredentialResetScheduler
from scheduler.TradingScheduler import TradingScheduler

logger = get_logger(__name__)

trading_bp = Blueprint('trading', __name__)

# Initialize database connection
db = PortfolioDB()
trading_handler = TradingHandler(db.conn_manager)

@trading_bp.route('/api/tokens/add', methods=['POST', 'OPTIONS'])
def addToken():
    """
    Add Token API - POST /api/tokens/add
    
    REQUEST BODY (New tokens - with timeframes):
    {
         "tokenAddress": "So11111111111111111111111111111111111111112",
         "pairAddress": "4w2cysotX6czaUGmmWg13hDpY4QEMG2CzeKYEQyK9Ama",
         "timeframes": ["30min", "1h", "4h"]
    }
    
    REQUEST BODY (Old tokens - With timeframes and Per-timeframe EMA):
    {
         "tokenAddress": "So11111111111111111111111111111111111111112",
         "pairAddress": "4w2cysotX6czaUGmmWg13hDpY4QEMG2CzeKYEQyK9Ama",
         "timeframes": ["30min", "1h", "4h"],
         "ema21": {
             "30min": {"value": 1.25, "referenceTime": "10:30 AM"},
             "1h": {"value": 1.28, "referenceTime": "10 AM"},
             "4h": {"value": 1.30, "referenceTime": "8 AM"}
         },
         "ema34": {
             "30min": {"value": 1.22, "referenceTime": "10:30 AM"},
             "1h": {"value": 1.24, "referenceTime": "10 AM"},
             "4h": {"value": 1.26, "referenceTime": "8 AM"}
         }
    }
    """
    if request.method == 'OPTIONS':
         return jsonify({}), 200

    try:
        
         data = request.get_json()
         isValid, errorMessage, requestData = TradingAPIUtil.validateRequestData(data)
         if not isValid:
             return jsonify({'success': False, 'error': errorMessage}), 400

         tokenAddress = requestData[TradingAPIConstants.RequestParameters.TOKEN_ADDRESS]
         pairAddress = requestData[TradingAPIConstants.RequestParameters.PAIR_ADDRESS]
         addedBy = requestData[TradingAPIConstants.RequestParameters.ADDED_BY]

         # Step 2: Check if token already exists in tracked tokens
         existingTokens = trading_handler.getActiveTokens()
         for existing in existingTokens:
             if existing[TradingHandlerConstants.TrackedTokens.TOKEN_ADDRESS] == tokenAddress:
                 return jsonify({
                     'success': False,
                     'error': f'Token {tokenAddress} is already being tracked',
                     'conflictType': 'ALREADY_ACTIVE'
                 }), 409

         # Step 3: Get token information from DexScreener API to check pair age
         dexAction = DexScreenerAction()
         tokenInfoFromAPI = dexAction.getTokenPrice(tokenAddress)
         
         if not tokenInfoFromAPI:
             return jsonify({
                 'success': False,
                 'error': 'Token not found on DexScreener or no valid trading pairs available'
             }), 404

         # Step 4: Calculate pair age to determine flow
         currentTime = int(time.time())
         pairCreatedTime = tokenInfoFromAPI.pairCreatedAt // 1000  # ms to seconds
         pairAgeInDays = (currentTime - pairCreatedTime) / 86400

         logger.info(f"Processing token {tokenInfoFromAPI.symbol} (age: {pairAgeInDays:.1f} days)")

         # Step 5: Initialize TradingActionEnhanced for token processing
         tradingAction = TradingActionEnhanced(db)

         # Step 6: Route based on pair age
         if pairAgeInDays <= TokenFlowConstants.NEW_TOKEN_MAX_AGE_DAYS:
             # New token flow - requires timeframes array
             tokenAddition = addNewToken(
                 requestData, tradingAction, tokenAddress, pairAddress, 
                 tokenInfoFromAPI, pairCreatedTime, addedBy
             )
         else:
             # Old token flow (>7 days) - requires timeframes and per-timeframe EMA data
             tokenAddition = addOldToken(
                 requestData, tradingAction, tokenAddress, pairAddress, 
                 tokenInfoFromAPI, pairCreatedTime, addedBy
             )

         # Step 7: Return appropriate response
         if tokenAddition['success']:
             successResponse = TradingAPIUtil.formatSuccessResponse(
                 tokenAddition, tokenAddress, pairAddress, pairAgeInDays
             )
             return jsonify(successResponse), 201
         else:
             errorResponse, statusCode = TradingAPIUtil.formatErrorResponse(
                 tokenAddition.get('error', 'Unknown error occurred')
             )
             return jsonify(errorResponse), statusCode

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
      
       # Disable the token using optimized database operation
       logger.info(f"Disabling token: {token_address} - Reason: {reason}")
       
       result = trading_handler.disableToken(
           tokenAddress=token_address,
           disabledBy=disabled_by,
           reason=reason
       )
       
       if not result['success']:
           if 'not found' in result['error'].lower():
               return jsonify({
                   'success': False,
                   'error': f'Token {token_address} not found or already disabled'
               }), 404
           else:
               return jsonify({
                   'success': False,
                   'error': f'Failed to disable token: {result["error"]}'
               }), 500
       
       token_info = result['tokenInfo']
       logger.info(f"Successfully disabled token {token_info['symbol']} ({token_address})")
       
       return jsonify({
           'success': True,
           'tokenAddress': token_address,
           'symbol': token_info['symbol'],
           'name': token_info['name'],
           'reason': reason,
           'disabledBy': disabled_by,
           'message': f'Token {token_info["symbol"]} disabled successfully'
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

        CredentialResetScheduler().processCredentialResets()


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


def addNewToken(requestData: dict, tradingAction, tokenAddress: str, pairAddress: str,
                           tokenInfoFromAPI, pairCreatedTime: int, addedBy: str):
    """
    Handle new token flow with timeframes validation and processing
    
    Args:
         requestData: Request payload containing timeframes
         tradingAction: TradingActionEnhanced instance
         tokenAddress: Token contract address
         pairAddress: Token pair address
         tokenInfoFromAPI: Token info from DexScreener
         pairCreatedTime: Unix timestamp of pair creation
         addedBy: User who added the token
         
    Returns:
         Result from addNewTokenWithTimeframes method
    """
    # Extract and validate timeframes
    timeframes = requestData.get('timeframes', [])
    
    # Process new token with validated timeframes
    return tradingAction.addNewTokenWithTimeframes(
         tokenAddress=tokenAddress,
         pairAddress=pairAddress,
         symbol=tokenInfoFromAPI.symbol,
         name=tokenInfoFromAPI.name,
         pairCreatedTime=pairCreatedTime,
         timeframes=timeframes,
         addedBy=addedBy
    )


def addOldToken(requestData: dict, tradingAction, tokenAddress: str, pairAddress: str,
                 tokenInfoFromAPI, pairCreatedTime: int, addedBy: str):
    """
    Handle old token flow with timeframes and EMA validation
    
    Args:
         requestData: Request payload containing timeframes and EMA data
         tradingAction: TradingActionEnhanced instance
         tokenAddress: Token contract address
         pairAddress: Token pair address
         tokenInfoFromAPI: Token info from DexScreener
         pairCreatedTime: Unix timestamp of pair creation
         addedBy: User who added the token
         
    Returns:
         Result from addOldTokenWithTimeframes method
    """
    # Extract and validate timeframes
    timeframes = requestData.get(TradingAPIConstants.RequestParameters.TIMEFRAMES, [])
    
    # Validate and process EMA data
    processedEMAData = None
    if requestData[TradingAPIConstants.Log.EMA_21_TYPE] and requestData[TradingAPIConstants.Log.EMA_34_TYPE]:
         # Calculate pair age for validation
         currentTime = int(time.time())
         pairAgeInDays = (currentTime - pairCreatedTime) / 86400
         
         # Validate and process per-timeframe EMA data
         isValid, errorMessage, processedEMAData = TradingAPIUtil.validateOldTokenRequirementsAndProcessEMAData(
             pairAgeInDays,
             requestData[TradingAPIConstants.Log.EMA_21_TYPE],
             requestData[TradingAPIConstants.Log.EMA_34_TYPE]
         )
         if not isValid:
             return {'success': False, 'error': errorMessage}

    if not processedEMAData:        
         return {'success': False, 'error': 'EMA data required for old tokens'}

    
    # Process old token with validated timeframes and EMA data
    return tradingAction.addOldTokenWithTimeframes(
         tokenAddress=tokenAddress,
         pairAddress=pairAddress,
         symbol=tokenInfoFromAPI.symbol,
         name=tokenInfoFromAPI.name,
         pairCreatedTime=pairCreatedTime,
         timeframes=timeframes,
         perTimeframeEMAData=processedEMAData,
         addedBy=addedBy
    )