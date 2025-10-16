from config.Config import get_config
from flask import jsonify, Blueprint, request
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler
from actions.TradingActionEnhanced import TradingActionEnhanced
from logs.logger import get_logger
from actions.DexscrennerAction import DexScreenerAction
from api.trading.request import AddTokenRequest, TokenInfo
from api.trading.response import AddTokenResponse
from api.trading.validation import TokenRequestValidator
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
    
    REQUEST BODY:
    {
        "tokenAddress": "So11111111111111111111111111111111111111112",
        "pairAddress": "4w2cysotX6czaUGmmWg13hDpY4QEMG2CzeKYEQyK9Ama",
        "timeframes": ["30min", "1h", "4h"],
        "addedBy": "user@example.com"
    }
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    try:
        # Step 1: Get and validate request data
        data = request.get_json()
        isValid, errorMessage = TokenRequestValidator.validateRequestData(data)
        if not isValid:
            return jsonify(AddTokenResponse.error_response(errorMessage).to_dict()), 400

        # Step 2: Convert to POJO
        addTokenRequest = AddTokenRequest.from_dict(data)
        
        # Step 3: Check if token exists and enable if found
        existingTokenId = trading_handler.enableTokenIfExists(addTokenRequest.tokenAddress)
        
        if existingTokenId:
            return jsonify(AddTokenResponse.error_response(
                f'Token {addTokenRequest.tokenAddress} is already being tracked'
            ).to_dict()), 409

        # Step 4: Get token information from DexScreener API        
        dexAction = DexScreenerAction()
        tokenInfoFromAPI = dexAction.getTokenPrice(addTokenRequest.tokenAddress)
        
        if not tokenInfoFromAPI:
            return jsonify(AddTokenResponse.error_response(
                'Token not found on DexScreener or no valid trading pairs available'
            ).to_dict()), 404

        # Step 5: Convert DexScreener response to TokenInfo POJO        
        tokenInfo = TokenInfo(
            symbol=tokenInfoFromAPI.symbol,
            name=tokenInfoFromAPI.name,
            pairCreatedAt=tokenInfoFromAPI.pairCreatedAt,
            price=tokenInfoFromAPI.price     
        )

        logger.info(f"Processing token {tokenInfo.symbol} (age: {tokenInfo.pairAgeInDays:.1f} days)")

        tradingAction = TradingActionEnhanced(db)
        response = tradingAction.addTokenForTracking(addTokenRequest, tokenInfo)

        if response.success:
            return jsonify(response.to_dict()), 201
        else:
            return jsonify(response.to_dict()), 500

    except Exception as e:
        logger.error(f"Error in addToken API: {str(e)}", exc_info=True)
        return jsonify(AddTokenResponse.error_response(
            f'Internal server error: {str(e)}'
        ).to_dict()), 500


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


@trading_bp.route('/api/tokens/enable', methods=['POST', 'OPTIONS'])
def enableToken():
    """
    Enable Token API - POST /api/tokens/enable
    
    REQUEST BODY:
    {
        "tokenAddress": "So11111111111111111111111111111111111111112",
        "reason": "Volume improved",
        "enabledBy": "admin@example.com"
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
        enabled_by = data.get('enabledBy', 'api_user')
        
        # Validate required fields
        if not token_address:
            return jsonify({
                'success': False,
                'error': 'Missing required field: tokenAddress'
            }), 400
        
        # Enable the token using optimized database operation
        logger.info(f"Enabling token: {token_address} - Reason: {reason}")
        
        result = trading_handler.enableToken(
            tokenAddress=token_address,
            enabledBy=enabled_by,
            reason=reason
        )
        
        if not result['success']:
            if 'not found' in result['error'].lower():
                return jsonify({
                    'success': False,
                    'error': f'Token {token_address} not found or already enabled'
                }), 404
            else:
                return jsonify({
                    'success': False,
                    'error': f'Failed to enable token: {result["error"]}'
                }), 500
        
        token_info = result['tokenInfo']
        logger.info(f"Successfully enabled token {token_info['symbol']} ({token_address})")
        
        return jsonify({
            'success': True,
            'tokenAddress': token_address,
            'symbol': token_info['symbol'],
            'name': token_info['name'],
            'reason': reason,
            'enabledBy': enabled_by,
            'message': f'Token {token_info["symbol"]} enabled successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Error in enableToken API: {str(e)}", exc_info=True)
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
        trading_scheduler = TradingScheduler()
        trading_scheduler.handleTradingUpdatesFromJob()
        
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
        elif status == 'disabled':
            tokens = trading_handler.getDisabledTokens()
        else:  # all
            active_tokens = trading_handler.getActiveTokens()
            disabled_tokens = trading_handler.getDisabledTokens()
            tokens = active_tokens + disabled_tokens
       
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


