"""
DexScreener API - Provides token price and information endpoints
"""

from flask import jsonify, Blueprint, request
from actions.DexscrennerAction import DexScreenerAction
from logs.logger import get_logger

logger = get_logger(__name__)

# Create blueprint for DexScreener API
dexscreener_bp = Blueprint('dexscreener', __name__)

@dexscreener_bp.route('/api/price/token/<token_address>', methods=['GET', 'OPTIONS'])
def get_token_price(token_address):
    """
    Get token price and information from DexScreener API
    
    GET /api/price/token/{token_address}
    
    Returns:
    {
        "status": "success",
        "data": {
            "name": "Token Name",
            "symbol": "SYMBOL",
            "price": 0.123,
            "fdv": 1000000,
            "marketCap": 500000,
            "pairAddress": "pair_address_here",
            "pairCreatedAt": 1640995200000,
            "dexId": "raydium",
            "liquidityUsd": 100000
        }
    }
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        # Validate token address
        if not token_address or len(token_address) < 32:
            return jsonify({
                'status': 'error',
                'error': 'Invalid token address'
            }), 400
        
        # Initialize DexScreener action
        dex_action = DexScreenerAction()
        
        # Get token price information
        token_price = dex_action.getTokenPrice(token_address)
        
        if not token_price:
            return jsonify({
                'status': 'error',
                'error': 'Token not found or no valid trading pairs available'
            }), 404
        
        # Format response data
        response_data = {
            'name': token_price.name,
            'symbol': token_price.symbol,
            'price': token_price.price,
            'fdv': token_price.fdv,
            'marketCap': token_price.marketCap,
            'pairAddress': token_price.pairAddress,
            'pairCreatedAt': token_price.pairCreatedAt,
            'dexId': token_price.dexId,
            'liquidityUsd': token_price.liquidityUsd
        }
        
        logger.info(f"Successfully fetched token info for {token_price.symbol} ({token_address})")
        
        return jsonify({
            'status': 'success',
            'data': response_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_token_price API: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': f'Internal server error: {str(e)}'
        }), 500


@dexscreener_bp.route('/api/price/tokens/batch', methods=['POST', 'OPTIONS'])
def get_batch_token_prices():
    """
    Get token prices for multiple tokens in batch
    
    POST /api/price/tokens/batch
    
    Request Body:
    {
        "tokenAddresses": ["address1", "address2", "address3"],
        "chainId": "solana"  // optional, defaults to "solana"
    }
    
    Returns:
    {
        "status": "success",
        "data": {
            "address1": {
                "name": "Token Name",
                "symbol": "SYMBOL",
                "price": 0.123,
                // ... other fields
            },
            "address2": null,  // if token not found
            // ...
        }
    }
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'status': 'error',
                'error': 'No JSON data provided'
            }), 400
        
        token_addresses = data.get('tokenAddresses', [])
        chain_id = data.get('chainId', 'solana')
        
        if not token_addresses or not isinstance(token_addresses, list):
            return jsonify({
                'status': 'error',
                'error': 'tokenAddresses must be a non-empty array'
            }), 400
        
        if len(token_addresses) > 100:
            return jsonify({
                'status': 'error',
                'error': 'Maximum 100 tokens allowed per batch request'
            }), 400
        
        # Initialize DexScreener action
        dex_action = DexScreenerAction()
        
        # Get batch token prices
        batch_prices = dex_action.getBatchTokenPrices(token_addresses, chain_id)
        
        # Format response data
        formatted_data = {}
        for address, token_price in batch_prices.items():
            if token_price:
                formatted_data[address] = {
                    'name': token_price.name,
                    'symbol': token_price.symbol,
                    'price': token_price.price,
                    'fdv': token_price.fdv,
                    'marketCap': token_price.marketCap,
                    'pairAddress': token_price.pairAddress,
                    'pairCreatedAt': token_price.pairCreatedAt,
                    'dexId': token_price.dexId,
                    'liquidityUsd': token_price.liquidityUsd
                }
            else:
                formatted_data[address] = None
        
        found_count = sum(1 for v in formatted_data.values() if v is not None)
        logger.info(f"Successfully processed batch request for {len(token_addresses)} tokens, found {found_count}")
        
        return jsonify({
            'status': 'success',
            'data': formatted_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_batch_token_prices API: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': f'Internal server error: {str(e)}'
        }), 500
