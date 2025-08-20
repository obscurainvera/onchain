from config.Config import get_config
from flask import Blueprint, jsonify, request
from database.operations.PortfolioDB import PortfolioDB
from database.tradingattention.TradingAttentionReportHandler import TradingAttentionReportHandler
from actions.DexscrennerAction import DexScreenerAction
from logs.logger import get_logger
from datetime import datetime
import time

logger = get_logger(__name__)

trading_attention_report_bp = Blueprint('trading_attention_report', __name__)

@trading_attention_report_bp.route('/api/reports/tradingattention', methods=['GET', 'OPTIONS'])
def get_trading_attention_report():
    """
    Get highly optimized trading attention report.
    Returns latest tokens in attention with 7-day history including:
    - Token name, ID, market cap, token age
    - Daily attention data with min/max scores and prices
    - Price differences and timestamps
    
    Optimized for cloud deployment with minimal egress and maximum performance.
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        logger.info("Starting trading attention report generation")
        start_time = time.time()
        
        # Use the handler to get the optimized report data
        with PortfolioDB() as db:
            handler = TradingAttentionReportHandler(db)
            
            if handler is None:
                logger.error("TradingAttentionReportHandler initialization failed")
                return jsonify({
                    'status': 'error',
                    'message': "Trading attention report handler not available"
                }), 500
            
            # Get the core report data with highly optimized single query
            report_data = handler.getTradingAttentionReport()
            
            if not report_data:
                logger.warning("No trading attention data found")
                return jsonify({
                    'status': 'success',
                    'data': []
                })
            
            logger.info(f"Retrieved core data for {len(report_data)} tokens in {time.time() - start_time:.2f}s")
            
            # Extract token IDs from report data instead of making another DB call
            all_token_ids = list(set([token['token_id'] for token in report_data]))
            logger.info(f"Token IDs to fetch market cap for: {all_token_ids}")
            
            # Batch fetch market cap data for all tokens
            market_cap_data = {}
            if all_token_ids:
                try:
                    logger.info(f"Fetching market cap for {len(all_token_ids)} tokens")
                    dex_screener = DexScreenerAction()
                    
                    mcap_start_time = time.time()
                    batch_price_data = dex_screener.getBatchTokenPrices(all_token_ids)
                    mcap_end_time = time.time()
                    
                    logger.info(f"Completed market cap batch fetch in {mcap_end_time - mcap_start_time:.2f}s")
                    logger.info(f"DexScreener returned data for {len(batch_price_data)} tokens")
                    
                    # Extract market cap and token age from batch data
                    for token_id, price_data in batch_price_data.items():
                        if price_data:
                            logger.info(f"Token {token_id}: Market cap = {price_data.marketCap}")
                            market_cap_data[token_id] = {
                                'market_cap': price_data.marketCap,
                                'token_age': 0  # DexScreener doesn't provide token age, default to 0
                            }
                        else:
                            logger.warning(f"No price data returned for token {token_id}")
                            
                except Exception as e:
                    logger.error(f"Error fetching market cap data: {str(e)}")
                    # Continue without external market cap data
            else:
                logger.warning("No token IDs found to fetch market cap for")
            
            # Helper function to format market cap
            def format_market_cap(value):
                if not value or value == 0:
                    return "0"
                
                if value >= 1_000_000_000:  # Billions
                    formatted = value / 1_000_000_000
                    return f"{formatted:.1f}B".rstrip('0').rstrip('.')
                elif value >= 1_000_000:  # Millions
                    formatted = value / 1_000_000
                    return f"{formatted:.1f}M".rstrip('0').rstrip('.')
                elif value >= 1_000:  # Thousands
                    formatted = value / 1_000
                    return f"{formatted:.1f}K".rstrip('0').rstrip('.')
                else:
                    return f"{value:.0f}"
            
            # Update report data with external market cap data
            logger.info(f"Updating market cap for {len(report_data)} tokens")
            for token in report_data:
                token_id = token['token_id']
                original_mcap = token['market_cap']
                
                # Update market cap and token age from DexScreener data
                if token_id in market_cap_data:
                    external_data = market_cap_data[token_id]
                    token['market_cap'] = external_data['market_cap']
                    token['token_age'] = external_data['token_age']
                    logger.info(f"Updated token {token_id}: {original_mcap} → {token['market_cap']}")
                else:
                    logger.warning(f"No market cap data found for token {token_id}")
                
                # Ensure numeric types
                try:
                    token['market_cap'] = float(token['market_cap']) if token['market_cap'] else 0
                    token['token_age'] = float(token['token_age']) if token['token_age'] else 0
                except:
                    logger.error(f"Error converting market cap for token {token_id}")
                    token['market_cap'] = 0
                    token['token_age'] = 0
                
                # Add formatted market cap
                token['market_cap_formatted'] = format_market_cap(token['market_cap'])
                logger.info(f"Final token {token_id}: market_cap={token['market_cap']}, formatted={token['market_cap_formatted']}")
            
            end_time = time.time()
            total_duration = end_time - start_time
            
            logger.info(f"Trading attention report completed in {total_duration:.2f}s for {len(report_data)} tokens")
            
            # Return data in exact format specified
            return jsonify({
                "status": "success",
                "data": report_data
            })

    except Exception as e:
        logger.error(f"Error in trading attention report API: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Internal server error: {str(e)}'
        }), 500

@trading_attention_report_bp.route('/api/reports/tradingattention/token/<token_id>', methods=['GET', 'OPTIONS'])
def get_token_attention_detail(token_id):
    """
    Get detailed attention data for a specific token.
    Provides extended history and granular attention score data.
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        logger.info(f"Fetching detailed attention data for token: {token_id}")
        
        with PortfolioDB() as db:
            handler = TradingAttentionReportHandler(db)
            
            if handler is None:
                logger.error("TradingAttentionReportHandler initialization failed")
                return jsonify({
                    'status': 'error',
                    'message': "Trading attention report handler not available"
                }), 500
            
            # Get detailed data for specific token (could extend the handler for this)
            # For now, filter from main report
            report_data = handler.getTradingAttentionReport()
            token_data = next((token for token in report_data if token['token_id'] == token_id), None)
            
            if not token_data:
                logger.warning(f"No attention data found for token: {token_id}")
                return jsonify({
                    'status': 'success',
                    'data': None
                })
            
            logger.info(f"Retrieved detailed attention data for token: {token_id}")
            
            return jsonify({
                'status': 'success',
                'data': token_data
            })
            
    except Exception as e:
        logger.error(f"Error fetching token attention detail: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Internal server error: {str(e)}'
        }), 500

@trading_attention_report_bp.route('/api/reports/tradingattention/stats', methods=['GET', 'OPTIONS'])
def get_trading_attention_stats():
    """
    Get summary statistics for trading attention report.
    Provides high-level metrics for dashboard use.
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        logger.info("Fetching trading attention statistics")
        
        with PortfolioDB() as db:
            handler = TradingAttentionReportHandler(db)
            
            if handler is None:
                logger.error("TradingAttentionReportHandler initialization failed")
                return jsonify({
                    'status': 'error',
                    'message': "Trading attention report handler not available"
                }), 500
            
            # Get report data for stats calculation
            report_data = handler.getTradingAttentionReport()
            
            if not report_data:
                return jsonify({
                    'status': 'success',
                    'data': {
                        'total_tokens': 0,
                        'total_market_cap': 0,
                        'avg_attention_score': 0,
                        'highest_attention_token': None
                    }
                })
            
            # Calculate statistics
            total_tokens = len(report_data)
            total_market_cap = sum(token['market_cap'] for token in report_data)
            
            # Get latest day attention scores
            latest_scores = []
            highest_attention_token = None
            max_score = 0
            
            for token in report_data:
                if token['attention_data']:
                    # Get day 1 (latest) max score
                    day_1_data = next((day for day in token['attention_data'] if day['day'] == 1), None)
                    if day_1_data:
                        score = day_1_data['max_score']
                        latest_scores.append(score)
                        if score > max_score:
                            max_score = score
                            highest_attention_token = {
                                'name': token['name'],
                                'token_id': token['token_id'],
                                'max_score': score
                            }
            
            avg_attention_score = sum(latest_scores) / len(latest_scores) if latest_scores else 0
            
            stats = {
                'total_tokens': total_tokens,
                'total_market_cap': round(total_market_cap, 2),
                'avg_attention_score': round(avg_attention_score, 2),
                'highest_attention_token': highest_attention_token
            }
            
            logger.info(f"Generated trading attention statistics: {stats}")
            
            return jsonify({
                'status': 'success',
                'data': stats
            })
            
    except Exception as e:
        logger.error(f"Error fetching trading attention stats: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Internal server error: {str(e)}'
        }), 500