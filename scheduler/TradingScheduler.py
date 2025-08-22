from config.Config import get_config
from typing import Optional, Dict, Any, List
from database.operations.PortfolioDB import PortfolioDB
from actions.TradingAction import TradingAction
from datetime import datetime, timedelta
from logs.logger import get_logger
import time
import traceback
import pytz

logger = get_logger(__name__)

class TradingScheduler:
    """
    Trading scheduler for automated crypto data fetching and processing
    Runs every 5 minutes to process tokens due for data updates
    """

    def __init__(self, db: PortfolioDB = None):
        """
        Initialize the trading scheduler
        
        Args:
            db: Database instance (optional, will create if not provided)
        """
        self.db = db if db else PortfolioDB()
        self.trading_action = TradingAction(self.db)
        self.config = get_config()
        
        # Scheduler configuration
        self.batch_size = 50  # Process up to 50 tokens per run
        self.max_failures_threshold = 5  # Disable token after 5 consecutive failures
        self.rate_limit_delay = 1.0  # Seconds between API calls
        
        logger.info("TradingScheduler initialized")

    def handleTradingDataFromAPI(self) -> Dict[str, Any]:
        """
        Main entry point for scheduled trading data processing
        Called by the scheduler every 5 minutes
        
        Returns:
            Dict: Processing results summary
        """
        try:
            logger.info("=== Starting scheduled trading data fetch ===")
            start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            # Get tokens due for fetching
            tokens_due = self.db.trading.getTokensDueForFetch(limit=self.batch_size)
            
            if not tokens_due:
                logger.info("No tokens due for fetching at this time")
                return {
                    'success': True,
                    'tokensProcessed': 0,
                    'tokensSuccessful': 0,
                    'tokensFailed': 0,
                    'message': 'No tokens due for processing'
                }
            
            logger.info(f"Found {len(tokens_due)} tokens due for fetching")
            
            # Process tokens
            results = self.processBatchTokens(tokens_due)
            
            # Calculate processing time
            end_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            processing_time = (end_time - start_time).total_seconds()
            
            # Log summary
            logger.info(f"=== Scheduled trading data fetch completed ===")
            logger.info(f"Processing time: {processing_time:.2f} seconds")
            logger.info(f"Tokens processed: {results['tokensProcessed']}")
            logger.info(f"Successful: {results['tokensSuccessful']}")
            logger.info(f"Failed: {results['tokensFailed']}")
            
            # Update results with timing
            results.update({
                'processingTimeSeconds': processing_time,
                'startTime': start_time.isoformat(),
                'endTime': end_time.isoformat()
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error in scheduled trading data processing: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                'success': False,
                'error': f'Scheduled processing failed: {str(e)}',
                'tokensProcessed': 0,
                'tokensSuccessful': 0,
                'tokensFailed': 0
            }

    def processBatchTokens(self, tokens_due: List[Dict]) -> Dict[str, Any]:
        """
        High-performance batch processing of tokens using optimized queries
        Processes all tokens in minimal database round trips
        """
        tokens_processed = 0
        tokens_successful = 0
        tokens_failed = 0
        token_results = []
        
        try:
            # Extract token addresses for batch operations
            token_addresses = [token['tokenaddress'] for token in tokens_due if token['consecutivefailures'] < self.max_failures_threshold]
            
            if not token_addresses:
                logger.info("No tokens eligible for processing")
                return {
                    'success': True,
                    'tokensProcessed': 0,
                    'tokensSuccessful': 0,
                    'tokensFailed': 0,
                    'message': 'No eligible tokens for processing'
                }
            
            logger.info(f"Batch processing {len(token_addresses)} tokens")
            
            # Get all indicator states in single query
            indicator_states = self.db.trading.getBatchIndicatorStates(token_addresses, '15m')
            
            # Get latest candles for all tokens
            latest_candles = self.db.trading.getBatchLatestCandles(token_addresses, '15m')
            
            # Process each token with fetched data
            for token_metadata in tokens_due:
                token_address = token_metadata['tokenaddress']
                symbol = token_metadata['symbol']
                
                if token_metadata['consecutivefailures'] >= self.max_failures_threshold:
                    continue
                
                try:
                    # Process single token with batch-fetched data
                    success = self.processSingleTokenOptimized(
                        token_metadata,
                        indicator_states.get(token_address, {}),
                        latest_candles.get(token_address)
                    )
                    
                    tokens_processed += 1
                    
                    # Record result for batch status update
                    token_results.append({
                        'token_address': token_address,
                        'success': success,
                        'current_failures': token_metadata['consecutivefailures']
                    })
                    
                    if success:
                        tokens_successful += 1
                        logger.debug(f"Successfully processed {symbol}")
                    else:
                        tokens_failed += 1
                        logger.warning(f"Failed to process {symbol}")
                
                except Exception as e:
                    logger.error(f"Error processing token {symbol}: {e}")
                    tokens_processed += 1
                    tokens_failed += 1
                    token_results.append({
                        'token_address': token_address,
                        'success': False,
                        'current_failures': token_metadata['consecutivefailures']
                    })
            
            # Batch update fetch status for all tokens
            updated_count = self.db.trading.updateFetchStatusBatch(token_results)
            logger.info(f"Updated fetch status for {updated_count} tokens")
            
            return {
                'success': True,
                'tokensProcessed': tokens_processed,
                'tokensSuccessful': tokens_successful,
                'tokensFailed': tokens_failed,
                'message': f'Batch processed {tokens_processed} tokens: {tokens_successful} successful, {tokens_failed} failed'
            }
            
        except Exception as e:
            logger.error(f"Error in batch token processing: {e}")
            return {
                'success': False,
                'error': f'Batch processing failed: {str(e)}',
                'tokensProcessed': tokens_processed,
                'tokensSuccessful': tokens_successful,
                'tokensFailed': tokens_failed
            }

    def processSingleTokenOptimized(self, token_metadata: Dict, indicator_states: Dict, latest_candle: Optional[Dict]) -> bool:
        """
        Process single token with pre-fetched data to avoid database round trips
        """
        try:
            token_address = token_metadata['tokenaddress']
            pair_address = token_metadata['pairaddress']
            symbol = token_metadata['symbol']
            
            # Calculate time range for fetching
            now = int(time.time())
            time_from = now - 7200  # 2 hours ago
            
            if latest_candle:
                time_from = max(time_from, latest_candle['unixtime'])
            
            # Fetch new data from BirdEye
            ohlcv_data = self.trading_action.fetchBirdEyeOHLCV(
                pair_address=pair_address,
                timeframe="15m",
                time_from=time_from,
                time_to=now
            )
            
            if not ohlcv_data:
                return False
            
            # Process new candles efficiently
            new_candles = []
            indicator_updates = []
            
            for item in ohlcv_data:
                # Skip if we already have this candle
                if latest_candle and item.unixTime <= latest_candle['unixtime']:
                    continue
                
                # Prepare candle data
                candle_data = {
                    'tokenaddress': token_address,
                    'pairaddress': pair_address,
                    'timeframe': '15m',
                    'unixtime': item.unixTime,
                    'openprice': item.o,
                    'highprice': item.h,
                    'lowprice': item.l,
                    'closeprice': item.c,
                    'volume': item.v,
                    'datasource': 'api'
                }
                
                new_candles.append(candle_data)
                
                # Calculate indicators if we have enough data
                indicator_update = self.calculateIndicatorsOptimized(
                    token_address, candle_data, indicator_states
                )
                if indicator_update:
                    indicator_updates.append(indicator_update)
            
            if new_candles:
                # Batch insert new candles
                inserted_count = self.db.trading.batchInsertCandles(new_candles)
                
                # Batch update indicators
                if indicator_updates:
                    updated_count = self.db.trading.bulkUpdateIndicators(indicator_updates)
                    logger.debug(f"Updated indicators for {updated_count} candles")
                
                logger.debug(f"Processed {inserted_count} new candles for {symbol}")
                
            return len(new_candles) > 0
            
        except Exception as e:
            logger.error(f"Error in optimized token processing: {e}")
            return False

    def calculateIndicatorsOptimized(self, token_address: str, candle_data: Dict, indicator_states: Dict) -> Optional[Dict]:
        """
        Calculate indicators efficiently using pre-fetched states
        """
        try:
            from decimal import Decimal
            
            close_price = Decimal(str(candle_data['closeprice']))
            high_price = Decimal(str(candle_data['highprice']))
            low_price = Decimal(str(candle_data['lowprice']))
            volume = Decimal(str(candle_data['volume']))
            
            update_data = {'candle_id': None}  # Will be set after insert
            
            # Calculate EMA21
            ema21_state = indicator_states.get('ema_21')
            if ema21_state and ema21_state.get('iswarmedup'):
                current_ema21 = Decimal(str(ema21_state['currentvalue']))
                multiplier = Decimal(2) / Decimal(22)  # 2/(21+1)
                new_ema21 = (close_price * multiplier) + (current_ema21 * (1 - multiplier))
                update_data['ema21'] = new_ema21
            
            # Calculate EMA34
            ema34_state = indicator_states.get('ema_34')
            if ema34_state and ema34_state.get('iswarmedup'):
                current_ema34 = Decimal(str(ema34_state['currentvalue']))
                multiplier = Decimal(2) / Decimal(35)  # 2/(34+1)
                new_ema34 = (close_price * multiplier) + (current_ema34 * (1 - multiplier))
                update_data['ema34'] = new_ema34
            
            # Calculate VWAP (simplified for performance)
            typical_price = (high_price + low_price + close_price) / 3
            update_data['vwap'] = typical_price  # Simplified calculation
            
            return update_data if len(update_data) > 1 else None
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return None

    def handleFailedTokens(self, failed_tokens: List[Dict]) -> None:
        """
        Handle tokens that failed processing - disable if too many failures
        
        Args:
            failed_tokens: List of failed token information
        """
        for failed_token in failed_tokens:
            try:
                consecutive_failures = failed_token.get('consecutiveFailures', 0)
                
                if consecutive_failures >= self.max_failures_threshold:
                    token_address = failed_token['tokenAddress']
                    symbol = failed_token['symbol']
                    
                    logger.warning(f"Disabling token {symbol} due to {consecutive_failures} consecutive failures")
                    
                    success = self.db.trading.disableToken(
                        tokenAddress=token_address,
                        disabledBy='scheduler',
                        reason=f'Disabled due to {consecutive_failures} consecutive API failures'
                    )
                    
                    if success:
                        logger.info(f"Successfully disabled token {symbol}")
                    else:
                        logger.error(f"Failed to disable token {symbol}")
                        
            except Exception as e:
                logger.error(f"Error handling failed token: {e}")

    def processIndicators(self, token_address: str, candle_data: Dict) -> bool:
        """
        Process indicators for a newly received candle
        
        Args:
            token_address: Token contract address
            candle_data: New candle data
            
        Returns:
            bool: Success status
        """
        try:
            from database.trading.TradingModels import EMACalculationInput, VWAPCalculationInput
            from decimal import Decimal
            
            # Calculate EMA21
            ema21_input = EMACalculationInput(
                tokenaddress=token_address,
                timeframe=candle_data['timeframe'],
                period=21,
                closeprice=Decimal(str(candle_data['closeprice'])),
                unixtime=candle_data['unixtime']
            )
            ema21_value = self.trading_action.calculateEMA(ema21_input)
            
            # Calculate EMA34
            ema34_input = EMACalculationInput(
                tokenaddress=token_address,
                timeframe=candle_data['timeframe'],
                period=34,
                closeprice=Decimal(str(candle_data['closeprice'])),
                unixtime=candle_data['unixtime']
            )
            ema34_value = self.trading_action.calculateEMA(ema34_input)
            
            # Calculate VWAP
            vwap_input = VWAPCalculationInput(
                tokenaddress=token_address,
                timeframe=candle_data['timeframe'],
                highprice=Decimal(str(candle_data['highprice'])),
                lowprice=Decimal(str(candle_data['lowprice'])),
                closeprice=Decimal(str(candle_data['closeprice'])),
                volume=Decimal(str(candle_data['volume'])),
                unixtime=candle_data['unixtime']
            )
            vwap_value = self.trading_action.calculateVWAP(vwap_input)
            
            # Update candle with validation columns
            if ema21_value or ema34_value or vwap_value:
                # Update the OHLCV record with calculated indicators
                # This would be done in the trading handler
                logger.debug(f"Calculated indicators for {token_address}: EMA21={ema21_value}, EMA34={ema34_value}, VWAP={vwap_value}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing indicators for {token_address}: {e}")
            return False

    def checkAggregationTriggers(self, token_address: str, candle_unixtime: int) -> None:
        """
        Check if new candle triggers aggregation to higher timeframes
        
        Args:
            token_address: Token contract address
            candle_unixtime: Unix timestamp of the new candle
        """
        try:
            from database.trading.TradingModels import shouldTrigger1HourAggregation, shouldTrigger4HourAggregation
            
            # Check for 1-hour aggregation trigger (at :45 minutes)
            if shouldTrigger1HourAggregation(candle_unixtime):
                logger.info(f"Triggering 1-hour aggregation for {token_address}")
                self.aggregate1HourCandle(token_address, candle_unixtime)
            
            # Check for 4-hour aggregation trigger (at :45 minutes of 4-hour periods)
            if shouldTrigger4HourAggregation(candle_unixtime):
                logger.info(f"Triggering 4-hour aggregation for {token_address}")
                self.aggregate4HourCandle(token_address, candle_unixtime)
                
        except Exception as e:
            logger.error(f"Error checking aggregation triggers for {token_address}: {e}")

    def aggregate1HourCandle(self, token_address: str, trigger_unixtime: int) -> bool:
        """
        Aggregate 15-minute candles into 1-hour candle
        
        Args:
            token_address: Token contract address
            trigger_unixtime: Unix timestamp that triggered aggregation
            
        Returns:
            bool: Success status
        """
        try:
            # Calculate hour bucket start
            hour_start = (trigger_unixtime // 3600) * 3600
            hour_end = hour_start + 3599
            
            # Get 15-minute candles for this hour (4 candles needed)
            candles_15m = self.db.trading.getCandlesForAggregation(
                tokenAddress=token_address,
                timeframe="15m",
                startTime=hour_start,
                endTime=hour_end
            )
            
            if len(candles_15m) != 4:
                logger.warning(f"Incomplete hour for aggregation: {len(candles_15m)}/4 candles for {token_address}")
                return False
            
            # Use parser to aggregate candles
            from parsers.TradingParser import TradingParser
            from database.trading.TradingModels import OHLCVCandle
            
            # Convert dict to OHLCVCandle objects
            candle_objects = []
            for candle_dict in candles_15m:
                candle = OHLCVCandle(
                    tokenaddress=candle_dict['tokenaddress'],
                    pairaddress=candle_dict['pairaddress'],
                    timeframe=candle_dict['timeframe'],
                    unixtime=candle_dict['unixtime'],
                    openprice=candle_dict['openprice'],
                    highprice=candle_dict['highprice'],
                    lowprice=candle_dict['lowprice'],
                    closeprice=candle_dict['closeprice'],
                    volume=candle_dict['volume'],
                    iscomplete=candle_dict['iscomplete']
                )
                candle_objects.append(candle)
            
            # Aggregate to 1-hour candle
            aggregated_candle = TradingParser.aggregateCandles(candle_objects, "1h")
            
            if not aggregated_candle:
                logger.error(f"Failed to aggregate 1-hour candle for {token_address}")
                return False
            
            # Insert aggregated candle
            candle_id = self.db.trading.insertOHLCVCandle(
                tokenAddress=aggregated_candle.tokenaddress,
                pairAddress=aggregated_candle.pairaddress,
                timeframe="1h",
                unixTime=aggregated_candle.unixtime,
                openPrice=aggregated_candle.openprice,
                highPrice=aggregated_candle.highprice,
                lowPrice=aggregated_candle.lowprice,
                closePrice=aggregated_candle.closeprice,
                volume=aggregated_candle.volume,
                dataSource="aggregated"
            )
            
            if candle_id:
                logger.info(f"Successfully aggregated 1-hour candle for {token_address}")
                
                # Process indicators for the new 1-hour candle
                candle_dict = {
                    'timeframe': '1h',
                    'unixtime': aggregated_candle.unixtime,
                    'closeprice': aggregated_candle.closeprice,
                    'highprice': aggregated_candle.highprice,
                    'lowprice': aggregated_candle.lowprice,
                    'volume': aggregated_candle.volume
                }
                self.processIndicators(token_address, candle_dict)
                
                return True
            else:
                logger.error(f"Failed to insert aggregated 1-hour candle for {token_address}")
                return False
                
        except Exception as e:
            logger.error(f"Error aggregating 1-hour candle for {token_address}: {e}")
            return False

    def aggregate4HourCandle(self, token_address: str, trigger_unixtime: int) -> bool:
        """
        Aggregate 15-minute candles into 4-hour candle
        
        Args:
            token_address: Token contract address
            trigger_unixtime: Unix timestamp that triggered aggregation
            
        Returns:
            bool: Success status
        """
        try:
            # Calculate 4-hour bucket start
            four_hour_start = (trigger_unixtime // 14400) * 14400
            four_hour_end = four_hour_start + 14399
            
            # Get 15-minute candles for this 4-hour period (16 candles needed)
            candles_15m = self.db.trading.getCandlesForAggregation(
                tokenAddress=token_address,
                timeframe="15m",
                startTime=four_hour_start,
                endTime=four_hour_end
            )
            
            if len(candles_15m) != 16:
                logger.warning(f"Incomplete 4-hour period for aggregation: {len(candles_15m)}/16 candles for {token_address}")
                return False
            
            # Similar aggregation logic as 1-hour, but for 4-hour timeframe
            from parsers.TradingParser import TradingParser
            from database.trading.TradingModels import OHLCVCandle
            
            # Convert and aggregate (same logic as 1-hour)
            candle_objects = []
            for candle_dict in candles_15m:
                candle = OHLCVCandle(
                    tokenaddress=candle_dict['tokenaddress'],
                    pairaddress=candle_dict['pairaddress'],
                    timeframe=candle_dict['timeframe'],
                    unixtime=candle_dict['unixtime'],
                    openprice=candle_dict['openprice'],
                    highprice=candle_dict['highprice'],
                    lowprice=candle_dict['lowprice'],
                    closeprice=candle_dict['closeprice'],
                    volume=candle_dict['volume'],
                    iscomplete=candle_dict['iscomplete']
                )
                candle_objects.append(candle)
            
            aggregated_candle = TradingParser.aggregateCandles(candle_objects, "4h")
            
            if aggregated_candle:
                candle_id = self.db.trading.insertOHLCVCandle(
                    tokenAddress=aggregated_candle.tokenaddress,
                    pairAddress=aggregated_candle.pairaddress,
                    timeframe="4h",
                    unixTime=aggregated_candle.unixtime,
                    openPrice=aggregated_candle.openprice,
                    highPrice=aggregated_candle.highprice,
                    lowPrice=aggregated_candle.lowprice,
                    closePrice=aggregated_candle.closeprice,
                    volume=aggregated_candle.volume,
                    dataSource="aggregated"
                )
                
                if candle_id:
                    logger.info(f"Successfully aggregated 4-hour candle for {token_address}")
                    return True
            
            logger.error(f"Failed to aggregate 4-hour candle for {token_address}")
            return False
            
        except Exception as e:
            logger.error(f"Error aggregating 4-hour candle for {token_address}: {e}")
            return False

    def getSystemStatus(self) -> Dict[str, Any]:
        """
        Get current system status and health metrics
        
        Returns:
            Dict: System status information
        """
        try:
            # Get active tokens
            active_tokens = self.db.trading.getActiveTokens()
            active_count = len(active_tokens)
            
            # Get tokens due for fetching
            tokens_due = self.db.trading.getTokensDueForFetch(limit=1000)
            due_count = len(tokens_due)
            
            # Calculate failure statistics
            total_failures = 0
            failed_tokens = 0
            overdue_tokens = 0
            
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            for token in tokens_due:
                if token['consecutivefailures'] > 0:
                    failed_tokens += 1
                    total_failures += token['consecutivefailures']
                
                # Check if token is overdue (next fetch time is in the past)
                if token['nextfetchat'] < current_time:
                    overdue_tokens += 1
            
            # Calculate health score
            if active_count == 0:
                health_score = 100
            else:
                failure_rate = failed_tokens / active_count
                overdue_rate = overdue_tokens / active_count
                health_score = max(0, 100 - (failure_rate * 50) - (overdue_rate * 50))
            
            # Determine health status
            if health_score >= 90:
                health_status = "HEALTHY"
            elif health_score >= 70:
                health_status = "WARNING"
            else:
                health_status = "CRITICAL"
            
            return {
                'success': True,
                'systemHealth': {
                    'status': health_status,
                    'score': round(health_score, 2),
                    'timestamp': current_time.isoformat()
                },
                'statistics': {
                    'activeTokens': active_count,
                    'tokensDueForFetch': due_count,
                    'overdueTokens': overdue_tokens,
                    'tokensWithFailures': failed_tokens,
                    'totalFailures': total_failures,
                    'averageFailuresPerToken': round(total_failures / max(1, failed_tokens), 2)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                'success': False,
                'error': f'Failed to get system status: {str(e)}'
            }