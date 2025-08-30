"""
SchedulerUtil - Utility functions for Trading Scheduler

Contains all batch processing, validation, and database utility functions
extracted from TradingScheduler for better modularity and separation of concerns.
"""

from config.Config import get_config
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler
from database.trading.TradingModels import SchedulerConfig
from actions.TradingActionEnhanced import TradingActionEnhanced
from actions.TradingActionUtil import TradingActionUtil
from logs.logger import get_logger
from typing import List, Dict, Any, Optional
from sqlalchemy import text
import time
from services.BirdEyeServiceHandler import BirdEyeServiceHandler

logger = get_logger(__name__)


class SchedulerUtil:
    """
    Static utility class containing all batch processing and helper functions
    for the Trading Scheduler system
    
    All methods are static to avoid unnecessary instance creation overhead.
    Dependencies are passed as parameters to each method as needed.
    """
    
    # ===============================================================
    # VALIDATION UTILITIES
    # ===============================================================
    
    @staticmethod
    def validateTokens(tokens: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate and sanitize token records"""
        validated = []
        required_fields = ['tokenaddress', 'pairaddress', 'symbol']
        
        for token in tokens:
            try:
                # Check required fields
                if not all(field in token and token[field] for field in required_fields):
                    logger.warning(f"Token missing required fields: {token.get('symbol', 'unknown')}")
                    continue
                
                # Validate addresses format (basic check)
                if len(token['tokenaddress']) < 32 or len(token['pairaddress']) < 32:
                    logger.warning(f"Invalid address format for token: {token['symbol']}")
                    continue
                    
                validated.append(token)
                
            except Exception as e:
                logger.error(f"Error validating token {token.get('symbol', 'unknown')}: {e}")
                continue
        
        return validated
    
    @staticmethod
    def validateTokenForAggregation(token: Dict[str, Any]) -> bool:
        """Validate if token is ready for aggregation"""
        required_fields = ['tokenaddress', 'pairaddress']
        return all(field in token and token[field] for field in required_fields)
    
    @staticmethod
    def logProcessingMetrics(total_tokens: int, success_count: int, error_count: int, elapsed_time: float):
        """Log processing metrics for monitoring"""
        success_rate = (success_count / total_tokens * 100) if total_tokens > 0 else 0
        avg_time_per_token = elapsed_time / total_tokens if total_tokens > 0 else 0
        
        logger.info(f"METRICS - Total: {total_tokens}, Success Rate: {success_rate:.1f}%, "
                   f"Avg Time/Token: {avg_time_per_token:.2f}s, Total Time: {elapsed_time:.2f}s")
    
    # ===============================================================
    # ENHANCED BATCH PROCESSING METHODS
    # ===============================================================
    
    
    @staticmethod
    def batchAggregateTimeFrames(tokens: List[Dict[str, Any]], trading_handler: TradingHandler) -> bool:
        """Enhanced batch aggregation with validation and error isolation"""
        try:
            if not tokens:
                return True
            logger.info(f"Starting aggregation for {len(tokens)} tokens")
            SchedulerUtil.batchAggregate(tokens, trading_handler)
            return True
            
        except Exception as e:
            logger.error(f"Error in batch aggregation validation: {e}", exc_info=True)
            return False
    
    @staticmethod
    def batchUpdateIndicatorsWithMemoryManagement(tokens: List[Dict[str, Any]], trading_handler: TradingHandler, trading_action) -> bool:
        """Enhanced indicator updates with memory management and chunking"""
        try:
            if not tokens:
                return True
            
            # Process in chunks to manage memory
            chunk_size = min(50, len(tokens))  # Process max 50 tokens at once for indicators
            
            for i in range(0, len(tokens), chunk_size):
                chunk = tokens[i:i + chunk_size]
                logger.info(f"Processing indicator chunk {i//chunk_size + 1}/{(len(tokens) + chunk_size - 1)//chunk_size}")
                
                try:
                    # Get historical data for this chunk
                    historical_data = SchedulerUtil.batchGet2DayHistoricalData(chunk, SchedulerConfig(), trading_handler)
                    
                    # Update indicators for this chunk
                    SchedulerUtil.batchUpdateAllIndicators(chunk, historical_data, trading_action)
                    
                    # Clear memory
                    del historical_data
                    
                except Exception as chunk_error:
                    logger.error(f"Error processing indicator chunk: {chunk_error}")
                    continue  # Continue with next chunk
            
            return True
            
        except Exception as e:
            logger.error(f"Error in batch indicator update with memory management: {e}", exc_info=True)
            return False
    
    # ===============================================================
    # CORE BATCH PROCESSING METHODS
    # ===============================================================
    
    @staticmethod
    def batchFetch15mCandlesForAllTokens(tokens: List[Dict[str, Any]], config: SchedulerConfig, trading_handler: TradingHandler, trading_action) -> Dict[str, Dict]:
        """
        OPTIMIZED: Batch fetch candles from API for ALL tokens, collect data, then batch persist at once
        
        Note: Retry logic was removed to simplify error handling and reduce complexity.
        API failures are now handled gracefully by returning failure results for affected tokens
        rather than retrying, which can cause delays and potential rate limiting issues.
        """
        all_candles_to_insert = []
        successful_tokens_list = []
        failed_tokens_count = 0
        
        # STEP 1: Fetch from API for all tokens (collect data only)
        for token in tokens:
            try:
                token_address = token['tokenaddress']
                pair_address = token['pairaddress']
                symbol = token['symbol']
                
                # Get last fetch time from timeframemetadata.lastfetchedat
                last_fetch_time = None
                if 'lastfetchedat' in token and token['lastfetchedat']:
                    if hasattr(token['lastfetchedat'], 'timestamp'):
                        last_fetch_time = int(token['lastfetchedat'].timestamp())
                    else:
                        last_fetch_time = token['lastfetchedat']
                
                # If no lastfetchedat, start from lastfetchedat + 1 or pair creation time
                if not last_fetch_time:
                    last_fetch_time = token.get('paircreatedtime', int(time.time()) - 86400)
                else:
                    # Start from lastfetchedat + 1 second to avoid duplicates
                    last_fetch_time = last_fetch_time + 1
                
                # Fetch candles from API using BirdEyeServiceHandler
                birdeye_service = BirdEyeServiceHandler(trading_action.db)
                api_result = birdeye_service.getCandleDataForToken(
                    token_address, pair_address, last_fetch_time, int(time.time()), symbol
                )
                
                if api_result['success'] and api_result['candles']:
                    # Candles already have token metadata from BirdEyeServiceHandler
                    all_candles_to_insert.extend(api_result['candles'])
                    successful_tokens_list.append(token)
                    logger.info(f"API fetched {len(api_result['candles'])} candles for {symbol}")
                else:
                    failed_tokens_count += 1
                    logger.warning(f"Failed to fetch candles for {symbol}: {api_result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                failed_tokens_count += 1
                logger.error(f"Error fetching candles for {token['symbol']}: {e}")
        
        # STEP 2: Batch persist ALL candles at once
        if all_candles_to_insert:
            success_count = SchedulerUtil.batchPersistAllCandles(all_candles_to_insert, trading_handler)
            logger.info(f"Batch persisted {success_count} total candles for {len(tokens)} tokens")
        
        successful_tokens_count = len(successful_tokens_list)
        logger.info(f"API fetch completed: {successful_tokens_count} successful, {failed_tokens_count} failed out of {len(tokens)} tokens")
        
        return {
            'successful_tokens': successful_tokens_count, 
            'failed_tokens': failed_tokens_count,
            'successful_tokens_list': successful_tokens_list
        }
    
    
    @staticmethod
    def batchPersistAllCandles(all_candles: List[Dict], trading_handler: TradingHandler) -> int:
        """
        Batch persist ALL candles from ALL tokens in single database transaction
        """
        try:
            if not all_candles:
                return 0
            
            success_count = 0
            
            with trading_handler.conn_manager.transaction() as cursor:
                # Group candles by token for timeframe record creation
                tokens_processed = set()
                
                # Ensure timeframe records exist for all unique token-pair combinations
                for candle in all_candles:
                    token_pair = (candle['tokenaddress'], candle['pairaddress'])
                    if token_pair not in tokens_processed:
                        try:
                            trading_handler._createTimeframeRecord(
                                candle['tokenaddress'], candle['pairaddress'], '15m'
                            )
                        except:
                            pass  # Record may already exist
                        tokens_processed.add(token_pair)
                
                # Batch insert all candles
                insert_data = []
                for candle in all_candles:
                    insert_data.append((
                        candle['tokenaddress'], candle['pairaddress'], candle['timeframe'],
                        candle['unixtime'], candle['openprice'], candle['highprice'],
                        candle['lowprice'], candle['closeprice'], candle['volume'], 
                        candle['datasource']
                    ))
                
                cursor.executemany(text("""
                    INSERT INTO ohlcvdetails 
                    (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
                     highprice, lowprice, closeprice, volume, datasource)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                """), insert_data)
                
                success_count = len(all_candles)
                
                # OPTIMIZED: Batch update lastfetchedat for all tokens in single query
                token_latest_times = {}
                for candle in all_candles:
                    token_addr = candle['tokenaddress']
                    token_latest_times[token_addr] = max(
                        token_latest_times.get(token_addr, 0), candle['unixtime']
                    )
                
                if token_latest_times:
                    # OPTIMIZED: Single batch update using CASE statement
                    token_addrs = list(token_latest_times.keys())
                    latest_times = [token_latest_times[addr] for addr in token_addrs]
                    
                    cursor.execute(text("""
                        UPDATE timeframemetadata 
                        SET lastfetchedat = to_timestamp(
                            CASE tokenaddress
                            """ + " ".join([f"WHEN %s THEN %s" for _ in token_addrs]) + """
                            END
                        )
                        WHERE tokenaddress = ANY(%s) AND timeframe = '15m'
                    """), [item for pair in zip(token_addrs, latest_times) for item in pair] + [token_addrs])
            
            return success_count
            
        except Exception as e:
            logger.error(f"Error in batch persist: {e}")
            return 0
    
    @staticmethod
    def batchAggregate(tokens: List[Dict[str, Any]], trading_handler: TradingHandler):
        """
        OPTIMIZED: Batch aggregate ALL tokens with single DB query approach
        Gets all 15m candles needing aggregation and processes both 1h and 4h together
        """
        try:
            logger.info(f"BATCH AGGREGATING for {len(tokens)} tokens")
            
            # Get all token addresses
            token_addresses = [t['tokenaddress'] for t in tokens]
            
            # SINGLE QUERY: Get all 15m candles needing aggregation for ALL tokens
            candles_by_token = trading_handler.getCandlesForAggregation(token_addresses)
            
            if not candles_by_token:
                logger.info("No candles found for aggregation")
                return
            
            # Process aggregation efficiently - calculate 1h and 4h together
            all_aggregated_candles = SchedulerUtil.batchProcessOptimizedAggregation(candles_by_token)
            
            # ATOMIC: Insert aggregated candles and update fetch times in single transaction
            if all_aggregated_candles:
                success_count = trading_handler.batchInsertAggregatedCandlesAndUpdateFetchTimes(
                    all_aggregated_candles, ['1h', '4h']
                )
                logger.info(f"BATCH AGGREGATION completed: inserted {success_count} aggregated candles for {len(tokens)} tokens")
            else:
                logger.info("No aggregated candles to insert")
            
        except Exception as e:
            logger.error(f"Error in batch aggregation: {e}")
    
    @staticmethod
    def batchProcessOptimizedAggregation(candles_by_token: Dict[str, Dict]) -> List[Dict]:
        """
        Clean aggregation logic following the clear approach:
        
        1. Get all 15m candles using MIN(last_1h_fetch, last_4h_fetch)
        2. Aggregate 15m -> 1h for all candles
        3. Aggregate 1h -> 4h from the 1h candles
        4. Persist only candles where unixtime > corresponding last_fetch_time
        """
        try:
            all_aggregated_to_persist = []
            
            for token_addr, token_data in candles_by_token.items():
                candles_15m = token_data['candles_15m']
                if not candles_15m:
                    continue
                
                pair_addr = token_data['pairaddress']
                lastfetchedat_1h = token_data['lastfetchedat_1h']
                lastfetchedat_4h = token_data['lastfetchedat_4h']
                nextfetchat_1h = token_data['nextfetchat_1h'] 
                nextfetchat_4h = token_data['nextfetchat_4h']
                
                logger.debug(f"Processing {token_addr}: {len(candles_15m)} 15m candles, "
                           f"nextfetchat_1h={nextfetchat_1h}, nextfetchat_4h={nextfetchat_4h}")
                
                # Step 1: Aggregate all 15m candles to 1h
                hourly_result = TradingActionUtil.aggregateToHourlyInMemory(candles_15m)
                all_1h_candles = hourly_result.get('candles', [])
                
                # Step 2: Filter and persist only 1h candles >= nextfetchat_1h
                new_1h_candles = []
                for candle in all_1h_candles:
                    if candle['unixtime'] >= nextfetchat_1h:
                        new_1h_candles.append(candle)
                        all_aggregated_to_persist.append({
                            'tokenaddress': token_addr,
                            'pairaddress': pair_addr,
                            'timeframe': '1h',
                            'unixtime': candle['unixtime'],
                            'openprice': candle['openprice'],
                            'highprice': candle['highprice'],
                            'lowprice': candle['lowprice'],
                            'closeprice': candle['closeprice'],
                            'volume': candle['volume']
                        })
                
                # Step 3: Aggregate all 1h candles to 4h
                new_4h_candles = []  # Initialize outside the if block
                if all_1h_candles:
                    four_hourly_result = TradingActionUtil.aggregateTo4HourlyInMemory(all_1h_candles)
                    all_4h_candles = four_hourly_result.get('candles', [])
                    
                    # Step 4: Filter and persist only 4h candles >= nextfetchat_4h
                    for candle in all_4h_candles:
                        if candle['unixtime'] >= nextfetchat_4h:
                            new_4h_candles.append(candle)
                            all_aggregated_to_persist.append({
                                'tokenaddress': token_addr,
                                'pairaddress': pair_addr,
                                'timeframe': '4h',
                                'unixtime': candle['unixtime'],
                                'openprice': candle['openprice'],
                                'highprice': candle['highprice'],
                                'lowprice': candle['lowprice'],
                                'closeprice': candle['closeprice'],
                                'volume': candle['volume']
                            })
                
                logger.debug(f"Token {token_addr}: Will persist {len(new_1h_candles)} 1h and {len(new_4h_candles)} 4h candles")
            
            # Summary logging
            h1_count = sum(1 for c in all_aggregated_to_persist if c['timeframe'] == '1h')
            h4_count = sum(1 for c in all_aggregated_to_persist if c['timeframe'] == '4h')
            logger.info(f"Aggregation complete: {h1_count} new 1h candles, {h4_count} new 4h candles to persist")
            
            return all_aggregated_to_persist
            
        except Exception as e:
            logger.error(f"Error in optimized aggregation processing: {e}")
            return []
    
    
    @staticmethod
    def batchGet2DayHistoricalData(tokens: List[Dict[str, Any]], config: SchedulerConfig, trading_handler: TradingHandler) -> Dict[str, Dict]:
        """
        Get 2-day historical data for ALL tokens and ALL timeframes using TradingHandler
        Returns data keyed by token_address -> timeframe -> [candles]
        """
        try:
            token_addresses = [t['tokenaddress'] for t in tokens]
            return trading_handler.get2DayHistoricalData(token_addresses, config.HISTORICAL_DATA_DAYS)
        except Exception as e:
            logger.error(f"Error getting 2-day historical data: {e}")
            return {}
    
    @staticmethod
    def batchUpdateAllIndicators(tokens: List[Dict[str, Any]], historical_data: Dict[str, Dict], trading_action):
        """
        BATCH UPDATE: VWAP and EMA indicators for ALL tokens using pre-loaded historical data
        This eliminates individual DB queries per token for historical data
        """
        try:
            logger.info(f"BATCH UPDATING indicators for {len(tokens)} tokens")
            
            for token in tokens:
                token_address = token['tokenaddress']
                pair_address = token['pairaddress']
                pair_created_time = token['paircreatedtime']
                symbol = token['symbol']
                
                # Get pre-loaded historical data for this token
                token_historical = historical_data.get(token_address, {})
                
                if not token_historical:
                    logger.debug(f"No historical data available for {symbol}")
                    continue
                
                try:
                    # Update VWAP using pre-loaded data
                    vwap_result = trading_action._processVWAPWithPreloadedCandles(
                        token_address, pair_address, pair_created_time, token_historical
                    )
                    
                    if not vwap_result['success']:
                        logger.warning(f"VWAP update failed for {symbol}: {vwap_result.get('error')}")
                    
                    # Update EMA using pre-loaded data
                    ema_result = trading_action._processEMAWithPreloadedCandles(
                        token_address, pair_address, pair_created_time, token_historical
                    )
                    
                    if not ema_result['success']:
                        logger.warning(f"EMA update failed for {symbol}: {ema_result.get('error')}")
                    
                    logger.debug(f"Indicators updated for {symbol}")
                    
                except Exception as token_error:
                    logger.error(f"Error updating indicators for {symbol}: {token_error}")
                    continue
            
            logger.info("BATCH INDICATOR UPDATE completed for all tokens")
            
        except Exception as e:
            logger.error(f"Error in batch indicator update: {e}")