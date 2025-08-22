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
from scheduler.SchedulerConstants import FetchResultKeys, CandleDataKeys, TokenKeys, CandleKeys, Timeframes, DataSources
from scheduler.VWAPProcessor import VWAPProcessor
from scheduler.EMAProcessor import EMAProcessor

logger = get_logger(__name__)


class SchedulerUtil:

    @staticmethod
    def batchUpdateEMA(successfulTokensList: List[Dict[str, Any]], trading_handler: TradingHandler) -> bool:
        """
        PRODUCTION-READY: Batch update EMA calculations for all successful tokens
        
        Implements sophisticated EMA state management:
        - Checks EMA status (NOT_AVAILABLE vs AVAILABLE)
        - Compares lastfetchedat vs emaavailabletime for readiness
        - Uses incremental calculation for existing EMAs
        - Updates both ohlcvdetails and emastates atomically
        
        Args:
            successfulTokensList: List of tokens that had successful candle fetches
            trading_handler: Database handler for operations
            
        Returns:
            bool: True if all EMA processing succeeded
        """
        try:
            if not successfulTokensList:
                return True
            
            logger.info(f"Starting EMA processing for {len(successfulTokensList)} tokens")
            
            emaProcessor = EMAProcessor(trading_handler)
            success = emaProcessor.processEMAForAllTokens(successfulTokensList)
            
            if success:
                logger.info(f"EMA processing completed successfully for all {len(successfulTokensList)} tokens")
            else:
                logger.warning("EMA processing encountered errors for some tokens")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in batch EMA processing: {e}", exc_info=True)
            return False
    
    
    @staticmethod
    def batchUpdateVWAP(successfulTokensList: List[Dict[str, Any]], trading_handler: TradingHandler) -> bool:
        """
        PRODUCTION-READY: Batch update VWAP calculations for all successful tokens
        
        Implements sophisticated VWAP session management:
        - Detects same day vs new day scenarios
        - Uses cumulative calculations for efficiency  
        - Handles session resets and boundary conditions
        - Updates both ohlcvdetails and vwapsessions atomically
        
        Args:
            successfulTokensList: List of tokens that had successful candle fetches
            trading_handler: Database handler for operations
            
        Returns:
            bool: True if all VWAP processing succeeded
        """
        try:
            if not successfulTokensList:
                return True
            
            logger.info(f"Starting VWAP processing for {len(successfulTokensList)} tokens")
            
            success = VWAPProcessor(trading_handler).processVWAPForAllTokens(successfulTokensList)
            
            if success:
                logger.info(f"VWAP processing completed successfully for all {len(successfulTokensList)} tokens")
            else:
                logger.warning("VWAP processing encountered errors for some tokens")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in batch VWAP processing: {e}", exc_info=True)
            return False
    
    
    @staticmethod
    def batchFetch15mCandlesForAllTokens(tokens: List[Dict[str, Any]], config: SchedulerConfig, trading_handler: TradingHandler, trading_action) -> Dict[str, Dict]:
        """
        OPTIMIZED: Batch fetch candles from API for ALL tokens, collect data, then batch persist at once
        API failures are now handled gracefully by returning failure results for affected tokens
        rather than retrying, which can cause delays and potential rate limiting issues.
        """
        sucessfulTokenList = []
        failedTokens = 0
        candleData = {}  # Unified map: token_address -> {candles, latest_time, count}
        
        # STEP 1: Fetch from API for all tokens (collect data only)
        for token in tokens:
            try:
                tokenAddress = token['tokenaddress']
                pairAddress = token['pairaddress']
                symbol = token['symbol']
                
                # Get last fetch time from timeframemetadata.lastfetchedat
                lastFetchedAtTime = None
                if 'lastfetchedat' in token and token['lastfetchedat']:
                    if hasattr(token['lastfetchedat'], 'timestamp'):
                        lastFetchedAtTime = int(token['lastfetchedat'].timestamp())
                    else:
                        lastFetchedAtTime = token['lastfetchedat']
                
                # If no lastfetchedat, start from lastfetchedat + 1 or pair creation time
                if not lastFetchedAtTime:
                    lastFetchedAtTime = token.get('paircreatedtime', int(time.time()) - 86400)
                else:
                    # Start from lastfetchedat + 1 second to avoid duplicates
                    lastFetchedAtTime = lastFetchedAtTime + 1
                
                # Fetch candles from API using BirdEyeServiceHandler
                birdeyeService = BirdEyeServiceHandler(trading_action.db)

                candleDataFromAPI = birdeyeService.getCandleDataForToken(
                    tokenAddress, pairAddress, lastFetchedAtTime, int(time.time()), symbol
                )
                
                if candleDataFromAPI['success'] and candleDataFromAPI[CandleDataKeys.CANDLES]:
                    # OPTIMIZED: Store everything in unified map structure using constants
                    candleData[tokenAddress] = {
                        CandleDataKeys.CANDLES: candleDataFromAPI[CandleDataKeys.CANDLES],
                        CandleDataKeys.LATEST_TIME: candleDataFromAPI[CandleDataKeys.LATEST_TIME],
                        CandleDataKeys.COUNT: candleDataFromAPI[CandleDataKeys.COUNT]
                    }
                    sucessfulTokenList.append(token)
                    logger.info(f"API fetched {len(candleDataFromAPI[CandleDataKeys.CANDLES])} candles for {symbol}")
                else:
                    failedTokens += 1
                    logger.warning(f"Failed to fetch candles for {symbol}: {candleDataFromAPI.get('error', 'Unknown error')}")
                    
            except Exception as e:
                failedTokens += 1
                logger.error(f"Error fetching candles for {token['symbol']}: {e}")
        
        # STEP 2: Batch persist ALL candles at once using unified map structure
        if candleData:
            successCount = trading_handler.batchPersistAllCandles(candleData)
            logger.info(f"Batch persisted {successCount} total candles from {len(candleData)} tokens")
        
        sucessfulTokens = len(sucessfulTokenList)
        logger.info(f"API fetch completed: {sucessfulTokens} successful, {failedTokens} failed out of {len(tokens)} tokens")
        
        return {
            FetchResultKeys.SUCCESSFUL_TOKENS: sucessfulTokens, 
            FetchResultKeys.FAILED_TOKENS: failedTokens,
            FetchResultKeys.SUCCESSFUL_TOKENS_LIST: sucessfulTokenList
        }
    
    
    @staticmethod
    def batchAggregate15MInto1HrAnd4Hr(tokens: List[Dict[str, Any]], trading_handler: TradingHandler):
        """
        OPTIMIZED: Batch aggregate ALL tokens with single DB query approach
        Gets all 15m candles needing aggregation and processes both 1h and 4h together
        """
        try:
            logger.info(f"BATCH AGGREGATING for {len(tokens)} tokens")
            
            # Get all token addresses
            tokenAddresses = [t['tokenaddress'] for t in tokens]
            
            # SINGLE QUERY: Get all 15m candles needing aggregation for ALL tokens
            candleData = trading_handler.getCandlesForAggregationFromScheduler(tokenAddresses)
            
            if not candleData:
                logger.info("No candles found for aggregation")
                return
            
            # Process aggregation efficiently - calculate 1h and 4h together
            aggregatedCandles = SchedulerUtil.batchCreateAggregatedCandles(candleData)
            
            # ATOMIC: Insert aggregated candles and update fetch times in single transaction
            if aggregatedCandles:
                successCount = trading_handler.batchInsertAggregatedCandlesAndUpdateFetchTimes(
                    aggregatedCandles, ['1h', '4h']
                )
                logger.info(f"BATCH AGGREGATION completed: inserted {successCount} aggregated candles for {len(tokens)} tokens")
            else:
                logger.info("No aggregated candles to insert")
            
        except Exception as e:
            logger.error(f"Error in batch aggregation: {e}")
    
    @staticmethod
    def batchCreateAggregatedCandles(candleData: Dict[str, Dict]) -> List[Dict]:
        """
        Clean aggregation logic following the clear approach:
        
        1. Get all 15m candles using MIN(last_1h_fetch, last_4h_fetch)
        2. Aggregate 15m -> 1h for all candles
        3. Aggregate 1h -> 4h from the 1h candles
        4. Persist only candles where unixtime > corresponding last_fetch_time for corresponding timeframe
        """
        try:
            aggregatedCandles = []
            
            for tokenAddress, tokenData in candleData.items():
                candles15m = tokenData['candles_15m']
                if not candles15m:
                    continue
                
                pairAddress = tokenData['pairaddress']
                lastfetchedat_1h = tokenData['lastfetchedat_1h']
                lastfetchedat_4h = tokenData['lastfetchedat_4h']
                nextfetchat1h = tokenData['nextfetchat_1h'] 
                nextfetchat4h = tokenData['nextfetchat_4h']
                
                logger.debug(f"Processing {tokenAddress}: {len(candles15m)} 15m candles, "
                           f"nextfetchat_1h={nextfetchat1h}, nextfetchat_4h={nextfetchat4h}")
                
                # Step 1: Aggregate all 15m candles to 1h
                hourlyAggregatedCandles = TradingActionUtil.aggregateToHourlyInMemory(candles15m).get('candles', [])
                
                # Step 2: Filter and persist only 1h candles >= nextfetchat_1h
                hourlyAggregatedCandlesThatNeedToBePersisted = []
                for candle in hourlyAggregatedCandles:
                    if candle['unixtime'] >= nextfetchat1h:
                        hourlyAggregatedCandlesThatNeedToBePersisted.append(candle)
                        aggregatedCandles.append({
                            'tokenaddress': tokenAddress,
                            'pairaddress': pairAddress,
                            'timeframe': '1h',
                            'unixtime': candle['unixtime'],
                            'openprice': candle['openprice'],
                            'highprice': candle['highprice'],
                            'lowprice': candle['lowprice'],
                            'closeprice': candle['closeprice'],
                            'volume': candle['volume']
                        })
                
                # Step 3: Aggregate all 1h candles to 4h
                fourHourlyAggregatedCandlesThatNeedToBePersisted = []  # Initialize outside the if block
                if hourlyAggregatedCandles:
                    fourHourlyAggregatedCandles = TradingActionUtil.aggregateToFourHourlyInMemory(hourlyAggregatedCandles).get('candles', [])
                    # Step 4: Filter and persist only 4h candles >= nextfetchat_4h
                    for candle in fourHourlyAggregatedCandles:
                        if candle['unixtime'] >= nextfetchat4h:
                            fourHourlyAggregatedCandlesThatNeedToBePersisted.append(candle)
                            aggregatedCandles.append({
                                'tokenaddress': tokenAddress,
                                'pairaddress': pairAddress,
                                'timeframe': '4h',
                                'unixtime': candle['unixtime'],
                                'openprice': candle['openprice'],
                                'highprice': candle['highprice'],
                                'lowprice': candle['lowprice'],
                                'closeprice': candle['closeprice'],
                                'volume': candle['volume']
                            })
                
                logger.debug(f"Token {tokenAddress}: Will persist {len(hourlyAggregatedCandlesThatNeedToBePersisted)} 1h and {len(fourHourlyAggregatedCandlesThatNeedToBePersisted)} 4h candles")
            
            return aggregatedCandles
            
        except Exception as e:
            logger.error(f"Error in optimized aggregation processing: {e}")
            return []
    