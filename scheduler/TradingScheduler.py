"""
TradingScheduler - Main entry point for routine trading data updates

This scheduler runs every 5 minutes to:
1. Fetch latest 15-minute candle data from BirdEye API
2. Perform smart aggregation to 1hr and 4hr timeframes
3. Update VWAP indicators with session management
4. Update EMA indicators with availability time checking
5. Process all tokens sequentially for API rate limiting compliance

Features:
- UTC-based time calculations
- API rate limiting with 2-second delays
- Duplicate candle prevention
- 5-minute buffer for new token additions
- Comprehensive logging and error handling

All utility functions have been moved to SchedulerUtil.py for better modularity.
"""

from config.Config import get_config
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler
from database.trading.TradingModels import SchedulerConfig
from scheduler.SchedulerUtil import SchedulerUtil
from scheduler.SchedulerConstants import FetchResultKeys
from logs.logger import get_logger
from typing import List, Dict, Any
import time
from actions.TradingActionEnhanced import TradingActionEnhanced 

logger = get_logger(__name__)


class TradingScheduler:
    """
    Main Trading Scheduler class - handles job entry point and coordination
    
    All batch processing, API calls, database operations, and utility functions
    have been moved to SchedulerUtil.py for better separation of concerns.
    """
    
    def __init__(self, dbPath: str = None):
        """
        Initialize trading scheduler with database instance and utilities
        
        Args:
            dbPath: Path to database (DEPRECATED - uses PortfolioDB config)
        """
        self.db = PortfolioDB()
        self.trading_handler = TradingHandler(self.db)
        self.trading_action = TradingActionEnhanced(self.db)
        
        # Use configuration constants from TradingModels
        self.config = SchedulerConfig()
        self.current_time = int(time.time())
        
        logger.info("Trading scheduler initialized with static SchedulerUtil methods")

    def handleTradingUpdatesFromJob(self):
        """
        Main entry point for scheduled trading updates
        
        This method:
        1. Finds tokens ready for update
        2. Processes them using SchedulerUtil batch processing
        3. Returns success status
        """
        try:
            logger.info("Starting scheduled trading data updates")
            startTime = time.time()
            
            # Get all tokens ready for 15m candle updates -- we only get 15m from the API
            tokensToFetch15mCandles = self.getTokensToFetch15MinCandles()
            
            if not tokensToFetch15mCandles:
                logger.info("No tokens ready for update")
                return True
            
            logger.info(f"Processing {len(tokensToFetch15mCandles)} tokens for updates")
            
            # Process tokens using SchedulerUtil batch processing
            successCount, errorCount = self.fetchCandlesAndUpdateIndicators(tokensToFetch15mCandles)
            
            elapsedTime = time.time() - startTime
            logger.info(f"Trading updates completed: {successCount} successful, {errorCount} errors in {elapsedTime:.2f}s")
            
            return successCount > 0
            
        except Exception as e:
            logger.error(f"Critical error in trading updates: {e}")
            return False

    def getTokensToFetch15MinCandles(self) -> List[Dict[str, Any]]:
        """
        Get all tokens that need 15-minute candle updates using optimized TradingHandler method
        
        Uses the new getTokensDueForFetchWithBuffer method that applies the 5-minute buffer
        directly in the SQL query instead of filtering in Python - much more efficient!
        
        Returns:
            List of token records ready for update
        """
        try:
            # Use optimized TradingHandler method with buffer built into the query
            tokens = self.trading_handler.getTokensToFetch15MinCandlesWithBuffer(
                buffer_seconds=self.config.NEW_TOKEN_BUFFER_SECONDS
            )
            
            logger.info(f"Found {len(tokens)} tokens ready for update (with buffer applied in query)")
            return tokens
                
        except Exception as e:
            logger.error(f"Error getting tokens ready for update: {e}")
            return []

    def fetchCandlesAndUpdateIndicators(self, tokens: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        Process tokens sequentially using SchedulerUtil batch processing
        
        Step-by-step process:
        1. Input validation - Check if tokens list is empty
        2. Batch API calls - Fetch 15-minute candle data from BirdEye API for all tokens with retry mechanism
        3. Filter successful tokens - Identify tokens that had successful API responses
        4. Batch aggregation - Aggregate candle data to 1hr and 4hr timeframes for successful tokens
        5. Batch indicators update - Update VWAP and EMA indicators with memory management
        6. Logging and metrics - Record processing statistics and performance metrics
        
        Args:
            tokens: List of token records to process
            
        Returns:
            tuple: (success_count, error_count)
        """
        # Input validation
        if not tokens:
            logger.info("No tokens provided for processing")
            return 0, 0

        try:
            logger.info(f"BATCH PROCESSING {len(tokens)} tokens")
            startTime = time.time()
            
            # STEP 1: Batch API calls 
            candle15mFetchedFromAPI = SchedulerUtil.batchFetch15mCandlesForAllTokens(tokens, self.config, self.trading_handler, self.trading_action)
            successCount = candle15mFetchedFromAPI[FetchResultKeys.SUCCESSFUL_TOKENS]
            failedCount = candle15mFetchedFromAPI[FetchResultKeys.FAILED_TOKENS]
            successfulTokensList = candle15mFetchedFromAPI[FetchResultKeys.SUCCESSFUL_TOKENS_LIST]
            
            logger.info(f"API phase completed: {successCount} successful, {failedCount} failed out of {len(tokens)} tokens")
            
            # STEP 2: Batch aggregation with validation
            SchedulerUtil.batchAggregate15MInto1HrAnd4Hr(tokens, self.trading_handler)
        
            
            # STEP 3: Batch VWAP processing with sophisticated session management
            isVwapCompleted = SchedulerUtil.batchUpdateVWAP(successfulTokensList, self.trading_handler)
            if not isVwapCompleted:
                logger.warning("Batch VWAP processing encountered errors")
            
            # STEP 4: Batch EMA processing with sophisticated state management
            isEMACompleted = SchedulerUtil.batchUpdateEMA(successfulTokensList, self.trading_handler)
            if not isEMACompleted:
                logger.warning("Batch EMA processing encountered errors")
            
            errorCount = len(tokens) - successCount
            timeTaken = time.time() - startTime
            
            logger.info(f"BATCH PROCESSING completed in {timeTaken:.2f}s: {successCount} successful, {errorCount} errors")
            
            return successCount, errorCount
            
        except Exception as e:
            logger.error(f"Critical error in batch processing: {e}", exc_info=True)
            return 0, len(tokens)

    def handleTradingDataFromAPI(self) -> Dict[str, Any]:
        """
        Legacy method - calls new handleTradingUpdatesFromJob method
        
        Returns:
            Dict: Processing results summary
        """
        success = self.handleTradingUpdatesFromJob()
        return {
            'success': success,
            'message': 'Trading updates processed' if success else 'Trading updates failed'
        }