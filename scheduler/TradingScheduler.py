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
            start_time = time.time()
            
            # Get all tokens ready for 15m candle updates -- we only get 15m from the API
            tokens_to_update = self._getTokensThatHasCandleInfoToBeRetrieved()
            
            if not tokens_to_update:
                logger.info("No tokens ready for update")
                return True
            
            logger.info(f"Processing {len(tokens_to_update)} tokens for updates")
            
            # Process tokens using SchedulerUtil batch processing
            success_count, error_count = self._processTokensSequentially(tokens_to_update)
            
            elapsed_time = time.time() - start_time
            logger.info(f"Trading updates completed: {success_count} successful, {error_count} errors in {elapsed_time:.2f}s")
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Critical error in trading updates: {e}")
            return False

    def _getTokensThatHasCandleInfoToBeRetrieved(self) -> List[Dict[str, Any]]:
        """
        Get all tokens that need 15-minute candle updates using optimized TradingHandler method
        
        Uses the new getTokensDueForFetchWithBuffer method that applies the 5-minute buffer
        directly in the SQL query instead of filtering in Python - much more efficient!
        
        Returns:
            List of token records ready for update
        """
        try:
            # Use optimized TradingHandler method with buffer built into the query
            tokens = self.trading_handler.getTokensDueForFetchWithBuffer(
                buffer_seconds=self.config.NEW_TOKEN_BUFFER_SECONDS
            )
            
            logger.info(f"Found {len(tokens)} tokens ready for update (with buffer applied in query)")
            return tokens
                
        except Exception as e:
            logger.error(f"Error getting tokens ready for update: {e}")
            return []

    def _processTokensSequentially(self, tokens: List[Dict[str, Any]]) -> tuple[int, int]:
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
            start_time = time.time()
            
            # STEP 1: Batch API calls 
            fetch_result = SchedulerUtil.batchFetch15mCandlesForAllTokens(tokens, self.config, self.trading_handler, self.trading_action)
            success_count = fetch_result['successful_tokens']
            failed_count = fetch_result['failed_tokens']
            successful_tokens = fetch_result['successful_tokens_list']
            
            logger.info(f"API phase completed: {success_count} successful, {failed_count} failed out of {len(tokens)} tokens")
            
            # STEP 2: Batch aggregation with validation
            if successful_tokens:
                aggregation_success = SchedulerUtil.batchAggregateTimeFrames(
                    successful_tokens, self.trading_handler
                )
                if not aggregation_success:
                    logger.warning("Batch aggregation encountered errors")
            
            # STEP 3: Batch historical data and indicators with memory management
            if successful_tokens:
                indicators_success = SchedulerUtil.batchUpdateIndicatorsWithMemoryManagement(
                    successful_tokens, self.trading_handler, self.trading_action
                )
                if not indicators_success:
                    logger.warning("Batch indicator update encountered errors")
            
            error_count = len(tokens) - success_count
            elapsed_time = time.time() - start_time
            
            logger.info(f"BATCH PROCESSING completed in {elapsed_time:.2f}s: {success_count} successful, {error_count} errors")
            
            # Production metrics logging using static SchedulerUtil
            SchedulerUtil.logProcessingMetrics(len(tokens), success_count, error_count, elapsed_time)
            
            return success_count, error_count
            
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