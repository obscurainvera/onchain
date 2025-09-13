
from database.trading.TradingHandler import TradingHandler

from logs.logger import get_logger
from scheduler.VWAPProcessor import VWAPProcessor
from scheduler.EMAProcessor import EMAProcessor

logger = get_logger(__name__)


class SchedulerUtil:

    @staticmethod
    def batchUpdateEMAForScheduler(trading_handler: TradingHandler) -> bool:
        """
        NEW OPTIMIZED EMA PROCESSOR: Removes dependency on successful tokens list
        
        This method implements the new EMA scheduler flow that:
        1. Uses single optimized query with JOINs to get all necessary data
        2. Processes ALL active tokens automatically (no token filtering needed)
        3. Handles all three cases: available, not_available_ready, not_available_insufficient
        4. True batch processing for optimal performance
        5. Resilient to server downtime - no gaps in EMA calculations
        
        Args:
            trading_handler: Database handler for operations
            
        Returns:
            bool: True if all EMA processing succeeded
        """
        try:
            logger.info("Starting NEW EMA scheduler processing for all active tokens")
            
            # Use the new optimized EMA processor (no token filtering)
            success = EMAProcessor(trading_handler).processEMAForScheduler()
            return success
            
        except Exception as e:
            logger.error(f"Error in NEW EMA scheduler processing: {e}", exc_info=True)
            return False
    
    
    @staticmethod
    def batchUpdateVWAPForScheduler(trading_handler: TradingHandler) -> bool:
        """
        NEW SCHEDULER FLOW: Optimized VWAP processing with single-query approach.
        
        This method implements the new VWAP scheduler flow that solves the previous issues:
        1. Single optimized query with JOINs to get all necessary data
        2. Handles both incremental updates (Case 1) and full resets (Case 2)
        3. Automatic day boundary detection for VWAP resets
        4. True batch processing for optimal performance
        5. Resilient to server downtime - no gaps in VWAP calculations
        
        Args:
            trading_handler: Database handler for operations
            
        Returns:
            bool: True if all VWAP processing succeeded
        """
        try:
            logger.info("Starting NEW VWAP scheduler processing for all active tokens")
            
            # Use the new optimized VWAP processor (no token filtering)
            success = VWAPProcessor(trading_handler).processVWAPForScheduler()
            return success
            
        except Exception as e:
            logger.error(f"Error in NEW VWAP scheduler processing: {e}", exc_info=True)
            return False
    
    
    