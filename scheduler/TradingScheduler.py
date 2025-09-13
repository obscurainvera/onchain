
from config.Config import get_config
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler
from database.trading.TradingModels import SchedulerConfig
from scheduler.SchedulerUtil import SchedulerUtil
from scheduler.SchedulerConstants import FetchResultKeys, CandleDataKeys
from constants.TradingConstants import TimeframeConstants
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
            logger.info("Starting scheduled trading data updates - NEW MULTI-TIMEFRAME FLOW")
            startTime = time.time()
            
            # NEW FLOW: Get all timeframe records ready for fetching (30min, 1h, 4h)
            timeframeRecords = self.getAllTimeframeRecordsForCandleFetchingFromAPI()
            
            if not timeframeRecords:
                logger.info("No timeframe records ready for update")
                return True
            
            logger.info(f"Processing {len(timeframeRecords)} timeframe records for updates")
            
            # Process using new Moralis-based flow with batch operations
            successCount, errorCount = self.fetchAllCandlesAndUpdateIndicators(timeframeRecords)
            
            elapsedTime = time.time() - startTime
            logger.info(f"NEW FLOW: Trading updates completed: {successCount} successful, {errorCount} errors in {elapsedTime:.2f}s")
            
            return successCount > 0
            
        except Exception as e:
            logger.error(f"Critical error in trading updates: {e}")
            return False

    def getAllTimeframeRecordsForCandleFetchingFromAPI(self) -> List[Dict[str, Any]]:
        """
        NEW SCHEDULER FLOW: Get all timeframe records (not just 15m) ready for data fetching
        
        This replaces the old 15m-only approach. Now we get ALL timeframes that need updates:
        - 30min, 1h, 4h timeframes
        - Uses 5-minute buffer for newly created tokens
        - Returns timeframe-level records instead of token-level records
        
        Returns:
            List of timeframe records ready for update
        """
        try:
            # Get all timeframe records ready for fetching with buffer
            timeframeRecordsReadyForFetching = self.trading_handler.getAllTimeframeRecordsReadyForFetching(
                buffer_seconds=self.config.NEW_TOKEN_BUFFER_SECONDS
            )
            
            logger.info(f"Found {len(timeframeRecordsReadyForFetching)} timeframe records ready for update (with buffer applied)")
            return timeframeRecordsReadyForFetching
                
        except Exception as e:
            logger.error(f"Error getting timeframe records ready for update: {e}")
            return []

    def fetchAllCandlesAndUpdateIndicators(self, timeframeRecords: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        NEW SCHEDULER FLOW: Process multi-timeframe records using Moralis API
        
        Production-ready implementation with:
        - Modular design with focused helper methods
        - Comprehensive error handling and logging
        - Batch operations for optimal performance
        - Clear separation of concerns
        
        Args:
            timeframe_records: List of timeframe records ready for processing
            
        Returns:
            tuple: (success_count, error_count)
        """
        if not timeframeRecords:
            logger.info("No timeframe records provided for processing")
            return 0, 0

        try:
            logger.info(f"NEW FLOW: Starting batch processing of {len(timeframeRecords)} timeframe records")
            startTime = time.time()
            
            # STEP 1: Batch fetch candle data from Moralis API
            allCandlesFromAPI = self.batchFetchCandles(timeframeRecords)
            
            # STEP 2: Batch persist all fetched data
            persistedCandles = self.batchPersistCandles(allCandlesFromAPI['candleData'])
            
            # STEP 3: Process indicators
            calculatedIndicators = self.calculateAndUpdateIndicators()
            
            # STEP 4: Log final results
            return self.log(allCandlesFromAPI, persistedCandles, calculatedIndicators, len(timeframeRecords), startTime)
            
        except Exception as e:
            logger.error(f"Critical error in new scheduler flow: {e}", exc_info=True)
            return 0, len(timeframeRecords)

    def batchFetchCandles(self, timeframeRecords: List[Dict]) -> Dict:
        """
        Batch fetch candle data from Moralis API for all timeframe records
        
        Returns:
            Dict containing candleData, successfulRecords, and totalCreditsUsed
        """
        allCandleData = {}
        successfulRecords = []
        totalCreditsUsed = 0
        currentTime = int(time.time())
        
        logger.info("Phase 1: Batch fetching candle data from Moralis API")
        
        for i, record in enumerate(timeframeRecords, 1):
            try:
                # Calculate appropriate fromTime
                fromTime = self.calculateFromTime(record)
                
                # Log progress for large batches
                if i % 10 == 0 or i == len(timeframeRecords):
                    logger.info(f"Processing record {i}/{len(timeframeRecords)}: {record['symbol']} {record['timeframe']}")
                
                # Fetch candle data
                candlesFromAPI = self.fetchCandleFromAPI(record, fromTime, currentTime)
                
                if candlesFromAPI['success']:
                    # Process successful fetch
                    key = f"{record['tokenaddress']}_{record['timeframe']}"
                    allCandleData[key] = self.formatCandlesFromAPIForBatchPersistence(candlesFromAPI)
                    successfulRecords.append(record)
                    totalCreditsUsed += candlesFromAPI.get('creditsUsed', 0)
                    
                    logger.debug(f"✓ Fetched {candlesFromAPI[CandleDataKeys.COUNT]} candles for {record['symbol']} {record['timeframe']}")
                else:
                    logger.warning(f"✗ Failed to fetch {record['timeframe']} data for {record['symbol']}: {candlesFromAPI.get('error', 'Unknown error')}")
                    
            except Exception as e:
                logger.error(f"✗ Error processing record {record['tokenaddress']}_{record['timeframe']}: {e}")
                continue
        
        logger.info(f"Phase 1 completed: {len(successfulRecords)}/{len(timeframeRecords)} successful fetches, {totalCreditsUsed} credits used")
        
        return {
            'candleData': allCandleData,
            'successfulRecords': successfulRecords,
            'totalCreditsUsed': totalCreditsUsed
        }

    def calculateFromTime(self, record: Dict) -> int:
        """
        Calculate appropriate fromTime based on lastfetchedat status
        
        Business Logic: 
        - If lastfetchedat IS NULL: fromTime = paircreatedtime - (timeframe_seconds * 2)
        - If lastfetchedat IS NOT NULL: fromTime = lastfetchedat timestamp
        """
        if record['lastfetchedat'] is None:
            # New record: use buffer approach like token addition flow
            timeframeInSeconds = TimeframeConstants.getSeconds(record['timeframe'])
            fromTime = record['paircreatedtime'] - (timeframeInSeconds * 2)
            logger.debug(f"New record {record['tokenaddress']}_{record['timeframe']}: fromTime = paircreatedtime - buffer ({fromTime})")
        else:
            # Existing record: continue from last successful fetch
            fromTime = record['lastfetchedat']  # lastfetchedat is already a Unix timestamp (int)
            logger.debug(f"Existing record {record['tokenaddress']}_{record['timeframe']}: fromTime = lastfetchedat ({fromTime})")
        
        return fromTime

    def fetchCandleFromAPI(self, record: Dict, fromTime: int, currentTime: int) -> Dict:
        """
        Fetch candle data for a single timeframe record using Moralis API
        
        Returns:
            Dict with success status, candles, and metadata
        """
        try:
            return self.trading_action.moralis_handler.getCandleDataForToken(
                tokenAddress=record['tokenaddress'],
                pairAddress=record['pairaddress'],
                fromTime=fromTime,
                toTime=currentTime,
                timeframe=record['timeframe'],
                symbol=record['symbol']
            )
        except Exception as e:
            logger.error(f"Moralis API error for {record['symbol']} {record['timeframe']}: {e}")
            return {'success': False, 'error': str(e)}

    def formatCandlesFromAPIForBatchPersistence(self, candleResult: Dict) -> Dict:
        """Format candle result for batch persistence"""
        return {
            CandleDataKeys.CANDLES: candleResult[CandleDataKeys.CANDLES],
            CandleDataKeys.LATEST_TIME: candleResult[CandleDataKeys.LATEST_TIME],
            CandleDataKeys.COUNT: candleResult[CandleDataKeys.COUNT]
        }

    def batchPersistCandles(self, allCandleData: Dict) -> Dict:
        """
        Batch persist all candle data in single database transaction
        
        Returns:
            Dict with persistence results
        """
        logger.info("Phase 2: Batch persisting candle data")
        
        if not allCandleData:
            logger.warning("No candle data to persist")
            return {'candlesInserted': 0, 'success': False}
        
        try:
            candlesInserted = self.trading_handler.batchPersistAllCandles(allCandleData)
            logger.info(f"✓ Phase 2 completed: Batch persisted {candlesInserted} candles across {len(allCandleData)} timeframes")
            
            return {'candlesInserted': candlesInserted, 'success': True}
            
        except Exception as e:
            logger.error(f"✗ Phase 2 failed: Error in batch persistence: {e}")
            return {'candlesInserted': 0, 'success': False, 'error': str(e)}

    def calculateAndUpdateIndicators(self) -> Dict:
        """
        Process VWAP and EMA indicators for tokens with successful data fetches
        
        Uses existing SchedulerUtil batch methods for optimal performance
        
        Returns:
            Dict with indicator processing results
        """
        
        logger.info("Phase 3: Processing indicators for successful tokens")
        
        try:
            # Batch VWAP processing with new optimized flow (processes ALL active tokens)
            vwapSuccess = SchedulerUtil.batchUpdateVWAPForScheduler(self.trading_handler)
            if vwapSuccess:
                logger.info("✓ VWAP batch processing completed successfully")
            else:
                logger.warning("✗ VWAP batch processing encountered errors")
            
            # NEW EMA processing with optimized scheduler flow (no token filtering needed)
            emaSuccess = SchedulerUtil.batchUpdateEMAForScheduler(self.trading_handler)
            if emaSuccess:
                logger.info("✓ NEW EMA batch processing completed successfully")
            else:
                logger.warning("✗ NEW EMA batch processing encountered errors")
            
            logger.info(f"✓ Phase 3 completed: Processed VWAP and EMA for all active tokens")
            
            return {
                'vwapSuccess': vwapSuccess,
                'emaSuccess': emaSuccess,
                'tokensProcessed': 'all_active_tokens'
            }
            
        except Exception as e:
            logger.error(f"✗ Phase 3 failed: Error in indicator processing: {e}")
            return {'vwapSuccess': False, 'emaSuccess': False, 'tokensProcessed': 0, 'error': str(e)}

   

    def log(self, fetchResults: Dict, persistResults: Dict, 
                            indicatorResults: Dict, totalRecords: int, startTime: float) -> tuple[int, int]:
        """
        Log comprehensive results and return success/error counts
        
        Returns:
            tuple: (success_count, error_count)
        """
        timeTaken = time.time() - startTime
        successCount = len(fetchResults['successfulRecords'])
        errorCount = totalRecords - successCount
        
        # Comprehensive logging
        logger.info("=" * 80)
        logger.info("NEW SCHEDULER FLOW - FINAL RESULTS")
        logger.info("=" * 80)
        logger.info(f"- Processing Summary:")
        logger.info(f"   • Total timeframe records processed: {totalRecords}")
        logger.info(f"   • Successful API fetches: {successCount}")
        logger.info(f"   • Failed API fetches: {errorCount}")
        logger.info(f"   • Total credits used: {fetchResults['totalCreditsUsed']}")
        logger.info(f"   • Processing time: {timeTaken:.2f}s")
        logger.info(f"")
        logger.info(f"- Data Persistence:")
        logger.info(f"   • Candles inserted: {persistResults['candlesInserted']}")
        logger.info(f"   • Persistence success: {persistResults['success']}")
        logger.info(f"")
        logger.info(f"- Indicator Processing:")
        logger.info(f"   • Unique tokens processed: {indicatorResults['tokensProcessed']}")
        logger.info(f"   • VWAP processing success: {indicatorResults['vwapSuccess']}")
        logger.info(f"   • EMA processing success: {indicatorResults['emaSuccess']}")
        logger.info("=" * 80)
        
        return successCount, errorCount


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