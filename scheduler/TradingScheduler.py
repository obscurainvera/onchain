
from constants.TradingSchedulerConstants import TradingSchedulerConstants
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler
from scheduler.SchedulerUtil import SchedulerUtil
from scheduler.SchedulerConstants import CandleDataKeys
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
        self.current_time = int(time.time())
        
        logger.info("Trading scheduler initialized with static SchedulerUtil methods")

    def handleTradingUpdatesFromJob(self):
        try:
            logger.info("Starting trading scheduler to fetch new candles and update indicators")
            self.fetchAllNewCandles()
            self.calculateAndUpdateIndicators()
            logger.info("Trading scheduler completed fetching new candles and updating indicators")
            return True
            
        except Exception as e:
            logger.error(f"Critical error in trading updates: {e}")
            return False

    
    def fetchAllNewCandles(self):
        try:
            # NEW FLOW: Get all timeframe records ready for fetching (30min, 1h, 4h)
            timeframeRecords = self.getAllTimeframeRecordsForCandleFetchingFromAPI()
            if timeframeRecords:
                allCandlesFromAPI = self.batchFetchCandles(timeframeRecords)
                self.batchPersistCandles(allCandlesFromAPI[TradingSchedulerConstants.CandleFetching.CANDLE_DATA])
            
        except Exception as e:
            logger.error(f"Critical error in new scheduler flow: {e}", exc_info=True)


    def calculateAndUpdateIndicators(self):
        
        try:
            vwapSuccess = SchedulerUtil.batchUpdateVWAPForScheduler(self.trading_handler)
            if vwapSuccess:
                logger.info("✓ VWAP batch processing completed successfully")
            else:
                logger.warning("✗ VWAP batch processing encountered errors")
            
           
            emaSuccess = SchedulerUtil.batchUpdateEMAForScheduler(self.trading_handler)
            if emaSuccess:
                logger.info("✓ NEW EMA batch processing completed successfully")
            else:
                logger.warning("✗ NEW EMA batch processing encountered errors")
        except Exception as e:
            logger.error(f"✗ Phase 3 failed: Error in indicator processing: {e}")

    

    def getAllTimeframeRecordsForCandleFetchingFromAPI(self) -> List[Dict[str, Any]]:
        try:
            timeframeRecordsReadyForFetching = self.trading_handler.getAllTimeframeRecordsReadyForFetching(
                buffer_seconds=1000
            )
            
            logger.info(f"Found {len(timeframeRecordsReadyForFetching)} timeframe records ready for update (with buffer applied)")
            return timeframeRecordsReadyForFetching
                
        except Exception as e:
            logger.error(f"Error getting timeframe records ready for update: {e}")
            return []

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
            TradingSchedulerConstants.CandleFetching.CANDLE_DATA: allCandleData,
            TradingSchedulerConstants.CandleFetching.SUCCESSFUL_RECORDS: successfulRecords,
            TradingSchedulerConstants.CandleFetching.TOTAL_CREDITS_USED: totalCreditsUsed
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