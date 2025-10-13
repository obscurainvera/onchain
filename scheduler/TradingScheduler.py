from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler
from logs.logger import get_logger
from typing import List, Dict, Any
import time
from actions.TradingActionEnhanced import TradingActionEnhanced
from api.trading.request import TrackedToken, OHLCVDetails
from scheduler.VWAPProcessor import VWAPProcessor
from scheduler.EMAProcessor import EMAProcessor
from scheduler.AVWAPProcessor import AVWAPProcessor
from scheduler.RSIProcessor import RSIProcessor
from scheduler.AlertsProcessor import AlertsProcessor
from utils.CommonUtil import CommonUtil

logger = get_logger(__name__)


class TradingScheduler:
    
    
    def __init__(self, dbPath: str = None):
        self.db = PortfolioDB()
        self.trading_handler = TradingHandler(self.db.conn_manager)
        self.trading_action = TradingActionEnhanced(self.db)
        self.vwap_processor = VWAPProcessor(self.trading_handler)
        self.ema_processor = EMAProcessor(self.trading_handler)
        self.avwap_processor = AVWAPProcessor(self.trading_handler, self.trading_action.moralis_handler)
        self.rsi_processor = RSIProcessor(self.trading_handler)
        self.alerts_processor = AlertsProcessor(self.trading_handler)
        self.current_time = int(time.time())
        logger.info("Trading scheduler initialized with POJO-based flow and alerts")

    def handleTradingUpdatesFromJob(self):
        try:
            logger.info("Trading scheduler started")
            
            self.fetchCandlesAndPersist()
            
            self.calculateAndPersistVWAPIndicators()
            
            self.calculateAndPersistEMAIndicators()
            
            self.calculateAndPersistAVWAPIndicators()
            
            self.calculateAndPersistRSIIndicators()
            
            self.calculateAndPersistAlerts() # we need to check whether running the alerts processing in a sequential order affect the time take to run this scheduler, if it goes over 10 mins, then there would a delay fetching the recent candles
            
            logger.info("Trading scheduler completed")
            return True
            
        except Exception as e:
            logger.error(f"Critical error in trading updates: {e}")
            return False

    def fetchCandlesAndPersist(self):
        try:
            trackedTokens = self.trading_handler.getAllTimeframeRecordsReadyForFetching(buffer_seconds=1000)
            if not trackedTokens:
                logger.info("No timeframe records ready for fetching")
                return
            
            self.fetchCandlesForTrackedTokens(trackedTokens)
            self.trading_handler.batchPersistTrackedTokensData(trackedTokens, maxCandlesPerTimeframe=None)
            
            logger.info("✓ Candles fetched and persisted")
            
        except Exception as e:
            logger.error(f"✗ Candle Fetching/Persistence Failed: {e}")
            # Don't raise - continue with indicator updates even if candle fetching fails
      

    def fetchCandlesForTrackedTokens(self, trackedTokens: List[TrackedToken]):
        logger.info("Fetching candles for tracked tokens")
        
        for trackedToken in trackedTokens:
            logger.info(f"Fetching candles for {trackedToken.symbol} ({len(trackedToken.timeframeRecords)} timeframes)")
            
            for timeframeRecord in trackedToken.timeframeRecords:
                if timeframeRecord.shouldFetchFromAPI(self.current_time):
                    logger.info(f"Fetching {timeframeRecord.timeframe} candles for {trackedToken.symbol}")
                    
                    try:
                        candleResponse = self.trading_action.moralis_handler.getCandleDataForToken(
                            tokenAddress=trackedToken.tokenAddress,
                            pairAddress=trackedToken.pairAddress,
                            fromTime=timeframeRecord.lastFetchedAt,
                            toTime=self.current_time,
                            timeframe=timeframeRecord.timeframe,
                            symbol=trackedToken.symbol
                        )
                        
                        if candleResponse.success:
                            for candle in candleResponse.candles:
                                ohlcvDetail = OHLCVDetails(
                                    tokenAddress=candle.tokenAddress,
                                    pairAddress=candle.pairAddress,
                                    timeframe=candle.timeframe,
                                    unixTime=candle.unixTime,
                                    timeBucket=CommonUtil.calculateInitialStartTime(candle.unixTime, candle.timeframe),
                                    openPrice=candle.openPrice,
                                    highPrice=candle.highPrice,
                                    lowPrice=candle.lowPrice,
                                    closePrice=candle.closePrice,
                                    volume=candle.volume,
                                    trades=int(candle.trades),
                                    isComplete=True,
                                    dataSource=candle.dataSource
                                )
                                timeframeRecord.addOHLCVDetail(ohlcvDetail)
                            
                            timeframeRecord.ohlcvDetails.sort(key=lambda x: x.unixTime)
                            
                            nextFetchTime = CommonUtil.calculateNextFetchTimeForTimeframe(candleResponse.latestTime, timeframeRecord.timeframe)
                            timeframeRecord.updateAfterFetch(candleResponse.latestTime, nextFetchTime)
                            
                            logger.info(f"✓ Fetched {len(timeframeRecord.ohlcvDetails)} {timeframeRecord.timeframe} candles for {trackedToken.symbol}")
                        else:
                            logger.warning(f"✗ Failed to fetch {timeframeRecord.timeframe} data for {trackedToken.symbol}: {candleResponse.error}")
                            
                    except Exception as e:
                        logger.error(f"✗ Error fetching {timeframeRecord.timeframe} data for {trackedToken.symbol}: {e}")
                else:
                    logger.info(f"Skipping {timeframeRecord.timeframe} fetch for {trackedToken.symbol} (nextFetchTime: {timeframeRecord.nextFetchAt} > currentTime: {self.current_time})")

    def calculateAndPersistVWAPIndicators(self):
        try:
            logger.info("VWAP Calculation Started")
            
            trackedTokens = self.trading_handler.getAllVWAPDataForScheduler()
            if not trackedTokens:
                logger.info("No VWAP data to process")
                return
            
            self.vwap_processor.calculateVWAPForAllTrackedTokens(trackedTokens)
            self.trading_handler.batchPersistVWAPData(trackedTokens)
            
            logger.info("✓ VWAP Calculation Completed")
            
        except Exception as e:
            logger.error(f"✗ VWAP Calculation Failed: {e}")

    def calculateAndPersistEMAIndicators(self):
        try:
            logger.info("EMA Calculation Started")
            
            trackedTokens = self.trading_handler.getAllEMADataWithCandlesForScheduler()
            if not trackedTokens:
                logger.info("No EMA data to process")
                return
            
            self.ema_processor.calculateEMAForAllRetrievedTokens(trackedTokens)
            self.trading_handler.batchPersistEMAData(trackedTokens)
            
            logger.info(f"✓ EMA Calculation Completed for {len(trackedTokens)} tokens")
            
        except Exception as e:
            logger.error(f"✗ EMA Calculation Failed: {e}")

    def calculateAndPersistAVWAPIndicators(self):
        
        try:
            logger.info("AVWAP Calculation Started")
            
            trackedTokens = self.trading_handler.getAllAVWAPDataForScheduler()
            
            if not trackedTokens:
                logger.info("No AVWAP data to process")
                return
            
            self.avwap_processor.calculateAVWAPForAllTrackedTokens(trackedTokens)
            self.trading_handler.batchPersistAVWAPData(trackedTokens)
            
            logger.info(f"✓ AVWAP Calculation Completed for {len(trackedTokens)} tokens")
            
        except Exception as e:
            logger.error(f"✗ AVWAP Calculation Failed: {e}")
    
    def calculateAndPersistRSIIndicators(self):
        try:
            logger.info("RSI Calculation Started")
            
            trackedTokens = self.trading_handler.getAllRSIDataForScheduler()
            
            if not trackedTokens:
                logger.info("No RSI data to process")
                return
            
            self.rsi_processor.calculateRSIForAllTrackedTokens(trackedTokens)
            self.trading_handler.batchPersistRSIData(trackedTokens)
            
            logger.info(f"✓ RSI Calculation Completed for {len(trackedTokens)} tokens")
            
        except Exception as e:
            logger.error(f"✗ RSI Calculation Failed: {e}")
    
    def calculateAndPersistAlerts(self):
        try:
            logger.info("Alert Processing Started")
            
            alertsWithNewCandles = self.trading_handler.getCurrentAlertStateAndNewCandles()
            if not alertsWithNewCandles:
                logger.info("No alerts to process")
                return
            
            # Process alerts for all tracked tokens
            self.alerts_processor.processAlertsFromScheduler(alertsWithNewCandles)
            
            # Persist updated alert data
            alertsUpdated = self.trading_handler.batchPersistAlerts(alertsWithNewCandles)
            
            logger.info(f"✓ Alert Processing Completed for {alertsUpdated} alerts")
            
        except Exception as e:
            logger.error(f"✗ Alert Processing Failed: {e}")

    def handleTradingDataFromAPI(self) -> Dict[str, Any]:
        success = self.handleTradingUpdatesFromJob()
        return {
            'success': success,
            'message': 'Trading updates processed' if success else 'Trading updates failed'
        }