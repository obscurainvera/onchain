
from typing import Dict, Any, List, Optional
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler, AdditionSource
import time
from logs.logger import get_logger
from services.BirdEyeServiceHandler import BirdEyeServiceHandler
from services.MoralisServiceHandler import MoralisServiceHandler
from constants.TradingConstants import TimeframeConstants, TokenFlowConstants, ValidationMessages
from constants.TradingAPIConstants import TradingAPIConstants
from constants.TradingHandlerConstants import TradingHandlerConstants
from scheduler.VWAPProcessor import VWAPProcessor
from scheduler.EMAProcessor import EMAProcessor
from scheduler.AVWAPProcessor import AVWAPProcessor
from scheduler.SchedulerConstants import CandleDataKeys
from api.trading.request import (AddTokenRequest, TokenInfo, AllTimeframesCandleData, TimeframeCandleData, 
                                 TimeframeRecord, OHLCVDetails, EMAState, AVWAPState)
from api.trading.response import AddTokenResponse
from models.Candle import Candle
from utils.CommonUtil import CommonUtil
from scheduler.VWAPProcessor import VWAPProcessor
from scheduler.EMAProcessor import EMAProcessor
from scheduler.AVWAPProcessor import AVWAPProcessor


logger = get_logger(__name__)

class TradingActionEnhanced:
    """Enhanced Trading Action with comprehensive token processing flows"""
    
    def __init__(self, db: PortfolioDB):
        """Initialize with database handler"""
        self.db = db
        self.trading_handler = TradingHandler(db.conn_manager)
        self.birdeye_handler = BirdEyeServiceHandler(db)
        self.moralis_handler = MoralisServiceHandler(db)
        
        # Initialize processor instances        
        self.vwap_processor = VWAPProcessor(self.trading_handler)
        self.ema_processor = EMAProcessor(self.trading_handler)
        self.avwap_processor = AVWAPProcessor(self.trading_handler, self.moralis_handler)
 

    
    def addTokenToTrackedTokensDatabase(self, tokenAddress: str, pairAddress: str, symbol: str, 
                                name: str, pairCreatedTime: int, addedBy: str) -> int:
        """Setup token in tracked tokens table"""
        tokenId = self.trading_handler.addToken(
            tokenAddress=tokenAddress,
            symbol=symbol,
            name=name,
            pairAddress=pairAddress,
            pairCreatedTime=pairCreatedTime,
            additionSource=AdditionSource.MANUAL if addedBy != "automatic_system" else AdditionSource.AUTOMATIC,
            addedBy=addedBy
        )
        
        if not tokenId:
            raise ValueError(ValidationMessages.FAILED_TO_ADD_TOKEN)
        
        return tokenId
    
    def addInitialTimeframeRecords(self, tokenAddress: str, pairAddress: str, 
                                  timeframes: List[str], pairCreatedTime: int, addedBy: str) -> List[TimeframeRecord]:
        """Setup timeframe metadata records for scheduling and return as POJOs"""
        timeframeRecords = self.trading_handler.createTimeframeInitialRecords(
            tokenAddress, pairAddress, timeframes, pairCreatedTime
        )
        
        if not timeframeRecords:
            result = self.trading_handler.disableToken(tokenAddress, addedBy, "Failed to create timeframe records")
            if not result['success']:
                logger.warning(f"Failed to disable token {tokenAddress} after timeframe creation failure: {result['error']}")
            raise ValueError(ValidationMessages.FAILED_TIMEFRAME_RECORDS)
        
        return timeframeRecords
   
    
    
    def calculateAVWAPFromHistoricalData(self, tokenAddress: str, pairAddress: str, 
                                        pairCreatedTime: int, timeframes: List[str]) -> Dict[str, Any]:
                                        
        return self.avwap_processor.calculateAVWAPFromHistoricalData(
            tokenAddress, pairAddress, pairCreatedTime, timeframes
        )

    def addTokenForTracking(self, request: AddTokenRequest, tokenInfo: TokenInfo) -> AddTokenResponse:
        try:
            logger.info(f"Adding token {tokenInfo.symbol} with optimized unified flow for timeframes: {request.timeframes}")
            
            # Step 1: Add token to tracked tokens table
            tokenId = self.addTokenToTrackedTokensDatabase(
                request.tokenAddress, request.pairAddress, tokenInfo.symbol, 
                tokenInfo.name, tokenInfo.pairCreatedTimeSeconds, request.addedBy
            )
            
            # Step 2: Create initial timeframe records and get them back as POJOs
            timeframeRecords = self.addInitialTimeframeRecords(
                request.tokenAddress, request.pairAddress, request.timeframes, 
                tokenInfo.pairCreatedTimeSeconds, request.addedBy
            )
            
            # Step 3: Smart candle fetching based on nextFetchTime
            candleDataByTimeframe = self.fetchCandlesBasedOnNextFetchTime(
                request.tokenAddress, request.pairAddress, tokenInfo.symbol, 
                timeframeRecords, tokenInfo.pairCreatedTimeSeconds
            )
            
            # Step 4: Calculate all indicators in memory
            self.calculateAllIndicatorsInMemory(
                candleDataByTimeframe, request.tokenAddress, request.pairAddress, 
                tokenInfo.pairCreatedTimeSeconds
            )
            
            # Step 5: Persist everything in one transaction (latest 2 candles + all indicator data)
            candlesInserted = self.updateCandleAndIndicatorData(
                candleDataByTimeframe
            )
            
            return AddTokenResponse.success_response(
                tokenId=tokenId,
                tokenAddress=request.tokenAddress,
                pairAddress=request.pairAddress,
                tokenAge=tokenInfo.pairAgeInDays,
                candlesInserted=candlesInserted,
                creditsUsed=0,
                timeframes=request.timeframes
            )
            
        except Exception as e:
            logger.error(f"Error in unified token addition flow: {e}")
            return self.handleUnifiedTokenAdditionError(request.tokenAddress, request.addedBy, e)
    
    def fetchCandlesBasedOnNextFetchTime(self, tokenAddress: str, pairAddress: str, symbol: str,
                                        timeframeRecords: List[TimeframeRecord], pairCreatedTime: int) -> Dict[str, TimeframeRecord]:
        # Calculate adjusted fromTime to avoid missing first candle 
        # for example : is case of new tokens- it we directly pass the paircreatedtime as fromtime - 
        # then there is a chance for the api to exclude the initial candle as the api excludes 'fromtime' 
        # so due to this always set the 
        # fromtime = (paircreatedtime - (timeframeseconds * 2)) - 
        # which means if the paircreatedtime=4:56 and the time frame is 30M, 
        # then the new fromtime =(4:56 - (30m * 2)) = (4:56 - 1hr) = 3:56
        currentTime = int(time.time())
        result = {}
        
        for timeframeRecord in timeframeRecords:
            if timeframeRecord.shouldFetchFromAPI(currentTime):
                # Fetch candles for this timeframe
                logger.info(f"Fetching {timeframeRecord.timeframe} candles for {symbol} (nextFetchTime: {timeframeRecord.nextFetchAt} <= currentTime: {currentTime})")
                
                try:
                    # Fetch candle data from API
                    timeframeSeconds = TimeframeConstants.getSeconds(timeframeRecord.timeframe)
                    adjustedFromTime = pairCreatedTime - (timeframeSeconds * TokenFlowConstants.FROM_TIME_BUFFER_MULTIPLIER)
                    candleResponse = self.moralis_handler.getCandleDataForToken(
                        tokenAddress=tokenAddress,
                        pairAddress=pairAddress,
                        fromTime=adjustedFromTime,
                        toTime=currentTime,
                        timeframe=timeframeRecord.timeframe,
                        symbol=symbol
                    )
                    
                    if candleResponse.success:
                        # Convert models.Candle to OHLCVDetails POJOs
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
                        
                        # Sort candles by unixTime in ascending order (required for EMA calculation)
                        timeframeRecord.ohlcvDetails.sort(key=lambda x: x.unixTime)
                        
                        # Update timeframe metadata using CandleResponse data
                        nextFetchTime = CommonUtil.calculateNextFetchTimeForTimeframe(candleResponse.latestTime, timeframeRecord.timeframe)
                        timeframeRecord.updateAfterFetch(candleResponse.latestTime, nextFetchTime)
                        
                        logger.info(f"Fetched {len(timeframeRecord.ohlcvDetails)} {timeframeRecord.timeframe} candles for {symbol} (sorted by unixTime)")
                    else:
                        logger.warning(f"Failed to fetch {timeframeRecord.timeframe} data for {symbol}: {candleResponse.error}")
                        
                except Exception as e:
                    logger.error(f"Error fetching {timeframeRecord.timeframe} data for {symbol}: {e}")
            else:
                logger.info(f"Skipping {timeframeRecord.timeframe} fetch for {symbol} (nextFetchTime: {timeframeRecord.nextFetchAt} > currentTime: {currentTime})")
            
            result[timeframeRecord.timeframe] = timeframeRecord
        
        return result
    
    def calculateAllIndicatorsInMemory(self, candleDataByTimeframe: Dict[str, TimeframeRecord], 
                                     tokenAddress: str, pairAddress: str, pairCreatedTime: int):
        try:
            logger.info(f"Calculating indicators in memory for {tokenAddress} across {len(candleDataByTimeframe)} timeframes")
            
            for timeframe, timeframeRecord in candleDataByTimeframe.items():
                logger.info(f"Processing indicators for {timeframe} timeframe")
                
                # Calculate VWAP
                VWAPProcessor(self.trading_handler).calculateVWAPInMemory(timeframeRecord, tokenAddress, pairAddress)
                
                # Calculate EMA
                EMAProcessor(self.trading_handler).calculateEMAInMemory(timeframeRecord, tokenAddress, pairAddress, pairCreatedTime)
                
                # Calculate AVWAP
                AVWAPProcessor(self.trading_handler).calculateAVWAPInMemory(timeframeRecord, tokenAddress, pairAddress)
            
            logger.info(f"Successfully calculated all indicators in memory for {tokenAddress}")
            
        except Exception as e:
            logger.error(f"Error calculating indicators in memory for {tokenAddress}: {e}")
            # Don't raise - indicators are supplementary, token addition should still succeed
    
    
    def updateCandleAndIndicatorData(self, candleDataByTimeframe: Dict[str, TimeframeRecord], maxCandlesPerTimeframe: int = 2) -> int:
        try:
            # Convert dict to list for batch processing
            timeframeRecords = list(candleDataByTimeframe.values())
            
            # Use the optimized batch method from TradingHandler with configurable candle limit
            totalCandlesInserted = self.trading_handler.batchPersistOptimizedTokenData(
                timeframeRecords, maxCandlesPerTimeframe
            )
            
            logger.info(f"Persisted {totalCandlesInserted} candles (max {maxCandlesPerTimeframe} per timeframe) and all indicator data in single transaction")
            return totalCandlesInserted
            
        except Exception as e:
            logger.error(f"Error persisting optimized data: {e}")
            return 0

    

    def handleUnifiedTokenAdditionError(self, tokenAddress: str, addedBy: str, error: Exception) -> AddTokenResponse:
        """
        Handle and cleanup token addition errors in unified flow
        
        Args:
            tokenAddress: Token address that failed
            addedBy: User who attempted to add the token
            error: Exception that occurred
            
        Returns:
            AddTokenResponse with error details
        """
        logger.error(f"Error in unified token addition: {error}")
        try:
            result = self.trading_handler.disableToken(tokenAddress, addedBy, f"Addition failed: {str(error)}")
            if not result['success']:
                logger.warning(f"Failed to disable token {tokenAddress} after addition failure: {result['error']}")
        except Exception as e:
            logger.warning(f"Exception while disabling token {tokenAddress} after addition failure: {e}")
        
        return AddTokenResponse.error_response(str(error))
