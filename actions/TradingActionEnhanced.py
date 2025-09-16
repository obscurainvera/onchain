
from typing import Dict, Any, List
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler, AdditionSource
import time
from logs.logger import get_logger
from services.BirdEyeServiceHandler import BirdEyeServiceHandler
from services.MoralisServiceHandler import MoralisServiceHandler
from constants.TradingConstants import TimeframeConstants, TokenFlowConstants, ValidationMessages
from scheduler.VWAPProcessor import VWAPProcessor
from scheduler.EMAProcessor import EMAProcessor
from scheduler.SchedulerConstants import CandleDataKeys


logger = get_logger(__name__)

class TradingActionEnhanced:
    """Enhanced Trading Action with comprehensive token processing flows"""
    
    def __init__(self, db: PortfolioDB):
        """Initialize with database handler"""
        self.db = db
        self.trading_handler = TradingHandler(db.conn_manager)
        
        # Use unified service handlers
        self.birdeye_handler = BirdEyeServiceHandler(db)
        self.moralis_handler = MoralisServiceHandler(db)
        
        # Initialize processor instances
        self.vwap_processor = VWAPProcessor(self.trading_handler)
        self.ema_processor = EMAProcessor(self.trading_handler)
 

    def addNewTokenWithTimeframes(self, tokenAddress: str, pairAddress: str, symbol: str, name: str, 
                                 pairCreatedTime: int, timeframes: List[str], addedBy: str) -> Dict[str, Any]:
        """
        NEW FLOW: Add new token with specified timeframes using Moralis API
        
        This method implements the complete new token onboarding flow:
        1. Database setup (token + timeframe scheduling)
        2. Historical data fetching with proper edge case handling
        3. Batch data persistence for optimal performance
        """
        try:
            logger.info(f"Adding new token {symbol} with timeframes: {timeframes}")
            
            # Step 1: add token to trackedtokens table
            tokenId = self.addTokenToTrackedTokensDatabase(
                tokenAddress, pairAddress, symbol, name, pairCreatedTime, addedBy
            )
            
            # Step 2: add initial timeframe records with lastfetchedat as null
            self.addInitialTimeframeRecords(tokenAddress, pairAddress, timeframes, pairCreatedTime, addedBy)
            
            # Step 3: Fetch and persist candle data for all timeframes
            candleResults = self.persitCandlesFetchedFromAPI(
                tokenAddress, pairAddress, symbol, timeframes, pairCreatedTime
            )
            
            # Step 4: Calculate and update VWAP and EMA for all timeframes
            self.calculateAndUpdateIndicatorsForNewTokenFromAPI(tokenAddress, pairAddress, pairCreatedTime, timeframes)
            
            return self.constructSuccessResponseForNewTokenFromAPI(tokenId, candleResults, timeframes)
            
        except Exception as e:
            return self.handleTokenAdditionError(tokenAddress, addedBy, e)
    
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
                                  timeframes: List[str], pairCreatedTime: int, addedBy: str):
        """Setup timeframe metadata records for scheduling"""
        timeframeCreated = self.trading_handler.createTimeframeInitialRecords(
            tokenAddress, pairAddress, timeframes, pairCreatedTime
        )
        
        if not timeframeCreated:
            result = self.trading_handler.disableToken(tokenAddress, addedBy, "Failed to create timeframe records")
            if not result['success']:
                logger.warning(f"Failed to disable token {tokenAddress} after timeframe creation failure: {result['error']}")
            raise ValueError(ValidationMessages.FAILED_TIMEFRAME_RECORDS)
    
    def persitCandlesFetchedFromAPI(self, tokenAddress: str, pairAddress: str, symbol: str,
                                     timeframes: List[str], pairCreatedTime: int) -> Dict:
        """Fetch candle data for all timeframes and persist in batch"""
        currentTime = int(time.time())
        allCandleData = {}
        totalCreditsUsed = 0
        
        for timeframe in timeframes:
            candleResult = self.fetchCandlesFromAPI(
                tokenAddress, pairAddress, symbol, timeframe, pairCreatedTime, currentTime
            )
            
            if candleResult.get(CandleDataKeys.CANDLES):
                allCandleData[f"{tokenAddress}_{timeframe}"] = {
                    CandleDataKeys.CANDLES: candleResult[CandleDataKeys.CANDLES],
                    CandleDataKeys.LATEST_TIME: candleResult[CandleDataKeys.LATEST_TIME],
                    CandleDataKeys.COUNT: candleResult[CandleDataKeys.COUNT]
                }
                totalCreditsUsed += candleResult.get('creditsUsed', 0)
                logger.info(f"Fetched {candleResult[CandleDataKeys.COUNT]} {timeframe} candles")
        
        # Persist all data in batch
        candlesInserted = 0
        if allCandleData:
            candlesInserted = self.trading_handler.batchPersistAllCandles(allCandleData)
            logger.info(f"Persisted {candlesInserted} total candles across all timeframes")
        
        return {'candlesInserted': candlesInserted, 'creditsUsed': totalCreditsUsed}
    
    def calculateAndUpdateIndicatorsForNewTokenFromAPI(self, tokenAddress: str, pairAddress: str, 
                                       pairCreatedTime: int, timeframes: List[str]):
        """
        Calculate and update VWAP and EMA indicators for all timeframes
        
        This method processes indicators for the new token across all requested timeframes:
        - VWAP: Volume-weighted average price calculations
        - EMA: Exponential moving averages (21 and 34 periods)
        """
        try:
            logger.info(f"Calculating indicators for {tokenAddress} across timeframes: {timeframes}")
            
            # Get all candles for all timeframes that were just persisted
            # Only process records where lastfetchedat IS NOT NULL (as per requirements)
            allCandles = self.trading_handler.getAllCandlesFromAllTimeframes(tokenAddress, pairAddress)
            
            if not allCandles:
                logger.warning(f"No candles found for indicator calculation for {tokenAddress}")
                return
            
            # Process VWAP for all timeframes
            vwapResults = self.vwap_processor.calculateVWAPFromAPI(tokenAddress, pairAddress, pairCreatedTime, allCandles)
            
            # Process EMA for all timeframes  
            calculatedEMA = self.ema_processor.calcualteEMAForNewTokenFromAPI(tokenAddress, pairAddress, pairCreatedTime, allCandles)
            
            logger.info(f"Successfully calculated indicators for {tokenAddress}")
            
        except Exception as e:
            logger.error(f"Error calculating indicators for {tokenAddress}: {e}")
            # Don't raise - indicators are supplementary, token addition should still succeed

    def calculateAndUpdateIndicatorsForOldTokenFromAPI(self, tokenAddress: str, pairAddress: str, 
                                       pairCreatedTime: int, timeframes: List[str], perTimeframeEMAData: Dict):
        """
        Calculate and update VWAP and EMA indicators for all timeframes
        
        This method processes indicators for the new token across all requested timeframes:
        - VWAP: Volume-weighted average price calculations
        - EMA: Exponential moving averages (21 and 34 periods)
        """
        try:
            logger.info(f"Calculating indicators for {tokenAddress} across timeframes: {timeframes}")
            
            # Get all candles for all timeframes that were just persisted
            # Only process records where lastfetchedat IS NOT NULL (as per requirements)
            allCandles = self.trading_handler.getAllCandlesFromAllTimeframes(tokenAddress, pairAddress)
            
            if not allCandles:
                logger.warning(f"No candles found for indicator calculation for {tokenAddress}")
                return
            
            # Process VWAP for all timeframes
            self.vwap_processor.calculateVWAPFromAPI(tokenAddress, pairAddress, pairCreatedTime, allCandles)
           
            
            # Process EMA for all timeframes  
            self.ema_processor.setEMAForOldTokenFromAPI(
                tokenAddress, pairAddress, pairCreatedTime, perTimeframeEMAData, allCandles
            )
            
            
            logger.info(f"Successfully calculated indicators for {tokenAddress}")
            
        except Exception as e:
            logger.error(f"Error calculating indicators for {tokenAddress}: {e}")
            # Don't raise - indicators are supplementary, token addition should still succeed
    
    def addOldTokenWithTimeframes(self, tokenAddress: str, pairAddress: str, symbol: str, name: str,
                                 pairCreatedTime: int, timeframes: List[str], perTimeframeEMAData: Dict,
                                 addedBy: str) -> Dict[str, Any]:
        """
        OLD TOKEN FLOW: Add old token (>7 days) with specified timeframes using Moralis API
        
        This method implements the complete old token onboarding flow:
        1. Database setup (token + timeframe scheduling)  
        2. Historical data fetching (48 hours) for each timeframe
        3. Batch data persistence with timeframe updates
        4. VWAP and EMA calculations for successfully fetched timeframes
        """
        try:
            logger.info(f"Adding old token {symbol} with timeframes: {timeframes}")
            
            # Step 1: Setup token in database
            tokenId = self.addTokenToTrackedTokensDatabase(
                tokenAddress, pairAddress, symbol, name, pairCreatedTime, addedBy
            )
            
            # Step 2: Setup timeframe scheduling (with null lastfetchedat initially)
            self.addInitialTimeframeRecords(tokenAddress, pairAddress, timeframes, pairCreatedTime, addedBy)
            
            # Step 3: Fetch and persist 48-hour historical data for each timeframe
            candleResults = self.fetchAndPersist48HourDataForOldToken(
                tokenAddress, pairAddress, symbol, timeframes
            )
            
            # Step 4: Calculate and update VWAP and EMA for successfully fetched timeframes
            self.calculateAndUpdateIndicatorsForOldTokenFromAPI(tokenAddress, pairAddress, pairCreatedTime, timeframes, perTimeframeEMAData)
            
            return self.constructSuccessResponseForOldTokenFromAPI(tokenId, candleResults, timeframes, perTimeframeEMAData)
            
        except Exception as e:
            return self.handleTokenAdditionError(tokenAddress, addedBy, e)
    
    def fetchAndPersist48HourDataForOldToken(self, tokenAddress: str, pairAddress: str, 
                                           symbol: str, timeframes: List[str]) -> Dict:
        """
        Fetch 48-hour historical data for each timeframe using Moralis API and persist in batch
        """
        try:
            currentTime = int(time.time())
            fromTime = currentTime - (48 * 3600)  # 48 hours ago
            
            allCandleData = {}
            totalCreditsUsed = 0
            
            logger.info(f"Fetching 48-hour data for {symbol} from {fromTime} to {currentTime}")
            
            # Fetch data for each timeframe
            for timeframe in timeframes:
                logger.info(f"Fetching {timeframe} data for {symbol}")
                
                candleDataFromAPI = self.moralis_handler.getCandleDataForToken(
                    tokenAddress, pairAddress, fromTime, currentTime, timeframe, symbol
                )
                
                if candleDataFromAPI['success'] and candleDataFromAPI[CandleDataKeys.CANDLES]:
                    # Format for batch persistence with token_address_timeframe key
                    key = f"{tokenAddress}_{timeframe}"
                    allCandleData[key] = {
                        CandleDataKeys.CANDLES: candleDataFromAPI[CandleDataKeys.CANDLES],
                        CandleDataKeys.LATEST_TIME: candleDataFromAPI[CandleDataKeys.LATEST_TIME],
                        CandleDataKeys.COUNT: candleDataFromAPI[CandleDataKeys.COUNT]
                    }
                    totalCreditsUsed += candleDataFromAPI.get('creditsUsed', 0)
                    
                    logger.info(f"Fetched {candleDataFromAPI[CandleDataKeys.COUNT]} {timeframe} candles for {symbol}")
                else:
                    logger.warning(f"Failed to fetch {timeframe} data for {symbol}: {candleDataFromAPI.get('error', 'Unknown error')}")
            
            # Persist all data in batch
            candlesInserted = 0
            if allCandleData:
                candlesInserted = self.trading_handler.batchPersistAllCandles(allCandleData)
                logger.info(f"Persisted {candlesInserted} total candles across all timeframes for old token {symbol}")
            
            return {'candlesInserted': candlesInserted, 'creditsUsed': totalCreditsUsed}
            
        except Exception as e:
            logger.error(f"Error fetching 48-hour data for old token {symbol}: {e}")
            raise
    
    def constructSuccessResponseForOldTokenFromAPI(self, tokenId: int, candleResults: Dict, 
                                        timeframes: List[str], perTimeframeEMAData: Dict) -> Dict[str, Any]:
        """Build success response for old token addition"""
        return {
            'success': True,
            'tokenId': tokenId,
            'flow': 'old_token_with_timeframes',
            'timeframes': timeframes,
            'candlesInserted': candleResults['candlesInserted'],
            'creditsUsed': candleResults['creditsUsed'],
            'emaDataProcessed': len(perTimeframeEMAData) if perTimeframeEMAData else 0,
            'message': f'Old token successfully added with {len(timeframes)} timeframes'
        }

    def fetchCandlesFromAPI(self, tokenAddress: str, pairAddress: str, symbol: str,
                               timeframe: str, pairCreatedTime: int, currentTime: int) -> Dict:
        """Fetch candles for a specific timeframe with proper fromTime adjustment"""
        # Calculate adjusted fromTime to avoid missing first candle 
        # for example : is case of new tokens- it we directly pass the paircreatedtime as fromtime - 
        # then there is a chance for the api to exclude the initial candle as the api excludes 'fromtime' 
        # so due to this always set the 
        # fromtime = (paircreatedtime - (timeframeseconds * 2)) - 
        # which means if the paircreatedtime=4:56 and the time frame is 30M, 
        # then the new fromtime =(4:56 - (30m * 2)) = (4:56 - 1hr) = 3:56
        timeframeSeconds = TimeframeConstants.getSeconds(timeframe)
        adjustedFromTime = pairCreatedTime - (timeframeSeconds * TokenFlowConstants.FROM_TIME_BUFFER_MULTIPLIER)
        
        logger.info(f"Fetching {timeframe} candles from {adjustedFromTime} to {currentTime} "
                   f"(adjusted from pair creation time {pairCreatedTime})")
        
        candleResult = self.moralis_handler.getCandleDataForToken(
            tokenAddress=tokenAddress,
            pairAddress=pairAddress,
            fromTime=adjustedFromTime,
            toTime=currentTime,
            timeframe=timeframe,
            symbol=symbol
        )
        
        if not candleResult['success']:
            error_msg = ValidationMessages.getErrorMessage(timeframe, candleResult.get('error'))
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        return candleResult
    
    def constructSuccessResponseForNewTokenFromAPI(self, tokenId: int, candleResults: Dict, timeframes: List[str]) -> Dict:
        """Build standardized success response"""
        return {
            'success': True,
            'tokenId': tokenId,
            'mode': TokenFlowConstants.MODE_NEW_TOKEN_WITH_TIMEFRAMES,
            'candlesInserted': candleResults['candlesInserted'],
            'creditsUsed': candleResults['creditsUsed'],
            'timeframes': timeframes
        }
    
    def handleTokenAdditionError(self, tokenAddress: str, addedBy: str, error: Exception) -> Dict:
        """Handle and cleanup token addition errors"""
        logger.error(f"Error in new token addition with timeframes: {error}")
        try:
            result = self.trading_handler.disableToken(tokenAddress, addedBy, f"Addition failed: {str(error)}")
            if not result['success']:
                logger.warning(f"Failed to disable token {tokenAddress} after addition failure: {result['error']}")
        except Exception as e:
            logger.warning(f"Exception while disabling token {tokenAddress} after addition failure: {e}")
        return {'success': False, 'error': str(error)}
