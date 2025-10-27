"""
EMAProcessor - Handles EMA calculations and state management for scheduler

NEW OPTIMIZED EMA CALCULATION PROCESS:
=====================================

This processor implements a completely optimized approach that removes dependency on successful tokens list
and processes ALL active tokens automatically using a single optimized database query.

STEP 1: Single Optimized Query
- JOIN emastates with trackedtokens to get only active tokens
- JOIN with timeframemetadata to get lastfetchedat for each timeframe
- JOIN with ohlcvdetails to get candles where unixtime > lastupdatedunix
- All data retrieved in ONE highly optimized query for maximum scalability

STEP 2: Data Organization
- Organize data by token/timeframe/EMA period structure
- Each token contains ema21 and ema34 data for all timeframes
- Each timeframe contains: ema_value, last_updated_at, status, ema_available_at, last_fetched_at, candles

STEP 3: Action Determination (Three Cases)
Case A - NOT_AVAILABLE & lastfetchedat >= emaavailabletime:
  → We now have enough candles (21+ for EMA21)
  → Calculate EMA from scratch for all available candles
  → Update ohlcvdetails with EMA values for each candle
  → Update emastates: status=AVAILABLE, emavalue=latest_ema, lastupdatedunix=latest_candle

Case B - NOT_AVAILABLE & lastfetchedat < emaavailabletime:
  → Not enough candles yet (need 21 candles for EMA21)
  → Skip processing, wait for more data

Case C - AVAILABLE:
  → EMA already calculated up to lastupdatedunix timestamp
  → Process NEW candles after lastupdatedunix from pre-fetched data
  → Calculate incremental EMA using existing emavalue as starting point
  → Update ohlcvdetails with new EMA values
  → Update emastates with latest emavalue and lastupdatedunix

STEP 4: EMA Calculation Formula
- First 20 candles: No EMA (not enough data)
- 21st candle: EMA = SMA of first 21 candles
- 22nd+ candles: EMA = (Close × Multiplier) + (Previous_EMA × (1 - Multiplier))
- Where Multiplier = 2 / (Period + 1)

STEP 5: Batch Processing (ATOMIC)
- Process ALL tokens/timeframes/EMAs in memory
- Update ohlcvdetails.ema21value/ema34value for processed candles
- Update emastates with new emavalue, lastupdatedunix, nextfetchtime
- All operations in single transaction

BENEFITS:
- No dependency on successful tokens list
- Single optimized query (highly scalable)
- Processes ALL active tokens automatically
- Eliminates complex logic and points of failure
- True batch processing for optimal performance
- Resilient to server downtime - no gaps in EMA calculations
"""

from typing import List, TYPE_CHECKING
from logs.logger import get_logger
from database.trading.TradingHandler import TradingHandler
from utils.CommonUtil import CommonUtil
from database.trading.TradingHandler import EMAStatus
from database.trading.TradingHandler import EMAStatus
from api.trading.request import EMAState


if TYPE_CHECKING:
    from api.trading.request.TrackedToken import TrackedToken

logger = get_logger(__name__)

class EMACalculationType:
    """Result container for EMA action determination"""
    NOT_AVAILABLE_INSUFFICIENT = 'not_available_insufficient'  # Not enough candles yet
    NOT_AVAILABLE_READY = 'not_available_ready'               # Ready for first calculation  
    AVAILABLE_UPDATE = 'available_update'                     # Update existing EMA

class EMAProcessor:
    """
    Production-ready EMA processor with sophisticated state management and incremental calculations
    """
    
    def __init__(self, trading_handler: TradingHandler):
        self.trading_handler = trading_handler
        self.EMA_PERIODS = [12, 21, 34]

    def calculateEMAForAllRetrievedTokens(self, trackedTokens: List['TrackedToken']) -> None:
        try:
            totalProcessed = 0
            
            for trackedToken in trackedTokens:
                for timeframeRecord in trackedToken.timeframeRecords:
                    # Process EMA12, EMA21 and EMA34
                    for emaPeriod in self.EMA_PERIODS:
                        # Get the appropriate EMA state
                        emaState = timeframeRecord.ema12State if emaPeriod == 12 else timeframeRecord.ema21State if emaPeriod == 21 else timeframeRecord.ema34State
                        
                        if not emaState:
                            continue
                        
                        # Determine action type based on status and data availability
                        emaCalculationType = self.findEMACalculationType(
                            emaState.status, 
                            timeframeRecord.lastFetchedAt or 0, 
                            emaState.emaAvailableTime or 0
                        )
                        
                        if emaCalculationType == EMACalculationType.NOT_AVAILABLE_INSUFFICIENT:
                            continue
                        
                        # Calculate EMA based on action type
                        if emaCalculationType == EMACalculationType.NOT_AVAILABLE_READY:
                            # First calculation - calculate from scratch
                            self.performFirstEMACalculationWithPOJOs(
                                timeframeRecord, emaPeriod, trackedToken, emaState.emaAvailableTime or 0
                            )
                        elif emaCalculationType == EMACalculationType.AVAILABLE_UPDATE:
                            # Incremental update - use existing EMA value
                            self.performIncrementalEMAUpdateWithPOJOs(
                                timeframeRecord, emaPeriod, trackedToken, emaState.emaValue or 0, 
                                emaState.lastUpdatedUnix or 0
                            )
                        
                        totalProcessed += 1
        
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error processing EMA calculations for {trackedToken.symbol} - {timeframeRecord.timeframe}: {e}")

    def findEMACalculationType(self, status: int, lastFetchedAt: int, emaAvailableAt: int) -> str:
        """
        Determine EMA action based on status and data availability
        
        Args:
            status: EMA status (1=NOT_AVAILABLE, 2=AVAILABLE)
            lastFetchedAt: Last time candles were fetched
            emaAvailableAt: When EMA becomes available
            
        Returns:
            str: Action type (NOT_AVAILABLE_INSUFFICIENT, NOT_AVAILABLE_READY, AVAILABLE_UPDATE)
        """
        if status == EMAStatus.NOT_AVAILABLE:
            if lastFetchedAt >= emaAvailableAt:
                return EMACalculationType.NOT_AVAILABLE_READY
            else:
                return EMACalculationType.NOT_AVAILABLE_INSUFFICIENT
        elif status == EMAStatus.AVAILABLE:
            return EMACalculationType.AVAILABLE_UPDATE
        
        return EMACalculationType.NOT_AVAILABLE_INSUFFICIENT

    def performFirstEMACalculationWithPOJOs(self, timeframeRecord, emaPeriod: int, 
                                           trackedToken: 'TrackedToken', emaAvailableAt: int) -> None:
        """
        Perform first-time EMA calculation using POJOs and update them directly
        """
        try:
            tokenAddress = trackedToken.tokenAddress
            pairAddress = trackedToken.pairAddress
            symbol = trackedToken.symbol

            # Filter candles to only include those from emaAvailableAt onwards
            filteredCandles = [c for c in timeframeRecord.ohlcvDetails if c.unixTime >= emaAvailableAt]
            
            if len(filteredCandles) < emaPeriod:
                logger.warning(f"TRADING SCHEDULER :: Not enough candles for first EMA{emaPeriod} calculation: {symbol} - {timeframeRecord.timeframe}")
                return

            logger.info(f"TRADING SCHEDULER :: First EMA{emaPeriod} calculation for {symbol} - {timeframeRecord.timeframe} - started")
            
            # Use the shared EMA calculation method with POJOs
            timeframeInSeconds = CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
            success = self.calcualteFirstEMAFromCandles(
                timeframeRecord, emaPeriod, tokenAddress, pairAddress, 
                timeframeRecord.timeframe, emaAvailableAt, 0, timeframeInSeconds
            )
            
            if success:
                logger.info(f"TRADING SCHEDULER :: First EMA{emaPeriod} calculation for {symbol} - {timeframeRecord.timeframe} - completed")
            else:
                logger.warning(f"TRADING SCHEDULER :: First EMA{emaPeriod} calculation for {symbol} - {timeframeRecord.timeframe} - failed")
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in first EMA calculation : {e}")
    
    def performIncrementalEMAUpdateWithPOJOs(self, timeframeRecord, emaPeriod: int, 
                                            trackedToken: 'TrackedToken', 
                                            currentEMA: float, lastUpdatedAt: int) -> None:
        """
        Perform incremental EMA update using POJOs and update them directly
        """
        try:
            # Filter candles to only include new ones after lastUpdatedAt
            symbol = trackedToken.symbol
            newCandles = [c for c in timeframeRecord.ohlcvDetails if c.unixTime > lastUpdatedAt]
            
            if not newCandles:
                logger.info(f"TRADING SCHEDULER :: No new candles for incremental EMA update: {symbol} - {timeframeRecord.timeframe}")
                return
            
            currentEMAValue = currentEMA
            latestUNIX = lastUpdatedAt

            logger.info(f"TRADING SCHEDULER :: Incremental EMA{emaPeriod} update for {symbol} - {timeframeRecord.timeframe} - started")
            
            for candle in newCandles:
                currentEMAValue = self.calculateEMAValue(currentEMAValue, candle.closePrice, emaPeriod)
                latestUNIX = candle.unixTime
                
                # Update the candle POJO directly with EMA value
                if emaPeriod == 12:
                    candle.ema12Value = currentEMAValue
                elif emaPeriod == 21:
                    candle.ema21Value = currentEMAValue
                elif emaPeriod == 34:
                    candle.ema34Value = currentEMAValue
            
            # Update the EMAState POJO directly
            emaState = timeframeRecord.ema12State if emaPeriod == 12 else timeframeRecord.ema21State if emaPeriod == 21 else timeframeRecord.ema34State
            if emaState:
                emaState.emaValue = currentEMAValue
                emaState.lastUpdatedUnix = latestUNIX
                emaState.nextFetchTime = latestUNIX + CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
                emaState.status = EMAStatus.AVAILABLE
            
            logger.info(f"TRADING SCHEDULER :: Incremental EMA{emaPeriod} update for {symbol} - {timeframeRecord.timeframe} - completed")
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in incremental EMA update for {symbol} - {timeframeRecord.timeframe} - {e}")


    def calculateEMAValue(self, previousEMA: float, currentPrice, period: int) -> float:
        """
        Calculate single EMA value using standard EMA formula
        EMA = (Close - Previous_EMA) * (2 / (Period + 1)) + Previous_EMA
        """
        # Convert currentPrice to float if it's a Decimal
        currentPriceFloat = float(currentPrice)
        multiplier = 2.0 / (period + 1)
        return (currentPriceFloat - previousEMA) * multiplier + previousEMA
    
    def calculateInitialCandleStartTime(self, pairCreatedTime: int, timeframe: str) -> int:
        """
        Calculate the initial candle start time - delegates to CommonUtil
        """
        return CommonUtil.calculateInitialStartTime(pairCreatedTime, timeframe)
    
    
    
    def calcualteFirstEMAFromCandles(self, timeframeRecord, emaPeriod: int,
                               tokenAddress: str, pairAddress: str, timeframe: str,
                               ema_available_time: int, pair_created_time: int, timeframe_in_seconds: int) -> bool:
        """
        SHARED METHOD: Calculate EMA from OHLCVDetails POJOs and update TimeframeRecord directly

        CORRECT EMA LOGIC:
        - For EMA12: Calculate SMA using first 12 candles for the 12th candle
        - For EMA21: Calculate SMA using first 21 candles for the 21st candle
        - For EMA34: Calculate SMA using first 34 candles for the 34th candle
        - Then calculate EMA for all subsequent candles using standard EMA formula
        - This matches the emaavailabletime logic that ensures we have enough candles

        Args:
            timeframeRecord: TimeframeRecord POJO (will be updated in place)
            ema_period: EMA period (12 or 21 or 34)
            token_address: Token address
            pair_address: Pair address
            timeframe: Timeframe
            ema_available_time: When EMA becomes available
            pair_created_time: When pair was created
            timeframe_in_seconds: Timeframe in seconds

        Returns:
            bool: True if calculation successful, False otherwise
        """
        try:
            tokenAddress = trackedToken.tokenAddress
            pairAddress = trackedToken.pairAddress
            symbol = trackedToken.symbol
            candles = timeframeRecord.ohlcvDetails
            if len(candles) < emaPeriod:
                logger.info(f"TRADING SCHEDULER :: Not enough candles for EMA{emaPeriod} calculation: {symbol} - {timeframe}")
                return False

            currentEMA = None
            latestUNIX = 0

            for i, candle in enumerate(candles):
                if i < emaPeriod - 1:
                    # Skip first (emaPeriod-1) candles - no EMA value yet
                    # For EMA21: Skip first 20 candles (index 0-19)
                    continue
                elif i == emaPeriod - 1:
                    # emaPeriod-th candle: Calculate SMA as initial EMA value
                    # For EMA21: 21st candle (index 20) gets SMA of first 21 candles
                    sma = sum(candles[j].closePrice for j in range(i + 1)) / (i + 1)
                    currentEMA = sma
                else:
                    # Subsequent candles: Calculate EMA using previous EMA value
                    # For EMA21: 22nd+ candles use standard EMA formula
                    currentEMA = self.calculateEMAValue(currentEMA, candle.closePrice, emaPeriod)

                latestUNIX = candle.unixTime

                # Update the candle POJO directly with EMA value
                if emaPeriod == 12:
                    candle.ema12Value = currentEMA
                elif emaPeriod == 21:
                    candle.ema21Value = currentEMA
                elif emaPeriod == 34:
                    candle.ema34Value = currentEMA

            if currentEMA is None:
                logger.info(f"TRADING SCHEDULER :: No EMA values calculated for {symbol} - {timeframe} EMA{emaPeriod}")
                return False

            
            emaState = EMAState(
                tokenAddress=tokenAddress,
                pairAddress=pairAddress,
                timeframe=timeframe,
                emaKey=str(emaPeriod),
                emaValue=currentEMA,
                lastUpdatedUnix=latestUNIX,
                nextFetchTime=latestUNIX + timeframe_in_seconds,
                emaAvailableTime=ema_available_time,
                pairCreatedTime=pair_created_time,
                status=EMAStatus.AVAILABLE
            )

            # Set the EMAState directly in timeframeRecord
            if emaPeriod == 12:
                timeframeRecord.ema12State = emaState
            elif emaPeriod == 21:
                timeframeRecord.ema21State = emaState
            elif emaPeriod == 34:
                timeframeRecord.ema34State = emaState

            return True

        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in shared EMA calculation: {e}")
            return False


    def calculateEMAInMemory(self, timeframeRecord, tokenAddress: str, pairAddress: str, pairCreatedTime: int) -> None:
        try:
            if not timeframeRecord.ohlcvDetails:
                logger.warning(f"TRADING API :: No candles available for EMA {tokenAddress} - {timeframeRecord.timeframe}")
                return
            
            logger.info(f"TRADING API :: Processing EMA {tokenAddress} - {timeframeRecord.timeframe} with {len(timeframeRecord.ohlcvDetails)} candles")
            
            # Get the latest fetched time from candles (same logic as calcualteEMAForNewTokenFromAPI)
            latestFetchedAtTime = max(candle.unixTime for candle in timeframeRecord.ohlcvDetails)
            timeframeInSeconds = CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
            
            # Process EMA12, EMA21 and EMA34
            for emaPeriod in [12, 21, 34]:
                # Calculate EMA available time (same logic as calcualteEMAForNewTokenFromAPI)
                initialCandleStartTime = self.calculateInitialCandleStartTime(pairCreatedTime, timeframeRecord.timeframe)
                emaAvailableTime = initialCandleStartTime + (emaPeriod * timeframeInSeconds)
                
                logger.info(f"TRADING API :: Processing EMA{emaPeriod} {tokenAddress} - {timeframeRecord.timeframe}: available at {emaAvailableTime}, latest fetched: {latestFetchedAtTime}")
                
                # Check if we have enough data to calculate EMA (same logic as calcualteEMAForNewTokenFromAPI)
                if latestFetchedAtTime >= emaAvailableTime:
                    logger.info(f"TRADING API :: EMA{emaPeriod} data available, calculation started for {tokenAddress} - {timeframeRecord.timeframe}")
                    
                    # Use the SHARED EMA calculation method - updates timeframeRecord directly
                    success = self.calcualteFirstEMAFromCandles(
                        timeframeRecord, emaPeriod, tokenAddress, pairAddress, 
                        timeframeRecord.timeframe, emaAvailableTime, pairCreatedTime, timeframeInSeconds
                    )
                    
                    if not success:
                        logger.warning(f"TRADING API :: Failed to calculate EMA{emaPeriod} for {tokenAddress} - {timeframeRecord.timeframe}")
                    else:
                        logger.info(f"TRADING API :: EMA{emaPeriod} calculation completed for {tokenAddress} - {timeframeRecord.timeframe}")
                else:
                    logger.info(f"TRADING API :: EMA{emaPeriod} not available, will be processed when data reaches {emaAvailableTime} for {tokenAddress} - {timeframeRecord.timeframe}")
                    
                    # Create EMA state with status 2 (AVAILABLE) and emaAvailableTime so scheduler can pick it up
                    emaState = EMAState(
                        tokenAddress=tokenAddress,
                        pairAddress=pairAddress,
                        timeframe=timeframeRecord.timeframe,
                        emaKey=str(emaPeriod),
                        emaValue=None,  # No value yet
                        lastUpdatedUnix=None,  # No updates yet
                        nextFetchTime=None,  # Will be calculated when data becomes available
                        emaAvailableTime=emaAvailableTime,
                        pairCreatedTime=pairCreatedTime,
                        status=EMAStatus.NOT_AVAILABLE  
                    )
                    
                    # Set the EMA state in the timeframe record
                    if emaPeriod == 12:
                        timeframeRecord.ema12State = emaState
                    elif emaPeriod == 21:
                        timeframeRecord.ema21State = emaState
                    elif emaPeriod == 34:
                        timeframeRecord.ema34State = emaState
                    
            logger.info(f"TRADING API :: Completed EMA calculation for {tokenAddress} - {timeframeRecord.timeframe}")
            
        except Exception as e:
            logger.info(f"TRADING API :: Error calculating EMA in memory for {tokenAddress} - {timeframeRecord.timeframe}: {e}")
    
   