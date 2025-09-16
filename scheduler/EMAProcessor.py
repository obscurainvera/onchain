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

from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal
from constants.TradingAPIConstants import TradingAPIConstants
from logs.logger import get_logger
from database.trading.TradingHandler import TradingHandler
from actions.TradingActionUtil import TradingActionUtil
from utils.CommonUtil import CommonUtil
from constants.TradingHandlerConstants import TradingHandlerConstants
from database.trading.TradingHandler import EMAStatus

from utils.IndicatorConstants import IndicatorConstants

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
        self.EMA_PERIODS = [21, 34]
        
    

    def processEMAForScheduler(self) -> bool:
        """
        NEW OPTIMIZED EMA PROCESSOR: Removes dependency on successful tokens list
        
        This method implements the new EMA scheduler flow that:
        1. Uses single optimized query with JOINs to get all necessary data
        2. Processes ALL active tokens automatically (no token filtering needed)
        3. Handles all three cases: available, not_available_ready, not_available_insufficient
        4. True batch processing for optimal performance
        5. Resilient to server downtime - no gaps in EMA calculations
        
        Returns:
            bool: True if all EMA processing succeeded
        """
        try:
            logger.info("Starting NEW EMA scheduler processing for all active tokens")
            
            # STEP 1: Get all EMA data in single optimized query
            emaStateAndCandleData = self.getAllEMAStateAndCandleData()
            
            if not emaStateAndCandleData:
                logger.info("No EMA data found for processing")
                return True
            
            # STEP 2: Process all EMA calculations in memory
            emaCandlesUpdatedData, emaStateUpdatedData = self.calculateEMAForAllRetrievedTokens(emaStateAndCandleData)
            
            # STEP 3: Save all results in batch
            success = self.recordCalculatedEMAForAllRetrievedTokens(emaCandlesUpdatedData, emaStateUpdatedData)
            
            if success:
                logger.info(f"NEW EMA scheduler processing completed: {len(emaCandlesUpdatedData)} EMA updates, {len(emaStateUpdatedData)} state updates")
            else:
                logger.warning("NEW EMA scheduler processing encountered errors")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in NEW EMA scheduler processing: {e}", exc_info=True)
            return False
    
    def getAllEMAStateAndCandleData(self) -> Dict[str, Dict]:
        """
        SINGLE OPTIMIZED QUERY: Get all EMA data with corresponding candles in one batch
        
        This method implements the new approach:
        1. JOIN emastates with trackedtokens to get only active tokens
        2. JOIN with timeframemetadata to get lastfetchedat for each timeframe
        3. JOIN with ohlcvdetails to get candles where unixtime > lastupdatedunix
        4. All in one highly optimized query for scalability
        
        Returns:
            Dict: {
                token_address: {
                    pair_id: pair_address,
                    ema21: {
                        timeframe: {
                            ema_value: current_ema_value,
                            last_updated_at: last_updated_unix,
                            status: ema_status,
                            ema_available_at: ema_available_time,
                            last_fetched_at: last_fetched_time,
                            candles: [list_of_candles]
                        }
                    },
                    ema34: { ... }
                }
            }
        """
        try:
            # Get all EMA data with candles in single optimized query
            emaDataWithCandles = self.trading_handler.getAllEMADataWithCandlesForScheduler()
            
            logger.info(f"Retrieved EMA data for {len(emaDataWithCandles)} tokens")
            return emaDataWithCandles
            
        except Exception as e:
            logger.error(f"Error getting EMA data with candles: {e}")
            return {}

    def calculateEMAForAllRetrievedTokens(self, emaDataWithCandles: Dict[str, Dict]) -> Tuple[List, List]:
        """
        Process all EMA calculations based on the organized data structure
        
        Args:
            emaDataWithCandles: Organized EMA data with candles from _getAllEMADataWithCandles
        
        Returns:
            Tuple[List, List]: (ema_candle_updates, ema_state_updates)
        """
        try:
            emaCandlesUpdatedData = []
            emaStateUpdatedData = []
            
            for tokenAddress, tokenData in emaDataWithCandles.items():
                pairAddress = tokenData['pair_id']
                
                # Process both EMA21 and EMA34
                for emaPeriod in self.EMA_PERIODS:
                    emaKey = f"ema{emaPeriod}"
                    emaData = tokenData.get(emaKey, {})
                    
                    if not emaData:
                        continue
                    
                    # Process each timeframe for this EMA period
                    for timeframe, timeframeData in emaData.items():
                        if not timeframeData:
                            continue
                        
                        # Extract data
                        emaValue = timeframeData.get(TradingHandlerConstants.EMAStates.EMA_VALUE)
                        lastUpdatedAt = timeframeData.get(TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX, 0)
                        status = timeframeData.get(TradingHandlerConstants.EMAStates.STATUS)
                        emaAvailableAt = timeframeData.get(TradingHandlerConstants.EMAStates.EMA_AVAILABLE_TIME, 0)
                        lastFetchedAt = timeframeData.get(TradingHandlerConstants.TimeframeMetadata.LAST_FETCHED_AT, 0)
                        candles = timeframeData.get(IndicatorConstants.EMAStates.CANDLES, [])
                        
                        if not candles:
                            continue
                        
                        # Determine action type based on status and data availability
                        emaCalculationType = self.findEMACalculationType(status, lastFetchedAt, emaAvailableAt)
                        
                        if emaCalculationType == EMACalculationType.NOT_AVAILABLE_INSUFFICIENT:
                            # Not enough candles yet, skip
                            continue
                        
                        # Calculate EMA based on action type
                        if emaCalculationType == EMACalculationType.NOT_AVAILABLE_READY:
                            # First calculation - calculate from scratch
                            emaCandleData, emaStateData = self.performFirstEMACalculation(
                                tokenAddress, pairAddress, timeframe, emaPeriod, candles, emaAvailableAt
                            )
                        elif emaCalculationType == EMACalculationType.AVAILABLE_UPDATE:
                            # Incremental update - use existing EMA value
                            emaCandleData, emaStateData = self.performIncrementalEMAUpdate(
                                tokenAddress, pairAddress, timeframe, emaPeriod, candles, emaValue, lastUpdatedAt
                            )
                        else:
                            continue
                        
                        # Collect results
                        if emaCandleData:
                            emaCandlesUpdatedData.extend(emaCandleData)
                        if emaStateData:
                            emaStateUpdatedData.append(emaStateData)
            
            logger.info(f"Processed EMA calculations: {len(emaCandlesUpdatedData)} candle updates, {len(emaStateUpdatedData)} state updates")
            return emaCandlesUpdatedData, emaStateUpdatedData
        
        except Exception as e:
            logger.error(f"Error processing EMA calculations: {e}")
            return [], []

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

    def performFirstEMACalculation(self, tokenAddress: str, pairAddress: str, timeframe: str, 
                                          emaPeriod: int, candles: List[Dict], emaAvailableAt: int) -> Tuple[Optional[List], Optional[Dict]]:
        """
        Perform first-time EMA calculation using pre-fetched candles
        """
        try:
            # Filter candles to only include those from emaAvailableAt onwards
            filteredCandles = [c for c in candles if c[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME] >= emaAvailableAt]
            
            if len(filteredCandles) < emaPeriod:
                logger.warning(f"Not enough candles for first EMA{emaPeriod} calculation: {tokenAddress} {timeframe}")
                return None, None
            
            # Use the shared EMA calculation method
            emaCandleData, emaStateData = self.calcualteFirstEMAFromCandles(
                filteredCandles, emaPeriod, tokenAddress, timeframe
            )
            
            return emaCandleData, emaStateData
            
        except Exception as e:
            logger.error(f"Error in first EMA calculation from data: {e}")
            return None, None
    
    def performIncrementalEMAUpdate(self, tokenAddress: str, pairAddress: str, timeframe: str,
                                           emaPeriod: int, candles: List[Dict], currentEMA: float, lastUpdatedAt: int) -> Tuple[Optional[List], Optional[Dict]]:
        """
        Perform incremental EMA update using existing EMA value and pre-fetched candles
        """
        try:
            # Filter candles to only include new ones after lastUpdatedAt
            newCandles = [c for c in candles if c[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME] > lastUpdatedAt]
            
            if not newCandles:
                logger.debug(f"No new candles for incremental EMA update: {tokenAddress} {timeframe}")
                return None, None
            
            emaCandleData = []
            latestUNIX = lastUpdatedAt
            currentEMAValue = currentEMA
            
            for candle in newCandles:
                currentEMAValue = self.calculateEMAValue(currentEMAValue, candle[IndicatorConstants.EMAStates.CANDLE_CLOSE_PRICE], emaPeriod)
                latestUNIX = candle[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME]
                
                # Add update record for this candle
                emaCandleData.append({
                    TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS: tokenAddress,
                    TradingHandlerConstants.OHLCVDetails.TIMEFRAME: timeframe,
                    IndicatorConstants.EMAStates.EMA_PERIOD: emaPeriod,
                    TradingHandlerConstants.OHLCVDetails.UNIX_TIME: candle[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME],
                    IndicatorConstants.EMAStates.EMA_VALUE: currentEMAValue
                })
            
            # Create state update
            emaStateData = {
                TradingHandlerConstants.EMAStates.TOKEN_ADDRESS: tokenAddress,
                TradingHandlerConstants.EMAStates.TIMEFRAME: timeframe,
                IndicatorConstants.EMAStates.EMA_PERIOD: emaPeriod,
                IndicatorConstants.EMAStates.EMA_VALUE: currentEMAValue,
                TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX: latestUNIX,
                TradingHandlerConstants.EMAStates.STATUS: EMAStatus.AVAILABLE
            }
            
            return emaCandleData, emaStateData
            
        except Exception as e:
            logger.error(f"Error in incremental EMA update from data: {e}")
            return None, None
     
   
    def recordCalculatedEMAForAllRetrievedTokens(self, emaCandlesUpdatedData: List, emaStateUpdatedData: List) -> bool:
        """
        STEP 4: Save all results in batch database update
        
        Returns:
            bool: Success status
        """
        return self.trading_handler.batchUpdateEMAData(emaCandlesUpdatedData, emaStateUpdatedData)
    

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
    
    def calcualteEMAForNewTokenFromAPI(self, tokenAddress: str, pairAddress: str, pairCreatedTime: int,
                                      allCandles: Dict[str, List[Dict]]) -> Dict:
        """
        API FLOW ONLY: Process EMA for new token addition using pre-loaded candles
        
        NOTE: This function exists for API flow because:
        - API flow has candles already loaded in memory (no need to fetch from DB)
        - Processes all timeframes and periods at once during token addition
        - Different from scheduler's incremental EMA updates which work with existing states
        """
        try:

            
            # Collect all EMA operations in memory
            batchEMAStateUpdatedData = []
            batchEMACandleUpdatedData = []
            
            # Process EMA for each available timeframe using pre-loaded data
            for timeframe, candles in allCandles.items():
                # FIXED: Get the latest fetched time from candles for this specific timeframe
                # instead of using current system time
                if not candles:
                    continue
                    
                latestFetchedAtTime = max(candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME] for candle in candles)
                
                for emaPeriod in [IndicatorConstants.EMAStates.EMA_21, IndicatorConstants.EMAStates.EMA_34]:    
                    # there is a problem, the current ema available time calculation works as next fetch time, paircreatedtime = 1757701876(12 September 2025 18:31:16), timeframe = 1h, ema =21, calculated value =1757775600(13 September 2025 15:00:00) - but initial ema 21 is calculated for the candle 13 September 2025 14:00:00 - but we here store the unix when that candle becomes available but we use this unix directly with the lastfetchedat time to check if we have enough data to calculate EMA - so ema calculation will have one candle delay
                    timeframeInSeconds = CommonUtil.getTimeframeSeconds(timeframe)
                    initialCandleStartTime = self.calculateInitialCandleStartTime(pairCreatedTime, timeframe)
                    emaAvailableTime = initialCandleStartTime + (emaPeriod * timeframeInSeconds)
                    
                    logger.info(f"Processing EMA{emaPeriod} state for {tokenAddress} {timeframe}: available at {emaAvailableTime}, latest fetched: {latestFetchedAtTime}")
                    
                    # Prepare EMA state data for batch operation
                    currentEMAStateData = {
                        TradingHandlerConstants.EMAStates.TOKEN_ADDRESS: tokenAddress,
                        TradingHandlerConstants.EMAStates.PAIR_ADDRESS: pairAddress,
                        TradingHandlerConstants.EMAStates.TIMEFRAME: timeframe,
                        TradingHandlerConstants.EMAStates.EMA_KEY: str(emaPeriod),
                        TradingHandlerConstants.EMAStates.PAIR_CREATED_TIME: pairCreatedTime,
                        TradingHandlerConstants.EMAStates.EMA_AVAILABLE_TIME: emaAvailableTime,
                        TradingHandlerConstants.EMAStates.EMA_VALUE: None,
                        TradingHandlerConstants.EMAStates.STATUS: EMAStatus.NOT_AVAILABLE,
                        TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX: None,
                        TradingHandlerConstants.EMAStates.NEXT_FETCH_TIME: None
                    }
                    
                    # FIXED: Check if we have enough data to calculate EMA using latest_fetched_time instead of current_time
                    # This ensures EMA calculation is based on actual data availability, not system time
                    if latestFetchedAtTime >= emaAvailableTime:
                        logger.info(f"EMA{emaPeriod} data available for {tokenAddress} {timeframe}, calculating...")

                        # Use the SHARED EMA calculation method (eliminates redundancy)
                        emaCandleUpdatedData, emaStateUpdatedData = self.calcualteFirstEMAFromCandles(
                            candles, emaPeriod, tokenAddress, timeframe
                        )

                        if emaCandleUpdatedData and emaStateUpdatedData:
                            # Calculate next fetch time
                            nextFetchTime = emaStateUpdatedData[TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX] + timeframeInSeconds

                            # Update EMA state data with calculated values
                            currentEMAStateData.update({
                                TradingHandlerConstants.EMAStates.EMA_VALUE: emaStateUpdatedData[TradingHandlerConstants.EMAStates.EMA_VALUE],
                                TradingHandlerConstants.EMAStates.STATUS: EMAStatus.AVAILABLE,
                                TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX: emaStateUpdatedData[TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX],
                                TradingHandlerConstants.EMAStates.NEXT_FETCH_TIME: nextFetchTime
                            })
                            
                            # Collect EMA candle updates for batch operation
                            for candle in emaCandleUpdatedData:
                                batchEMACandleUpdatedData.append({
                                    TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS: tokenAddress,
                                    TradingHandlerConstants.OHLCVDetails.TIMEFRAME: timeframe,
                                    IndicatorConstants.EMAStates.EMA_PERIOD: emaPeriod, 
                                    TradingHandlerConstants.OHLCVDetails.UNIX_TIME: candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME],
                                    IndicatorConstants.EMAStates.EMA_VALUE: candle[IndicatorConstants.EMAStates.EMA_VALUE]
                                })

                            logger.info(f"EMA{emaPeriod} calculated for {tokenAddress} {timeframe}: final value {emaStateUpdatedData[IndicatorConstants.EMAStates.EMA_VALUE]}")
                        else:
                            logger.warning(f"Failed to calculate EMA{emaPeriod} for {tokenAddress} {timeframe}: calculation returned None")
                    else:
                        logger.info(f"EMA{emaPeriod} not yet available for {tokenAddress} {timeframe} - need data up to {emaAvailableTime}, have up to {latestFetchedAtTime}")
                    
                    # Add to batch operations
                    batchEMAStateUpdatedData.append(currentEMAStateData)
            
            # update EMA  
            self.trading_handler.updateEMA(batchEMAStateUpdatedData, batchEMACandleUpdatedData)
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error processing EMA with preloaded candles: {e}")
            return {'success': False, 'error': str(e)}
    
    def calcualteFirstEMAFromCandles(self, candles: List[Dict], emaPeriod: int,
                               token_address: str = "", timeframe: str = "") -> Tuple[Optional[List], Optional[Dict]]:
        """
        SHARED METHOD: Calculate EMA from any candle data source (pre-loaded or DB-fetched)

        CORRECT EMA LOGIC:
        - For EMA21: Calculate SMA using first 21 candles for the 21st candle
        - For EMA34: Calculate SMA using first 34 candles for the 34th candle
        - Then calculate EMA for all subsequent candles using standard EMA formula
        - This matches the emaavailabletime logic that ensures we have enough candles

        Args:
            candles: List of candle dictionaries with 'closeprice' and 'unixtime' keys
            ema_period: EMA period (21 or 34)
            token_address: Token address (for logging)
            timeframe: Timeframe (for logging)

        Returns:
            Tuple[Optional[List], Optional[Dict]]: (ema_value_updates, state_update)
        """
        try:
            if len(candles) < emaPeriod:
                logger.warning(f"Not enough candles for EMA{emaPeriod} calculation: {token_address} {timeframe}")
                return None, None

            emaCandleUpdatedData = []
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
                    sma = sum(float(candles[j][TradingHandlerConstants.OHLCVDetails.CLOSE_PRICE]) for j in range(i + 1)) / (i + 1)
                    currentEMA = sma
                else:
                    # Subsequent candles: Calculate EMA using previous EMA value
                    # For EMA21: 22nd+ candles use standard EMA formula
                    currentEMA = self.calculateEMAValue(currentEMA, candle[TradingHandlerConstants.OHLCVDetails.CLOSE_PRICE], emaPeriod)

                latestUNIX = candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]

                # Add update record for this candle (starting from emaPeriod-th candle)
                emaCandleUpdatedData.append({
                    TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS: token_address,
                    TradingHandlerConstants.OHLCVDetails.TIMEFRAME: timeframe,
                    IndicatorConstants.EMAStates.EMA_PERIOD: emaPeriod,
                    TradingHandlerConstants.OHLCVDetails.UNIX_TIME: candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME],
                    IndicatorConstants.EMAStates.EMA_VALUE: currentEMA
                })

            if not emaCandleUpdatedData:
                logger.warning(f"No EMA values calculated for {token_address} {timeframe} EMA{emaPeriod}")
                return None, None

            # Create state update
            emaStateUpdatedData = {
                TradingHandlerConstants.EMAStates.TOKEN_ADDRESS: token_address,
                TradingHandlerConstants.EMAStates.TIMEFRAME: timeframe,
                IndicatorConstants.EMAStates.EMA_PERIOD: emaPeriod,
                IndicatorConstants.EMAStates.EMA_VALUE: currentEMA,
                TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX: latestUNIX,
                TradingHandlerConstants.EMAStates.STATUS: EMAStatus.AVAILABLE
            }

            return emaCandleUpdatedData, emaStateUpdatedData

        except Exception as e:
            logger.error(f"Error in shared EMA calculation: {e}")
            return None, None


    def setEMAForOldTokenFromAPI(self, tokenAddress: str, pairAddress: str, 
                                                               pairCreatedTime: int, perTimeframeEMAData: Dict,
                                                               allCandles: Dict[str, List[Dict]]) -> Dict:
        """
        API FLOW: Process EMA for old token using per-timeframe reference times and EMA values
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Pair contract address  
            pairCreatedTime: When the pair was created (unix timestamp)
            perTimeframeEMAData: Dict with structure:
                {
                    "15m": {"ema21": {"value": 1.23, "referenceTime": 1234567890}, ...},
                    "1h": {"ema21": {"value": 4.56, "referenceTime": 1234567890}, ...},
                    "4h": {"ema21": {"value": 7.89, "referenceTime": 1234567890}, ...}
                }
            all_candles_data: Pre-loaded candles for all timeframes
            
        Returns:
            Dict with success status
        """
        try:
            logger.info(f"Processing per-timeframe EMA for old token {tokenAddress}")
            
            # Collections for batch operations
            emaStateUpdatedData = []
            emaCandleUpdatedData = []
            
            # Process each timeframe that has both candle data and user-provided EMA data
            for timeframe in allCandles.keys():
                if timeframe not in perTimeframeEMAData: # example: {"15m": {"ema21": {"value": 1.23, "referenceTime": 1234567890}, "ema34": {"value": 2.34, "referenceTime": 1234567890}}, "1h": {"ema21": {"value": 4.56, "referenceTime": 1234567890}, "ema34": {"value": 5.67, "referenceTime": 1234567890}}, "4h": {"ema21": {"value": 7.89, "referenceTime": 1234567890}, "ema34": {"value": 8.90, "referenceTime": 1234567890}}}
                    logger.debug(f"Skipping {timeframe} - no EMA data provided by user")
                    continue
                    
                candles = allCandles[timeframe]
                timeframeEMAData = perTimeframeEMAData[timeframe] # example: {"ema21": {"value": 1.23, "referenceTime": 1234567890}, "ema34": {"value": 2.34, "referenceTime": 1234567890}}
                
                logger.info(f"Processing timeframe {timeframe} with {len(candles)} candles")
                
                # Process both EMA21 and EMA34 for this timeframe
                for emaPeriod in [IndicatorConstants.EMAStates.EMA_21, IndicatorConstants.EMAStates.EMA_34]:
                    emaKey = f"ema{emaPeriod}" # example: "ema21"
                    
                    if emaKey not in timeframeEMAData:
                        logger.debug(f"Skipping EMA{emaPeriod} for {timeframe} - no user value provided")
                        continue
                    
                    emaInfo = timeframeEMAData[emaKey] # example: {"ema21": {"value": 1.23, "referenceTime": 1234567890}}
                    emaValue = Decimal(str(emaInfo[TradingAPIConstants.RequestParameters.VALUE])) # example: 1.23
                    emaTime = emaInfo[TradingAPIConstants.RequestParameters.REFERENCE_TIME] # example: 1234567890
                    
                    logger.info(f"Setting EMA{emaPeriod} for {tokenAddress} {timeframe}: value={emaValue} at timestamp {emaTime}")
                    
                    # Prepare EMA state record
                    emaStateData = TradingActionUtil.collectDataForEMAStateQueryFromAPI(
                        tokenAddress, pairAddress, timeframe, emaPeriod, emaValue,
                        pairCreatedTime, emaTime, EMAStatus.AVAILABLE
                    )
                    emaStateUpdatedData.append(emaStateData)
                    
                    # Find the reference candle and prepare candle update
                    userEnteredEMACandle = next((c for c in candles if c[TradingHandlerConstants.OHLCVDetails.UNIX_TIME] == emaTime), None)
                    if userEnteredEMACandle:
                        emaCandleUpdate = TradingActionUtil.collectDataForEMACandleUpdateQueryFromAPI(
                            tokenAddress, timeframe, emaPeriod, emaTime, emaValue
                        )
                        emaCandleUpdatedData.append(emaCandleUpdate)
                        logger.info(f"Prepared EMA{emaPeriod} candle update for {tokenAddress} {timeframe} at {emaTime}")
                    else:
                        logger.warning(f"Reference candle at timestamp {emaTime} not found for {tokenAddress} {timeframe} - skipping EMA{emaPeriod} candle update")
            
            # update EMA  
            logger.info(f"Executing per-timeframe EMA operations: {len(emaStateUpdatedData)} state records, {len(emaCandleUpdatedData)} candle updates")
            self.trading_handler.updateEMA(emaStateUpdatedData, emaCandleUpdatedData)
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error processing per-timeframe EMA for old token: {e}")
            return {'success': False, 'error': str(e)}
    
