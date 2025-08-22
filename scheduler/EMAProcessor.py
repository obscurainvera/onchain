"""
EMAProcessor - Handles EMA calculations and state management for scheduler

STEP-BY-STEP EMA CALCULATION PROCESS:
===================================

EXAMPLE: Token XYZ, 15m timeframe, EMA21, Current time: 14:00

STEP 1: Status Detection
- Check emastates table for XYZ-15m-21 record
- Get lastfetchedat from timeframemetadata table for XYZ-15m
- Compare: lastfetchedat vs emaavailabletime

STEP 2: Determine Action
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
  → Get NEW candles after lastupdatedunix from ohlcvdetails
  → Calculate incremental EMA using existing emavalue as starting point
  → Update ohlcvdetails with new EMA values
  → Update emastates with latest emavalue and lastupdatedunix

STEP 3: EMA Calculation Formula
- First 20 candles: No EMA (not enough data)
- 21st candle: EMA = SMA of first 21 candles
- 22nd+ candles: EMA = (Close × Multiplier) + (Previous_EMA × (1 - Multiplier))
- Where Multiplier = 2 / (Period + 1)

STEP 4: Database Updates (ATOMIC)
- Update ohlcvdetails.ema21value/ema34value for processed candles
- Update emastates with new emavalue, lastupdatedunix, nextfetchtime
- All operations in single transaction
"""

from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal
from logs.logger import get_logger
from database.trading.TradingHandler import TradingHandler, EMAStatus
from scheduler.SchedulerConstants import Timeframes
from actions.TradingActionUtil import TradingActionUtil
import time

logger = get_logger(__name__)

class EMAActionResult:
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
        self.TIMEFRAMES = [Timeframes.FIFTEEN_MIN, Timeframes.ONE_HOUR, Timeframes.FOUR_HOUR]
    
    def processEMAForAllTokens(self, successfulTokensList: List[Dict[str, Any]]) -> bool:
        """
        PRODUCTION-READY: Process EMA calculations with TRUE batch optimization
        
        MODULAR DESIGN:
        - _fetchBatchMetadata(): Get all EMA states and fetch times
        - _analyzeEMAOperations(): Determine what operations to perform
        - _calculateAllEMAValues(): Process all calculations in memory
        - _saveBatchEMAResults(): Batch update database with results
        
        Total DB queries: 6 (regardless of token count)
        """
        try:
            if not successfulTokensList:
                return True
                
            logger.info(f"Processing EMA for {len(successfulTokensList)} tokens across {len(self.TIMEFRAMES)} timeframes")
            
            # STEP 1: Fetch all metadata in batch
            allEMAStates, allLastFetchedAtTimes = self.fetchEMAStateAndLastFetchedAtTimes(successfulTokensList)
            
            # STEP 2: Analyze operations and prepare candle requirements
            infoNeededToCalculateEMA, emaActionType = self._analyzeEMAOperations(successfulTokensList, allEMAStates, allLastFetchedAtTimes)
            
            # STEP 3: Fetch all required candles and calculate EMAs
            emaCandlesUpdatedData, emaStateUpdatedData = self._calculateAllEMAValues(infoNeededToCalculateEMA, emaActionType)
            
            # STEP 4: Save all results in batch
            success = self._saveBatchEMAResults(emaCandlesUpdatedData, emaStateUpdatedData)
            
            logger.info(f"EMA processing completed: {len(emaCandlesUpdatedData)} EMA updates, {len(emaStateUpdatedData)} state updates")
            return success
            
        except Exception as e:
            logger.error(f"Error in batch EMA processing: {e}")
            return False
    
    def fetchEMAStateAndLastFetchedAtTimes(self, successfulTokensList: List[Dict[str, Any]]) -> Tuple[Dict, Dict]:
        """
        STEP 1: Fetch all metadata in 2 batch queries
        
        Returns:
            Tuple[Dict, Dict]: (ema_states, fetch_times)
        """
        tokenAddresses = [token['tokenaddress'] for token in successfulTokensList]
        
        allEMAState = self.trading_handler.getAllEMAStateInfo(tokenAddresses, self.TIMEFRAMES, self.EMA_PERIODS)
        allLastFetchedAtTimes = self.trading_handler.getAllLastFetchTimes(tokenAddresses, self.TIMEFRAMES)
        
        return allEMAState, allLastFetchedAtTimes
    
    def _analyzeEMAOperations(self, successfulTokensList: List[Dict[str, Any]], 
                             allEMAStates: Dict, allLastFetchedAtTimes: Dict) -> Tuple[List[Dict], Dict]:
        """
        STEP 2: Analyze all operations and prepare candle requirements
        
        Returns:
            Tuple[List[Dict], Dict]: (ema_operations, operation_map)
        """
        emaOperations = []
        operationMap = {}
        
        for token in successfulTokensList:
            for timeframe in self.TIMEFRAMES:
                for emaPeriod in self.EMA_PERIODS:
                    emaState = allEMAStates.get(token['tokenaddress'], {}).get(timeframe, {}).get(emaPeriod)
                    lastFetchedAtTime = allLastFetchedAtTimes.get(token['tokenaddress'], {}).get(timeframe)
                    
                    if not lastFetchedAtTime or not emaState:
                        continue
                    
                    actionType = self._determineEMAAction(emaState, lastFetchedAtTime)
                    
                    if actionType == EMAActionResult.NOT_AVAILABLE_INSUFFICIENT:
                        continue
                    
                    operationId = f"{token['tokenaddress']}_{timeframe}_{emaPeriod}_{actionType}"
                    operationMap[operationId] = (token, timeframe, emaPeriod, actionType, emaState)
                    
                    if actionType == EMAActionResult.NOT_AVAILABLE_READY:
                        emaOperations.append({
                            'token_address': token['tokenaddress'],
                            'timeframe': timeframe,
                            'ema_period': emaPeriod,
                            'operation_type': 'first_calculation',
                            'from_time': emaState['emaavailabletime'],
                            'operation_id': operationId
                        })
                    elif actionType == EMAActionResult.AVAILABLE_UPDATE:
                        emaOperations.append({
                            'token_address': token['tokenaddress'],
                            'timeframe': timeframe,
                            'ema_period': emaPeriod,
                            'operation_type': 'incremental_update',
                            'from_time': emaState['lastupdatedunix'],
                            'operation_id': operationId,
                            'current_ema': emaState['emavalue']
                        })
        
        return emaOperations, operationMap
    
    def _calculateAllEMAValues(self, infoNeededToCalculateEMA: List[Dict], emaActionType: Dict) -> Tuple[List, List]:
        """
        STEP 3: Fetch all required candles and calculate EMAs in memory
        
        Returns:
            Tuple[List, List]: (ema_updates, state_updates)
        """
        # Fetch ALL candles in single UNION query
        candlesNeededToCalculateEMA = self.trading_handler.getAllCandlesNeededToCalculateEMA(infoNeededToCalculateEMA)
        
        emaCandlesUpdatedData = []
        emaStateUpdatedData = []
        
        for operation in infoNeededToCalculateEMA:
            operationId = operation['operation_id']
            candles = candlesNeededToCalculateEMA.get(operationId, [])
            
            if not candles:
                continue
            
            # Calculate EMA values for this operation
            emaCandleData, emaStateData = self.calculateEMA(operation, candles)
            
            if emaCandleData:
                emaCandlesUpdatedData.extend(emaCandleData)
            if emaStateData:
                emaStateUpdatedData.append(emaStateData)
        
        return emaCandlesUpdatedData, emaStateUpdatedData
    
    def _saveBatchEMAResults(self, emaCandlesUpdatedData: List, emaStateUpdatedData: List) -> bool:
        """
        STEP 4: Save all results in batch database update
        
        Returns:
            bool: Success status
        """
        return self.trading_handler.batchUpdateEMAData(emaCandlesUpdatedData, emaStateUpdatedData)
    
    def calculateEMA(self, operation: Dict, candles: List[Dict]) -> Tuple[Optional[List], Optional[Dict]]:
        """
        PRODUCTION-READY: Calculate EMA for specific operation using pre-fetched candles
        
        Args:
            operation: Dict with operation details (token_address, timeframe, ema_period, etc.)
            candles: Pre-fetched candles for this operation
            
        Returns:
            Tuple[Optional[List], Optional[Dict]]: (ema_value_updates, state_update)
        """
        try:
            tokenAddress = operation['token_address']
            timeframe = operation['timeframe']
            operationType = operation['operation_type']
            
            if not candles:
                logger.debug(f"No candles available for EMA calculation: {tokenAddress}_{timeframe}")
                return None, None
            
            # Sort candles by unixtime to ensure correct calculation order
            sortedCandles = sorted(candles, key=lambda x: x['unixtime'])
            
            if operationType == 'first_calculation':
                return self._performFirstEMACalculation(operation, sortedCandles)
            elif operationType == 'incremental_update':   
                return self._performIncrementalEMAUpdate(operation, sortedCandles)
            else:
                logger.error(f"Unknown operation type: {operationType}")
                return None, None
                
        except Exception as e:
            logger.error(f"Error in EMA calculation for operation: {e}")
            return None, None
    
    def _performFirstEMACalculation(self, operation: Dict, candles: List[Dict]) -> Tuple[Optional[List], Optional[Dict]]:
        """
        Perform first-time EMA calculation using SMA initialization
        
        CORRECT EMA LOGIC:
        - For EMA21: Calculate SMA using first 20 candles for the 20th candle
        - For EMA34: Calculate SMA using first 33 candles for the 33rd candle
        - Then calculate EMA for all subsequent candles using standard EMA formula
        - This matches the emaavailabletime logic that ensures we have enough candles
        """
        try:
            tokenAddress = operation['token_address']
            timeframe = operation['timeframe']
            emaPeriod = operation['ema_period']

            # Use the SHARED EMA calculation method (eliminates redundancy)
            emaCandleUpdatedData, emaStateUpdatedData = self.calculateEMAFromCandles(
                candles, emaPeriod, tokenAddress, timeframe
            )

            return emaCandleUpdatedData, emaStateUpdatedData
            
        except Exception as e:
            logger.error(f"Error in first EMA calculation: {e}")
            return None, None
    
    def _performIncrementalEMAUpdate(self, operation: Dict, candles: List[Dict]) -> Tuple[Optional[List], Optional[Dict]]:
        """
        Perform incremental EMA update using existing EMA value
        """
        try:
            tokenAddress = operation['token_address']
            timeframe = operation['timeframe']
            emaPeriod = operation['ema_period']
            currentEMA = operation['current_ema']
            fromTime = operation['from_time']
            
            # Process only new candles after last processed time
            newCandlesThatNeedsToBeProcessed = [c for c in candles if c['unixtime'] > fromTime]
            
            if not newCandlesThatNeedsToBeProcessed:
                logger.debug(f"No new candles for incremental EMA update: {tokenAddress}_{timeframe}")
                return None, None
            
            emaCandleUpdatedData = []
            latestUNIX = fromTime
            
            for candle in newCandlesThatNeedsToBeProcessed:
                currentEMA = self._calculateSingleEMAValue(currentEMA, candle['closeprice'], emaPeriod)
                latestUNIX = candle['unixtime']
                
                # Add update record for this candle
                emaCandleUpdatedData.append({
                    'token_address': tokenAddress,
                    'timeframe': timeframe,
                    'ema_period': emaPeriod,
                    'unixtime': candle['unixtime'],
                    'ema_value': currentEMA
                })
            
            # Create state update
            emaStateUpdatedData = {
                'token_address': tokenAddress,
                'timeframe': timeframe,
                'ema_period': emaPeriod,
                'ema_value': currentEMA,
                'last_updated_unix': latestUNIX,
                'status': EMAStatus.AVAILABLE
            }
            
            return emaCandleUpdatedData, emaStateUpdatedData
            
        except Exception as e:
            logger.error(f"Error in incremental EMA update: {e}")
            return None, None
    
    def _calculateSingleEMAValue(self, previousEMA: float, currentPrice: float, period: int) -> float:
        """
        Calculate single EMA value using standard EMA formula
        EMA = (Close - Previous_EMA) * (2 / (Period + 1)) + Previous_EMA
        """
        multiplier = 2.0 / (period + 1)
        return (currentPrice - previousEMA) * multiplier + previousEMA
    
    def _determineEMAAction(self, emaState: Dict, lastFetchedAtTime: int) -> str:
        """
        CRITICAL LOGIC: Determine what EMA action to take based on state and data availability
        
        Args:
            ema_state: EMA state info from database
            last_fetch_time: Last time candles were fetched for this token-timeframe
            
        Returns:
            - NOT_AVAILABLE_INSUFFICIENT: Not enough candles yet (lastfetchedat < emaavailabletime)
            - NOT_AVAILABLE_READY: Ready for first calculation (lastfetchedat >= emaavailabletime)
            - AVAILABLE_UPDATE: EMA exists, update with new candles (status = AVAILABLE)
        """
        status = emaState['status']
        emaAvailableTime = emaState['emaavailabletime']
        
        if status == EMAStatus.NOT_AVAILABLE:
            if lastFetchedAtTime >= emaAvailableTime:
                return EMAActionResult.NOT_AVAILABLE_READY
            else:
                return EMAActionResult.NOT_AVAILABLE_INSUFFICIENT
        elif status == EMAStatus.AVAILABLE:
            return EMAActionResult.AVAILABLE_UPDATE
        
        # Fallback for unknown status
        return EMAActionResult.NOT_AVAILABLE_INSUFFICIENT
    
   
    
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
            from actions.TradingActionUtil import TradingActionUtil
            
            # Collect all EMA operations in memory
            batchEMAStateUpdatedData = []
            batchEMACandleUpdatedData = []
            
            # Process EMA for each available timeframe using pre-loaded data
            for timeframe, candles in allCandles.items():
                # FIXED: Get the latest fetched time from candles for this specific timeframe
                # instead of using current system time
                if not candles:
                    continue
                    
                latestFetchedAtTime = max(candle['unixtime'] for candle in candles)
                
                for emaPeriod in [21, 34]:
                    # Calculate when EMA becomes available: pairCreatedTime + (ema_period-1) * timeframe_seconds
                    timeframeInSeconds = TradingActionUtil.getTimeframeSeconds(timeframe)
                    emaAvailableTime = pairCreatedTime + ((emaPeriod - 1) * timeframeInSeconds)
                    
                    logger.info(f"Processing EMA{emaPeriod} state for {tokenAddress} {timeframe}: available at {emaAvailableTime}, latest fetched: {latestFetchedAtTime}")
                    
                    # Prepare EMA state data for batch operation
                    currentEMAStateData = {
                        'tokenAddress': tokenAddress,
                        'pairAddress': pairAddress,
                        'timeframe': timeframe,
                        'emaKey': str(emaPeriod),
                        'pairCreatedTime': pairCreatedTime,
                        'emaAvailableTime': emaAvailableTime,
                        'emaValue': None,
                        'status': EMAStatus.NOT_AVAILABLE,
                        'lastUpdatedUnix': None,
                        'nextFetchTime': None
                    }
                    
                    # FIXED: Check if we have enough data to calculate EMA using latest_fetched_time instead of current_time
                    # This ensures EMA calculation is based on actual data availability, not system time
                    if latestFetchedAtTime >= emaAvailableTime:
                        logger.info(f"EMA{emaPeriod} data available for {tokenAddress} {timeframe}, calculating...")

                        # Use the SHARED EMA calculation method (eliminates redundancy)
                        ema_candle_data, ema_state_data = self.calculateEMAFromCandles(
                            candles, emaPeriod, tokenAddress, timeframe
                        )

                        if ema_candle_data and ema_state_data:
                            # Calculate next fetch time
                            next_fetch_time = ema_state_data['last_updated_unix'] + timeframeInSeconds

                            # Update EMA state data with calculated values
                            currentEMAStateData.update({
                                'emaValue': ema_state_data['ema_value'],
                                'status': EMAStatus.AVAILABLE,
                                'lastUpdatedUnix': ema_state_data['last_updated_unix'],
                                'nextFetchTime': next_fetch_time
                            })
                            
                            # Collect EMA candle updates for batch operation
                            for candle_update in ema_candle_data:
                                batchEMACandleUpdatedData.append({
                                    'tokenAddress': tokenAddress,
                                    'timeframe': timeframe,
                                    'ema_period': emaPeriod,
                                    'unixtime': candle_update['unixtime'],
                                    'ema_value': candle_update['ema_value']
                                })

                            logger.info(f"EMA{emaPeriod} calculated for {tokenAddress} {timeframe}: final value {ema_state_data['ema_value']}")
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
    
    def calculateEMAFromCandles(self, candles: List[Dict], ema_period: int,
                               token_address: str = "", timeframe: str = "") -> Tuple[Optional[List], Optional[Dict]]:
        """
        SHARED METHOD: Calculate EMA from any candle data source (pre-loaded or DB-fetched)

        CORRECT EMA LOGIC:
        - For EMA21: Calculate SMA using first 20 candles for the 20th candle
        - For EMA34: Calculate SMA using first 33 candles for the 33rd candle
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
            if len(candles) < ema_period:
                logger.warning(f"Not enough candles for EMA{ema_period} calculation: {token_address} {timeframe}")
                return None, None

            ema_candle_updated_data = []
            current_ema = None
            latest_unix = 0

            for i, candle in enumerate(candles):
                if i < ema_period - 1:
                    # Skip first (emaPeriod-1) candles - no EMA value yet
                    # For EMA21: Skip first 19 candles (index 0-18)
                    continue
                elif i == ema_period - 1:
                    # (emaPeriod-1)th candle: Calculate SMA as initial EMA value
                    # For EMA21: 20th candle (index 19) gets SMA of first 20 candles
                    sma = sum(candles[j]['closeprice'] for j in range(i + 1)) / (i + 1)
                    current_ema = sma
                else:
                    # Subsequent candles: Calculate EMA using previous EMA value
                    # For EMA21: 21st+ candles use standard EMA formula
                    current_ema = self._calculateSingleEMAValue(current_ema, candle['closeprice'], ema_period)

                latest_unix = candle['unixtime']

                # Add update record for this candle (starting from (emaPeriod-1)th candle)
                ema_candle_updated_data.append({
                    'token_address': token_address,
                    'timeframe': timeframe,
                    'ema_period': ema_period,
                    'unixtime': candle['unixtime'],
                    'ema_value': current_ema
                })

            if not ema_candle_updated_data:
                logger.warning(f"No EMA values calculated for {token_address} {timeframe} EMA{ema_period}")
                return None, None

            # Create state update
            ema_state_updated_data = {
                'token_address': token_address,
                'timeframe': timeframe,
                'ema_period': ema_period,
                'ema_value': current_ema,
                'last_updated_unix': latest_unix,
                'status': EMAStatus.AVAILABLE
            }

            return ema_candle_updated_data, ema_state_updated_data

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
                for emaPeriod in [21, 34]:
                    emaKey = f"ema{emaPeriod}" # example: "ema21"
                    
                    if emaKey not in timeframeEMAData:
                        logger.debug(f"Skipping EMA{emaPeriod} for {timeframe} - no user value provided")
                        continue
                    
                    emaInfo = timeframeEMAData[emaKey] # example: {"ema21": {"value": 1.23, "referenceTime": 1234567890}}
                    emaValue = Decimal(str(emaInfo["value"])) # example: 1.23
                    emaTime = emaInfo["referenceTime"] # example: 1234567890
                    
                    logger.info(f"Setting EMA{emaPeriod} for {tokenAddress} {timeframe}: value={emaValue} at timestamp {emaTime}")
                    
                    # Calculate timing for next EMA fetch
                    timeframeInSeconds = TradingActionUtil.getTimeframeSeconds(timeframe)
                    next_fetch_time = emaTime + timeframeInSeconds
                    
                    # Prepare EMA state record
                    emaStateData = TradingActionUtil.collectDataForEMAStateQueryFromAPI(
                        tokenAddress, pairAddress, timeframe, emaPeriod, emaValue,
                        pairCreatedTime, emaTime, EMAStatus.AVAILABLE
                    )
                    emaStateUpdatedData.append(emaStateData)
                    
                    # Find the reference candle and prepare candle update
                    userEnteredEMACandle = next((c for c in candles if c['unixtime'] == emaTime), None)
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
    
