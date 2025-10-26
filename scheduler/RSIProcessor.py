"""
RSIProcessor - Handles RSI (Relative Strength Index) and Stochastic RSI calculations

TECHNICAL IMPLEMENTATION:
========================

RSI Calculation (Wilder's Smoothing):
1. Calculate price changes (gains/losses)
2. Initial averages: SMA of first 14 gains/losses
3. Subsequent: Wilder's smoothing = (Previous Avg × 13 + New Value) / 14
4. RSI = 100 - (100 / (1 + RS)) where RS = Avg Gain / Avg Loss
5. Returns: 0-100 range

Stochastic RSI Calculation:
1. Requires 14 RSI values
2. Stoch RSI = ((Current RSI - Lowest RSI) / (Highest RSI - Lowest RSI)) × 100
3. Returns: 0-100 range

%K Calculation (Standard SMA):
1. Requires 3 Stochastic RSI values
2. %K = SMA(Stochastic RSI, 3 periods)
3. Returns: 0-100 range

%D Calculation (Signal Line):
1. Requires 3 %K values
2. %D = SMA(%K, 3 periods)
3. Returns: 0-100 range

VALUE RANGES (All use 0-100):
- RSI: 0-100
- Stochastic RSI: 0-100
- %K: 0-100
- %D: 0-100

Minimum Candles Required:
- RSI: 15 candles (14 changes + 1)
- Stoch RSI: 28 candles (15 for first RSI + 13 more)
- %K: 31 candles (28 for Stoch RSI + 3 more for SMA)
- %D: 34 candles (31 for %K + 3 more for SMA)
"""

from typing import List, Optional, Tuple, TYPE_CHECKING
from logs.logger import get_logger
from utils.CommonUtil import CommonUtil
from api.trading.request.RSIState import RSIState

if TYPE_CHECKING:
    from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails

logger = get_logger(__name__)


class RSIStatus:
    """RSI status constants"""
    NOT_AVAILABLE = 1
    AVAILABLE = 2


class RSICalculationType:
    """RSI calculation type constants"""
    NOT_AVAILABLE_INSUFFICIENT = 'not_available_insufficient'  # Not enough candles yet
    NOT_AVAILABLE_READY = 'not_available_ready'                # Ready for first calculation
    AVAILABLE_UPDATE = 'available_update'                      # Update existing RSI


class RSIProcessor:
    """
    Production-ready RSI processor with Wilder's smoothing and Stochastic RSI
    Matches DexScreener's calculations exactly with progressive averaging
    """
    
    def __init__(self, trading_handler=None):
        self.trading_handler = trading_handler
        self.RSI_INTERVAL = 14
        self.STOCH_RSI_INTERVAL = 14
        self.K_INTERVAL = 3
        self.D_INTERVAL = 3
    
    def calculateRSIForAllTrackedTokens(self, trackedTokens: List['TrackedToken']) -> None:
        """
        Calculate RSI indicators for all tracked tokens (called from scheduler)
        
        Args:
            trackedTokens: List of TrackedToken POJOs with candles and RSI state
        """
        try:
            totalProcessed = 0
            
            for trackedToken in trackedTokens:
                for timeframeRecord in trackedToken.timeframeRecords:
                    rsiState = timeframeRecord.rsiState
                    
                    if not rsiState:
                        continue
                    
                    # Determine calculation type
                    rsiCalculationType = self.findRSICalculationType(
                        rsiState.status,
                        timeframeRecord.lastFetchedAt or 0,
                        rsiState.rsiAvailableTime or 0
                    )
                    
                    if rsiCalculationType == RSICalculationType.NOT_AVAILABLE_INSUFFICIENT:
                        logger.debug(f"Insufficient data for RSI: {trackedToken.symbol} {timeframeRecord.timeframe}")
                        continue
                    
                    # Calculate RSI based on type
                    if rsiCalculationType == RSICalculationType.NOT_AVAILABLE_READY:
                        # First calculation - calculate from scratch
                        self.performFirstRSICalculation(
                            timeframeRecord, trackedToken.tokenAddress,
                            trackedToken.pairAddress, rsiState.rsiAvailableTime or 0
                        )
                    elif rsiCalculationType == RSICalculationType.AVAILABLE_UPDATE:
                        # Incremental update - use existing RSI state
                        self.performIncrementalRSIUpdate(
                            timeframeRecord, trackedToken.tokenAddress,
                            trackedToken.pairAddress
                        )
                    
                    totalProcessed += 1
            
            logger.info(f"Processed RSI calculations for {totalProcessed} timeframe records")
        
        except Exception as e:
            logger.info(f"Error processing RSI calculations: {e}", exc_info=True)
    
    def findRSICalculationType(self, status: int, lastFetchedAt: int, rsiAvailableAt: int) -> str:
        """
        Determine RSI calculation type based on status and data availability
        
        Args:
            status: RSI status (1=NOT_AVAILABLE, 2=AVAILABLE)
            lastFetchedAt: Last time candles were fetched
            rsiAvailableAt: When RSI becomes available
            
        Returns:
            RSI calculation type constant
        """
        if status == RSIStatus.NOT_AVAILABLE:
            if lastFetchedAt >= rsiAvailableAt:
                return RSICalculationType.NOT_AVAILABLE_READY
            else:
                return RSICalculationType.NOT_AVAILABLE_INSUFFICIENT
        elif status == RSIStatus.AVAILABLE:
            return RSICalculationType.AVAILABLE_UPDATE
        
        return RSICalculationType.NOT_AVAILABLE_INSUFFICIENT
    
    def performFirstRSICalculation(self, timeframeRecord: 'TimeframeRecord',
                                   tokenAddress: str, pairAddress: str,
                                   rsiAvailableAt: int) -> None:
        """
        Perform first-time RSI calculation from scratch
        
        Args:
            timeframeRecord: TimeframeRecord POJO with candles
            tokenAddress: Token address
            pairAddress: Pair address
            rsiAvailableAt: When RSI becomes available
        """
        try:
            candles = timeframeRecord.ohlcvDetails
            if len(candles) < self.RSI_INTERVAL + 1:
                logger.warning(f"Not enough candles for first RSI calculation: {tokenAddress} {timeframeRecord.timeframe}")
                return
            
            # Calculate first RSI using all candles
            timeframeInSeconds = CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
            success = self.calculateFirstRSIFromCandles(
                timeframeRecord, tokenAddress, pairAddress,
                timeframeRecord.timeframe, rsiAvailableAt,
                timeframeRecord.rsiState.pairCreatedTime or 0, timeframeInSeconds
            )
            
            if success:
                logger.info(f"First RSI calculation completed for {tokenAddress} {timeframeRecord.timeframe}")
            else:
                logger.warning(f"First RSI calculation failed for {tokenAddress} {timeframeRecord.timeframe}")
        
        except Exception as e:
            logger.info(f"Error in first RSI calculation: {e}", exc_info=True)
    
    def performIncrementalRSIUpdate(self, timeframeRecord: 'TimeframeRecord',
                                    tokenAddress: str, pairAddress: str) -> None:
        """
        Perform incremental RSI update using existing state with modular flows
        
        This reuses the same modular flows as first calculation:
        - RSI Flow
        - Stochastic RSI Flow (via processStochasticRSI helper)
        - %K Flow (via processStochasticRSI helper)
        - %D Flow (via processStochasticRSI helper)
        
        Args:
            timeframeRecord: TimeframeRecord POJO with candles and RSI state
            tokenAddress: Token address
            pairAddress: Pair address
        """
        try:
            rsiState = timeframeRecord.rsiState
            candles = timeframeRecord.ohlcvDetails
            
            if not rsiState or not candles:
                logger.warning(f"No RSI state or candles for update: {tokenAddress} {timeframeRecord.timeframe}")
                return
            
            # Filter new candles after lastUpdatedUnix
            newCandles = [c for c in candles if c.unixTime > (rsiState.lastUpdatedUnix or 0)]
            
            if not newCandles:
                logger.debug(f"No new candles for RSI update: {tokenAddress} {timeframeRecord.timeframe}")
                return
            
            # Get previous close from RSI state (stored from last calculation)
            previousClose = rsiState.lastClosePrice
            
            if previousClose is None:
                # Fallback: if not in state, get from previous candle in list
                previousClose = self.getPreviousCloseFromCandles(candles, rsiState.lastUpdatedUnix)
            
            # Process each new candle using modular flows
            for candle in newCandles:
                # --- RSI FLOW ---
                self.processRSI(rsiState, candle, previousClose)
                
                # --- STOCHASTIC RSI, %K, %D FLOWS ---
                self.processStochasticRSI(rsiState, candle, rsiState.rsiValue)
                
                # Update previousClose for next iteration
                previousClose = candle.closePrice
            
            # Update next fetch time and status
            timeframeInSeconds = CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
            rsiState.nextFetchTime = rsiState.lastUpdatedUnix + timeframeInSeconds
            rsiState.status = RSIStatus.AVAILABLE
            
            logger.info(f"✓ Incremental RSI update completed for {tokenAddress} {timeframeRecord.timeframe}: processed {len(newCandles)} new candles")
        
        except Exception as e:
            logger.info(f"✗ Error in incremental RSI update: {e}", exc_info=True)
    
    def calculateFirstRSIFromCandles(self, timeframeRecord: 'TimeframeRecord',
                                     tokenAddress: str, pairAddress: str,
                                     timeframe: str, rsiAvailableTime: int,
                                     pairCreatedTime: int, timeframeInSeconds: int) -> bool:
        """
        Calculate initial RSI and all derived indicators from scratch using modular flows
        
        Process:
        1. Calculate initial RSI using SMA for first 14 periods
        2. Use modular flows for subsequent candles:
           - RSI Flow
           - Stochastic RSI Flow
           - %K Flow
           - %D Flow
        """
        try:
            candles = timeframeRecord.ohlcvDetails
            if len(candles) <= self.RSI_INTERVAL + 1:
                logger.warning(f"Not enough candles for RSI: {tokenAddress} {timeframe}")
                return False
            
            # ==================== STEP 1: Calculate Initial RSI (SMA Method) ====================
            gains = []
            losses = []
            
            for i in range(1, self.RSI_INTERVAL + 1):
                change = candles[i].closePrice - candles[i-1].closePrice
                gains.append(change if change > 0 else 0)
                losses.append(abs(change) if change < 0 else 0)
            
            # Calculate initial average gain/loss (SMA)
            avgGain = sum(gains) / self.RSI_INTERVAL
            avgLoss = sum(losses) / self.RSI_INTERVAL
            
            # Calculate first RSI value
            firstRSI = self.calculateRSIValue(avgGain, avgLoss)
            
            # Update first RSI candle (candle at index RSI_INTERVAL)
            candles[self.RSI_INTERVAL].rsiValue = firstRSI
            
            logger.info(f"First RSI value calculated: {firstRSI:.2f} for {tokenAddress} {timeframe}")
            
            # ==================== STEP 2: Create RSI State with Initial Values ====================
            rsiState = RSIState(
                tokenAddress=tokenAddress,
                pairAddress=pairAddress,
                timeframe=timeframe,
                rsiInterval=self.RSI_INTERVAL,
                rsiAvailableTime=rsiAvailableTime,
                rsiValue=firstRSI,
                avgGain=avgGain,
                avgLoss=avgLoss,
                lastClosePrice=candles[self.RSI_INTERVAL].closePrice,
                stochRSIInterval=self.STOCH_RSI_INTERVAL,
                stochRSIValue=None,
                rsiValues=[firstRSI],
                kInterval=self.K_INTERVAL,
                kValue=None,
                stochRSIValues=[],
                dInterval=self.D_INTERVAL,
                dValue=None,
                kValues=[],
                lastUpdatedUnix=candles[self.RSI_INTERVAL].unixTime,
                nextFetchTime=candles[self.RSI_INTERVAL].unixTime + timeframeInSeconds,
                pairCreatedTime=pairCreatedTime,
                status=RSIStatus.AVAILABLE
            )
            
            # ==================== STEP 3: Process Remaining Candles Using Modular Flows ====================
            for i in range(self.RSI_INTERVAL + 1, len(candles)):
                candle = candles[i]
                previousClose = candles[i-1].closePrice
                
                # --- RSI FLOW ---
                self.processRSI(rsiState, candle, previousClose)
                
                # --- STOCHASTIC RSI, %K, %D FLOWS ---
                self.processStochasticRSI(rsiState, candle, rsiState.rsiValue)
            
            # Set RSI state in timeframe record
            timeframeRecord.rsiState = rsiState
            
            logger.info(f"✓ RSI calculated for {tokenAddress} {timeframe}: RSI={rsiState.rsiValue:.2f}, StochRSI={rsiState.stochRSIValue}, K={rsiState.kValue}, D={rsiState.dValue}")
            return True
        
        except Exception as e:
            logger.info(f"✗ Error in RSI calculation from candles: {e}", exc_info=True)
            return False
    
    def calculateRSIInMemory(self, timeframeRecord: 'TimeframeRecord',
                            tokenAddress: str, pairAddress: str,
                            pairCreatedTime: int) -> None:
        try:
            if not timeframeRecord.ohlcvDetails:
                logger.warning(f"TRADING API :: No candles available for RSI {tokenAddress} - {timeframeRecord.timeframe}")
                return
            
            logger.info(f"TRADING API :: RSI calculation started for {tokenAddress} - {timeframeRecord.timeframe} ")
            
            # Get latest fetched time from candles
            latestFetchedAtTime = max(candle.unixTime for candle in timeframeRecord.ohlcvDetails)
            timeframeInSeconds = CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
            
            # Calculate RSI available time
            initialCandleStartTime = CommonUtil.calculateInitialStartTime(pairCreatedTime, timeframeRecord.timeframe)
            rsiAvailableTime = initialCandleStartTime + ((self.RSI_INTERVAL + 1) * timeframeInSeconds)
            
            logger.info(f"TRADING API :: RSI for {tokenAddress} - {timeframeRecord.timeframe}: available at {rsiAvailableTime}, latest fetched: {latestFetchedAtTime}")
            
            # Check if we have enough data to calculate RSI
            if latestFetchedAtTime >= rsiAvailableTime:
                logger.info(f"TRADING API :: RSI data available, calculation started for {tokenAddress} - {timeframeRecord.timeframe}")
                
                # Calculate RSI from candles
                success = self.calculateFirstRSIFromCandles(
                    timeframeRecord, tokenAddress, pairAddress,
                    timeframeRecord.timeframe, rsiAvailableTime,
                    pairCreatedTime, timeframeInSeconds
                )
                
                if not success:
                    logger.warning(f"TRADING API :: Failed to calculate RSI for {tokenAddress} - {timeframeRecord.timeframe}")
                else:
                    logger.info(f"TRADING API :: RSI calculation completed for {tokenAddress} - {timeframeRecord.timeframe}")
            else:
                logger.info(f"TRADING API :: RSI not available, will be processed when data reaches {rsiAvailableTime} for {tokenAddress} - {timeframeRecord.timeframe}")
                # Create empty RSI state
                rsiState = RSIState.createEmpty(
                    tokenAddress=tokenAddress,
                    pairAddress=pairAddress,
                    timeframe=timeframeRecord.timeframe,
                    rsiAvailableTime=rsiAvailableTime,
                    pairCreatedTime=pairCreatedTime,
                    status=RSIStatus.NOT_AVAILABLE
                )
                
                timeframeRecord.rsiState = rsiState
            
            logger.info(f"TRADING API :: Completed RSI calculation for {tokenAddress} - {timeframeRecord.timeframe}")
        
        except Exception as e:
            logger.info(f"TRADING API :: Error calculating RSI in memory for {tokenAddress} - {timeframeRecord.timeframe}: {e}", exc_info=True)
    
    
    def processRSI(self, rsiState: 'RSIState', candle: 'OHLCVDetails', previousClose: float) -> None:
        """
        MODULAR RSI FLOW: Process RSI calculation and update state
        
        This flow handles:
        - Calculating gain/loss from price change
        - Updating average gain/loss with Wilder's smoothing
        - Calculating RSI value
        - Updating RSI state with new values
        - Updating candle with RSI value
        - Storing last close price for next calculation
        
        Args:
            rsiState: RSI state POJO (updated in-place)
            candle: Current candle POJO (updated in-place)
            previousClose: Previous candle's close price
        """
        # Calculate gain/loss
        change = candle.closePrice - previousClose
        gain = change if change > 0 else 0
        loss = abs(change) if change < 0 else 0
        
        # Update average gain/loss using Wilder's smoothing
        rsiState.avgGain = self.calculateWildersSmoothing(rsiState.avgGain, gain, self.RSI_INTERVAL)
        rsiState.avgLoss = self.calculateWildersSmoothing(rsiState.avgLoss, loss, self.RSI_INTERVAL)
        
        # Calculate RSI
        rsiValue = self.calculateRSIValue(rsiState.avgGain, rsiState.avgLoss)
        
        # Update state and candle
        rsiState.rsiValue = rsiValue
        rsiState.lastClosePrice = candle.closePrice
        rsiState.lastUpdatedUnix = candle.unixTime
        candle.rsiValue = rsiValue
    
    # ==================== CORE CALCULATION FUNCTIONS ====================
    
    def calculateWildersSmoothing(self, previousAvg: float, newValue: float, period: int) -> float:
        """
        Calculate Wilder's smoothing: (Previous Avg × (period-1) + New Value) / period
        
        Args:
            previousAvg: Previous average value
            newValue: New value to incorporate
            period: Smoothing period
            
        Returns:
            New smoothed average
        """
        return (previousAvg * (period - 1) + newValue) / period
    
    def calculateRSIValue(self, avgGain: float, avgLoss: float) -> float:
        """
        Calculate RSI value: 100 - (100 / (1 + RS)) where RS = Avg Gain / Avg Loss
        """
        if avgLoss == 0:
            return 100.0
        
        rs = avgGain / avgLoss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi
    
    def calculateStochasticRSI(self, rsiValues: List[float]) -> float:
        """
        Calculate Stochastic RSI: ((Current RSI - Lowest RSI) / (Highest RSI - Lowest RSI)) × 100
        
        Returns value in 0-100 range to match DexScreener
        
        Args:
            rsiValues: List of recent RSI values (last 14)
            
        Returns:
            Stochastic RSI value (0-100 range)
        """
        if len(rsiValues) < self.STOCH_RSI_INTERVAL:
            return 0.0
        
        # Use only the last 14 values for calculation
        recentRSIValues = rsiValues[-self.STOCH_RSI_INTERVAL:]
        
        highestRSI = max(recentRSIValues)
        lowestRSI = min(recentRSIValues)
        currentRSI = recentRSIValues[-1]  # Most recent RSI
        
        if highestRSI == lowestRSI:
            return 50.0  # Neutral value when no range (50 in 0-100 range)
        
        # Return value in 0-100 range
        stochRSI = ((currentRSI - lowestRSI) / (highestRSI - lowestRSI)) * 100.0
        return stochRSI
    
    def calculateK(self, stochRSIValues: List[float]) -> Optional[float]:
        """
        Calculate %K: 3-period SMA of Stochastic RSI
        
        IMPORTANT: Using standard SMA (waits for 3 values).
        If DexScreener shows values earlier, switch to calculateKProgressive()
        
        Args:
            stochRSIValues: List of Stochastic RSI values (0-100 range)
            
        Returns:
            %K value (0-100 range) or None if insufficient data
        """
        if len(stochRSIValues) < self.K_INTERVAL:
            return None
        
        # Standard SMA of last 3 Stochastic RSI values
        return sum(stochRSIValues[-self.K_INTERVAL:]) / self.K_INTERVAL
    
    def calculateKProgressive(self, stochRSIValues: List[float]) -> Optional[float]:
        """
        Calculate %K: Progressive averaging (shows value immediately)
        
        - With 1 value: %K = that value
        - With 2 values: %K = average of 2
        - With 3+ values: %K = SMA of last 3 (rolling)
        
        Use this if DexScreener shows K values immediately after first Stochastic RSI
        
        Args:
            stochRSIValues: List of Stochastic RSI values (0-100 range)
            
        Returns:
            %K value (0-100 range) or None if no data
        """
        if not stochRSIValues:
            return None
        
        if len(stochRSIValues) == 1:
            return stochRSIValues[0]
        elif len(stochRSIValues) == 2:
            return sum(stochRSIValues) / 2
        else:
            # Use last 3 values
            return sum(stochRSIValues[-3:]) / 3
    
    def processStochasticRSI(self, rsiState: RSIState, candle: 'OHLCVDetails', rsi: float) -> None:
        """
        Process Stochastic RSI, %K, and %D calculations
        
        Standard Stochastic RSI approach:
        - Stoch RSI: Uses last 14 RSI values, returns 0-100 range
        - %K: 3-period SMA of Stochastic RSI (only when 3+ values available)
        - %D: 3-period SMA of %K (only when 3+ %K values available)
        
        Args:
            rsiState: RSI state POJO
            candle: Current candle POJO
            rsi: Current RSI value
        """
        # Add RSI to values list
        rsiState.addRSIValue(rsi)
        
        # Calculate Stochastic RSI if we have enough RSI values
        if len(rsiState.rsiValues) >= self.STOCH_RSI_INTERVAL:
            stochRSI = self.calculateStochasticRSI(rsiState.rsiValues)
            rsiState.stochRSIValue = stochRSI
            candle.stochRSIValue = stochRSI
            
            # Add Stochastic RSI to values list
            rsiState.addStochRSIValue(stochRSI)
            
            # Calculate %K (standard SMA, requires 3 values)
            kValue = self.calculateK(rsiState.stochRSIValues)
            if kValue is not None:
                rsiState.kValue = kValue
                candle.stochRSIK = kValue
                
                # Add %K to values list
                rsiState.addKValue(kValue)
                
                # Calculate %D if we have enough %K values (standard SMA of 3 %K values)
                if len(rsiState.kValues) >= self.D_INTERVAL:
                    dValue = sum(rsiState.kValues[-self.D_INTERVAL:]) / self.D_INTERVAL
                    rsiState.dValue = dValue
                    candle.stochRSID = dValue
    
    def getPreviousCloseFromCandles(self, candles: List['OHLCVDetails'], lastUpdatedUnix: Optional[int]) -> float:
        """
        FALLBACK METHOD: Get previous close price from candles when not in RSI state
        
        This is only used as a fallback when lastClosePrice is not stored in RSI state
        (e.g., for legacy data or first-time calculations).
        
        Args:
            candles: List of candles
            lastUpdatedUnix: Last updated timestamp
            
        Returns:
            Previous close price
        """
        if not lastUpdatedUnix:
            return candles[0].closePrice if candles else 0.0
        
        # Find the candle at lastUpdatedUnix
        for i, candle in enumerate(candles):
            if candle.unixTime == lastUpdatedUnix:
                return candle.closePrice
        
        # Fallback to first candle
        return candles[0].closePrice if candles else 0.0

