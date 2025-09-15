"""
VWAPProcessor - Handles VWAP calculations and session management

SIMPLIFIED VWAP CALCULATION PROCESS:
===================================

EXAMPLE: Token ABC, 1hr timeframe, Current time: 15:00

STEP 1: Get All VWAP Data (Single Query)
- Fetch all active tokens with their timeframes and VWAP session data
- Get unprocessed candles (where ohlcv.unixtime > COALESCE(vs.lastcandleunix, 0))
- Single optimized query with LEFT JOINs for maximum efficiency

STEP 2: Process Each Token/Timeframe
- For each token and timeframe combination:
  a) Check if existing session data exists (hasExistingSession)
  b) If existing: Use current cumulative values (incremental update)
  c) If not existing: Start fresh with today's day boundaries (full reset)

STEP 3: Iterate Through Candles
- Process each candle chronologically
- Calculate typical_price = (high + low + close) / 3
- Update cumulative: total_pv += (typical_price × volume)
- Update cumulative: total_volume += volume
- Calculate current VWAP = total_pv / total_volume
- Create candle update record for each processed candle

STEP 4: Create Session Update (Once Per Session)
- After processing all candles for a session:
  - Create single session update record
  - Type: 'incremental' (existing session) or 'full_reset' (new session)
  - Include final cumulative values and session boundaries

STEP 5: Batch Database Updates (ATOMIC)
- Update ohlcvdetails.vwapvalue for all processed candles
- Update existing vwapsessions records (incremental) or insert new ones (full_reset)
- All operations in single transaction for data consistency

KEY SIMPLIFICATIONS:
- No upfront decision between incremental/full reset
- Dynamic determination during candle iteration
- Single session update per timeframe (not per candle)
- Use today's day boundaries for new sessions
- Simplified 4-step process: initialize → iterate → calculate → update
"""

from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timezone

from sqlalchemy import null
from logs.logger import get_logger
from database.trading.TradingHandler import TradingHandler
from scheduler.SchedulerConstants import CandleDataKeys, Timeframes
from utils.CommonUtil import CommonUtil
import time
from actions.TradingActionUtil import TradingActionUtil
from constants.TradingHandlerConstants import TradingHandlerConstants
from utils.IndicatorConstants import IndicatorConstants

logger = get_logger(__name__)

class VWAPSessionResult:
    """Result container for VWAP session operations"""
    NEW_SESSION = 'new_session'
    SAME_DAY_UPDATE = 'same_day_update' 
    NEW_DAY_RESET = 'new_day_reset'

class VWAPProcessor:
    """
    Production-ready VWAP processor with atomic transactions and session management
    """
    
    def __init__(self, trading_handler: TradingHandler):
        self.trading_handler = trading_handler
        
    def processVWAPForScheduler(self) -> bool:
        """
        NEW SCHEDULER FLOW: Process VWAP calculations with optimized single-query approach.
        
        This method implements the new VWAP scheduler flow:
        1. Single optimized query to get all VWAP data with JOINs
        2. Process candles chronologically with day boundary detection
        3. Handle both incremental updates (Case 1) and full resets (Case 2)
        4. Batch update all VWAP values and sessions
        
        Returns:
            bool: True if all VWAP operations completed successfully
        """
        try:
            logger.info("Processing VWAP for all active tokens using new scheduler flow")
            
            # Get all VWAP data in a single optimized query (no token filtering)
            vwapDataByToken = self.trading_handler.getAllVWAPDataForScheduler()
            
            if not vwapDataByToken:
                logger.info("No VWAP data found for processing")
                return True
            
            # Process VWAP calculations for all tokens
            calcualtedVWAP = self.calculateVWAPForAllTokens(vwapDataByToken)
            
            if not calcualtedVWAP[IndicatorConstants.VWAPSessions.CANDLE_UPDATES] and not calcualtedVWAP[IndicatorConstants.VWAPSessions.SESSION_UPDATES]:
                logger.info("No VWAP updates to process")
                return True
            
            # Execute all VWAP updates in batch
            success = self.recordVWAPCandleAndVWAPStateUpdatedData(calcualtedVWAP)
            
            logger.info(f"VWAP scheduler processing completed: {len(calcualtedVWAP[IndicatorConstants.VWAPSessions.CANDLE_UPDATES])} candle updates, "
                       f"{len(calcualtedVWAP[IndicatorConstants.VWAPSessions.SESSION_UPDATES])} session updates")
            return success
            
        except Exception as e:
            logger.error(f"Error in VWAP scheduler processing: {e}")
            return False
    
    def calculateVWAPForAllTokens(self, vwapDataByToken: Dict[str, Dict[str, Dict]]) -> Dict[str, List[Dict]]:
        """
        Process VWAP calculations for all tokens with day boundary detection.
        
        Args:
            vwapDataByToken: Dict mapping tokenAddress -> timeframe -> VWAP data
            
        Returns:
            Dict with 'candleUpdates' and 'sessionUpdates' lists
        """
        allCandleUpdates = []
        allSessionUpdates = []
        
        for tokenAddress, timeframeData in vwapDataByToken.items(): 
            for timeframe, vwapData in timeframeData.items(): 
                timeframeResults = self.calculateVWAPForATokenForACertainTimeframe(tokenAddress, timeframe, vwapData)
                allCandleUpdates.extend(timeframeResults[IndicatorConstants.VWAPSessions.CANDLE_UPDATES])
                allSessionUpdates.extend(timeframeResults[IndicatorConstants.VWAPSessions.SESSION_UPDATES])
        
        return {
            IndicatorConstants.VWAPSessions.CANDLE_UPDATES: allCandleUpdates,
            IndicatorConstants.VWAPSessions.SESSION_UPDATES: allSessionUpdates
        }
    
    def calculateVWAPForATokenForACertainTimeframe(self, tokenAddress: str, timeframe: str, vwapData: Dict) -> Dict[str, List[Dict]]:
        """
        Process VWAP calculations for a specific timeframe with day boundary detection.
        
        This method handles:
        1. Chronological processing of candles
        2. Day boundary detection for VWAP resets
        3. Incremental vs full reset logic during iteration
        
        Returns:
            Dict with 'candleUpdates' and 'sessionUpdates' lists
        """
        candles = vwapData.get(IndicatorConstants.VWAPSessions.CANDLES, [])
        if not candles:
            return {IndicatorConstants.VWAPSessions.CANDLE_UPDATES: [], 
            IndicatorConstants.VWAPSessions.SESSION_UPDATES: []}
        
        pairAddress = vwapData[TradingHandlerConstants.TrackedTokens.PAIR_ADDRESS]
        
        # Process all candles with day boundary detection
        return self.calculateVWAP(tokenAddress, pairAddress, timeframe, candles, vwapData)
    
    def calculateVWAP(self, tokenAddress: str, pairAddress: str, 
                                             timeframe: str, candles: List[Dict], 
                                             vwapData: Dict) -> Dict[str, List[Dict]]:
        """
        Process candles with day boundary detection during iteration.
        
        This method iterates through candles and resets VWAP session when crossing day boundaries:
        1. Check if candle is beyond current session end time
        2. If yes: Reset VWAP session for new day and calculate fresh VWAP
        3. If no: Continue with incremental VWAP calculation
        
        Returns:
            Dict with 'candleUpdates' and 'sessionUpdates' lists
        """
        candleUpdates = []
        sessionUpdates = []
        
        # 1. Initialize session state
        hasExistingSession = vwapData[TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX] > 0
        
        if hasExistingSession:
            # Use existing session data
            currentCumulativePV = Decimal(str(vwapData[TradingHandlerConstants.VWAPSessions.CUMULATIVE_PV] or 0))
            currentCumulativeVolume = Decimal(str(vwapData[TradingHandlerConstants.VWAPSessions.CUMULATIVE_VOLUME] or 0))
            sessionStartUnix = vwapData[TradingHandlerConstants.VWAPSessions.SESSION_START_UNIX]
            sessionEndUnix = vwapData[TradingHandlerConstants.VWAPSessions.SESSION_END_UNIX]
        else:
            # No existing session - will be set when first candle is processed
            currentCumulativePV = Decimal('0')
            currentCumulativeVolume = Decimal('0')
            sessionStartUnix = None
            sessionEndUnix = None
        
        logger.info(f"Processing {len(candles)} candles for {tokenAddress} {timeframe}: "
                   f"hasExistingSession={hasExistingSession}")
        
        # 2. Iterate through candles and calculate VWAP with day boundary detection
        for candle in candles:
            candleUnix = candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
            candleDay = candleUnix // 86400  # Get day number for this candle
            
            # Check if we need to reset VWAP session (new day)
            if sessionEndUnix is not None:
                if CommonUtil.isNewDay(candleUnix, sessionEndUnix):
                    # Day boundary crossed - reset VWAP session for new day
                    logger.info(f"Day boundary detected for {tokenAddress} {timeframe}: "
                              f"candle day {candleDay} > session day {sessionEndUnix // 86400}")
                    
                    # Reset for new day (but keep same session record)
                    currentCumulativePV = Decimal('0')
                    currentCumulativeVolume = Decimal('0')
                    sessionStartUnix, sessionEndUnix = CommonUtil.getSessionStartAndEndUnix(candleUnix)
                    hasExistingSession = False  # This is now a fresh session for new day
            
            # If no existing session, initialize day boundaries from first candle
            if sessionStartUnix is None:
                sessionStartUnix, sessionEndUnix = CommonUtil.getSessionStartAndEndUnix(candleUnix)
            
            # Calculate VWAP for this candle
            typicalPrice = (candle[TradingHandlerConstants.OHLCVDetails.HIGH_PRICE] + 
            candle[TradingHandlerConstants.OHLCVDetails.LOW_PRICE] + 
            candle[TradingHandlerConstants.OHLCVDetails.CLOSE_PRICE]) / 3

            priceVolume = typicalPrice * candle[TradingHandlerConstants.OHLCVDetails.VOLUME]
            
            # Update cumulative values
            currentCumulativePV += priceVolume
            currentCumulativeVolume += candle[TradingHandlerConstants.OHLCVDetails.VOLUME]
            
            # Calculate current VWAP
            currentVWAP = currentCumulativePV / currentCumulativeVolume if currentCumulativeVolume > 0 else 0
            
            # Create candle update (one per candle)
            candleUpdate = {
                TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS: tokenAddress,
                TradingHandlerConstants.OHLCVDetails.PAIR_ADDRESS: pairAddress,
                TradingHandlerConstants.OHLCVDetails.TIMEFRAME: timeframe,
                TradingHandlerConstants.OHLCVDetails.UNIX_TIME: candleUnix,
                TradingHandlerConstants.OHLCVDetails.VWAP_VALUE: float(currentVWAP)
            }
            candleUpdates.append(candleUpdate)
        
        # 3. Create ONE session update for the current session (token+pair+timeframe)
        if candles:  # Only if we processed any candles
            lastCandleUnix = candles[-1][TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
            sessionUpdate = {
                IndicatorConstants.VWAPSessions.SESSION_TYPE: IndicatorConstants.VWAPSessions.INCREMENTAL if hasExistingSession else IndicatorConstants.VWAPSessions.FULL_RESET,
                TradingHandlerConstants.VWAPSessions.TOKEN_ADDRESS: tokenAddress,
                TradingHandlerConstants.VWAPSessions.PAIR_ADDRESS: pairAddress,
                TradingHandlerConstants.VWAPSessions.TIMEFRAME: timeframe,
                TradingHandlerConstants.VWAPSessions.SESSION_START_UNIX: sessionStartUnix,
                TradingHandlerConstants.VWAPSessions.SESSION_END_UNIX: sessionEndUnix,
                TradingHandlerConstants.VWAPSessions.CUMULATIVE_PV: float(currentCumulativePV),
                TradingHandlerConstants.VWAPSessions.CUMULATIVE_VOLUME: float(currentCumulativeVolume),
                TradingHandlerConstants.VWAPSessions.CURRENT_VWAP: float(currentVWAP),
                TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX: lastCandleUnix,
                TradingHandlerConstants.VWAPSessions.NEXT_CANDLE_FETCH: lastCandleUnix + CommonUtil.getTimeframeSeconds(timeframe)
            }
            sessionUpdates.append(sessionUpdate)
        
        return {
            IndicatorConstants.VWAPSessions.CANDLE_UPDATES: candleUpdates,
            IndicatorConstants.VWAPSessions.SESSION_UPDATES: sessionUpdates
        }
    
    
    
    
    def recordVWAPCandleAndVWAPStateUpdatedData(self, calculatedVWAP: Dict[str, List[Dict]]) -> bool:
        """Execute all VWAP updates in batch."""
        try:
            return self.trading_handler.batchUpdateVWAPData(
                calculatedVWAP[IndicatorConstants.VWAPSessions.CANDLE_UPDATES], 
                calculatedVWAP[IndicatorConstants.VWAPSessions.SESSION_UPDATES]
            )
        except Exception as e:
            logger.error(f"Error executing batch VWAP updates: {e}")
            return False

    

    
    
    def calculateVWAPForResetOrNew(self, candles: List[Dict]) -> Dict:
        """
        Calculate VWAP for list of candles
        Returns cumulative data and individual candle VWAPs
        """
        if not candles:
            return {
                TradingHandlerConstants.VWAPSessions.CUMULATIVE_PV: Decimal('0'),
                TradingHandlerConstants.VWAPSessions.CUMULATIVE_VOLUME: Decimal('0'),
                TradingHandlerConstants.VWAPSessions.CURRENT_VWAP: Decimal('0'),
                IndicatorConstants.VWAPSessions.CANDLE_VWAPS: [],
                TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX: 0
            }
        
        cumulativePriceVolume = Decimal('0')
        cumulativeVolume = Decimal('0')
        candlesWithVWAP = []
        
        for candle in candles:
            # Calculate typical price (HLC/3)
            high = Decimal(str(candle[TradingHandlerConstants.OHLCVDetails.HIGH_PRICE]))
            low = Decimal(str(candle[TradingHandlerConstants.OHLCVDetails.LOW_PRICE]))
            close = Decimal(str(candle[TradingHandlerConstants.OHLCVDetails.CLOSE_PRICE]))
            volume = Decimal(str(candle[TradingHandlerConstants.OHLCVDetails.VOLUME]))
            
            price = (high + low + close) / Decimal('3')
            priceVolume = price * volume
            
            cumulativePriceVolume += priceVolume
            cumulativeVolume += volume
            
            # Calculate running VWAP
            vwap = cumulativePriceVolume / cumulativeVolume if cumulativeVolume > 0 else Decimal('0')
            
            candlesWithVWAP.append({
                TradingHandlerConstants.OHLCVDetails.UNIX_TIME: candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME],
                TradingHandlerConstants.OHLCVDetails.VWAP_VALUE : vwap
            })
        
        return {
            TradingHandlerConstants.VWAPSessions.CUMULATIVE_PV: cumulativePriceVolume,
            TradingHandlerConstants.VWAPSessions.CUMULATIVE_VOLUME: cumulativeVolume, 
            TradingHandlerConstants.VWAPSessions.CURRENT_VWAP: vwap,
            IndicatorConstants.VWAPSessions.CANDLE_VWAPS: candlesWithVWAP,
            TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX: candles[-1][TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
        }
    
    def _calculateIncrementalVWAP(self, exsistingVwapSession: Dict, newCandlesToUpdateVwap: List[Dict]) -> Dict:
        """
        Calculate incremental VWAP using existing cumulative data
        More efficient than recalculating from scratch
        """
        cumulative_pv = exsistingVwapSession['cumulativepv']
        cumulative_volume = exsistingVwapSession['cumulativevolume']
        candle_vwaps = []
        
        for candle in newCandlesToUpdateVwap:
            # Calculate typical price (HLC/3)
            high = Decimal(str(candle['highprice']))
            low = Decimal(str(candle['lowprice']))
            close = Decimal(str(candle['closeprice']))
            volume = Decimal(str(candle['volume']))
            
            typical_price = (high + low + close) / Decimal('3')
            price_volume = typical_price * volume
            
            cumulative_pv += price_volume
            cumulative_volume += volume
            
            # Calculate running VWAP
            vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else Decimal('0')
            
            candle_vwaps.append({
                'unixtime': candle['unixtime'],
                'vwap': vwap
            })
        
        return {
            'cumulative_pv': cumulative_pv,
            'cumulative_volume': cumulative_volume,
            'final_vwap': vwap if newCandlesToUpdateVwap else exsistingVwapSession['currentvwap'],
            'candle_vwaps': candle_vwaps,
            # Get the timestamp of the most recent candle ([-1] gets the last element in the list)
            # If no new candles, use the existing session's last candle time
            'latest_candle_time': newCandlesToUpdateVwap[-1]['unixtime'] if newCandlesToUpdateVwap else exsistingVwapSession['lastcandleunix']
        }
    

    def calculateVWAPFromAPI(self, tokenAddress: str, pairAddress: str, pairCreatedTime: int, allCandles: Dict[str, List[Dict]]) -> Dict:
        """
        API FLOW ONLY: Process VWAP for new token addition using pre-loaded candles

        Idea behind this : when we are adding a new token or old token, we only need to calculate vwap for all timeframes for the current day.
        we usually get the past 2 days candles but only take the current day candles and update the vwap values candle wise and session wise
        
        NOTE: This function exists for API flow because:
        - API flow has candles already loaded in memory (no need to fetch from DB)
        - Processes all timeframes at once during token addition
        - Different from scheduler's incremental VWAP updates
        """
        try:
            currentTime = int(time.time())
            dayStart = self.calculateDayStart(currentTime)
            
            # Process all timeframes and prepare batch operations
            vwapSessions = self.calculateVWAPForAllTimeframes(
                tokenAddress, pairAddress, allCandles, dayStart, currentTime
            )
            
            if not vwapSessions:
                logger.warning(f"No VWAP operations prepared for {tokenAddress}")
                return {'success': True, 'message': 'No timeframes to process'}
            
            # Execute all VWAP operations in batch
            result = self.trading_handler.recordVwapCandleUpdateAndVwapSessionUpdateFromAPI(
                tokenAddress, pairAddress, vwapSessions
            )
            
            logger.info(f"VWAP processing completed for {tokenAddress}: "
                       f"{len(vwapSessions)} timeframes processed")
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing VWAP for {tokenAddress}: {e}")
            return {'success': False, 'error': str(e)}
    
    def calculateDayStart(self, currentTime: int) -> int:
        """Calculate start of current day in UTC."""
        sessionStart, _ = CommonUtil.getSessionStartAndEndUnix(currentTime)
        return sessionStart
    
    def calculateVWAPForAllTimeframes(self, tokenAddress: str, pairAddress: str, 
                                   allCandles: Dict[str, List[Dict]], 
                                   dayStart: int, currentTime: int) -> List[Dict]:
        """
        Process all timeframes and prepare VWAP operations.
        
        Returns:
            List of VWAP operation data for batch processing
        """
        vwapSessions = []
        
        for timeframe, candles in allCandles.items():
            todayCandles = self.filterTodaysCandles(candles, dayStart)
            
            if todayCandles:
                vwapSession = self.createVWAPSessionWithCandles(timeframe, todayCandles, dayStart)
                logger.info(f"Prepared VWAP operation for {tokenAddress} {timeframe}: {len(todayCandles)} candles")
            else:
                vwapSession = self.createEmptyVWAPSession(timeframe, dayStart, currentTime)
                logger.info(f"Prepared empty VWAP operation for {tokenAddress} {timeframe}")
            
            vwapSessions.append(vwapSession)
        
        return vwapSessions
    
    def filterTodaysCandles(self, candles: List[Dict], dayStart: int) -> List[Dict]:
        """Filter candles to only include today's data."""
        if not candles:
            return []
        
        _, dayEnd = CommonUtil.getSessionStartAndEndUnix(dayStart)
        return [c for c in candles if dayStart <= c[TradingHandlerConstants.OHLCVDetails.UNIX_TIME] <= dayEnd]
    
    def createVWAPSessionWithCandles(self, timeframe: str, todayCandles: List[Dict], 
                                      dayStart: int) -> Dict:
        """Create VWAP operation data for timeframes with candle data."""
        calculatedVwap = self.calculateVWAPForResetOrNew(todayCandles)
        nextCandleFetchTime = TradingActionUtil.calculateNextCandleFetch(timeframe, calculatedVwap[TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX])
        _, dayEnd = CommonUtil.getSessionStartAndEndUnix(dayStart)
        
        return {
            TradingHandlerConstants.TimeframeMetadata.TIMEFRAME: timeframe,
            IndicatorConstants.VWAPSessions.TODAY_CANDLES: todayCandles,
            TradingHandlerConstants.VWAPSessions.CURRENT_VWAP: calculatedVwap,
            TradingHandlerConstants.VWAPSessions.NEXT_CANDLE_FETCH: nextCandleFetchTime,
            TradingHandlerConstants.VWAPSessions.SESSION_START_UNIX: dayStart,
            TradingHandlerConstants.VWAPSessions.SESSION_END_UNIX: dayEnd,
            IndicatorConstants.VWAPSessions.HAS_CANDLES: True
        }
    
    def createEmptyVWAPSession(self, timeframe: str, dayStart: int, 
                                currentTime: int) -> Dict:
        """Create empty VWAP operation data for timeframes without candle data."""
        emptyVwapResult = {
            TradingHandlerConstants.VWAPSessions.CUMULATIVE_PV: 0,
            TradingHandlerConstants.VWAPSessions.CUMULATIVE_VOLUME: 0,
            TradingHandlerConstants.VWAPSessions.CURRENT_VWAP: 0,
            TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX: None,
            IndicatorConstants.VWAPSessions.CANDLE_VWAPS: []
        }
        _, dayEnd = CommonUtil.getSessionStartAndEndUnix(dayStart)
        
        return {
            TradingHandlerConstants.TimeframeMetadata.TIMEFRAME: timeframe,
            IndicatorConstants.VWAPSessions.TODAY_CANDLES: [],
            TradingHandlerConstants.VWAPSessions.CURRENT_VWAP: emptyVwapResult,
            TradingHandlerConstants.VWAPSessions.NEXT_CANDLE_FETCH: None,
            TradingHandlerConstants.VWAPSessions.SESSION_START_UNIX: dayStart,
            TradingHandlerConstants.VWAPSessions.SESSION_END_UNIX: dayEnd,
            IndicatorConstants.VWAPSessions.HAS_CANDLES: False
        }
    
    
