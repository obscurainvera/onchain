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
from constants.VWAPConstants import *
import time
from actions.TradingActionUtil import TradingActionUtil

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
        self.TIMEFRAMES = [Timeframes.FIFTEEN_MIN, Timeframes.ONE_HOUR, Timeframes.FOUR_HOUR]
    
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
            
            if not calcualtedVWAP[CANDLE_UPDATES] and not calcualtedVWAP[SESSION_UPDATES]:
                logger.info("No VWAP updates to process")
                return True
            
            # Execute all VWAP updates in batch
            success = self.recordVWAPCandleAndVWAPStateUpdatedData(calcualtedVWAP)
            
            totalUpdates = len(calcualtedVWAP[CANDLE_UPDATES]) + len(calcualtedVWAP[SESSION_UPDATES])
            logger.info(f"VWAP scheduler processing completed: {len(calcualtedVWAP[CANDLE_UPDATES])} candle updates, "
                       f"{len(calcualtedVWAP[SESSION_UPDATES])} session updates")
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
                allCandleUpdates.extend(timeframeResults[CANDLE_UPDATES])
                allSessionUpdates.extend(timeframeResults[SESSION_UPDATES])
        
        return {
            CANDLE_UPDATES: allCandleUpdates,
            SESSION_UPDATES: allSessionUpdates
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
        candles = vwapData.get('candles', [])
        if not candles:
            return {CANDLE_UPDATES: [], SESSION_UPDATES: []}
        
        pairAddress = vwapData['pairAddress']
        
        # Process all candles with day boundary detection
        return self.calculateVWAP(tokenAddress, pairAddress, timeframe, candles, vwapData)
    
    def calculateVWAP(self, tokenAddress: str, pairAddress: str, 
                                             timeframe: str, candles: List[Dict], 
                                             vwapData: Dict) -> Dict[str, List[Dict]]:
        """
        Process candles with day boundary detection during iteration.
        
        This method iterates through candles and decides during iteration whether to:
        1. Do incremental update (same day, existing session)
        2. Do full reset (new day or no existing session)
        
        Returns:
            Dict with 'candleUpdates' and 'sessionUpdates' lists
        """
        candleUpdates = []
        sessionUpdates = []
        
        # 1. Check if we have existing session data
        hasExistingSession = vwapData['lastCandleUnix'] > 0
        
        if hasExistingSession:
            # Use existing session data
            currentCumulativePV = Decimal(str(vwapData['cumulativePV'] or 0))
            currentCumulativeVolume = Decimal(str(vwapData['cumulativeVolume'] or 0))
            sessionStartUnix = vwapData['sessionStartUnix']
            sessionEndUnix = vwapData['sessionEndUnix']
        else:
            # 2. No existing session - start fresh
            currentCumulativePV = Decimal('0')
            currentCumulativeVolume = Decimal('0')
            currentTime = int(time.time())
            sessionStartUnix = (currentTime // 86400) * 86400  # Start of today
            sessionEndUnix = sessionStartUnix + 86400          # End of today
        
        logger.info(f"Processing {len(candles)} candles for {tokenAddress} {timeframe}: "
                   f"hasExistingSession={hasExistingSession}")
        
        # 3. Iterate through candles and calculate VWAP
        for candle in candles:
            candleUnix = candle['unixtime']
            
            # Calculate VWAP for this candle
            typicalPrice = (candle['highprice'] + candle['lowprice'] + candle['closeprice']) / 3
            priceVolume = typicalPrice * candle['volume']
            
            # Update cumulative values
            currentCumulativePV += priceVolume
            currentCumulativeVolume += candle['volume']
            
            # Calculate current VWAP
            currentVWAP = currentCumulativePV / currentCumulativeVolume if currentCumulativeVolume > 0 else 0
            
            # Create candle update (one per candle)
            candleUpdate = {
                TOKEN_ADDRESS: tokenAddress,
                PAIR_ADDRESS: pairAddress,
                TIMEFRAME: timeframe,
                CANDLE_UNIX: candleUnix,
                VWAP_VALUE: float(currentVWAP)
            }
            candleUpdates.append(candleUpdate)
        
        # 4. Create VWAP session data for batch update
        if candles:  # Only if we processed any candles
            lastCandleUnix = candles[-1]['unixtime']
            sessionUpdate = {
                SESSION_TYPE: INCREMENTAL if hasExistingSession else FULL_RESET,
                TOKEN_ADDRESS: tokenAddress,
                PAIR_ADDRESS: pairAddress,
                TIMEFRAME: timeframe,
                SESSION_START_UNIX: sessionStartUnix,
                SESSION_END_UNIX: sessionEndUnix,
                CUMULATIVE_PV: float(currentCumulativePV),
                CUMULATIVE_VOLUME: float(currentCumulativeVolume),
                CURRENT_VWAP: float(currentVWAP),
                LAST_CANDLE_UNIX: lastCandleUnix,
                NEXT_CANDLE_FETCH: lastCandleUnix + self.getTimeframeInSeconds(timeframe)
            }
            sessionUpdates.append(sessionUpdate)
        
        return {
            CANDLE_UPDATES: candleUpdates,
            SESSION_UPDATES: sessionUpdates
        }
    
    
    def getTimeframeInSeconds(self, timeframe: str) -> int:
        """Get timeframe duration in seconds."""
        timeframeMap = {
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '4h': 14400
        }
        return timeframeMap.get(timeframe, 3600)
    
    def recordVWAPCandleAndVWAPStateUpdatedData(self, calculatedVWAP: Dict[str, List[Dict]]) -> bool:
        """Execute all VWAP updates in batch."""
        try:
            return self.trading_handler.batchUpdateVWAPData(
                calculatedVWAP[CANDLE_UPDATES], 
                calculatedVWAP[SESSION_UPDATES]
            )
        except Exception as e:
            logger.error(f"Error executing batch VWAP updates: {e}")
            return False

    
    
    
    def calculateVwapSessionRange(self, timestamp: int) -> Tuple[int, int]:
        """Calculate session start (00:00:00) and end (23:59:59) for given timestamp"""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        session_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        session_end = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        return int(session_start.timestamp()), int(session_end.timestamp())
    
    
    def calculateVWAPForResetOrNew(self, candles: List[Dict]) -> Dict:
        """
        Calculate VWAP for list of candles
        Returns cumulative data and individual candle VWAPs
        """
        if not candles:
            return {
                'cumulative_pv': Decimal('0'),
                'cumulative_volume': Decimal('0'),
                'final_vwap': Decimal('0'),
                'candle_vwaps': [],
                'latest_candle_time': 0
            }
        
        cumulativePriceVolume = Decimal('0')
        cumulativeVolume = Decimal('0')
        candlesWithVWAP = []
        
        for candle in candles:
            # Calculate typical price (HLC/3)
            high = Decimal(str(candle['highprice']))
            low = Decimal(str(candle['lowprice']))
            close = Decimal(str(candle['closeprice']))
            volume = Decimal(str(candle['volume']))
            
            price = (high + low + close) / Decimal('3')
            priceVolume = price * volume
            
            cumulativePriceVolume += priceVolume
            cumulativeVolume += volume
            
            # Calculate running VWAP
            vwap = cumulativePriceVolume / cumulativeVolume if cumulativeVolume > 0 else Decimal('0')
            
            candlesWithVWAP.append({
                'unixtime': candle['unixtime'],
                'vwap': vwap
            })
        
        return {
            'cumulative_pv': cumulativePriceVolume,
            'cumulative_volume': cumulativeVolume, 
            'final_vwap': vwap,
            'candle_vwaps': candlesWithVWAP,
            'latest_candle_time': candles[-1]['unixtime']
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
        return (currentTime // 86400) * 86400
    
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
                vwapSession = self.createVWAPSessionWithCandles(
                    timeframe, todayCandles, dayStart
                )
                logger.info(f"Prepared VWAP operation for {tokenAddress} {timeframe}: "
                           f"{len(todayCandles)} candles")
            else:
                vwapSession = self.createEmptyVWAPSession(
                    timeframe, dayStart, currentTime
                )
                logger.info(f"Prepared empty VWAP operation for {tokenAddress} {timeframe}")
            
            vwapSessions.append(vwapSession)
        
        return vwapSessions
    
    def filterTodaysCandles(self, candles: List[Dict], dayStart: int) -> List[Dict]:
        """Filter candles to only include today's data."""
        if not candles:
            return []
        
        dayEnd = dayStart + 86400
        return [c for c in candles if dayStart <= c['unixtime'] < dayEnd]
    
    def createVWAPSessionWithCandles(self, timeframe: str, todayCandles: List[Dict], 
                                      dayStart: int) -> Dict:
        """Create VWAP operation data for timeframes with candle data."""
        calculatedVwap = self.calculateVWAPForResetOrNew(todayCandles)
        nextCandleFetchTime = TradingActionUtil.calculateNextCandleFetch(
            timeframe, calculatedVwap['latest_candle_time']
        )
        dayEnd = dayStart + 86400
        
        return {
            'timeframe': timeframe,
            'today_candles': todayCandles,
            'vwap_result': calculatedVwap,
            'next_candle_fetch': nextCandleFetchTime,
            'day_start': dayStart,
            'day_end': dayEnd,
            'has_candles': True
        }
    
    def createEmptyVWAPSession(self, timeframe: str, dayStart: int, 
                                currentTime: int) -> Dict:
        """Create empty VWAP operation data for timeframes without candle data."""
        emptyVwapResult = {
            'cumulative_pv': 0,
            'cumulative_volume': 0,
            'final_vwap': 0,
            'latest_candle_time': None,
            'candle_vwaps': []
        }
        dayEnd = dayStart + 86400
        
        return {
            'timeframe': timeframe,
            'today_candles': [],
            'vwap_result': emptyVwapResult,
            'next_candle_fetch': None,
            'day_start': dayStart,
            'day_end': dayEnd,
            'has_candles': False
        }
    
    
