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

from typing import List
from decimal import Decimal

from logs.logger import get_logger
from database.trading.TradingHandler import TradingHandler
from utils.CommonUtil import CommonUtil
import time
from api.trading.request.VWAPSession import VWAPSession

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
        
    
    def calculateDayStart(self, currentTime: int) -> int:
        """Calculate start of current day in UTC."""
        sessionStart, _ = CommonUtil.getSessionStartAndEndUnix(currentTime)
        return sessionStart
    
    
    def calculateVWAPForAllTrackedTokens(self, trackedTokens) -> None:           
        for trackedToken in trackedTokens:  
            for timeframeRecord in trackedToken.timeframeRecords:
                self.calculateVWAPFromScheduler(timeframeRecord, trackedToken.tokenAddress, trackedToken.pairAddress)
                logger.info(f"✓ Processed VWAP for {trackedToken.symbol} {timeframeRecord.timeframe}")
                

    def calculateVWAPFromScheduler(self, timeframeRecord, tokenAddress: str, pairAddress: str) -> None:
        try:
            if not timeframeRecord.ohlcvDetails:
                logger.warning(f"No candles available for VWAP calculation: {tokenAddress} {timeframeRecord.timeframe}")
                return
            
            # Sort candles by unixTime to ensure chronological processing
            timeframeRecord.ohlcvDetails.sort(key=lambda x: x.unixTime)
            candles = timeframeRecord.ohlcvDetails
            
            # Initialize session state
            hasExistingSession = timeframeRecord.vwapSession is not None and timeframeRecord.vwapSession.lastCandleUnix is not None
            
            if hasExistingSession:
                # Use existing session data
                currentCumulativePV = timeframeRecord.vwapSession.cumulativePV or Decimal('0')
                currentCumulativeVolume = timeframeRecord.vwapSession.cumulativeVolume or Decimal('0')
                sessionStartUnix = timeframeRecord.vwapSession.sessionStartUnix
                sessionEndUnix = timeframeRecord.vwapSession.sessionEndUnix
            else:
                # No existing session - will be set when first candle is processed
                currentCumulativePV = Decimal('0')
                currentCumulativeVolume = Decimal('0')
                sessionStartUnix = None
                sessionEndUnix = None
            
            logger.info(f"Processing {len(candles)} candles for {tokenAddress} {timeframeRecord.timeframe}: "
                       f"hasExistingSession={hasExistingSession}")
            
            # Process candles chronologically with day boundary detection
            for candle in candles:
                candleUnix = candle.unixTime
                candleDay = candleUnix // 86400  # Get day number for this candle
                
                # Check if we need to reset VWAP session (new day)
                if sessionEndUnix is not None:
                    if CommonUtil.isNewDay(candleUnix, sessionEndUnix):
                        # Day boundary crossed - reset VWAP session for new day
                        logger.info(f"Day boundary detected for {tokenAddress} {timeframeRecord.timeframe}: "
                                  f"candle day {candleDay} > session day {sessionEndUnix // 86400}")
                        
                        # Reset for new day
                        currentCumulativePV = Decimal('0')
                        currentCumulativeVolume = Decimal('0')
                        sessionStartUnix, sessionEndUnix = CommonUtil.getSessionStartAndEndUnix(candleUnix)
                        hasExistingSession = False  # This is now a fresh session for new day
                
                # If no existing session, initialize day boundaries from first candle
                if sessionStartUnix is None:
                    sessionStartUnix, sessionEndUnix = CommonUtil.getSessionStartAndEndUnix(candleUnix)
                
                # Calculate VWAP for this candle
                typicalPrice = (candle.highPrice + candle.lowPrice + candle.closePrice) / Decimal('3')
                priceVolume = typicalPrice * candle.volume
                
                # Update cumulative values
                currentCumulativePV += priceVolume
                currentCumulativeVolume += candle.volume
                
                # Calculate current VWAP and update the candle POJO directly
                if currentCumulativeVolume > 0:
                    currentVWAP = currentCumulativePV / currentCumulativeVolume
                    candle.updateVWAPValue(float(currentVWAP))
            
            # Update VWAPSession POJO with final session data
            if candles:  # Only if we processed any candles
                lastCandleUnix = candles[-1].unixTime
                timeframeSeconds = CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
                
                timeframeRecord.vwapSession = VWAPSession(
                    tokenAddress=tokenAddress,
                    pairAddress=pairAddress,
                    timeframe=timeframeRecord.timeframe,
                    sessionStartUnix=sessionStartUnix,
                    sessionEndUnix=sessionEndUnix,
                    cumulativePV=float(currentCumulativePV),
                    cumulativeVolume=float(currentCumulativeVolume),
                    currentVWAP=float(currentCumulativePV / currentCumulativeVolume) if currentCumulativeVolume > 0 else 0.0,
                    lastCandleUnix=lastCandleUnix,
                    nextCandleFetch=lastCandleUnix + timeframeSeconds
                )
                
                logger.info(f"Updated VWAP session for {tokenAddress} {timeframeRecord.timeframe}: "
                           f"VWAP={timeframeRecord.vwapSession.currentVWAP:.8f}")
            
        except Exception as e:
            logger.error(f"Error calculating VWAP with POJOs for {tokenAddress} {timeframeRecord.timeframe}: {e}")

    def calculateVWAPInMemory(self, timeframeRecord, tokenAddress: str, pairAddress: str) -> None:
        try:
            if not timeframeRecord.ohlcvDetails:
                logger.warning(f"No candles available for VWAP calculation: {tokenAddress} {timeframeRecord.timeframe}")
                return
            
            currentTime = int(time.time())
            timeframeSeconds = CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
            dayStart = self.calculateDayStart(currentTime)
            _, dayEnd = CommonUtil.getSessionStartAndEndUnix(dayStart)
            
            # Filter only today's candles
            todayCandles = [candle for candle in timeframeRecord.ohlcvDetails 
                           if dayStart <= candle.unixTime <= dayEnd]
            
            if not todayCandles:
                logger.info(f"No today's candles for VWAP calculation: {tokenAddress} {timeframeRecord.timeframe}")
                # Create empty VWAP session for today
                timeframeRecord.vwapSession = VWAPSession(
                    tokenAddress=tokenAddress,
                    pairAddress=pairAddress,
                    timeframe=timeframeRecord.timeframe,
                    sessionStartUnix=dayStart,
                    sessionEndUnix=dayEnd,
                    cumulativePV=0.0,
                    cumulativeVolume=0.0,
                    currentVWAP=0.0,
                    lastCandleUnix=None,
                    nextCandleFetch=None
                )
                return
            
            logger.info(f"Processing VWAP for {tokenAddress} {timeframeRecord.timeframe} with {len(todayCandles)} today's candles")
            
            # Calculate VWAP values for today's candles only
            cumulativePV = Decimal('0')
            cumulativeVolume = Decimal('0')
            
            for candle in todayCandles:
                # Calculate typical price (HLC/3)
                typicalPrice = (candle.highPrice + candle.lowPrice + candle.closePrice) / Decimal('3')
                priceVolume = typicalPrice * candle.volume
                
                # Update cumulative values
                cumulativePV += priceVolume
                cumulativeVolume += candle.volume
                
                # Calculate current VWAP and update the candle
                if cumulativeVolume > 0:
                    currentVWAP = cumulativePV / cumulativeVolume
                    candle.updateVWAPValue(float(currentVWAP))
            
            
            
            timeframeRecord.vwapSession = VWAPSession(
                tokenAddress=tokenAddress,
                pairAddress=pairAddress,
                timeframe=timeframeRecord.timeframe,
                sessionStartUnix=dayStart,
                sessionEndUnix=dayEnd,
                cumulativePV=float(cumulativePV),
                cumulativeVolume=float(cumulativeVolume),
                currentVWAP=float(currentVWAP) if cumulativeVolume > 0 else 0.0,
                lastCandleUnix=todayCandles[-1].unixTime if todayCandles else None,
                nextCandleFetch=todayCandles[-1].unixTime + timeframeSeconds if todayCandles else None
            )
            
            logger.info(f"Calculated VWAP for {tokenAddress} {timeframeRecord.timeframe}: {timeframeRecord.vwapSession.currentVWAP:.8f} (from {len(todayCandles)} today's candles)")
            
        except Exception as e:
            logger.error(f"Error calculating VWAP in memory for {tokenAddress} {timeframeRecord.timeframe}: {e}")
    
    
