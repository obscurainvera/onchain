"""
AVWAPProcessor - Handles AVWAP (Anchored Volume Weighted Average Price) processing for trading tokens

This processor manages AVWAP state and value persistence based on user-provided data from the API.
Unlike VWAP which is calculated from candle data, AVWAP values are provided by users and stored
as reference points for trading decisions.

FUNCTIONALITY:
==============

1. setAVWAPForTokenFromAPI: Process user-provided AVWAP data and persist to database
   - Validates AVWAP data structure and values
   - Creates avwapstates records with proper timestamps
   - Updates ohlcvdetails.avwapvalue for reference candles
   - Handles both new and old token flows

2. Data Structure Expected:
   avwap: {
       "1h": {"value": "10.5", "referenceTime": "1234567890"},
       "30min": {"value": "10.3", "referenceTime": "1234567890"},
       "4h": {"value": "10.7", "referenceTime": "1234567890"}
   }

3. Database Operations:
   - INSERT/UPDATE avwapstates table with AVWAP values and timestamps
   - UPDATE ohlcvdetails.avwapvalue for the reference candle
   - Calculate nextfetchtime based on timeframe intervals

USAGE:
======
- Called from TradingActionEnhanced after EMA processing
- Processes both new and old token AVWAP data uniformly
- Maintains consistency with existing processor patterns
"""

from typing import List, TYPE_CHECKING
from logs.logger import get_logger

from utils.CommonUtil import CommonUtil
from api.trading.request.AVWAPState import AVWAPState

if TYPE_CHECKING:
    from api.trading.request import TrackedToken

logger = get_logger(__name__)


class AVWAPProcessor:
    """Processor for AVWAP (Anchored Volume Weighted Average Price) operations"""
    
    def __init__(self, trading_handler, moralis_handler=None):
        """
        Initialize AVWAP processor with required dependencies
        
        Args:
            trading_handler: TradingHandler instance for database operations
            moralis_handler: MoralisServiceHandler instance for data fetching (optional)
        """
        self.trading_handler = trading_handler
        self.moralis_handler = moralis_handler
    
    
    
    
    
   
    
    
    def calculateAVWAPInMemory(self, timeframeRecord, tokenAddress: str, pairAddress: str) -> None:
        try:
            if not timeframeRecord.ohlcvDetails:
                logger.warning(f"No candles available for AVWAP calculation: {tokenAddress} {timeframeRecord.timeframe}")
                return
            
            logger.info(f"Processing AVWAP for {tokenAddress} {timeframeRecord.timeframe} with {len(timeframeRecord.ohlcvDetails)} candles")
            
            # Calculate cumulative values
            cumulativePV = 0.0
            cumulativeVolume = 0.0
            lastUpdatedUnix = 0
            
            for candle in timeframeRecord.ohlcvDetails:
                # Calculate typical price (HLC/3)
                typicalPrice = (candle.highPrice + candle.lowPrice + candle.closePrice) / 3.0
                priceVolume = typicalPrice * candle.volume
                
                # Update cumulative values
                cumulativePV += priceVolume
                cumulativeVolume += candle.volume
                lastUpdatedUnix = max(lastUpdatedUnix, candle.unixTime)
                
                # Calculate AVWAP for this candle
                if cumulativeVolume > 0:
                    currentAVWAP = cumulativePV / cumulativeVolume
                    candle.updateAVWAPValue(currentAVWAP)
        
            
            timeframeSeconds = CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
            nextFetchTime = lastUpdatedUnix + timeframeSeconds if lastUpdatedUnix else None
            
            timeframeRecord.avwapState = AVWAPState(
                tokenAddress=tokenAddress,
                pairAddress=pairAddress,
                timeframe=timeframeRecord.timeframe,
                avwap=cumulativePV / cumulativeVolume if cumulativeVolume > 0 else None,
                cumulativePV=cumulativePV,
                cumulativeVolume=cumulativeVolume,
                lastUpdatedUnix=lastUpdatedUnix,
                nextFetchTime=nextFetchTime
            )
            
            logger.info(f"Calculated AVWAP for {tokenAddress} {timeframeRecord.timeframe}: {timeframeRecord.avwapState.avwap:.8f}")
            
        except Exception as e:
            logger.error(f"Error calculating AVWAP in memory for {tokenAddress} {timeframeRecord.timeframe}: {e}")

    def calculateAVWAPForAllTrackedTokens(self, trackedTokens: List['TrackedToken']) -> None:
        
        try:
            totalProcessed = 0
            
            for trackedToken in trackedTokens:
                for timeframeRecord in trackedToken.timeframeRecords:
                    if timeframeRecord.avwapState and timeframeRecord.ohlcvDetails:
                        # Calculate AVWAP incrementally using existing state
                        self.calculateAVWAPIncrementalWithPOJOs(
                            timeframeRecord, trackedToken.tokenAddress, trackedToken.pairAddress
                        )
                        totalProcessed += 1
            
            logger.info(f"Processed AVWAP calculations for {totalProcessed} timeframe records using POJOs")
        
        except Exception as e:
            logger.error(f"Error processing AVWAP calculations with POJOs: {e}")

    def calculateAVWAPIncrementalWithPOJOs(self, timeframeRecord, tokenAddress: str, pairAddress: str) -> None:
        try:
            avwapState = timeframeRecord.avwapState
            candles = timeframeRecord.ohlcvDetails
            
            if not avwapState or not candles:
                logger.warning(f"No AVWAP state or candles available for {tokenAddress} {timeframeRecord.timeframe}")
                return
            
            # Sort candles by unixTime to ensure chronological processing
            candles.sort(key=lambda x: x.unixTime)
            
            # Initialize cumulative values from existing AVWAP state
            currentCumulativePV = avwapState.cumulativePV or 0.0
            currentCumulativeVolume = avwapState.cumulativeVolume or 0.0
            latestUnix = avwapState.lastUpdatedUnix or 0
            
            # Process only new candles (after lastUpdatedUnix)
            newCandles = [c for c in candles if c.unixTime > latestUnix]
            
            if not newCandles:
                logger.debug(f"No new candles for AVWAP update: {tokenAddress} {timeframeRecord.timeframe}")
                return
            
            logger.info(f"Processing {len(newCandles)} new candles for AVWAP: {tokenAddress} {timeframeRecord.timeframe}")
            
            # Process new candles incrementally
            for candle in newCandles:
                # Calculate typical price (HLC/3) for AVWAP
                typicalPrice = (candle.highPrice + candle.lowPrice + candle.closePrice) / 3.0
                priceVolume = typicalPrice * candle.volume
                
                # Update cumulative values
                currentCumulativePV += priceVolume
                currentCumulativeVolume += candle.volume
                
                # Calculate current AVWAP and update the candle
                if currentCumulativeVolume > 0:
                    currentAVWAP = currentCumulativePV / currentCumulativeVolume
                    candle.updateAVWAPValue(currentAVWAP)
                    latestUnix = candle.unixTime
            
            # Update AVWAPState POJO with new values
            avwapState.avwap = currentCumulativePV / currentCumulativeVolume if currentCumulativeVolume > 0 else 0.0
            avwapState.cumulativePV = currentCumulativePV
            avwapState.cumulativeVolume = currentCumulativeVolume
            avwapState.lastUpdatedUnix = latestUnix
            avwapState.nextFetchTime = latestUnix + CommonUtil.getTimeframeSeconds(timeframeRecord.timeframe)
            
            logger.info(f"Updated AVWAP for {tokenAddress} {timeframeRecord.timeframe}: {avwapState.avwap:.8f} (processed {len(newCandles)} new candles)")
            
        except Exception as e:
            logger.error(f"Error calculating AVWAP incrementally with POJOs for {tokenAddress} {timeframeRecord.timeframe}: {e}")
    
