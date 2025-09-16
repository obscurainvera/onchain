"""
TradingActionUtil - Utility class for trading action helper methods

This class contains pure utility methods that don't require database connections
or instance state, making them easily testable and reusable.
"""

from typing import List, Dict, Any
from decimal import Decimal
from datetime import datetime
import logging
from utils import IndicatorConstants
from utils.CommonUtil import CommonUtil
from constants.TradingHandlerConstants import TradingHandlerConstants
from utils.IndicatorConstants import IndicatorConstants


logger = logging.getLogger(__name__)


class TradingActionUtil:
    """
    Utility class containing helper methods for trading operations.
    
    This class contains:
    - Timeframe conversion utilities
    - Data aggregation helpers
    - Calculation utilities
    - Data validation methods
    
    All methods are static and don't require instance state.
    """
    
    # ===============================================================
    # TIMEFRAME UTILITIES
    # ===============================================================
    
    @staticmethod
    def getTimeframeSeconds(timeframe: str) -> int:
        """Convert timeframe string to seconds - delegates to CommonUtil"""
        return CommonUtil.getTimeframeSeconds(timeframe)
    
    @staticmethod
    def calculateNextCandleFetch(timeframe: str, latestCandleTime: int) -> int:
        """Calculate when the next candle fetch should occur"""
        timeFrameInSeconds = TradingActionUtil.getTimeframeSeconds(timeframe)
        return latestCandleTime + timeFrameInSeconds
    
    # ===============================================================
    # DATA AGGREGATION UTILITIES
    # ===============================================================
    
   
    @staticmethod
    def calculateVWAPForSpecificTimeframe(candles: List[Dict]) -> Dict:
        """
        Calculate VWAP from a list of candles for a specific timeframe
        
        Args:
            candles: List of candle dicts with keys: highprice, lowprice, closeprice, volume, unixtime
            
        Returns:
            Dict with keys: cumulative_pv, cumulative_volume, final_vwap, last_candle_time
        """
        if not candles:
            return {
                'cumulative_pv': 0,
                'cumulative_volume': 0,
                'final_vwap': 0,
                'latest_candle_time': None
            }
        
        cumulativePV = Decimal('0')
        cumulativeVolumn = Decimal('0')
        latestCandleUnixTime = None
        
        for candle in candles:
            # Calculate typical price (HLC/3)
            high = Decimal(str(candle['highprice']))
            low = Decimal(str(candle['lowprice']))
            close = Decimal(str(candle['closeprice']))
            volume = Decimal(str(candle['volume']))
            
            price = (high + low + close) / Decimal('3')
            priceVolume = price * volume
            
            cumulativePV += priceVolume
            cumulativeVolumn += volume
            
            # Track the last candle time
            if latestCandleUnixTime is None or candle['unixtime'] > latestCandleUnixTime:
                latestCandleUnixTime = candle['unixtime']
        
        # Calculate final VWAP
        finalVwap = cumulativePV / cumulativeVolumn if cumulativeVolumn > 0 else Decimal('0')
        
        return {
            'cumulative_pv': float(cumulativePV),
            'cumulative_volume': float(cumulativeVolumn),
            'final_vwap': float(finalVwap),
            'latest_candle_time': latestCandleUnixTime
        }
    
    # ===============================================================
    # EMA CALCULATION UTILITIES
    # ===============================================================
    
    @staticmethod
    def calculateEMAFromCandles(candles: List[Dict], ema_period: int, close_price_key: str = 'closeprice') -> List[Dict]:
        """
        Calculate EMA from a list of candles using standard TradingView approach
        
        Args:
            candles: List of candle dicts sorted by unixtime (ascending)
            ema_period: EMA period (e.g., 21, 34)
            close_price_key: Key name for close price in candle dict
            
        Returns:
            List of dicts with keys: unixtime, ema_value, candle_count
        """
        if not candles or len(candles) < ema_period:
            return []
        
        ema_results = []
        ema_value = None
        multiplier = Decimal('2') / (Decimal(str(ema_period)) + Decimal('1'))
        
        # Sort candles by timestamp to ensure correct order
        sorted_candles = sorted(candles, key=lambda x: x['unixtime'])
        
        for i, candle in enumerate(sorted_candles):
            close_price = Decimal(str(candle[close_price_key]))
            
            if i < ema_period - 1:
                # First 19 candles: not enough data for EMA
                continue
            elif i == ema_period - 1:
                # 20th candle: Initialize EMA with SMA
                sma_sum = sum(Decimal(str(c[close_price_key])) for c in sorted_candles[i-ema_period+1:i+1])
                ema_value = sma_sum / Decimal(str(ema_period))
            else:
                # 21st candle onwards: Calculate EMA
                ema_value = (close_price * multiplier) + (ema_value * (Decimal('1') - multiplier))
            
            ema_results.append({
                'unixtime': candle['unixtime'],
                'ema_value': float(ema_value),
                'candle_count': i + 1
            })
        
        return ema_results
    
    # ===============================================================
    # DATA VALIDATION UTILITIES
    # ===============================================================
    
    @staticmethod
    def validateCandleData(candle: Dict, required_keys: List[str] = None) -> bool:
        """
        Validate that a candle dict contains required keys and valid data
        
        Args:
            candle: Candle dictionary to validate
            required_keys: List of required keys, defaults to standard OHLCV keys
            
        Returns:
            bool: True if valid, False otherwise
        """
        if required_keys is None:
            required_keys = ['unixtime', 'openprice', 'highprice', 'lowprice', 'closeprice', 'volume']
        
        # Check all required keys exist
        for key in required_keys:
            if key not in candle:
                return False
        
        # Validate price relationships (high >= low, etc.)
        try:
            high = float(candle['highprice'])
            low = float(candle['lowprice'])
            open_price = float(candle['openprice'])
            close = float(candle['closeprice'])
            volume = float(candle['volume'])
            
            # Basic validation rules
            if high < low:
                return False
            if high < open_price or high < close:
                return False
            if low > open_price or low > close:
                return False
            if volume < 0:
                return False
                
            return True
            
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def filterTodaysCandles(candles: List[Dict], day_start: int) -> List[Dict]:
        """
        Filter candles to only include those from the current day
        
        Args:
            candles: List of candle dicts
            day_start: Unix timestamp of day start (00:00 UTC)
            
        Returns:
            List of candles from today only
        """
        _, day_end = CommonUtil.getSessionStartAndEndUnix(day_start)
        return [c for c in candles if day_start <= c[TradingHandlerConstants.OHLCVDetails.UNIX_TIME] <= day_end]
    
    # ===============================================================
    # EMA STATE UTILITIES
    # ===============================================================
    
    @staticmethod
    def collectDataForEMAStateQueryFromAPI(tokenAddress: str, pairAddress: str, timeframe: str, 
                           ema_period: int, ema_value: Decimal, pairCreatedTime: int, 
                           referenceUnixTime: int, status: Any) -> Dict:
        """
        Prepare EMA state data dictionary for database operations
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Pair contract address  
            timeframe: Timeframe (15m, 1h, 4h)
            ema_period: EMA period (21, 34)
            ema_value: EMA value as Decimal
            pairCreatedTime: When pair was created
            referenceUnixTime: Reference timestamp
            status: EMA status enum value
            
        Returns:
            Dict formatted for database insertion
        """
        timeframeInSeconds = TradingActionUtil.getTimeframeSeconds(timeframe)
        nextFetchTime = referenceUnixTime + timeframeInSeconds
        
        return {
            TradingHandlerConstants.EMAStates.TOKEN_ADDRESS  : tokenAddress,
            TradingHandlerConstants.EMAStates.PAIR_ADDRESS: pairAddress,
            TradingHandlerConstants.EMAStates.TIMEFRAME: timeframe,
            TradingHandlerConstants.EMAStates.EMA_KEY: str(ema_period),
            TradingHandlerConstants.EMAStates.PAIR_CREATED_TIME: pairCreatedTime,
            TradingHandlerConstants.EMAStates.EMA_AVAILABLE_TIME: referenceUnixTime,
            TradingHandlerConstants.EMAStates.EMA_VALUE: ema_value,
            TradingHandlerConstants.EMAStates.STATUS: status,
            TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX: referenceUnixTime,
            TradingHandlerConstants.EMAStates.NEXT_FETCH_TIME: nextFetchTime
        }
    
    @staticmethod
    def collectDataForEMACandleUpdateQueryFromAPI(tokenAddress: str, timeframe: str, ema_period: int, 
                              unixtime: int, ema_value: Decimal) -> Dict:
        """
        Prepare EMA candle update data dictionary
        
        Args:
            tokenAddress: Token contract address
            timeframe: Timeframe (15m, 1h, 4h)
            ema_period: EMA period (21, 34)
            unixtime: Candle timestamp
            ema_value: EMA value as Decimal
            
        Returns:
            Dict formatted for candle update operations
        """
        return {
            TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS: tokenAddress,
            TradingHandlerConstants.OHLCVDetails.TIMEFRAME: timeframe,
            IndicatorConstants.EMAStates.EMA_PERIOD: ema_period,
            TradingHandlerConstants.OHLCVDetails.UNIX_TIME: unixtime,
            IndicatorConstants.EMAStates.EMA_VALUE: ema_value
        }