"""
TradingActionUtil - Utility class for trading action helper methods

This class contains pure utility methods that don't require database connections
or instance state, making them easily testable and reusable.
"""

from typing import List, Dict, Any
from decimal import Decimal
from datetime import datetime
import logging

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
        """Convert timeframe string to seconds"""
        timeframe_map = {
            '15m': 900,
            '1h': 3600,
            '4h': 14400
        }
        return timeframe_map.get(timeframe, 900)
    
    @staticmethod
    def calculateNextCandleFetch(timeframe: str, latestCandleTime: int) -> int:
        """Calculate when the next candle fetch should occur"""
        timeFrameInSeconds = TradingActionUtil.getTimeframeSeconds(timeframe)
        return latestCandleTime + timeFrameInSeconds
    
    # ===============================================================
    # DATA AGGREGATION UTILITIES
    # ===============================================================
    
    @staticmethod
    def aggregateToHourlyInMemory(all15MinCandles: List[Dict]) -> Dict:
        """
        Aggregate 15min candles to hourly in memory - returns candles and latest time
        
        Args:
            candles_15m: List of 15-minute candles with keys: unixtime, openprice, highprice, lowprice, closeprice, volume
            
        Returns:
            Dict with keys: 'candles' (List[Dict]), 'latest_time' (int)
        """
        # Group candles by hour periods
        hourGroups = {}
        for candle in all15MinCandles:
            # Calculate the hour bucket by truncating to the nearest hour (e.g., 1609459200 for 2021-01-01 00:00:00)
            hourBucket = (candle['unixtime'] // 3600) * 3600
            if hourBucket not in hourGroups:
                hourGroups[hourBucket] = []
            hourGroups[hourBucket].append(candle)
        
        # Process each hour group
        hourlyCandles = []
        latestTime = None
        
        for hourStart, candles in hourGroups.items():
            if len(candles) == 4:  # Must have exactly 4 candles
                # Verify they are the right times (:00, :15, :30, :45)
                expectedMinutes = [0, 15, 30, 45]
                actualMinutes = [datetime.fromtimestamp(c['unixtime']).minute for c in candles]
                actualMinutes.sort()
                
                if actualMinutes == expectedMinutes:
                    # OPTIMIZED: No sorting needed - find candles by minute directly
                    openCandle = next(c for c in candles if datetime.fromtimestamp(c['unixtime']).minute == 0)
                    closeCandle = next(c for c in candles if datetime.fromtimestamp(c['unixtime']).minute == 45)
                    
                    hourlyCandle = {
                        'unixtime': hourStart,
                        'openprice': openCandle['openprice'],
                        'closeprice': closeCandle['closeprice'],
                        'highprice': max(c['highprice'] for c in candles),
                        'lowprice': min(c['lowprice'] for c in candles),
                        'volume': sum(c['volume'] for c in candles)
                    }
                    
                    hourlyCandles.append(hourlyCandle)
                    
                    # Track latest time during aggregation
                    if latestTime is None or hourStart > latestTime:
                        latestTime = hourStart
        
        return {
            'candles': hourlyCandles,
            'latest_time': latestTime,
            'next_fetch_time': latestTime + 3600 if latestTime else None  # 1 hour = 3600 seconds
        }
    
    @staticmethod
    def aggregateToFourHourlyInMemory(all1HrCandles: List[Dict]) -> Dict:
        """
        Aggregate 1h candles to 4-hourly in memory - returns candles and latest time
        
        Args:
            candles_1h: List of 1-hour candles with keys: unixtime, openprice, highprice, lowprice, closeprice, volume
            
        Returns:
            Dict with keys: 'candles' (List[Dict]), 'latest_time' (int)
        """
        # Group candles by 4-hour periods (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
        fourHourGroups = {}
        for candle in all1HrCandles:
            # Calculate 4-hour bucket (aligned to 00:00, 04:00, 08:00, etc.)
            fourHourBucket = (candle['unixtime'] // 14400) * 14400
            if fourHourBucket not in fourHourGroups:
                fourHourGroups[fourHourBucket] = []
            fourHourGroups[fourHourBucket].append(candle)
        
        # Process each 4-hour group
        fourHourlyCandles = []
        latestTime = None
        
        for fourHourStart, candles in fourHourGroups.items():
            if len(candles) == 4:  # Must have exactly 4 hourly candles
                # Verify they are the right times (00, 01, 02, 03 or 04, 05, 06, 07, etc.)
                expectedHoursCandle = [(fourHourStart // 3600 + i) % 24 for i in range(4)]
                actualHoursCandle = [datetime.fromtimestamp(c['unixtime']).hour for c in candles]
                actualHoursCandle.sort()
                expectedHoursCandle.sort()
                
                if actualHoursCandle == expectedHoursCandle:
                    # OPTIMIZED: No sorting needed - find candles by hour directly
                    openCandle = next(c for c in candles if datetime.fromtimestamp(c['unixtime']).hour == actualHoursCandle[0])
                    clodeCandle = next(c for c in candles if datetime.fromtimestamp(c['unixtime']).hour == actualHoursCandle[-1])
                    
                    fourHourlyCandle = {
                        'unixtime': fourHourStart,
                        'openprice': openCandle['openprice'],
                        'closeprice': clodeCandle['closeprice'],
                        'highprice': max(c['highprice'] for c in candles),
                        'lowprice': min(c['lowprice'] for c in candles),
                        'volume': sum(c['volume'] for c in candles)
                    }
                    
                    fourHourlyCandles.append(fourHourlyCandle)
                    
                    # Track latest time during aggregation
                    if latestTime is None or fourHourStart > latestTime:
                        latestTime = fourHourStart
        
        return {
            'candles': fourHourlyCandles,
            'latest_time': latestTime,
            'next_fetch_time': latestTime + 14400 if latestTime else None  # 4 hours = 14400 seconds
        }
    
    # ===============================================================
    # VWAP CALCULATION UTILITIES
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
        day_end = day_start + 86400  # 24 hours later
        return [c for c in candles if day_start <= c['unixtime'] < day_end]
    
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
            'tokenAddress': tokenAddress,
            'pairAddress': pairAddress,
            'timeframe': timeframe,
            'emaKey': str(ema_period),
            'pairCreatedTime': pairCreatedTime,
            'emaAvailableTime': referenceUnixTime,
            'emaValue': ema_value,
            'status': status,
            'lastUpdatedUnix': referenceUnixTime,
            'nextFetchTime': nextFetchTime
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
            'tokenAddress': tokenAddress,
            'timeframe': timeframe,
            'ema_period': ema_period,
            'unixtime': unixtime,
            'ema_value': ema_value
        }