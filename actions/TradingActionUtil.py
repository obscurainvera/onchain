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
    def calculateNextCandleFetch(timeframe: str, last_candle_time: int) -> int:
        """Calculate when the next candle fetch should occur"""
        timeframe_seconds = TradingActionUtil.getTimeframeSeconds(timeframe)
        return last_candle_time + timeframe_seconds
    
    # ===============================================================
    # DATA AGGREGATION UTILITIES
    # ===============================================================
    
    @staticmethod
    def aggregateToHourlyInMemory(candles_15m: List[Dict]) -> Dict:
        """
        Aggregate 15min candles to hourly in memory - returns candles and latest time
        
        Args:
            candles_15m: List of 15-minute candles with keys: unixtime, openprice, highprice, lowprice, closeprice, volume
            
        Returns:
            Dict with keys: 'candles' (List[Dict]), 'latest_time' (int)
        """
        # Group candles by hour periods
        hour_groups = {}
        for candle in candles_15m:
            hour_bucket = (candle['unixtime'] // 3600) * 3600
            if hour_bucket not in hour_groups:
                hour_groups[hour_bucket] = []
            hour_groups[hour_bucket].append(candle)
        
        # Process each hour group
        hourly_candles = []
        latest_time = None
        
        for hour_start, candles in hour_groups.items():
            if len(candles) == 4:  # Must have exactly 4 candles
                # Verify they are the right times (:00, :15, :30, :45)
                expected_minutes = [0, 15, 30, 45]
                actual_minutes = [datetime.fromtimestamp(c['unixtime']).minute for c in candles]
                actual_minutes.sort()
                
                if actual_minutes == expected_minutes:
                    # Create 1h candle from 4 complete 15min candles
                    sorted_candles = sorted(candles, key=lambda x: x['unixtime'])
                    
                    hourly_candle = {
                        'unixtime': hour_start,
                        'openprice': sorted_candles[0]['openprice'],
                        'closeprice': sorted_candles[-1]['closeprice'],
                        'highprice': max(c['highprice'] for c in sorted_candles),
                        'lowprice': min(c['lowprice'] for c in sorted_candles),
                        'volume': sum(c['volume'] for c in sorted_candles)
                    }
                    
                    hourly_candles.append(hourly_candle)
                    
                    # Track latest time during aggregation
                    if latest_time is None or hour_start > latest_time:
                        latest_time = hour_start
        
        return {
            'candles': hourly_candles,
            'latest_time': latest_time
        }
    
    @staticmethod
    def aggregateTo4HourlyInMemory(candles_1h: List[Dict]) -> Dict:
        """
        Aggregate 1h candles to 4-hourly in memory - returns candles and latest time
        
        Args:
            candles_1h: List of 1-hour candles with keys: unixtime, openprice, highprice, lowprice, closeprice, volume
            
        Returns:
            Dict with keys: 'candles' (List[Dict]), 'latest_time' (int)
        """
        # Group candles by 4-hour periods (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
        four_hour_groups = {}
        for candle in candles_1h:
            # Calculate 4-hour bucket (aligned to 00:00, 04:00, 08:00, etc.)
            four_hour_bucket = (candle['unixtime'] // 14400) * 14400
            if four_hour_bucket not in four_hour_groups:
                four_hour_groups[four_hour_bucket] = []
            four_hour_groups[four_hour_bucket].append(candle)
        
        # Process each 4-hour group
        four_hourly_candles = []
        latest_time = None
        
        for four_hour_start, candles in four_hour_groups.items():
            if len(candles) == 4:  # Must have exactly 4 hourly candles
                # Verify they are the right times (00, 01, 02, 03 or 04, 05, 06, 07, etc.)
                expected_hours = [(four_hour_start // 3600 + i) % 24 for i in range(4)]
                actual_hours = [datetime.fromtimestamp(c['unixtime']).hour for c in candles]
                actual_hours.sort()
                expected_hours.sort()
                
                if actual_hours == expected_hours:
                    # Create 4h candle from 4 complete 1h candles
                    sorted_candles = sorted(candles, key=lambda x: x['unixtime'])
                    
                    four_hourly_candle = {
                        'unixtime': four_hour_start,
                        'openprice': sorted_candles[0]['openprice'],
                        'closeprice': sorted_candles[-1]['closeprice'],
                        'highprice': max(c['highprice'] for c in sorted_candles),
                        'lowprice': min(c['lowprice'] for c in sorted_candles),
                        'volume': sum(c['volume'] for c in sorted_candles)
                    }
                    
                    four_hourly_candles.append(four_hourly_candle)
                    
                    # Track latest time during aggregation
                    if latest_time is None or four_hour_start > latest_time:
                        latest_time = four_hour_start
        
        return {
            'candles': four_hourly_candles,
            'latest_time': latest_time
        }
    
    # ===============================================================
    # VWAP CALCULATION UTILITIES
    # ===============================================================
    
    @staticmethod
    def calculateVWAPForCandles(candles: List[Dict]) -> Dict:
        """
        Calculate VWAP from a list of candles
        
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
                'last_candle_time': None
            }
        
        cumulative_pv = Decimal('0')
        cumulative_volume = Decimal('0')
        last_candle_time = None
        
        for candle in candles:
            # Calculate typical price (HLC/3)
            high = Decimal(str(candle['highprice']))
            low = Decimal(str(candle['lowprice']))
            close = Decimal(str(candle['closeprice']))
            volume = Decimal(str(candle['volume']))
            
            typical_price = (high + low + close) / Decimal('3')
            price_volume = typical_price * volume
            
            cumulative_pv += price_volume
            cumulative_volume += volume
            
            # Track the last candle time
            if last_candle_time is None or candle['unixtime'] > last_candle_time:
                last_candle_time = candle['unixtime']
        
        # Calculate final VWAP
        final_vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else Decimal('0')
        
        return {
            'cumulative_pv': float(cumulative_pv),
            'cumulative_volume': float(cumulative_volume),
            'final_vwap': float(final_vwap),
            'last_candle_time': last_candle_time
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
    def prepareEMAStateData(tokenAddress: str, pairAddress: str, timeframe: str, 
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
        timeframe_seconds = TradingActionUtil.getTimeframeSeconds(timeframe)
        next_fetch_time = referenceUnixTime + timeframe_seconds
        
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
            'nextFetchTime': next_fetch_time
        }
    
    @staticmethod
    def prepareEMACandleUpdate(tokenAddress: str, timeframe: str, ema_period: int, 
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