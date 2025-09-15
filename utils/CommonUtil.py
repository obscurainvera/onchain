from datetime import datetime, timezone
from typing import Tuple

class CommonUtil:
    """
    Common utility methods for date/time calculations
    """
    
    @staticmethod
    def getSessionStartAndEndUnix(unixTimestamp: int) -> Tuple[int, int]:
        """
        Get session start and end unix timestamps for a given unix timestamp.
        
        Args:
            unix_timestamp: Unix timestamp in seconds
            
        Returns:
            Tuple of (session_start_unix, session_end_unix)
            - session_start_unix: Start of day (00:00:00)
            - session_end_unix: End of day (23:59:59)
        """
        # Convert to datetime in UTC
        dt = datetime.fromtimestamp(unixTimestamp, tz=timezone.utc)
        
        # Get start of day (00:00:00)
        startOfTheDay = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        sessionStartUnix = int(startOfTheDay.timestamp())
        
        # Get end of day (23:59:59)
        endOfTheDay = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        sessionEndUnix = int(endOfTheDay.timestamp())
        
        return sessionStartUnix, sessionEndUnix
    
    @staticmethod
    def getDayBoundaries(unixTimestamp: int) -> Tuple[int, int]:
        """
        Alias for getSessionStartAndEndUnix for backward compatibility
        """
        return CommonUtil.getSessionStartAndEndUnix(unixTimestamp)
    
    @staticmethod
    def isNewDay(candleUnix: int, sessionEndUnix: int) -> bool:
        """
        Check if a candle unix timestamp represents a new day compared to session end.
        
        Args:
            candle_unix: Unix timestamp of the candle
            session_end_unix: Unix timestamp of session end
            
        Returns:
            True if candle is from a new day, False otherwise
        """
        candleDay = candleUnix // 86400
        sessionDay = sessionEndUnix // 86400
        return candleDay > sessionDay
    
    @staticmethod
    def getTimeframeSeconds(timeframe: str) -> int:
        """
        Convert timeframe string to seconds with support for various formats.
        
        Args:
            timeframe: Timeframe string (e.g., '30m', '1h', '4h', '15m', '1d', '1w')
            
        Returns:
            Number of seconds in the timeframe
            
        Raises:
            ValueError: If timeframe format is not recognized
        """
        # Normalize timeframe to lowercase
        tf = timeframe.lower().strip()
        
        # Handle minute timeframes
        if tf.endswith('m'):
            try:
                minutes = int(tf[:-1])
                return minutes * 60
            except ValueError:
                raise ValueError(f"Invalid minute timeframe format: {timeframe}")
        
        # Handle hour timeframes
        elif tf.endswith('h'):
            try:
                hours = int(tf[:-1])
                return hours * 3600
            except ValueError:
                raise ValueError(f"Invalid hour timeframe format: {timeframe}")
        
        # Handle day timeframes
        elif tf.endswith('d'):
            try:
                days = int(tf[:-1])
                return days * 86400
            except ValueError:
                raise ValueError(f"Invalid day timeframe format: {timeframe}")
        
        # Handle week timeframes
        elif tf.endswith('w'):
            try:
                weeks = int(tf[:-1])
                return weeks * 7 * 86400
            except ValueError:
                raise ValueError(f"Invalid week timeframe format: {timeframe}")
        
        # Handle numeric timeframes (assume minutes)
        elif tf.isdigit():
            minutes = int(tf)
            return minutes * 60
        
        # Handle common aliases
        timeframe_aliases = {
            '15min': 900,
            '30min': 1800,
            '1hour': 3600,
            '4hour': 14400,
            '1day': 86400,
            '1week': 604800
        }
        
        if tf in timeframe_aliases:
            return timeframe_aliases[tf]
        
        raise ValueError(f"Unsupported timeframe format: {timeframe}. "
                        f"Supported formats: 15m, 30m, 1h, 4h, 1d, 1w, etc.")
    
    @staticmethod
    def getTimeframeInSeconds(timeframe: str) -> int:
        """
        Alias for getTimeframeSeconds for backward compatibility
        """
        return CommonUtil.getTimeframeSeconds(timeframe)
    
    @staticmethod
    def calculateNextFetchTimeForTimeframe(latestTime: int, timeframe: str) -> int:
        """
        Calculate next fetch time based on specific timeframe.
        
        CRITICAL: This calculates when the NEXT candle will be available, not when the current candle ends.
        
        Example:
        - latestTime = 8:30 (last candle we fetched)
        - timeframe = '30m'
        - Returns: 9:30 (when the 9:00 candle will be available)
        
        The logic:
        1. Find the start of the current candle (8:00 for 8:30)
        2. Add one timeframe duration (8:00 + 30min = 8:30)
        3. Add another timeframe duration (8:30 + 30min = 9:00) - this is when next candle starts
        4. Add another timeframe duration (9:00 + 30min = 9:30) - this is when next candle is available
        
        Args:
            latestTime: Unix timestamp of the latest candle we fetched
            timeframe: Timeframe string ('15m', '30m', '1h', '4h', etc.)
            
        Returns:
            int: Unix timestamp when the next candle will be available for fetching
        """
        timeframeSeconds = CommonUtil.getTimeframeSeconds(timeframe)
    
        # Calculate next fetch time: current candle start + 2 timeframes
        currentCandleStart = (latestTime // timeframeSeconds) * timeframeSeconds
        nextFetchTime = currentCandleStart + (2 * timeframeSeconds)
        return nextFetchTime
    
    @staticmethod
    def calculateInitialStartTime(pairCreatedTime: int, timeframe: str) -> int:
        """
        Calculate the initial candle start time based on pair created time and timeframe.
        
        This function rounds down the pair created time to the nearest timeframe boundary.
        
        Example:
        - pairCreatedTime = 2:30 Sept 7 2025 (1757212200)
        - timeframe = '1h'
        - Returns: 2:00 Sept 7 2025 (1757210400) - the start of the 1h candle containing 2:30
        
        Args:
            pairCreatedTime: Unix timestamp when the pair was created
            timeframe: Timeframe string ('15m', '30m', '1h', '4h', '1d', etc.)
            
        Returns:
            int: Unix timestamp of the initial candle start time
        """
        timeframeSeconds = CommonUtil.getTimeframeSeconds(timeframe)
        return (pairCreatedTime // timeframeSeconds) * timeframeSeconds


    @staticmethod
    def calculateNextFetchTimeForInitialTimeframeRecord(pairCreatedTime: int, timeframe: str) -> int:
        """
        Calculate next fetch time based on specific timeframe.
        
        CRITICAL: This calculates when the NEXT candle will be available, not when the current candle ends.
        
        Example:
        - latestTime = 8:30 (last candle we fetched)
        - timeframe = '30m'
        - Returns: 9:30 (when the 9:00 candle will be available)
        
        The logic:
        1. Find the start of the current candle (8:00 for 8:30)
        2. Add one timeframe duration (8:00 + 30min = 8:30)
        3. Add another timeframe duration (8:30 + 30min = 9:00) - this is when next candle starts
        4. Add another timeframe duration (9:00 + 30min = 9:30) - this is when next candle is available
        
        Args:
            latestTime: Unix timestamp of the latest candle we fetched
            timeframe: Timeframe string ('15m', '30m', '1h', '4h', etc.)
            
        Returns:
            int: Unix timestamp when the next candle will be available for fetching
        """
        timeframeSeconds = CommonUtil.getTimeframeSeconds(timeframe)
    
        # Calculate next fetch time: current candle start + 2 timeframes
        currentCandleStart = CommonUtil.calculateInitialStartTime(pairCreatedTime, timeframe)
        nextFetchTime = currentCandleStart + timeframeSeconds
        return nextFetchTime
