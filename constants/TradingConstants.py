"""
Trading system constants for improved maintainability and modularity
"""

class TimeframeConstants:
    """Constants for timeframe operations"""
    
    # Supported Moralis timeframes
    THIRTY_MIN = '30min'
    ONE_HOUR = '1h'
    FOUR_HOUR = '4h'
    
    # Valid timeframes for new token flow
    VALID_NEW_TOKEN_TIMEFRAMES = [THIRTY_MIN, ONE_HOUR, FOUR_HOUR]
    
    # Timeframe to seconds mapping
    SECONDS_MAP = {
        THIRTY_MIN: 1800,
        ONE_HOUR: 3600,
        FOUR_HOUR: 14400
    }
    
    @classmethod
    def getSeconds(cls, timeframe: str) -> int:
        """Get seconds for a given timeframe"""
        return cls.SECONDS_MAP.get(timeframe, 0)
    
    @classmethod
    def isCorrectTimeframe(cls, timeframe: str) -> bool:
        """Check if timeframe is valid for new token flow"""
        return timeframe in cls.VALID_NEW_TOKEN_TIMEFRAMES

class TokenFlowConstants:
    """Constants for token addition flow"""
    
    # Age thresholds (in days)
    NEW_TOKEN_MAX_AGE_DAYS = 7
    
    # Adjustment multiplier for fromTime calculation
    FROM_TIME_BUFFER_MULTIPLIER = 2
    
    # Response modes
    MODE_NEW_TOKEN_WITH_TIMEFRAMES = 'new_token_with_timeframes'
    MODE_OLD_TOKEN_PER_TIMEFRAME_EMA = 'old_token_per_timeframe_ema'

class ValidationMessages:
    """Standardized validation and error messages"""
    
    TIMEFRAMES_REQUIRED = 'Timeframes array is required for new tokens (≤7 days)'
    INVALID_TIMEFRAMES = 'Invalid timeframes: {invalid_timeframes}'
    TOKEN_ALREADY_ACTIVE = 'Token {token_address} is already being tracked'
    FAILED_TO_ADD_TOKEN = 'Failed to add token to database'
    FAILED_TIMEFRAME_RECORDS = 'Failed to create timeframe metadata records'
    FAILED_FETCH_CANDLES = 'Failed to fetch {timeframe} candles: {error}'
    
    @classmethod
    def constructInvalidTimeframeMessage(cls, invalid_timeframes: list) -> str:
        return cls.INVALID_TIMEFRAMES.format(invalid_timeframes=invalid_timeframes)
    
    @classmethod
    def get_token_already_active_message(cls, token_address: str) -> str:
        return cls.TOKEN_ALREADY_ACTIVE.format(token_address=token_address)
    
    @classmethod
    def getErrorMessage(cls, timeframe: str, error: str) -> str:
        return cls.FAILED_FETCH_CANDLES.format(timeframe=timeframe, error=error)