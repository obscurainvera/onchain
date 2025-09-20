"""
Trading API Constants - Request parameter names and other API-related constants

Centralized constants for Trading API request parameters to prevent string mismatches.
"""

class TradingAPIConstants:
    """Constants for Trading API operations"""
    
    class RequestParameters:
        """Request parameter names used in Trading API endpoints"""
        TOKEN_ADDRESS = 'tokenAddress'
        PAIR_ADDRESS = 'pairAddress'
        ADDED_BY = 'addedBy'
        TIMEFRAMES = 'timeframes' 
        REFERENCE_TIME = 'referenceTime'
        VALUE = 'value'

    class Values:
        REQUIRED_TIMEFRAMES = ['30min', '1h', '4h']

    class Log:
        EMA_21_TYPE = 'ema21'
        EMA_34_TYPE = 'ema34'
        AVWAP_TYPE = 'avwap'
