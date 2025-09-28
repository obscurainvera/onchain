"""
Indicator Constants - Database column names for all indicator tables

Centralized constants to prevent field name mismatches when retrieving data from database.
All constants match the actual database schema column names.
"""

class IndicatorConstants:
    """Database column name constants for all indicator tables"""
    
    # trackedtokens table columns
    class VWAPSessions:
        CANDLE_VWAPS = 'candle_vwaps'
        TODAY_CANDLES = 'today_candles'
        HAS_CANDLES = 'has_candles'
        CANDLES = 'candles'
        CANDLE_UPDATES = 'candle_updates'
        SESSION_UPDATES = 'session_updates'
        SESSION_TYPE = 'session_type'
        INCREMENTAL = 'incremental'
        FULL_RESET = 'full_reset'
        

    class EMAStates:
        EMA_21 = 21
        EMA_34 = 34
        EMA_VALUE = 'emavalue'
        EMA_PERIOD = 'emaperiod'
        CANDLE_UNIX_TIME = 'candle_unixtime' #used in query
        CANDLE_CLOSE_PRICE = 'candle_closeprice' #used in query
        CANDLES = 'candles'
        PAIR_ID = 'pair_id'
        EMA21 = 'ema21'
        EMA34 = 'ema34'

    class AVWAPStates:
        AVWAP = 'avwap'
        CUMULATIVE_PV = 'cumulativePV'
        CUMULATIVE_VOLUME = 'cumulativeVolume'
        LAST_UPDATED_UNIX = 'lastUpdatedUnix'

        