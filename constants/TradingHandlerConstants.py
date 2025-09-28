"""
TradingHandler Constants - Database column names for all trading tables

Centralized constants to prevent field name mismatches when retrieving data from database.
All constants match the actual database schema column names.
"""


class TradingHandlerConstants:
    """Database column name constants for all trading tables"""
    
    # trackedtokens table columns
    class TrackedTokens:
        TRACKED_TOKEN_ID = 'trackedtokenid'
        TOKEN_ADDRESS = 'tokenaddress'
        SYMBOL = 'symbol'
        NAME = 'name'
        PAIR_ADDRESS = 'pairaddress'
        PAIR_CREATED_TIME = 'paircreatedtime'
        ADDITION_SOURCE = 'additionsource'
        ADDED_BY = 'addedby'
        METADATA = 'metadata'
        STATUS = 'status'
        ENABLED_AT = 'enabledat'
        DISABLED_AT = 'disabledat'
        DISABLED_BY = 'disabledby'
        CREATED_AT = 'createdat'
        LAST_UPDATED_AT = 'lastupdatedat'
    
    # timeframemetadata table columns
    class TimeframeMetadata:
        ID = 'id'
        TOKEN_ADDRESS = 'tokenaddress'
        PAIR_ADDRESS = 'pairaddress'
        TIMEFRAME = 'timeframe'
        NEXT_FETCH_AT = 'nextfetchat'
        LAST_FETCHED_AT = 'lastfetchedat'
        IS_ACTIVE = 'isactive'
        CREATED_AT = 'createdat'
        LAST_UPDATED_AT = 'lastupdatedat'
        TIMEFRAME_ID = 'timeframeid'
    
    # ohlcvdetails table columns
    class OHLCVDetails:
        ID = 'id'
        TIMEFRAME_ID = 'timeframeid'
        TOKEN_ADDRESS = 'tokenaddress'
        PAIR_ADDRESS = 'pairaddress'
        TIMEFRAME = 'timeframe'
        UNIX_TIME = 'unixtime'
        TIME_BUCKET = 'timebucket'
        OPEN_PRICE = 'openprice'
        HIGH_PRICE = 'highprice'
        LOW_PRICE = 'lowprice'
        CLOSE_PRICE = 'closeprice'
        VOLUME = 'volume'
        TRADES = 'trades'
        DATA_SOURCE = 'datasource'
        IS_COMPLETE = 'iscomplete'
        EMA_21 = 'ema21'
        EMA_34 = 'ema34'
        CREATED_AT = 'createdat'
        LAST_UPDATED_AT = 'lastupdatedat'
        VWAP_VALUE = 'vwapvalue'
        AVWAP_VALUE = 'avwapvalue'
    
    # emastates table columns
    class EMAStates:
        TOKEN_ADDRESS = 'tokenaddress'
        PAIR_ADDRESS = 'pairaddress'
        TIMEFRAME = 'timeframe'
        EMA_KEY = 'emakey'
        EMA_VALUE = 'emavalue'
        LAST_UPDATED_UNIX = 'lastupdatedunix'
        NEXT_FETCH_TIME = 'nextfetchtime'
        EMA_AVAILABLE_TIME = 'emaavailabletime'
        PAIR_CREATED_TIME = 'paircreatedtime'
        STATUS = 'status'
        CREATED_AT = 'createdat'
        LAST_UPDATED_AT = 'lastupdatedat'
    
    # vwapsessions table columns
    class VWAPSessions:
        TOKEN_ADDRESS = 'tokenaddress'
        PAIR_ADDRESS = 'pairaddress'
        TIMEFRAME = 'timeframe'
        SESSION_START_UNIX = 'sessionstartunix'
        SESSION_END_UNIX = 'sessionendunix'
        CUMULATIVE_PV = 'cumulativepv'
        CUMULATIVE_VOLUME = 'cumulativevolume'
        CURRENT_VWAP = 'currentvwap'
        LAST_CANDLE_UNIX = 'lastcandleunix'
        NEXT_CANDLE_FETCH = 'nextcandlefetch'
        CREATED_AT = 'createdat'
        LAST_UPDATED_AT = 'lastupdatedat'
    
    # avwapstates table columns
    class AVWAPStates:
        TOKEN_ADDRESS = 'tokenaddress'
        PAIR_ADDRESS = 'pairaddress'
        TIMEFRAME = 'timeframe'
        AVWAP = 'avwap'
        CUMULATIVE_PV = 'cumulativepv'
        CUMULATIVE_VOLUME = 'cumulativevolume'
        LAST_UPDATED_UNIX = 'lastupdatedunix'
        NEXT_FETCH_TIME = 'nextfetchtime'
        CREATED_AT = 'createdat'
        LAST_UPDATED_AT = 'lastupdatedat'
    
    # Common field names used across multiple tables
    class Common:
        TOKEN_ADDRESS = 'tokenaddress'
        PAIR_ADDRESS = 'pairaddress'
        TIMEFRAME = 'timeframe'
        STATUS = 'status'
        CREATED_AT = 'createdat'
        LAST_UPDATED_AT = 'lastupdatedat'
