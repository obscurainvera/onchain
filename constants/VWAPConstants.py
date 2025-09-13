"""
VWAP Constants - Centralized field names for VWAP operations

Simple constants to prevent field name mismatches across VWAP processing.
"""

# VWAP Candle Update Field Names
TOKEN_ADDRESS = 'tokenAddress'
PAIR_ADDRESS = 'pairAddress'
TIMEFRAME = 'timeframe'
CANDLE_UNIX = 'candleUnix'
VWAP_VALUE = 'vwapValue'

# VWAP Session Update Field Names
SESSION_TYPE = 'type'
SESSION_START_UNIX = 'sessionStartUnix'
SESSION_END_UNIX = 'sessionEndUnix'
CUMULATIVE_PV = 'cumulativePV'
CUMULATIVE_VOLUME = 'cumulativeVolume'
CURRENT_VWAP = 'currentVWAP'
LAST_CANDLE_UNIX = 'lastCandleUnix'
NEXT_CANDLE_FETCH = 'nextCandleFetch'

# VWAP Session Types
INCREMENTAL = 'incremental'
FULL_RESET = 'full_reset'

# VWAP Data Structure Keys
CANDLE_UPDATES = 'candleUpdates'
SESSION_UPDATES = 'sessionUpdates'
