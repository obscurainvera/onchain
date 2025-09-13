"""
EMA Constants - Centralized field names for EMA operations

Simple constants to prevent field name mismatches across EMA processing.
"""

# EMA Candle Update Field Names
TOKEN_ADDRESS = 'tokenAddress'
PAIR_ADDRESS = 'pairAddress'
TIMEFRAME = 'timeframe'
CANDLE_UNIX = 'candleUnix'
EMA_VALUE = 'emaValue'
EMA_PERIOD = 'emaPeriod'

# EMA State Update Field Names
EMA_KEY = 'emaKey'
LAST_UPDATED_UNIX = 'lastUpdatedUnix'
NEXT_FETCH_TIME = 'nextFetchTime'
EMA_AVAILABLE_TIME = 'emaAvailableTime'
PAIR_CREATED_TIME = 'pairCreatedTime'
STATUS = 'status'

# EMA Periods
EMA_21 = 21
EMA_34 = 34

# EMA Status Values
NOT_AVAILABLE = 1
AVAILABLE = 2

# EMA Data Structure Keys
CANDLE_UPDATES = 'candleUpdates'
STATE_UPDATES = 'stateUpdates'
