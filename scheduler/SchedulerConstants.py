"""
SchedulerConstants - Constants for scheduler operations

Contains all constant values used across scheduler components to prevent
hardcoded strings that could break the system if changed.
"""

class FetchResultKeys:
    """Constants for fetch result dictionary keys"""
    SUCCESSFUL_TOKENS = 'successful_tokens'
    FAILED_TOKENS = 'failed_tokens' 
    SUCCESSFUL_TOKENS_LIST = 'successful_tokens_list'

class CandleDataKeys:
    """Constants for candle data dictionary keys"""
    CANDLES = 'candles'
    LATEST_TIME = 'latest_time'
    COUNT = 'count'

class TokenKeys:
    """Constants for token dictionary keys"""
    TOKEN_ADDRESS = 'tokenaddress'
    PAIR_ADDRESS = 'pairaddress'
    SYMBOL = 'symbol'
    LAST_FETCHED_AT = 'lastfetchedat'
    PAIR_CREATED_TIME = 'paircreatedtime'

class CandleKeys:
    """Constants for individual candle dictionary keys"""
    UNIX_TIME = 'unixtime'
    OPEN_PRICE = 'openprice'
    HIGH_PRICE = 'highprice'
    LOW_PRICE = 'lowprice'
    CLOSE_PRICE = 'closeprice'
    VOLUME = 'volume'
    TIMEFRAME = 'timeframe'
    DATASOURCE = 'datasource'

class Timeframes:
    """Constants for timeframe values"""
    FIFTEEN_MIN = '15m'
    ONE_HOUR = '1h'
    FOUR_HOUR = '4h'

class DataSources:
    """Constants for data source values"""
    BIRDEYE = 'birdeye'
    AGGREGATED = 'aggregated'