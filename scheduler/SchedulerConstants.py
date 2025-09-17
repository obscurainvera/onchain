"""
SchedulerConstants - Constants for scheduler operations

Contains all constant values used across scheduler components to prevent
hardcoded strings that could break the system if changed.
"""



class CandleDataKeys:
    """Constants for candle data dictionary keys"""
    CANDLES = 'candles'
    LATEST_TIME = 'latest_time'
    COUNT = 'count'


class DataSources:
    """Constants for data source values"""
    BIRDEYE = 'birdeye'
    AGGREGATED = 'aggregated'