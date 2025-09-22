"""
OHLCV Details POJO - Clean data structure for candle data
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class OHLCVDetails:
    """POJO for OHLCV candle data with indicator values"""
    
    # Core OHLCV data
    timeframeId: Optional[int] = None
    tokenAddress: str = ""
    pairAddress: str = ""
    timeframe: str = ""
    unixTime: int = 0
    timeBucket: int = 0
    openPrice: float = 0.0
    highPrice: float = 0.0
    lowPrice: float = 0.0
    closePrice: float = 0.0
    volume: float = 0.0
    trades: int = 0
    isComplete: bool = True
    dataSource: str = "api"
    
    # Indicator values (calculated in memory)
    vwapValue: Optional[float] = None
    avwapValue: Optional[float] = None
    ema21Value: Optional[float] = None
    ema34Value: Optional[float] = None
    

    
    def updateVWAPValue(self, vwapValue: float):
        """Update VWAP value for this candle"""
        self.vwapValue = vwapValue
    
    def updateAVWAPValue(self, avwapValue: float):
        """Update AVWAP value for this candle"""
        self.avwapValue = avwapValue
    
    def updateEMAValues(self, ema21Value: float, ema34Value: float):
        """Update EMA values for this candle"""
        self.ema21Value = ema21Value
        self.ema34Value = ema34Value
    
    def toDict(self) -> dict:
        """Convert to dictionary for database insertion"""
        return {
            'timeframeid': self.timeframeId,
            'tokenaddress': self.tokenAddress,
            'pairaddress': self.pairAddress,
            'timeframe': self.timeframe,
            'unixtime': self.unixTime,
            'timebucket': self.timeBucket,
            'openprice': self.openPrice,
            'highprice': self.highPrice,
            'lowprice': self.lowPrice,
            'closeprice': self.closePrice,
            'volume': self.volume,
            'trades': self.trades,
            'vwapvalue': self.vwapValue,
            'avwapvalue': self.avwapValue,
            'ema21value': self.ema21Value,
            'ema34value': self.ema34Value,
            'iscomplete': self.isComplete,
            'datasource': self.dataSource
        }
