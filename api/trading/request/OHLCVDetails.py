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
    ema12Value: Optional[float] = None
    ema21Value: Optional[float] = None
    ema34Value: Optional[float] = None
    
    # RSI Indicator values
    rsiValue: Optional[float] = None
    stochRSIValue: Optional[float] = None
    stochRSIK: Optional[float] = None
    stochRSID: Optional[float] = None
    
    # Alert-related fields
    trend: Optional[str] = None
    status: Optional[str] = None
    trend12: Optional[str] = None
    status12: Optional[str] = None
    
    def updateVWAPValue(self, vwapValue: float):
        """Update VWAP value for this candle"""
        self.vwapValue = vwapValue
    
    def updateAVWAPValue(self, avwapValue: float):
        """Update AVWAP value for this candle"""
        self.avwapValue = avwapValue
    
    def updateEMA12Value(self, ema12Value: float):
        """Update EMA 12 value for this candle"""
        self.ema12Value = ema12Value
    
    def updateEMA12TrendStatus(self, trend12: str = None, status12: str = None):
        """Update EMA 12 trend and status for this candle"""
        if trend12 is not None:
            self.trend12 = trend12
        if status12 is not None:
            self.status12 = status12
    
    def updateEMAValues(self, ema12Value: float = None, ema21Value: float = None, ema34Value: float = None):
        """Update EMA values for this candle"""
        if ema12Value is not None:
            self.ema12Value = ema12Value
        if ema21Value is not None:
            self.ema21Value = ema21Value
        if ema34Value is not None:
            self.ema34Value = ema34Value
    
    def updateRSIValues(self, rsiValue: float = None, stochRSIValue: float = None, 
                        stochRSIK: float = None, stochRSID: float = None):
        """Update RSI indicator values for this candle"""
        if rsiValue is not None:
            self.rsiValue = rsiValue
        if stochRSIValue is not None:
            self.stochRSIValue = stochRSIValue
        if stochRSIK is not None:
            self.stochRSIK = stochRSIK
        if stochRSID is not None:
            self.stochRSID = stochRSID
    
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
            'ema12value': self.ema12Value,
            'ema21value': self.ema21Value,
            'ema34value': self.ema34Value,
            'rsivalue': self.rsiValue,
            'stochrsivalue': self.stochRSIValue,
            'stochrsik': self.stochRSIK,
            'stochrsid': self.stochRSID,
            'trend': self.trend,
            'status': self.status,
            'trend12': self.trend12,
            'status12': self.status12,
            'iscomplete': self.isComplete,
            'datasource': self.dataSource
        }
