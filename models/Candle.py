"""
Candle POJO class representing a single candle data point
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone


@dataclass
class Candle:
    """Represents a single candle/OHLCV data point"""
    
    tokenAddress: str
    pairAddress: str
    unixTime: int
    openPrice: float
    highPrice: float
    lowPrice: float
    closePrice: float
    volume: float
    timeframe: str
    dataSource: str
    trades: float = 0.0
    
    
    def isComplete(self, currentTime: Optional[int] = None) -> bool:
        
        if currentTime is None:
            currentTime = int(datetime.now(timezone.utc).timestamp())
        
        timeframeSeconds = self._getTimeframeSeconds()
        currentLiveCandleStartTime = (currentTime // timeframeSeconds) * timeframeSeconds
        
        return self.unixTime < currentLiveCandleStartTime
    
    def _getTimeframeSeconds(self) -> int:
        """Get timeframe in seconds"""
        timeframeMapping = {
            '30min': 1800,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400
        }
        return timeframeMapping.get(self.timeframe, 3600)  # Default to 1h
    
    def toDict(self) -> dict:
        """Convert candle to dictionary for database insertion"""
        return {
            'tokenaddress': self.tokenAddress,
            'pairaddress': self.pairAddress,
            'unixtime': self.unixTime,
            'openprice': self.openPrice,
            'highprice': self.highPrice,
            'lowprice': self.lowPrice,
            'closeprice': self.closePrice,
            'volume': self.volume,
            'timeframe': self.timeframe,
            'datasource': self.dataSource,
            'trades': self.trades
        }
    
    @classmethod
    def fromRawData(cls, rawCandle: dict, tokenAddress: str, pairAddress: str, 
                   timeframe: str, dataSource: str = 'moralis') -> 'Candle':
        """Create Candle from raw API data"""
        return cls(
            tokenAddress=tokenAddress,
            pairAddress=pairAddress,
            unixTime=rawCandle.get('unixTime', 0),
            openPrice=float(rawCandle.get('open', 0)),
            highPrice=float(rawCandle.get('high', 0)),
            lowPrice=float(rawCandle.get('low', 0)),
            closePrice=float(rawCandle.get('close', 0)),
            volume=float(rawCandle.get('volume', 0)),
            timeframe=timeframe,
            dataSource=dataSource,
            trades=float(rawCandle.get('trades', 0))
        )
