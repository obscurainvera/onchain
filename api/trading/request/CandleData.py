"""
Candle Data POJO - Clean data structure for candle information
"""

from typing import List, Dict
from dataclasses import dataclass
from models.Candle import Candle


@dataclass
class TimeframeCandleData:
    """POJO for candle data for a specific timeframe"""
    
    timeframe: str
    candles: List[Candle]
    latestTime: int
    count: int
    creditsUsed: int = 0
    
    def __post_init__(self):
        """Validate the candle data"""
        if not self.timeframe:
            raise ValueError("timeframe is required")
        if not isinstance(self.candles, list):
            raise ValueError("candles must be a list")
        if self.count < 0:
            raise ValueError("count cannot be negative")


@dataclass
class AllTimeframesCandleData:
    """POJO for candle data across all timeframes"""
    
    tokenAddress: str
    pairAddress: str
    timeframeData: Dict[str, TimeframeCandleData]
    totalCandlesInserted: int = 0
    totalCreditsUsed: int = 0
    
    def __post_init__(self):
        """Validate and calculate totals"""
        if not self.tokenAddress:
            raise ValueError("tokenAddress is required")
        if not self.pairAddress:
            raise ValueError("pairAddress is required")
        if not self.timeframeData:
            raise ValueError("timeframeData is required")
        
        # Calculate totals
        self.totalCandlesInserted = sum(data.count for data in self.timeframeData.values())
        self.totalCreditsUsed = sum(data.creditsUsed for data in self.timeframeData.values())
    
    def addTimeframeData(self, timeframe: str, candleData: TimeframeCandleData):
        """Add candle data for a specific timeframe"""
        self.timeframeData[timeframe] = candleData
        # Recalculate totals
        self.totalCandlesInserted = sum(data.count for data in self.timeframeData.values())
        self.totalCreditsUsed = sum(data.creditsUsed for data in self.timeframeData.values())
    
    def getTimeframeData(self, timeframe: str) -> TimeframeCandleData:
        """Get candle data for a specific timeframe"""
        return self.timeframeData.get(timeframe)
    
    def hasTimeframeData(self, timeframe: str) -> bool:
        """Check if data exists for a specific timeframe"""
        return timeframe in self.timeframeData
