"""
CandleResponse POJO class for API responses
"""
from dataclasses import dataclass, field
from typing import List, Optional
from .Candle import Candle


@dataclass
class CandleResponse:
    """Represents the response from candle data API calls"""
    
    success: bool
    candles: List[Candle] = field(default_factory=list)
    creditsUsed: int = 0
    latestTime: Optional[int] = None
    candleCount: int = 0
    error: Optional[str] = None
    
    
    
    def addCandle(self, candle: Candle):
        """Add a candle to the response"""
        self.candles.append(candle)
        self.candleCount = len(self.candles)
        
        if self.latestTime is None or candle.unixTime > self.latestTime:
            self.latestTime = candle.unixTime
    
    def addCandles(self, candles: List[Candle]):
        """Add multiple candles to the response"""
        self.candles.extend(candles)
        self.candleCount = len(self.candles)
        
        if candles:
            maxTime = max(candle.unixTime for candle in candles)
            if self.latestTime is None or maxTime > self.latestTime:
                self.latestTime = maxTime
    
    def getCandlesAsDict(self) -> List[dict]:
        """Get candles as list of dictionaries for database insertion"""
        return [candle.toDict() for candle in self.candles]
    
    def filterCompleteCandles(self, currentTime: Optional[int] = None) -> 'CandleResponse':
        completeCandles = [candle for candle in self.candles if candle.isComplete(currentTime)]
        
        return CandleResponse(
            success=self.success,
            candles=completeCandles,
            creditsUsed=self.creditsUsed,
            latestTime=self.latestTime,
            candleCount=len(completeCandles),
            error=self.error
        )
    
    def filterByTimeRange(self, fromTime: int, toTime: int) -> 'CandleResponse':
        """Create a new response with candles within the specified time range"""
        filteredCandles = [
            candle for candle in self.candles 
            if fromTime < candle.unixTime <= toTime
        ]
        
        return CandleResponse(
            success=self.success,
            candles=filteredCandles,
            creditsUsed=self.creditsUsed,
            latestTime=self.latestTime,
            candleCount=len(filteredCandles),
            error=self.error
        )
    
    def isEmpty(self) -> bool:
        """Check if response has no candles"""
        return len(self.candles) == 0
    
    def hasError(self) -> bool:
        """Check if response has an error"""
        return not self.success or self.error is not None
    
    @classmethod
    def successResponse(cls, candles: List[Candle], creditsUsed: int = 0, latestTime: int = 0) -> 'CandleResponse':
        """Create a successful response"""
        return cls(
            success=True,
            candles=candles,
            creditsUsed=creditsUsed,
            candleCount=len(candles),
            latestTime=latestTime
        )
    
    @classmethod
    def errorResponse(cls, error: str) -> 'CandleResponse':
        """Create an error response"""
        return cls(
            success=False,
            error=error,
            candles=[],
            candleCount=0,
            latestTime=0
        )
    
    @classmethod
    def emptyResponse(cls) -> 'CandleResponse':
        """Create an empty successful response"""
        return cls(
            success=True,
            candles=[],
            candleCount=0,
            latestTime=0
        )
