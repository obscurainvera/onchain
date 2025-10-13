"""
RSIState - POJO for RSI and Stochastic RSI indicator state

This class represents the complete state of RSI indicators for a specific token/timeframe:
- RSI (Relative Strength Index) with Wilder's smoothing
- Stochastic RSI 
- %K (fast line with progressive averaging)
- %D (slow line/signal line)

Stores all necessary state for incremental calculations:
- Average gain/loss for RSI updates
- Recent RSI values for Stochastic RSI calculation
- Recent Stochastic RSI values for %K calculation
- Recent %K values for %D calculation
"""

from typing import Optional, List
from dataclasses import dataclass, field


@dataclass
class RSIState:
    """RSI and Stochastic RSI state for a token/timeframe"""
    
    # Identification
    tokenAddress: str
    pairAddress: str
    timeframe: str
    
    # RSI Configuration
    rsiInterval: int = 14  # Number of periods for RSI calculation
    rsiAvailableTime: Optional[int] = None  # Unix timestamp when RSI becomes available
    
    # RSI State
    rsiValue: Optional[float] = None  # Current RSI value
    avgGain: Optional[float] = None  # Average gain for Wilder's smoothing
    avgLoss: Optional[float] = None  # Average loss for Wilder's smoothing
    lastClosePrice: Optional[float] = None  # Last candle's close price (for next gain/loss calculation)
    
    # Stochastic RSI Configuration
    stochRSIInterval: int = 14  # Number of RSI periods for Stochastic RSI
    
    # Stochastic RSI State
    stochRSIValue: Optional[float] = None  # Current Stochastic RSI value
    rsiValues: List[float] = field(default_factory=list)  # Recent RSI values (max 14)
    
    # %K Configuration and State
    kInterval: int = 3  # Number of periods for %K smoothing
    kValue: Optional[float] = None  # Current %K value
    stochRSIValues: List[float] = field(default_factory=list)  # Recent Stochastic RSI values (max 3)
    
    # %D Configuration and State
    dInterval: int = 3  # Number of periods for %D smoothing
    dValue: Optional[float] = None  # Current %D value
    kValues: List[float] = field(default_factory=list)  # Recent %K values (max 3)
    
    # Timestamps
    lastUpdatedUnix: Optional[int] = None  # Last candle processed
    nextFetchTime: Optional[int] = None  # Next expected candle time
    pairCreatedTime: Optional[int] = None  # When the trading pair was created
    
    # Status
    status: int = 1  # 1 = NOT_AVAILABLE, 2 = AVAILABLE
    
    def __post_init__(self):
        """Ensure lists are properly initialized"""
        if self.rsiValues is None:
            self.rsiValues = []
        if self.stochRSIValues is None:
            self.stochRSIValues = []
        if self.kValues is None:
            self.kValues = []
    
    def hasEnoughDataForRSI(self) -> bool:
        """Check if we have enough data to calculate RSI"""
        return self.avgGain is not None and self.avgLoss is not None
    
    def hasEnoughDataForStochRSI(self) -> bool:
        """Check if we have enough RSI values to calculate Stochastic RSI"""
        return len(self.rsiValues) >= self.stochRSIInterval
    
    def hasEnoughDataForK(self) -> bool:
        """Check if we have enough Stochastic RSI values to calculate %K"""
        return len(self.stochRSIValues) >= 1  # Progressive averaging - show immediately
    
    def hasEnoughDataForD(self) -> bool:
        """Check if we have enough %K values to calculate %D"""
        return len(self.kValues) >= self.dInterval
    
    def addRSIValue(self, rsiValue: float) -> None:
        """Add RSI value and maintain max size"""
        self.rsiValues.append(rsiValue)
        if len(self.rsiValues) > self.stochRSIInterval:
            self.rsiValues.pop(0)
    
    def addStochRSIValue(self, stochRSIValue: float) -> None:
        """Add Stochastic RSI value and maintain max size"""
        self.stochRSIValues.append(stochRSIValue)
        if len(self.stochRSIValues) > self.kInterval:
            self.stochRSIValues.pop(0)
    
    def addKValue(self, kValue: float) -> None:
        """Add %K value and maintain max size"""
        self.kValues.append(kValue)
        if len(self.kValues) > self.dInterval:
            self.kValues.pop(0)
    
    @classmethod
    def createEmpty(cls, tokenAddress: str, pairAddress: str, timeframe: str, 
                    rsiAvailableTime: int, pairCreatedTime: int, status: int = 1) -> 'RSIState':
        """Create an empty RSI state for a new token/timeframe"""
        return cls(
            tokenAddress=tokenAddress,
            pairAddress=pairAddress,
            timeframe=timeframe,
            rsiInterval=14,
            rsiAvailableTime=rsiAvailableTime,
            rsiValue=None,
            avgGain=None,
            avgLoss=None,
            stochRSIInterval=14,
            stochRSIValue=None,
            rsiValues=[],
            kInterval=3,
            kValue=None,
            stochRSIValues=[],
            dInterval=3,
            dValue=None,
            kValues=[],
            lastUpdatedUnix=None,
            nextFetchTime=None,
            pairCreatedTime=pairCreatedTime,
            status=status
        )
    
    def toDict(self) -> dict:
        """Convert to dictionary for database operations"""
        return {
            'tokenAddress': self.tokenAddress,
            'pairAddress': self.pairAddress,
            'timeframe': self.timeframe,
            'rsiInterval': self.rsiInterval,
            'rsiAvailableTime': self.rsiAvailableTime,
            'rsiValue': self.rsiValue,
            'avgGain': self.avgGain,
            'avgLoss': self.avgLoss,
            'lastClosePrice': self.lastClosePrice,
            'stochRSIInterval': self.stochRSIInterval,
            'stochRSIValue': self.stochRSIValue,
            'rsiValues': self.rsiValues,
            'kInterval': self.kInterval,
            'kValue': self.kValue,
            'stochRSIValues': self.stochRSIValues,
            'dInterval': self.dInterval,
            'dValue': self.dValue,
            'kValues': self.kValues,
            'lastUpdatedUnix': self.lastUpdatedUnix,
            'nextFetchTime': self.nextFetchTime,
            'pairCreatedTime': self.pairCreatedTime,
            'status': self.status
        }

