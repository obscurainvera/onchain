"""
Timeframe Record POJO - Clean data structure for timeframe metadata
"""

from typing import List, Optional, TYPE_CHECKING
from dataclasses import dataclass, field

# Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from .OHLCVDetails import OHLCVDetails
    from .VWAPSession import VWAPSession
    from .EMAState import EMAState
    from .AVWAPState import AVWAPState
    from .Alert import Alert

@dataclass
class TimeframeRecord:
    """POJO for timeframe metadata with integrated indicator data"""
    
    # Core timeframe data
    timeframeId: Optional[int] = None
    tokenAddress: str = ""
    pairAddress: str = ""
    timeframe: str = ""
    nextFetchAt: int = 0
    lastFetchedAt: Optional[int] = None
    isActive: bool = True
    
    # Integrated data with proper typing
    ohlcvDetails: List['OHLCVDetails'] = field(default_factory=list)
    vwapSession: Optional['VWAPSession'] = None
    ema21State: Optional['EMAState'] = None
    ema34State: Optional['EMAState'] = None
    avwapState: Optional['AVWAPState'] = None
    alert: Optional['Alert'] = None
    
    def addOHLCVDetail(self, ohlcv: 'OHLCVDetails'):
        """Add OHLCV detail to this timeframe"""
        self.ohlcvDetails.append(ohlcv)
    
    def getLastTwoCandles(self) -> List['OHLCVDetails']:
        """Get the last two candles for persistence"""
        return self.ohlcvDetails[-2:] if len(self.ohlcvDetails) >= 2 else self.ohlcvDetails
    
    def getCandlesForPersistence(self, maxCandles: Optional[int] = None) -> List['OHLCVDetails']:
        """
        Get candles for persistence based on maxCandles parameter
        
        Args:
            maxCandles: Maximum number of candles to return (None = all candles)
            
        Returns:
            List of OHLCVDetails to persist
        """
        if maxCandles is None:
            # Return all candles (scheduler flow)
            return self.ohlcvDetails
        else:
            # Return only the last N candles (API flow)
            return self.ohlcvDetails[-maxCandles:] if len(self.ohlcvDetails) >= maxCandles else self.ohlcvDetails
    
    def hasCandles(self) -> bool:
        """Check if this timeframe has candle data"""
        return len(self.ohlcvDetails) > 0
    
    def shouldFetchFromAPI(self, currentTime: int) -> bool:
        """Check if this timeframe should fetch data from API"""
        return self.nextFetchAt <= currentTime
    
    def updateAfterFetch(self, latestTime: int, nextFetchTime: int):
        """Update timeframe metadata after successful fetch"""
        self.lastFetchedAt = latestTime
        self.nextFetchAt = nextFetchTime
