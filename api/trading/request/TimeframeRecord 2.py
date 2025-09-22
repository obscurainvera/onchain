"""
Timeframe Record POJO - Clean data structure for timeframe metadata
"""

from dataclasses import dataclass
from typing import Optional, List


@dataclass
class TimeframeRecord:
    """POJO for timeframe metadata records"""
    
    id: Optional[int] = None
    tokenAddress: str = ""
    pairAddress: str = ""
    timeframe: str = ""
    nextFetchAt: int = 0
    lastFetchedAt: Optional[int] = None
    isActive: bool = True
    createdAt: Optional[str] = None
    lastUpdatedAt: Optional[str] = None
    
    def __post_init__(self):
        """Validate the timeframe record"""
        if not self.tokenAddress:
            raise ValueError("tokenAddress is required")
        if not self.pairAddress:
            raise ValueError("pairAddress is required")
        if not self.timeframe:
            raise ValueError("timeframe is required")
        if self.nextFetchAt <= 0:
            raise ValueError("nextFetchAt must be positive")
    
    def isReadyForFetch(self, currentTime: int) -> bool:
        """Check if this timeframe is ready for data fetching"""
        return self.nextFetchAt <= currentTime
    
    def getCandlesForPersistence(self, maxCandles: Optional[int] = None) -> List:
        """
        Get candles for persistence based on maxCandles parameter
        
        Args:
            maxCandles: Maximum number of candles to return (None = all candles)
            
        Returns:
            List of candles to persist (empty list for this basic version)
        """
        # This is the basic version without OHLCVDetails integration
        # Return empty list as this version doesn't have candle data
        return []
    
    def toDict(self) -> dict:
        """Convert to dictionary for database operations"""
        return {
            'id': self.id,
            'tokenaddress': self.tokenAddress,
            'pairaddress': self.pairAddress,
            'timeframe': self.timeframe,
            'nextfetchat': self.nextFetchAt,
            'lastfetchedat': self.lastFetchedAt,
            'isactive': self.isActive,
            'createdat': self.createdAt,
            'lastupdatedat': self.lastUpdatedAt
        }
    
    @classmethod
    def fromDict(cls, data: dict) -> 'TimeframeRecord':
        """Create TimeframeRecord from dictionary"""
        return cls(
            id=data.get('id'),
            tokenAddress=data.get('tokenaddress', ''),
            pairAddress=data.get('pairaddress', ''),
            timeframe=data.get('timeframe', ''),
            nextFetchAt=data.get('nextfetchat', 0),
            lastFetchedAt=data.get('lastfetchedat'),
            isActive=data.get('isactive', True),
            createdAt=data.get('createdat'),
            lastUpdatedAt=data.get('lastupdatedat')
        )
