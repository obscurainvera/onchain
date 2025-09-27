"""
EMA State POJO - Clean data structure for EMA state data
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class EMAState:
    """POJO for EMA state data"""
    
    tokenAddress: str = ""
    pairAddress: str = ""
    timeframe: str = ""
    emaKey: str = "" 
    emaValue: Optional[float] = None
    lastUpdatedUnix: Optional[int] = None
    nextFetchTime: Optional[int] = None
    emaAvailableTime: Optional[int] = None
    pairCreatedTime: Optional[int] = None
    status: int = 1  # 1 = NOT_AVAILABLE, 2 = AVAILABLE

    
    def updateEMAValue(self, emaValue: float, lastUpdatedUnix: int, nextFetchTime: int):
        """Update EMA value and timing"""
        self.emaValue = emaValue
        self.lastUpdatedUnix = lastUpdatedUnix
        self.nextFetchTime = nextFetchTime
        self.status = 2  # Mark as available
    
    def markAsNotAvailable(self):
        """Mark EMA as not available"""
        self.status = 1
        self.emaValue = None
        self.lastUpdatedUnix = None
        self.nextFetchTime = None
    
    def isAvailable(self) -> bool:
        """Check if EMA is available"""
        return self.status == 2 and self.emaValue is not None
    
    def toDict(self) -> dict:
        """Convert to dictionary for database insertion"""
        return {
            'tokenaddress': self.tokenAddress,
            'pairaddress': self.pairAddress,
            'timeframe': self.timeframe,
            'emakey': self.emaKey,
            'emavalue': self.emaValue,
            'lastupdatedunix': self.lastUpdatedUnix,
            'nextfetchtime': self.nextFetchTime,
            'emaavailabletime': self.emaAvailableTime,
            'paircreatedtime': self.pairCreatedTime,
            'status': self.status
        }
