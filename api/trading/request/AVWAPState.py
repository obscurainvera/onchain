"""
AVWAP State POJO - Clean data structure for AVWAP state data
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AVWAPState:
    """POJO for AVWAP state data"""
    
    tokenAddress: str = ""
    pairAddress: str = ""
    timeframe: str = ""
    avwap: float = 0.0
    cumulativePV: float = 0.0
    cumulativeVolume: float = 0.0
    lastUpdatedUnix: int = 0
    nextFetchTime: Optional[int] = None
    
    def __post_init__(self):
        """Validate AVWAP state data"""
        if self.avwap < 0:
            raise ValueError("avwap cannot be negative")
        if self.cumulativePV < 0:
            raise ValueError("cumulativePV cannot be negative")
        if self.cumulativeVolume < 0:
            raise ValueError("cumulativeVolume cannot be negative")
        if self.lastUpdatedUnix <= 0:
            raise ValueError("lastUpdatedUnix must be positive")
    
    def updateAVWAPData(self, avwap: float, cumulativePV: float, 
                       cumulativeVolume: float, lastUpdatedUnix: int, 
                       nextFetchTime: int):
        """Update AVWAP data"""
        self.avwap = avwap
        self.cumulativePV = cumulativePV
        self.cumulativeVolume = cumulativeVolume
        self.lastUpdatedUnix = lastUpdatedUnix
        self.nextFetchTime = nextFetchTime
    
    def toDict(self) -> dict:
        """Convert to dictionary for database insertion"""
        return {
            'tokenaddress': self.tokenAddress,
            'pairaddress': self.pairAddress,
            'timeframe': self.timeframe,
            'avwap': self.avwap,
            'cumulativepv': self.cumulativePV,
            'cumulativevolume': self.cumulativeVolume,
            'lastupdatedunix': self.lastUpdatedUnix,
            'nextfetchtime': self.nextFetchTime
        }
