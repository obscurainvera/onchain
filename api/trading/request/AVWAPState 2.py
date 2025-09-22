"""
AVWAP State POJO - Clean data structure for AVWAP state data
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AVWAPState:
    """POJO for AVWAP state data"""
    
    tokenAddress: str
    pairAddress: str
    timeframe: str
    avwap: float
    cumulativePV: float
    cumulativeVolume: float
    lastUpdatedUnix: int
    nextFetchTime: int
    createdAt: Optional[str] = None
    lastUpdatedAt: Optional[str] = None
    
   
    
    def toDict(self) -> dict:
        """Convert to dictionary for database operations"""
        return {
            'tokenaddress': self.tokenAddress,
            'pairaddress': self.pairAddress,
            'timeframe': self.timeframe,
            'avwap': self.avwap,
            'cumulativepv': self.cumulativePV,
            'cumulativevolume': self.cumulativeVolume,
            'lastupdatedunix': self.lastUpdatedUnix,
            'nextfetchtime': self.nextFetchTime,
            'createdat': self.createdAt,
            'lastupdatedat': self.lastUpdatedAt
        }
    
    @classmethod
    def fromDict(cls, data: dict) -> 'AVWAPState':
        """Create AVWAPState from dictionary"""
        return cls(
            tokenAddress=data.get('tokenaddress', ''),
            pairAddress=data.get('pairaddress', ''),
            timeframe=data.get('timeframe', ''),
            avwap=float(data.get('avwap', 0)),
            cumulativePV=float(data.get('cumulativepv', 0)),
            cumulativeVolume=float(data.get('cumulativevolume', 0)),
            lastUpdatedUnix=data.get('lastupdatedunix', 0),
            nextFetchTime=data.get('nextfetchtime', 0),
            createdAt=data.get('createdat'),
            lastUpdatedAt=data.get('lastupdatedat')
        )
