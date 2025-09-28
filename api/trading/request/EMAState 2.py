"""
EMA State POJO - Clean data structure for EMA state data
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class EMAState:
    """POJO for EMA state data"""
    
    tokenAddress: str
    pairAddress: str
    timeframe: str
    emaKey: str  # "21" or "34"
    emaValue: float
    lastUpdatedUnix: int
    nextFetchTime: int
    emaAvailableTime: int
    pairCreatedTime: int
    status: int  # 1 = NOT_AVAILABLE, 2 = AVAILABLE
    createdAt: Optional[str] = None
    lastUpdatedAt: Optional[str] = None
    
    
    
    def toDict(self) -> dict:
        """Convert to dictionary for database operations"""
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
            'status': self.status,
            'createdat': self.createdAt,
            'lastupdatedat': self.lastUpdatedAt
        }
    
    @classmethod
    def fromDict(cls, data: dict) -> 'EMAState':
        """Create EMAState from dictionary"""
        return cls(
            tokenAddress=data.get('tokenaddress', ''),
            pairAddress=data.get('pairaddress', ''),
            timeframe=data.get('timeframe', ''),
            emaKey=data.get('emakey', ''),
            emaValue=float(data.get('emavalue', 0)),
            lastUpdatedUnix=data.get('lastupdatedunix', 0),
            nextFetchTime=data.get('nextfetchtime', 0),
            emaAvailableTime=data.get('emaavailabletime', 0),
            pairCreatedTime=data.get('paircreatedtime', 0),
            status=data.get('status', 1),
            createdAt=data.get('createdat'),
            lastUpdatedAt=data.get('lastupdatedat')
        )
