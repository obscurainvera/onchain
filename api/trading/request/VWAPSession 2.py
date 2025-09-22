"""
VWAP Session POJO - Clean data structure for VWAP session data
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class VWAPSession:
    """POJO for VWAP session data"""
    
    tokenAddress: str
    pairAddress: str
    timeframe: str
    sessionStartUnix: int
    sessionEndUnix: int
    cumulativePV: float
    cumulativeVolume: float
    currentVWAP: float
    lastCandleUnix: int
    nextCandleFetch: int
    createdAt: Optional[str] = None
    lastUpdatedAt: Optional[str] = None
    
    
    def toDict(self) -> dict:
        """Convert to dictionary for database operations"""
        return {
            'tokenaddress': self.tokenAddress,
            'pairaddress': self.pairAddress,
            'timeframe': self.timeframe,
            'sessionstartunix': self.sessionStartUnix,
            'sessionendunix': self.sessionEndUnix,
            'cumulativepv': self.cumulativePV,
            'cumulativevolume': self.cumulativeVolume,
            'currentvwap': self.currentVWAP,
            'lastcandleunix': self.lastCandleUnix,
            'nextcandlefetch': self.nextCandleFetch,
            'createdat': self.createdAt,
            'lastupdatedat': self.lastUpdatedAt
        }
    
    @classmethod
    def fromDict(cls, data: dict) -> 'VWAPSession':
        """Create VWAPSession from dictionary"""
        return cls(
            tokenAddress=data.get('tokenaddress', ''),
            pairAddress=data.get('pairaddress', ''),
            timeframe=data.get('timeframe', ''),
            sessionStartUnix=data.get('sessionstartunix', 0),
            sessionEndUnix=data.get('sessionendunix', 0),
            cumulativePV=float(data.get('cumulativepv', 0)),
            cumulativeVolume=float(data.get('cumulativevolume', 0)),
            currentVWAP=float(data.get('currentvwap', 0)),
            lastCandleUnix=data.get('lastcandleunix', 0),
            nextCandleFetch=data.get('nextcandlefetch', 0),
            createdAt=data.get('createdat'),
            lastUpdatedAt=data.get('lastupdatedat')
        )
