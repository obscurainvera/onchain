"""
VWAP Session POJO - Clean data structure for VWAP session data
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class VWAPSession:
    """POJO for VWAP session data"""
    
    tokenAddress: str = ""
    pairAddress: str = ""
    timeframe: str = ""
    sessionStartUnix: int = 0
    sessionEndUnix: int = 0
    cumulativePV: float = 0.0
    cumulativeVolume: float = 0.0
    currentVWAP: float = 0.0
    lastCandleUnix: Optional[int] = None
    nextCandleFetch: Optional[int] = None
    
    def __post_init__(self):
        """Validate VWAP session data"""
        if self.sessionStartUnix <= 0:
            raise ValueError("sessionStartUnix must be positive")
        if self.sessionEndUnix <= 0:
            raise ValueError("sessionEndUnix must be positive")
        if self.sessionEndUnix <= self.sessionStartUnix:
            raise ValueError("sessionEndUnix must be > sessionStartUnix")
        if self.cumulativePV < 0:
            raise ValueError("cumulativePV cannot be negative")
        if self.cumulativeVolume < 0:
            raise ValueError("cumulativeVolume cannot be negative")
        if self.currentVWAP < 0:
            raise ValueError("currentVWAP cannot be negative")
    
    def updateSession(self, sessionStartUnix: int, sessionEndUnix: int, 
                     cumulativePV: float, cumulativeVolume: float, 
                     currentVWAP: float, lastCandleUnix: int, nextCandleFetch: int):
        """Update VWAP session data"""
        self.sessionStartUnix = sessionStartUnix
        self.sessionEndUnix = sessionEndUnix
        self.cumulativePV = cumulativePV
        self.cumulativeVolume = cumulativeVolume
        self.currentVWAP = currentVWAP
        self.lastCandleUnix = lastCandleUnix
        self.nextCandleFetch = nextCandleFetch
    
    def toDict(self) -> dict:
        """Convert to dictionary for database insertion"""
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
            'nextcandlefetch': self.nextCandleFetch
        }
