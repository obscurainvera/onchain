"""
Token Info POJO - Clean data structure for token information from external APIs
"""

from dataclasses import dataclass


@dataclass
class TokenInfo:
    """POJO for token information from DexScreener API"""
    
    symbol: str
    name: str
    pairCreatedAt: int  # Unix timestamp in milliseconds
    price: float
    
    @property
    def pairCreatedTimeSeconds(self) -> int:
        """Get pair creation time in seconds"""
        return self.pairCreatedAt // 1000
    
    @property
    def pairAgeInDays(self) -> float:
        """Calculate pair age in days"""
        import time
        currentTime = int(time.time())
        pairCreatedTime = self.pairCreatedTimeSeconds
        return (currentTime - pairCreatedTime) / 86400
