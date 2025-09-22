"""
FetchState POJO for managing API fetch state
"""
from dataclasses import dataclass, field
from typing import List, Set, Optional, Dict


@dataclass
class FetchState:
    """Manages state during API pagination and fetching"""
    
    chain: str
    currentFromTime: int
    currentToTime: Optional[int] = None
    latestTime: int = 0
    allRawCandles: List[Dict] = field(default_factory=list)
    processedTimestamps: Set[int] = field(default_factory=set)
    totalCreditsUsed: int = 0
    currentApiKey: Optional[Dict] = None
    currentAvailableCredits: int = 0
    apiKeysUsed: List[Dict] = field(default_factory=list)
    
    def addRawCandle(self, candle: Dict):
        """Add raw candle to collection"""
        self.allRawCandles.append(candle)
    
    def addProcessedTimestamp(self, timestamp: int):
        """Add timestamp to processed set"""
        self.processedTimestamps.add(timestamp)
    
    def isTimestampProcessed(self, timestamp: int) -> bool:
        """Check if timestamp was already processed"""
        return timestamp in self.processedTimestamps
    
    def updateLatestTime(self, timestamp: int):
        """Update latest time if timestamp is newer"""
        if timestamp > self.latestTime:
            self.latestTime = timestamp
    
    def addApiKeyUsage(self, apiKeyId: str, creditsUsed: int):
        """Add API key usage record"""
        self.apiKeysUsed.append({
            'api_key_id': apiKeyId,
            'credits_used': creditsUsed
        })
    
    def useCredits(self, credits: int):
        """Deduct credits from current available credits"""
        self.currentAvailableCredits -= credits
        self.totalCreditsUsed += credits
    
    @classmethod
    def createForFetch(cls, chain: str, fromTime: int) -> 'FetchState':
        """Factory method to create FetchState for API fetching"""
        return cls(
            chain=chain,
            currentFromTime=fromTime,
            latestTime=fromTime
        )
