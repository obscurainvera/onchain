from dataclasses import dataclass, field
from typing import Optional, List, Dict, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from .TimeframeRecord import TimeframeRecord

@dataclass
class TrackedToken:
    """POJO for tracked token information"""
    
    trackedTokenId: int
    tokenAddress: str
    symbol: str
    name: str
    pairAddress: str
    pairCreatedTime: Optional[int] = None
    additionSource: int = 1  # 1: MANUAL, 2: AUTOMATIC
    status: int = 1  # 1: ACTIVE, 2: DISABLED
    enabledAt: Optional[datetime] = None
    disabledAt: Optional[datetime] = None
    createdAt: Optional[datetime] = None
    lastUpdatedAt: Optional[datetime] = None
    addedBy: Optional[str] = None
    disabledBy: Optional[str] = None
    metadata: Optional[Dict] = None
    
    # Timeframe records associated with this token
    timeframeRecords: List['TimeframeRecord'] = field(default_factory=list)
    
    def addTimeframeRecord(self, timeframeRecord: 'TimeframeRecord'):
        """Add a timeframe record to this token"""
        self.timeframeRecords.append(timeframeRecord)
    
    def getTimeframeRecord(self, timeframe: str) -> Optional['TimeframeRecord']:
        """Get timeframe record by timeframe"""
        for record in self.timeframeRecords:
            if record.timeframe == timeframe:
                return record
        return None
