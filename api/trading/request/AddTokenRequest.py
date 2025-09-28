"""
Add Token Request POJO - Clean data structure for token addition requests
"""

from typing import List
from dataclasses import dataclass


@dataclass
class AddTokenRequest:
    """Request POJO for adding a token to tracking"""
    
    tokenAddress: str
    pairAddress: str
    timeframes: List[str]
    addedBy: str = "api_user"
    
    def __post_init__(self):
        """Validate the request data after initialization"""
        if not self.tokenAddress or not self.tokenAddress.strip():
            raise ValueError("tokenAddress is required")
        
        if not self.pairAddress or not self.pairAddress.strip():
            raise ValueError("pairAddress is required")
            
        if not self.timeframes or len(self.timeframes) == 0:
            raise ValueError("timeframes are required")
            
        # Clean up the data
        self.tokenAddress = self.tokenAddress.strip()
        self.pairAddress = self.pairAddress.strip()
        self.addedBy = self.addedBy.strip() if self.addedBy else "api_user"
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AddTokenRequest':
        """Create AddTokenRequest from dictionary"""
        return cls(
            tokenAddress=data.get('tokenAddress', ''),
            pairAddress=data.get('pairAddress', ''),
            timeframes=data.get('timeframes', []),
            addedBy=data.get('addedBy', 'api_user')
        )
