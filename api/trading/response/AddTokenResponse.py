"""
Add Token Response POJO - Clean data structure for token addition responses
"""

from typing import List, Optional
from dataclasses import dataclass


@dataclass
class AddTokenResponse:
    """Response POJO for token addition operations"""
    
    success: bool
    tokenId: Optional[int] = None
    tokenAddress: Optional[str] = None
    pairAddress: Optional[str] = None
    tokenAge: Optional[float] = None
    candlesInserted: int = 0
    creditsUsed: int = 0
    timeframes: Optional[List[str]] = None
    error: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response"""
        result = {
            'success': self.success
        }
        
        if self.success:
            if self.tokenId is not None:
                result['tokenId'] = self.tokenId
            if self.tokenAddress:
                result['tokenAddress'] = self.tokenAddress
            if self.pairAddress:
                result['pairAddress'] = self.pairAddress
            if self.tokenAge is not None:
                result['tokenAge'] = round(self.tokenAge, 1)
            if self.candlesInserted > 0:
                result['candlesInserted'] = self.candlesInserted
            if self.creditsUsed > 0:
                result['creditsUsed'] = self.creditsUsed
            if self.timeframes:
                result['timeframes'] = self.timeframes
        else:
            if self.error:
                result['error'] = self.error
                
        return result
    
    @classmethod
    def success_response(cls, tokenId: int, tokenAddress: str, pairAddress: str, 
                        tokenAge: float, candlesInserted: int, creditsUsed: int, 
                        timeframes: List[str]) -> 'AddTokenResponse':
        """Create a success response"""
        return cls(
            success=True,
            tokenId=tokenId,
            tokenAddress=tokenAddress,
            pairAddress=pairAddress,
            tokenAge=tokenAge,
            candlesInserted=candlesInserted,
            creditsUsed=creditsUsed,
            timeframes=timeframes
        )
    
    @classmethod
    def error_response(cls, error: str) -> 'AddTokenResponse':
        """Create an error response"""
        return cls(
            success=False,
            error=error
        )
