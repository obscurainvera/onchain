"""
Enum for AVWAP price position status
Defines valid status values for tracking price position relative to AVWAP
"""

from enum import Enum
from dataclasses import dataclass
from typing import Tuple

@dataclass
class AVWAPPositionInfo:
    """
    AVWAP position information container
    
    Attributes:
        positionName: Human readable position name
        positionCode: Integer position code
    """
    positionName: str
    positionCode: int

class AVWAPPricePosition(Enum):
    """
    AVWAP price position enum with name and code
    
    Each enum value contains:
    - positionName: String description of the position
    - positionCode: Integer code for database storage
    
    Usage:
    - BELOW_AVWAP (0): Price is below AVWAP (initial state, ready for breakout alert)
    - ABOVE_AVWAP (1): Price is above AVWAP (breakout occurred, alert sent)
    """

    # Define position values
    BELOW_AVWAP = ("belowAVWAP", 0)
    ABOVE_AVWAP = ("aboveAVWAP", 1)
    
    def __init__(self, positionName: str, positionCode: int):
        self.positionName = positionName
        self.positionCode = positionCode
    
    def getInfo(self) -> AVWAPPositionInfo:
        """Returns position information as a dataclass"""
        return AVWAPPositionInfo(self.positionName, self.positionCode)
    
    @classmethod
    def fromCode(cls, code: int) -> 'AVWAPPricePosition':
        """Get position enum from position code"""
        for position in cls:
            if position.positionCode == code:
                return position
        raise ValueError(f"No AVWAP position found for code: {code}")
    
    def __str__(self) -> str:
        """String representation of position"""
        return f"{self.name}({self.positionName}:{self.positionCode})"
    
    def __repr__(self) -> str:
        """Detailed string representation"""
        return f"AVWAPPricePosition.{self.name}"

