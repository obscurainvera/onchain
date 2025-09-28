"""
AlertsProcessor Types - Standalone POJOs and enums for alerts processing

This module contains all the data structures used by the AlertsProcessor
to maintain clean separation of concerns and type safety.
"""

from typing import Optional
from enum import Enum


class IntervalType(Enum):
    """Enum for price interval types"""
    ABOVE_ALL = "above_all"
    BELOW_ALL = "below_all"
    BETWEEN = "between"
    UNKNOWN = "unknown"


class BandType:
    """Band type identifiers for status encoding"""
    AVWAP = 'A'
    VWAP = 'V'
    EMA21 = '2'
    EMA34 = '3'


class PositionType:
    """Position types relative to bands"""
    ABOVE = 'A'
    BELOW = 'B'


class BandInfo:
    """Information about a single band"""
    
    def __init__(self, bandType: str, value: float):
        self.bandType = bandType
        self.value = value
        self.shortCode = self._generateShortCode(bandType)
    
    def _generateShortCode(self, bandType: str) -> str:
        """Generate short code for the band type"""
        if bandType == 'EMA21':
            return '2'
        elif bandType == 'EMA34':
            return '3'
        elif bandType == 'EMA12':
            return '1'
        elif bandType.startswith('EMA'):
            # Extract number from EMA label (e.g., EMA5 -> 5, EMA20 -> 20)
            try:
                ema_number = bandType[3:]  # Remove 'EMA' prefix
                return ema_number
            except:
                return bandType[0]  # Fallback to first character
        else:
            return bandType[0]  # First character for AVWAP, VWAP, etc.


class PriceInterval:
    """POJO representing where the close price falls relative to bands"""
    
    def __init__(self, intervalType: IntervalType, upperBand: Optional[BandInfo] = None, lowerBand: Optional[BandInfo] = None):
        self.intervalType = intervalType
        self.upperBand = upperBand
        self.lowerBand = lowerBand
    
    def isAboveAll(self) -> bool:
        """Check if price is above all bands"""
        return self.intervalType == IntervalType.ABOVE_ALL
    
    def isBelowAll(self) -> bool:
        """Check if price is below all bands"""
        return self.intervalType == IntervalType.BELOW_ALL
    
    def isBetween(self) -> bool:
        """Check if price is between two bands"""
        return self.intervalType == IntervalType.BETWEEN
    
    def isUnknown(self) -> bool:
        """Check if interval type is unknown"""
        return self.intervalType == IntervalType.UNKNOWN
