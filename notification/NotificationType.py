"""
Simple notification enums
"""
from enum import Enum


class NotificationType(Enum):
    BULLISH_CROSS = "bullish_cross"
    BAND_TOUCH = "band_touch"