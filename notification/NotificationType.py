"""
Simple notification enums
"""
from enum import Enum


class NotificationType(Enum):
    BULLISH_CROSS = "bullish_cross"
    BEARISH_CROSS = "bearish_cross"
    BAND_TOUCH = "band_touch"
    AVWAP_BREAKOUT = "avwap_breakout"
    AVWAP_BREAKDOWN = "avwap_breakdown"
    STOCH_RSI_OVERSOLD = "stoch_rsi_oversold"
    STOCH_RSI_OVERBOUGHT = "stoch_rsi_overbought"