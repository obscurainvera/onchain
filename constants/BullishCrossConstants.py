"""
Bullish Cross Constants - Constants specific to bullish cross notifications

This module contains all constants used specifically for bullish cross
alert processing and notifications.
"""

from database.auth.ChatCredentialsEnum import ChatCredentials


class BullishCrossDefaults:
    """Default values for bullish cross notification system"""
    DEFAULT_CHAT_NAME = ChatCredentials.BULLISH_CROSS_CHAT.value
    EMA_SHORT_PERIOD = 21
    EMA_LONG_PERIOD = 34
    STRATEGY_TYPE = "EMA Cross Strategy"


class BullishCrossFields:
    """Field names and identifiers for bullish cross processing"""
    SHORT_MA_NAME = "EMA21"
    LONG_MA_NAME = "EMA34"
    SIGNAL_TYPE = "Bullish Cross"
    NOTIFICATION_TYPE = "bullish_cross"


class BullishCrossUrls:
    """URL templates for bullish cross notifications"""
    DEXSCREENER_BASE = "https://dexscreener.com/solana/{tokenAddress}"
    RAYDIUM_SWAP_BASE = "https://raydium.io/swap/?inputCurrency=sol&outputCurrency={tokenAddress}"
    TRADINGVIEW_BASE = "https://www.tradingview.com/chart/?symbol={symbol}USD"


class BandTouchDefaults:
    """Default values for band touch notification system"""
    DEFAULT_CHAT_NAME = ChatCredentials.BAND_TOUCH_CHAT.value
    MAX_TOUCH_NOTIFICATIONS = 2  # Only notify for first and second touches
    STRATEGY_TYPE = "EMA Band Touch Strategy"


class BandTouchFields:
    """Field names and identifiers for band touch processing"""
    TOUCH_COUNT_FIELD = "touchCount"
    LATEST_TOUCH_FIELD = "latestTouchUnix"
    SIGNAL_TYPE = "Band Touch"
    NOTIFICATION_TYPE = "band_touch"


class BandTouchUrls:
    """URL templates for band touch notifications"""
    DEXSCREENER_BASE = "https://dexscreener.com/solana/{tokenAddress}"
    RAYDIUM_SWAP_BASE = "https://raydium.io/swap/?inputCurrency=sol&outputCurrency={tokenAddress}"
    TRADINGVIEW_BASE = "https://www.tradingview.com/chart/?symbol={symbol}USD"
    
