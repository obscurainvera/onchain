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


class BearishCrossDefaults:
    """Default values for bearish cross notification system"""
    DEFAULT_CHAT_NAME = ChatCredentials.BEARISH_CROSS_CHAT.value
    EMA_SHORT_PERIOD = 21
    EMA_LONG_PERIOD = 34
    STRATEGY_TYPE = "EMA Cross Strategy"


class BearishCrossFields:
    """Field names and identifiers for bearish cross processing"""
    SHORT_MA_NAME = "EMA21"
    LONG_MA_NAME = "EMA34"
    SIGNAL_TYPE = "Bearish Cross"
    NOTIFICATION_TYPE = "bearish_cross"


class BearishCrossUrls:
    """URL templates for bearish cross notifications"""
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


class AVWAPBreakoutDefaults:
    """Default values for AVWAP breakout notification system"""
    DEFAULT_CHAT_NAME = ChatCredentials.AVWAP_BREAKOUT_CHAT.value
    STRATEGY_TYPE = "AVWAP Breakout Strategy"


class AVWAPBreakoutFields:
    """Field names and identifiers for AVWAP breakout processing"""
    SIGNAL_TYPE = "AVWAP Breakout"
    NOTIFICATION_TYPE = "avwap_breakout"


class AVWAPBreakoutUrls:
    """URL templates for AVWAP breakout notifications"""
    DEXSCREENER_BASE = "https://dexscreener.com/solana/{tokenAddress}"
    RAYDIUM_SWAP_BASE = "https://raydium.io/swap/?inputCurrency=sol&outputCurrency={tokenAddress}"
    TRADINGVIEW_BASE = "https://www.tradingview.com/chart/?symbol={symbol}USD"


class AVWAPBreakdownDefaults:
    """Default values for AVWAP breakdown notification system"""
    DEFAULT_CHAT_NAME = ChatCredentials.AVWAP_BREAKDOWN_CHAT.value
    STRATEGY_TYPE = "AVWAP Breakdown Strategy"


class AVWAPBreakdownFields:
    """Field names and identifiers for AVWAP breakdown processing"""
    SIGNAL_TYPE = "AVWAP Breakdown"
    NOTIFICATION_TYPE = "avwap_breakdown"


class AVWAPBreakdownUrls:
    """URL templates for AVWAP breakdown notifications"""
    DEXSCREENER_BASE = "https://dexscreener.com/solana/{tokenAddress}"
    RAYDIUM_SWAP_BASE = "https://raydium.io/swap/?inputCurrency=sol&outputCurrency={tokenAddress}"
    TRADINGVIEW_BASE = "https://www.tradingview.com/chart/?symbol={symbol}USD"


class StochRSIOversoldDefaults:
    """Default values for Stochastic RSI oversold notification system"""
    DEFAULT_CHAT_NAME = ChatCredentials.STOCH_RSI_OVERSOLD_CHAT.value
    STRATEGY_TYPE = "Stochastic RSI Oversold Strategy"
    # Configurable oversold thresholds (0-100 range for Stochastic RSI %K and %D)
    K_OVERSOLD_THRESHOLD = 20.0  # %K below this value is considered oversold
    D_OVERSOLD_THRESHOLD = 20.0  # %D below this value is considered oversold


class StochRSIOversoldFields:
    """Field names and identifiers for Stochastic RSI oversold processing"""
    SIGNAL_TYPE = "Stochastic RSI Oversold Setup"
    NOTIFICATION_TYPE = "stoch_rsi_oversold"


class StochRSIOversoldUrls:
    """URL templates for Stochastic RSI oversold notifications"""
    DEXSCREENER_BASE = "https://dexscreener.com/solana/{tokenAddress}"
    RAYDIUM_SWAP_BASE = "https://raydium.io/swap/?inputCurrency=sol&outputCurrency={tokenAddress}"
    TRADINGVIEW_BASE = "https://www.tradingview.com/chart/?symbol={symbol}USD"


class StochRSIOverboughtDefaults:
    """Default values for Stochastic RSI overbought notification system"""
    DEFAULT_CHAT_NAME = ChatCredentials.STOCH_RSI_OVERBOUGHT_CHAT.value
    STRATEGY_TYPE = "Stochastic RSI Overbought Strategy"
    # Configurable overbought thresholds (0-100 range for Stochastic RSI %K and %D)
    K_OVERBOUGHT_THRESHOLD = 80.0  # %K above this value is considered overbought
    D_OVERBOUGHT_THRESHOLD = 80.0  # %D above this value is considered overbought


class StochRSIOverboughtFields:
    """Field names and identifiers for Stochastic RSI overbought processing"""
    SIGNAL_TYPE = "Stochastic RSI Overbought Setup"
    NOTIFICATION_TYPE = "stoch_rsi_overbought"


class StochRSIOverboughtUrls:
    """URL templates for Stochastic RSI overbought notifications"""
    DEXSCREENER_BASE = "https://dexscreener.com/solana/{tokenAddress}"
    RAYDIUM_SWAP_BASE = "https://raydium.io/swap/?inputCurrency=sol&outputCurrency={tokenAddress}"
    TRADINGVIEW_BASE = "https://www.tradingview.com/chart/?symbol={symbol}USD"
    
