"""
Stochastic RSI Oversold notification type
"""
from dataclasses import dataclass
from typing import Optional, List
from notification.MessageFormat import CommonMessage, MessageButton


class StochRSIOversold:
    """Stochastic RSI Oversold notification - confluence of bullish trend + band touch + oversold RSI"""
    
    @dataclass
    class Data:
        """POJO for Stochastic RSI oversold notification data"""
        symbol: str
        tokenAddress: str
        timeframe: str
        currentPrice: float
        touchedBand: str  # "EMA21" or "EMA34"
        bandValue: float
        trend: str  # "BULLISH"
        kValue: float  # Stochastic RSI %K
        dValue: float  # Stochastic RSI %D
        emaShortValue: Optional[float] = None
        emaShortLabel: Optional[str] = None  # e.g., "EMA12", "EMA21"
        emaLongValue: Optional[float] = None
        emaLongLabel: Optional[str] = None  # e.g., "EMA21", "EMA34"
        rsiValue: Optional[float] = None
        stochRSIValue: Optional[float] = None
        kThreshold: float = 20.0  # Oversold threshold used
        dThreshold: float = 20.0
        unixTime: int = 0
        time: str = ""
        volume24h: Optional[float] = None
        marketCap: Optional[float] = None
        strategyType: Optional[str] = None
        dexScreenerUrl: Optional[str] = None
        tradingUrl: Optional[str] = None
        chartUrl: Optional[str] = None
    
    @staticmethod
    def formatMessage(data: Data) -> CommonMessage:
        """Format Stochastic RSI oversold data into common message for Telegram"""
        
        # Create the formatted message
        formatted = f"<b>Stochastic RSI Oversold Setup Alert</b>\n\n"
        formatted += f"<b> - Symbol:</b> {data.symbol}\n"
        formatted += f"<b> - Trend:</b> {data.trend}\n"
        formatted += f"<b> - Oversold:</b> %K={data.kValue:.2f}, %D={data.dValue:.2f}\n"
        formatted += f"<b> - Timeframe:</b> {data.timeframe}\n"
        formatted += f"<b> - Current Price:</b> ${data.currentPrice:,.8f}\n"
        formatted += f"<b> - {data.touchedBand} Value:</b> ${data.bandValue:,.8f}\n"
        
        # Show EMA values with labels
        if data.emaShortValue is not None and data.emaShortLabel:
            formatted += f"<b> - {data.emaShortLabel} : </b> ${data.emaShortValue:,.6f}\n"
        
        if data.emaLongValue is not None and data.emaLongLabel:
            formatted += f"<b> - {data.emaLongLabel} : </b> ${data.emaLongValue:,.6f}\n"
        
        if data.rsiValue is not None:
            formatted += f"<b> - RSI:</b> {data.rsiValue:.2f}\n"
        
        formatted += f"<b> - %K Threshold:</b> {data.kThreshold:.0f} (K below = oversold)\n"
        formatted += f"<b> - %D Threshold:</b> {data.dThreshold:.0f} (D below = oversold)\n"
        
        if data.time:
            formatted += f"<b> - Time:</b> {data.time}\n"
        
        if data.marketCap:
            if data.marketCap >= 1_000_000:
                formatted += f"<b> - Market Cap:</b> ${data.marketCap/1_000_000:.2f}M\n"
            elif data.marketCap >= 1_000:
                formatted += f"<b> - Market Cap:</b> ${data.marketCap/1_000:.2f}K\n"
            else:
                formatted += f"<b> - Market Cap:</b> ${data.marketCap:,.2f}\n"
        
        if data.strategyType:
            formatted += f"<b> - Strategy:</b> {data.strategyType}\n"
        
        formatted += f"\n<b> - Token Address:</b>\n<code>{data.tokenAddress}</code>\n"
        
        # Create buttons
        buttons = []
        if data.dexScreenerUrl:
            buttons.append(MessageButton("DexScreener", data.dexScreenerUrl))
        
        return CommonMessage(
            formattedMessage=formatted,
            tokenId=data.tokenAddress,
            strategyType=data.strategyType or "Stochastic RSI Oversold Strategy",
            buttons=buttons if buttons else None
        )

