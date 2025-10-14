"""
Stochastic RSI Overbought notification type
"""
from dataclasses import dataclass
from typing import Optional, List
from notification.MessageFormat import CommonMessage, MessageButton


class StochRSIOverbought:
    """Stochastic RSI Overbought notification - confluence of bullish trend + band touch + overbought RSI"""
    
    @dataclass
    class Data:
        """POJO for Stochastic RSI overbought notification data"""
        symbol: str
        tokenAddress: str
        timeframe: str
        currentPrice: float
        touchedBand: str  # "EMA12", "EMA21", or "EMA34"
        bandValue: float
        trend: str  # "BULLISH" or "BEARISH"
        kValue: float  # Stochastic RSI %K
        dValue: float  # Stochastic RSI %D
        emaShortValue: Optional[float] = None
        emaShortLabel: Optional[str] = None  # e.g., "EMA12", "EMA21"
        emaLongValue: Optional[float] = None
        emaLongLabel: Optional[str] = None  # e.g., "EMA21", "EMA34"
        rsiValue: Optional[float] = None
        stochRSIValue: Optional[float] = None
        kThreshold: float = 80.0  # Overbought threshold used
        dThreshold: float = 80.0
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
        """Format Stochastic RSI overbought data into common message for Telegram"""
        
        # Create the formatted message
        formatted = f"<b>Stochastic RSI Overbought Setup Alert</b>\n\n"
        formatted += f"<b> - Symbol:</b> {data.symbol}\n"
        formatted += f"<b> - Trend:</b> {data.trend}\n"
        formatted += f"<b> - Band Touch:</b> Price touched {data.touchedBand}\n"
        formatted += f"<b> - Overbought:</b> %K={data.kValue:.2f}, %D={data.dValue:.2f}\n"
        formatted += f"<b> - Timeframe:</b> {data.timeframe}\n"
        formatted += f"<b> - Current Price:</b> ${data.currentPrice:,.8f}\n"
        formatted += f"<b> - {data.touchedBand} Value:</b> ${data.bandValue:,.8f}\n"
        
        # Show EMA values with labels
        if data.emaShortValue is not None and data.emaShortLabel:
            formatted += f"<b> - {data.emaShortLabel}:</b> ${data.emaShortValue:,.6f}\n"
        
        if data.emaLongValue is not None and data.emaLongLabel:
            formatted += f"<b> - {data.emaLongLabel}:</b> ${data.emaLongValue:,.6f}\n"
        
        if data.rsiValue is not None:
            formatted += f"<b> - RSI:</b> {data.rsiValue:.2f}\n"
        
        formatted += f"<b> - %K Threshold:</b> {data.kThreshold:.0f} (K above = overbought)\n"
        formatted += f"<b> - %D Threshold:</b> {data.dThreshold:.0f} (D above = overbought)\n"
        
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
            strategyType=data.strategyType or "Stochastic RSI Overbought Strategy",
            buttons=buttons if buttons else None
        )

