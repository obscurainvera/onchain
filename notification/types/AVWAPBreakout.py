"""
AVWAP Breakout notification type
"""
from dataclasses import dataclass
from typing import Optional, List
from notification.MessageFormat import CommonMessage, MessageButton


class AVWAPBreakout:
    """AVWAP Breakout notification - when price breaks above the Anchored VWAP"""
    
    @dataclass
    class Data:
        """POJO for AVWAP breakout notification data"""
        symbol: str
        tokenAddress: str
        timeframe: str  # e.g., "1h", "4h", "1d"
        currentPrice: float
        avwapValue: float
        priceChangePercent: Optional[float] = None  # % above AVWAP
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
        """Format AVWAP breakout data into common message for Telegram"""
        
        priceChangePercent = ((data.currentPrice - data.avwapValue) / data.avwapValue) * 100 if data.avwapValue else 0
        
        formatted = f"<b>AVWAP Breakout Alert</b>\n\n"
        formatted += f"<b> - Symbol:</b> {data.symbol}\n"
        formatted += f"<b> - Signal:</b> Price crossed above AVWAP\n"
        formatted += f"<b> - Timeframe:</b> {data.timeframe}\n"
        formatted += f"<b> - Current Price:</b> ${data.currentPrice:,.8f}\n"
        formatted += f"<b> - AVWAP Value:</b> ${data.avwapValue:,.8f}\n"
        formatted += f"<b> - % diff:</b> +{priceChangePercent:.2f}%\n"
        
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
            strategyType=data.strategyType or "AVWAP Breakout Strategy",
            buttons=buttons if buttons else None
        )

