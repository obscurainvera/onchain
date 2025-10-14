"""
Bearish Cross notification type
"""
from dataclasses import dataclass
from typing import Optional, List
from notification.MessageFormat import CommonMessage, MessageButton


class BearishCross:
    """Bearish Cross notification - when a shorter MA crosses below a longer MA"""
    
    @dataclass
    class Data:
        """POJO for bearish cross notification data"""
        symbol: str
        tokenAddress: str
        shortMa: int  # e.g., 21
        longMa: int   # e.g., 34
        timeframe: str  # e.g., "1h", "4h", "1d"
        currentPrice: float
        unixTime: int
        time: str
        volume24h: Optional[float] = None
        marketCap: Optional[float] = None
        priceChange24h: Optional[float] = None
        strategyType: Optional[str] = None
        dexScreenerUrl: Optional[str] = None
        tradingUrl: Optional[str] = None
        chartUrl: Optional[str] = None
    
    @staticmethod
    def formatMessage(data: Data) -> CommonMessage:
        """Format bearish cross data into common message for Telegram"""
        
        formatted = f"<b>Bearish Cross Alert - EMA{data.longMa} >> EMA{data.shortMa}</b>\n\n"
        formatted += f"<b> - Symbol:</b> {data.symbol}\n"
        formatted += f"<b> - Signal:</b> EMA{data.shortMa} crossed below EMA{data.longMa}\n"
        formatted += f"<b> - Timeframe:</b> {data.timeframe}\n"
        formatted += f"<b> - Current Price:</b> ${data.currentPrice:,.6f}\n"
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
        
        buttons = []
        if data.dexScreenerUrl:
            buttons.append(MessageButton("DexScreener", data.dexScreenerUrl))
        
        return CommonMessage(
            formattedMessage=formatted,
            tokenId=data.tokenAddress,
            strategyType=data.strategyType or f"Bearish Cross MA{data.shortMa}/MA{data.longMa}",
            buttons=buttons if buttons else None
        )

