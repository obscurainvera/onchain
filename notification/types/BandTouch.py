"""
Band Touch notification type
"""
from dataclasses import dataclass
from typing import Optional, List
from notification.MessageFormat import CommonMessage, MessageButton


class BandTouch:
    """Band Touch notification - when price touches EMA bands during bullish trend"""
    
    @dataclass
    class Data:
        """POJO for band touch notification data"""
        symbol: str
        tokenAddress: str
        timeframe: str  # e.g., "1h", "4h", "1d"
        currentPrice: float
        touchCount: int  # 1 for first touch, 2 for second touch
        unixTime: int
        time: str
        volume24h: Optional[float] = None
        marketCap: Optional[float] = None
        priceChange24h: Optional[float] = None
        strategyType: Optional[str] = None
        signalType: Optional[str] = None
        dexScreenerUrl: Optional[str] = None
        tradingUrl: Optional[str] = None
        chartUrl: Optional[str] = None
    
    @staticmethod
    def formatMessage(data: Data) -> CommonMessage:
        """Format band touch data into common message for Telegram"""
        
        # Create the formatted message
        formatted = f"🎯 <b>Band Touch Alert</b>\n\n"
        formatted += f"<b>Symbol:</b> {data.symbol}\n"
        formatted += f"<b>Signal:</b> {data.signalType or 'EMA Band Touch'}\n"
        formatted += f"<b>Touch Count:</b> #{data.touchCount}\n"
        formatted += f"<b>Timeframe:</b> {data.timeframe}\n"
        formatted += f"<b>Current Price:</b> ${data.currentPrice:,.6f}\n"
        formatted += f"<b>Time:</b> {data.time}\n"
        
        if data.marketCap:
            if data.marketCap >= 1_000_000:
                formatted += f"<b>Market Cap:</b> ${data.marketCap/1_000_000:.2f}M\n"
            elif data.marketCap >= 1_000:
                formatted += f"<b>Market Cap:</b> ${data.marketCap/1_000:.2f}K\n"
            else:
                formatted += f"<b>Market Cap:</b> ${data.marketCap:,.2f}\n"
        
        if data.strategyType:
            formatted += f"<b>Strategy:</b> {data.strategyType}\n"
        
        formatted += f"\n<b>Token Address:</b>\n<code>{data.tokenAddress}</code>\n"
        
        # Create buttons
        buttons = []
        if data.dexScreenerUrl:
            buttons.append(MessageButton("📊 DexScreener", data.dexScreenerUrl))
        
        return CommonMessage(
            formattedMessage=formatted,
            tokenId=data.tokenAddress,
            strategyType=data.strategyType or "EMA Band Touch Strategy",
            buttons=buttons if buttons else None
        )
