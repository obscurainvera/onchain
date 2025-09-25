"""
Bullish Cross notification type
"""
from dataclasses import dataclass
from typing import Optional, List
from notification.MessageFormat import CommonMessage, MessageButton


class BullishCross:
    """Bullish Cross notification - when a shorter MA crosses above a longer MA"""
    
    @dataclass
    class Data:
        """POJO for bullish cross notification data"""
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
        """Format bullish cross data into common message for Telegram"""
        
        # Create the formatted message
        formatted = f"📈 <b>Bullish Cross Alert</b>\n\n"
        formatted += f"<b>Symbol:</b> {data.symbol}\n"
        formatted += f"<b>Signal:</b> MA{data.shortMa} crossed above MA{data.longMa}\n"
        formatted += f"<b>Timeframe:</b> {data.timeframe}\n"
        formatted += f"<b>Current Price:</b> ${data.currentPrice:,.6f}\n"
        
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
        if data.chartUrl:
            buttons.append(MessageButton("📈 Chart", data.chartUrl))
        if data.tradingUrl:
            buttons.append(MessageButton("🔄 Trade", data.tradingUrl))
        
        return CommonMessage(
            formattedMessage=formatted,
            tokenId=data.tokenAddress,
            strategyType=data.strategyType or f"Bullish Cross MA{data.shortMa}/MA{data.longMa}",
            buttons=buttons if buttons else None
        )
