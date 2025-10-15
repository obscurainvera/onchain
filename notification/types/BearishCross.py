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
        
        formatted = f"<b>{data.symbol} - {data.timeframe} - bearish cross</b>\n\n"
        formatted += f"<b> - {data.longMa} >> {data.shortMa}</b>\n\n"
        formatted += f"<b> - time :</b> {data.time}\n\n"

        
        if data.marketCap:
            if data.marketCap >= 1_000_000:
                formatted += f"<b> - mc :</b> ${data.marketCap/1_000_000:.2f}M - <b>price :</b> ${data.currentPrice:,.6f}\n\n"
            elif data.marketCap >= 1_000:
                formatted += f"<b> - mc :</b> ${data.marketCap/1_000:.2f}K - <b>price:</b> ${data.currentPrice:,.6f}\n\n"
            else:
                formatted += f"<b> - mc :</b> ${data.marketCap:,.2f} - <b>price:</b> ${data.currentPrice:,.6f}\n\n"

        formatted += f"\n<b> - ca :</b>\n<code>{data.tokenAddress}</code>\n"
        
        
        buttons = []
        if data.dexScreenerUrl:
            buttons.append(MessageButton("DexScreener", data.dexScreenerUrl))
        
        return CommonMessage(
            formattedMessage=formatted,
            tokenId=data.tokenAddress,
            strategyType=data.strategyType or f"bearish cross MA{data.shortMa}/MA{data.longMa}",
            buttons=buttons if buttons else None
        )

