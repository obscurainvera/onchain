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
        formatted = f"<b>{data.symbol} - {data.timeframe} - stoch rsi oversold</b>\n\n"

        if data.marketCap:
            if data.marketCap >= 1_000_000:
                formatted += f"<b> - mc :</b> ${data.marketCap/1_000_000:.2f}M - <b>price :</b> ${data.currentPrice:,.6f}\n\n"
            elif data.marketCap >= 1_000:
                formatted += f"<b> - mc :</b> ${data.marketCap/1_000:.2f}K - <b>price:</b> ${data.currentPrice:,.6f}\n\n"
            else:
                formatted += f"<b> - mc :</b> ${data.marketCap:,.2f} - <b>price:</b> ${data.currentPrice:,.6f}\n\n"


        formatted += f"<b> - trend :</b> {data.trend}\n\n"

        formatted += f"<b> - band touch :</b> price touched {data.touchedBand}\n\n"

        formatted += f"<b> - oversold :</b>\n"
        
        if data.rsiValue is not None:
            formatted += f"<b> - rsi:</b> {data.rsiValue:.2f}\n"
        
        formatted += f"<b> - %k :</b> {data.kValue} - {data.kThreshold:.0f}\n"
        formatted += f"<b> - %d :</b> {data.dValue} - {data.dThreshold:.0f}\n"
        
        if data.time:
            formatted += f"<b> - time:</b> {data.time}\n"
    
    
        formatted += f"\n<b> - ca :</b>\n<code>{data.tokenAddress}</code>\n"
        
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

