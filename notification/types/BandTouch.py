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
        emaShortValue: Optional[float] = None
        emaShortLabel: Optional[str] = None  # e.g., "EMA12", "EMA21"
        emaLongValue: Optional[float] = None
        emaLongLabel: Optional[str] = None  # e.g., "EMA21", "EMA34"
        rsiValue: Optional[float] = None
        stochRSIK: Optional[float] = None  # Stochastic RSI %K
        stochRSID: Optional[float] = None  # Stochastic RSI %D
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
        formatted = f"<b>{data.symbol} - {data.timeframe} - band touch</b>\n\n"
        formatted += f"<b> - count :</b> #{data.touchCount}\n\n"

        if data.marketCap:
            if data.marketCap >= 1_000_000:
                formatted += f"<b> - mc :</b> ${data.marketCap/1_000_000:.2f}M - <b>price :</b> ${data.currentPrice:,.6f}\n\n"
            elif data.marketCap >= 1_000:
                formatted += f"<b> - mc :</b> ${data.marketCap/1_000:.2f}K - <b>price:</b> ${data.currentPrice:,.6f}\n\n"
            else:
                formatted += f"<b> - mc :</b> ${data.marketCap:,.2f} - <b>price:</b> ${data.currentPrice:,.6f}\n\n"
        
        # Show EMA values with labels
        if data.emaShortValue is not None and data.emaShortLabel:
            formatted += f"<b> - {data.emaShortLabel}:</b> ${data.emaShortValue:,.6f}\n"
        
        if data.emaLongValue is not None and data.emaLongLabel:
            formatted += f"<b> - {data.emaLongLabel}:</b> ${data.emaLongValue:,.6f}\n\n"
        
        # Show RSI indicators
        if data.rsiValue is not None:
            formatted += f"<b> - rsi:</b> {data.rsiValue:.2f}\n"
        
        if data.stochRSIK is not None:
            formatted += f"<b> - %k:</b> {data.stochRSIK:.2f}\n"
        
        if data.stochRSID is not None:
            formatted += f"<b> - %d:</b> {data.stochRSID:.2f}\n\n"
        
        formatted += f"<b> - time:</b> {data.time}\n"
        
        formatted += f"\n<b> - ca :</b>\n<code>{data.tokenAddress}</code>\n"
        
        # Create buttons
        buttons = []
        if data.dexScreenerUrl:
            buttons.append(MessageButton("DexScreener", data.dexScreenerUrl))
        
        return CommonMessage(
            formattedMessage=formatted,
            tokenId=data.tokenAddress,
            strategyType=data.strategyType or "EMA Band Touch Strategy",
            buttons=buttons if buttons else None
        )
