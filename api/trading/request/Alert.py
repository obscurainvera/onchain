"""
Alert POJO for tracking indicator crosses and price touches
"""
from typing import Optional
from enum import Enum
from config.AVWAPPricePositionEnum import AVWAPPricePosition

class TrendType(Enum):
    """Trend types for alert tracking"""
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"

class Alert:
    """
    Alert data model for tracking technical indicator signals
    """
    
    def __init__(self, 
                 alertId: Optional[int] = None,
                 tokenId: int = None,
                 tokenAddress: str = None,
                 pairAddress: str = None,
                 timeframe: str = None,
                 vwap: Optional[float] = None,
                 ema12: Optional[float] = None,
                 ema21: Optional[float] = None,
                 ema34: Optional[float] = None,
                 avwap: Optional[float] = None,
                 rsiValue: Optional[float] = None,
                 stochRSIValue: Optional[float] = None,
                 stochRSIK: Optional[float] = None,
                 stochRSID: Optional[float] = None,
                 lastUpdatedUnix: Optional[int] = None,
                 trend: Optional[str] = None,
                 status: Optional[str] = None,
                 trend12: Optional[str] = None,
                 status12: Optional[str] = None,
                 touchCount: int = 0,
                 latestTouchUnix: Optional[int] = None,
                 touchCount12: int = 0,
                 latestTouchUnix12: Optional[int] = None,
                 avwapPricePosition: int = AVWAPPricePosition.BELOW_AVWAP.positionCode):
        """
        Initialize Alert instance
        
        Args:
            alertId: Auto-generated primary key
            tokenId: Tracked token ID reference
            tokenAddress: Token contract address
            pairAddress: Trading pair address
            timeframe: Timeframe for the alert
            vwap: VWAP value at time of alert
            ema12: 12-period EMA value
            ema21: 21-period EMA value
            ema34: 34-period EMA value
            avwap: AVWAP value
            rsiValue: RSI value (0-100)
            stochRSIValue: Stochastic RSI value (0-100)
            stochRSIK: Stochastic RSI %K value (fast line)
            stochRSID: Stochastic RSI %D value (slow line/signal)
            lastUpdatedUnix: Last update timestamp
            trend: Current trend (BULLISH/BEARISH/NEUTRAL)
            status: Encoded status representing band order and price position
            touchCount: Number of times price touched EMA bands
            latestTouchUnix: Timestamp of latest touch
            touchCount12: Number of times price touched EMA12 bands
            latestTouchUnix12: Timestamp of latest touch
            avwapPricePosition: Integer code tracking price position relative to AVWAP
                (0 = BELOW_AVWAP, 1 = ABOVE_AVWAP)
        """
        self.alertId = alertId
        self.tokenId = tokenId
        self.tokenAddress = tokenAddress
        self.pairAddress = pairAddress
        self.timeframe = timeframe
        self.vwap = vwap
        self.ema12 = ema12
        self.ema21 = ema21
        self.ema34 = ema34
        self.avwap = avwap
        self.rsiValue = rsiValue
        self.stochRSIValue = stochRSIValue
        self.stochRSIK = stochRSIK
        self.stochRSID = stochRSID
        self.lastUpdatedUnix = lastUpdatedUnix
        self.trend = trend
        self.status = status
        self.trend12 = trend12
        self.status12 = status12
        self.touchCount = touchCount
        self.latestTouchUnix = latestTouchUnix
        self.touchCount12 = touchCount12
        self.latestTouchUnix12 = latestTouchUnix12
        self.avwapPricePosition = avwapPricePosition
    
    def updateIndicatorValues(self, vwap: float = None, ema21: float = None, 
                             ema34: float = None, avwap: float = None, ema12: float = None,
                             rsiValue: float = None, stochRSIValue: float = None,
                             stochRSIK: float = None, stochRSID: float = None):
        """Update indicator values"""
        if vwap is not None:
            self.vwap = vwap
        if ema12 is not None:
            self.ema12 = ema12
        if ema21 is not None:
            self.ema21 = ema21
        if ema34 is not None:
            self.ema34 = ema34
        if avwap is not None:
            self.avwap = avwap
        if rsiValue is not None:
            self.rsiValue = rsiValue
        if stochRSIValue is not None:
            self.stochRSIValue = stochRSIValue
        if stochRSIK is not None:
            self.stochRSIK = stochRSIK
        if stochRSID is not None:
            self.stochRSID = stochRSID
    
    def updateTrendAndStatus(self, trend: str, status: str, candleUnixTime: int):
        """Update trend and status with timestamp"""
        self.trend = trend
        self.status = status
        self.lastUpdatedUnix = candleUnixTime

    def updateTrendAndStatus12(self, trend12: str, status12: str, candleUnixTime: int):
        """Update trend and status with timestamp"""
        self.trend12 = trend12
        self.status12 = status12
        self.lastUpdatedUnix12 = candleUnixTime
    
    def recordTouch(self, candleUnixTime: int):
        """Record a touch event"""
        self.touchCount += 1
        self.latestTouchUnix = candleUnixTime

    def recordTouch12(self, candleUnixTime: int):
        """Record a touch event"""
        self.touchCount12 += 1
        self.latestTouchUnix12 = candleUnixTime
    
    def resetTouch(self):
        """Reset touch count on bearish cross"""
        self.touchCount = 0
        self.latestTouchUnix = None

    def resetTouch12(self):
        """Reset touch count on bearish cross"""
        self.touchCount12 = 0
        self.latestTouchUnix12 = None
    
    def isBullishCross(self, previousTrend: str, currentTrend: str) -> bool:
        """Check if a bullish cross occurred"""
        return previousTrend == TrendType.BEARISH.value and currentTrend == TrendType.BULLISH.value
    
    def isBearishCross(self, previousTrend: str, currentTrend: str) -> bool:
        """Check if a bearish cross occurred"""
        return previousTrend == TrendType.BULLISH.value and currentTrend == TrendType.BEARISH.value
    
    def shouldRecordTouch(self, currentCandleUnixTime: int, touchThresholdSeconds: int = 7200) -> bool:
        """
        Check if a touch should be recorded based on time difference
        
        Args:
            currentCandleUnixTime: Current candle unix timestamp
            touchThresholdSeconds: Minimum seconds between touches (default 2 hours = 7200)
            
        Returns:
            bool: True if touch should be recorded
        """
        if self.latestTouchUnix is None:
            return True
        return (currentCandleUnixTime - self.latestTouchUnix) >= touchThresholdSeconds
    
    def markPriceAboveAVWAP(self) -> None:
        """Mark that price is now above AVWAP"""
        self.avwapPricePosition = AVWAPPricePosition.ABOVE_AVWAP.positionCode
    
    def markPriceBelowAVWAP(self) -> None:
        """Mark that price is now below AVWAP (reset for next breakout)"""
        self.avwapPricePosition = AVWAPPricePosition.BELOW_AVWAP.positionCode
    
    def shouldSendAVWAPBreakoutAlert(self, closePrice: float, avwapValue: Optional[float]) -> bool:
        """
        Determine if AVWAP breakout alert should be sent
        
        Logic:
        - Send alert once when:
          1. AVWAP value exists
          2. Close price is above AVWAP
          3. Price wasn't above AVWAP before (position = BELOW_AVWAP)
        
        Args:
            closePrice: Current candle close price
            avwapValue: Current AVWAP value
            
        Returns:
            bool: True if alert should be sent (first time above AVWAP)
        """
        if avwapValue is None:
            return False
        
        # Send alert only on first breakout (when price crosses from below to above)
        if closePrice > avwapValue and self.avwapPricePosition == AVWAPPricePosition.BELOW_AVWAP.positionCode:
            return True
        
        return False
