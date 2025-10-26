"""
Stochastic RSI Overbought Notification Handler - Handles Stochastic RSI overbought confluence alerts

This module contains all logic specific to Stochastic RSI overbought notifications,
including data preparation, URL building, and message formatting.
"""

from typing import Optional, TYPE_CHECKING
from logs.logger import get_logger
from constants.BullishCrossConstants import StochRSIOverboughtDefaults, StochRSIOverboughtFields, StochRSIOverboughtUrls
from notification.utils.NotificationUtil import NotificationUtil
from notification.NotificationManager import NotificationService
from notification.NotificationType import NotificationType
from notification.types.StochRSIOverbought import StochRSIOverbought
from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails
from actions.DexscrennerAction import DexScreenerAction

logger = get_logger(__name__)


class StochRSIOverboughtNotification:
    """Static methods for handling Stochastic RSI overbought notifications"""
    
    @staticmethod
    def sendAlert(chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                  candle: 'OHLCVDetails', touchedBand: str, bandValue: float, 
                  shortEmaLabel: str, longEmaLabel: str) -> bool:
        try:
            chatCredentials = NotificationUtil.getChatCredentials(chatName)
            if not chatCredentials:
                logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: No credentials found for chat: {chatName}")
                return False
            
            stochRSIOverboughtData = StochRSIOverboughtNotification.createStochRSIOverboughtData(
                trackedToken, timeframeRecord, candle, touchedBand, bandValue, shortEmaLabel, longEmaLabel
            )
            
            commonMessage = StochRSIOverbought.formatMessage(stochRSIOverboughtData)
            
            notificationService = NotificationService()
            success = notificationService.sendNotification(
                chatCredentials=chatCredentials,
                notificationType=NotificationType.STOCH_RSI_OVERBOUGHT,
                commonMessage=commonMessage
            )
            
            return success
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: Error sending Stochastic RSI overbought notification for {trackedToken.symbol} - {NotificationType.STOCH_RSI_OVERBOUGHT.value}: {e}")
            return False
    
    @staticmethod
    def _getTrendForEMACombination(candle: 'OHLCVDetails', shortEmaLabel: str, longEmaLabel: str) -> str:
        """
        Get the appropriate trend based on EMA combination being used
        
        Args:
            candle: OHLCVDetails object containing trend data
            shortEmaLabel: Short EMA label (e.g., "EMA12", "EMA21")
            longEmaLabel: Long EMA label (e.g., "EMA21", "EMA34")
            
        Returns:
            Trend string (BULLISH/BEARISH/NEUTRAL)
        """
        # For EMA12/EMA21 combination, use trend12
        if shortEmaLabel == "EMA12" and longEmaLabel == "EMA21":
            return candle.trend12 or "NEUTRAL"
        # For EMA21/EMA34 combination, use trend
        elif shortEmaLabel == "EMA21" and longEmaLabel == "EMA34":
            return candle.trend or "NEUTRAL"
        # Default fallback
        else:
            return candle.trend or "NEUTRAL"

    @staticmethod
    def createStochRSIOverboughtData(trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                                     candle: 'OHLCVDetails', touchedBand: str, bandValue: float,
                                     shortEmaLabel: str, longEmaLabel: str) -> StochRSIOverbought.Data:
        # Get EMA values from candle
        emaMap = {
            'EMA12': candle.ema12Value,
            'EMA21': candle.ema21Value,
            'EMA34': candle.ema34Value
        }
        
        # Get the appropriate trend based on EMA combination (override the passed trend parameter)
        actualTrend = StochRSIOverboughtNotification._getTrendForEMACombination(candle, shortEmaLabel, longEmaLabel)
        
        # Fetch market cap from DexScreener
        marketCap = None
        try:
            dexScreener = DexScreenerAction()
            tokenPrice = dexScreener.getTokenPrice(trackedToken.tokenAddress)
            if tokenPrice:
                marketCap = tokenPrice.marketCap
        except Exception as e:
            logger.warning(f"Failed to fetch market cap for {trackedToken.symbol}: {e}")
        
        return StochRSIOverbought.Data(
            symbol=trackedToken.symbol,
            tokenAddress=trackedToken.tokenAddress,
            timeframe=timeframeRecord.timeframe,
            currentPrice=float(candle.closePrice),
            touchedBand=touchedBand,
            bandValue=bandValue,
            trend=actualTrend,
            kValue=float(candle.stochRSIK) if candle.stochRSIK is not None else 0.0,
            dValue=float(candle.stochRSID) if candle.stochRSID is not None else 0.0,
            emaShortValue=float(emaMap.get(shortEmaLabel)) if emaMap.get(shortEmaLabel) is not None else None,
            emaShortLabel=shortEmaLabel,
            emaLongValue=float(emaMap.get(longEmaLabel)) if emaMap.get(longEmaLabel) is not None else None,
            emaLongLabel=longEmaLabel,
            rsiValue=float(candle.rsiValue) if candle.rsiValue is not None else None,
            stochRSIValue=float(candle.stochRSIValue) if candle.stochRSIValue is not None else None,
            kThreshold=StochRSIOverboughtDefaults.K_OVERBOUGHT_THRESHOLD,
            dThreshold=StochRSIOverboughtDefaults.D_OVERBOUGHT_THRESHOLD,
            unixTime=candle.unixTime,
            time=NotificationUtil.formatUnixTime(candle.unixTime),
            marketCap=marketCap,
            strategyType=StochRSIOverboughtDefaults.STRATEGY_TYPE,
            dexScreenerUrl=StochRSIOverboughtNotification.buildDexScreenerUrl(trackedToken.tokenAddress)
        )
    
    @staticmethod
    def buildDexScreenerUrl(tokenAddress: str) -> Optional[str]:
        try:
            return StochRSIOverboughtUrls.DEXSCREENER_BASE.format(tokenAddress=tokenAddress)
        except Exception:
            return None

    
