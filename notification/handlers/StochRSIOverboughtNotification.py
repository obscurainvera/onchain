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

logger = get_logger(__name__)


class StochRSIOverboughtNotification:
    """Static methods for handling Stochastic RSI overbought notifications"""
    
    @staticmethod
    def sendAlert(chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                  candle: 'OHLCVDetails', touchedBand: str, bandValue: float, trend: str, 
                  shortEmaLabel: str, longEmaLabel: str) -> bool:
        try:
            chatCredentials = NotificationUtil.getChatCredentials(chatName)
            if not chatCredentials:
                logger.error(f"No credentials found for chat: {chatName}")
                return False
            
            stochRSIOverboughtData = StochRSIOverboughtNotification.createStochRSIOverboughtData(
                trackedToken, timeframeRecord, candle, touchedBand, bandValue, trend, shortEmaLabel, longEmaLabel
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
            logger.error(f"Error sending Stochastic RSI overbought notification for {trackedToken.symbol}: {e}")
            return False
    
    @staticmethod
    def createStochRSIOverboughtData(trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                                     candle: 'OHLCVDetails', touchedBand: str, bandValue: float, trend: str,
                                     shortEmaLabel: str, longEmaLabel: str) -> StochRSIOverbought.Data:
        # Get EMA values from candle
        emaMap = {
            'EMA12': candle.ema12Value,
            'EMA21': candle.ema21Value,
            'EMA34': candle.ema34Value
        }
        
        return StochRSIOverbought.Data(
            symbol=trackedToken.symbol,
            tokenAddress=trackedToken.tokenAddress,
            timeframe=timeframeRecord.timeframe,
            currentPrice=float(candle.closePrice),
            touchedBand=touchedBand,
            bandValue=bandValue,
            trend=trend,
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
            strategyType=StochRSIOverboughtDefaults.STRATEGY_TYPE,
            dexScreenerUrl=StochRSIOverboughtNotification.buildDexScreenerUrl(trackedToken.tokenAddress)
        )
    
    @staticmethod
    def buildDexScreenerUrl(tokenAddress: str) -> Optional[str]:
        try:
            return StochRSIOverboughtUrls.DEXSCREENER_BASE.format(tokenAddress=tokenAddress)
        except Exception:
            return None

    
