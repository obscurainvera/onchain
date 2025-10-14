"""
Stochastic RSI Oversold Notification Handler - Handles Stochastic RSI oversold confluence alerts

This module contains all logic specific to Stochastic RSI oversold notifications,
including data preparation, URL building, and message formatting.
"""

from typing import Optional, TYPE_CHECKING
from logs.logger import get_logger
from constants.BullishCrossConstants import StochRSIOversoldDefaults, StochRSIOversoldFields, StochRSIOversoldUrls
from notification.utils.NotificationUtil import NotificationUtil
from notification.NotificationManager import NotificationService
from notification.NotificationType import NotificationType
from notification.types.StochRSIOversold import StochRSIOversold
from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails

logger = get_logger(__name__)


class StochRSIOversoldNotification:
    """Static methods for handling Stochastic RSI oversold notifications"""
    
    @staticmethod
    def sendAlert(chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                  candle: 'OHLCVDetails', touchedBand: str, bandValue: float, 
                  shortEmaLabel: str, longEmaLabel: str) -> bool:
        try:
            chatCredentials = NotificationUtil.getChatCredentials(chatName)
            if not chatCredentials:
                logger.error(f"No credentials found for chat: {chatName}")
                return False
            
            stochRSIOversoldData = StochRSIOversoldNotification.createStochRSIOversoldData(
                trackedToken, timeframeRecord, candle, touchedBand, bandValue, shortEmaLabel, longEmaLabel
            )
            
            commonMessage = StochRSIOversold.formatMessage(stochRSIOversoldData)
            
            notificationService = NotificationService()
            success = notificationService.sendNotification(
                chatCredentials=chatCredentials,
                notificationType=NotificationType.STOCH_RSI_OVERSOLD,
                commonMessage=commonMessage
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending Stochastic RSI oversold notification for {trackedToken.symbol}: {e}")
            return False
    
    @staticmethod
    def createStochRSIOversoldData(trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                                    candle: 'OHLCVDetails', touchedBand: str, bandValue: float,
                                    shortEmaLabel: str, longEmaLabel: str) -> StochRSIOversold.Data:
        # Get EMA values from candle
        emaMap = {
            'EMA12': candle.ema12Value,
            'EMA21': candle.ema21Value,
            'EMA34': candle.ema34Value
        }
        
        return StochRSIOversold.Data(
            symbol=trackedToken.symbol,
            tokenAddress=trackedToken.tokenAddress,
            timeframe=timeframeRecord.timeframe,
            currentPrice=float(candle.closePrice),
            touchedBand=touchedBand,
            bandValue=bandValue,
            trend="BULLISH",
            kValue=float(candle.stochRSIK) if candle.stochRSIK is not None else 0.0,
            dValue=float(candle.stochRSID) if candle.stochRSID is not None else 0.0,
            emaShortValue=float(emaMap.get(shortEmaLabel)) if emaMap.get(shortEmaLabel) is not None else None,
            emaShortLabel=shortEmaLabel,
            emaLongValue=float(emaMap.get(longEmaLabel)) if emaMap.get(longEmaLabel) is not None else None,
            emaLongLabel=longEmaLabel,
            rsiValue=float(candle.rsiValue) if candle.rsiValue is not None else None,
            stochRSIValue=float(candle.stochRSIValue) if candle.stochRSIValue is not None else None,
            kThreshold=StochRSIOversoldDefaults.K_OVERSOLD_THRESHOLD,
            dThreshold=StochRSIOversoldDefaults.D_OVERSOLD_THRESHOLD,
            unixTime=candle.unixTime,
            time=NotificationUtil.formatUnixTime(candle.unixTime),
            strategyType=StochRSIOversoldDefaults.STRATEGY_TYPE,
            dexScreenerUrl=StochRSIOversoldNotification.buildDexScreenerUrl(trackedToken.tokenAddress)
        )
    
    @staticmethod
    def buildDexScreenerUrl(tokenAddress: str) -> Optional[str]:
        try:
            return StochRSIOversoldUrls.DEXSCREENER_BASE.format(tokenAddress=tokenAddress)
        except Exception:
            return None

    
