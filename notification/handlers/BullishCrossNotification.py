"""
Bullish Cross Notification Handler - Handles bullish cross specific notifications

This module contains all logic specific to bullish cross notifications,
including data preparation, URL building, and message formatting.
"""

from typing import Optional, TYPE_CHECKING
from logs.logger import get_logger
from constants.BullishCrossConstants import BullishCrossDefaults, BullishCrossFields, BullishCrossUrls
from notification.utils.NotificationUtil import NotificationUtil
from notification.NotificationManager import NotificationService
from notification.NotificationType import NotificationType
from notification.types.BullishCross import BullishCross
from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails

logger = get_logger(__name__)


class BullishCrossNotification:
    """Static methods for handling bullish cross notifications"""
    
    @staticmethod
    def sendAlert(chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails') -> bool:
        try:
            # if not NotificationUtil.validateChatName(chatName):
            #     logger.error(f"Invalid chat name: {chatName}")
            #     return False
            
            chatCredentials = NotificationUtil.getChatCredentials(chatName)
            if not chatCredentials:
                logger.error(f"No credentials found for chat: {chatName}")
                return False
            
            bullishData = BullishCrossNotification.createBullishCrossData(trackedToken, timeframeRecord, candle)
            
            commonMessage = BullishCross.formatMessage(bullishData)
            
            notificationService = NotificationService()
            success = notificationService.sendNotification(
                chatCredentials=chatCredentials,
                notificationType=NotificationType.BULLISH_CROSS,
                commonMessage=commonMessage
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending bullish cross notification for {trackedToken.symbol}: {e}")
            return False
    
    @staticmethod
    def createBullishCrossData(trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                               candle: 'OHLCVDetails') -> BullishCross.Data:
        return BullishCross.Data(
            symbol=trackedToken.symbol,
            tokenAddress=trackedToken.tokenAddress,
            shortMa=BullishCrossDefaults.EMA_SHORT_PERIOD,
            longMa=BullishCrossDefaults.EMA_LONG_PERIOD,
            timeframe=timeframeRecord.timeframe,
            currentPrice=float(candle.closePrice),
            unixTime=candle.unixTime,
            time=NotificationUtil.formatUnixTime(candle.unixTime),
            strategyType=BullishCrossDefaults.STRATEGY_TYPE,
            dexScreenerUrl=BullishCrossNotification.buildDexScrennerUrl(trackedToken.tokenAddress)
        )
    
    @staticmethod
    def buildDexScrennerUrl(tokenAddress: str) -> Optional[str]:
        try:
            return BullishCrossUrls.DEXSCREENER_BASE.format(tokenAddress=tokenAddress)
        except Exception:
            return None

    