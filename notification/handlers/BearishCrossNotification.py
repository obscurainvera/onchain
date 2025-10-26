"""
Bearish Cross Notification Handler - Handles bearish cross specific notifications

This module contains all logic specific to bearish cross notifications,
including data preparation, URL building, and message formatting.
"""

from typing import Optional, TYPE_CHECKING
from logs.logger import get_logger
from constants.BullishCrossConstants import BearishCrossDefaults, BearishCrossFields, BearishCrossUrls
from notification.utils.NotificationUtil import NotificationUtil
from notification.NotificationManager import NotificationService
from notification.NotificationType import NotificationType
from notification.types.BearishCross import BearishCross
from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails
from actions.DexscrennerAction import DexScreenerAction

logger = get_logger(__name__)


class BearishCrossNotification:
    """Static methods for handling bearish cross notifications"""
    
    @staticmethod
    def sendAlert(chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails', shortMa: int, longMa: int) -> bool:
        try:
            # if not NotificationUtil.validateChatName(chatName):
            #     logger.error(f"Invalid chat name: {chatName}")
            #     return False
            
            chatCredentials = NotificationUtil.getChatCredentials(chatName)
            if not chatCredentials:
                logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: No credentials found for chat: {chatName}")
                return False
            
            bearishData = BearishCrossNotification.createBearishCrossData(trackedToken, timeframeRecord, candle, shortMa, longMa)
            
            commonMessage = BearishCross.formatMessage(bearishData)
            
            notificationService = NotificationService()
            success = notificationService.sendNotification(
                chatCredentials=chatCredentials,
                notificationType=NotificationType.BEARISH_CROSS,
                commonMessage=commonMessage
            )
            
            return success
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: Error sending bearish cross notification for {trackedToken.symbol} - {NotificationType.BEARISH_CROSS.value}: {e}")
            return False
    
    @staticmethod
    def createBearishCrossData(trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                               candle: 'OHLCVDetails', shortMa: int, longMa: int) -> BearishCross.Data:
        # Fetch market cap from DexScreener
        marketCap = None
        try:
            dexScreener = DexScreenerAction()
            tokenPrice = dexScreener.getTokenPrice(trackedToken.tokenAddress)
            if tokenPrice:
                marketCap = tokenPrice.marketCap
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: Failed to fetch market cap for {trackedToken.symbol}: {e}")
        
        return BearishCross.Data(
            symbol=trackedToken.symbol,
            tokenAddress=trackedToken.tokenAddress,
            shortMa=shortMa,
            longMa=longMa,
            timeframe=timeframeRecord.timeframe,
            currentPrice=float(candle.closePrice),
            unixTime=candle.unixTime,
            time=NotificationUtil.formatUnixTime(candle.unixTime),
            marketCap=marketCap,
            strategyType=BearishCrossDefaults.STRATEGY_TYPE,
            dexScreenerUrl=BearishCrossNotification.buildDexScreenerUrl(trackedToken.tokenAddress)
        )
    
    @staticmethod
    def buildDexScreenerUrl(tokenAddress: str) -> Optional[str]:
        try:
            return BearishCrossUrls.DEXSCREENER_BASE.format(tokenAddress=tokenAddress)
        except Exception:
            return None

    
