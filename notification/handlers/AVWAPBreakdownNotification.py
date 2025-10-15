"""
AVWAP Breakdown Notification Handler - Handles AVWAP breakdown specific notifications

This module contains all logic specific to AVWAP breakdown notifications,
including data preparation, URL building, and message formatting.
"""

from typing import Optional, TYPE_CHECKING
from logs.logger import get_logger
from constants.BullishCrossConstants import AVWAPBreakdownDefaults, AVWAPBreakdownFields, AVWAPBreakdownUrls
from notification.utils.NotificationUtil import NotificationUtil
from notification.NotificationManager import NotificationService
from notification.NotificationType import NotificationType
from notification.types.AVWAPBreakdown import AVWAPBreakdown
from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails
from actions.DexscrennerAction import DexScreenerAction

logger = get_logger(__name__)


class AVWAPBreakdownNotification:
    """Static methods for handling AVWAP breakdown notifications"""
    
    @staticmethod
    def sendAlert(chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails') -> bool:
        try:
            chatCredentials = NotificationUtil.getChatCredentials(chatName)
            if not chatCredentials:
                logger.error(f"No credentials found for chat: {chatName}")
                return False
            
            avwapData = AVWAPBreakdownNotification.createAVWAPBreakdownData(trackedToken, timeframeRecord, candle)
            
            commonMessage = AVWAPBreakdown.formatMessage(avwapData)
            
            notificationService = NotificationService()
            success = notificationService.sendNotification(
                chatCredentials=chatCredentials,
                notificationType=NotificationType.AVWAP_BREAKDOWN,
                commonMessage=commonMessage
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending AVWAP breakdown notification for {trackedToken.symbol}: {e}")
            return False
    
    @staticmethod
    def createAVWAPBreakdownData(trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', 
                                 candle: 'OHLCVDetails') -> AVWAPBreakdown.Data:
        # Fetch market cap from DexScreener
        marketCap = None
        try:
            dexScreener = DexScreenerAction()
            tokenPrice = dexScreener.getTokenPrice(trackedToken.tokenAddress)
            if tokenPrice:
                marketCap = tokenPrice.marketCap
        except Exception as e:
            logger.warning(f"Failed to fetch market cap for {trackedToken.symbol}: {e}")
        
        return AVWAPBreakdown.Data(
            symbol=trackedToken.symbol,
            tokenAddress=trackedToken.tokenAddress,
            timeframe=timeframeRecord.timeframe,
            currentPrice=float(candle.closePrice),
            avwapValue=float(candle.avwapValue) if candle.avwapValue else 0.0,
            unixTime=candle.unixTime,
            time=NotificationUtil.formatUnixTime(candle.unixTime),
            marketCap=marketCap,
            strategyType=AVWAPBreakdownDefaults.STRATEGY_TYPE,
            dexScreenerUrl=AVWAPBreakdownNotification.buildDexScreenerUrl(trackedToken.tokenAddress)
        )
    
    @staticmethod
    def buildDexScreenerUrl(tokenAddress: str) -> Optional[str]:
        try:
            return AVWAPBreakdownUrls.DEXSCREENER_BASE.format(tokenAddress=tokenAddress)
        except Exception:
            return None

    
