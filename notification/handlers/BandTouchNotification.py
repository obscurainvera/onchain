"""
Band Touch Notification Handler

This module handles sending band touch notifications when EMAs are touched
during a bullish trend. Only sends notifications for the first and second touches.
"""

from typing import Optional, TYPE_CHECKING
from logs.logger import get_logger
from notification.NotificationManager import NotificationService
from notification.NotificationType import NotificationType
from notification.types.BandTouch import BandTouch
from notification.utils.NotificationUtil import NotificationUtil
from constants.BullishCrossConstants import BandTouchDefaults, BandTouchUrls, BandTouchFields
from database.auth.ChatCredentialsEnum import ChatCredentials

if TYPE_CHECKING:
    from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails, Alert

logger = get_logger(__name__)


class BandTouchNotification:
    """
    Handles sending band touch notifications.
    All methods are static as they do not maintain any state.
    """

    @staticmethod
    def sendAlert(chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord',
                  candle: 'OHLCVDetails', alert: 'Alert') -> bool:
        """
        Send band touch notification.
        Only sends for first and second touches.
        """
        try:
            if not ChatCredentials.isValidChatName(chatName):
                logger.error(f"Invalid chat name: {chatName}")
                return False

            # Check if we should send notification (only first and second touches)
            if alert.touchCount > BandTouchDefaults.MAX_TOUCH_NOTIFICATIONS:
                logger.debug(f"Skipping band touch notification for {trackedToken.symbol} - touch count {alert.touchCount} exceeds max {BandTouchDefaults.MAX_TOUCH_NOTIFICATIONS}")
                return False

            chatCredentials = NotificationUtil.getChatCredentials(chatName)
            if not chatCredentials:
                logger.error(f"No credentials found for chat: {chatName}")
                return False

            notificationService = NotificationService()

            bandTouchData = BandTouch.Data(
                symbol=trackedToken.symbol,
                tokenAddress=trackedToken.tokenAddress,
                timeframe=timeframeRecord.timeframe,
                currentPrice=float(candle.closePrice),
                touchCount=alert.touchCount,
                unixTime=candle.unixTime,
                time=NotificationUtil.formatUnixTime(candle.unixTime),
                strategyType=BandTouchDefaults.STRATEGY_TYPE,
                signalType=BandTouchFields.SIGNAL_TYPE,
                dexScreenerUrl=BandTouchUrls.DEXSCREENER_BASE.format(tokenAddress=trackedToken.tokenAddress)
                )

            commonMessage = BandTouch.formatMessage(bandTouchData)

            success = notificationService.sendNotification(
                chatId=chatCredentials['chatId'],
                notificationType=NotificationType.BAND_TOUCH,
                commonMessage=commonMessage
            )

            if success:
                logger.info(f"Band touch notification sent for {trackedToken.symbol} {timeframeRecord.timeframe} (touch #{alert.touchCount})")
            else:
                logger.error(f"Failed to send band touch notification for {trackedToken.symbol} {timeframeRecord.timeframe}")

            return success

        except Exception as e:
            logger.error(f"Error sending band touch notification for {trackedToken.symbol}: {e}")
            return False
