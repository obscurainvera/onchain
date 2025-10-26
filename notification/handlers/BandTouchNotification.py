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
from actions.DexscrennerAction import DexScreenerAction

if TYPE_CHECKING:
    from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails, Alert

logger = get_logger(__name__)


class BandTouchNotification:
    """
    Handles sending band touch notifications.
    All methods are static as they do not maintain any state.
    """
    
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
    def sendAlert(chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord',
                  candle: 'OHLCVDetails', alert: 'Alert', shortEmaLabel: str, longEmaLabel: str) -> bool:
        """
        Send band touch notification.
        Only sends for first and second touches.
        
        Args:
            shortEmaLabel: Label for short EMA (e.g., "EMA12", "EMA21")
            longEmaLabel: Label for long EMA (e.g., "EMA21", "EMA34")
        """
        try:
            if not ChatCredentials.isValidChatName(chatName):
                logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: Invalid chat name: {chatName}")
                return False

            # Check if we should send notification (only first and second touches)
            if alert.touchCount > BandTouchDefaults.MAX_TOUCH_NOTIFICATIONS:
                logger.debug(f"Skipping band touch notification for {trackedToken.symbol} - touch count {alert.touchCount} exceeds max {BandTouchDefaults.MAX_TOUCH_NOTIFICATIONS}")
                return False

            chatCredentials = NotificationUtil.getChatCredentials(chatName)
            if not chatCredentials:
                logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: No credentials found for chat: {chatName}")
                return False

            notificationService = NotificationService()
            
            # Get EMA values from candle
            emaMap = {
                'EMA12': candle.ema12Value,
                'EMA21': candle.ema21Value,
                'EMA34': candle.ema34Value
            }
            
            shortEmaValue = emaMap.get(shortEmaLabel)
            longEmaValue = emaMap.get(longEmaLabel)

            # Fetch market cap from DexScreener
            marketCap = None
            try:
                dexScreener = DexScreenerAction()
                tokenPrice = dexScreener.getTokenPrice(trackedToken.tokenAddress)
                if tokenPrice:
                    marketCap = tokenPrice.marketCap
            except Exception as e:
                logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: Failed to fetch market cap for {trackedToken.symbol}: {e}")

            bandTouchData = BandTouch.Data(
                symbol=trackedToken.symbol,
                tokenAddress=trackedToken.tokenAddress,
                timeframe=timeframeRecord.timeframe,
                currentPrice=float(candle.closePrice),
                touchCount=alert.touchCount,
                unixTime=candle.unixTime,
                time=NotificationUtil.formatUnixTime(candle.unixTime),
                emaShortValue=float(shortEmaValue) if shortEmaValue is not None else None,
                emaShortLabel=shortEmaLabel,
                emaLongValue=float(longEmaValue) if longEmaValue is not None else None,
                emaLongLabel=longEmaLabel,
                rsiValue=float(candle.rsiValue) if candle.rsiValue is not None else None,
                stochRSIK=float(candle.stochRSIK) if candle.stochRSIK is not None else None,
                stochRSID=float(candle.stochRSID) if candle.stochRSID is not None else None,
                marketCap=marketCap,
                strategyType=BandTouchDefaults.STRATEGY_TYPE,
                signalType=BandTouchFields.SIGNAL_TYPE,
                dexScreenerUrl=BandTouchUrls.DEXSCREENER_BASE.format(tokenAddress=trackedToken.tokenAddress)
                )

            commonMessage = BandTouch.formatMessage(bandTouchData)

            success = notificationService.sendNotification(
                chatCredentials=chatCredentials,
                notificationType=NotificationType.BAND_TOUCH,
                commonMessage=commonMessage
            )

            if success:
                logger.info(f"Band touch notification sent for {trackedToken.symbol} {timeframeRecord.timeframe} (touch #{alert.touchCount})")
            else:
                logger.info(f"Failed to send band touch notification for {trackedToken.symbol} {timeframeRecord.timeframe}")

            return success

        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: NOTIFICATION :: Error sending band touch notification for {trackedToken.symbol} - {NotificationType.BAND_TOUCH.value}: {e}")
            return False
