"""
Notification Utilities - Common utility functions for all notification types

This module contains static utility methods that can be used across
different notification types without maintaining state.
"""

from typing import Optional, TYPE_CHECKING
from datetime import datetime
from logs.logger import get_logger
from database.auth.CredentialsHandler import CredentialsHandler
from database.auth.ChatCredentialsEnum import ChatCredentials
from database.auth.ServiceCredentialsEnum import CredentialType, CredentialField

if TYPE_CHECKING:
    from api.trading.request import TrackedToken, OHLCVDetails

logger = get_logger(__name__)


class NotificationUtil:
    """Static utility methods for notification processing"""
    
    @staticmethod
    def getChatCredentials(chatName: str) -> Optional[dict]:
        """
        Get chat credentials (chatId and apiKey) from database
        
        Args:
            chatName: Chat name from ChatCredentials enum
            
        Returns:
            dict: Dictionary with 'chatId' and 'apiKey' keys, or None if not found
        """
        try:
            credentialsHandler = CredentialsHandler()
            
            # Get chat ID
            chatIdCredentials = credentialsHandler.getCredentialsByType(chatName, CredentialType.CHAT_ID.value)
            if not chatIdCredentials:
                logger.info(f"No {CredentialType.CHAT_ID.value} found for {chatName}")
                return None
            
            # Get API key (bot token)
            apiKeyCredentials = credentialsHandler.getCredentialsByType(chatName, CredentialType.API_KEY.value)
            if not apiKeyCredentials:
                logger.info(f"No {CredentialType.API_KEY.value} found for {chatName}")
                return None
            
            return {
                'chatId': chatIdCredentials[CredentialField.API_KEY],  # CHAT_ID is stored in apikey field
                'apiKey': apiKeyCredentials[CredentialField.API_KEY],   # API_KEY (bot token)
                'chatName': chatName
            }
            
        except Exception as e:
            logger.info(f"Error getting chat credentials for {chatName}: {e}")
            return None
    
    @staticmethod
    def validateChatName(chatName: str) -> bool:
        """
        Validate chat name against ChatCredentials enum
        
        Args:
            chatName: Chat name to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        return ChatCredentials.isValidChatName(chatName)
    
    @staticmethod
    def formatUnixTime(unixTime: int) -> str:
        """
        Format unix timestamp to readable string
        
        Args:
            unixTime: Unix timestamp
            
        Returns:
            str: Formatted time string
        """
        try:
            dt = datetime.fromtimestamp(unixTime)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return "Unknown time"
    
    @staticmethod
    def getVolume24h(trackedToken: 'TrackedToken') -> Optional[float]:
        """
        Get 24h volume if available
        
        Args:
            trackedToken: Token to get volume for
            
        Returns:
            Optional[float]: 24h volume or None if not available
        """
        try:
            # This would need to be implemented based on your data structure
            # For now, return None as placeholder
            return None
        except Exception:
            return None
    
    @staticmethod
    def getMarketCap(trackedToken: 'TrackedToken') -> Optional[float]:
        """
        Get market cap if available
        
        Args:
            trackedToken: Token to get market cap for
            
        Returns:
            Optional[float]: Market cap or None if not available
        """
        try:
            # This would need to be implemented based on your data structure
            # For now, return None as placeholder
            return None
        except Exception:
            return None
    
    @staticmethod
    def getPriceChange24h(candle: 'OHLCVDetails') -> Optional[float]:
        """
        Calculate 24h price change if possible
        
        Args:
            candle: OHLCV candle data
            
        Returns:
            Optional[float]: 24h price change percentage or None if not available
        """
        try:
            # This would need historical data to calculate
            # For now, return None as placeholder
            return None
        except Exception:
            return None

