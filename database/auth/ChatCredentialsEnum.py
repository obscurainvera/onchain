"""
Chat Credentials Enum - Defines available chat groups for notifications

This enum contains all the chat groups that can receive notifications.
Each enum value corresponds to a servicename in the servicecredentials table.
"""

from enum import Enum
from typing import List


class ChatCredentials(Enum):
    """Enum for chat credential service names"""
    
    BULLISH_CROSS_CHAT = "BULLISH_CROSS_CHAT"
    BEARISH_CROSS_CHAT = "BEARISH_CROSS_CHAT"
    BAND_TOUCH_CHAT = "BAND_TOUCH_CHAT"
    AVWAP_BREAKOUT_CHAT = "AVWAP_BREAKOUT_CHAT"
    AVWAP_BREAKDOWN_CHAT = "AVWAP_BREAKDOWN_CHAT"
    STOCH_RSI_OVERSOLD_CHAT = "STOCH_RSI_OVERSOLD_CHAT"
    STOCH_RSI_OVERBOUGHT_CHAT = "STOCH_RSI_OVERBOUGHT_CHAT"
    
    @classmethod
    def getAllChatNames(cls) -> List[str]:
        """Get all available chat names"""
        return [chat.value for chat in cls]
    
    @classmethod
    def isValidChatName(cls, chatName: str) -> bool:
        """Check if a chat name is valid"""
        return chatName in cls.getAllChatNames()
    
    @classmethod
    def getByName(cls, chatName: str) -> 'ChatCredentials':
        """Get enum by chat name"""
        for chat in cls:
            if chat.value == chatName:
                return chat
        raise ValueError(f"Invalid chat name: {chatName}")
