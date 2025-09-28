"""
Common notification data models and imports
"""
from dataclasses import dataclass
from typing import Optional, List

# Common models used by all notification types
@dataclass
class CommonMessage:
    """Common message format for sending"""
    formattedMessage: str
    tokenId: Optional[str] = None
    strategyType: Optional[str] = None
    buttons: Optional[List['MessageButton']] = None


@dataclass
class MessageButton:
    """Button for messages"""
    text: str
    url: str
