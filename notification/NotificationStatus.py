"""
Notification status enum for tracking notification states
"""
from enum import Enum


class NotificationStatus(Enum):
    """
    Enum for notification status values.
    
    Used to track the lifecycle of notifications from creation to delivery.
    """
    
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    CANCELLED = "cancelled"
    
    def __str__(self) -> str:
        """String representation returns the value"""
        return self.value
    
    @classmethod
    def from_string(cls, status_str: str) -> 'NotificationStatus':
        """
        Create NotificationStatus from string value.
        
        Args:
            status_str: String representation of status
            
        Returns:
            NotificationStatus enum value
            
        Raises:
            ValueError: If status_str is not a valid status
        """
        for status in cls:
            if status.value == status_str:
                return status
        raise ValueError(f"Invalid notification status: {status_str}")
