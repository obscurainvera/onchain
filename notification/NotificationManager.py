"""
Simple notification service
"""
from tkinter import N
from typing import Optional
import json
import requests
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from database.notification.NotificationHandler import NotificationHandler
from database.auth.CredentialsHandler import CredentialsHandler
from database.auth.ServiceCredentialsEnum import CredentialType
from notification.MessageFormat import CommonMessage
from notification.NotificationType import NotificationType
from logs.logger import get_logger

logger = get_logger(__name__)


class NotificationService:
    """
    Simple notification service that:
    1. Accepts chatId, notificationType, and specific notification data
    2. Formats the data using the notification type's formatter
    3. Saves to database and sends via Telegram
    """
    
    def __init__(self, dbManager: Optional[DatabaseConnectionManager] = None):
        """Initialize notification service"""
        if dbManager is None:
            dbManager = DatabaseConnectionManager()
        
        self.dbManager = dbManager
        self.notificationHandler = NotificationHandler(dbManager)
        self.credentialsHandler = CredentialsHandler(dbManager)
        self.session = requests.Session()
    
    def sendNotification(self, chatCredentials: dict, notificationType: NotificationType, 
                        commonMessage: CommonMessage) -> bool:
        
        try:
            # Step 1: Save to database as pending
            notificationId = self.recordNotification(
                chatCredentials.get('chatName'), notificationType, commonMessage
            )
            
            if not notificationId:
                logger.error("Failed to save notification to database")
                return False
            
            # Step 3: Send via Telegram
            success = self.sendTGMessage(chatCredentials, commonMessage)
            
            # Step 4: Update status
            if success:
                self.updateNotificationStatus(notificationId, "sent")
                logger.info(f"Successfully sent notification {notificationId}")
            else:
                self.updateNotificationStatus(notificationId, "failed", "Failed to send to Telegram")
                logger.error(f"Failed to send notification {notificationId}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in sendNotification: {e}")
            return False
    
    def recordNotification(self, chatName: str, notificationType: NotificationType, 
                               commonMessage: CommonMessage) -> Optional[int]:
        """Save notification record to database using NotificationHandler"""
        try:
            # Prepare buttons JSON
            buttonsJson = None
            if commonMessage.buttons:
                buttonsData = [{"text": btn.text, "url": btn.url} for btn in commonMessage.buttons]
                buttonsJson = json.dumps(buttonsData)
            
            # Use NotificationHandler to create the record
            return self.notificationHandler.createNotification(
                source=notificationType.value,
                chatGroup=chatName,
                content=commonMessage.formattedMessage,
                tokenId=commonMessage.tokenId,
                strategyType=commonMessage.strategyType,
                buttons=buttonsJson
            )
            
        except Exception as e:
            logger.error(f"Error saving notification record: {e}")
            return None
    
    
    def sendTGMessage(self, chatCredentials: dict, 
                       commonMessage: CommonMessage) -> bool:
        """Send message to Telegram"""
        try:
            botToken = chatCredentials.get('apiKey')
            chatId = chatCredentials.get('chatId')
            url = f"https://api.telegram.org/bot{botToken}/sendMessage"
            
            payload = {
                'chat_id': chatId,
                'text': commonMessage.formattedMessage,
                'parse_mode': 'HTML'
            }
            
            # Add buttons if present
            if commonMessage.buttons:
                inlineKeyboard = []
                row = []
                
                for button in commonMessage.buttons:
                    row.append({
                        "text": button.text,
                        "url": button.url
                    })
                    
                    # Create new row every 2 buttons
                    if len(row) == 2:
                        inlineKeyboard.append(row)
                        row = []
                
                # Add remaining buttons
                if row:
                    inlineKeyboard.append(row)
                
                payload['reply_markup'] = {
                    'inline_keyboard': inlineKeyboard
                }
            
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending to Telegram: {e}")
            return False
    
    def updateNotificationStatus(self, notificationId: int, status: str, 
                                 errorDetails: Optional[str] = None) -> None:
        """Update notification status using NotificationHandler"""
        try:
            # Use NotificationHandler to update the status
            self.notificationHandler.updateNotificationStatus(
                notificationId=notificationId,
                status=status,
                errorDetails=errorDetails
            )
                
        except Exception as e:
            logger.error(f"Error updating notification status: {e}")
    
    def getNotificationById(self, notificationId: int) -> Optional[dict]:
        """
        Get notification by ID using NotificationHandler
        
        Args:
            notificationId: ID of notification to retrieve
            
        Returns:
            Optional[dict]: Notification data if found
        """
        try:
            return self.notificationHandler.getNotificationById(notificationId)
        except Exception as e:
            logger.error(f"Error getting notification by ID: {e}")
            return None
