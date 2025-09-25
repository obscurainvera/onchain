from config.Config import get_config
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from database.operations.BaseDBHandler import BaseDBHandler
from framework.notificationframework.NotificationEnums import NotificationStatus
from logs.logger import get_logger
from sqlalchemy import text
from typing import Optional
from datetime import datetime


logger = get_logger(__name__)

class NotificationHandler(BaseDBHandler):
    
    def __init__(self, conn_manager=None):
        if conn_manager is None:
            conn_manager = DatabaseConnectionManager()
        super().__init__(conn_manager)
        self.tableName = 'notification'
        self.createTables()
    
    def createTables(self) -> None:
        config = get_config()
        
        try:
            with self.conn_manager.transaction() as cursor:
                table_name = self.tableName
                default_status = NotificationStatus.PENDING.value
                
                if config.DB_TYPE == 'postgres':
                    cursor.execute(text("""
                        CREATE TABLE IF NOT EXISTS notification (
                            id SERIAL PRIMARY KEY,
                            source TEXT NOT NULL,
                            chatgroup TEXT NOT NULL,
                            content TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT %s,
                            tokenid TEXT,
                            strategytype TEXT,
                            servicetype TEXT,
                            errordetails TEXT,
                            buttons TEXT,
                            createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            updatedat TIMESTAMP,
                            sentat TIMESTAMP
                        )
                    """), (default_status,))
                else:
                    cursor.execute(text("""
                        CREATE TABLE IF NOT EXISTS notification (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            source TEXT NOT NULL,
                            chatgroup TEXT NOT NULL,
                            content TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT ?,
                            tokenid TEXT,
                            strategytype TEXT,
                            servicetype TEXT,
                            errordetails TEXT,
                            buttons TEXT,
                            createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            updatedat TIMESTAMP,
                            sentat TIMESTAMP
                        )
                    """), (default_status,))
        except Exception as e:
            logger.error(f"Error ensuring notification table exists: {e}")
    
    def createNotification(self, source: str, chatGroup: str, content: str, 
                          tokenId: Optional[str] = None, strategyType: Optional[str] = None,
                          buttons: Optional[str] = None) -> Optional[int]:
        """
        Create a new notification record
        
        Args:
            source: Notification source/type
            chatGroup: Chat group/chat ID
            content: Formatted message content
            tokenId: Optional token ID
            strategyType: Optional strategy type
            buttons: Optional buttons JSON string
            
        Returns:
            Optional[int]: Notification ID if successful, None otherwise
        """
        try:
            config = get_config()
            currentTime = datetime.utcnow()
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    cursor.execute(text("""
                        INSERT INTO notification (
                            source, chatgroup, content, status, tokenid, 
                            strategytype, buttons, createdat, updatedat
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """), (
                        source, chatGroup, content, "pending", 
                        tokenId, strategyType, buttons, currentTime, currentTime
                    ))
                    result = cursor.fetchone()
                    return result[0] if result else None
                else:
                    cursor.execute(text("""
                        INSERT INTO notification (
                            source, chatgroup, content, status, tokenid, 
                            strategytype, buttons, createdat, updatedat
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """), (
                        source, chatGroup, content, "pending", 
                        tokenId, strategyType, buttons, currentTime, currentTime
                    ))
                    return cursor.lastrowid
                    
        except Exception as e:
            logger.error(f"Error creating notification: {e}")
            return None
    
    def updateNotificationStatus(self, notificationId: int, status: str, 
                                errorDetails: Optional[str] = None) -> bool:
        """
        Update notification status
        
        Args:
            notificationId: ID of notification to update
            status: New status
            errorDetails: Optional error details
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            config = get_config()
            currentTime = datetime.utcnow()
            sentAt = currentTime if status == "sent" else None
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    cursor.execute(text("""
                        UPDATE notification 
                        SET status = %s, errordetails = %s, sentat = %s, updatedat = %s
                        WHERE id = %s
                    """), (status, errorDetails, sentAt, currentTime, notificationId))
                else:
                    cursor.execute(text("""
                        UPDATE notification 
                        SET status = ?, errordetails = ?, sentat = ?, updatedat = ?
                        WHERE id = ?
                    """), (status, errorDetails, sentAt, currentTime, notificationId))
                
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error updating notification status: {e}")
            return False
    
    def getNotificationById(self, notificationId: int) -> Optional[dict]:
        """
        Get notification by ID
        
        Args:
            notificationId: ID of notification to retrieve
            
        Returns:
            Optional[dict]: Notification data if found, None otherwise
        """
        try:
            config = get_config()
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    cursor.execute(text("""
                        SELECT id, source, chatgroup, content, status, tokenid, 
                               strategytype, servicetype, errordetails, buttons,
                               createdat, updatedat, sentat
                        FROM notification 
                        WHERE id = %s
                    """), (notificationId,))
                else:
                    cursor.execute(text("""
                        SELECT id, source, chatgroup, content, status, tokenid, 
                               strategytype, servicetype, errordetails, buttons,
                               createdat, updatedat, sentat
                        FROM notification 
                        WHERE id = ?
                    """), (notificationId,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'source': row[1],
                        'chatgroup': row[2],
                        'content': row[3],
                        'status': row[4],
                        'tokenid': row[5],
                        'strategytype': row[6],
                        'servicetype': row[7],
                        'errordetails': row[8],
                        'buttons': row[9],
                        'createdat': row[10],
                        'updatedat': row[11],
                        'sentat': row[12]
                    }
                return None
                
        except Exception as e:
            logger.error(f"Error getting notification by ID: {e}")
            return None
    