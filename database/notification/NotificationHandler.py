from config.Config import get_config
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
"""
Handler for notification database operations
"""
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import json
import sqlite3
import pytz
from database.operations.BaseDBHandler import BaseDBHandler
from database.operations.schema import Notification, NotificationButton
from framework.notificationframework.NotificationEnums import NotificationStatus
from logs.logger import get_logger
from sqlalchemy import text


logger = get_logger(__name__)

class NotificationHandler(BaseDBHandler):
    """
    Handler for notification database operations
    """
    
    def __init__(self, conn_manager=None):
        if conn_manager is None:
            conn_manager = DatabaseConnectionManager()
        """Initialize with connection manager"""
        super().__init__(conn_manager)
        self.tableName = 'notification'
        self._ensureTableExists()
    
    def _ensureTableExists(self) -> None:
        """Ensure the notification table exists"""
        config = get_config()
        
        try:
            with self.conn_manager.transaction() as cursor:
                table_name = self.tableName
                default_status = NotificationStatus.PENDING.value
                
                if config.DB_TYPE == 'postgres':
                    # PostgreSQL syntax - use %s instead of named parameters
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
                    
                    # Add tokenid column if it doesn't exist (for existing databases)
                    try:
                        cursor.execute(text("""
                            ALTER TABLE notification 
                            ADD COLUMN IF NOT EXISTS tokenid TEXT
                        """))
                    except Exception:
                        # Column might already exist, ignore error
                        pass
                    
                    # Add strategytype column if it doesn't exist (for existing databases)
                    try:
                        cursor.execute(text("""
                            ALTER TABLE notification 
                            ADD COLUMN IF NOT EXISTS strategytype TEXT
                        """))
                    except Exception:
                        # Column might already exist, ignore error
                        pass
                    
                    # Create indexes for faster queries
                    cursor.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_notification_status
                        ON notification (status)
                    """))
                    cursor.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_notification_tokenid_createdat
                        ON notification (tokenid, createdat)
                    """))
                    cursor.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_notification_strategytype_tokenid_createdat
                        ON notification (strategytype, tokenid, createdat)
                    """))
                else:
                    # SQLite syntax
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
                    
                    # Add tokenid column if it doesn't exist (for existing SQLite databases)
                    try:
                        cursor.execute(text("ALTER TABLE notification ADD COLUMN tokenid TEXT"))
                    except Exception:
                        # Column might already exist, ignore error
                        pass
                    
                    # Add strategytype column if it doesn't exist (for existing SQLite databases)
                    try:
                        cursor.execute(text("ALTER TABLE notification ADD COLUMN strategytype TEXT"))
                    except Exception:
                        # Column might already exist, ignore error
                        pass
                    
                    # Create indexes for faster queries
                    cursor.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_notification_status
                        ON notification (status)
                    """))
                    cursor.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_notification_tokenid_createdat
                        ON notification (tokenid, createdat)
                    """))
                    cursor.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_notification_strategytype_tokenid_createdat
                        ON notification (strategytype, tokenid, createdat)
                    """))
        except Exception as e:
            logger.error(f"Error ensuring notification table exists: {e}")
            # Don't re-raise, as we want to allow graceful fallback
            
    def createNotification(self, notification: Notification) -> Optional[Notification]:

        try:
            config = get_config()
        
            with self.conn_manager.transaction() as cursor:
                now = self.getCurrentIstTime()
            
            # Set timestamps
                notification.createdat = now
                notification.updatedat = now
            
            # Serialize buttons to JSON if present
                buttons_json = json.dumps([{"text": btn.text, "url": btn.url} for btn in notification.buttons]) if notification.buttons else None
            
            # Insert into database
                if config.DB_TYPE == 'postgres':
                    insert_sql = f'''
                        INSERT INTO {self.tableName} 
                        (source, chatgroup, content, status, tokenid, strategytype, servicetype, errordetails, buttons, createdat, updatedat, sentat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    '''
                    cursor.execute(text(insert_sql), (
                        notification.source,
                        notification.chatgroup,
                        notification.content,
                        notification.status,
                        notification.tokenid,
                        notification.strategytype,
                        notification.servicetype,
                        notification.errordetails,
                        buttons_json,
                        notification.createdat,
                        notification.updatedat,
                        notification.sentat
                    ))
                    row = cursor.fetchone()
                    if row:
                        notification.id = row.get('id')
                    else:
                        logger.error("No ID returned after inserting notification")
                        return None
                            
            return notification
            
        except Exception as e:
            logger.error(f"Failed to create notification: {e}")
            return None
    
    def updateNotification(self, notification: Notification) -> bool:
        """
        Update an existing notification record
        
        Args:
            notification: Notification object to update
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            config = get_config()
            
            if not notification.id:
                logger.error("Cannot update notification without an ID")
                return False
            
            with self.conn_manager.transaction() as cursor:
                # Update timestamp
                notification.updatedat = self.getCurrentIstTime()
                
                # Serialize buttons to JSON if present
                buttons_json = json.dumps([{"text": btn.text, "url": btn.url} for btn in notification.buttons]) if notification.buttons else None
                
                # Update record
                if config.DB_TYPE == 'postgres':
                    update_sql = f'''
                        UPDATE {self.tableName}
                        SET source = %s,
                            chatgroup = %s,
                            content = %s,
                            status = %s,
                            servicetype = %s,
                            errordetails = %s,
                            buttons = %s,
                            updatedat = %s,
                            sentat = %s
                        WHERE id = %s
                    '''
                    cursor.execute(text(update_sql), (
                        notification.source,
                        notification.chatgroup,
                        notification.content,
                        notification.status,
                        notification.servicetype,
                        notification.errordetails,
                        buttons_json,
                        notification.updatedat,
                        notification.sentat,
                        notification.id
                    ))
                else:
                    update_sql = f'''
                        UPDATE {self.tableName}
                        SET source = ?,
                            chatgroup = ?,
                            content = ?,
                            status = ?,
                            servicetype = ?,
                            errordetails = ?,
                            buttons = ?,
                            updatedat = ?,
                            sentat = ?
                        WHERE id = ?
                    '''
                    cursor.execute(text(update_sql), (
                        notification.source,
                        notification.chatgroup,
                        notification.content,
                        notification.status,
                        notification.servicetype,
                        notification.errordetails,
                        buttons_json,
                        notification.updatedat,
                        notification.sentat,
                        notification.id
                    ))
                
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Failed to update notification: {e}")
            return False
    
    def getNotificationById(self, notificationId: int) -> Optional[Notification]:
        """
        Get a notification by ID
        
        Args:
            notificationId: ID of the notification to retrieve
            
        Returns:
            Optional[Notification]: Notification object if found, None otherwise
        """
        try:
            config = get_config()
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    select_sql = f'''
                        SELECT id, source, chatgroup, content, status, servicetype, 
                               errordetails, buttons, createdat, updatedat, sentat
                        FROM {self.tableName}
                        WHERE id = %s
                    '''
                    cursor.execute(text(select_sql), (notificationId,))
                else:
                    select_sql = f'''
                        SELECT id, source, chatgroup, content, status, servicetype, 
                               errordetails, buttons, createdat, updatedat, sentat
                        FROM {self.tableName}
                        WHERE id = ?
                    '''
                    cursor.execute(text(select_sql), (notificationId,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                return self._rowToNotification(row)
                
        except Exception as e:
            logger.error(f"Failed to get notification by ID: {e}")
            return None
    
    def getPendingNotifications(self, limit: int = 10) -> List[Notification]:
        """
        Get pending notifications to be sent
        
        Args:
            limit: Maximum number of notifications to retrieve
            
        Returns:
            List[Notification]: List of pending notifications
        """
        try:
            config = get_config()
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    select_sql = f'''
                        SELECT id, source, chatgroup, content, status, servicetype, 
                               errordetails, buttons, createdat, updatedat, sentat
                        FROM {self.tableName}
                        WHERE status = %s
                        ORDER BY createdat ASC
                        LIMIT %s
                    '''
                    cursor.execute(text(select_sql), (NotificationStatus.PENDING.value, limit))
                else:
                    select_sql = f'''
                        SELECT id, source, chatgroup, content, status, servicetype, 
                               errordetails, buttons, createdat, updatedat, sentat
                        FROM {self.tableName}
                        WHERE status = ?
                        ORDER BY createdat ASC
                        LIMIT ?
                    '''
                    cursor.execute(text(select_sql), (NotificationStatus.PENDING.value, limit))
                
                rows = cursor.fetchall()
                return [self._rowToNotification(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get pending notifications: {e}")
            return []
    
    def getFailedNotifications(self, limit: int = 10) -> List[Notification]:
        """
        Get failed notifications
        
        Args:
            limit: Maximum number of notifications to retrieve
            
        Returns:
            List[Notification]: List of failed notifications
        """
        try:
            config = get_config()
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    select_sql = f'''
                        SELECT id, source, chatgroup, content, status, servicetype, 
                               errordetails, buttons, createdat, updatedat, sentat
                        FROM {self.tableName}
                        WHERE status = %s
                        ORDER BY updatedat DESC
                        LIMIT %s
                    '''
                    cursor.execute(text(select_sql), (NotificationStatus.FAILED.value, limit))
                else:
                    select_sql = f'''
                        SELECT id, source, chatgroup, content, status, servicetype, 
                               errordetails, buttons, createdat, updatedat, sentat
                        FROM {self.tableName}
                        WHERE status = ?
                        ORDER BY updatedat DESC
                        LIMIT ?
                    '''
                    cursor.execute(text(select_sql), (NotificationStatus.FAILED.value, limit))
                
                rows = cursor.fetchall()
                return [self._rowToNotification(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get failed notifications: {e}")
            return []
    
    def getNotificationsBySource(self, source: str, limit: int = 10) -> List[Notification]:
        """
        Get notifications by source
        
        Args:
            source: Source of the notifications
            limit: Maximum number of notifications to retrieve
            
        Returns:
            List[Notification]: List of notifications from the specified source
        """
        try:
            config = get_config()
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    select_sql = f'''
                        SELECT id, source, chatgroup, content, status, servicetype, 
                               errordetails, buttons, createdat, updatedat, sentat
                        FROM {self.tableName}
                        WHERE source = %s
                        ORDER BY createdat DESC
                        LIMIT %s
                    '''
                    cursor.execute(text(select_sql), (source, limit))
                else:
                    select_sql = f'''
                        SELECT id, source, chatgroup, content, status, servicetype, 
                               errordetails, buttons, createdat, updatedat, sentat
                        FROM {self.tableName}
                        WHERE source = ?
                        ORDER BY createdat DESC
                        LIMIT ?
                    '''
                    cursor.execute(text(select_sql), (source, limit))
                
                rows = cursor.fetchall()
                return [self._rowToNotification(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get notifications by source: {e}")
            return []
    
    def _rowToNotification(self, row: Tuple) -> Notification:
        """
        Convert a database row to a Notification object
        
        Args:
            row: Database row tuple
            
        Returns:
            Notification: Notification object
        """
        # Parse buttons JSON
        buttons = []
        if row[7]:  # buttons field
            try:
                buttons_data = json.loads(row[7])
                buttons = [NotificationButton(text=btn["text"], url=btn["url"]) for btn in buttons_data]
            except Exception as e:
                logger.error(f"Failed to parse buttons JSON: {e}")
        
        return Notification(
            id=row[0],
            source=row[1],
            chatgroup=row[2],
            content=row[3],
            status=row[4],
            tokenid=row[5],
            strategytype=row[6],
            servicetype=row[7],
            errordetails=row[8],
            buttons=buttons,
            createdat=row[10] if row[10] else None,
            updatedat=row[11] if row[11] else None,
            sentat=row[12] if row[12] else None
        )
    
    def hasRecentNotification(self, tokenid: str, strategy_type: str, hours: int) -> bool:
        """
        Check if a token has received a notification of a specific type within the time window
        
        Args:
            tokenid: Token ID to check
            strategy_type: Type of notification strategy (from NotificationStrategyType enum)
            hours: Hours to look back for notifications
            
        Returns:
            bool: True if recent notification exists, False otherwise
        """
        try:
            config = get_config()
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    cursor.execute(
                        text(f"""
                        SELECT COUNT(*) as count
                        FROM notification 
                        WHERE tokenid = %s 
                        AND strategytype = %s
                        AND createdat >= NOW() - INTERVAL '{hours} HOUR'
                        """),
                        (tokenid, strategy_type)
                    )
                else:
                    cursor.execute(
                        text("""
                        SELECT COUNT(*) as count
                        FROM notification 
                        WHERE tokenid = ? 
                        AND strategytype = ?
                        AND createdat >= datetime('now', '-' || ? || ' hours')
                        """),
                        (tokenid, strategy_type, hours)
                    )
                result = cursor.fetchone()
                return result['count'] > 0 if result else False
        except Exception as e:
            logger.error(f"Error checking recent notifications for token {tokenid}: {str(e)}")
            return False

    def getBatchNotificationRecords(self, tokenids: List[str], strategy_types: List[str], hours: int) -> List[Dict]:
        """
        Get all notification records for multiple tokens and strategy types within time window
        Returns actual records so strategies can implement their own filtering logic
        
        Args:
            tokenids: List of token IDs to check
            strategy_types: List of strategy types to check  
            hours: Hours to look back for notifications
            
        Returns:
            List[Dict]: List of notification records
        """
        try:
            if not tokenids or not strategy_types:
                return []
                
            config = get_config()
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    cursor.execute(
                        text("""
                        SELECT * FROM notification 
                        WHERE tokenid = ANY(%s) 
                        AND strategytype = ANY(%s)
                        AND createdat >= NOW() - INTERVAL '%s HOUR'
                        ORDER BY tokenid, strategytype, createdat DESC
                        """),
                        (tokenids, strategy_types, hours)
                    )
                else:
                    token_placeholders = ','.join(['?' for _ in tokenids])
                    strategy_placeholders = ','.join(['?' for _ in strategy_types])
                    cursor.execute(
                        text(f"""
                        SELECT * FROM notification 
                        WHERE tokenid IN ({token_placeholders})
                        AND strategytype IN ({strategy_placeholders})
                        AND createdat >= datetime('now', '-' || ? || ' hours')
                        ORDER BY tokenid, strategytype, createdat DESC
                        """),
                        (*tokenids, *strategy_types, hours)
                    )
                
                results = cursor.fetchall()
                notification_records = [dict(row) for row in results]
                        
            logger.info(f"Retrieved {len(notification_records)} notification records for {len(tokenids)} tokens and {len(strategy_types)} strategy types over {hours} hours")
            return notification_records
            
        except Exception as e:
            logger.error(f"Error getting batch notification records: {str(e)}")
            return []

    def getNotificationCountsByToken(self, tokenid: str) -> Dict[str, Dict]:
        """
        Get notification counts for a specific token by strategy type
        Returns counts for all time and for today, with min/max times for sustained_performance
        
        Args:
            tokenid: Token ID to check
            
        Returns:
            Dict with structure:
            {
                'sustained_performance': {
                    'all_time': count, 
                    'today': count,
                    'min_time_today_utc': datetime or None,
                    'max_time_today_utc': datetime or None
                },
                'huge_jump': {'all_time': count, 'today': count},
                'new_top_ranked': {'all_time': count, 'today': count}
            }
        """
        try:
            config = get_config()
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    cursor.execute(
                        text("""
                        SELECT 
                            strategytype,
                            COUNT(*) as all_time_count,
                            COUNT(*) FILTER (WHERE DATE(createdat) = CURRENT_DATE) as today_count,
                            MIN(createdat) FILTER (WHERE DATE(createdat) = CURRENT_DATE AND strategytype = 'sustained_performance') as min_time_today,
                            MAX(createdat) FILTER (WHERE DATE(createdat) = CURRENT_DATE AND strategytype = 'sustained_performance') as max_time_today
                        FROM notification 
                        WHERE tokenid = %s 
                        AND strategytype IN ('sustained_performance', 'huge_jump', 'new_top_ranked')
                        GROUP BY strategytype
                        """),
                        (tokenid,)
                    )
                else:
                    cursor.execute(
                        text("""
                        SELECT 
                            strategytype,
                            COUNT(*) as all_time_count,
                            SUM(CASE WHEN DATE(createdat) = DATE('now') THEN 1 ELSE 0 END) as today_count,
                            MIN(CASE WHEN DATE(createdat) = DATE('now') AND strategytype = 'sustained_performance' THEN createdat END) as min_time_today,
                            MAX(CASE WHEN DATE(createdat) = DATE('now') AND strategytype = 'sustained_performance' THEN createdat END) as max_time_today
                        FROM notification 
                        WHERE tokenid = ? 
                        AND strategytype IN ('sustained_performance', 'huge_jump', 'new_top_ranked')
                        GROUP BY strategytype
                        """),
                        (tokenid,)
                    )
                
                results = cursor.fetchall()
                
                # Initialize counts with default values
                counts = {
                    'sustained_performance': {
                        'all_time': 0, 
                        'today': 0,
                        'min_time_today_utc': None,
                        'max_time_today_utc': None
                    },
                    'huge_jump': {'all_time': 0, 'today': 0},
                    'new_top_ranked': {'all_time': 0, 'today': 0}
                }
                
                # Track min/max times across all rows for sustained_performance
                min_time_today = None
                max_time_today = None
                
                # Update with actual counts
                for row in results:
                    strategy_type = row['strategytype']
                    if strategy_type in counts:
                        counts[strategy_type]['all_time'] = row['all_time_count']
                        counts[strategy_type]['today'] = row['today_count']
                        
                        # Collect min/max times for sustained_performance
                        if row.get('min_time_today'):
                            if min_time_today is None or row['min_time_today'] < min_time_today:
                                min_time_today = row['min_time_today']
                        if row.get('max_time_today'):
                            if max_time_today is None or row['max_time_today'] > max_time_today:
                                max_time_today = row['max_time_today']
                
                # Set min/max times for sustained_performance
                counts['sustained_performance']['min_time_today_utc'] = min_time_today
                counts['sustained_performance']['max_time_today_utc'] = max_time_today
                
                return counts
                
        except Exception as e:
            logger.error(f"Error getting notification counts for token {tokenid}: {str(e)}")
            return {
                'sustained_performance': {
                    'all_time': 0, 
                    'today': 0,
                    'min_time_today_utc': None,
                    'max_time_today_utc': None
                },
                'huge_jump': {'all_time': 0, 'today': 0},
                'new_top_ranked': {'all_time': 0, 'today': 0}
            } 