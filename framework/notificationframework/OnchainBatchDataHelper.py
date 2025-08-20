"""
Simple batch data helper for onchain notification strategies
Optimizes performance by batch loading historical and notification data
"""
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pytz
from database.operations.schema import OnchainInfo
from database.operations.PortfolioDB import PortfolioDB
from framework.notificationframework.NotificationEnums import NotificationStrategyType
from logs.logger import get_logger

logger = get_logger(__name__)


class OnchainBatchDataHelper:
    """
    Helper class to batch load data needed by notification strategies
    Reduces database hits by loading all required data in optimized batches
    """
    
    def __init__(self, db: PortfolioDB):
        self.db = db
        self.max_hours_needed = 5  # Maximum hours any strategy needs (with buffer)
        self.strategy_types = [
            NotificationStrategyType.FLUCTUATION.value,
            NotificationStrategyType.HUGE_JUMP.value,
            NotificationStrategyType.SUSTAINED_PERFORMANCE.value
        ]
    
    def getBatchData(self, tokens: List[OnchainInfo]) -> Tuple[Dict[str, Dict], Dict[str, List[Dict]], List[Dict]]:
        """
        Load all required data for tokens in optimized batches
        
        Args:
            tokens: List of tokens to load data for
            
        Returns:
            Tuple containing:
            - existing_tokens_info: Dict[token_id, token_info]
            - historical_data_map: Dict[token_id, List[historical_records]]
            - notification_records: List[notification_records]
        """
        if not tokens:
            return {}, {}, []
            
        token_ids = [token.tokenid for token in tokens]
        logger.info(f"Loading batch data for {len(token_ids)} tokens")
        
        # Step 1: Batch load existing token info
        existing_tokens_info = self.getOnchainInfo(token_ids)
        
        # Step 2: Batch load historical data (max hours needed)
        historical_data_map = self.getOnchainTokenHistoricalData(token_ids)
        
        # Step 3: Batch load notification records
        notification_records = self.getPastNotificationRecords(token_ids)
        
        logger.info(f"Successfully loaded batch data: {len(existing_tokens_info)} existing tokens, "
                   f"{sum(len(data) for data in historical_data_map.values())} historical records, "
                   f"{len(notification_records)} notification records")
        
        return existing_tokens_info, historical_data_map, notification_records
    
    def getOnchainInfo(self, token_ids: List[str]) -> Dict[str, Dict]:
        """Load existing token information in batch"""
        try:
            return self.db.onchain.getOnchainInfoTokens(token_ids)
        except Exception as e:
            logger.error(f"Error loading existing tokens info: {str(e)}")
            return {}
    
    def getOnchainTokenHistoricalData(self, token_ids: List[str]) -> Dict[str, List[Dict]]:
        """Load historical data for all tokens in batch"""
        try:
            return self.db.onchain.getBatchTokenHistoricalData(token_ids, self.max_hours_needed)
        except Exception as e:
            logger.error(f"Error loading historical data: {str(e)}")
            return {}
    
    def getPastNotificationRecords(self, token_ids: List[str]) -> List[Dict]:
        """Load notification records for all tokens and strategies in batch"""
        try:
            return self.db.notification.getBatchNotificationRecords(
                token_ids, 
                self.strategy_types, 
                self.max_hours_needed
            )
        except Exception as e:
            logger.error(f"Error loading notification records: {str(e)}")
            return []
    
    @staticmethod
    def filterHistoricalOnchainTokenDataByHrs(historical_data: List[Dict], hours: float) -> List[Dict]:
        """
        Filter historical data to specific time window
        
        Args:
            historical_data: List of historical data (sorted by createdat DESC)
            hours: Number of hours to look back
            
        Returns:
            List[Dict]: Filtered historical data
        """
        if not historical_data:
            return []
        
        # Calculate cutoff time
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # Filter records within time window
        filtered_data = []
        for record in historical_data:
            record_time = record.get('createdat')
            if record_time and isinstance(record_time, datetime):
                if record_time >= cutoff_time:
                    filtered_data.append(record)
            else:
                # If createdat is not a datetime object, include it (fallback)
                filtered_data.append(record)
        
        return filtered_data
    
    @staticmethod
    def hasRecentNotification(notification_records: List[Dict], token_id: str, strategy_type: str, hours: int) -> bool:
        """
        Check if token has recent notification for given strategy from batch data
        
        Args:
            notification_records: List of all notification records
            token_id: Token ID to check
            strategy_type: Strategy type to check
            hours: Hours to look back
            
        Returns:
            bool: True if recent notification exists
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        for record in notification_records:
            if (record.get('tokenid') == token_id and 
                record.get('strategytype') == strategy_type):
                
                record_time = record.get('createdat')
                if record_time and isinstance(record_time, datetime):
                    if record_time >= cutoff_time:
                        return True
        
        return False
    
    @staticmethod
    def getStrategyCooldownHrs(strategy_type: str) -> int:
        """
        Get cooldown hours for different strategy types
        
        Args:
            strategy_type: Strategy type
            
        Returns:
            int: Cooldown hours
        """
        cooldown_map = {
            NotificationStrategyType.FLUCTUATION.value: 1,
            NotificationStrategyType.HUGE_JUMP.value: 1,
            NotificationStrategyType.SUSTAINED_PERFORMANCE.value: 3
        }
        return cooldown_map.get(strategy_type, 1)
    
    @staticmethod
    def calculatePriceAnalytics(historical_data: List[Dict], hours: float = 3.0) -> Optional[Dict]:
        """
        Calculate comprehensive price analytics for a given time window
        
        Args:
            historical_data: Historical data sorted by createdat DESC (newest first)
            hours: Time window in hours to analyze
            
        Returns:
            Dict containing price analytics or None if insufficient data
        """
        try:
            if not historical_data:
                return None
            
            # Filter data to the specified time window
            filtered_data = OnchainBatchDataHelper.filterHistoricalOnchainTokenDataByHrs(historical_data, hours)
            
            if len(filtered_data) < 2:
                logger.warning(f"Insufficient data points ({len(filtered_data)}) for price analytics")
                return None
            
            # Sort by createdat ASC to get chronological order (oldest to newest)
            chronological_data = sorted(filtered_data, key=lambda x: x.get('createdat', datetime.min))
            
            # Extract prices and timestamps
            price_points = []
            for record in chronological_data:
                price = record.get('price')
                timestamp = record.get('createdat')
                if price is not None and timestamp:
                    price_points.append({
                        'price': float(price),
                        'timestamp': timestamp
                    })
            
            if len(price_points) < 2:
                logger.warning("Insufficient valid price points for analytics")
                return None
            
            # Calculate price metrics
            starting_point = price_points[0]
            ending_point = price_points[-1]
            
            # Find highest and lowest price points
            highest_point = max(price_points, key=lambda x: x['price'])
            lowest_point = min(price_points, key=lambda x: x['price'])
            
            # Calculate percentage changes with robust error handling
            def calculate_percentage_change(from_price: float, to_price: float) -> float:
                """Calculate percentage change with division by zero protection"""
                if from_price == 0:
                    return 0.0 if to_price == 0 else float('inf')
                return ((to_price - from_price) / from_price) * 100
            
            # Build analytics dictionary
            analytics = {
                'starting_price': {
                    'price': starting_point['price'],
                    'timestamp': starting_point['timestamp'],
                    'formatted_time': starting_point['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')
                },
                'ending_price': {
                    'price': ending_point['price'],
                    'timestamp': ending_point['timestamp'],
                    'formatted_time': ending_point['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'change_from_start_pct': round(calculate_percentage_change(
                        starting_point['price'], ending_point['price']
                    ), 2)
                },
                'highest_price': {
                    'price': highest_point['price'],
                    'timestamp': highest_point['timestamp'],
                    'formatted_time': highest_point['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'change_from_start_pct': round(calculate_percentage_change(
                        starting_point['price'], highest_point['price']
                    ), 2)
                },
                'lowest_price': {
                    'price': lowest_point['price'],
                    'timestamp': lowest_point['timestamp'],
                    'formatted_time': lowest_point['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'change_from_start_pct': round(calculate_percentage_change(
                        starting_point['price'], lowest_point['price']
                    ), 2),
                    'change_to_high_pct': round(calculate_percentage_change(
                        lowest_point['price'], highest_point['price']
                    ), 2)
                },
                'analytics_metadata': {
                    'total_data_points': len(price_points),
                    'time_window_hours': hours,
                    'price_volatility': round(calculate_percentage_change(
                        lowest_point['price'], highest_point['price']
                    ), 2)
                }
            }
            
            logger.debug(f"Calculated price analytics: {analytics['analytics_metadata']['total_data_points']} points, "
                        f"{analytics['analytics_metadata']['price_volatility']}% volatility")
            
            return analytics
            
        except Exception as e:
            logger.error(f"Error calculating price analytics: {str(e)}")
            return None

    def getNotificationCountsForToken(self, token_id: str) -> Dict[str, Dict[str, int]]:
        """
        Get notification counts for a specific token using the database connection
        
        Args:
            token_id: Token ID to get counts for
            
        Returns:
            Dict with notification counts by strategy type
        """
        try:
            return self.db.notification.getNotificationCountsByToken(token_id)
        except Exception as e:
            logger.error(f"Error getting notification counts for token {token_id}: {str(e)}")
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

    @staticmethod
    def convertUtcToIstTimeOnly(utc_datetime: datetime) -> Optional[str]:
        """
        Convert UTC datetime to IST and return only HH:MM format
        
        Args:
            utc_datetime: UTC datetime object
            
        Returns:
            String in HH:MM format or None if conversion fails
        """
        try:
            if not utc_datetime:
                return None
                
            # Ensure the datetime is timezone-aware (UTC)
            if utc_datetime.tzinfo is None:
                utc_datetime = pytz.UTC.localize(utc_datetime)
            elif utc_datetime.tzinfo != pytz.UTC:
                utc_datetime = utc_datetime.astimezone(pytz.UTC)
            
            # Convert to IST
            ist_timezone = pytz.timezone('Asia/Kolkata')
            ist_datetime = utc_datetime.astimezone(ist_timezone)
            
            # Return only HH:MM AM/PM format
            return ist_datetime.strftime('%I:%M %p')
            
        except Exception as e:
            logger.error(f"Error converting UTC to IST: {str(e)}")
            return None