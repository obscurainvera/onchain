from config.Config import get_config
"""
Strategies for determining which onchain tokens should trigger notifications
"""
from typing import Optional, Dict, Any, List, Set, Tuple
from database.operations.schema import OnchainInfo
from database.operations.PortfolioDB import PortfolioDB
from database.onchain.OnchainHandler import OnchainHandler
from framework.notificationframework.NotificationManager import NotificationManager
from framework.notificationframework.NotificationContent import TokenNotificationContent
from framework.notificationframework.NotificationEnums import NotificationSource, ChatGroup, NotificationStrategyType
from framework.notificationframework.OnchainBatchDataHelper import OnchainBatchDataHelper
from logs.logger import get_logger

logger = get_logger(__name__)

class OnchainNotificationStrategies:
    """
    Strategies for determining which onchain tokens should trigger notifications
    This class centralizes all notification criteria for easier management and modification
    """
    
    @classmethod
    def getOnchainTokenInfo(cls, db: PortfolioDB, token_ids: List[str]) -> Dict[str, Dict]:
        """
        Get information about existing tokens efficiently in a single database query
        
        Args:
            db: Database instance
            token_ids: List of token IDs to check
            
        Returns:
            Dict[str, Dict]: Dictionary mapping token IDs to their info
        """
        if not token_ids:
            return {}
            
        try:
            # Use the efficient batch query method
            return db.onchain.getOnchainInfoTokens(token_ids)
        except Exception as e:
            logger.error(f"Error getting existing tokens info: {str(e)}")
            return {}
    
    @staticmethod
    def is_new_token(token: OnchainInfo, existingToken: Optional[Dict]) -> bool:
        """
        Determine if a token is new (not previously seen in the database)
        
        Args:
            token: OnchainInfo object to evaluate
            existingToken: Token info from database if exists, None if new
            
        Returns:
            bool: True if token is new, False otherwise
        """
        isNewToken = existingToken is not None and existingToken.get('count', 0) == 1
        
        if isNewToken:
            logger.info(f"Found new token: {token.name} with rank {token.rank}")
            
        return isNewToken
    
    @staticmethod
    def is_top_ranked(token: OnchainInfo, min_rank: int = 1, max_rank: int = 10) -> bool:
        """
        Determine if a token has a top rank within the specified range
        
        Args:
            token: OnchainInfo object to evaluate
            min_rank: Minimum rank (inclusive)
            max_rank: Maximum rank (inclusive)
            
        Returns:
            bool: True if token rank is within range, False otherwise
        """
        if token.rank and min_rank <= token.rank <= max_rank:
            logger.info(f"Token {token.name} has top rank: {token.rank}")
            return True
            
        return False
    
    @staticmethod
    def has_high_liquidity(token: OnchainInfo, min_liquidity: float = 50000) -> bool:
        """
        Determine if a token has high liquidity
        
        Args:
            token: OnchainInfo object to evaluate
            min_liquidity: Minimum liquidity threshold
            
        Returns:
            bool: True if token has high liquidity, False otherwise
        """
        if token.liquidity and token.liquidity > min_liquidity:
            logger.info(f"Token {token.name} has high liquidity: {token.liquidity}")
            return True
            
        return False
    
    @staticmethod
    def has_high_price_change(token: OnchainInfo, min_change_percent: float = 5) -> bool:
        """
        Determine if a token has significant price change in the last hour
        
        Args:
            token: OnchainInfo object to evaluate
            min_change_percent: Minimum price change percentage
            
        Returns:
            bool: True if token has significant price change, False otherwise
        """
        if token.price1h and token.price1h > min_change_percent:
            logger.info(f"Token {token.name} has high price change: {token.price1h}%")
            return True
            
        return False
    
    @staticmethod
    def has_many_makers(token: OnchainInfo, min_makers: int = 100) -> bool:
        """
        Determine if a token has a high number of makers
        
        Args:
            token: OnchainInfo object to evaluate
            min_makers: Minimum number of makers
            
        Returns:
            bool: True if token has many makers, False otherwise
        """
        if token.makers and token.makers > min_makers:
            logger.info(f"Token {token.name} has many makers: {token.makers}")
            return True
            
        return False
    
    @staticmethod
    def shouldNotifyNewAndTopTanked(token: OnchainInfo, existingToken: Optional[Dict]) -> bool:
        """
        Current notification strategy: Only notify for new tokens with rank 1-10
        
        Args:
            token: OnchainInfo object to check
            existingToken: Existing token info from database or None if token is new
            
        Returns:
            bool: True if notification should be sent, False otherwise
        """
        isNewToken = OnchainNotificationStrategies.is_new_token(token, existingToken)
        isTopRanked = OnchainNotificationStrategies.is_top_ranked(token, 1, 10)
        
        if isNewToken and isTopRanked:
            logger.info(f"Will send notification for new token {token.name} with rank {token.rank}")
            return True
        
        return False
    
    
    @staticmethod
    def shouldNotifyFluctuation(token: OnchainInfo, existingToken: Optional[Dict], db: PortfolioDB) -> Tuple[bool, Dict]:
        """
        Detect tokens oscillating between top 10 and top 20 ranks
        
        Args:
            token: Current token info
            existingToken: Existing token from database
            db: Database instance for historical queries
            
        Returns:
            Tuple[bool, Dict]: (should_notify, fluctuation_details)
        """
        # Only check existing tokens in top 10
        if not existingToken or existingToken.get('count', 0) <= 1:
            return False, {}
            
        if not token.rank or token.rank > 10:
            return False, {}
            
        # Check for recent fluctuation notifications (1 hour cooldown)
        if db.notification.hasRecentNotification(token.tokenid, NotificationStrategyType.FLUCTUATION.value, 1):
            return False, {}
            
        # Get last 1 hour of historical data
        historical_data = db.onchain.getTokenHistoricalData(token.tokenid, 1)
        
        if len(historical_data) < 10:
            return False, {}
            
        # Analyze rank fluctuation pattern
        ranks = [row['rank'] for row in historical_data if row['rank'] is not None]
            
        # Count oscillations between top 10 (<=10) and top 20 (11-20)
        oscillations = 0
        prev_zone = None
        
        for rank in reversed(ranks):  # Process chronologically
            if rank <= 10:
                current_zone = "top10"
            elif rank <= 20:
                current_zone = "top20"
            else:
                # Token went beyond top 20, not a fluctuation
                return False, {}
                
            if prev_zone and prev_zone != current_zone:
                oscillations += 1
                
            prev_zone = current_zone
            
        # Need at least 2 complete oscillations (4 zone changes)
        if oscillations >= 8:
            fluctuation_details = {
                "oscillation_count": oscillations // 2,
                "data_points": len(ranks),
                "rank_range": f"{min(ranks)}-{max(ranks)}"
            }
            logger.info(f"Token {token.name} shows fluctuation pattern: {oscillations//2} oscillations")
            return True, fluctuation_details
            
        return False, {}
    
    @staticmethod
    def shouldNotifyHugeJump(token: OnchainInfo, existingToken: Optional[Dict], db: PortfolioDB) -> Tuple[bool, Dict]:
        """
        Detect tokens jumping from rank >20 to top 15 with sustained performance
        
        Logic: Find 4 consecutive entries with rank ≤15 that are preceded by rank >20
        
        Args:
            token: Current token info
            existingToken: Existing token from database
            db: Database instance for historical queries
            
        Returns:
            Tuple[bool, Dict]: (should_notify, jump_details)
        """
        # Only check existing tokens currently in top 15
        if not existingToken or existingToken.get('count', 0) <= 1:
            return False, {}
            
        if not token.rank or token.rank > 15:
            return False, {}
            
        # Get last 30 minutes of historical data (0.5 hours)
        historical_data = db.onchain.getTokenHistoricalData(token.tokenid, 0.5)
        
        if len(historical_data) < 5:  # Need at least 5 data points (4 consecutive + 1 preceding)
            return False, {}
            
        ranks = [row['rank'] for row in historical_data if row['rank'] is not None]
        
        if len(ranks) < 5:
            return False, {}
            
        # Reverse to get chronological order (oldest to newest)
        chronological_ranks = list(reversed(ranks))
        
        # Look for pattern: rank >20 followed by 4 consecutive ranks ≤15
        for i in range(len(chronological_ranks) - 4):
            # Check if current position has rank > 20
            if chronological_ranks[i] > 20:
                # Check if next 4 positions all have rank ≤ 15
                next_four = chronological_ranks[i + 1:i + 5]
                if len(next_four) == 4 and all(r <= 15 for r in next_four):
                    # Found the pattern!
                    jump_details = {
                        "from_rank": chronological_ranks[i],
                        "to_rank": token.rank,
                        "consecutive_top15": 4,
                        "jump_magnitude": chronological_ranks[i] - token.rank,
                        "sustained_ranks": next_four
                    }
                    logger.info(f"Token {token.name} huge jump: rank {chronological_ranks[i]} -> sustained top 15 ({next_four}) -> current {token.rank}")
                    return True, jump_details
                    
        return False, {}
    
    @staticmethod
    def shouldNotifySustainedPerformance(token: OnchainInfo, existingToken: Optional[Dict], db: PortfolioDB) -> Tuple[bool, Dict]:
        """
        Detect tokens maintaining top 10 ranks for extended periods (2+ hours)
        
        Args:
            token: Current token info
            existingToken: Existing token from database
            db: Database instance for historical queries
            
        Returns:
            Tuple[bool, Dict]: (should_notify, performance_details)
        """
        # Only check existing tokens in top 10
        if not existingToken or existingToken.get('count', 0) <= 1:
            return False, {}
            
        if not token.rank or token.rank > 10:
            return False, {}
            
        # Check for recent sustained performance notifications (4 hour cooldown)
        if db.notification.hasRecentNotification(token.tokenid, NotificationStrategyType.SUSTAINED_PERFORMANCE.value, 3):
            return False, {}
            
        historical_data = db.onchain.getTokenHistoricalData(token.tokenid, 3)
        
        if len(historical_data) < 40:
            return False, {}
            
        ranks = [row['rank'] for row in historical_data if row['rank'] is not None]
            
        # Calculate performance metrics
        top10_periods = sum(1 for r in ranks if r <= 10)
        total_periods = len(ranks)
        fluctuation_count = sum(1 for r in ranks if 11 <= r <= 20)
        
        # Calculate sustained percentage (with penalties for fluctuations)
        sustained_percentage = (top10_periods / total_periods) * 100
        
        # Must maintain top 10 for >= 80% of time
        if sustained_percentage >= 90:
            performance_details = {
                "sustained_percentage": round(sustained_percentage, 1),
                "top10_periods": top10_periods,
                "total_periods": total_periods,
                "fluctuation_count": fluctuation_count,
                "duration_hours": round(total_periods * 2 / 60, 1)  # Convert to hours
            }
            logger.info(f"Token {token.name} sustained performance: {sustained_percentage:.1f}% over {total_periods} periods")
            return True, performance_details
            
        return False, {}
        
    @staticmethod
    def getChatGroupForStrategy(strategy_name: str) -> ChatGroup:
        """
        Get the appropriate chat group for a notification strategy
        Different strategies may send notifications to different chat groups
        
        Args:
            strategy_name: Name of the strategy being used
            
        Returns:
            ChatGroup: The chat group to send notifications to
        """
        # Map strategies to chat groups
        strategy_chat_map = {
            NotificationStrategyType.NEW_TOP_RANKED.value: ChatGroup.ONCHAIN_CHAT,
            NotificationStrategyType.FLUCTUATION.value: ChatGroup.ONCHAIN_FLUX_CHAT,
            NotificationStrategyType.HUGE_JUMP.value: ChatGroup.ONCHAIN_FLUX_CHAT,
            NotificationStrategyType.SUSTAINED_PERFORMANCE.value: ChatGroup.ONCHAIN_SUSTAINING_CHAT,
        }
        
        # Return the mapped chat group or default to ONCHAIN_CHAT
        return strategy_chat_map.get(strategy_name, ChatGroup.ONCHAIN_CHAT)
        
    @staticmethod
    def createNotificationContent(onchainTokenInfo: OnchainInfo, strategyName: str, strategy_details: Dict = None) -> TokenNotificationContent:
        """
        Convert OnchainInfo object to TokenNotificationContent with strategy-specific details
        
        Args:
            onchainTokenInfo: OnchainInfo object to convert
            strategyName: Name of the strategy triggering the notification
            strategy_details: Additional details specific to the strategy
            
        Returns:
            TokenNotificationContent: Notification content for the token
        """
        
        return TokenNotificationContent(
            subject=strategyName,
            tokenid=onchainTokenInfo.tokenid,
            name=onchainTokenInfo.name,
            chain=onchainTokenInfo.chain,
            price=onchainTokenInfo.price,
            marketcap=onchainTokenInfo.marketcap,
            liquidity=onchainTokenInfo.liquidity,
            makers=onchainTokenInfo.makers,
            rank=onchainTokenInfo.rank,
            id=onchainTokenInfo.id,
            onchaininfoid=onchainTokenInfo.onchaininfoid,
            age=onchainTokenInfo.age,
            count=onchainTokenInfo.count,
            createdat=onchainTokenInfo.createdat,
            updatedat=onchainTokenInfo.updatedat,
            dexScreenerUrl=f"https://dexscreener.com/solana/{onchainTokenInfo.tokenid}",
            otherinfo=strategy_details
        )
        
    @classmethod
    def handleNotification(cls, tokenToBeProcessed: OnchainInfo, onchainInfoForToken: Optional[Dict], notificationManager: NotificationManager, db: PortfolioDB) -> bool:
        """
        Process a token and send notification if it meets the criteria
        
        Args:
            token: OnchainInfo object to process
            existingToken: Existing token info from database or None if token is new
            notificationManager: NotificationManager instance for sending notifications
            db: Database instance for historical queries
            
        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        try:
            # Determine which strategy to use and if notification should be sent
            strategyName = None
            shouldNotify = False
            strategy_details = {}
            
            # Check new top ranked strategy (existing)
            if cls.shouldNotifyNewAndTopTanked(tokenToBeProcessed, onchainInfoForToken):
                strategyName = NotificationStrategyType.NEW_TOP_RANKED.value
                shouldNotify = True
            
            # Check returning token strategies (only if not already triggering new token notification)
            if not shouldNotify:
                # Check fluctuation detection
                fluctuation_result, fluctuation_details = cls.shouldNotifyFluctuation(tokenToBeProcessed, onchainInfoForToken, db)
                if fluctuation_result:
                    strategyName = NotificationStrategyType.FLUCTUATION.value
                    shouldNotify = True
                    strategy_details = fluctuation_details
                
                # Check huge jump detection (if not fluctuation)
                if not shouldNotify:
                    jump_result, jump_details = cls.shouldNotifyHugeJump(tokenToBeProcessed, onchainInfoForToken, db)
                    if jump_result:
                        strategyName = NotificationStrategyType.HUGE_JUMP.value
                        shouldNotify = True
                        strategy_details = jump_details
                
                # Check sustained performance (if not other patterns)
                if not shouldNotify:
                    sustained_result, sustained_details = cls.shouldNotifySustainedPerformance(tokenToBeProcessed, onchainInfoForToken, db)
                    if sustained_result:
                        strategyName = NotificationStrategyType.SUSTAINED_PERFORMANCE.value
                        shouldNotify = True
                        strategy_details = sustained_details
            
            if not shouldNotify:
                return False
                
            # Get the appropriate chat group for this strategy
            chatGroup = cls.getChatGroupForStrategy(strategyName)
            logger.info(f"Using strategy '{strategyName}' with chat group '{chatGroup.value}' for token {tokenToBeProcessed.name}")
                
            # Convert to notification content with strategy details
            content = cls.createNotificationContent(tokenToBeProcessed, strategyName, strategy_details)
            
            # Send notification to the strategy-specific chat group
            result = notificationManager.sendTokenNotification(
                source=NotificationSource.ONCHAIN,
                tokenContent=content,
                chatGroup=chatGroup
            )
            
            if result:
                logger.info(f"Successfully sent {strategyName} notification for token {tokenToBeProcessed.name} with rank {tokenToBeProcessed.rank}")
            else:
                logger.warning(f"Failed to send {strategyName} notification for token {tokenToBeProcessed.name}")
                
            return result
            
        except Exception as e:
            logger.error(f"Error processing token {tokenToBeProcessed.name} for notification: {str(e)}")
            return False
            
    @classmethod
    def sendNotification(cls, tokensNeedsToBeProcessed: List[OnchainInfo], db: PortfolioDB, notificationManager: NotificationManager) -> int:
        """
        Process a list of tokens and send notifications for those that meet criteria
        
        Args:
            tokens: List of OnchainInfo objects to process
            db: Database instance to efficiently query existing tokens
            notificationManager: NotificationManager instance for sending notifications
            
        Returns:
            int: Number of notifications sent successfully
        """
        if not tokensNeedsToBeProcessed:
            logger.info("No tokens to process for notifications")
            return 0
            
        sentCount = 0
        
        # Extract all token IDs
        tokenIdsNeedsToBeProcessed = [token.tokenid for token in tokensNeedsToBeProcessed]
        
        # Get existing tokens info in a single efficient query
        onchainInfoForTokenNeedsToBeProcessed = cls.getOnchainTokenInfo(db, tokenIdsNeedsToBeProcessed)
        logger.info(f"Found {len(onchainInfoForTokenNeedsToBeProcessed)} existing tokens out of {len(tokenIdsNeedsToBeProcessed)} total tokens")
        
        # Process tokens that meet notification criteria
        for token in tokensNeedsToBeProcessed:
            try:
                # Get existing token info if available
                onchainInfoForToken = onchainInfoForTokenNeedsToBeProcessed.get(token.tokenid)
                
                # Process token for notification
                if cls.handleNotification(token, onchainInfoForToken, notificationManager, db):
                    sentCount += 1
                    
            except Exception as e:
                logger.error(f"Error processing token {token.name}: {str(e)}")
                continue
                
        logger.info(f"Sent {sentCount} onchain token notifications")
        return sentCount
    
    @classmethod
    def sendNotificationOptimized(cls, tokensNeedsToBeProcessed: List[OnchainInfo], db: PortfolioDB, notificationManager: NotificationManager) -> int:
        if not tokensNeedsToBeProcessed:
            logger.info("No tokens to process for notifications")
            return 0
            
        logger.info(f"Starting optimized notification processing for {len(tokensNeedsToBeProcessed)} tokens")
        
        # Step 1: Load all required data in optimized batches
        batch_helper = OnchainBatchDataHelper(db)
        onchainTokenInfo, onchainHistoricalData, pastNotificationData = batch_helper.getBatchData(tokensNeedsToBeProcessed)
        
        logger.info(f"Loaded batch data: {len(onchainTokenInfo)} existing tokens, "
                   f"{sum(len(data) for data in onchainHistoricalData.values())} historical records, "
                   f"{len(pastNotificationData)} notification records")
        
        # Step 2: Process each token using batch data
        sentCount = 0
        for token in tokensNeedsToBeProcessed:
            try:
                # Get data for this token from batch
                tokenInfo = onchainTokenInfo.get(token.tokenid)
                historicalTokenData = onchainHistoricalData.get(token.tokenid, [])
                
                # Process token for notification using batch data
                if cls.handleNotificationOptimized(token, tokenInfo, historicalTokenData, pastNotificationData, notificationManager, batch_helper):
                    sentCount += 1
                    
            except Exception as e:
                logger.error(f"Error processing token {token.name}: {str(e)}")
                continue
                
        logger.info(f"Optimized processing sent {sentCount} onchain token notifications")
        return sentCount
    
    @classmethod
    def handleNotificationOptimized(cls, token: OnchainInfo, existing_token_info: Optional[Dict], 
                                  historical_data: List[Dict], notification_records: List[Dict], 
                                  notificationManager: NotificationManager, batch_helper: OnchainBatchDataHelper = None) -> bool:
        """
        Process a token using batch-loaded data to determine if notification should be sent
        
        Args:
            token: OnchainInfo object to process
            existing_token_info: Existing token info from batch data
            historical_data: Historical data for this token from batch data
            notification_records: All notification records from batch data
            notificationManager: NotificationManager instance
            batch_helper: OnchainBatchDataHelper instance for additional queries
            
        Returns:
            bool: True if notification was sent successfully
        """
        try:
            strategyName = None
            shouldNotify = False
            strategy_details = {}
            
            # Check new top ranked strategy (existing)
            if cls.shouldNotifyNewAndTopTanked(token, existing_token_info):
                strategyName = NotificationStrategyType.NEW_TOP_RANKED.value
                shouldNotify = True
            
            # Check returning token strategies using batch data
            if not shouldNotify:
                # Check fluctuation detection
                fluctuation_result, fluctuation_details = cls.shouldNotifyFluctuationOptimized(
                    token, existing_token_info, historical_data, notification_records
                )
                if fluctuation_result:
                    strategyName = NotificationStrategyType.FLUCTUATION.value
                    shouldNotify = True
                    strategy_details = fluctuation_details
                
                # Check huge jump detection (if not fluctuation)
                if not shouldNotify:
                    jump_result, jump_details = cls.shouldNotifyHugeJumpOptimized(
                        token, existing_token_info, historical_data, notification_records
                    )
                    if jump_result:
                        strategyName = NotificationStrategyType.HUGE_JUMP.value
                        shouldNotify = True
                        strategy_details = jump_details
                
                # Check sustained performance (if not other patterns)
                if not shouldNotify:
                    sustained_result, sustained_details = cls.shouldNotifySustainedPerformanceOptimized(
                        token, existing_token_info, historical_data, notification_records, batch_helper
                    )
                    if sustained_result:
                        strategyName = NotificationStrategyType.SUSTAINED_PERFORMANCE.value
                        shouldNotify = True
                        strategy_details = sustained_details
            
            if not shouldNotify:
                return False
                
            # Get the appropriate chat group for this strategy
            chatGroup = cls.getChatGroupForStrategy(strategyName)
            logger.info(f"Using strategy '{strategyName}' with chat group '{chatGroup.value}' for token {token.name}")
                
            # Convert to notification content with strategy details
            content = cls.createNotificationContent(token, strategyName, strategy_details)
            
            # Send notification to the strategy-specific chat group
            result = notificationManager.sendTokenNotification(
                source=NotificationSource.ONCHAIN,
                tokenContent=content,
                chatGroup=chatGroup
            )
            
            if result:
                logger.info(f"Successfully sent {strategyName} notification for token {token.name} with rank {token.rank}")
            else:
                logger.warning(f"Failed to send {strategyName} notification for token {token.name}")
                
            return result
            
        except Exception as e:
            logger.error(f"Error processing token {token.name} for notification: {str(e)}")
            return False
    
    @staticmethod
    def shouldNotifyFluctuationOptimized(token: OnchainInfo, existing_token_info: Optional[Dict], 
                                       historical_data: List[Dict], notification_records: List[Dict]) -> Tuple[bool, Dict]:
        """
        OPTIMIZED: Detect tokens oscillating between top 10 and top 20 ranks using batch data
        
        Args:
            token: Current token info
            existing_token_info: Existing token from batch data
            historical_data: Historical data for this token from batch data
            notification_records: All notification records from batch data
            
        Returns:
            Tuple[bool, Dict]: (should_notify, fluctuation_details)
        """
        # Only check existing tokens in top 10
        if not existing_token_info or existing_token_info.get('count', 0) <= 1:
            return False, {}
            
        if not token.rank or token.rank > 10:
            return False, {}
            
        # Check for recent fluctuation notifications (1 hour cooldown) using batch data
        if OnchainBatchDataHelper.hasRecentNotification(
            notification_records, token.tokenid, NotificationStrategyType.FLUCTUATION.value, 1
        ):
            return False, {}
            
        # Filter historical data to last 1 hour
        filtered_historical_data = OnchainBatchDataHelper.filterHistoricalOnchainTokenDataByHrs(historical_data, 1.0)
        
        if len(filtered_historical_data) < 5:
            return False, {}
            
        # Analyze rank fluctuation pattern
        ranks = [row['rank'] for row in filtered_historical_data if row['rank'] is not None]
            
        # Count oscillations between top 10 (<=10) and top 20 (11-20)
        oscillations = 0
        prev_zone = None
        
        for rank in reversed(ranks):  # Process chronologically
            if rank <= 10:
                current_zone = "top10"
            elif rank <= 20:
                current_zone = "top20"
            else:
                # Token went beyond top 20, not a fluctuation
                return False, {}
                
            if prev_zone and prev_zone != current_zone:
                oscillations += 1
                
            prev_zone = current_zone
            
        # Calculate fluctuation percentage: 80% of records should be fluctuating
        total_records = len(ranks)
        fluctuation_percentage = (oscillations / total_records) * 100 if total_records > 0 else 0
        
        # Notify if 80% or more of the records show fluctuation pattern
        if fluctuation_percentage >=60.0:
            fluctuation_details = {
                "oscillation_count": oscillations // 2,
                "data_points": len(ranks),
                "rank_range": f"{min(ranks)}-{max(ranks)}",
                "fluctuation_percentage": round(fluctuation_percentage, 1)
            }
            logger.info(f"Token {token.name} shows fluctuation pattern: {fluctuation_percentage:.1f}% fluctuating ({oscillations} oscillations in {total_records} records)")
            return True, fluctuation_details
            
        return False, {}
    
    @staticmethod
    def shouldNotifyHugeJumpOptimized(token: OnchainInfo, existing_token_info: Optional[Dict],
                                    historical_data: List[Dict], notification_records: List[Dict]) -> Tuple[bool, Dict]:
        """
        OPTIMIZED: Detect tokens jumping from rank >20 to top 15 with sustained performance using batch data
        
        Args:
            token: Current token info
            existing_token_info: Existing token from batch data
            historical_data: Historical data for this token from batch data
            notification_records: All notification records from batch data
            
        Returns:
            Tuple[bool, Dict]: (should_notify, jump_details)
        """
        # Only check existing tokens currently in top 15
        if not existing_token_info or existing_token_info.get('count', 0) <= 1:
            return False, {}
            
        if not token.rank or token.rank > 15:
            return False, {}
            
        # Check for recent huge jump notifications (1 hour cooldown) using batch data
        if OnchainBatchDataHelper.hasRecentNotification(
            notification_records, token.tokenid, NotificationStrategyType.HUGE_JUMP.value, 1
        ):
            return False, {}
            
        # Filter historical data to last 1 hour
        filtered_historical_data = OnchainBatchDataHelper.filterHistoricalOnchainTokenDataByHrs(historical_data, 1.0)
        
        if len(filtered_historical_data) < 5:  # Need at least 21 data points (20 consecutive + 1 preceding)
            return False, {}
            
        ranks = [row['rank'] for row in filtered_historical_data if row['rank'] is not None]
        
            
        # Reverse to get chronological order (oldest to newest)
        chronological_ranks = list(reversed(ranks))
        
        # Look for pattern: rank >20 followed by 20 consecutive ranks ≤15
        for i in range(len(chronological_ranks) - 3):
            # Check if current position has rank > 20
            if chronological_ranks[i] > 20:
                # Check if next 20 positions all have rank ≤ 15
                next_three = chronological_ranks[i + 1:i + 4]
                if len(next_three) == 3 and all(r <= 15 for r in next_three):
                    # Found the pattern!
                    jump_details = {
                        "from_rank": chronological_ranks[i],
                        "to_rank": token.rank,
                        "consecutive_top15": 3,
                        "jump_magnitude": chronological_ranks[i] - token.rank,
                        "sustained_ranks": next_three
                    }
                    logger.info(f"Token {token.name} huge jump: rank {chronological_ranks[i]} -> sustained top 15 ({len(next_three)} consecutive) -> current {token.rank}")
                    return True, jump_details
                    
        return False, {}
    
    @staticmethod
    def shouldNotifySustainedPerformanceOptimized(token: OnchainInfo, existing_token_info: Optional[Dict],
                                                historical_data: List[Dict], notification_records: List[Dict],
                                                batch_helper: OnchainBatchDataHelper = None) -> Tuple[bool, Dict]:
        """
        OPTIMIZED: Detect tokens maintaining top 10 ranks for extended periods (2+ hours) using batch data
        Includes comprehensive price analytics for the 3-hour sustenance window
        
        Args:
            token: Current token info
            existing_token_info: Existing token from batch data
            historical_data: Historical data for this token from batch data
            notification_records: All notification records from batch data
            batch_helper: OnchainBatchDataHelper instance for additional queries
            
        Returns:
            Tuple[bool, Dict]: (should_notify, performance_details with price analytics and notification history)
        """
        # Only check existing tokens in top 10
        if not existing_token_info or existing_token_info.get('count', 0) <= 1:
            return False, {}
            
        if not token.rank or token.rank > 10:
            return False, {}
            
        # Check for recent sustained performance notifications (3 hour cooldown) using batch data
        if OnchainBatchDataHelper.hasRecentNotification(
            notification_records, token.tokenid, NotificationStrategyType.SUSTAINED_PERFORMANCE.value, 3
        ):
            return False, {}
            
        # Filter historical data to last 3 hours
        filtered_historical_data = OnchainBatchDataHelper.filterHistoricalOnchainTokenDataByHrs(historical_data, 3.0)
        
        if len(filtered_historical_data) < 15:
            return False, {}
            
        ranks = [row['rank'] for row in filtered_historical_data if row['rank'] is not None]
            
        # Calculate performance metrics
        top10_periods = sum(1 for r in ranks if r <= 10)
        total_periods = len(ranks)
        fluctuation_count = sum(1 for r in ranks if 11 <= r <= 20)
        
        # Calculate sustained percentage (with penalties for fluctuations)
        sustained_percentage = (top10_periods / total_periods) * 100
        
        # Must maintain top 10 for >= 80% of time
        if sustained_percentage >= 80:
            # Calculate comprehensive price analytics for the 3-hour window
            price_analytics = OnchainBatchDataHelper.calculatePriceAnalytics(historical_data, 3.0)
            
            # Get notification counts for this token
            notification_counts = {
                'sustained_performance': {
                    'all_time': 0, 
                    'today': 0,
                    'min_time_today_utc': None,
                    'max_time_today_utc': None
                },
                'huge_jump': {'all_time': 0, 'today': 0},
                'new_top_ranked': {'all_time': 0, 'today': 0}
            }
            
            if batch_helper:
                notification_counts = batch_helper.getNotificationCountsForToken(token.tokenid)
            
            # Convert min/max times to IST for sustained performance (HH:MM format only)
            min_time_ist = OnchainBatchDataHelper.convertUtcToIstTimeOnly(
                notification_counts['sustained_performance']['min_time_today_utc']
            )
            max_time_ist = OnchainBatchDataHelper.convertUtcToIstTimeOnly(
                notification_counts['sustained_performance']['max_time_today_utc']
            )
            
            # Build performance details with price analytics and notification history
            performance_details = {
                "sustained_percentage": round(sustained_percentage, 1),
                "top10_periods": top10_periods,
                "total_periods": total_periods,
                "fluctuation_count": fluctuation_count,
                "duration_hours": round(total_periods * 10 / 60, 1),  # Convert to hours
                "price_analytics": price_analytics,  # Include comprehensive price analytics
                "notification_history": {
                    "sustained_performance_all_time": notification_counts['sustained_performance']['all_time'],
                    "sustained_performance_today": notification_counts['sustained_performance']['today'],
                    "min": min_time_ist,
                    "max": max_time_ist,
                    "huge_jump_all_time": notification_counts['huge_jump']['all_time'],
                    "huge_jump_today": notification_counts['huge_jump']['today'],
                    "new_top_ranked_all_time": notification_counts['new_top_ranked']['all_time']
                }
            }
            
            # Enhanced logging with price and time information
            time_info = ""
            if min_time_ist and max_time_ist:
                time_info = f" Today's sustained alerts: {notification_counts['sustained_performance']['today']} " \
                           f"({min_time_ist} - {max_time_ist} IST)"
            elif notification_counts['sustained_performance']['today'] > 0:
                time_info = f" Today's sustained alerts: {notification_counts['sustained_performance']['today']}"
            
            if price_analytics:
                logger.info(f"Token {token.name} sustained performance: {sustained_percentage:.1f}% over {total_periods} periods. "
                           f"Price: ${price_analytics['starting_price']['price']:.6f} -> ${price_analytics['ending_price']['price']:.6f} "
                           f"({price_analytics['ending_price']['change_from_start_pct']:+.2f}%){time_info}")
            else:
                logger.info(f"Token {token.name} sustained performance: {sustained_percentage:.1f}% over {total_periods} periods{time_info}")
            
            return True, performance_details
            
        return False, {}
