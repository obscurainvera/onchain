from config.Config import get_config
"""
Notification content models for structured messages
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from decimal import Decimal
from datetime import datetime
from framework.notificationframework.NotificationEnums import NotificationStrategyType

@dataclass
class TokenNotificationContent:
    """
    Structured content for token notifications
    """
    """
    Structured content for token notifications, aligned with OnchainInfo plus DexScreener URL
    """
    # Required fields from OnchainInfo
    subject: str  # Notification subject
    tokenid: str
    name: str
    chain: str
    price: Decimal
    marketcap: Decimal
    liquidity: Decimal
    makers: int
    rank: int
    
    # Optional fields from OnchainInfo
    id: Optional[int] = None
    onchaininfoid: Optional[int] = None
    age: Optional[str] = None
    count: int = 1
    createdat: Optional[datetime] = None
    updatedat: Optional[datetime] = None
    
    # DexScreener URL from original TokenNotificationContent
    dexScreenerUrl: Optional[str] = None
    
    # Additional strategy-specific information
    otherinfo: Optional[dict] = None
    
    
    def formatTelegramMessageForOnchain(self) -> str:
        """
        Format the content as a Telegram message with HTML formatting
        Uses strategy-specific formatting based on the subject and otherinfo
        
        Returns:
            str: Formatted message for Telegram
        """
        # Determine strategy type from subject
        strategy_type = self.subject
        
        # Use strategy-specific formatter
        if strategy_type == NotificationStrategyType.NEW_TOP_RANKED.value:
            return self._formatNewTopRankedMessage()
        elif strategy_type == NotificationStrategyType.FLUCTUATION.value:
            return self._formatFluctuationMessage()
        elif strategy_type == NotificationStrategyType.HUGE_JUMP.value:
            return self._formatHugeJumpMessage()
        elif strategy_type == NotificationStrategyType.SUSTAINED_PERFORMANCE.value:
            return self._formatSustainedPerformanceMessage()
        else:
            return self._formatDefaultMessage()
    
    def _formatNewTopRankedMessage(self) -> str:
        """Format message for new top ranked tokens"""
        message = [
            "<b>🚀 NEW TOKEN ALERT - TOP RANKED</b>\n\n",
            f" • <b>Alert:</b> {self.subject}\n",
            f" • <b>Contract:</b> <code>{self.tokenid}</code>\n\n"
        ]
        
        if self.name:
            message.append(f" • <b>Name:</b> <u><b><i>{self.name}</i></b></u>\n")
        
        message.extend([
            f" • <b>Rank:</b> #{self.rank}\n",
            f" • <b>Price:</b> ${self.price:,.6f}\n",
            f" • <b>Market Cap:</b> {self._format_number(self.marketcap)}\n",
            f" • <b>Liquidity:</b> {self._format_number(self.liquidity)}\n",
            f" • <b>Makers:</b> {self.makers:,}\n"
        ])
        
        if self.age:
            message.append(f" • <b>Age:</b> {self.age}\n")
            
        return self._addOtherInfoAndFinalize(message)
    
    def _formatFluctuationMessage(self) -> str:
        """Format message for fluctuation alerts"""
        message = [
            "<b>🔄 FLUCTUATION ALERT</b>\n\n",
            f" • <b>Alert:</b> {self.subject}\n",
            f" • <b>Contract:</b> <code>{self.tokenid}</code>\n"
        ]
        
        if self.name:
            message.append(f" • <b>Name:</b> <u><b><i>{self.name}</i></b></u>\n")
        
        message.extend([
            f" • <b>Current Rank:</b> #{self.rank}\n",
            f" • <b>Price:</b> ${self.price:,.6f}\n",
            f" • <b>Market Cap:</b> {self._format_number(self.marketcap)}\n",
            f" • <b>Liquidity:</b> {self._format_number(self.liquidity)}\n"
        ])
        
        return self._addOtherInfoAndFinalize(message)
    
    def _formatHugeJumpMessage(self) -> str:
        """Format message for huge jump alerts"""
        message = [
            "<b>⚡ HUGE JUMP ALERT</b>\n\n",
            f" • <b>Alert:</b> {self.subject}\n",
            f" • <b>Contract:</b> <code>{self.tokenid}</code>\n"
        ]
        
        if self.name:
            message.append(f"🏷 <b>Name:</b> <u><b><i>{self.name}</i></b></u>\n")
        
        message.extend([
            f" • <b>Current Rank:</b> #{self.rank}\n",
            f" • <b>Price:</b> ${self.price:,.6f}\n",
            f" • <b>Market Cap:</b> {self._format_number(self.marketcap)}\n",
            f" • <b>Liquidity:</b> {self._format_number(self.liquidity)}\n"
        ])
        
        return self._addOtherInfoAndFinalize(message)
    
    def _formatSustainedPerformanceMessage(self) -> str:
        """Format message for sustained performance alerts with comprehensive price analytics"""
        message = [
            "<b>🎯 SUSTAINED PERFORMANCE ALERT</b>\n\n",
            f" • <b>Alert:</b> {self.subject}\n",
            f" • <b>Contract:</b> <code>{self.tokenid}</code>\n"
        ]
        
        if self.name:
            message.append(f"🏷 <b>Name:</b> <u><b><i>{self.name}</i></b></u>\n")
        
        message.extend([
            f" • <b>Current Rank:</b> #{self.rank}\n",
            f" • <b>Current Price:</b> ${self.price:,.6f}\n",
            f" • <b>Market Cap:</b> {self._format_number(self.marketcap)}\n",
            f" • <b>Liquidity:</b> {self._format_number(self.liquidity)}\n"
        ])
        
        # Add price analytics if available in otherinfo
        if self.otherinfo and 'price_analytics' in self.otherinfo:
            price_analytics = self.otherinfo['price_analytics']
            if price_analytics:
                message.append("\n<b>3-Hour Price Analytics:</b>\n")
                
                # Starting price
                starting = price_analytics['starting_price']
                message.append(f"<b>Start:</b> ${starting['price']:.6f} <i>({starting['formatted_time']})</i>\n")
                
                # Ending price with change
                ending = price_analytics['ending_price']
                change_pct = ending['change_from_start_pct']
                change_emoji = "🟢" if change_pct >= 0 else "🔴"
                message.append(f"<b>End:</b> ${ending['price']:.6f} <b> = </b> <b>{change_pct:+.2f}%</b> {change_emoji} \n<i>({ending['formatted_time']})</i><b> Start -> End </b>\n")
                
                # Highest price
                highest = price_analytics['highest_price']
                high_change_pct = highest['change_from_start_pct']
                change_emoji = "🟢" if high_change_pct >= 0 else "🔴"
                message.append(f" <b>High:</b> ${highest['price']:.6f} <b> = </b> <b>{high_change_pct:+.2f}%</b> {change_emoji} \n<i>({highest['formatted_time']})</i><b> Start -> High </b>\n")
                
                # Lowest price
                lowest = price_analytics['lowest_price']
                low_change_pct = lowest['change_from_start_pct']
                low_to_high_pct = lowest['change_to_high_pct']
                change_emoji = "🟢" if low_change_pct >= 0 else "🔴"
                message.append(f" <b>Low:</b> ${lowest['price']:.6f} <b> = </b> <b>{low_change_pct:+.2f}%</b> {change_emoji} \n<i>({lowest['formatted_time']})</i><b> Start -> Low </b>\n")

                message.append(f"⚡ <b>Low→High:</b> <b>{low_to_high_pct:+.2f}%</b>\n")
                
                # Volatility summary
                volatility = price_analytics['analytics_metadata']['price_volatility']
                message.append(f"<b>3H Volatility:</b> <b>{volatility:.2f}%</b>\n")
        
        return self._addOtherInfoAndFinalize(message)
    
    def _formatDefaultMessage(self) -> str:
        """Format default message for unknown strategy types"""
        message = [
            "<b>📢 ONCHAIN ALERT</b>\n\n",
            f" • <b>Subject:</b> {self.subject}\n",
            f" • <b>Contract:</b> <code>{self.tokenid}</code>\n"
        ]
        
        if self.name:
            message.append(f" • <b>Name:</b> <u><b><i>{self.name}</i></b></u>\n")
        
        message.extend([
            f" • <b>Rank:</b> #{self.rank}\n",
            f" • <b>Price:</b> ${self.price:,.6f}\n",
            f" • <b>Market Cap:</b> {self._format_number(self.marketcap)}\n",
            f" • <b>Liquidity:</b> {self._format_number(self.liquidity)}\n",
            f" • <b>Makers:</b> {self.makers:,}\n"
        ])
        
        if self.age:
            message.append(f"⏳ <b>Age:</b> {self.age}\n")
            
        return self._addOtherInfoAndFinalize(message)
    
    def _format_number(self, value) -> str:
        """
        Format large numbers into K/M/B notation
        
        Args:
            value: Numeric value (int or float)
            
        Returns:
            str: Formatted number (e.g., 1000 -> 1K, 1000000 -> 1M)
        """
        try:
            value = float(value)
            if value >= 1_000_000_000:
                return f"${value / 1_000_000_000:.1f}B"
            elif value >= 1_000_000:
                return f"${value / 1_000_000:.1f}M"
            elif value >= 1_000:
                return f"${value / 1_000:.1f}K"
            return f"${value:,.2f}"
        except (ValueError, TypeError):
            return str(value)
    
    def _addOtherInfoAndFinalize(self, message: List[str]) -> str:
        """
        Add otherinfo details and finalize the message
        
        Args:
            message: List of message parts
            
        Returns:
            str: Finalized message string
        """
        # Add all otherinfo details if present (excluding price_analytics as it's handled separately)
        if self.otherinfo:
            # Filter out price_analytics since it's handled in strategy-specific formatters
            filtered_info = {k: v for k, v in self.otherinfo.items() if k != 'price_analytics'}
            
            if filtered_info:
                message.append("\n")
                for key, value in filtered_info.items():
                    if key == 'notification_history':
                        message.append(f"  • <b>Notification History:</b>\n")
                        if isinstance(value, dict):
                            if 'sustained_performance_all_time' in value:
                                message.append(f"    ◦ Sustained Performance: {value.get('sustained_performance_today', 0)} today, {value.get('sustained_performance_all_time', 0)} all-time\n")
                            if 'huge_jump_all_time' in value:
                                message.append(f"    ◦ Huge Jump: {value.get('huge_jump_today', 0)} today, {value.get('huge_jump_all_time', 0)} all-time\n")
                            if 'new_top_ranked_all_time' in value:
                                message.append(f"    ◦ New Top Ranked: {value.get('new_top_ranked_all_time', 0)} all-time\n")
                            if 'min' in value and 'max' in value:
                                message.append(f"    ◦ Time Range: {value.get('min', 'N/A')} - {value.get('max', 'N/A')}\n")
                        else:
                            message.append(f"    {value}\n")
                    else:
                        formatted_key = key.replace('_', ' ').title()
                        message.append(f"  • <b>{formatted_key}:</b> {value}\n")
        
        return "".join(message)
        
    def getDefaultButtons(self) -> List[dict]:
        """
        Get default buttons for the notification
        
        Returns:
            List[dict]: List of button configurations
        """
        buttons = []
        
        # Add DexScreener button if URL is available
        if self.dexScreenerUrl:
            buttons.append({
                "text": f"DS = {self.name}",
                "url": self.dexScreenerUrl
            })
        else:
            # Use a default URL if none is provided
            buttons.append({
                "text": f"DS = {self.name}",
                "url": f"https://dexscreener.com/solana/{self.tokenid}"
            })
            
        buttons.append({
            "text": f"CE = {self.name}",
            "url": f"https://app.chainedge.io/solana/?search={self.tokenid}"
        })
        
        return buttons 
    
       