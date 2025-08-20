from config.Config import get_config

"""Parsers for trading attention data"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from decimal import Decimal, InvalidOperation
import re
from logs.logger import get_logger
from datetime import datetime
from database.operations.schema import TradingAttentionInfo
from actions.DexscrennerAction import DexScreenerAction, TokenPrice
import pytz

logger = get_logger(__name__)


def parseTradingAttentionResponse(response: Dict) -> List[TradingAttentionInfo]:
    """
    Parse response from trading attention API and convert to TradingAttentionInfo objects
    Use DexscreennerAction to get current price data in batches
    
    Args:
        response: API response dictionary
        
    Returns:
        List[TradingAttentionInfo]: List of parsed TradingAttentionInfo objects
    """
    try:
        items = response.get("data", [])
        if not items:
            logger.warning("No items found in trading attention response")
            return []

        logger.info(f"Found {len(items)} trading attention items")
        
        # Initialize DexScreener service
        dexScreener = DexScreenerAction()
        
        # Get all token IDs for batch processing
        token_ids = [item.get("token_id") for item in items if item.get("token_id")]
        
        # Get price data for all tokens in batches
        logger.info(f"Fetching price data for {len(token_ids)} tokens from DexScreener")
        price_data_map = dexScreener.getBatchTokenPrices(token_ids)
        logger.info(f"Successfully fetched price data for {sum(1 for v in price_data_map.values() if v is not None)} tokens")
        
        result = []
        for item in items:
            try:
                # Extract required fields
                tokenId = item.get("token_id")
                if not tokenId:
                    logger.warning("Skipping item without token_id")
                    continue

                # Convert to IST timezone
                ist = pytz.timezone('Asia/Kolkata')
                now = datetime.now(ist)
                
                # Get price data from DexScreener
                price_data = price_data_map.get(tokenId)
                current_price = Decimal(str(price_data.price)) if price_data else None
                
                tradingAttentionInfo = TradingAttentionInfo(
                    tokenid=tokenId,
                    name=item.get("token_symbol", ""),
                    score=_parseDecimal(item.get("att_score_percentage", "0")),
                    date=item.get("date", ""),
                    colour=item.get("color", ""),
                    currentprice=current_price,
                    createdat=now,
                    lastupdatedat=now,
                )

                result.append(tradingAttentionInfo)
                logger.info(
                    f"Successfully parsed trading attention token {tokenId} - {item.get('token_symbol')} with score {tradingAttentionInfo.score}, using DexScreener data: {price_data is not None}"
                )

            except Exception as e:
                logger.error(f"Failed to parse trading attention item: {str(e)}, item: {item}")
                continue

        logger.info(f"Successfully parsed {len(result)} trading attention tokens")
        return result

    except Exception as e:
        logger.error(f"Failed to parse trading attention response: {str(e)}")
        return []


def _parseDecimal(value: Any) -> Decimal:
    """Parse string/number to Decimal, handling None and empty strings"""
    if value is None or value == "null" or value == "undefined" or value == "":
        return Decimal("0")
    
    try:
        # Handle string values with commas
        if isinstance(value, str):
            value = value.replace(",", "")
        return Decimal(str(value))
    except (ValueError, InvalidOperation):
        logger.warning(f"Could not parse decimal value: {value}")
        return Decimal("0")


def _parseDatetime(value: str) -> Optional[datetime]:
    """Parse datetime string to datetime object in IST timezone"""
    if not value or value == "null" or value == "undefined":
        return None
    try:
        # Parse the datetime and convert to IST
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        ist = pytz.timezone('Asia/Kolkata')
        return dt.astimezone(ist)
    except (ValueError, TypeError):
        return None 