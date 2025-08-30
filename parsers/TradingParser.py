from typing import List, Optional, Dict, Any
from database.trading.TradingModels import BirdEyeOHLCVItem, BirdEyeOHLCVResponse, OHLCVCandle
from decimal import Decimal
from logs.logger import get_logger

logger = get_logger(__name__)

class TradingParser:
    """Parser for crypto trading API responses"""
    
    @staticmethod
    def parseBirdEyeOHLCVResponse(response_data: Dict[str, Any]) -> List[BirdEyeOHLCVItem]:
        """
        Parse BirdEye OHLCV API response
        
        Args:
            response_data: Raw response from BirdEye API
            
        Returns:
            List[BirdEyeOHLCVItem]: Parsed OHLCV items
        """
        try:
            if not response_data.get("success", False):
                logger.error(f"BirdEye API returned unsuccessful response: {response_data}")
                return []
            
            items = response_data.get("data", {}).get("items", [])
            
            if not items:
                logger.warning("No OHLCV items found in BirdEye response")
                return []
            
            parsed_items = []
            for item in items:
                try:
                    # Validate required fields
                    required_fields = ["address", "c", "h", "l", "o", "unixTime", "v"]
                    if not all(field in item for field in required_fields):
                        logger.warning(f"Skipping item with missing fields: {item}")
                        continue
                    
                    # Create BirdEyeOHLCVItem
                    ohlcv_item = BirdEyeOHLCVItem(
                        address=item["address"],
                        c=float(item["c"]),      # close
                        h=float(item["h"]),      # high
                        l=float(item["l"]),      # low
                        o=float(item["o"]),      # open
                        type=item.get("type", "15m"),
                        unixTime=int(item["unixTime"]),
                        v=float(item["v"])       # volume
                    )
                    
                    # Basic validation
                    if ohlcv_item.h < ohlcv_item.l:
                        logger.warning(f"Invalid OHLCV data: high ({ohlcv_item.h}) < low ({ohlcv_item.l})")
                        continue
                    
                    if ohlcv_item.h < ohlcv_item.o or ohlcv_item.h < ohlcv_item.c:
                        logger.warning(f"Invalid OHLCV data: high ({ohlcv_item.h}) < open/close")
                        continue
                    
                    if ohlcv_item.l > ohlcv_item.o or ohlcv_item.l > ohlcv_item.c:
                        logger.warning(f"Invalid OHLCV data: low ({ohlcv_item.l}) > open/close")
                        continue
                    
                    if ohlcv_item.v < 0:
                        logger.warning(f"Invalid OHLCV data: negative volume ({ohlcv_item.v})")
                        continue
                    
                    parsed_items.append(ohlcv_item)
                    
                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing OHLCV item {item}: {e}")
                    continue
            
            logger.info(f"Successfully parsed {len(parsed_items)} OHLCV items from BirdEye response")
            return parsed_items
            
        except Exception as e:
            logger.error(f"Error parsing BirdEye OHLCV response: {e}")
            return []
    
    @staticmethod
    def convertBirdEyeToOHLCVCandle(birdeye_item: BirdEyeOHLCVItem, 
                                   token_address: str, 
                                   pair_address: str,
                                   data_source: str = "api") -> OHLCVCandle:
        """
        Convert BirdEye OHLCV item to OHLCVCandle model
        
        Args:
            birdeye_item: BirdEye OHLCV data item
            token_address: Token contract address
            pair_address: DEX pair address
            data_source: Source of data ("api" or "aggregated")
            
        Returns:
            OHLCVCandle: Converted candle object
        """
        try:
            return OHLCVCandle(
                tokenaddress=token_address,
                pairaddress=pair_address,
                timeframe=birdeye_item.type,
                unixtime=birdeye_item.unixTime,
                openprice=Decimal(str(birdeye_item.o)),
                highprice=Decimal(str(birdeye_item.h)),
                lowprice=Decimal(str(birdeye_item.l)),
                closeprice=Decimal(str(birdeye_item.c)),
                volume=Decimal(str(birdeye_item.v)),
                datasource=data_source
            )
        except Exception as e:
            logger.error(f"Error converting BirdEye item to OHLCVCandle: {e}")
            return None
    
    @staticmethod
    def batchConvertBirdEyeToOHLCVCandles(birdeye_items: List[BirdEyeOHLCVItem],
                                         token_address: str,
                                         pair_address: str,
                                         data_source: str = "api") -> List[OHLCVCandle]:
        """
        Batch convert BirdEye OHLCV items to OHLCVCandle models
        
        Args:
            birdeye_items: List of BirdEye OHLCV data items
            token_address: Token contract address
            pair_address: DEX pair address
            data_source: Source of data ("api" or "aggregated")
            
        Returns:
            List[OHLCVCandle]: List of converted candle objects
        """
        candles = []
        
        for item in birdeye_items:
            candle = TradingParser.convertBirdEyeToOHLCVCandle(
                birdeye_item=item,
                token_address=token_address,
                pair_address=pair_address,
                data_source=data_source
            )
            
            if candle:
                candles.append(candle)
        
        logger.info(f"Batch converted {len(candles)} BirdEye items to OHLCVCandles")
        return candles
    
    @staticmethod
    def validateOHLCVData(candle: OHLCVCandle) -> tuple[bool, Optional[str]]:
        """
        Validate OHLCV candle data
        
        Args:
            candle: OHLCV candle to validate
            
        Returns:
            tuple[bool, Optional[str]]: (is_valid, error_message)
        """
        try:
            # Check price relationships
            if candle.highprice < candle.lowprice:
                return False, f"High price ({candle.highprice}) cannot be less than low price ({candle.lowprice})"
            
            if candle.highprice < candle.openprice:
                return False, f"High price ({candle.highprice}) cannot be less than open price ({candle.openprice})"
            
            if candle.highprice < candle.closeprice:
                return False, f"High price ({candle.highprice}) cannot be less than close price ({candle.closeprice})"
            
            if candle.lowprice > candle.openprice:
                return False, f"Low price ({candle.lowprice}) cannot be greater than open price ({candle.openprice})"
            
            if candle.lowprice > candle.closeprice:
                return False, f"Low price ({candle.lowprice}) cannot be greater than close price ({candle.closeprice})"
            
            # Check for negative values
            if candle.openprice <= 0:
                return False, f"Open price must be positive, got {candle.openprice}"
            
            if candle.highprice <= 0:
                return False, f"High price must be positive, got {candle.highprice}"
            
            if candle.lowprice <= 0:
                return False, f"Low price must be positive, got {candle.lowprice}"
            
            if candle.closeprice <= 0:
                return False, f"Close price must be positive, got {candle.closeprice}"
            
            if candle.volume < 0:
                return False, f"Volume cannot be negative, got {candle.volume}"
            
            # Check timeframe validity
            valid_timeframes = ["15m", "1h", "4h"]
            if candle.timeframe not in valid_timeframes:
                return False, f"Invalid timeframe {candle.timeframe}, must be one of {valid_timeframes}"
            
            # Check unix timestamp (should be reasonable)
            if candle.unixtime < 1640995200:  # 2022-01-01
                return False, f"Unix timestamp {candle.unixtime} is too old"
            
            if candle.unixtime > 2147483647:  # 2038-01-19 (32-bit timestamp limit)
                return False, f"Unix timestamp {candle.unixtime} is too large"
            
            # Check address formats
            if len(candle.tokenaddress) != 44:
                return False, f"Token address must be 44 characters, got {len(candle.tokenaddress)}"
            
            if len(candle.pairaddress) != 44:
                return False, f"Pair address must be 44 characters, got {len(candle.pairaddress)}"
            
            return True, None
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    @staticmethod
    def aggregateCandles(candles: List[OHLCVCandle], target_timeframe: str) -> Optional[OHLCVCandle]:
        """
        Aggregate multiple candles into a single candle for higher timeframe
        
        Args:
            candles: List of candles to aggregate (must be sorted by time)
            target_timeframe: Target timeframe ("1h" or "4h")
            
        Returns:
            OHLCVCandle: Aggregated candle or None if aggregation fails
        """
        try:
            if not candles:
                logger.warning("No candles provided for aggregation")
                return None
            
            # Sort candles by time to ensure correct order
            sorted_candles = sorted(candles, key=lambda c: c.unixtime)
            
            # Validate all candles are complete
            for candle in sorted_candles:
                if not candle.iscomplete:
                    logger.warning(f"Cannot aggregate incomplete candle at {candle.unixtime}")
                    return None
            
            # Calculate aggregated values
            first_candle = sorted_candles[0]
            last_candle = sorted_candles[-1]
            
            aggregated_candle = OHLCVCandle(
                tokenaddress=first_candle.tokenaddress,
                pairaddress=first_candle.pairaddress,
                timeframe=target_timeframe,
                unixtime=first_candle.unixtime,  # Start time of period
                openprice=first_candle.openprice,
                highprice=max(c.highprice for c in sorted_candles),
                lowprice=min(c.lowprice for c in sorted_candles),
                closeprice=last_candle.closeprice,
                volume=sum(c.volume for c in sorted_candles),
                datasource="aggregated",
                iscomplete=True
            )
            
            # Validate aggregated candle
            is_valid, error_msg = TradingParser.validateOHLCVData(aggregated_candle)
            if not is_valid:
                logger.error(f"Aggregated candle validation failed: {error_msg}")
                return None
            
            logger.info(f"Successfully aggregated {len(sorted_candles)} candles to {target_timeframe}")
            return aggregated_candle
            
        except Exception as e:
            logger.error(f"Error aggregating candles: {e}")
            return None
    
    @staticmethod
    def filterDuplicateCandles(candles: List[OHLCVCandle]) -> List[OHLCVCandle]:
        """
        Filter duplicate candles based on token, timeframe, and unix time
        
        Args:
            candles: List of candles to filter
            
        Returns:
            List[OHLCVCandle]: Filtered list with duplicates removed
        """
        seen = set()
        filtered_candles = []
        
        for candle in candles:
            # Create unique identifier
            candle_id = f"{candle.tokenaddress}_{candle.timeframe}_{candle.unixtime}"
            
            if candle_id not in seen:
                seen.add(candle_id)
                filtered_candles.append(candle)
            else:
                logger.debug(f"Filtered duplicate candle: {candle_id}")
        
        if len(filtered_candles) != len(candles):
            logger.info(f"Filtered {len(candles) - len(filtered_candles)} duplicate candles")
        
        return filtered_candles