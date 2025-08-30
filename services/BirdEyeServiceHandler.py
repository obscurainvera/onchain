"""
Handles all BirdEye API related operations including pagination and candle data processing
"""
from typing import Dict, List, Optional, Tuple
import requests
import time
from decimal import Decimal
from logs.logger import get_logger
from database.operations.PortfolioDB import PortfolioDB
from database.auth.ServiceCredentialsEnum import ServiceCredentials

logger = get_logger(__name__)

class BirdEyeServiceHandler:
    """Service handler for BirdEye API operations with proper credit management"""
    
    def __init__(self, db: PortfolioDB):
        """Initialize with database connection and service configuration"""
        self.db = db
        self.service = ServiceCredentials.BIRDEYE
        self.baseUrl = self.service.metadata['base_url']
        self.creditsPerCall = self.service.metadata.get('credits_per_call', 40)
        self.session = requests.Session()
    
    def getCandleData(self, pairAddress: str, fromTime: int, toTime: int, 
                     timeframe: str = '15m') -> Optional[List[Dict]]:
        """
        Get all candle data for a token pair within time range
        Uses pagination to handle API limits on large time ranges
        
        Args:
            pairAddress: Token pair address
            fromTime: Start timestamp
            toTime: End timestamp  
            timeframe: Candle timeframe (default: 15m)
            
        Returns:
            Optional[List[Dict]]: List of all candles or None if failed
        """
        try:
            all_candles = []
            current_from_time = fromTime
            api_count = 0
            
            # Fetch candles with pagination based on 1000 candle limit
            while current_from_time < toTime:
                # Get API key with sufficient credits
                api_key_data = self.db.credentials.getNextValidApiKey(
                    serviceName=self.service.service_name,
                    requiredCredits=self.creditsPerCall
                )
                if not api_key_data:
                    logger.error("No valid BirdEye API key available")
                    return None if not all_candles else all_candles
                
                logger.info(f"Fetching candles from BirdEye API for pair {pairAddress} (batch {api_count + 1})")
                result = self.getCandleChunk(
                    apiKey=api_key_data['apikey'],
                    pairAddress=pairAddress,
                    fromTime=current_from_time,
                    toTime=toTime,
                    timeframe=timeframe
                )
                api_count += 1
                
                if not result:
                    break
                
                # Unpack response
                candles = result.get('data', {}).get('items', [])
                
                # Update API key credits
                self.db.credentials.deductAPIKeyCredits(api_key_data['id'], self.creditsPerCall)
                
                # Add candles to list and track latest time
                if candles:
                    latest_time = current_from_time
                    for candle in candles:
                        all_candles.append(candle)
                        candle_time = candle.get('unixTime', 0)
                        if candle_time > latest_time:
                            latest_time = candle_time
                    
                    # Check if we need more data based on BirdEye's 1000 candle limit
                    # If latest candle time < toTime AND we got exactly 1000 candles, there might be more
                    if latest_time < toTime and len(candles) == 1000:
                        current_from_time = latest_time + 1
                        logger.info(f"Got 1000 candles, fetching more from {current_from_time}")
                    else:
                        # Either we reached toTime or got less than 1000 candles (no more data)
                        break
                else:
                    # No candles returned, break the loop
                    break
                
                # Rate limiting between requests
                time.sleep(1)
            
            logger.info(f"Retrieved {len(all_candles)} total candles for pair {pairAddress} in {api_count} API calls")
            return all_candles
            
        except Exception as e:
            logger.error(f"Failed to get candle data for {pairAddress}: {e}")
            return None
    
    def getCandleChunk(self, apiKey: str, pairAddress: str, fromTime: int, 
                      toTime: int, timeframe: str = '15m') -> Optional[Dict]:
        """
        Get single chunk of candle data from BirdEye API
        
        Args:
            apiKey: Valid BirdEye API key
            pairAddress: Token pair address
            fromTime: Start timestamp
            toTime: End timestamp
            timeframe: Candle timeframe
            
        Returns:
            Optional[Dict]: Raw API response or None if failed
        """
        params = {
            'address': pairAddress,
            'type': timeframe,
            'time_from': fromTime,
            'time_to': toTime
        }
        
        headers = {
            'X-API-KEY': apiKey,
            'accept': 'application/json'
        }
        
        try:
            response = self.session.get(
                f"{self.baseUrl}/ohlcv",
                headers=headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get('success'):
                return data
            else:
                logger.error(f"BirdEye API error response: {data}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"BirdEye API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in BirdEye API call: {e}")
            return None
    
    def processRawCandles(self, raw_candles: List[Dict], last_unix_time: int = 0) -> List[Dict]:
        """
        Process raw API response into clean candle format
        
        Args:
            raw_candles: Raw candle data from API
            last_unix_time: Last processed unix time to avoid duplicates
            
        Returns:
            List[Dict]: Processed candle data
        """
        processed = []
        
        for candle_data in raw_candles:
            try:
                unix_time = candle_data.get('unixTime')
                if not unix_time or unix_time <= last_unix_time:
                    continue  # Skip duplicates and invalid times
                
                candle = {
                    'unixtime': unix_time,
                    'openprice': float(candle_data.get('o', 0)),
                    'highprice': float(candle_data.get('h', 0)),
                    'lowprice': float(candle_data.get('l', 0)),
                    'closeprice': float(candle_data.get('c', 0)),
                    'volume': float(candle_data.get('v', 0)),
                    'timeframe': '15m',
                    'datasource': 'birdeye'
                }
                processed.append(candle)
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid candle data: {e}")
                continue
        
        return processed
    
    def getCandleDataForToken(self, tokenAddress: str, pairAddress: str, 
                             fromTime: int, toTime: int, symbol: str = '') -> Dict:
        """
        Get processed candle data for a specific token
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Token pair address  
            fromTime: Start timestamp
            toTime: End timestamp
            symbol: Token symbol for logging
            
        Returns:
            Dict: Result with success status, candles, and metadata
        """
        try:
            # Get raw candle data
            raw_candles = self.getCandleData(pairAddress, fromTime, toTime)
            
            if not raw_candles:
                return {
                    'success': False,
                    'error': 'No candle data received from API',
                    'candles': [],
                    'count': 0
                }
            
            # Process raw candles
            processed_candles = self.processRawCandles(raw_candles, fromTime - 1) # because we add 1 second to the last fetch time
            
            # Add token metadata to each candle
            for candle in processed_candles:
                candle.update({
                    'tokenaddress': tokenAddress,
                    'pairaddress': pairAddress,
                    'symbol': symbol
                })
            
            # Calculate latest time
            latest_time = max(candle['unixtime'] for candle in processed_candles) if processed_candles else fromTime
            
            return {
                'success': True,
                'candles': processed_candles,
                'latest_time': latest_time,
                'count': len(processed_candles)
            }
            
        except Exception as e:
            logger.error(f"Error getting candle data for token {symbol}: {e}")
            return {
                'success': False,
                'error': str(e),
                'candles': [],
                'count': 0
            }