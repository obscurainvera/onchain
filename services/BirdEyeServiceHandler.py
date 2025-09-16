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
from scheduler.SchedulerConstants import CandleDataKeys

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
    
    def getAllCandleDataFromAPI(self, tokenAddress: str, pairAddress: str, fromTime: int, 
                             toTime: int, timeframe: str = '15m') -> Dict:
        """
        UNIFIED CANDLE FETCHING: Strong, clean, readable, and modular
        
        LOGIC:
        1. Get API key with available credits
        2. Hit API and subtract credits locally (no persist)  
        3. Check pagination needs
        4. If more calls needed: check local credits, get new key if needed
        5. Persist all credits at end in single operation
        6. Filter duplicates based on fromTime (like processRawCandles)
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Token pair address
            fromTime: Start timestamp (used to filter out duplicates)
            toTime: End timestamp
            timeframe: Candle timeframe (default: 15m)
            
        Returns:
            Dict: {success, candles, creditsUsed, latestTime, error?}
        """
        try:
            logger.info(f"Unified candle fetch: {tokenAddress} from {fromTime} to {toTime}")
            
            # Initialize fetch state
            birdeyeAPIMeta = self.createEmptyMeta(fromTime)
            
            # get all candles from BirdEye API
            self.getAllCandles(pairAddress, toTime, timeframe, birdeyeAPIMeta)
            
            # Persist all credits used in single database operation
            self.deductCreditsFromCorrespondingAPIKey(birdeyeAPIMeta)
            
            # Process and prepare final results with duplicate filtering
            processedCandlesFromAPI, latestFetchedAtTime = self.processCandlesFromAPI(
                tokenAddress, pairAddress, birdeyeAPIMeta['all_raw_candles'], fromTime
            )
            
            logger.info(f"Unified fetch completed: {len(processedCandlesFromAPI)} candles, "
                       f"{birdeyeAPIMeta['total_credits_used']} credits used")
            
            return {
                'success': True,
                'candles': processedCandlesFromAPI,
                'creditsUsed': birdeyeAPIMeta['total_credits_used'],
                'latestTime': latestFetchedAtTime,         
                'candleCount': len(processedCandlesFromAPI)
            }
            
        except Exception as e:
            logger.error(f"Unified candle fetch failed for {tokenAddress}: {e}")
            return {
                'success': False,
                'error': str(e),
                'candles': [],
                'creditsUsed': 0,
                'candleCount': 0
            }
    
    def createEmptyMeta(self, fromTime: int) -> Dict:
        """Initialize fetch state for unified candle fetching"""
        return {
            'current_from_time': fromTime,
            'latest_time': fromTime,
            'all_raw_candles': [],
            'total_credits_used': 0,
            'current_api_key': None,
            'current_available_credits': 0,
            'api_keys_used': []  # Track all API keys and credits used
        }
    
    def getAllCandles(self, pairAddress: str, toTime: int, timeframe: str, birdeyeAPIMeta: Dict):
        """Execute the unified fetching strategy with smart credit management"""
        # Get initial API key
        if not self.getNewAPIKey(birdeyeAPIMeta):
            raise Exception('No more valid API keys available')
         
        apiCount = 0
        
        while birdeyeAPIMeta['current_from_time'] < toTime:
            # Check if current API key has enough credits
            if not self.hasEnoughCredits(birdeyeAPIMeta):
                # Persist current API key credits and get new one
                self.switchToNewAPIKey(birdeyeAPIMeta)
            
            # Make API call
            apiCount += 1
            logger.info(f"API call #{apiCount} from {birdeyeAPIMeta['current_from_time']} to {toTime}")
            
            result = self.getCandleChunk(
                apiKey=birdeyeAPIMeta['current_api_key']['apikey'],
                pairAddress=pairAddress,
                fromTime=birdeyeAPIMeta['current_from_time'],
                toTime=toTime,
                timeframe=timeframe
            )
            
            if not result:
                logger.warning("API call returned no result, stopping fetch")
                break
            
            # Process API response
            candles = result.get('data', {}).get('items', [])
            if not candles:
                logger.info("No more candles available, stopping fetch")
                break
            
            # Update fetch state
            self.updateBirdeyeMeta(candles, birdeyeAPIMeta)
            
            # Check if more data is needed (pagination logic)
            if not self.needsMoreData(candles, birdeyeAPIMeta, toTime):
                break
            
            # Rate limiting between requests
            time.sleep(1)
        
        logger.info(f"Fetch completed after {apiCount} API calls")
    
    
    def getNewAPIKey(self, fetch_state: Dict) -> bool:
        """Get new API key and store its available credits"""
        api_key_data = self.db.credentials.getNextValidApiKey(
            serviceName=self.service.service_name,
            requiredCredits=self.creditsPerCall
        )
        
        if not api_key_data:
            return False
        
        fetch_state['current_api_key'] = api_key_data
        fetch_state['current_available_credits'] = api_key_data.get('availablecredits', 0)
        
        logger.info(f"Got new API key with {fetch_state['current_available_credits']} available credits")
        return True
    
    def hasEnoughCredits(self, fetch_state: Dict) -> bool:
        """Check if current API key has enough credits for next call"""
        return fetch_state['current_available_credits'] >= self.creditsPerCall
    
    def switchToNewAPIKey(self, fetch_state: Dict):
        """Switch to new API key when current one doesn't have enough credits"""
        # Record current API key usage for later persistence
        if fetch_state['current_api_key']:
            original_credits = fetch_state['current_api_key'].get('availablecredits', 0)
            credits_used = original_credits - fetch_state['current_available_credits']
            
            fetch_state['api_keys_used'].append({
                'api_key_id': fetch_state['current_api_key']['id'],
                'credits_used': credits_used
            })
            
            logger.info(f"API key exhausted, used {credits_used} credits, switching to new key")
        
        # Get new API key
        if not self.getNewAPIKey(fetch_state):
            raise Exception('No more valid API keys available')
    
    def updateBirdeyeMeta(self, candles: List[Dict], fetch_state: Dict):
        """Update fetch state after successful API call"""
        # Add candles to collection
        fetch_state['all_raw_candles'].extend(candles)
        
        # Update credits (locally, not persisted yet)
        fetch_state['current_available_credits'] -= self.creditsPerCall
        fetch_state['total_credits_used'] += self.creditsPerCall
        
        # Update latest time
        latest_candle_time = max(candle.get('unixTime', 0) for candle in candles)
        fetch_state['latest_time'] = max(fetch_state['latest_time'], latest_candle_time)
    
    def needsMoreData(self, candles: List[Dict], fetch_state: Dict, toTime: int) -> bool:
        """Determine if more API calls are needed based on pagination logic"""
        # If we got exactly 1000 candles AND latest time < toTime, there might be more
        if len(candles) == 1000 and fetch_state['latest_time'] < toTime:
            fetch_state['current_from_time'] = fetch_state['latest_time'] + 1
            logger.info(f"Need more data: got 1000 candles, continuing from {fetch_state['current_from_time']}")
            return True
        
        logger.info(f"No more data needed: got {len(candles)} candles, reached end")
        return False
    
    def deductCreditsFromCorrespondingAPIKey(self, fetch_state: Dict):
        """Persist all credits used across all API keys in single database operation"""
        # Add current API key if it was used
        if fetch_state['current_api_key'] and fetch_state['total_credits_used'] > 0:
            original_credits = fetch_state['current_api_key'].get('availablecredits', 0)
            credits_used = original_credits - fetch_state['current_available_credits']
            
            if credits_used > 0:
                fetch_state['api_keys_used'].append({
                    'api_key_id': fetch_state['current_api_key']['id'],
                    'credits_used': credits_used
                })
        
        # Persist all credits in batch
        for key_usage in fetch_state['api_keys_used']:
            self.db.credentials.deductAPIKeyCredits(
                key_usage['api_key_id'], 
                key_usage['credits_used']
            )
            logger.debug(f"Persisted {key_usage['credits_used']} credits for API key {key_usage['api_key_id']}")
    
    def processCandlesFromAPI(self, tokenAddress: str, pairAddress: str, 
                                            candlesFromAPI: List[Dict], fromTime: int) -> tuple[List[Dict], int]:
        """
        Process raw candles with duplicate filtering (like processRawCandles)
        
        SCHEDULER FLOW COMPATIBILITY:
        - Filters duplicates based on fromTime (like the original processRawCandles)
        - Skips invalid timestamps
        - Adds token metadata for database insertion
        - Returns latest processed time (like processRawCandles)
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Token pair address  
            raw_candles: Raw candle data from BirdEye API
            fromTime: Last processed unix time to avoid duplicates
            
        Returns:
            Tuple[List[Dict], int]: (processed_candles, latest_time)
        """
        processedCandlesFromAPI = []
        latestFetchedAtTime = fromTime
        
        for candle in candlesFromAPI:
            try:
                unixTime = candle.get('unixTime')
                if not unixTime or unixTime <= fromTime:
                    continue  # Skip duplicates and invalid times (same logic as processRawCandles)
                
                processed_candle = {
                    'tokenaddress': tokenAddress,
                    'pairaddress': pairAddress,
                    'unixtime': unixTime,
                    'openprice': float(candle.get('o', 0)),
                    'highprice': float(candle.get('h', 0)),
                    'lowprice': float(candle.get('l', 0)),
                    'closeprice': float(candle.get('c', 0)),
                    'volume': float(candle.get('v', 0)),
                    'timeframe': '15m',
                    'datasource': 'birdeye'
                }
                processedCandlesFromAPI.append(processed_candle)
                
                # Track latest time during processing (like processRawCandles)
                if unixTime > latestFetchedAtTime:
                    latestFetchedAtTime = unixTime
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid candle data skipped: {e}")
                continue
        
        return processedCandlesFromAPI, latestFetchedAtTime
    
    
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
                f"{self.baseUrl}/defi/ohlcv/pair",
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
    
    
    def getCandleDataForToken(self, tokenAddress: str, pairAddress: str, 
                             fromTime: int, toTime: int, symbol: str = '') -> Dict:
        """
        Get processed candle data for a specific token using UNIFIED approach
        
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
            # Use unified candle fetching approach
            result = self.getAllCandleDataFromAPI(tokenAddress, pairAddress, fromTime, toTime)
            
            if not result['success']:
                return {
                    'success': False,
                    'error': result.get('error', 'No candle data received from API'),
                    CandleDataKeys.CANDLES: [],
                    CandleDataKeys.COUNT: 0
                }
            
            # Processed candles are ready for use
            processedCandles = result['candles']
            
            return {
                'success': True,
                CandleDataKeys.CANDLES: processedCandles,
                CandleDataKeys.LATEST_TIME: result['latestTime'],
                CandleDataKeys.COUNT: result['candleCount']
            }
            
        except Exception as e:
            logger.error(f"Error getting candle data for token {symbol}: {e}")
            
            return {
                'success': False,
                'error': str(e),
                CandleDataKeys.CANDLES: [],
                CandleDataKeys.COUNT: 0
            }