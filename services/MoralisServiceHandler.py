"""
Handles all Moralis API related operations including pagination, deduplication and candle data processing
Addresses complex edge cases: reverse chronological order, inclusive/exclusive boundaries, incomplete candles
"""
from typing import Dict, List, Optional, Set, Tuple
import requests
import time
from datetime import datetime, timezone
from logs.logger import get_logger
from database.operations.PortfolioDB import PortfolioDB
from database.auth.ServiceCredentialsEnum import ServiceCredentials
from scheduler.SchedulerConstants import CandleDataKeys

logger = get_logger(__name__)

class MoralisFetchStateKeys:
    """Constants for fetch state dictionary keys to ensure consistency and reduce errors"""
    CHAIN = 'chain'
    CURRENT_FROM_TIME = 'current_from_time'
    CURRENT_TO_TIME = 'current_to_time'
    LATEST_TIME = 'latest_time'
    ALL_RAW_CANDLES = 'all_raw_candles'
    PROCESSED_TIMESTAMPS = 'processed_timestamps'
    TOTAL_CREDITS_USED = 'total_credits_used'
    CURRENT_API_KEY = 'current_api_key'
    CURRENT_AVAILABLE_CREDITS = 'current_available_credits'
    API_KEYS_USED = 'api_keys_used'

class MoralisAPIConstants:
    """Constants for Moralis API configuration and mappings"""
    MAX_RECORDS_PER_CALL = 1000
    DEFAULT_CURRENCY = 'usd'
    MILLISECONDS_MULTIPLIER = 1000
    RATE_LIMIT_DELAY_SECONDS = 1
    
    # Timeframe to seconds mapping
    TIMEFRAME_SECONDS = {
        '1s': 1, '10s': 10, '30s': 30,
        '1min': 60, '5min': 300, '10min': 600, '30min': 1800,
        '1h': 3600, '4h': 14400, '12h': 43200,
        '1d': 86400, '1w': 604800, '1M': 2592000
    }
    
    # Internal timeframe mapping
    TIMEFRAME_MAPPING = {
        '30min': '30min',
        '1h': '1h', 
        '4h': '4h'
    }

class MoralisServiceHandler:
    """Service handler for Moralis API operations with advanced pagination and deduplication"""
    
    def __init__(self, db: PortfolioDB):
        """Initialize with database connection and service configuration"""
        self.db = db
        self.service = ServiceCredentials.MORALIS
        self.baseUrl = self.service.metadata['base_url']
        self.creditsPerCall = self.service.metadata.get('credits_per_call', 150)
        self.defaultChain = self.service.metadata.get('default_chain', 'mainnet')
        self.supportedTimeframes = self.service.metadata.get('supported_timeframes', [])
        self.session = requests.Session()
    
    def getAllCandleDataFromAPI(self, tokenAddress: str, pairAddress: str, fromTime: int, 
                              toTime: int, timeframe: str) -> Dict:
        """
        UNIFIED CANDLE FETCHING: Handles all Moralis API edge cases with robust pagination
        
        COMPLEX EDGE CASES HANDLED:
        1. Reverse chronological order (newest first)
        2. Inclusive toTime, exclusive fromTime boundaries  
        3. Unreliable cursor - use timestamp-based pagination
        4. Duplicate detection with Set-based deduplication
        5. Incomplete candle filtering based on current time
        6. Smart credit management across multiple API calls
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Token pair address
            fromTime: Start timestamp (exclusive boundary)
            toTime: End timestamp (inclusive boundary)  
            timeframe: Candle timeframe (must be supported by Moralis)
            chain: Blockchain network (defaults to mainnet)
            
        Returns:
            Dict: {success, candles, creditsUsed, latestTime, error?}
        """
        try:
            logger.info(f"Moralis unified fetch: {tokenAddress} from {fromTime} to {toTime}, timeframe: {timeframe}")
            
            # Validate timeframe
            if not self.isCorrectTimeframe(timeframe):
                return self.constructErrorResponse(f"Unsupported timeframe: {timeframe}")
            
            # Initialize fetch state
            moralisAPIResponse = self.constructEmptyAPIResponse(fromTime, self.defaultChain)
            
            # Get all candles with smart pagination
            self.getAllCandlesWithPagination(pairAddress, fromTime, toTime, timeframe, moralisAPIResponse)
            
            # Persist all credits used in single database operation
            self.deductCreditsFromCorrespondingAPIKey(moralisAPIResponse)
            
            # Process candles with deduplication and incomplete candle filtering
            processedCandles, latestFetchedTime = self.checkAndProcessCandles(
                tokenAddress, pairAddress, moralisAPIResponse[MoralisFetchStateKeys.ALL_RAW_CANDLES], fromTime, timeframe
            )
            
            logger.info(f"Moralis fetch completed: {len(processedCandles)} candles, "
                       f"{moralisAPIResponse[MoralisFetchStateKeys.TOTAL_CREDITS_USED]} credits used")
            
            return {
                'success': True,
                'candles': processedCandles,
                'creditsUsed': moralisAPIResponse[MoralisFetchStateKeys.TOTAL_CREDITS_USED],
                'latestTime': latestFetchedTime,
                'candleCount': len(processedCandles)
            }
            
        except Exception as e:
            logger.error(f"Moralis unified fetch failed for {tokenAddress}: {e}")
            return self.constructErrorResponse(str(e))
    
    def constructEmptyAPIResponse(self, fromTime: int, chain: str) -> Dict:
        """Initialize fetch state for unified candle fetching"""
        return {
            MoralisFetchStateKeys.CHAIN: chain,
            MoralisFetchStateKeys.CURRENT_FROM_TIME: fromTime,
            MoralisFetchStateKeys.CURRENT_TO_TIME: None,
            MoralisFetchStateKeys.LATEST_TIME: fromTime,
            MoralisFetchStateKeys.ALL_RAW_CANDLES: [],
            MoralisFetchStateKeys.PROCESSED_TIMESTAMPS: set(),
            MoralisFetchStateKeys.TOTAL_CREDITS_USED: 0,
            MoralisFetchStateKeys.CURRENT_API_KEY: None,
            MoralisFetchStateKeys.CURRENT_AVAILABLE_CREDITS: 0,
            MoralisFetchStateKeys.API_KEYS_USED: []
        }
    
    def getAllCandlesWithPagination(self, pairAddress: str, fromTime: int, toTime: int, 
                                   timeframe: str, moralisAPIResponse: Dict):
        """Execute smart pagination strategy with timestamp-based continuation"""
        
        # Get initial API key
        if not self.getNewAPIKey(moralisAPIResponse):
            raise Exception('No more valid API keys available')
        
        moralisAPIResponse[MoralisFetchStateKeys.CURRENT_TO_TIME] = toTime
        apiCallCount = 0
        
        while moralisAPIResponse[MoralisFetchStateKeys.CURRENT_TO_TIME] > fromTime:
            # Check if current API key has enough credits
            if not self.hasEnoughCredits(moralisAPIResponse):
                self.switchToNewAPIKey(moralisAPIResponse)
            
            apiCallCount += 1
            logger.info(f"Moralis API call #{apiCallCount}: from {fromTime} to {moralisAPIResponse[MoralisFetchStateKeys.CURRENT_TO_TIME]}")
            
            # Make API call
            apiResponse = self.hitAPI(
                apiKey=moralisAPIResponse[MoralisFetchStateKeys.CURRENT_API_KEY]['apikey'],
                pairAddress=pairAddress,
                fromTime=fromTime,
                toTime=moralisAPIResponse[MoralisFetchStateKeys.CURRENT_TO_TIME],
                timeframe=timeframe,
                chain=moralisAPIResponse[MoralisFetchStateKeys.CHAIN]
            )
            
            if not apiResponse or not apiResponse.get('result'):
                logger.warning("No result or empty result from API, stopping fetch")
                break
            
            candlesFromAPI = apiResponse['result']
            if not candlesFromAPI:
                logger.info("No candles in result, stopping fetch")
                break
            
            # Process batch with deduplication
            newCandlesCount, oldestTimestamp = self.formatAndDeduplicateCandles(candlesFromAPI, moralisAPIResponse)
            
            if newCandlesCount == 0:
                logger.info("No new candles found (all duplicates), stopping fetch")
                break
            
            # Update credits after successful call
            self.updateCreditsUsed(moralisAPIResponse)
            
            # Determine if more data needed - pagination logic
            if not self.needsMoreData(candlesFromAPI, oldestTimestamp, fromTime, apiResponse.get('cursor')):
                break
            
            # Update pagination boundary: set toTime to oldest timestamp for next call
            # This handles the inclusive/exclusive boundary issue
            moralisAPIResponse[MoralisFetchStateKeys.CURRENT_TO_TIME] = oldestTimestamp
            
            # Rate limiting
            time.sleep(MoralisAPIConstants.RATE_LIMIT_DELAY_SECONDS)
        
        logger.info(f"Moralis pagination completed after {apiCallCount} API calls")
    
    def formatAndDeduplicateCandles(self, candles: List[Dict], moralisAPIMeta: Dict) -> Tuple[int, int]:
        """Process batch of candles with deduplication tracking"""
        newCandlesCount = 0
        oldestTimestamp = None
        
        for candle in candles:
            timestamp = candle.get('timestamp')
            if not timestamp:
                continue
                
            # Convert ISO timestamp to unix
            unixTime = self.convertISOToUnix(timestamp)
            if unixTime is None:
                continue
            
            # Track oldest timestamp for pagination
            if oldestTimestamp is None or unixTime < oldestTimestamp:
                oldestTimestamp = unixTime
            
            # Deduplication check
            if unixTime in moralisAPIMeta[MoralisFetchStateKeys.PROCESSED_TIMESTAMPS]:
                logger.debug(f"Duplicate candle found at {unixTime}, skipping")
                continue
            
            # Add to processed set and collection
            moralisAPIMeta[MoralisFetchStateKeys.PROCESSED_TIMESTAMPS].add(unixTime)
            moralisAPIMeta[MoralisFetchStateKeys.ALL_RAW_CANDLES].append({
                **candle,
                'unixTime': unixTime
            })
            newCandlesCount += 1
            
            # Track latest time
            if unixTime > moralisAPIMeta[MoralisFetchStateKeys.LATEST_TIME]:
                moralisAPIMeta[MoralisFetchStateKeys.LATEST_TIME] = unixTime
        
        logger.debug(f"Processed batch: {newCandlesCount} new candles, oldest: {oldestTimestamp}")
        return newCandlesCount, oldestTimestamp
    
    def needsMoreData(self, candles: List[Dict], oldestTimestamp: int, 
                      fromTime: int, cursor: str) -> bool:
        """Determine if more API calls needed based on pagination logic"""
        
        # If we got less than 1000 candles, likely reached end
        if cursor:
            logger.info(f"Got cursor, continuing pagination")
            return True
        else:
            return False
    
    def checkAndProcessCandles(self, tokenAddress: str, pairAddress: str, 
                             candlesFromAPI: List[Dict], fromTime: int, timeframe: str) -> Tuple[List[Dict], int]:
        """
        Process raw candles with duplicate filtering and incomplete candle removal
        
        PROCESSING LOGIC:
        1. Filter duplicates based on fromTime (like BirdEye handler)
        2. Remove incomplete candles based on current time and timeframe
        3. Add token metadata for database insertion
        4. Return latest processed time
        """
        processedCandles = []
        latestFetchedAtTime = fromTime # adjusted fromTime to avoid missing first candle for new tokens
        currentTime = int(datetime.now(timezone.utc).timestamp())
        
        # Calculate incomplete candle threshold
        currentLiveCandleStartTime = self.calculateCurrentLiveCandleStartTime(currentTime, timeframe)
        
        for candle in candlesFromAPI:
            try:
                unixTime = candle.get('unixTime')
                if not unixTime or unixTime <= fromTime:
                    continue  # Skip duplicates and invalid times
                
                # Filter out incomplete candles
                if unixTime >= currentLiveCandleStartTime:
                    logger.debug(f"Filtering incomplete candle at {unixTime} (threshold: {currentLiveCandleStartTime})")
                    continue
                
                # Process complete candle
                processed_candle = {
                    'tokenaddress': tokenAddress,
                    'pairaddress': pairAddress,
                    'unixtime': unixTime,
                    'openprice': float(candle.get('open', 0)),
                    'highprice': float(candle.get('high', 0)),
                    'lowprice': float(candle.get('low', 0)),
                    'closeprice': float(candle.get('close', 0)),
                    'volume': float(candle.get('volume', 0)),
                    'timeframe': self.mapToInternalTimefframe(timeframe),
                    'datasource': 'moralis',
                    'trades' : float(candle.get('trades', 0))
                }
                processedCandles.append(processed_candle)
                
                # Track latest time
                if unixTime > latestFetchedAtTime:
                    latestFetchedAtTime = unixTime
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid candle data skipped: {e}")
                continue
        
        logger.info(f"Processed {len(processedCandles)} complete candles, "
                   f"filtered out incomplete candles >= {currentLiveCandleStartTime}")
        return processedCandles, latestFetchedAtTime
    
    def calculateCurrentLiveCandleStartTime(self, currentTime: int, timeframe: str) -> int:
        """
        Calculate threshold for incomplete candle detection
        
        Logic: If current time is 10:20 and timeframe is 1h,
        then incomplete threshold is 10:00 (current hour start)
        Complete candles would be <= 09:00
        """
        timeframeInSeconds = self.getTimeframeSeconds(timeframe)
        
        # Find the start of current timeframe period
        currentLiveCandleTimeStartTime = (currentTime // timeframeInSeconds) * timeframeInSeconds
        
        logger.debug(f"Incomplete threshold calculation: current={currentTime}, "
                    f"timeframe={timeframe}({timeframeInSeconds}s), threshold={currentLiveCandleTimeStartTime}")
        
        return currentLiveCandleTimeStartTime
    
    def getTimeframeSeconds(self, timeframe: str) -> int:
        """Convert timeframe string to seconds"""
        return MoralisAPIConstants.TIMEFRAME_SECONDS.get(timeframe, 0)
    
    def mapToInternalTimefframe(self, moralis_timeframe: str) -> str:
        """Map Moralis timeframe to internal format"""
        return MoralisAPIConstants.TIMEFRAME_MAPPING.get(moralis_timeframe, '15m')
    
    def convertISOToUnix(self, iso_timestamp: str) -> Optional[int]:
        """Convert ISO timestamp to unix timestamp"""
        try:
            dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
            return int(dt.timestamp())
        except Exception as e:
            logger.warning(f"Failed to convert timestamp {iso_timestamp}: {e}")
            return None
    
    def hitAPI(self, apiKey: str, pairAddress: str, fromTime: int, 
                       toTime: int, timeframe: str, chain: str) -> Optional[Dict]:
        """
        Get single chunk of candle data from Moralis API
        
        Args:
            apiKey: Valid Moralis API key
            pairAddress: Token pair address
            fromTime: Start timestamp (exclusive)
            toTime: End timestamp (inclusive)
            timeframe: Candle timeframe
            chain: Blockchain network
            
        Returns:
            Optional[Dict]: Raw API response or None if failed
        """
        
        # Convert unix timestamps to milliseconds for Moralis API
        fromDate = fromTime * MoralisAPIConstants.MILLISECONDS_MULTIPLIER
        toDate = toTime * MoralisAPIConstants.MILLISECONDS_MULTIPLIER
        
        url = f"{self.baseUrl}/token/{chain}/pairs/{pairAddress}/ohlcv"
        
        params = {
            'timeframe': timeframe,
            'currency': MoralisAPIConstants.DEFAULT_CURRENCY,
            'fromDate': fromDate,
            'toDate': toDate,
            'limit': MoralisAPIConstants.MAX_RECORDS_PER_CALL
        }
        
        headers = {
            'X-API-Key': apiKey,
            'accept': 'application/json'
        }
        
        try:
            response = self.session.get(
                url,
                headers=headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            logger.debug(f"Moralis API response: {len(data.get('result', []))} candles, "
                        f"cursor: {data.get('cursor', 'None')}")
            
            return data
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Moralis API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in Moralis API call: {e}")
            return None
    
    def getNewAPIKey(self, fetch_state: Dict) -> bool:
        """Get new API key and store its available credits"""
        api_key_data = self.db.credentials.getNextValidApiKey(
            serviceName=self.service.service_name,
            requiredCredits=self.creditsPerCall
        )
        
        if not api_key_data:
            return False
        
        fetch_state[MoralisFetchStateKeys.CURRENT_API_KEY] = api_key_data
        fetch_state[MoralisFetchStateKeys.CURRENT_AVAILABLE_CREDITS] = api_key_data.get('availablecredits', 0)
        
        logger.info(f"Got new Moralis API key with {fetch_state[MoralisFetchStateKeys.CURRENT_AVAILABLE_CREDITS]} credits")
        return True
    
    def hasEnoughCredits(self, fetch_state: Dict) -> bool:
        """Check if current API key has enough credits for next call"""
        return fetch_state[MoralisFetchStateKeys.CURRENT_AVAILABLE_CREDITS] >= self.creditsPerCall
    
    def switchToNewAPIKey(self, fetch_state: Dict):
        """Switch to new API key when current one doesn't have enough credits"""
        if fetch_state[MoralisFetchStateKeys.CURRENT_API_KEY]:
            original_credits = fetch_state[MoralisFetchStateKeys.CURRENT_API_KEY].get('availablecredits', 0)
            credits_used = original_credits - fetch_state[MoralisFetchStateKeys.CURRENT_AVAILABLE_CREDITS]
            
            fetch_state[MoralisFetchStateKeys.API_KEYS_USED].append({
                'api_key_id': fetch_state[MoralisFetchStateKeys.CURRENT_API_KEY]['id'],
                'credits_used': credits_used
            })
            
            logger.info(f"Moralis API key exhausted, used {credits_used} credits")
        
        if not self.getNewAPIKey(fetch_state):
            raise Exception('No more valid Moralis API keys available')
    
    def updateCreditsUsed(self, fetch_state: Dict):
        """Update credits after successful API call"""
        fetch_state[MoralisFetchStateKeys.CURRENT_AVAILABLE_CREDITS] -= self.creditsPerCall
        fetch_state[MoralisFetchStateKeys.TOTAL_CREDITS_USED] += self.creditsPerCall
    
    def deductCreditsFromCorrespondingAPIKey(self, fetch_state: Dict):
        """Persist all credits used across all API keys"""
        if fetch_state[MoralisFetchStateKeys.CURRENT_API_KEY] and fetch_state[MoralisFetchStateKeys.TOTAL_CREDITS_USED] > 0:
            original_credits = fetch_state[MoralisFetchStateKeys.CURRENT_API_KEY].get('availablecredits', 0)
            credits_used = original_credits - fetch_state[MoralisFetchStateKeys.CURRENT_AVAILABLE_CREDITS]
            
            if credits_used > 0:
                fetch_state[MoralisFetchStateKeys.API_KEYS_USED].append({
                    'api_key_id': fetch_state[MoralisFetchStateKeys.CURRENT_API_KEY]['id'],
                    'credits_used': credits_used
                })
        
        # Persist all credits in batch
        for key_usage in fetch_state[MoralisFetchStateKeys.API_KEYS_USED]:
            self.db.credentials.deductAPIKeyCredits(
                key_usage['api_key_id'], 
                key_usage['credits_used']
            )
            logger.debug(f"Persisted {key_usage['credits_used']} credits for Moralis API key {key_usage['api_key_id']}")
    
    def isCorrectTimeframe(self, timeframe: str) -> bool:
        """Validate if timeframe is supported by Moralis"""
        return timeframe in self.supportedTimeframes
    
    def constructErrorResponse(self, error_message: str) -> Dict:
        """Create standardized error response"""
        return {
            'success': False,
            'error': error_message,
            'candles': [],
            'creditsUsed': 0,
            'candleCount': 0
        }
    
    def getCandleDataForToken(self, tokenAddress: str, pairAddress: str, 
                             fromTime: int, toTime: int, timeframe: str, symbol: str = '') -> Dict:
        """
        Get processed candle data for a specific token using UNIFIED approach
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Token pair address  
            fromTime: Start timestamp
            toTime: End timestamp
            symbol: Token symbol for logging
            timeframe: Candle timeframe
            chain: Blockchain network
            
        Returns:
            Dict: Result with success status, candles, and metadata
        """
        try:
            result = self.getAllCandleDataFromAPI(
                tokenAddress, pairAddress, fromTime, toTime, timeframe
            )
            
            if not result['success']:
                return {
                    'success': False,
                    'error': result.get('error', 'No candle data received from Moralis API'),
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
            logger.error(f"Error getting Moralis candle data for token {symbol}: {e}")
            
            return {
                'success': False,
                'error': str(e),
                CandleDataKeys.CANDLES: [],
                CandleDataKeys.COUNT: 0
            }