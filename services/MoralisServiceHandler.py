"""
Handles all Moralis API related operations including pagination, deduplication and candle data processing
Addresses complex edge cases: reverse chronological order, inclusive/exclusive boundaries, incomplete candles
"""
from typing import Dict, List, Optional
import requests
import time
from datetime import datetime, timezone
from logs.logger import get_logger
from database.operations.PortfolioDB import PortfolioDB
from database.auth.ServiceCredentialsEnum import ServiceCredentials
from scheduler.SchedulerConstants import CandleDataKeys
from models.Candle import Candle
from models.CandleResponse import CandleResponse
from models.FetchState import FetchState

logger = get_logger(__name__)


class MoralisAPIConstants:
    """Constants for Moralis API configuration and mappings"""
    MAX_RECORDS_PER_CALL = 1000
    DEFAULT_CURRENCY = 'usd'
    MILLISECONDS_MULTIPLIER = 1000
    RATE_LIMIT_DELAY_SECONDS = 1
    
   
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
                              toTime: int, timeframe: str) -> CandleResponse:
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
            CandleResponse: Response with candles and metadata
        """
        try:
            logger.info(f"MORALIS :: fetch: {tokenAddress} from {fromTime} to {toTime}, timeframe: {timeframe}")
            
            # Validate timeframe
            if not self.isCorrectTimeframe(timeframe):
                return CandleResponse.errorResponse(f"MORALIS :: Unsupported timeframe: {timeframe}")
            
            # Initialize fetch state
            fetchState = FetchState.createForFetch(self.defaultChain, fromTime)
            
            # Get all candles with smart pagination
            self.getAllCandlesWithPagination(pairAddress, fromTime, toTime, timeframe, fetchState)
            
            # Persist all credits used in single database operation
            self.deductCreditsFromCorrespondingAPIKey(fetchState)
            
            # Process candles with deduplication and incomplete candle filtering
            processedCandles, latestTime = self.processCandles(tokenAddress, pairAddress, fetchState.allRawCandles, fromTime, timeframe)
            
            logger.info(f"Moralis fetch completed: {len(processedCandles)} candles, {fetchState.totalCreditsUsed} credits used")        
            
            return CandleResponse.successResponse(processedCandles, fetchState.totalCreditsUsed, latestTime)
            
        except Exception as e:
            logger.error(f"Moralis unified fetch failed for {tokenAddress}: {e}")
            return CandleResponse.errorResponse(str(e))
    
    
    def getAllCandlesWithPagination(self, pairAddress: str, fromTime: int, toTime: int, 
                                   timeframe: str, fetchState: FetchState):
        """Execute smart pagination strategy with timestamp-based continuation"""
        
        # Get initial API key
        if not self.getNewAPIKey(fetchState):
            raise Exception('MORALIS :: No more valid API keys available')
        
        fetchState.currentToTime = toTime
        apiCallCount = 0
        
        while fetchState.currentToTime > fromTime:
            # Check if current API key has enough credits
            if not self.hasEnoughCredits(fetchState):
                self.switchToNewAPIKey(fetchState)
            
            apiCallCount += 1
            logger.info(f"MORALIS :: API call #{apiCallCount}: from {fromTime} to {fetchState.currentToTime}")
            
            # Make API call
            apiResponse = self.hitAPI(
                apiKey=fetchState.currentApiKey['apikey'],
                pairAddress=pairAddress,
                fromTime=fromTime,
                toTime=fetchState.currentToTime,
                timeframe=timeframe,
                chain=fetchState.chain
            )
            
            if not apiResponse or not apiResponse.get('result'):
                logger.warning("MORALIS :: No result or empty result from API, stopping fetch")
                break
            
            candlesFromAPI = apiResponse['result']
            if not candlesFromAPI:
                logger.info("MORALIS :: No candles in result, stopping fetch")
                break
            
            # Process batch with deduplication
            newCandlesCount, oldestTimestamp = self.formatAndDeduplicateCandles(candlesFromAPI, fetchState)
            
            if newCandlesCount == 0:
                logger.info("MORALIS :: No new candles found (all duplicates), stopping fetch")
                break
            
            # Update credits after successful call
            self.updateCreditsUsed(fetchState)
            
            # Determine if more data needed - pagination logic
            if not self.needsMoreData(candlesFromAPI, oldestTimestamp, fromTime, apiResponse.get('cursor')):
                break
            
            # Update pagination boundary: set toTime to oldest timestamp for next call
            fetchState.currentToTime = oldestTimestamp
            
            # Rate limiting
            time.sleep(MoralisAPIConstants.RATE_LIMIT_DELAY_SECONDS)
        
        logger.info(f"MORALIS :: Pagination completed after {apiCallCount} API calls")
    
    def formatAndDeduplicateCandles(self, candles: List[Dict], fetchState: FetchState) -> tuple[int, int]:
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
            if fetchState.isTimestampProcessed(unixTime):
                logger.debug(f"Duplicate candle found at {unixTime}, skipping")
                continue
            
            # Add to processed set and collection
            fetchState.addProcessedTimestamp(unixTime)
            fetchState.addRawCandle({
                **candle,
                'unixTime': unixTime
            })
            newCandlesCount += 1
            
            # Track latest time
            fetchState.updateLatestTime(unixTime)
        
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
    
    def processCandles(self, tokenAddress: str, pairAddress: str, 
                      candlesFromAPI: List[Dict], fromTime: int, timeframe: str) -> tuple[List[Candle], int]:
        """Process raw candles with duplicate filtering and incomplete candle removal"""
        processedCandles = []
        currentTime = int(datetime.now(timezone.utc).timestamp())
        
        # Calculate incomplete candle threshold
        currentLiveCandleStartTime = self.calculateCurrentLiveCandleStartTime(currentTime, timeframe)
        latestTime = fromTime
        
        for candle in candlesFromAPI:
            try:
                unixTime = candle.get('unixTime')
                if not unixTime or unixTime <= fromTime:
                    continue  # Skip duplicates and invalid times
                
                # Filter out incomplete candles
                if unixTime >= currentLiveCandleStartTime:
                    logger.debug(f"Filtering incomplete candle at {unixTime} (threshold: {currentLiveCandleStartTime})")
                    continue
                
                # Create Candle object
                processedCandle = Candle.fromRawData(
                    rawCandle=candle,
                    tokenAddress=tokenAddress,
                    pairAddress=pairAddress,
                    timeframe=timeframe,
                    dataSource='moralis'
                )

                if processedCandle.unixTime > latestTime:
                    latestTime = processedCandle.unixTime

                processedCandles.append(processedCandle)
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid candle data skipped: {e}")
                continue
        
        logger.info(f"MORALIS :: Processed {len(processedCandles)} complete candles, "
                   f"filtered out incomplete candles >= {currentLiveCandleStartTime}")
        return processedCandles, latestTime
    
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
        """Convert timeframe string to seconds - delegates to CommonUtil"""
        from utils.CommonUtil import CommonUtil
        return CommonUtil.getTimeframeSeconds(timeframe)

    
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
            
            return data
                
        except Exception as e:
            logger.error(f"MORALIS :: Error in API call: {e}")
            return None
    
    def getNewAPIKey(self, fetchState: FetchState) -> bool:
        """Get new API key and store its available credits"""
        apiKeyData = self.db.credentials.getNextValidApiKey(
            serviceName=self.service.service_name,
            requiredCredits=self.creditsPerCall
        )
        
        if not apiKeyData:
            return False
        
        fetchState.currentApiKey = apiKeyData
        fetchState.currentAvailableCredits = apiKeyData.get('availablecredits', 0)
        
        logger.info(f"Got new Moralis API key with {fetchState.currentAvailableCredits} credits")
        return True
    
    def hasEnoughCredits(self, fetchState: FetchState) -> bool:
        """Check if current API key has enough credits for next call"""
        return fetchState.currentAvailableCredits >= self.creditsPerCall
    
    def switchToNewAPIKey(self, fetchState: FetchState):
        """Switch to new API key when current one doesn't have enough credits"""
        if fetchState.currentApiKey:
            originalCredits = fetchState.currentApiKey.get('availablecredits', 0)
            creditsUsed = originalCredits - fetchState.currentAvailableCredits
            
            fetchState.addApiKeyUsage(fetchState.currentApiKey['id'], creditsUsed)
            logger.info(f"Moralis API key exhausted, used {creditsUsed} credits")
        
        if not self.getNewAPIKey(fetchState):
            raise Exception('No more valid Moralis API keys available')
    
    def updateCreditsUsed(self, fetchState: FetchState):
        """Update credits after successful API call"""
        fetchState.useCredits(self.creditsPerCall)
    
    def deductCreditsFromCorrespondingAPIKey(self, fetchState: FetchState):
        """Persist all credits used across all API keys"""
        if fetchState.currentApiKey and fetchState.totalCreditsUsed > 0:
            originalCredits = fetchState.currentApiKey.get('availablecredits', 0)
            creditsUsed = originalCredits - fetchState.currentAvailableCredits
            
            if creditsUsed > 0:
                fetchState.addApiKeyUsage(fetchState.currentApiKey['id'], creditsUsed)
        
        # Persist all credits in batch
        for keyUsage in fetchState.apiKeysUsed:
            self.db.credentials.deductAPIKeyCredits(
                keyUsage['api_key_id'], 
                keyUsage['credits_used']
            )
            logger.debug(f"MORALIS :: Persisted {keyUsage['credits_used']} credits for API key {keyUsage['api_key_id']}")
    
    def isCorrectTimeframe(self, timeframe: str) -> bool:
        """Validate if timeframe is supported by Moralis"""
        return timeframe in self.supportedTimeframes
    
    
    def getCandleDataForToken(self, tokenAddress: str, pairAddress: str, 
                             fromTime: int, toTime: int, timeframe: str, symbol: str = '') -> CandleResponse:
        """Get candle data for a token"""       
        return self.getAllCandleDataFromAPI(tokenAddress, pairAddress, fromTime, toTime, timeframe)
            
        