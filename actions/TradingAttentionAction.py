from config.Config import get_config
from typing import Optional, Dict, Any, List, Union
from database.operations.PortfolioDB import PortfolioDB
from database.operations.schema import TradingAttentionInfo
import parsers.TradingAttentionParser as tradingAttentionParsers
import requests
from decimal import Decimal
import time
from datetime import datetime
from logs.logger import get_logger
from database.tradingattention.TradingAttentionHandler import TradingAttentionHandler
from services.AuthService import AuthService
from database.auth.ServiceCredentialsEnum import ServiceCredentials, CredentialType
import pytz

logger = get_logger(__name__)

class TradingAttentionAction:
    """Handles complete trading attention data request workflow"""
    
    def __init__(self, db: PortfolioDB):
        """
        Initialize action with required parameters
        Args:
            db: Database handler for persistence
        """
        self.db = db
        self.service = ServiceCredentials.CHAINEDGE
        self.baseUrl = self.service.metadata['base_url']
        self.tokenHandler = self.db.token

    def processTradingAttentionTokens(self, cookie: str) -> bool:
        """
        Fetch and persist trading attention tokens
        Args:
            cookie: Validated cookie for API request
        Returns:
            bool: Success status
        """
        try:
            # Make API request with provided cookie
            response = self.hitAPI(cookie)
            if not response:
                logger.error("API request failed")
                return False

            # Parse response into TradingAttentionInfo objects
            tradingAttentionTokens = tradingAttentionParsers.parseTradingAttentionResponse(response)
            if not tradingAttentionTokens:
                logger.error("No valid items found in response")
                return False
            
            logger.info(f"Found {len(tradingAttentionTokens)} trading attention tokens at time {datetime.now()}")
            
            # Persist to database and get successfully persisted tokens
            persistedTokens = self.persistTokens(tradingAttentionTokens)
            logger.info(f"Persisted {len(persistedTokens)} trading attention tokens at time {datetime.now()}")
            return len(persistedTokens) > 0

        except Exception as e:
            logger.error(f"Trading attention data action failed: {str(e)}")
            return False

    def hitAPI(self, cookie: str) -> Optional[Dict]:
        """Make trading attention API request"""
        try:
            # Get fresh access token using service credentials
            authService = AuthService(
                self.tokenHandler, 
                self.db,
                self.service
            )
            accessToken = authService.getValidAccessToken()
            
            if not accessToken:
                logger.error("Failed to get valid access token")
                return None
            
            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-IN,en-GB;q=0.9,en;q=0.8,en-US;q=0.7',
                'authorization': f'Bearer {accessToken}',
                'cookie': cookie,
                'origin': 'https://trading.chainedge.io',
                'priority': 'u=1, i',
                'referer': 'https://trading.chainedge.io/',
                'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0'
            }

            params = {
                'chain': 'sol'
            }

            # Use the trading attention API URL
            url = 'https://trading-api-ce111.chainedge.io/api/discoverChartDataFetchV2/'
            
            # Log the request URL for debugging
            logger.info(f"Making request to: {url} with params: {params}")
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            return None

    def persistTokens(self, tradingAttentionTokens: List[TradingAttentionInfo]) -> List[TradingAttentionInfo]:
        """
        Persist TradingAttentionInfo objects to database using optimized batch processing
        
        Args:
            tradingAttentionTokens: List of TradingAttentionInfo objects to persist
            
        Returns:
            List[TradingAttentionInfo]: List of tokens that were successfully persisted
        """
        try:
            logger.info(f"Starting batch persistence of {len(tradingAttentionTokens)} trading attention tokens at time {datetime.now()}")
            
            # Use the batch insertion method
            successfulTokens = self.db.tradingattention.batchInsertTradingAttentionTokens(tradingAttentionTokens)
            
            logger.info(f"Batch persisted {len(successfulTokens)} trading attention tokens at time {datetime.now()}")
            
            logger.info(f"Successfully batch persisted {len(successfulTokens)} trading attention tokens")
            return successfulTokens
            
        except Exception as e:
            logger.error(f"Error in batch token persistence: {str(e)}")
            return [] 