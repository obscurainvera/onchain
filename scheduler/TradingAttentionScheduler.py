from config.Config import get_config

"""
Take all the tokens from the trading attention data and persist them to the database

Runs multiple times per day
"""

from database.operations.PortfolioDB import PortfolioDB
from config.Security import COOKIE_MAP, isValidCookie
from logs.logger import get_logger
from actions.TradingAttentionAction import TradingAttentionAction
import time
import random
from dotenv import load_dotenv
from config.Security import isCookieExpired
import requests

logger = get_logger(__name__)


class TradingAttentionScheduler:
    """Manages trading attention data collection and scheduling"""

    def __init__(self, dbPath: str = None):
        """
        Initialize scheduler with database instance

        Args:
            dbPath: Path to SQLite database file (DEPRECATED)
        """
        self.db = PortfolioDB()
        self.action = TradingAttentionAction(self.db)
        logger.info(f"Trading attention scheduler initialized using database configuration")

    def processTradingAttentionData(self, cookie: str, addDelay: bool = False) -> bool:
        """
        Process trading attention data for a single cookie

        Args:
            cookie: API cookie to use
            addDelay: Whether to add random delay after processing
        Returns:
            bool: Success status
        """
        try:
            logger.info(f"Using cookie: {cookie[:15]}...")

            # Hit the health check API first
            try:
                requests.get('https://solportprod.onrender.com', timeout=10)
            except Exception as api_error:
                logger.warning(f"Health check API call failed: {api_error}. Continuing with processing...")

            # Execute trading attention data action with validated cookie
            success = self.action.processTradingAttentionTokens(cookie=cookie)

            if success:
                logger.info("Successfully processed trading attention data")
            else:
                logger.warning("Failed to process trading attention data")

            return success

        except Exception as e:
            logger.error(f"Error processing trading attention data: {e}")
            return False

    def handleTradingAttentionAnalysisFromJob(self):
        """Execute trading attention data collection and analysis with delays"""
        config = get_config()

        if isCookieExpired(config.VOLUME_EXPIRY):  # Using the same cookie as volume for now
            logger.warning("Volume cookie expired")
            return False

        self.processTradingAttentionData(config.VOLUME_COOKIE, addDelay=True)

    def handleTradingAttentionAnalysisFromAPI(self):
        """Execute trading attention data collection and analysis without delays"""
        config = get_config()

        if isCookieExpired(config.VOLUME_EXPIRY):  # Using the same cookie as volume for now
            logger.warning("Volume cookie expired")
            return False
        return self.processTradingAttentionData(cookie=config.VOLUME_COOKIE, addDelay=False) 