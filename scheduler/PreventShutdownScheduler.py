from config.Config import get_config
from logs.logger import get_logger
import requests

logger = get_logger(__name__)


class PreventShutdownScheduler:
    """Manages shutdown prevention by calling configured URLs"""

    def __init__(self):
        logger.info("PREVENT SHUTDOWN SCHEDULER :: PreventShutdown scheduler initialized")

    def callPreventShutdownUrl(self, url: str) -> bool:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                logger.info(f"PREVENT SHUTDOWN SCHEDULER :: Successfully called prevent shutdown URL: {url}")
                return True
            else:
                logger.info(f"PREVENT SHUTDOWN SCHEDULER :: Failed to call URL {url}: HTTP {response.status_code}")
                return False    
        except Exception as e:
            logger.info(f"PREVENT SHUTDOWN SCHEDULER :: Error calling URL {url}: {e}")
            return False

    def handlePreventShutdownFromJob(self):
        config = get_config()
        
        if config.PREVENT_SHUTDOWN_URL:
            self.callPreventShutdownUrl(config.PREVENT_SHUTDOWN_URL)
        
        if config.PREVENT_EXTERNAL_SHUTDOWN_URL:
            urls = [url.strip() for url in config.PREVENT_EXTERNAL_SHUTDOWN_URL.split(',') if url.strip()]
            for url in urls:
                self.callPreventShutdownUrl(url)

    def handlePreventShutdownFromAPI(self):
        return self.handlePreventShutdownFromJob()