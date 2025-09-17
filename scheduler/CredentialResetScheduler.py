from database.auth.CredentialsHandler import CredentialsHandler
from database.auth.ServiceCredentialsEnum import ServiceCredentials
from logs.logger import get_logger
from typing import Dict, List, Any
from datetime import datetime, timedelta

logger = get_logger(__name__)


class CredentialResetScheduler:
    """
    Scheduler for handling automatic credential resets based on service configurations.
    
    This scheduler runs daily to identify and reset credentials that have reached
    their reset time based on the configured reset duration for each service.
    """

    def __init__(self, credentials_handler: CredentialsHandler = None):
        """
        Initialize the credential reset scheduler
        
        Args:
            credentials_handler: Optional CredentialsHandler instance
        """
        self.credentials_handler = credentials_handler or CredentialsHandler()
        
    def processCredentialResets(self) -> Dict[str, Any]:
        """
        Main method to process all credential resets that are due using a single UPDATE query
        
        Returns:
            Dict containing processing results and statistics
        """
        try:
            logger.info("Starting credential reset processing")
            
            # Use single UPDATE query to reset all credentials due for reset
            result = self.credentials_handler.resetCredentialsDueForReset()
            
            if result["success"]:
                credentials_reset = result["credentials_reset"]
                logger.info(f"Successfully reset {credentials_reset} credentials")
                
                return {
                    "success": True,
                    "message": f"Reset {credentials_reset} credentials",
                    "credentials_processed": credentials_reset,
                    "successful_resets": credentials_reset,
                    "failed_resets": 0
                }
            else:
                logger.error(f"Failed to reset credentials: {result.get('error', 'Unknown error')}")
                return {
                    "success": False,
                    "message": f"Failed to reset credentials: {result.get('error', 'Unknown error')}",
                    "credentials_processed": 0,
                    "successful_resets": 0,
                    "failed_resets": 0
                }
            
        except Exception as e:
            logger.error(f"Critical error in credential reset processing: {str(e)}")
            return {
                "success": False,
                "message": f"Critical error: {str(e)}",
                "credentials_processed": 0,
                "successful_resets": 0,
                "failed_resets": 0
            }
    
    
    