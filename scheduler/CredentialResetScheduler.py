from database.auth.CredentialsHandler import CredentialsHandler
from database.auth.ServiceCredentialsEnum import ServiceCredentials
from logs.logger import get_logger
from typing import Dict, List, Any
from datetime import datetime, timedelta

logger = get_logger(__name__)


class CredentialResetScheduler:
    """
    Scheduler for handling automatic credential resets based on service configurations.
    
    This scheduler runs every 12 hours to identify and reset credentials that have reached
    their reset time based on the configured reset duration for each service.
    """

    def __init__(self, credentials_handler: CredentialsHandler = None):
        self.credentials_handler = credentials_handler or CredentialsHandler()
        
    def processCredentialResets(self) -> None:
        self.credentials_handler.resetCredentialsDueForReset()
    
    def runDailyResetJob(self) -> None:
        """Run credential reset job."""
        self.processCredentialResets()
            