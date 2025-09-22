"""
Token Request Validator - Clean validation logic for token requests
"""

from typing import Tuple
from constants.TradingConstants import TimeframeConstants, ValidationMessages
from logs.logger import get_logger

logger = get_logger(__name__)


class TokenRequestValidator:
    """Validator for token addition requests"""
    
    @staticmethod
    def validateRequestData(data: dict) -> Tuple[bool, str]:
        if not data:
            return False, 'No JSON data provided'
        
        # Check required fields
        tokenAddress = data.get('tokenAddress', '').strip()
        pairAddress = data.get('pairAddress', '').strip()
        
        if not tokenAddress:
            return False, 'Missing required field: tokenAddress'
        
        if not pairAddress:
            return False, 'Missing required field: pairAddress'
        
        return True, ''