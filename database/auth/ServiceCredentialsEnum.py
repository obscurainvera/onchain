from config.Config import get_config
from enum import Enum
from typing import Dict

class CredentialType(Enum):
    """Types of credentials supported"""
    API_KEY = "API_KEY"
    USER_PASS = "USER_PASS"
    CHAT_ID = "CHAT_ID"


class CredentialField:
    """Constants for credential field names in servicecredentials table"""
    API_KEY = "apikey"
    API_SECRET = "apisecret" 
    USERNAME = "username"
    PASSWORD = "password"
    METADATA = "metadata"
    SERVICE_NAME = "servicename"
    CREDENTIAL_TYPE = "credentialtype"
    IS_ACTIVE = "isactive"


class ServiceCredentials(Enum):
    
    CIELO = {
        "service_name": "cielo",
        "credential_type": CredentialType.API_KEY,
        "requires_credits": False,
        "reset_duration_days": None,  # Reset every 7 days
        "metadata": {
            "base_url": "https://feed-api.cielo.finance/api/v1",
            "credits_per_call": 3,
            "rate_limit": 100,
            "default_credits": 5000
        }
    }

    TELEGRAM = {
        "service_name": "telegram",
        "credential_type": CredentialType.API_KEY,
        "requires_credits": False,
        "reset_duration_days": None,  # No automatic reset
        "metadata": {
            "base_url": "https://api.telegram.org/bot{token}/sendMessage",
            "description": "Telegram Bot API for sending notifications",
            "credential_types": [CredentialType.API_KEY, CredentialType.CHAT_ID]
        }
    }

    BIRDEYE = {
        "service_name": "birdeye",
        "credential_type": CredentialType.API_KEY,
        "requires_credits": False,
        "reset_duration_days": None,  # Reset daily
        "metadata": {
            "base_url": "https://public-api.birdeye.so",
            "credits_per_call": 40,
            "rate_limit": 60,
            "description": "BirdEye API for Solana token OHLCV data",
            "default_credits": 30000
        }
    }

    MORALIS = {
        "service_name": "moralis",
        "credential_type": CredentialType.API_KEY,
        "requires_credits": True,
        "reset_duration_days": 2,
        "metadata": {
            "base_url": "https://solana-gateway.moralis.io",
            "credits_per_call": 150,
            "rate_limit": 30,
            "description": "Moralis API for token OHLCV data across multiple chains",
            "supported_timeframes": ["1s", "10s", "30s", "1min", "5min", "10min", "30min", "1h", "4h", "12h", "1d", "1w", "1M"],
            "default_chain": "mainnet",
            "default_credits": 39000
        }
    }

    def __init__(self, config: Dict):
        self.service_name = config["service_name"]
        self.credential_type = config["credential_type"]
        self.requires_credits = config["requires_credits"]
        self.reset_duration_days = config.get("reset_duration_days")
        self.metadata = config["metadata"]

    @classmethod
    def get_by_name(cls, service_name: str) -> "ServiceCredentials":
        """Get service configuration by service name"""
        for service in cls:
            if service.service_name == service_name:
                return service
        raise ValueError(f"Unknown service: {service_name}")

    @classmethod
    def get_all_services(cls) -> Dict[str, "ServiceCredentials"]:
        """Get mapping of all service names to their configurations"""
        return {service.service_name: service for service in cls}

    def __str__(self) -> str:
        return f"{self.service_name} ({self.credential_type.value})" 