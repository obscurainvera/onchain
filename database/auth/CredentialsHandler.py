from config.Config import get_config
from database.auth.ServiceCredentialsEnum import ServiceCredentials
from database.operations.BaseDBHandler import BaseDBHandler
from typing import Dict, Optional, Any, List
from datetime import datetime
from logs.logger import get_logger
import json
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from sqlalchemy import text

logger = get_logger(__name__)


class CredentialsHandler(BaseDBHandler):
    """
    Handler for managing service credentials including API keys and username/password pairs.
    Supports tracking API credits and other metadata.
    """

    def __init__(self, conn_manager=None):
        if conn_manager is None:
            conn_manager = DatabaseConnectionManager()
        super().__init__(conn_manager)
        self._createTables()

    def _createTables(self):
        """Creates the credentials tables"""
        with self.conn_manager.transaction() as cursor:
            config = get_config()

            if config.DB_TYPE == "postgres":
                # PostgreSQL syntax
                cursor.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS servicecredentials (
                        id SERIAL PRIMARY KEY,
                        servicename VARCHAR(100) NOT NULL,
                        credentialtype VARCHAR(20) NOT NULL,
                        isactive INTEGER DEFAULT 1,
                        metadata TEXT,
                        apikey TEXT,   
                        apisecret TEXT,
                        availablecredits INTEGER,
                        username TEXT,
                        password TEXT,
                        createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        lastusedat TIMESTAMP,
                        expiresat TIMESTAMP,
                        lastresetat TIMESTAMP,
                        nextresetat TIMESTAMP,
                        isresetavailable BOOLEAN DEFAULT FALSE,                    
                        UNIQUE(servicename, apikey),
                        UNIQUE(servicename, username)
                    )
                """
                    )
                )
            else:
                # SQLite syntax
                cursor.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS servicecredentials (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        servicename VARCHAR(100) NOT NULL,
                        credentialtype VARCHAR(20) NOT NULL,
                        isactive BOOLEAN DEFAULT 1,
                        metadata TEXT,
                        apikey TEXT,   
                        apisecret TEXT,
                        availablecredits INTEGER,
                        username TEXT,
                        password TEXT,
                        createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        lastusedat TIMESTAMP,
                        expiresat TIMESTAMP,
                        lastresetat TIMESTAMP,
                        nextresetat TIMESTAMP,
                        isresetavailable BOOLEAN DEFAULT FALSE,                    
                        UNIQUE(servicename, apikey),
                        UNIQUE(servicename, username)
                    )
                """
                    )
                )

    
    
    def getCredentialsByType(self, serviceName: str, credentialType: str) -> Optional[Dict[str, Any]]:
        """
        Get credentials for a service by credential type

        Args:
            serviceName: Name of the service
            credentialType: Type of credential to get (e.g., API_KEY, CHAT_ID)

        Returns:
            Optional[Dict[str, Any]]: Credential data or None if not found
        """
        try:
            config = get_config()
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == "postgres":
                    cursor.execute(
                        text(
                            """
                        SELECT * FROM servicecredentials
                        WHERE servicename = %s
                        AND credentialtype = %s
                        AND isactive = 1
                        ORDER BY updatedat DESC
                        LIMIT 1
                    """
                        ),
                        (serviceName, credentialType),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT * FROM servicecredentials
                        WHERE servicename = ?
                        AND credentialtype = ?
                        AND isactive = 1
                        ORDER BY updatedat DESC
                        LIMIT 1
                    """,
                        (serviceName, credentialType),
                    )

                result = cursor.fetchone()
                if result:
                    creds = dict(result)
                    if creds["metadata"]:
                        creds["metadata"] = json.loads(creds["metadata"])
                    return creds
                return None
        except Exception as e:
            logger.info(
                f"Failed to get credentials for {serviceName} with type {credentialType}: {str(e)}"
            )
            return None

    

    def getNextValidApiKey(self, serviceName: str, requiredCredits: int) -> Optional[Dict]:
        """
        Get next API key with sufficient credits

        Args:
            serviceName: Name of the service (e.g., 'cielo')
            requiredCredits: Minimum credits needed

        Returns:
            Optional[Dict]: API key details if found, None otherwise
        """
        try:
            config = get_config()
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == "postgres":
                    cursor.execute(
                        text(
                            """
                        SELECT id, apikey, availablecredits 
                        FROM servicecredentials
                        WHERE servicename = %s
                        AND credentialtype = 'API_KEY'
                        AND isactive = 1
                        AND availablecredits >= %s
                        ORDER BY lastusedat ASC NULLS FIRST
                        LIMIT 1
                    """
                        ),
                        (serviceName, requiredCredits),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT id, apikey, availablecredits 
                        FROM servicecredentials
                        WHERE servicename = ?
                        AND credentialtype = 'API_KEY'
                        AND isactive = 1
                        AND availablecredits >= ?
                        ORDER BY lastusedat ASC
                        LIMIT 1
                    """,
                        (serviceName, requiredCredits),
                    )

                result = cursor.fetchone()
                return dict(result) if result else None

        except Exception as e:
            logger.info(
                f"Failed to get next valid API key for {serviceName}: {str(e)}"
            )
            return None

    def deductAPIKeyCredits(self, keyId: int, creditsUsed: int) -> bool:
        """
        Update credits for used API key

        Args:
            keyId: ID of the API key
            creditsUsed: Number of credits to deduct

        Returns:
            bool: True if update successful, False otherwise
        """
        try:
            config = get_config()
            current_time = datetime.now()
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == "postgres":
                    cursor.execute(
                        text(
                            """
                        UPDATE servicecredentials
                        SET availablecredits = availablecredits - %s,
                            lastusedat = %s
                        WHERE id = %s
                    """
                        ),
                        (creditsUsed, current_time, keyId),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE servicecredentials
                        SET availablecredits = availablecredits - ?,
                            lastusedat = ?
                        WHERE id = ?
                    """,
                        (creditsUsed, current_time, keyId),
                    )
                return True
        except Exception as e:
            logger.info(f"Failed to update API key credits for key {keyId}: {str(e)}")
            return False

    def resetCredentialsDueForReset(self) -> None:
        try:
            currentTime = datetime.now()
            query, parameters = self.buildResetQuery(currentTime)
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text(query), parameters)
                rowsAffected = cursor.rowcount
                
                logger.info(f"Reset {rowsAffected} credentials due for reset")
                
        except Exception as e:
            logger.info(f"Failed to reset credentials: {str(e)}")

    def buildResetQuery(self, currentTime: datetime) -> tuple[str, list]:
        creditCases = []
        intervalCases = []
        parameters = [currentTime]  # for lastResetAt
        
        # Build CASE statements for each service that needs reset
        for service in ServiceCredentials:
            if self.shouldResetCredit(service):
                defaultCredits = service.metadata.get("default_credits", 1000)
                creditCases.append(f"WHEN servicename = '{service.service_name}' THEN {defaultCredits}")
                intervalCases.append(f"WHEN servicename = '{service.service_name}' THEN %s + INTERVAL '{service.reset_duration_days} days'")
                parameters.append(currentTime)  # for nextResetAt calculation
        
        # Build the complete SQL query
        creditCaseSql = "CASE " + " ".join(creditCases) + " ELSE availablecredits END"
        intervalCaseSql = "CASE " + " ".join(intervalCases) + " ELSE nextresetat END"
        
        query = f"""
            UPDATE servicecredentials 
            SET availablecredits = {creditCaseSql},
                lastresetat = %s,
                nextresetat = {intervalCaseSql},
                updatedat = %s
            WHERE isactive = 1 
            AND isresetavailable = TRUE 
            AND nextresetat IS NOT NULL 
            AND nextresetat <= %s
        """
        
        # Add remaining parameters
        parameters.extend([currentTime, currentTime])  # for updatedAt and WHERE clause
        
        return query, parameters

    def shouldResetCredit(self, service: ServiceCredentials) -> bool:
        return service.reset_duration_days and service.requires_credits