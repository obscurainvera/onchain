from config.Config import get_config
from decimal import Decimal
from typing import Dict, List, Optional
from datetime import datetime
import json
from database.operations.BaseDBHandler import BaseDBHandler
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from database.operations.schema import TradingAttentionInfo
from logs.logger import get_logger
import pytz
from sqlalchemy import text

logger = get_logger(__name__)

# Table Schema Documentation
SCHEMA_DOCS = {
    "tradingattention": {
        "id": "Internal unique ID",
        "tokenid": "Token's contract address",
        "name": "Trading symbol (e.g., 'KLED')",
        "score": "Trading attention score",
        "date": "Date of the attention score",
        "colour": "Color indicator (e.g., 'medium red')",
        "currentprice": "Current token price in USD",
        "createdat": "When record was created",
        "lastupdatedat": "Last data update",
    },
    "tradingattentionhistory": {
        "id": "Internal unique ID",
        "tokenid": "Token's contract address",
        "name": "Trading symbol (e.g., 'KLED')",
        "score": "Trading attention score at snapshot",
        "date": "Date of the attention score at snapshot",
        "colour": "Color indicator at snapshot",
        "currentprice": "Token price at snapshot",
        "createdat": "When record was created",
        "lastupdatedat": "Last data update at snapshot",
    },
}


class TradingAttentionHandler(BaseDBHandler):
    def __init__(self, conn_manager=None):
        if conn_manager is None:
            conn_manager = DatabaseConnectionManager()
        super().__init__(conn_manager)
        self.schema = SCHEMA_DOCS
        self._createTables()

    def _createTables(self):
        """Creates all necessary tables for the trading attention system"""
        try:
            with self.conn_manager.transaction() as cursor:
                # 1. TradingAttention Table
                cursor.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS tradingattention (
                        id SERIAL PRIMARY KEY,
                        tokenid TEXT NOT NULL UNIQUE,
                        name TEXT NOT NULL,
                        score DECIMAL NOT NULL,
                        date TEXT NOT NULL,
                        colour TEXT NOT NULL,
                        currentprice DECIMAL,
                        createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        lastupdatedat TIMESTAMP
                    )
                """
                    )
                )
                
                # Create indexes for tradingattention
                cursor.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_tradingattention_tokenid ON tradingattention (tokenid)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_tradingattention_score ON tradingattention (score DESC)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_tradingattention_date ON tradingattention (date)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_tradingattention_lastupdatedat ON tradingattention (lastupdatedat)"))

                # 2. TradingAttentionHistory Table
                cursor.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS tradingattentionhistory (
                        id SERIAL PRIMARY KEY,
                        tokenid TEXT NOT NULL,
                        name TEXT NOT NULL,
                        score DECIMAL NOT NULL,
                        date TEXT NOT NULL,
                        colour TEXT NOT NULL,
                        currentprice DECIMAL,
                        createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        lastupdatedat TIMESTAMP,
                        FOREIGN KEY(tokenid) REFERENCES tradingattention(tokenid)
                    )
                """
                    )
                )
                
                # Create indexes for tradingattentionhistory - CRITICAL for performance
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_tradingattentionhistory_tokenid_createdat ON tradingattentionhistory (tokenid, createdat DESC)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_tradingattentionhistory_tokenid ON tradingattentionhistory (tokenid)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_tradingattentionhistory_score ON tradingattentionhistory (score DESC)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_tradingattentionhistory_createdat ON tradingattentionhistory (createdat)"))

        except Exception as e:
            logger.error(f"Error creating tables for TradingAttentionHandler: {e}")

    def getTableDocumentation(self, tableName: str) -> dict:
        """Get documentation for a specific table"""
        return self.schema.get(tableName, {})

    def getColumnDescription(self, tableName: str, columnName: str) -> str:
        """Get description for a specific column"""
        tableSchema = self.schema.get(tableName, {})
        return tableSchema.get(columnName, "No description available")

    def getExistingTradingAttention(self, tokenId: str) -> Optional[Dict]:
        """Get current trading attention data if exists"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                        """
                    SELECT * FROM tradingattention 
                    WHERE tokenid = %s
                """
                    ),
                    (tokenId,),
                )
                result = cursor.fetchone()
                if result:
                    return dict(result)
                return None
        except Exception as e:
            logger.error(f"Error retrieving existing trading attention for {tokenId}: {e}")
            return None

    def insertTradingAttentionData(self, tradingAttentionToken: 'TradingAttentionInfo') -> bool:
        """
        Insert or update trading attention data into the database
        
        Args:
            tradingAttentionToken: TradingAttentionInfo object containing token data
            
        Returns:
            bool: Success status
        """
        try:
            # Convert datetime objects to IST timezone
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            # Check if token already exists in tradingattention
            existingData = self.getExistingTradingAttention(tradingAttentionToken.tokenid)
            
            if existingData:
                # Save current data to history before updating
                with self.conn_manager.transaction() as cursor:
                    cursor.execute(
                        text(
                            """
                        INSERT INTO tradingattentionhistory
                        (tokenid, name, score, date, colour, currentprice, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        ),
                        (
                            existingData["tokenid"],
                            existingData["name"],
                            existingData["score"],
                            existingData["date"],
                            existingData["colour"],
                            existingData["currentprice"],
                            existingData["createdat"],
                            existingData["lastupdatedat"],
                        ),
                    )
                
                # Update existing trading attention data
                with self.conn_manager.transaction() as cursor:
                    cursor.execute(
                        text(
                            """
                        UPDATE tradingattention
                        SET name = %s,
                            score = %s,
                            date = %s,
                            colour = %s,
                            currentprice = %s,
                            lastupdatedat = %s
                        WHERE tokenid = %s
                        """
                        ),
                        (
                            tradingAttentionToken.name,
                            tradingAttentionToken.score,
                            tradingAttentionToken.date,
                            tradingAttentionToken.colour,
                            tradingAttentionToken.currentprice,
                            now,
                            tradingAttentionToken.tokenid,
                        ),
                    )
                
                logger.info(f"Updated trading attention data for token {tradingAttentionToken.tokenid}")
            else:
                # Insert new trading attention data
                with self.conn_manager.transaction() as cursor:
                    cursor.execute(
                        text(
                            """
                        INSERT INTO tradingattention
                        (tokenid, name, score, date, colour, currentprice, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        ),
                        (
                            tradingAttentionToken.tokenid,
                            tradingAttentionToken.name,
                            tradingAttentionToken.score,
                            tradingAttentionToken.date,
                            tradingAttentionToken.colour,
                            tradingAttentionToken.currentprice,
                            now,
                            now,
                        ),
                    )
                
                logger.info(f"Inserted new trading attention data for token {tradingAttentionToken.tokenid}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error inserting/updating trading attention data: {e}")
            return False

    def getTradingAttentionData(self, tokenId: str) -> Optional[Dict]:
        """
        Get complete trading attention data for a token
        
        Args:
            tokenId: Token ID to retrieve
            
        Returns:
            Dict: Trading attention data or None if not found
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                        """
                    SELECT * FROM tradingattention
                    WHERE tokenid = %s
                    """
                    ),
                    (tokenId,),
                )
                result = cursor.fetchone()
                if result:
                    return dict(result)
                return None
        except Exception as e:
            logger.error(f"Error retrieving trading attention data: {e}")
            return None

    def getTopTradingAttentionTokens(self, limit: int = 100) -> List[Dict]:
        """
        Get top trading attention tokens based on score
        
        Args:
            limit: Maximum number of tokens to return
            
        Returns:
            List[Dict]: List of trading attention data sorted by score
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                        """
                    SELECT * FROM tradingattention
                    ORDER BY score DESC
                    LIMIT %s
                    """
                    ),
                    (limit,),
                )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error retrieving top trading attention tokens: {e}")
            return []

    def getTradingAttentionHistory(self, tokenId: str, limit: int = 100) -> List[Dict]:
        """
        Get historical trading attention data for a specific token
        
        Args:
            tokenId: Token ID to retrieve history for
            limit: Maximum number of history records to return
            
        Returns:
            List[Dict]: List of historical trading attention data
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                        """
                    SELECT * FROM tradingattentionhistory
                    WHERE tokenid = %s
                    ORDER BY createdat DESC
                    LIMIT %s
                    """
                    ),
                    (tokenId, limit),
                )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error retrieving trading attention history: {e}")
            return []

    def getBatchTradingAttentionData(self, tokenIds: List[str]) -> Dict[str, Dict]:
        """
        Get trading attention data for multiple token IDs at once - THIS IS THE BATCH RETRIEVAL FUNCTION
        
        Args:
            tokenIds: List of token IDs to retrieve
            
        Returns:
            Dict[str, Dict]: Dictionary mapping token IDs to their trading attention data
        """
        try:
            if not tokenIds:
                return {}
            
            with self.conn_manager.transaction() as cursor:
                return self._getBatchTradingAttentionDataWithCursor(cursor, tokenIds)
            
        except Exception as e:
            logger.error(f"Error retrieving batch trading attention data: {e}")
            return {}

    def _getBatchTradingAttentionDataWithCursor(self, cursor, tokenIds: List[str]) -> Dict[str, Dict]:
        """
        Internal method to get batch trading attention data using an existing cursor
        
        Args:
            cursor: Database cursor to use
            tokenIds: List of token IDs to retrieve
            
        Returns:
            Dict[str, Dict]: Dictionary mapping token IDs to their trading attention data
        """
        if not tokenIds:
            return {}
        
        config = get_config()
        
        if config.DB_TYPE == 'postgres':
            # PostgreSQL: Use ANY for optimal performance
            cursor.execute(
                text("""
                SELECT * FROM tradingattention 
                WHERE tokenid = ANY(%s)
                """),
                (tokenIds,)
            )
        else:
            # SQLite: Use IN clause
            placeholders = ','.join(['?' for _ in tokenIds])
            cursor.execute(
                text(f"""
                SELECT * FROM tradingattention 
                WHERE tokenid IN ({placeholders})
                """),
                tokenIds
            )
        
        results = cursor.fetchall()
        return {row['tokenid']: dict(row) for row in results}

    def batchInsertTradingAttentionTokens(self, tradingAttentionTokens: List['TradingAttentionInfo']) -> List['TradingAttentionInfo']:
        """
        Ultimate 2-call optimization: All operations in single mega CTE
        
        Performance: Only 2 database calls regardless of token count:
        1. Get existing data in single query
        2. Mega CTE: history inserts + main table upserts
        
        Args:
            tradingAttentionTokens: List of TradingAttentionInfo objects to persist
            
        Returns:
            List[TradingAttentionInfo]: List of successfully persisted tokens
        """
        if not tradingAttentionTokens:
            return []
            
        try:
            # Convert datetime objects to IST timezone
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            config = get_config()
            successfulTokens = []
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    # PostgreSQL: Ultimate 2-call optimization
                    tokenIds = [token.tokenid for token in tradingAttentionTokens]
                    
                    # Step 1: Get ALL existing data in single query using dedicated batch function
                    existingDataMap = self._getBatchTradingAttentionDataWithCursor(cursor, tokenIds)
                    
                    # Step 2: Ultimate mega-query - All operations in single CTE
                    # Build VALUES strings for all operations
                    historyValues = []
                    upsertValues = []
                    
                    for token in tradingAttentionTokens:
                        existing = existingDataMap.get(token.tokenid)
                        
                        # Escape strings properly for SQL injection prevention
                        escaped_tokenid = token.tokenid.replace("'", "''") if isinstance(token.tokenid, str) else str(token.tokenid)
                        escaped_name = token.name.replace("'", "''") if isinstance(token.name, str) else str(token.name)
                        escaped_date = token.date.replace("'", "''") if isinstance(token.date, str) else str(token.date)
                        escaped_colour = token.colour.replace("'", "''") if isinstance(token.colour, str) else str(token.colour)
                        
                        # Add history record if existing
                        if existing:
                            existing_name = existing['name'].replace("'", "''") if isinstance(existing['name'], str) else str(existing['name'])
                            existing_date = existing['date'].replace("'", "''") if isinstance(existing['date'], str) else str(existing['date'])
                            existing_colour = existing['colour'].replace("'", "''") if isinstance(existing['colour'], str) else str(existing['colour'])
                            existing_price = existing['currentprice'] if existing['currentprice'] else 'NULL'
                            historyValues.append(f"('{escaped_tokenid}', '{existing_name}', {existing['score']}, '{existing_date}', '{existing_colour}', {existing_price}, TIMESTAMP '{now}', TIMESTAMP '{now}')")
                        
                        # Prepare upsert
                        currentprice_str = str(token.currentprice) if token.currentprice else 'NULL'
                        upsertValues.append(f"('{escaped_tokenid}', '{escaped_name}', {token.score}, '{escaped_date}', '{escaped_colour}', {currentprice_str}, TIMESTAMP '{now}', TIMESTAMP '{now}')")
                    
                    # Ultimate single mega-query with all operations
                    mega_query = f"""
                    WITH history_inserts AS (
                        {f"INSERT INTO tradingattentionhistory (tokenid, name, score, date, colour, currentprice, createdat, lastupdatedat) VALUES {','.join(historyValues)} RETURNING tokenid" if historyValues else "SELECT NULL::text as tokenid WHERE false"}
                    ),
                    main_upserts AS (
                        INSERT INTO tradingattention (tokenid, name, score, date, colour, currentprice, createdat, lastupdatedat)
                        VALUES {','.join(upsertValues)}
                        ON CONFLICT (tokenid) DO UPDATE SET
                            name = EXCLUDED.name,
                            score = EXCLUDED.score,
                            date = EXCLUDED.date,
                            colour = EXCLUDED.colour,
                            currentprice = EXCLUDED.currentprice,
                            lastupdatedat = EXCLUDED.lastupdatedat
                        RETURNING tokenid
                    )
                    SELECT 
                        (SELECT COUNT(*) FROM history_inserts) as history_count,
                        (SELECT COUNT(*) FROM main_upserts) as upsert_count;
                    """
                    
                    cursor.execute(text(mega_query))
                    result = cursor.fetchone()
                    
                    logger.info(f"Mega-query processed: {result['history_count'] if result else 0} history, {result['upsert_count'] if result else 0} upserts")
                    successfulTokens = tradingAttentionTokens
                    
                else:
                    # SQLite: Optimized batch approach for SQLite
                    tokenIds = [token.tokenid for token in tradingAttentionTokens]
                    
                    # Get ALL existing data in one query using dedicated batch function
                    existingDataMap = self._getBatchTradingAttentionDataWithCursor(cursor, tokenIds)
                    
                    # Prepare batch operations
                    historyData = []
                    upsertData = []
                    
                    for token in tradingAttentionTokens:
                        existing = existingDataMap.get(token.tokenid)
                        
                        if existing:
                            # Add to history
                            historyData.append((
                                existing['tokenid'], existing['name'], existing['score'],
                                existing['date'], existing['colour'], existing['currentprice'],
                                existing['createdat'], existing['lastupdatedat']
                            ))
                        
                        # Prepare upsert
                        upsertData.append((
                            token.tokenid, token.name, token.score, token.date,
                            token.colour, token.currentprice, now, now
                        ))
                    
                    # Execute batch operations
                    if historyData:
                        cursor.executemany(
                            text("""
                            INSERT INTO tradingattentionhistory 
                            (tokenid, name, score, date, colour, currentprice, createdat, lastupdatedat)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """),
                            historyData
                        )
                    
                    if upsertData:
                        cursor.executemany(
                            text("""
                            INSERT OR REPLACE INTO tradingattention 
                            (tokenid, name, score, date, colour, currentprice, createdat, lastupdatedat)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """),
                            upsertData
                        )
                    
                    successfulTokens = tradingAttentionTokens
            
            logger.info(f"Ultimate 2-call optimization processed {len(successfulTokens)} tokens successfully")
            return successfulTokens
            
        except Exception as e:
            logger.error(f"Error in ultimate 2-call batch token insertion: {str(e)}")
            return [] 