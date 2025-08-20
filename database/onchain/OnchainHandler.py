from config.Config import get_config
from decimal import Decimal
from typing import Dict, List, Optional
from datetime import datetime
import json
from database.operations.BaseDBHandler import BaseDBHandler
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from database.operations.schema import OnchainInfo
from logs.logger import get_logger
import pytz
from sqlalchemy import text

logger = get_logger(__name__)

# Table Schema Documentation
SCHEMA_DOCS = {
    "onchaininfo": {
        "id": "Internal unique ID",
        "tokenid": "Token's contract address",
        "name": "Trading symbol (e.g., 'ROME')",
        "chain": "Blockchain (e.g., 'SOL')",
        "count": "Count of updates",
        "createdat": "When record was created",
        "updatedat": "Last data update",
    },
    "onchainstate": {
        "id": "Internal unique ID",
        "onchaininfoid": "Reference to onchaininfo.id",
        "tokenid": "Token's contract address",
        "price": "Current token price in USD",
        "marketcap": "Total market capitalization",
        "liquidity": "Available trading liquidity",
        "makers": "Number of makers",
        "price1h": "1-hour price change",
        "rank": "Ranking based on change_pct_1h",
        "age": "Token age",
        "createdat": "When record was created",
        "updatedat": "Last state update timestamp",
    },
    "onchainhistory": {
        "id": "Internal unique ID",
        "onchainstateid": "Reference to onchainstate.id",
        "tokenid": "Token's contract address",
        "price": "Token price at snapshot",
        "marketcap": "Market cap at snapshot",
        "liquidity": "Liquidity at snapshot",
        "makers": "Number of makers at snapshot",
        "price1h": "1-hour price change at snapshot",
        "rank": "Ranking at snapshot",
        "age": "Token age at snapshot",
        "createdat": "When record was created",
    },
}


class OnchainHandler(BaseDBHandler):
    def __init__(self, conn_manager=None):
        if conn_manager is None:
            conn_manager = DatabaseConnectionManager()
        super().__init__(conn_manager)
        self.schema = SCHEMA_DOCS
        self._createTables()

    def _createTables(self):
        """Creates all necessary tables for the onchain information system"""
        try:
            with self.conn_manager.transaction() as cursor:
                # 1. Base Token Information
                cursor.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS onchaininfo (
                        id SERIAL PRIMARY KEY,
                        tokenid TEXT NOT NULL UNIQUE,
                        name TEXT NOT NULL,
                        chain TEXT NOT NULL,
                        count INTEGER DEFAULT 1,
                        createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updatedat TIMESTAMP
                    )
                """
                    )
                )
                
                # Create indexes for onchaininfo
                cursor.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_onchaininfo_tokenid ON onchaininfo (tokenid)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchaininfo_chain ON onchaininfo (chain)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchaininfo_updatedat ON onchaininfo (updatedat)"))

                # 2. Token Current State
                cursor.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS onchainstate (
                        id SERIAL PRIMARY KEY,
                        onchaininfoid INTEGER NOT NULL,
                        tokenid TEXT NOT NULL UNIQUE,
                        price DECIMAL NOT NULL,
                        marketcap DECIMAL NOT NULL,
                        liquidity DECIMAL NOT NULL,
                        makers INTEGER NOT NULL,
                        price1h DECIMAL NOT NULL,
                        rank INTEGER NOT NULL,
                        age TEXT,
                        createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updatedat TIMESTAMP,
                        FOREIGN KEY(onchaininfoid) REFERENCES onchaininfo(id),
                        FOREIGN KEY(tokenid) REFERENCES onchaininfo(tokenid)
                    )
                """
                    )
                )
                
                # Create indexes for onchainstate
                cursor.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_onchainstate_tokenid ON onchainstate (tokenid)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchainstate_onchaininfoid ON onchainstate (onchaininfoid)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchainstate_rank ON onchainstate (rank)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchainstate_updatedat ON onchainstate (updatedat)"))

                # 3. Token History
                cursor.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS onchainhistory (
                        id SERIAL PRIMARY KEY,
                        onchainstateid INTEGER NOT NULL,
                        tokenid TEXT NOT NULL,
                        price DECIMAL NOT NULL,
                        marketcap DECIMAL NOT NULL,
                        liquidity DECIMAL NOT NULL,
                        makers INTEGER NOT NULL,
                        price1h DECIMAL NOT NULL,
                        rank INTEGER NOT NULL,
                        age TEXT,
                        createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(onchainstateid) REFERENCES onchainstate(id),
                        FOREIGN KEY(tokenid) REFERENCES onchaininfo(tokenid)
                    )
                """
                    )
                )
                
                # Create indexes for onchainhistory - CRITICAL for performance
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchainhistory_tokenid_createdat ON onchainhistory (tokenid, createdat DESC)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchainhistory_tokenid ON onchainhistory (tokenid)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchainhistory_onchainstateid ON onchainhistory (onchainstateid)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchainhistory_rank ON onchainhistory (rank)"))
                cursor.execute(text("CREATE INDEX IF NOT EXISTS idx_onchainhistory_createdat ON onchainhistory (createdat)"))

            # Create indices in separate transactions
            self._createIndex(
                "idx_onchaininfo_tokenid", "onchaininfo", "tokenid"
            )
            self._createIndex(
                "idx_onchainstate_tokenid", "onchainstate", "tokenid"
            )
            self._createIndex(
                "idx_onchainhistory_tokenid", "onchainhistory", "tokenid"
            )

        except Exception as e:
            logger.error(f"Error creating tables for OnchainHandler: {e}")

    def _createIndex(self, index_name, table_name, column_name):
        """Create an index safely in its own transaction"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_name})"
                    )
                )
        except Exception as e:
            logger.error(f"Error creating index {index_name}: {e}")

    def getTableDocumentation(self, tableName: str) -> dict:
        """Get documentation for a specific table"""
        return self.schema.get(tableName, {})

    def getColumnDescription(self, tableName: str, columnName: str) -> str:
        """Get description for a specific column"""
        tableSchema = self.schema.get(tableName, {})
        return tableSchema.get(columnName, "No description available")

    def getExistingTokenInfo(self, tokenId: str) -> Optional[Dict]:
        """Get current token info if exists"""
        with self.conn_manager.transaction() as cursor:
            cursor.execute(
                text(
                    """
                SELECT * FROM onchaininfo 
                WHERE tokenid = %s
            """
                ),
                (tokenId,),
            )
            result = cursor.fetchone()
            if result:
                return dict(result)
            return None

    def getExistingTokenState(self, tokenId: str) -> Optional[Dict]:
        """Get current token state if exists"""
        with self.conn_manager.transaction() as cursor:
            cursor.execute(
                text(
                    """
                SELECT * FROM onchainstate 
                WHERE tokenid = %s
            """
                ),
                (tokenId,),
            )
            result = cursor.fetchone()
            if result:
                return dict(result)
            return None

    def insertTokenData(self, onchainToken: 'OnchainInfo') -> bool:
        """
        Insert or update token data into the database
        
        Args:
            onchainToken: OnchainInfo object containing token data
            
        Returns:
            bool: Success status
        """
        try:
            # Convert datetime objects to IST timezone
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
    
            
            # First, check if token already exists in onchaininfo
            existingInfo = self.getExistingTokenInfo(onchainToken.tokenid)
            
            if existingInfo:
                # Update existing token info
                with self.conn_manager.transaction() as cursor:
                    cursor.execute(
                        text(
                            """
                        UPDATE onchaininfo
                        SET count = count + 1,
                            updatedat = %s
                        WHERE tokenid = %s
                        RETURNING id
                        """
                        ),
                        (now, onchainToken.tokenid),
                    )
                    result = cursor.fetchone()
                    onchainInfoId = result["id"]
            else:
                # Insert new token info
                with self.conn_manager.transaction() as cursor:
                    cursor.execute(
                        text(
                            """
                        INSERT INTO onchaininfo
                        (tokenid, name, chain, createdat, updatedat)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                        """
                        ),
                        (
                            onchainToken.tokenid,
                            onchainToken.name,
                            onchainToken.chain,
                            now,
                            now,
                        ),
                    )
                    result = cursor.fetchone()
                    onchainInfoId = result["id"]
            
            logger.info(f"Token {onchainToken.tokenid} info persisted at time {datetime.now()}")
            
            # Check if token state exists
            existingState = self.getExistingTokenState(onchainToken.tokenid)
            
            if existingState:
                # Save current state to history
                with self.conn_manager.transaction() as cursor:
                    cursor.execute(
                        text(
                            """
                        INSERT INTO onchainhistory
                        (onchainstateid, tokenid, price, marketcap, liquidity, makers, price1h, rank, age, createdat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        ),
                        (
                            existingState["id"],
                            existingState["tokenid"],
                            existingState["price"],
                            existingState["marketcap"],
                            existingState["liquidity"],
                            existingState["makers"],
                            existingState["price1h"],
                            existingState["rank"],
                            existingState["age"],
                            now,
                        ),
                    )
                
                # Update token state
                with self.conn_manager.transaction() as cursor:
                    cursor.execute(
                        text(
                            """
                        UPDATE onchainstate
                        SET price = %s,
                            marketcap = %s,
                            liquidity = %s,
                            makers = %s,
                            price1h = %s,
                            rank = %s,
                            age = %s,
                            updatedat = %s
                        WHERE tokenid = %s
                        """
                        ),
                        (
                            onchainToken.price,
                            onchainToken.marketcap,
                            onchainToken.liquidity,
                            onchainToken.makers,
                            onchainToken.price1h,
                            onchainToken.rank,
                            onchainToken.age,
                            now,
                            onchainToken.tokenid,
                        ),
                    )
            else:
                # Insert new token state
                with self.conn_manager.transaction() as cursor:
                    cursor.execute(
                        text(
                            """
                        INSERT INTO onchainstate
                        (onchaininfoid, tokenid, price, marketcap, liquidity, makers, price1h, rank, age, createdat, updatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        ),
                        (
                            onchainInfoId,
                            onchainToken.tokenid,
                            onchainToken.price,
                            onchainToken.marketcap,
                            onchainToken.liquidity,
                            onchainToken.makers,
                            onchainToken.price1h,
                            onchainToken.rank,
                            onchainToken.age,
                            now,
                            now,
                        ),
                    )
            
            logger.info(f"Token {onchainToken.tokenid} state persisted at time {datetime.now()}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error inserting token data: {e}")
            return False

    def getTokenState(self, tokenId: str) -> Optional[Dict]:
        """
        Get complete token state including info and current state
        
        Args:
            tokenId: Token ID to retrieve
            
        Returns:
            Dict: Combined token info and state or None if not found
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                        """
                    SELECT i.*, s.*
                    FROM onchaininfo i
                    JOIN onchainstate s ON i.tokenid = s.tokenid
                    WHERE i.tokenid = %s
                    """
                    ),
                    (tokenId,),
                )
                result = cursor.fetchone()
                if result:
                    return dict(result)
                return None
        except Exception as e:
            logger.error(f"Error retrieving token state: {e}")
            return None

    def getTopRankedTokens(self, limit: int = 100) -> List[Dict]:
        """
        Get top ranked tokens based on rank
        
        Args:
            limit: Maximum number of tokens to return
            
        Returns:
            List[Dict]: List of token states sorted by rank
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                        """
                    SELECT i.*, s.*
                    FROM onchaininfo i
                    JOIN onchainstate s ON i.tokenid = s.tokenid
                    ORDER BY s.rank ASC
                    LIMIT %s
                    """
                    ),
                    (limit,),
                )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error retrieving top ranked tokens: {e}")
            return []

    def getTokenHistory(self, tokenId: str, limit: int = 100) -> List[Dict]:
        """
        Get historical data for a specific token
        
        Args:
            tokenId: Token ID to retrieve history for
            limit: Maximum number of history records to return
            
        Returns:
            List[Dict]: List of historical token states
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                        """
                    SELECT *
                    FROM onchainhistory
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
            logger.error(f"Error retrieving token history: {e}")
            return []
        
        
    def getOnchainInfoTokens(self, tokenIds: List[str]) -> List[Dict]:
        """
        Get token information for multiple token IDs from onchaininfo table
    
        Args:
            tokenIds: List of token IDs to retrieve
        
        Returns:
            List[Dict]: List of token information dictionaries
        """
        try:
            if not tokenIds:
                return []
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text(
                     """
                        SELECT * FROM onchaininfo 
                        WHERE tokenid IN %s
                        """
                    ),
                    (tuple(tokenIds),),
                )
                results = cursor.fetchall()
                return {row['tokenid']: dict(row) for row in results}
            
        except Exception as e:
            logger.error(f"Error retrieving multiple token info: {e}")
            return []
    
    def getTokenHistoricalData(self, tokenid: str, hours: int = 24) -> List[Dict]:
        """
        Get historical data for a token within the specified time window
        
        Args:
            tokenid: Token ID to get history for
            hours: Number of hours to look back
            
        Returns:
            List[Dict]: Historical data sorted by creation time (newest first)
        """
        try:
            config = get_config()
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    cursor.execute(
                        text(f"""
                        SELECT * FROM onchainhistory 
                        WHERE tokenid = %s 
                        AND createdat >= NOW() - INTERVAL '{hours} HOUR'
                        ORDER BY createdat DESC
                        """),
                        (tokenid,)
                    )
                else:
                    cursor.execute(
                        text("""
                        SELECT * FROM onchainhistory 
                        WHERE tokenid = ? 
                        AND createdat >= datetime('now', '-' || ? || ' hours')
                        ORDER BY createdat DESC
                        """),
                        (tokenid, hours)
                    )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting historical data for token {tokenid}: {str(e)}")
            return []
    
    def getBatchTokenHistoricalData(self, tokenids: List[str], hours: int = 24) -> Dict[str, List[Dict]]:
        """
        Get historical data for multiple tokens within the specified time window
        Optimized batch query to reduce database hits
        
        Args:
            tokenids: List of token IDs to get history for
            hours: Number of hours to look back
            
        Returns:
            Dict[str, List[Dict]]: Dictionary mapping token IDs to their historical data
        """
        try:
            if not tokenids:
                return {}
                
            config = get_config()
            result_map = {}
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    cursor.execute(
                        text(f"""
                        SELECT * FROM onchainhistory 
                        WHERE tokenid = ANY(%s) 
                        AND createdat >= NOW() - INTERVAL '{hours} HOUR'
                        ORDER BY tokenid, createdat DESC
                        """),
                        (tokenids,)
                    )
                else:
                    placeholders = ','.join(['?' for _ in tokenids])
                    cursor.execute(
                        text(f"""
                        SELECT * FROM onchainhistory 
                        WHERE tokenid IN ({placeholders})
                        AND createdat >= datetime('now', '-' || ? || ' hours')
                        ORDER BY tokenid, createdat DESC
                        """),
                        (*tokenids, hours)
                    )
                
                results = cursor.fetchall()
                
                # Group results by token ID
                for row in results:
                    token_id = row['tokenid']
                    if token_id not in result_map:
                        result_map[token_id] = []
                    result_map[token_id].append(dict(row))
                
                # Ensure all requested tokens have an entry (even if empty)
                for token_id in tokenids:
                    if token_id not in result_map:
                        result_map[token_id] = []
                        
            logger.info(f"Retrieved batch historical data for {len(tokenids)} tokens over {hours} hours")
            return result_map
            
        except Exception as e:
            logger.error(f"Error getting batch historical data: {str(e)}")
            return {token_id: [] for token_id in tokenids}

    def batchInsertTokens(self, onchainTokens: List['OnchainInfo']) -> List['OnchainInfo']:
        """
        Ultimate 2-call optimization: All operations in single mega CTE
        
        Performance: Only 2 database calls regardless of token count:
        1. Get existing data with JOIN
        2. Mega CTE: history inserts + info upserts + state upserts
        
        Args:
            onchainTokens: List of OnchainInfo objects to persist
            
        Returns:
            List[OnchainInfo]: List of successfully persisted tokens
        """
        if not onchainTokens:
            return []
            
        try:
            # Convert datetime objects to IST timezone
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            config = get_config()
            
            # Prepare token data for the single query
            tokenData = []
            for token in onchainTokens:
                tokenData.append((
                    token.tokenid,
                    token.name,
                    token.chain,
                    token.price,
                    token.marketcap,
                    token.liquidity,
                    token.makers,
                    token.price1h,
                    token.rank,
                    token.age,
                    now,
                    now
                ))
            
            successfulTokens = []
            
            with self.conn_manager.transaction() as cursor:
                if config.DB_TYPE == 'postgres':
                    # PostgreSQL: Optimized approach with minimal queries
                    tokenIds = [token.tokenid for token in onchainTokens]
                    
                    # Step 1: Get existing data in single query
                    cursor.execute(
                        text("""
                        SELECT 
                            i.id as info_id, 
                            i.tokenid, 
                            i.count,
                            s.id as state_id,
                            s.price,
                            s.marketcap,
                            s.liquidity,
                            s.makers,
                            s.price1h,
                            s.rank,
                            s.age
                        FROM onchaininfo i
                        LEFT JOIN onchainstate s ON i.tokenid = s.tokenid
                        WHERE i.tokenid = ANY(%s)
                        """),
                        (tokenIds,)
                    )
                    
                    existingDataMap = {}
                    for row in cursor.fetchall():
                        existingDataMap[row['tokenid']] = dict(row)
                    
                    # Step 2: Ultimate mega-query - All operations in single CTE
                    # Build VALUES strings for all operations
                    historyValues = []
                    infoValues = []
                    stateValues = []
                    
                    for token in onchainTokens:
                        existing = existingDataMap.get(token.tokenid)
                        count = (existing['count'] + 1) if existing else 1
                        
                        # Escape strings properly for SQL injection prevention
                        escaped_tokenid = token.tokenid.replace("'", "''") if isinstance(token.tokenid, str) else str(token.tokenid)
                        escaped_name = token.name.replace("'", "''") if isinstance(token.name, str) else str(token.name)
                        escaped_chain = token.chain.replace("'", "''") if isinstance(token.chain, str) else str(token.chain)
                        # Age is an integer, just convert to string
                        escaped_age = str(token.age) if token.age is not None else ''
                        
                        # Add history record if state exists
                        if existing and existing['state_id']:
                            old_age = existing.get('age')
                            # Age is an integer, just convert to string
                            escaped_old_age = str(old_age) if old_age is not None else ''
                            historyValues.append(f"({existing['state_id']}, '{escaped_tokenid}', {existing['price']}, {existing['marketcap']}, {existing['liquidity']}, {existing['makers']}, {existing['price1h']}, {existing['rank']}, '{escaped_old_age}', TIMESTAMP '{now}')")
                        
                        infoValues.append(f"('{escaped_tokenid}', '{escaped_name}', '{escaped_chain}', {count}, TIMESTAMP '{now}', TIMESTAMP '{now}')")
                        stateValues.append(f"('{escaped_tokenid}', {token.price}, {token.marketcap}, {token.liquidity}, {token.makers}, {token.price1h}, {token.rank}, '{escaped_age}', TIMESTAMP '{now}', TIMESTAMP '{now}')")
                    
                    # Ultimate single mega-query with all operations in correct order
                    mega_query = f"""
                    WITH history_inserts AS (
                        {f"INSERT INTO onchainhistory (onchainstateid, tokenid, price, marketcap, liquidity, makers, price1h, rank, age, createdat) VALUES {','.join(historyValues)} RETURNING onchainstateid" if historyValues else "SELECT NULL::integer as onchainstateid WHERE false"}
                    ),
                    info_upserts AS (
                        INSERT INTO onchaininfo (tokenid, name, chain, count, createdat, updatedat)
                        VALUES {','.join(infoValues)}
                        ON CONFLICT (tokenid) DO UPDATE SET
                            count = EXCLUDED.count,
                            updatedat = EXCLUDED.updatedat
                        RETURNING id, tokenid
                    ),
                    all_info AS (
                        SELECT id, tokenid FROM info_upserts
                        UNION ALL
                        SELECT id, tokenid FROM onchaininfo 
                        WHERE tokenid = ANY(%s) AND tokenid NOT IN (SELECT tokenid FROM info_upserts)
                    ),
                    state_upserts AS (
                        INSERT INTO onchainstate 
                        (onchaininfoid, tokenid, price, marketcap, liquidity, makers, price1h, rank, age, createdat, updatedat)
                        SELECT 
                            ai.id,
                            sv.tokenid,
                            sv.price::decimal,
                            sv.marketcap::decimal, 
                            sv.liquidity::decimal,
                            sv.makers::integer,
                            sv.price1h::decimal,
                            sv.rank::integer,
                            sv.age,
                            sv.createdat::timestamp,
                            sv.updatedat::timestamp
                        FROM (VALUES {','.join(stateValues)}) AS sv(tokenid, price, marketcap, liquidity, makers, price1h, rank, age, createdat, updatedat)
                        JOIN all_info ai ON ai.tokenid = sv.tokenid
                        ON CONFLICT (tokenid) DO UPDATE SET
                            price = EXCLUDED.price,
                            marketcap = EXCLUDED.marketcap,
                            liquidity = EXCLUDED.liquidity,
                            makers = EXCLUDED.makers,
                            price1h = EXCLUDED.price1h,
                            rank = EXCLUDED.rank,
                            age = EXCLUDED.age,
                            updatedat = EXCLUDED.updatedat
                        RETURNING tokenid
                    )
                    SELECT 
                        (SELECT COUNT(*) FROM history_inserts) as history_count,
                        (SELECT COUNT(*) FROM info_upserts) as info_count,
                        (SELECT COUNT(*) FROM state_upserts) as state_count;
                    """
                    
                    cursor.execute(text(mega_query), (tokenIds,))
                    result = cursor.fetchone()
                    processed_count = len(onchainTokens)
                    
                    logger.info(f"Mega-query processed: {result['history_count'] if result else 0} history, {result['info_count'] if result else 0} info, {result['state_count'] if result else 0} state records")
                    
                else:
                    # SQLite: Separate queries (SQLite doesn't support complex CTEs as well)
                    # First get existing data
                    tokenIds = [token.tokenid for token in onchainTokens]
                    cursor.execute(
                        text(f"""
                        SELECT 
                            i.id as info_id, 
                            i.tokenid, 
                            i.count,
                            s.id as state_id,
                            s.price,
                            s.marketcap,
                            s.liquidity,
                            s.makers,
                            s.price1h,
                            s.rank,
                            s.age
                        FROM onchaininfo i
                        LEFT JOIN onchainstate s ON i.tokenid = s.tokenid
                        WHERE i.tokenid IN ({','.join(['?' for _ in tokenIds])})
                        """),
                        tokenIds
                    )
                    
                    existingDataMap = {}
                    for row in cursor.fetchall():
                        existingDataMap[row['tokenid']] = dict(row)
                    
                    # Prepare batch operations
                    historyData = []
                    infoData = []
                    stateData = []
                    
                    for token in onchainTokens:
                        existing = existingDataMap.get(token.tokenid)
                        
                        if existing and existing['state_id']:
                            # Add to history
                            historyData.append((
                                existing['state_id'], token.tokenid,
                                existing['price'], existing['marketcap'], existing['liquidity'],
                                existing['makers'], existing['price1h'], existing['rank'],
                                existing['age'], now
                            ))
                        
                        # Prepare info upsert
                        count = (existing['count'] + 1) if existing else 1
                        infoData.append((token.tokenid, token.name, token.chain, count, now, now))
                        
                        # Prepare state upsert (will get info_id after info upsert)
                        stateData.append((
                            token.tokenid, token.price, token.marketcap, token.liquidity,
                            token.makers, token.price1h, token.rank, token.age, now, now
                        ))
                    
                    # Execute in order: history -> info -> state
                    if historyData:
                        cursor.executemany(
                            text("""
                            INSERT INTO onchainhistory 
                            (onchainstateid, tokenid, price, marketcap, liquidity, makers, price1h, rank, age, createdat)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """),
                            historyData
                        )
                    
                    # Upsert info
                    cursor.executemany(
                        text("""
                        INSERT OR REPLACE INTO onchaininfo (tokenid, name, chain, count, createdat, updatedat)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """),
                        infoData
                    )
                    
                    # Get info IDs
                    cursor.execute(
                        text(f"""
                        SELECT id, tokenid FROM onchaininfo 
                        WHERE tokenid IN ({','.join(['?' for _ in tokenIds])})
                        """),
                        tokenIds
                    )
                    infoIdMap = {row['tokenid']: row['id'] for row in cursor.fetchall()}
                    
                    # Prepare final state data with info IDs
                    finalStateData = []
                    for i, (tokenid, price, marketcap, liquidity, makers, price1h, rank, age, createdat, updatedat) in enumerate(stateData):
                        info_id = infoIdMap.get(tokenid)
                        if info_id:
                            finalStateData.append((
                                info_id, tokenid, price, marketcap, liquidity,
                                makers, price1h, rank, age, createdat, updatedat
                            ))
                    
                    # Upsert state
                    if finalStateData:
                        cursor.executemany(
                            text("""
                            INSERT OR REPLACE INTO onchainstate 
                            (onchaininfoid, tokenid, price, marketcap, liquidity, makers, price1h, rank, age, createdat, updatedat)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """),
                            finalStateData
                        )
                    
                    processed_count = len(finalStateData)
                
                successfulTokens = onchainTokens[:processed_count]
            
            logger.info(f"Ultimate 2-call optimization processed {len(successfulTokens)} tokens successfully")
            return successfulTokens
            
        except Exception as e:
            logger.error(f"Error in ultimate 2-call batch token insertion: {str(e)}")
            return []
