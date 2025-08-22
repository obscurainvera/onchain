from config.Config import get_config
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import json
from database.operations.BaseDBHandler import BaseDBHandler
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from logs.logger import get_logger
import pytz
from sqlalchemy import text
from enum import IntEnum

logger = get_logger(__name__)

class AdditionSource(IntEnum):
    """Token addition source enumeration"""
    MANUAL = 1
    AUTOMATIC = 2

# Table Schema Documentation
TRADING_SCHEMA_DOCS = {
    "trackedtokens": {
        "trackedtokenid": "Internal unique ID",
        "tokenaddress": "Token contract address (44 chars)",
        "symbol": "Trading symbol (e.g., 'SOL')", 
        "name": "Full token name",
        "pairaddress": "DEX pair address for price data",
        "status": "1=active, 2=disabled",
        "enabledat": "When token was added to tracking",
        "disabledat": "When token was disabled",
        "addedby": "User who added the token",
        "disabledby": "User who disabled the token",
        "metadata": "Additional token info (JSON)"
    },
    "timeframemetadata": {
        "id": "Internal unique ID",
        "tokenaddress": "Token contract address",
        "pairaddress": "DEX pair address",
        "timeframe": "15m, 1h, or 4h",
        "nextfetchat": "When to fetch next data",
        "lastfetchedat": "Last fetch attempt time",
        "lastsuccessfullfetchat": "Last successful fetch",
        "fetchintervalseconds": "Fetch interval (default 900s)",
        "consecutivefailures": "Circuit breaker counter",
        "isactive": "Whether fetching is enabled"
    },
    "ohlcvdetails": {
        "id": "Internal unique ID",
        "timeframeid": "Reference to timeframemetadata",
        "tokenaddress": "Token contract address",
        "pairaddress": "DEX pair address", 
        "timeframe": "15m, 1h, or 4h",
        "unixtime": "Candle timestamp (Unix)",
        "timebucket": "Aggregation bucket timestamp",
        "openprice": "Opening price",
        "highprice": "Highest price in period",
        "lowprice": "Lowest price in period",
        "closeprice": "Closing price",
        "volume": "Trading volume",
        "vwapvalue": "VWAP at this candle (validation)",
        "ema21value": "EMA21 at this candle (validation)",
        "ema34value": "EMA34 at this candle (validation)",
        "iscomplete": "Whether candle data is complete",
        "datasource": "api=fetched, aggregated=derived"
    },
    "indicatorstates": {
        "tokenaddress": "Token contract address",
        "timeframe": "15m, 1h, or 4h", 
        "indicatorkey": "Indicator identifier (e.g., ema_21)",
        "currentvalue": "Current indicator value",
        "previousvalue": "Previous value for cross detection",
        "candlecount": "Number of candles processed",
        "iswarmedup": "TRUE when enough data for accuracy"
    },
    "vwapsessions": {
        "tokenaddress": "Token contract address",
        "timeframe": "15m, 1h, or 4h",
        "sessionstartunix": "Session start timestamp",
        "sessionendunix": "Session end timestamp", 
        "cumulativepv": "Cumulative price × volume",
        "cumulativevolume": "Cumulative volume",
        "currentvwap": "Current VWAP value",
        "highvwap": "Highest VWAP in session",
        "lowvwap": "Lowest VWAP in session",
        "candlecount": "Candles in this session"
    }
}


class TradingHandler(BaseDBHandler):
    def __init__(self, conn_manager=None):
        if conn_manager is None:
            conn_manager = DatabaseConnectionManager()
        super().__init__(conn_manager)
        self.schema = TRADING_SCHEMA_DOCS
        self._createTables()

    def _createTables(self):
        """Creates all necessary tables for the crypto trading system"""
        try:
            with self.conn_manager.transaction() as cursor:
                logger.info("Creating crypto trading system tables...")
                
                # Read and execute the complete schema
                schema_path = "/Users/henry-12046/Desktop/onchain/database/trading/crypto_trading_schema.sql"
                try:
                    with open(schema_path, 'r') as f:
                        schema_sql = f.read()
                    
                    # Split on semicolons and execute each statement
                    statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
                    
                    for statement in statements:
                        if statement and not statement.startswith('--'):
                            try:
                                cursor.execute(text(statement))
                            except Exception as e:
                                # Log but continue - some statements might already exist
                                logger.debug(f"Statement execution info: {e}")
                                
                except FileNotFoundError:
                    logger.warning("Schema file not found, creating basic tables")
                    self._createBasicTables(cursor)
                    
        except Exception as e:
            logger.error(f"Error creating trading tables: {e}")

    def _createBasicTables(self, cursor):
        """Create basic tables if schema file is not available"""
        
        # 1. Tracked Tokens
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS trackedtokens (
                trackedtokenid BIGSERIAL PRIMARY KEY,
                tokenaddress CHAR(44) NOT NULL UNIQUE,
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(100),
                pairaddress CHAR(44) NOT NULL,
                paircreatedtime BIGINT,
                additionsource INTEGER DEFAULT 1 CHECK (additionsource IN (1, 2)),
                status INTEGER DEFAULT 1 CHECK (status IN (1, 2)),
                enabledat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                disabledat TIMESTAMP WITH TIME ZONE,
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                addedby VARCHAR(100),
                disabledby VARCHAR(100),
                metadata JSONB
            )
        """))
        
        # 2. Timeframe Metadata
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS timeframemetadata (
                id BIGSERIAL PRIMARY KEY,
                tokenaddress CHAR(44) NOT NULL,
                pairaddress CHAR(44) NOT NULL,
                timeframe CHAR(3) NOT NULL CHECK (timeframe IN ('15m', '1h', '4h')),
                nextfetchat TIMESTAMP WITH TIME ZONE NOT NULL,
                lastfetchedat TIMESTAMP WITH TIME ZONE,
                lastsuccessfullfetchat TIMESTAMP WITH TIME ZONE,
                fetchintervalseconds INTEGER DEFAULT 900,
                consecutivefailures INTEGER DEFAULT 0,
                isactive BOOLEAN DEFAULT TRUE,
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(tokenaddress, pairaddress, timeframe)
            )
        """))
        
        # 3. OHLCV Details
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS ohlcvdetails (
                id BIGSERIAL PRIMARY KEY,
                timeframeid BIGINT NOT NULL REFERENCES timeframemetadata(id),
                tokenaddress CHAR(44) NOT NULL,
                pairaddress CHAR(44) NOT NULL,
                timeframe CHAR(3) NOT NULL,
                unixtime BIGINT NOT NULL,
                timebucket BIGINT NOT NULL,
                openprice DECIMAL(20,8) NOT NULL,
                highprice DECIMAL(20,8) NOT NULL,
                lowprice DECIMAL(20,8) NOT NULL,
                closeprice DECIMAL(20,8) NOT NULL,
                volume DECIMAL(20,4) NOT NULL,
                vwapvalue DECIMAL(20,8),
                ema21value DECIMAL(20,8),
                ema34value DECIMAL(20,8),
                iscomplete BOOLEAN DEFAULT TRUE,
                datasource VARCHAR(20) DEFAULT 'api',
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(tokenaddress, timeframe, unixtime),
                CHECK (highprice >= lowprice),
                CHECK (highprice >= openprice AND highprice >= closeprice),
                CHECK (lowprice <= openprice AND lowprice <= closeprice)
            )
        """))
        
        # 4. Indicator States
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS indicatorstates (
                tokenaddress CHAR(44),
                timeframe CHAR(3),
                indicatorkey VARCHAR(20),
                currentvalue DECIMAL(20,8),
                previousvalue DECIMAL(20,8),
                candlecount INTEGER DEFAULT 0,
                iswarmedup BOOLEAN DEFAULT FALSE,
                lastupdatedunix BIGINT,
                PRIMARY KEY (tokenaddress, timeframe, indicatorkey)
            )
        """))
        
        # 5. VWAP Sessions
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS vwapsessions (
                tokenaddress CHAR(44),
                timeframe CHAR(3),
                sessionstartunix BIGINT,
                sessionendunix BIGINT,
                cumulativepv DECIMAL(30,8),
                cumulativevolume DECIMAL(30,8),
                currentvwap DECIMAL(20,8),
                highvwap DECIMAL(20,8),
                lowvwap DECIMAL(20,8),
                lastcandleunix BIGINT,
                candlecount INTEGER DEFAULT 0,
                PRIMARY KEY (tokenaddress, timeframe, sessionstartunix)
            )
        """))
        
        # 6. Indicator Configurations
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS indicatorconfigs (
                configid SERIAL PRIMARY KEY,
                tokenaddress CHAR(44),
                timeframe CHAR(3),
                indicatortype VARCHAR(20) NOT NULL,
                parameters JSONB NOT NULL,
                configname VARCHAR(50),
                isactive BOOLEAN DEFAULT TRUE,
                priority INTEGER DEFAULT 100,
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(tokenaddress, timeframe, indicatortype, configname)
            )
        """))

    def getTableDocumentation(self, tableName: str) -> dict:
        """Get documentation for a specific table"""
        return self.schema.get(tableName, {})

    def getColumnDescription(self, tableName: str, columnName: str) -> str:
        """Get description for a specific column"""
        tableSchema = self.schema.get(tableName, {})
        return tableSchema.get(columnName, "No description available")

    # ===============================================================
    # TOKEN MANAGEMENT METHODS
    # ===============================================================
    
    def addToken(self, tokenAddress: str, symbol: str, name: str, pairAddress: str, 
                 pairCreatedTime: int = None, additionSource: AdditionSource = AdditionSource.MANUAL,
                 addedBy: str = None, metadata: dict = None) -> Optional[int]:
        """
        Add a new token to tracking
        
        Args:
            tokenAddress: Token contract address (44 chars)
            symbol: Trading symbol (e.g., 'SOL')
            name: Full token name
            pairAddress: DEX pair address for price data
            addedBy: User who added the token
            metadata: Additional token information
            
        Returns:
            int: trackedtokenid if successful, None if failed
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            with self.conn_manager.transaction() as cursor:
                # Check if token already exists
                cursor.execute(
                    text("SELECT trackedtokenid, status FROM trackedtokens WHERE tokenaddress = %s"),
                    (tokenAddress,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    if existing['status'] == 1:  # Active
                        logger.warning(f"Token {tokenAddress} is already active")
                        return None
                    else:  # Disabled - reactivate
                        cursor.execute(
                            text("""
                                UPDATE trackedtokens 
                                SET status = 1, enabledat = %s, disabledat = NULL, 
                                    lastupdatedat = %s, addedby = %s
                                WHERE tokenaddress = %s
                                RETURNING trackedtokenid
                            """),
                            (now, now, addedBy, tokenAddress)
                        )
                        result = cursor.fetchone()
                        logger.info(f"Reactivated token {tokenAddress}")
                        return result['trackedtokenid']
                
                # Insert new token
                cursor.execute(
                    text("""
                        INSERT INTO trackedtokens 
                        (tokenaddress, symbol, name, pairaddress, paircreatedtime, additionsource, addedby, metadata, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING trackedtokenid
                    """),
                    (tokenAddress, symbol, name, pairAddress, pairCreatedTime, int(additionSource), addedBy, 
                     json.dumps(metadata) if metadata else None, now, now)
                )
                result = cursor.fetchone()
                tokenId = result['trackedtokenid']
                
                # Create timeframe metadata for all timeframes
                for timeframe in ['15m', '1h', '4h']:
                    cursor.execute(
                        text("""
                            INSERT INTO timeframemetadata 
                            (tokenaddress, pairaddress, timeframe, nextfetchat, createdat, lastupdatedat)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """),
                        (tokenAddress, pairAddress, timeframe, now, now, now)
                    )
                
                logger.info(f"Added new token {tokenAddress} ({symbol}) to tracking")
                return tokenId
                
        except Exception as e:
            logger.error(f"Error adding token {tokenAddress}: {e}")
            return None

    def disableToken(self, tokenAddress: str, disabledBy: str = None, reason: str = None) -> bool:
        """
        Disable token tracking (soft delete)
        
        Args:
            tokenAddress: Token contract address
            disabledBy: User who disabled the token
            reason: Reason for disabling
            
        Returns:
            bool: Success status
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            with self.conn_manager.transaction() as cursor:
                # Update token status
                cursor.execute(
                    text("""
                        UPDATE trackedtokens 
                        SET status = 2, disabledat = %s, disabledby = %s, lastupdatedat = %s
                        WHERE tokenaddress = %s AND status = 1
                        RETURNING trackedtokenid
                    """),
                    (now, disabledBy, now, tokenAddress)
                )
                result = cursor.fetchone()
                
                if not result:
                    logger.warning(f"Token {tokenAddress} not found or already disabled")
                    return False
                
                # Disable timeframe metadata
                cursor.execute(
                    text("""
                        UPDATE timeframemetadata 
                        SET isactive = FALSE, lastupdatedat = %s
                        WHERE tokenaddress = %s
                    """),
                    (now, tokenAddress)
                )
                
                logger.info(f"Disabled token {tokenAddress} - reason: {reason}")
                return True
                
        except Exception as e:
            logger.error(f"Error disabling token {tokenAddress}: {e}")
            return False

    def getActiveTokens(self) -> List[Dict]:
        """Get all active tracked tokens with their metadata"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        SELECT t.*, 
                               COUNT(tm.id) as active_timeframes
                        FROM trackedtokens t
                        LEFT JOIN timeframemetadata tm ON t.tokenaddress = tm.tokenaddress 
                            AND tm.isactive = TRUE
                        WHERE t.status = 1
                        GROUP BY t.trackedtokenid
                        ORDER BY t.createdat DESC
                    """)
                )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting active tokens: {e}")
            return []

    # ===============================================================
    # SCHEDULER METHODS
    # ===============================================================
    
    def getTokensDueForFetch(self, limit: int = 50) -> List[Dict]:
        """
        Get tokens that need data fetching (only 15m timeframe)
        
        Args:
            limit: Maximum tokens to return
            
        Returns:
            List[Dict]: Tokens due for fetching
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        SELECT tm.*, tt.symbol, tt.name, tt.pairaddress as token_pair
                        FROM timeframemetadata tm
                        JOIN trackedtokens tt ON tm.tokenaddress = tt.tokenaddress
                        WHERE tm.timeframe = '15m'
                          AND tm.nextfetchat <= NOW()
                          AND tm.isactive = TRUE
                          AND tt.status = 1
                          AND tm.consecutivefailures < 5
                        ORDER BY tm.nextfetchat ASC
                        LIMIT %s
                    """),
                    (limit,)
                )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting tokens due for fetch: {e}")
            return []

    def updateFetchStatus(self, tokenAddress: str, timeframe: str, success: bool, nextFetchTime: datetime = None) -> bool:
        """
        Update fetch status for a token/timeframe
        
        Args:
            tokenAddress: Token contract address
            timeframe: Timeframe that was fetched
            success: Whether fetch was successful
            nextFetchTime: When to fetch next (optional)
            
        Returns:
            bool: Success status
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            if nextFetchTime is None:
                # Default to 15 minutes from now
                import datetime as dt
                nextFetchTime = now + dt.timedelta(minutes=15)
            
            with self.conn_manager.transaction() as cursor:
                if success:
                    cursor.execute(
                        text("""
                            UPDATE timeframemetadata 
                            SET lastfetchedat = %s, 
                                lastsuccessfullfetchat = %s,
                                nextfetchat = %s,
                                consecutivefailures = 0,
                                lastupdatedat = %s
                            WHERE tokenaddress = %s AND timeframe = %s
                        """),
                        (now, now, nextFetchTime, now, tokenAddress, timeframe)
                    )
                else:
                    cursor.execute(
                        text("""
                            UPDATE timeframemetadata 
                            SET lastfetchedat = %s,
                                nextfetchat = %s,
                                consecutivefailures = consecutivefailures + 1,
                                lastupdatedat = %s
                            WHERE tokenaddress = %s AND timeframe = %s
                        """),
                        (now, nextFetchTime, now, tokenAddress, timeframe)
                    )
                
                return True
        except Exception as e:
            logger.error(f"Error updating fetch status for {tokenAddress}: {e}")
            return False

    # ===============================================================
    # OHLCV DATA METHODS
    # ===============================================================
    
    def insertOHLCVCandle(self, tokenAddress: str, pairAddress: str, timeframe: str, 
                         unixTime: int, openPrice: Decimal, highPrice: Decimal, 
                         lowPrice: Decimal, closePrice: Decimal, volume: Decimal,
                         dataSource: str = 'api') -> Optional[int]:
        """
        Insert a single OHLCV candle
        
        Args:
            tokenAddress: Token contract address
            pairAddress: DEX pair address
            timeframe: 15m, 1h, or 4h
            unixTime: Candle timestamp
            openPrice: Opening price
            highPrice: Highest price
            lowPrice: Lowest price  
            closePrice: Closing price
            volume: Trading volume
            dataSource: 'api' or 'aggregated'
            
        Returns:
            int: Candle ID if successful, None if failed
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            # Calculate timebucket based on timeframe
            if timeframe == '15m':
                timeBucket = (unixTime // 900) * 900
            elif timeframe == '1h':
                timeBucket = (unixTime // 3600) * 3600
            elif timeframe == '4h':
                timeBucket = (unixTime // 14400) * 14400
            else:
                timeBucket = unixTime
            
            with self.conn_manager.transaction() as cursor:
                # Get timeframe metadata ID
                cursor.execute(
                    text("""
                        SELECT id FROM timeframemetadata 
                        WHERE tokenaddress = %s AND timeframe = %s
                    """),
                    (tokenAddress, timeframe)
                )
                result = cursor.fetchone()
                if not result:
                    logger.error(f"No timeframe metadata found for {tokenAddress} {timeframe}")
                    return None
                
                timeframeId = result['id']
                
                # Insert candle (ON CONFLICT DO UPDATE for upsert)
                cursor.execute(
                    text("""
                        INSERT INTO ohlcvdetails 
                        (timeframeid, tokenaddress, pairaddress, timeframe, unixtime, timebucket,
                         openprice, highprice, lowprice, closeprice, volume, datasource, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, unixtime) 
                        DO UPDATE SET
                            openprice = EXCLUDED.openprice,
                            highprice = EXCLUDED.highprice,
                            lowprice = EXCLUDED.lowprice,
                            closeprice = EXCLUDED.closeprice,
                            volume = EXCLUDED.volume,
                            datasource = EXCLUDED.datasource,
                            lastupdatedat = EXCLUDED.lastupdatedat
                        RETURNING id
                    """),
                    (timeframeId, tokenAddress, pairAddress, timeframe, unixTime, timeBucket,
                     openPrice, highPrice, lowPrice, closePrice, volume, dataSource, now, now)
                )
                result = cursor.fetchone()
                candleId = result['id'] if result else None
                
                logger.debug(f"Inserted/updated {timeframe} candle for {tokenAddress} at {unixTime}")
                return candleId
                
        except Exception as e:
            logger.error(f"Error inserting OHLCV candle: {e}")
            return None

    def getLatestCandles(self, tokenAddress: str, timeframe: str, limit: int = 100) -> List[Dict]:
        """
        Get latest candles for a token/timeframe
        
        Args:
            tokenAddress: Token contract address
            timeframe: 15m, 1h, or 4h
            limit: Maximum candles to return
            
        Returns:
            List[Dict]: Latest candles ordered by time DESC
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        SELECT * FROM ohlcvdetails
                        WHERE tokenaddress = %s AND timeframe = %s
                        ORDER BY unixtime DESC
                        LIMIT %s
                    """),
                    (tokenAddress, timeframe, limit)
                )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting latest candles: {e}")
            return []

    def getCandlesForAggregation(self, tokenAddress: str, timeframe: str, startTime: int, endTime: int) -> List[Dict]:
        """
        Get candles in a time range for aggregation (1h from 15m, 4h from 15m)
        
        Args:
            tokenAddress: Token contract address
            timeframe: Source timeframe (typically '15m')
            startTime: Start unix timestamp
            endTime: End unix timestamp
            
        Returns:
            List[Dict]: Candles in time range ordered by time ASC
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        SELECT * FROM ohlcvdetails
                        WHERE tokenaddress = %s 
                          AND timeframe = %s
                          AND unixtime >= %s 
                          AND unixtime <= %s
                          AND iscomplete = TRUE
                        ORDER BY unixtime ASC
                    """),
                    (tokenAddress, timeframe, startTime, endTime)
                )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting candles for aggregation: {e}")
            return []

    # ===============================================================
    # INDICATOR STATE METHODS
    # ===============================================================
    
    def getIndicatorState(self, tokenAddress: str, timeframe: str, indicatorKey: str) -> Optional[Dict]:
        """Get current indicator state"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        SELECT * FROM indicatorstates
                        WHERE tokenaddress = %s AND timeframe = %s AND indicatorkey = %s
                    """),
                    (tokenAddress, timeframe, indicatorKey)
                )
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error getting indicator state: {e}")
            return None

    def updateIndicatorState(self, tokenAddress: str, timeframe: str, indicatorKey: str,
                           currentValue: Decimal, previousValue: Decimal = None, 
                           candleCount: int = None, isWarmedUp: bool = False,
                           lastUpdatedUnix: int = None) -> bool:
        """Update indicator state with current and previous values"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        INSERT INTO indicatorstates 
                        (tokenaddress, timeframe, indicatorkey, currentvalue, previousvalue, 
                         candlecount, iswarmedup, lastupdatedunix)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, indicatorkey)
                        DO UPDATE SET
                            previousvalue = indicatorstates.currentvalue,
                            currentvalue = EXCLUDED.currentvalue,
                            candlecount = COALESCE(EXCLUDED.candlecount, indicatorstates.candlecount + 1),
                            iswarmedup = EXCLUDED.iswarmedup,
                            lastupdatedunix = EXCLUDED.lastupdatedunix
                    """),
                    (tokenAddress, timeframe, indicatorKey, currentValue, previousValue,
                     candleCount, isWarmedUp, lastUpdatedUnix)
                )
                return True
        except Exception as e:
            logger.error(f"Error updating indicator state: {e}")
            return False

    # ===============================================================
    # VWAP SESSION METHODS
    # ===============================================================
    
    def getVWAPSession(self, tokenAddress: str, timeframe: str, sessionStart: int) -> Optional[Dict]:
        """Get VWAP session data"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        SELECT * FROM vwapsessions
                        WHERE tokenaddress = %s AND timeframe = %s AND sessionstartunix = %s
                    """),
                    (tokenAddress, timeframe, sessionStart)
                )
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error getting VWAP session: {e}")
            return None

    def updateVWAPSession(self, tokenAddress: str, timeframe: str, sessionStart: int, sessionEnd: int,
                         cumulativePV: Decimal, cumulativeVolume: Decimal, currentVWAP: Decimal,
                         highVWAP: Decimal = None, lowVWAP: Decimal = None, 
                         lastCandleUnix: int = None, candleCount: int = None) -> bool:
        """Update VWAP session with new data"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        INSERT INTO vwapsessions 
                        (tokenaddress, timeframe, sessionstartunix, sessionendunix, 
                         cumulativepv, cumulativevolume, currentvwap, highvwap, lowvwap,
                         lastcandleunix, candlecount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, sessionstartunix)
                        DO UPDATE SET
                            cumulativepv = EXCLUDED.cumulativepv,
                            cumulativevolume = EXCLUDED.cumulativevolume,
                            currentvwap = EXCLUDED.currentvwap,
                            highvwap = GREATEST(vwapsessions.highvwap, EXCLUDED.highvwap),
                            lowvwap = LEAST(vwapsessions.lowvwap, EXCLUDED.lowvwap),
                            lastcandleunix = EXCLUDED.lastcandleunix,
                            candlecount = EXCLUDED.candlecount
                    """),
                    (tokenAddress, timeframe, sessionStart, sessionEnd, cumulativePV, 
                     cumulativeVolume, currentVWAP, highVWAP, lowVWAP, lastCandleUnix, candleCount)
                )
                return True
        except Exception as e:
            logger.error(f"Error updating VWAP session: {e}")
            return False

    # ===============================================================
    # BULK OPERATIONS FOR PERFORMANCE
    # ===============================================================
    
    def batchInsertCandles(self, candles: List[Dict]) -> int:
        """
        High-performance batch insert for OHLCV candles
        Optimized for 1000+ tokens with minimal overhead
        """
        if not candles:
            return 0
            
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            # Prepare batch data efficiently
            batch_data = []
            for candle in candles:
                unixTime = int(candle['unixtime'])
                timeframe = candle['timeframe']
                
                # Calculate timebucket in single operation
                if timeframe == '15m':
                    timeBucket = (unixTime // 900) * 900
                elif timeframe == '1h':
                    timeBucket = (unixTime // 3600) * 3600
                elif timeframe == '4h':
                    timeBucket = (unixTime // 14400) * 14400
                else:
                    timeBucket = unixTime
                
                batch_data.append((
                    candle.get('timeframeid'),
                    candle['tokenaddress'],
                    candle['pairaddress'], 
                    timeframe,
                    unixTime,
                    timeBucket,
                    Decimal(str(candle['openprice'])),
                    Decimal(str(candle['highprice'])),
                    Decimal(str(candle['lowprice'])),
                    Decimal(str(candle['closeprice'])),
                    Decimal(str(candle['volume'])),
                    candle.get('datasource', 'api'),
                    now,
                    now
                ))
            
            # Single transaction for maximum performance
            with self.conn_manager.transaction() as cursor:
                cursor.executemany(
                    text("""
                        INSERT INTO ohlcvdetails 
                        (timeframeid, tokenaddress, pairaddress, timeframe, unixtime, timebucket,
                         openprice, highprice, lowprice, closeprice, volume, datasource, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                    """),
                    batch_data
                )
                
                inserted_count = cursor.rowcount
                logger.info(f"Batch inserted {inserted_count} OHLCV candles")
                return inserted_count
                
        except Exception as e:
            logger.error(f"Error in batch candle insertion: {e}")
            return 0

    def bulkUpdateIndicators(self, updates: List[Dict]) -> int:
        """
        Bulk update indicator values using temp table for maximum efficiency
        
        Args:
            updates: List of {candle_id, vwap, ema21, ema34}
            
        Returns:
            int: Number of updated records
        """
        if not updates:
            return 0
        
        try:
            with self.conn_manager.transaction() as cursor:
                # Create temp table for bulk update
                cursor.execute(text("""
                    CREATE TEMP TABLE temp_indicator_updates (
                        candle_id BIGINT,
                        vwap_value DECIMAL(20,8),
                        ema21_value DECIMAL(20,8),
                        ema34_value DECIMAL(20,8)
                    ) ON COMMIT DROP
                """))
                
                # Prepare update data
                update_data = []
                for update in updates:
                    update_data.append((
                        update['candle_id'],
                        update.get('vwap'),
                        update.get('ema21'),
                        update.get('ema34')
                    ))
                
                # Bulk insert into temp table
                cursor.executemany(
                    text("INSERT INTO temp_indicator_updates VALUES (%s, %s, %s, %s)"),
                    update_data
                )
                
                # Single UPDATE with JOIN for maximum performance
                cursor.execute(text("""
                    UPDATE ohlcvdetails 
                    SET 
                        vwapvalue = COALESCE(tiu.vwap_value, ohlcvdetails.vwapvalue),
                        ema21value = COALESCE(tiu.ema21_value, ohlcvdetails.ema21value),
                        ema34value = COALESCE(tiu.ema34_value, ohlcvdetails.ema34value),
                        lastupdatedat = NOW()
                    FROM temp_indicator_updates tiu
                    WHERE ohlcvdetails.id = tiu.candle_id
                """))
                
                updated_count = cursor.rowcount
                logger.info(f"Bulk updated indicators for {updated_count} candles")
                return updated_count
                
        except Exception as e:
            logger.error(f"Error in bulk update indicators: {e}")
            return 0

    def getBatchIndicatorStates(self, token_addresses: List[str], timeframe: str) -> Dict[str, Dict]:
        """
        Get indicator states for multiple tokens in single query
        Eliminates N+1 query problem
        """
        try:
            if not token_addresses:
                return {}
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT 
                        tokenaddress,
                        indicatorkey,
                        currentvalue,
                        previousvalue,
                        candlecount,
                        iswarmedup
                    FROM indicatorstates
                    WHERE tokenaddress = ANY(%s) AND timeframe = %s
                """), (token_addresses, timeframe))
                
                # Organize by token and indicator
                results = {}
                for row in cursor.fetchall():
                    token_addr = row['tokenaddress']
                    indicator_key = row['indicatorkey']
                    
                    if token_addr not in results:
                        results[token_addr] = {}
                    
                    results[token_addr][indicator_key] = dict(row)
                
                return results
                
        except Exception as e:
            logger.error(f"Error getting batch indicator states: {e}")
            return {}

    def getBatchLatestCandles(self, token_addresses: List[str], timeframe: str) -> Dict[str, Dict]:
        """
        Get latest candle for multiple tokens efficiently
        """
        try:
            if not token_addresses:
                return {}
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT DISTINCT ON (tokenaddress)
                        tokenaddress, id, unixtime, openprice, highprice, lowprice,
                        closeprice, volume, vwapvalue, ema21value, ema34value
                    FROM ohlcvdetails
                    WHERE tokenaddress = ANY(%s) AND timeframe = %s AND iscomplete = TRUE
                    ORDER BY tokenaddress, unixtime DESC
                """), (token_addresses, timeframe))
                
                return {row['tokenaddress']: dict(row) for row in cursor.fetchall()}
                
        except Exception as e:
            logger.error(f"Error getting batch latest candles: {e}")
            return {}

    def getAggregationCandidates(self, target_timeframe: str, limit: int = 100) -> List[Dict]:
        """
        Find tokens needing aggregation with optimized time-based query
        """
        try:
            if target_timeframe == '1h':
                time_condition = "EXTRACT(MINUTE FROM to_timestamp(unixtime)) = 45"
                bucket_size = 3600
            elif target_timeframe == '4h':
                time_condition = "EXTRACT(MINUTE FROM to_timestamp(unixtime)) = 45 AND EXTRACT(HOUR FROM to_timestamp(unixtime)) % 4 = 3"
                bucket_size = 14400
            else:
                return []
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text(f"""
                    SELECT 
                        o.tokenaddress,
                        o.unixtime as trigger_time,
                        (o.unixtime / {bucket_size}) * {bucket_size} as period_start
                    FROM ohlcvdetails o
                    WHERE o.timeframe = '15m'
                      AND o.iscomplete = TRUE
                      AND {time_condition}
                      AND NOT EXISTS (
                          SELECT 1 FROM ohlcvdetails agg 
                          WHERE agg.tokenaddress = o.tokenaddress 
                            AND agg.timeframe = %s 
                            AND agg.unixtime = (o.unixtime / {bucket_size}) * {bucket_size}
                      )
                      AND o.unixtime >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '24 hours'))
                    ORDER BY o.unixtime DESC
                    LIMIT %s
                """), (target_timeframe, limit))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting aggregation candidates: {e}")
            return []

    def updateFetchStatusBatch(self, token_results: List[Dict]) -> int:
        """
        Batch update fetch status for multiple tokens
        """
        try:
            if not token_results:
                return 0
            
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            
            update_data = []
            for result in token_results:
                from datetime import timedelta
                next_fetch = now + timedelta(minutes=15)
                failures = 0 if result['success'] else result.get('current_failures', 0) + 1
                
                update_data.append((
                    now if result['success'] else None,
                    now,
                    next_fetch,
                    failures,
                    now,
                    result['token_address']
                ))
            
            with self.conn_manager.transaction() as cursor:
                cursor.executemany(text("""
                    UPDATE timeframemetadata 
                    SET lastsuccessfullfetchat = COALESCE(%s, lastsuccessfullfetchat),
                        lastfetchedat = %s,
                        nextfetchat = %s,
                        consecutivefailures = %s,
                        lastupdatedat = %s
                    WHERE tokenaddress = %s AND timeframe = '15m'
                """), update_data)
                
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"Error updating fetch status batch: {e}")
            return 0

    # ===============================================================
    # FLEXIBLE INDICATOR FRAMEWORK - THREE LAYER ARCHITECTURE
    # ===============================================================
    
    # LAYER 1: CONFIGURATION LAYER
    # ===============================================================
    
    def addIndicatorConfig(self, tokenAddress: str = None, timeframe: str = None, 
                          indicatorType: str = 'ema', parameters: Dict = None,
                          configName: str = None, priority: int = 100) -> Optional[int]:
        """Add indicator configuration"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    INSERT INTO indicatorconfigs 
                    (tokenaddress, timeframe, indicatortype, parameters, configname, priority)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING configid
                """), (tokenAddress, timeframe, indicatorType, json.dumps(parameters), configName, priority))
                
                result = cursor.fetchone()
                return result['configid'] if result else None
                
        except Exception as e:
            logger.error(f"Error adding indicator config: {e}")
            return None

    def getIndicatorConfigs(self, tokenAddress: str = None, timeframe: str = None) -> List[Dict]:
        """Get indicator configurations for token/timeframe"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT * FROM indicatorconfigs
                    WHERE (tokenaddress = %s OR tokenaddress IS NULL)
                      AND (timeframe = %s OR timeframe IS NULL)
                      AND isactive = TRUE
                    ORDER BY priority ASC
                """), (tokenAddress, timeframe))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting indicator configs: {e}")
            return []

    def updateIndicatorConfig(self, configId: int, parameters: Dict = None, 
                             isActive: bool = None, priority: int = None) -> bool:
        """Update indicator configuration"""
        try:
            updates = []
            params = []
            
            if parameters is not None:
                updates.append("parameters = %s")
                params.append(json.dumps(parameters))
            if isActive is not None:
                updates.append("isactive = %s")
                params.append(isActive)
            if priority is not None:
                updates.append("priority = %s")
                params.append(priority)
                
            if not updates:
                return True
                
            updates.append("updatedat = NOW()")
            params.append(configId)
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text(f"""
                    UPDATE indicatorconfigs 
                    SET {', '.join(updates)}
                    WHERE configid = %s
                """), params)
                
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error updating indicator config {configId}: {e}")
            return False

    # LAYER 2: STATE LAYER
    # ===============================================================
    
    def initializeIndicatorStates(self, tokenAddress: str, timeframe: str) -> bool:
        """
        Initialize indicator states for a token/timeframe based on configs
        Creates state entries for all applicable indicator configurations
        """
        try:
            configs = self.getIndicatorConfigs(tokenAddress, timeframe)
            
            with self.conn_manager.transaction() as cursor:
                for config in configs:
                    indicatorType = config['indicatortype']
                    parameters = config['parameters']
                    
                    # Generate indicator keys based on type and parameters
                    if indicatorType == 'ema':
                        if 'periods' in parameters:
                            # Multiple EMA periods like [21, 34, 50, 200]
                            for period in parameters['periods']:
                                indicator_key = f"ema_{period}"
                                self._createIndicatorStateEntry(cursor, tokenAddress, timeframe, indicator_key)
                        elif 'period' in parameters:
                            # Single EMA period
                            indicator_key = f"ema_{parameters['period']}"
                            self._createIndicatorStateEntry(cursor, tokenAddress, timeframe, indicator_key)
                            
                    elif indicatorType == 'vwap':
                        indicator_key = "vwap"
                        self._createIndicatorStateEntry(cursor, tokenAddress, timeframe, indicator_key)
                        
                    elif indicatorType == 'sma':
                        if 'periods' in parameters:
                            for period in parameters['periods']:
                                indicator_key = f"sma_{period}"
                                self._createIndicatorStateEntry(cursor, tokenAddress, timeframe, indicator_key)
                        elif 'period' in parameters:
                            indicator_key = f"sma_{parameters['period']}"
                            self._createIndicatorStateEntry(cursor, tokenAddress, timeframe, indicator_key)
                
                return True
                
        except Exception as e:
            logger.error(f"Error initializing indicator states: {e}")
            return False

    def _createIndicatorStateEntry(self, cursor, tokenAddress: str, timeframe: str, indicatorKey: str):
        """Create single indicator state entry"""
        cursor.execute(text("""
            INSERT INTO indicatorstates 
            (tokenaddress, timeframe, indicatorkey, currentvalue, previousvalue, 
             candlecount, iswarmedup, lastupdatedunix)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tokenaddress, timeframe, indicatorkey) DO NOTHING
        """), (tokenAddress, timeframe, indicatorKey, Decimal('0'), Decimal('0'), 0, False, 0))

    def getDynamicIndicatorStates(self, tokenAddress: str, timeframe: str) -> Dict[str, Dict]:
        """
        Get all indicator states for a token/timeframe organized by indicator type
        Returns: {"ema": {"21": {...}, "34": {...}}, "vwap": {...}}
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT * FROM indicatorstates
                    WHERE tokenaddress = %s AND timeframe = %s
                    ORDER BY indicatorkey
                """), (tokenAddress, timeframe))
                
                states = {}
                for row in cursor.fetchall():
                    indicator_key = row['indicatorkey']
                    
                    # Parse indicator type and period
                    if '_' in indicator_key:
                        indicator_type, period = indicator_key.split('_', 1)
                        if indicator_type not in states:
                            states[indicator_type] = {}
                        states[indicator_type][period] = dict(row)
                    else:
                        # Single indicators like vwap
                        states[indicator_key] = dict(row)
                
                return states
                
        except Exception as e:
            logger.error(f"Error getting dynamic indicator states: {e}")
            return {}

    def bulkUpdateIndicatorStates(self, updates: List[Dict]) -> int:
        """
        Bulk update indicator states efficiently
        
        Args:
            updates: List of {tokenaddress, timeframe, indicatorkey, currentvalue, 
                            previousvalue, candlecount, iswarmedup, lastupdatedunix}
        """
        try:
            if not updates:
                return 0
                
            with self.conn_manager.transaction() as cursor:
                # Create temp table
                cursor.execute(text("""
                    CREATE TEMP TABLE temp_indicator_state_updates (
                        tokenaddress CHAR(44),
                        timeframe CHAR(3),
                        indicatorkey VARCHAR(20),
                        currentvalue DECIMAL(20,8),
                        previousvalue DECIMAL(20,8),
                        candlecount INTEGER,
                        iswarmedup BOOLEAN,
                        lastupdatedunix BIGINT
                    ) ON COMMIT DROP
                """))
                
                # Bulk insert updates
                update_data = []
                for update in updates:
                    update_data.append((
                        update['tokenaddress'],
                        update['timeframe'],
                        update['indicatorkey'],
                        update['currentvalue'],
                        update.get('previousvalue'),
                        update.get('candlecount', 0),
                        update.get('iswarmedup', False),
                        update.get('lastupdatedunix', 0)
                    ))
                
                cursor.executemany(text("""
                    INSERT INTO temp_indicator_state_updates VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s)
                """), update_data)
                
                # Bulk upsert with JOIN
                cursor.execute(text("""
                    INSERT INTO indicatorstates 
                    (tokenaddress, timeframe, indicatorkey, currentvalue, previousvalue, 
                     candlecount, iswarmedup, lastupdatedunix)
                    SELECT * FROM temp_indicator_state_updates
                    ON CONFLICT (tokenaddress, timeframe, indicatorkey)
                    DO UPDATE SET
                        previousvalue = indicatorstates.currentvalue,
                        currentvalue = EXCLUDED.currentvalue,
                        candlecount = EXCLUDED.candlecount,
                        iswarmedup = EXCLUDED.iswarmedup,
                        lastupdatedunix = EXCLUDED.lastupdatedunix
                """))
                
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"Error bulk updating indicator states: {e}")
            return 0

    # LAYER 3: CALCULATION LAYER
    # ===============================================================
    
    def calculateDynamicIndicators(self, tokenAddress: str, timeframe: str, 
                                 latestCandle: Dict) -> Dict[str, Decimal]:
        """
        Calculate all configured indicators for a token/timeframe incrementally
        
        Args:
            tokenAddress: Token contract address
            timeframe: Timeframe to calculate for
            latestCandle: Latest OHLCV candle data
            
        Returns:
            Dict[str, Decimal]: Calculated indicator values {indicator_key: value}
        """
        try:
            configs = self.getIndicatorConfigs(tokenAddress, timeframe)
            current_states = self.getDynamicIndicatorStates(tokenAddress, timeframe)
            calculated_values = {}
            
            for config in configs:
                indicator_type = config['indicatortype']
                parameters = config['parameters']
                
                if indicator_type == 'ema':
                    ema_values = self._calculateEMAIndicators(
                        parameters, current_states.get('ema', {}), latestCandle
                    )
                    calculated_values.update(ema_values)
                    
                elif indicator_type == 'vwap':
                    vwap_value = self._calculateVWAPIndicator(
                        tokenAddress, timeframe, current_states.get('vwap', {}), latestCandle
                    )
                    if vwap_value is not None:
                        calculated_values['vwap'] = vwap_value
                        
                elif indicator_type == 'sma':
                    sma_values = self._calculateSMAIndicators(
                        tokenAddress, timeframe, parameters, current_states.get('sma', {}), latestCandle
                    )
                    calculated_values.update(sma_values)
            
            return calculated_values
            
        except Exception as e:
            logger.error(f"Error calculating dynamic indicators: {e}")
            return {}

    def _calculateEMAIndicators(self, parameters: Dict, current_states: Dict, candle: Dict) -> Dict[str, Decimal]:
        """Calculate EMA indicators based on parameters"""
        results = {}
        
        try:
            close_price = Decimal(str(candle['closeprice']))
            periods_to_calculate = []
            
            if 'periods' in parameters:
                periods_to_calculate = parameters['periods']
            elif 'period' in parameters:
                periods_to_calculate = [parameters['period']]
            
            for period in periods_to_calculate:
                period_str = str(period)
                indicator_key = f"ema_{period}"
                
                if period_str in current_states:
                    state = current_states[period_str]
                    current_value = Decimal(str(state['currentvalue']))
                    candle_count = state['candlecount']
                    
                    if candle_count == 0:
                        # First candle - use close price
                        new_ema = close_price
                        is_warmed_up = False
                    else:
                        # Standard EMA calculation: EMA = (Close * Multiplier) + (Previous_EMA * (1 - Multiplier))
                        multiplier = Decimal('2') / (Decimal(str(period)) + Decimal('1'))
                        new_ema = (close_price * multiplier) + (current_value * (Decimal('1') - multiplier))
                        is_warmed_up = candle_count >= (period * 2)  # Warmed up after 2x period
                    
                    results[indicator_key] = new_ema
                    
                    # Update state
                    self.updateIndicatorState(
                        candle['tokenaddress'], candle['timeframe'], indicator_key,
                        new_ema, current_value, candle_count + 1, is_warmed_up,
                        candle['unixtime']
                    )
                else:
                    # Initialize new EMA
                    self.updateIndicatorState(
                        candle['tokenaddress'], candle['timeframe'], indicator_key,
                        close_price, Decimal('0'), 1, False, candle['unixtime']
                    )
                    results[indicator_key] = close_price
            
            return results
            
        except Exception as e:
            logger.error(f"Error calculating EMA indicators: {e}")
            return {}

    def _calculateVWAPIndicator(self, tokenAddress: str, timeframe: str, 
                               current_state: Dict, candle: Dict) -> Optional[Decimal]:
        """Calculate VWAP indicator incrementally"""
        try:
            price = (Decimal(str(candle['highprice'])) + Decimal(str(candle['lowprice'])) + Decimal(str(candle['closeprice']))) / Decimal('3')
            volume = Decimal(str(candle['volume']))
            
            # Get or create VWAP session
            session_start = self._getVWAPSessionStart(candle['unixtime'], timeframe)
            vwap_session = self.getVWAPSession(tokenAddress, timeframe, session_start)
            
            if vwap_session:
                cumulative_pv = vwap_session['cumulativepv'] + (price * volume)
                cumulative_volume = vwap_session['cumulativevolume'] + volume
            else:
                cumulative_pv = price * volume
                cumulative_volume = volume
            
            current_vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else price
            
            # Update VWAP session
            self.updateVWAPSession(
                tokenAddress, timeframe, session_start, session_start + (24 * 3600),
                cumulative_pv, cumulative_volume, current_vwap,
                current_vwap, current_vwap, candle['unixtime'], 1
            )
            
            return current_vwap
            
        except Exception as e:
            logger.error(f"Error calculating VWAP: {e}")
            return None

    def _calculateSMAIndicators(self, tokenAddress: str, timeframe: str, parameters: Dict, 
                               current_states: Dict, candle: Dict) -> Dict[str, Decimal]:
        """Calculate SMA indicators (requires looking back at historical data)"""
        results = {}
        
        try:
            periods_to_calculate = []
            if 'periods' in parameters:
                periods_to_calculate = parameters['periods']
            elif 'period' in parameters:
                periods_to_calculate = [parameters['period']]
            
            for period in periods_to_calculate:
                # Get last N candles for SMA calculation
                recent_candles = self.getLatestCandles(tokenAddress, timeframe, period)
                
                if len(recent_candles) >= period:
                    # Calculate SMA
                    total_close = sum(Decimal(str(c['closeprice'])) for c in recent_candles[:period])
                    sma_value = total_close / Decimal(str(period))
                    
                    indicator_key = f"sma_{period}"
                    results[indicator_key] = sma_value
                    
                    # Update state
                    self.updateIndicatorState(
                        tokenAddress, timeframe, indicator_key,
                        sma_value, Decimal('0'), len(recent_candles), 
                        len(recent_candles) >= period, candle['unixtime']
                    )
            
            return results
            
        except Exception as e:
            logger.error(f"Error calculating SMA indicators: {e}")
            return {}

    def _getVWAPSessionStart(self, unix_time: int, timeframe: str) -> int:
        """Get VWAP session start time based on timeframe"""
        if timeframe == '15m':
            # Daily VWAP reset at midnight UTC
            return (unix_time // 86400) * 86400
        elif timeframe == '1h':
            # Daily VWAP reset at midnight UTC
            return (unix_time // 86400) * 86400
        elif timeframe == '4h':
            # Daily VWAP reset at midnight UTC
            return (unix_time // 86400) * 86400
        else:
            return unix_time

    def detectCrosses(self, tokenAddress: str, timeframe: str) -> List[Dict]:
        """
        Detect crosses between ANY configured EMA pairs
        Returns list of detected crosses with details
        """
        try:
            configs = self.getIndicatorConfigs(tokenAddress, timeframe, 'ema')
            if not configs:
                return []
            
            # Get all EMA periods configured for this token/timeframe
            all_periods = set()
            for config in configs:
                params = config['parameters']
                if 'periods' in params:
                    all_periods.update(params['periods'])
                elif 'period' in params:
                    all_periods.add(params['period'])
            
            if len(all_periods) < 2:
                return []
            
            sorted_periods = sorted(all_periods)
            crosses = []
            
            # Check all possible pairs
            for i in range(len(sorted_periods)):
                for j in range(i + 1, len(sorted_periods)):
                    short_period = sorted_periods[i]
                    long_period = sorted_periods[j]
                    
                    short_state = self.getIndicatorState(tokenAddress, timeframe, f"ema_{short_period}")
                    long_state = self.getIndicatorState(tokenAddress, timeframe, f"ema_{long_period}")
                    
                    if short_state and long_state and short_state['iswarmedup'] and long_state['iswarmedup']:
                        cross_type = self._detectCrossBetweenPair(short_state, long_state)
                        
                        if cross_type != 'no_cross':
                            crosses.append({
                                'tokenaddress': tokenAddress,
                                'timeframe': timeframe,
                                'short_period': short_period,
                                'long_period': long_period,
                                'cross_type': cross_type,
                                'short_current': short_state['currentvalue'],
                                'short_previous': short_state['previousvalue'],
                                'long_current': long_state['currentvalue'],
                                'long_previous': long_state['previousvalue'],
                                'detected_at': short_state['lastupdatedunix']
                            })
            
            return crosses
            
        except Exception as e:
            logger.error(f"Error detecting crosses: {e}")
            return []

    def _detectCrossBetweenPair(self, short_state: Dict, long_state: Dict) -> str:
        """Detect cross between two indicator states"""
        short_current = Decimal(str(short_state['currentvalue']))
        short_previous = Decimal(str(short_state['previousvalue']))
        long_current = Decimal(str(long_state['currentvalue']))
        long_previous = Decimal(str(long_state['previousvalue']))
        
        # Bullish cross: short was below long, now above
        if short_previous <= long_previous and short_current > long_current:
            return 'bullish_cross'
        
        # Bearish cross: short was above long, now below
        if short_previous >= long_previous and short_current < long_current:
            return 'bearish_cross'
        
        return 'no_cross'