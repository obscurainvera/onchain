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
from datetime import datetime, timezone, timedelta
from actions.TradingActionUtil import TradingActionUtil

logger = get_logger(__name__)

class AdditionSource(IntEnum):
    """Token addition source enumeration"""
    MANUAL = 1
    AUTOMATIC = 2

class EMAStatus(IntEnum):
    """EMA calculation status enumeration"""
    NOT_AVAILABLE = 1
    AVAILABLE = 2

# Actual Database Tables (based on _createBasicTables implementation)


class TradingHandler(BaseDBHandler):
    def __init__(self, conn_manager=None):
        if conn_manager is None:
            conn_manager = DatabaseConnectionManager()
        super().__init__(conn_manager)
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
        
        # 4. EMA States (replaces indicatorstates and indicatorconfigs)
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS emastates (
                tokenaddress CHAR(44),
                pairaddress CHAR(44),
                timeframe CHAR(3),
                emakey VARCHAR(20),
                emavalue DECIMAL(20,8),
                lastupdatedunix BIGINT,
                nextfetchtime BIGINT,
                emaavailabletime BIGINT,
                paircreatedtime BIGINT,
                status INTEGER DEFAULT 1 CHECK (status IN (1, 2)),
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (tokenaddress, timeframe, emakey)
            )
        """))
        
        # 5. VWAP Sessions
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS vwapsessions (
                tokenaddress CHAR(44),
                pairaddress CHAR(44),
                timeframe CHAR(3),
                sessionstartunix BIGINT,
                sessionendunix BIGINT,
                cumulativepv DECIMAL(30,8),
                cumulativevolume DECIMAL(30,8),
                currentvwap DECIMAL(20,8),
                lastcandleunix BIGINT,
                nextcandlefetch BIGINT,
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (tokenaddress, timeframe, sessionstartunix)
            )
        """))
        

    def getTableDocumentation(self, tableName: str) -> dict:
        """Get documentation for a specific table"""
        return self.schema.get(tableName, {})

    
    
    def createEmptyTimeFrameRecord(self, tokenAddress: str, pairAddress: str, timeframe: str) -> bool:
        """Create initial timeframe record with null timestamps"""
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    INSERT INTO timeframemetadata 
                    (tokenaddress, pairaddress, timeframe, nextfetchat, lastfetchedat, isactive, createdat, lastupdatedat)
                    VALUES (%s, %s, %s, NULL, NULL, %s, NOW(), NOW())
                    ON CONFLICT (tokenaddress, pairaddress, timeframe) DO NOTHING
                """), (tokenAddress, pairAddress, timeframe, True))
                
                return True
                
        except Exception as e:
            logger.error(f"Error creating timeframe record: {e}")
            return False

    def _updateTimeframeFetchStatus(self, tokenAddress: str, timeframe: str, fetchTime: int) -> bool:
        """Update timeframe fetch status"""
        try:
            with self.conn_manager.transaction() as cursor:
                # Calculate next fetch time
                if timeframe == '15m':
                    next_fetch = fetchTime + 900
                elif timeframe == '1h':
                    next_fetch = fetchTime + 3600
                elif timeframe == '4h':
                    next_fetch = fetchTime + 14400
                else:
                    next_fetch = fetchTime + 900
                
                cursor.execute(text("""
                    UPDATE timeframemetadata 
                    SET lastfetchedat = %s, nextfetchat = %s, lastupdatedat = NOW()
                    WHERE tokenaddress = %s AND timeframe = %s
                """), (datetime.fromtimestamp(fetchTime), datetime.fromtimestamp(next_fetch), 
                       tokenAddress, timeframe))
                
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error updating timeframe fetch status: {e}")
            return False


    def getColumnDescription(self, tableName: str, columnName: str) -> str:
        """Get description for a specific column"""
        tableSchema = self.schema.get(tableName, {})
        return tableSchema.get(columnName, "No description available")

    
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

  
    
    def getTokensToFetch15MinCandlesWithBuffer(self, buffer_seconds: int = 300) -> List[Dict]:
        """
        Get tokens that need data fetching with creation time buffer (optimized version)
        This method includes the 5-minute buffer directly in the SQL query
        """
        try:
            with self.conn_manager.transaction() as cursor:
                cursor.execute(
                    text("""
                        SELECT tm.*, tt.symbol, tt.name, tt.pairaddress as token_pair,
                               EXTRACT(EPOCH FROM tt.createdat) as paircreatedtime
                        FROM timeframemetadata tm
                        JOIN trackedtokens tt ON tm.tokenaddress = tt.tokenaddress
                        WHERE tm.timeframe = '15m'
                          AND tm.nextfetchat <= NOW()
                          AND tm.isactive = TRUE
                          AND tt.status = 1
                          AND (tt.createdat IS NULL OR tt.createdat <= NOW() - INTERVAL '%s seconds')
                        ORDER BY tm.nextfetchat ASC
                    """),
                    (buffer_seconds)
                )
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting tokens due for fetch with buffer: {e}")
            return []

    
    def getCandlesForAggregationFromScheduler(self, tokenAddresses: List[str]) -> Dict[str, Dict]:
        """
        Get all 15m candles using MIN(last_1h_fetch, last_4h_fetch) for each token.
        
        FIXED LOGIC:
        1. First get timeframe metadata for all tokens
        2. Calculate MIN fetch time per token
        3. Get 15m candles where unixtime > calculated MIN for each token

        15M :7:00 , 7:15, 7:30, 7:45, 8:00 , 8:15, 8:30, 8:45,9:00, 9:15, 9:30, 9:45,10:00, 10:15, 10:30, 10:45,11:00, 11:15, 11:30, 11:45,12:00, 12:15, 12:30, 12:45
        1hr : 8:00, 9:00, 10:00, 11:00   -- next fetch = 12:00, latest fetch = 11:00
        4hr : 8:00 --- next fetch = 12:00, latest fetch = 8:00

        now as the min latest fetch time is 8:00, we get all the candles from 8 till 12, even though 1 hr has till 11, we aggregate from 9,10,11,12 for 1hr and persist only 12 - now we have persisted 12 for 1hr 
        time frame - we need to update 12 as last fetched time and 13:00 as next fetch time
        Aggregate 1h: 12:00 → persist 12:00 (>= 12:00 nextfetchat)
        Aggregate 4h: 12:00 → persist 12:00 (>= 12:00 nextfetchat)
        Update: lastfetchedat = 12:00, nextfetchat = 13:00 (1h) / 16:00 (4h)
        """
        try:
            with self.conn_manager.transaction() as cursor:
                # Step 1: Get timeframe metadata for all requested tokens
                cursor.execute(text("""
                    WITH fetch_times AS (
                        SELECT 
                            t.tokenaddress,
                            COALESCE(EXTRACT(EPOCH FROM tm1h.lastfetchedat), 0) as lastfetchedat_1h,
                            COALESCE(EXTRACT(EPOCH FROM tm4h.lastfetchedat), 0) as lastfetchedat_4h,
                            COALESCE(EXTRACT(EPOCH FROM tm1h.nextfetchat), 0) as nextfetchat_1h,
                            COALESCE(EXTRACT(EPOCH FROM tm4h.nextfetchat), 0) as nextfetchat_4h,
                            LEAST(
                                COALESCE(EXTRACT(EPOCH FROM tm1h.nextfetchat), 0),
                                COALESCE(EXTRACT(EPOCH FROM tm4h.nextfetchat), 0)
                            ) as min_next_fetch_time
                        FROM unnest(%s) AS t(tokenaddress)
                        LEFT JOIN timeframemetadata tm1h ON t.tokenaddress = tm1h.tokenaddress 
                            AND tm1h.timeframe = '1h'
                        LEFT JOIN timeframemetadata tm4h ON t.tokenaddress = tm4h.tokenaddress 
                            AND tm4h.timeframe = '4h'
                    )
                    SELECT 
                        o.tokenaddress,
                        o.pairaddress,
                        o.unixtime,
                        o.openprice,
                        o.highprice,
                        o.lowprice,
                        o.closeprice,
                        o.volume,
                        ft.lastfetchedat_1h,
                        ft.lastfetchedat_4h,
                        ft.nextfetchat_1h,
                        ft.nextfetchat_4h
                    FROM ohlcvdetails o
                    INNER JOIN fetch_times ft ON o.tokenaddress = ft.tokenaddress
                    WHERE o.timeframe = '15m'
                        AND o.iscomplete = TRUE
                        AND o.unixtime >= ft.min_next_fetch_time
                    ORDER BY o.tokenaddress, o.unixtime ASC
                """), (tokenAddresses,))
                
                # Group 15m candles by token with fetch time metadata
                results = {}
                for row in cursor.fetchall():
                    tokenAddress = row['tokenaddress']
                    if tokenAddress not in results:
                        results[tokenAddress] = {
                            'candles_15m': [],
                            'lastfetchedat_1h': row['lastfetchedat_1h'],
                            'lastfetchedat_4h': row['lastfetchedat_4h'], 
                            'nextfetchat_1h': row['nextfetchat_1h'],
                            'nextfetchat_4h': row['nextfetchat_4h'],
                            'pairaddress': row['pairaddress']
                        }
                    
                    results[tokenAddress]['candles_15m'].append({
                        'tokenaddress': row['tokenaddress'],
                        'pairaddress': row['pairaddress'],
                        'unixtime': row['unixtime'],
                        'openprice': float(row['openprice']),
                        'highprice': float(row['highprice']),
                        'lowprice': float(row['lowprice']),
                        'closeprice': float(row['closeprice']),
                        'volume': float(row['volume'])
                    })
                return results
                
        except Exception as e:
            logger.error(f"Error getting candles for aggregation: {e}")
            return {}

    def batchInsertAggregatedCandlesAndUpdateFetchTimes(self, aggregatedCandles: List[Dict], timeframes: List[str]) -> int:
        """
        ATOMIC: Batch insert aggregated candles AND upsert fetch times in single transaction
        """
        try:
            if not aggregatedCandles:
                return 0
            
            with self.conn_manager.transaction() as cursor:
                # STEP 1: Insert aggregated candles
                candleInsertData = self.constructOHLCVDetailsData(aggregatedCandles)
                self.insertOHLCVDetailsData(cursor, candleInsertData)
                
                # STEP 2: Update timeframe metadata
                timeframeUpdateData = self.constructTimeFrameUpdateData(aggregatedCandles)
                self.insertTimeFrameUpdatesData(cursor, timeframeUpdateData)
                
                return len(aggregatedCandles)
            
        except Exception as e:
            logger.error(f"Error batch inserting aggregated candles and updating fetch times: {e}")
            return 0
    
    def constructOHLCVDetailsData(self, aggregatedCandles: List[Dict]) -> List[tuple]:
        """Prepare candle data for batch insert"""
        insertData = []
        for candle in aggregatedCandles:
            timebucket = self._calculateTimeBucket(candle['unixtime'], candle['timeframe'])
            insertData.append((
                candle['tokenaddress'], candle['pairaddress'], candle['timeframe'],
                candle['unixtime'], timebucket, candle['openprice'], candle['highprice'],
                candle['lowprice'], candle['closeprice'], candle['volume']
            ))
        return insertData
    
    def _calculateTimeBucket(self, unixtime: int, timeframe: str) -> int:
        """Calculate timebucket based on timeframe"""
        if timeframe == '1h':
            return (unixtime // 3600) * 3600
        elif timeframe == '4h':
            return (unixtime // 14400) * 14400
        else:
            return unixtime
    
    def insertOHLCVDetailsData(self, cursor, insertData: List[tuple]):
        """Execute batch candle inserts"""
        cursor.executemany(text("""
            INSERT INTO ohlcvdetails 
            (tokenaddress, pairaddress, timeframe, unixtime, timebucket, openprice, 
             highprice, lowprice, closeprice, volume, datasource, iscomplete, createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'aggregated', TRUE, NOW(), NOW())
            ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
        """), insertData)
    
    def constructTimeFrameUpdateData(self, aggregatedCandles: List[Dict]) -> List[tuple]:
        """Prepare timeframe metadata update data"""
        # OPTIMIZED: Single pass to group by token+timeframe with all needed data
        tokenTimeframeData = {}
        
        for candle in aggregatedCandles:
            key = (candle['tokenaddress'], candle['timeframe'])
            if key not in tokenTimeframeData:
                tokenTimeframeData[key] = {
                    'pairaddress': candle['pairaddress'],
                    'latest_time': candle['unixtime']
                }
            else:
                # Only update latest_time if this candle is newer
                tokenTimeframeData[key]['latest_time'] = max(
                    tokenTimeframeData[key]['latest_time'], 
                    candle['unixtime']
                )
        
        # Build update data with next fetch time calculation
        updateData = []
        for (tokenAddress, timeframe), data in tokenTimeframeData.items():
            nextFetchTime = self._calculateNextFetchTime(data['latest_time'], timeframe)
            if nextFetchTime:  # Only add if valid timeframe
                updateData.append((
                    tokenAddress, data['pairaddress'], timeframe,
                    nextFetchTime, data['latest_time'], nextFetchTime, data['latest_time']
                ))
        
        return updateData
    
    def _calculateNextFetchTime(self, latestTime: int, timeframe: str) -> int:
        """Calculate next fetch time based on timeframe"""
        timeframeSeconds = {'1h': 3600, '4h': 14400}
        return latestTime + timeframeSeconds.get(timeframe, 0)
    
    def insertTimeFrameUpdatesData(self, cursor, updateData: List[tuple]):
        """Execute batch timeframe metadata updates"""
        if not updateData:
            return
            
        cursor.executemany(text("""
            INSERT INTO timeframemetadata 
            (tokenaddress, pairaddress, timeframe, nextfetchat, lastfetchedat, createdat, lastupdatedat)
            VALUES (%s, %s, %s, to_timestamp(%s), to_timestamp(%s), NOW(), NOW())
            ON CONFLICT (tokenaddress, pairaddress, timeframe)
            DO UPDATE SET 
                nextfetchat = to_timestamp(%s),
                lastfetchedat = to_timestamp(%s),
                lastupdatedat = NOW()
        """), updateData)
        
        logger.debug(f"Updated fetch times for {len(updateData)} token-timeframe combinations")
    
    def get2DayHistoricalData(self, tokenAddresses: List[str], days: int = 2) -> Dict[str, Dict]:
        """
        Get historical data for multiple tokens across all timeframes for the specified number of days
        
        Args:
            tokenAddresses: List of token addresses
            days: Number of days of historical data to fetch
            
        Returns:
            Dict: {token_address: {timeframe: [candles]}}
        """
        try:
            if not tokenAddresses:
                return {}
                
            # Calculate cutoff time (N days ago at start of day)
            
            # Get current time and go back N days to start of day (00:00:00)
            currentTime = datetime.now(timezone.utc)
            daysAgo = currentTime.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)
            cutoffTime = int(daysAgo.timestamp())
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT tokenaddress, pairaddress, timeframe, unixtime,
                           openprice, highprice, lowprice, closeprice, volume
                    FROM ohlcvdetails 
                    WHERE tokenaddress = ANY(%s) 
                        AND unixtime >= %s
                        AND iscomplete = TRUE
                    ORDER BY tokenaddress, timeframe, unixtime ASC
                """), (tokenAddresses, cutoffTime))
                
                # Group results by token and timeframe
                results = {}
                for row in cursor.fetchall():
                    tokenAddress = row['tokenaddress']
                    timeframe = row['timeframe']
                    
                    if tokenAddress not in results:
                        results[tokenAddress] = {}
                    if timeframe not in results[tokenAddress]:
                        results[tokenAddress][timeframe] = []
                    
                    results[tokenAddress][timeframe].append({
                        'tokenaddress': row['tokenaddress'],
                        'pairaddress': row['pairaddress'],
                        'timeframe': timeframe,
                        'unixtime': row['unixtime'],
                        'openprice': float(row['openprice']),
                        'highprice': float(row['highprice']),
                        'lowprice': float(row['lowprice']),
                        'closeprice': float(row['closeprice']),
                        'volume': float(row['volume'])
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"Error getting {days}-day historical data: {e}")
            return {}
    
    def batchPersistAllCandles(self, candleData: Dict[str, Dict]) -> int:
        """
        OPTIMIZED: Batch persist ALL candles from ALL tokens using unified map structure
        
        Args:
            token_candle_data: Dict mapping token_address -> {
                'candles': List[Dict],
                'latest_time': int,
                'count': int
            }
            
        Returns:
            int: Number of successfully persisted candles
        """
        try:
            if not candleData:
                return 0
            
            totalCandles = 0
            
            with self.conn_manager.transaction() as cursor:
                # STEP 1: UPSERT timeframe records and build candle insert data in single pass
                timeframeTableUpdateData = []
                ohlcvDetailsData = []
                
                from scheduler.SchedulerConstants import CandleDataKeys, Timeframes
                
                for tokenAddress, data in candleData.items():
                    candles = data[CandleDataKeys.CANDLES]
                    if not candles:
                        continue
                        
                    # Get token info from first candle
                    firstCandle = candles[0]
                    
                    # Prepare timeframe UPSERT data
                    timeframeTableUpdateData.append((
                        tokenAddress,
                        firstCandle['pairaddress'], 
                        Timeframes.FIFTEEN_MIN,
                        data[CandleDataKeys.LATEST_TIME]  # Use latest_time for lastfetchedat
                    ))
                    
                    # Prepare all candle insert data for this token
                    for candle in candles:
                        ohlcvDetailsData.append((
                            candle['tokenaddress'], candle['pairaddress'], candle['timeframe'],
                            candle['unixtime'], candle['openprice'], candle['highprice'],
                            candle['lowprice'], candle['closeprice'], candle['volume'], 
                            candle['datasource']
                        ))
                        totalCandles += 1
                
                # STEP 2: UPSERT timeframe records (create if not exists, update lastfetchedat if exists)
                cursor.executemany(text("""
                    INSERT INTO timeframemetadata (tokenaddress, pairaddress, timeframe, lastfetchedat)
                    VALUES (%s, %s, %s, to_timestamp(%s))
                    ON CONFLICT (tokenaddress, timeframe) 
                    DO UPDATE SET lastfetchedat = to_timestamp(EXCLUDED.lastfetchedat)
                """), timeframeTableUpdateData)
                
                # STEP 3: Insert all candles
                cursor.executemany(text("""
                    INSERT INTO ohlcvdetails 
                    (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
                     highprice, lowprice, closeprice, volume, datasource)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                """), ohlcvDetailsData)
            
            return totalCandles
            
        except Exception as e:
            logger.error(f"Error in batch persist: {e}")
            return 0
    

    
    def getAllVWAPSessionInfo(self, token_addresses: List[str], timeframes: List[str]) -> Dict[str, Dict[str, Optional[Dict]]]:
        """
        Get VWAP session information for multiple tokens and timeframes in single query
        
        Returns:
            Dict: {token_address: {timeframe: session_info_dict or None}}
        """
        try:
            if not token_addresses or not timeframes:
                return {}
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT tokenaddress, timeframe, sessionstartunix, sessionendunix, 
                           cumulativepv, cumulativevolume, currentvwap, lastcandleunix, nextcandlefetch
                    FROM vwapsessions 
                    WHERE tokenaddress = ANY(%s) AND timeframe = ANY(%s)
                """), (token_addresses, timeframes))
                
                # Initialize result structure
                results = {}
                for token_address in token_addresses:
                    results[token_address] = {}
                    for timeframe in timeframes:
                        results[token_address][timeframe] = None
                
                # Populate with actual data
                for row in cursor.fetchall():
                    token_address = row['tokenaddress']
                    timeframe = row['timeframe']
                    results[token_address][timeframe] = {
                        'sessionstartunix': row['sessionstartunix'],
                        'sessionendunix': row['sessionendunix'], 
                        'cumulativepv': Decimal(str(row['cumulativepv'])),
                        'cumulativevolume': Decimal(str(row['cumulativevolume'])),
                        'currentvwap': Decimal(str(row['currentvwap'])),
                        'lastcandleunix': row['lastcandleunix'],
                        'nextcandlefetch': row['nextcandlefetch']
                    }
                
                return results
        except Exception as e:
            logger.error(f"Error getting batch VWAP session info: {e}")
            return {}
    
    def getAllLastFetchTimes(self, tokenAddresses: List[str], timeframes: List[str]) -> Dict[str, Dict[str, Optional[int]]]:
        """
        Get last fetch times for multiple tokens and timeframes in single query
        
        Returns:
            Dict: {token_address: {timeframe: unix_timestamp or None}}
        """
        try:
            if not tokenAddresses or not timeframes:
                return {}
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT tokenaddress, timeframe, lastfetchedat as lastfetchedat_unix
                    FROM timeframemetadata 
                    WHERE tokenaddress = ANY(%s) AND timeframe = ANY(%s)
                """), (tokenAddresses, timeframes))
                
                # Initialize result structure
                results = {}
                for tokenAddress in tokenAddresses:
                    results[tokenAddress] = {}
                    for timeframe in timeframes:
                        results[tokenAddress][timeframe] = None
                
                # Populate with actual data
                for row in cursor.fetchall():
                    tokenAddress = row['tokenaddress']
                    timeframe = row['timeframe']
                    unixTime = int(row['lastfetchedat_unix']) if row['lastfetchedat_unix'] else None
                    results[tokenAddress][timeframe] = unixTime
                
                return results
        except Exception as e:
            logger.error(f"Error getting batch last fetch times: {e}")
            return {}
    
    
    def getAllEMAStateInfo(self, token_addresses: List[str], timeframes: List[str], ema_periods: List[int]) -> Dict[str, Dict[str, Dict[int, Optional[Dict]]]]:
        """
        Get EMA state information for multiple tokens, timeframes, and periods in single query
        
        Returns:
            Dict: {token_address: {timeframe: {ema_period: ema_state_dict or None}}}
        """
        try:
            if not token_addresses or not timeframes or not ema_periods:
                return {}
            
            ema_period_strings = [str(p) for p in ema_periods]
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT tokenaddress, timeframe, emakey, emavalue, status, 
                           lastupdatedunix, nextfetchtime, emaavailabletime
                    FROM emastates 
                    WHERE tokenaddress = ANY(%s) AND timeframe = ANY(%s) AND emakey = ANY(%s)
                """), (token_addresses, timeframes, ema_period_strings))
                
                # Initialize result structure
                results = {}
                for token_address in token_addresses:
                    results[token_address] = {}
                    for timeframe in timeframes:
                        results[token_address][timeframe] = {}
                        for ema_period in ema_periods:
                            results[token_address][timeframe][ema_period] = None
                
                # Populate with actual data
                for row in cursor.fetchall():
                    token_address = row['tokenaddress']
                    timeframe = row['timeframe']
                    ema_period = int(row['emakey'])
                    
                    results[token_address][timeframe][ema_period] = {
                        'emavalue': Decimal(str(row['emavalue'])) if row['emavalue'] else None,
                        'status': row['status'],
                        'lastupdatedunix': row['lastupdatedunix'],
                        'nextfetchtime': row['nextfetchtime'],
                        'emaavailabletime': row['emaavailabletime']
                    }
                
                return results
        except Exception as e:
            logger.error(f"Error getting batch EMA state info: {e}")
            return {}
    
    
    
    def getBatchVWAPCandles(self, vwapOperations: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Get all required VWAP candles for multiple operations in single query
        
        Args:
            vwap_operations: List of operations with keys:
                - token_address, timeframe, operation_type, from_time, to_time (optional)
        x
        Returns:
            Dict: {f"{token_address}_{timeframe}_{operation_type}": [candles]}
        """
        try:
            if not vwapOperations:
                return {}
            
            # Build UNION query for all different candle requirements
            union_queries = []
            params = []
            
            for i, op in enumerate(vwapOperations):
                operation_id = f"{op['token_address']}_{op['timeframe']}_{op['operation_type']}"
                
                if op['operation_type'] == 'new_session':
                    # Get all candles since pair creation
                    union_queries.append(f"""
                        SELECT '{operation_id}' as operation_id, 
                               unixtime, highprice, lowprice, closeprice, volume
                        FROM ohlcvdetails 
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime >= %s
                            AND iscomplete = TRUE
                    """)
                    params.extend([op['token_address'], op['timeframe'], op['from_time']])
                
                elif op['operation_type'] == 'same_day_update':
                    # Get candles after last candle time
                    union_queries.append(f"""
                        SELECT '{operation_id}' as operation_id,
                               unixtime, highprice, lowprice, closeprice, volume
                        FROM ohlcvdetails 
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime > %s
                            AND iscomplete = TRUE
                    """)
                    params.extend([op['token_address'], op['timeframe'], op['from_time']])
                
                elif op['operation_type'] == 'new_day_reset':
                    # Get candles for specific date range
                    union_queries.append(f"""
                        SELECT '{operation_id}' as operation_id,
                               unixtime, highprice, lowprice, closeprice, volume
                        FROM ohlcvdetails 
                        WHERE tokenaddress = %s AND timeframe = %s 
                            AND unixtime >= %s AND unixtime <= %s
                            AND iscomplete = TRUE
                    """)
                    params.extend([op['token_address'], op['timeframe'], op['from_time'], op['to_time']])
            
            if not union_queries:
                return {}
            
            # Execute single UNION query
            final_query = " UNION ALL ".join(union_queries) + " ORDER BY operation_id, unixtime ASC"
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text(final_query), params)
                
                # Group results by operation_id
                results = {}
                for row in cursor.fetchall():
                    operation_id = row['operation_id']
                    if operation_id not in results:
                        results[operation_id] = []
                    
                    results[operation_id].append({
                        'unixtime': row['unixtime'],
                        'highprice': float(row['highprice']),
                        'lowprice': float(row['lowprice']),
                        'closeprice': float(row['closeprice']),
                        'volume': float(row['volume'])
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"Error getting batch VWAP candles: {e}")
            return {}
    
    def batchUpdateVWAPData(self, vwapCandleUpdatedData: List[Dict], vwapSessionUpdatedData: List[Dict]) -> bool:
        """
        Batch update VWAP values and sessions for all tokens at once
        
        Args:
            vwap_updates: List of candle VWAP updates 
            session_updates: List of session updates/creates
        
        Returns:
            bool: True if successful
        """
        try:
            with self.conn_manager.transaction() as cursor:
                # STEP 1: Batch update OHLCV VWAP values
                if vwapCandleUpdatedData:
                    vwapCandleData = []
                    for update in vwapCandleUpdatedData:
                        vwapCandleData.append((
                            float(update['vwap']),
                            update['token_address'],
                            update['timeframe'], 
                            update['unixtime']
                        ))
                    
                    cursor.executemany(text("""
                        UPDATE ohlcvdetails 
                        SET vwapvalue = %s, lastupdatedat = NOW()
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """), vwapCandleData)
                
                # STEP 2: Batch update/insert VWAP sessions
                if vwapSessionUpdatedData:
                    vwapSessionData = []
                    for update in vwapSessionUpdatedData:
                        vwapSessionData.append((
                            update['token_address'],
                            update['pair_address'],
                            update['timeframe'],
                            update['session_start'],
                            update['session_end'],
                            float(update['cumulative_pv']),
                            float(update['cumulative_volume']),
                            float(update['final_vwap']),
                            update['latest_candle_time'],
                            update['next_candle_fetch']
                        ))
                    
                    cursor.executemany(text("""
                        INSERT INTO vwapsessions 
                        (tokenaddress, pairaddress, timeframe, sessionstartunix, sessionendunix,
                         cumulativepv, cumulativevolume, currentvwap, lastcandleunix, nextcandlefetch,
                         createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (tokenaddress, timeframe) 
                        DO UPDATE SET
                            sessionstartunix = EXCLUDED.sessionstartunix,
                            sessionendunix = EXCLUDED.sessionendunix,
                            cumulativepv = EXCLUDED.cumulativepv,
                            cumulativevolume = EXCLUDED.cumulativevolume,
                            currentvwap = EXCLUDED.currentvwap,
                            lastcandleunix = EXCLUDED.lastcandleunix,
                            nextcandlefetch = EXCLUDED.nextcandlefetch,
                            lastupdatedat = NOW()
                    """), vwapSessionData)
                
                return True
                
        except Exception as e:
            logger.error(f"Error in batch VWAP update: {e}")
            return False
    
    def getAllCandlesNeededToCalculateEMA(self, infoNeededToCalculateEMA: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Get all required EMA candles for multiple operations in single query
        
        Args:
            ema_operations: List of operations with keys:
                - token_address, timeframe, operation_type, from_time, ema_period
        
        Returns:
            Dict: {f"{token_address}_{timeframe}_{ema_period}_{operation_type}": [candles]}
        """
        try:
            if not infoNeededToCalculateEMA:
                return {}
            
            # Build UNION query for all different candle requirements
            union_queries = []
            params = []
            
            for op in infoNeededToCalculateEMA:
                operation_id = f"{op['token_address']}_{op['timeframe']}_{op['ema_period']}_{op['operation_type']}"
                
                if op['operation_type'] == 'first_calculation':
                    # Get all candles since ema available time
                    union_queries.append(f"""
                        SELECT '{operation_id}' as operation_id,
                               unixtime, closeprice
                        FROM ohlcvdetails 
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime >= %s
                            AND iscomplete = TRUE
                    """)
                    params.extend([op['token_address'], op['timeframe'], op['from_time']])
                
                elif op['operation_type'] == 'incremental_update':
                    # Get candles after last updated time
                    union_queries.append(f"""
                        SELECT '{operation_id}' as operation_id,
                               unixtime, closeprice
                        FROM ohlcvdetails 
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime > %s
                            AND iscomplete = TRUE
                    """)
                    params.extend([op['token_address'], op['timeframe'], op['from_time']])
            
            if not union_queries:
                return {}
            
            # Execute single UNION query
            final_query = " UNION ALL ".join(union_queries) + " ORDER BY operation_id, unixtime ASC"
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text(final_query), params)
                
                # Group results by operation_id
                results = {}
                for row in cursor.fetchall():
                    operation_id = row['operation_id']
                    if operation_id not in results:
                        results[operation_id] = []
                    
                    results[operation_id].append({
                        'unixtime': row['unixtime'],
                        'closeprice': float(row['closeprice'])
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"Error getting batch EMA candles: {e}")
            return {}
    
    def batchInsertAggregatedCandlesWithTimeframeUpdate(self, aggregatedCandles: Dict, tokenAddress: str, pairAddress: str) -> bool:
        """
        Optimized method: Insert aggregated candles AND create/update timeframe records in single transaction
        Eliminates the need for separate create -> update operations
        """
        try:
            with self.conn_manager.transaction() as cursor:
                hourlyCandles = aggregatedCandles['hourly_candles']
                fourHourCandles = aggregatedCandles['four_hourly_candles']
                latestFetchedTimeFor1Hr = aggregatedCandles['latest_1h_time']
                latestFetchedTimeFor4Hr = aggregatedCandles['latest_4h_time']
                nextFetchTimeFor1Hr = aggregatedCandles['next_fetch_1h_time']
                nextFetchTimeFor4Hr = aggregatedCandles['next_fetch_4h_time']
                
                # 1. Insert hourly candles if any
                if hourlyCandles and latestFetchedTimeFor1Hr:
                    # Insert candles
                    insertQueryDataForHourlyCandles = []
                    for candle in hourlyCandles:
                        insertQueryDataForHourlyCandles.append((
                            tokenAddress, pairAddress, '1h',
                            candle['unixtime'], candle['openprice'], candle['highprice'],
                            candle['lowprice'], candle['closeprice'], candle['volume']
                        ))
                    
                    cursor.executemany(text("""
                        INSERT INTO ohlcvdetails 
                        (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
                         highprice, lowprice, closeprice, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                    """), insertQueryDataForHourlyCandles)
                    
                    # Create/update timeframe record with pre-calculated next fetch time in one operation
                    cursor.execute(text("""
                        INSERT INTO timeframemetadata 
                        (tokenaddress, pairaddress, timeframe, lastfetchedat, nextfetchat, isactive, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s, NOW(), NOW())
                        ON CONFLICT (tokenaddress, pairaddress, timeframe)
                        DO UPDATE SET 
                            lastfetchedat = to_timestamp(%s),
                            nextfetchat = to_timestamp(%s),
                            lastupdatedat = NOW()
                    """), (tokenAddress, pairAddress, '1h', 
                           latestFetchedTimeFor1Hr, nextFetchTimeFor1Hr, True,
                           latestFetchedTimeFor1Hr, nextFetchTimeFor1Hr))
                    
                    logger.info(f"Inserted {len(hourlyCandles)} hourly candles and updated timeframe in single operation")
                
                # 2. Insert 4-hourly candles if any
                if fourHourCandles and latestFetchedTimeFor4Hr:
                    # Insert candles
                    insertQueryDataForFourHourlyCandles = []
                    for candle in fourHourCandles:
                        insertQueryDataForFourHourlyCandles.append((
                            tokenAddress, pairAddress, '4h',
                            candle['unixtime'], candle['openprice'], candle['highprice'],
                            candle['lowprice'], candle['closeprice'], candle['volume']
                        ))
                    
                    cursor.executemany(text("""
                        INSERT INTO ohlcvdetails 
                        (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
                         highprice, lowprice, closeprice, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                    """), insertQueryDataForFourHourlyCandles)
                    
                    # Create/update timeframe record with pre-calculated next fetch time in one operation
                    cursor.execute(text("""
                        INSERT INTO timeframemetadata 
                        (tokenaddress, pairaddress, timeframe, lastfetchedat, nextfetchat, isactive, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s, NOW(), NOW())
                        ON CONFLICT (tokenaddress, pairaddress, timeframe)
                        DO UPDATE SET 
                            lastfetchedat = to_timestamp(%s),
                            nextfetchat = to_timestamp(%s),
                            lastupdatedat = NOW()
                    """), (tokenAddress, pairAddress, '4h', 
                           latestFetchedTimeFor4Hr, nextFetchTimeFor4Hr, True,
                           latestFetchedTimeFor4Hr, nextFetchTimeFor4Hr))
                    
                    logger.info(f"Inserted {len(fourHourCandles)} 4-hourly candles and updated timeframe in single operation")
                
                logger.info("All aggregation operations completed in optimized single transaction")
                return True
                
        except Exception as e:
            logger.error(f"Error in optimized aggregation transaction: {e}")
            return False

    def batchUpdateEMAData(self, emaCandlesUpdatedData: List[Dict], emaStateUpdatedData: List[Dict]) -> bool:
        """
        Batch update EMA values and states for all tokens at once
        
        Args:
            ema_updates: List of candle EMA updates 
            state_updates: List of EMA state updates
        
        Returns:
            bool: True if successful
        """
        try:
            with self.conn_manager.transaction() as cursor:
                # STEP 1: Batch update OHLCV EMA values
                if emaCandlesUpdatedData:
                    # Group by EMA period for efficient updates
                    ema21Data = []
                    ema34Data = []
                    
                    for update in emaCandlesUpdatedData:
                        if update['ema_period'] == 21:
                            ema21Data.append((
                                float(update['ema_value']),
                                update['token_address'],
                                update['timeframe'],
                                update['unixtime']
                            ))
                        elif update['ema_period'] == 34:
                            ema34Data.append((
                                float(update['ema_value']),
                                update['token_address'],
                                update['timeframe'],
                                update['unixtime']
                            ))
                    
                    # Batch update EMA21 values
                    if ema21Data:
                        cursor.executemany(text("""
                            UPDATE ohlcvdetails 
                            SET ema21value = %s, lastupdatedat = NOW()
                            WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                        """), ema21Data)
                    
                    # Batch update EMA34 values
                    if ema34Data:
                        cursor.executemany(text("""
                            UPDATE ohlcvdetails 
                            SET ema34value = %s, lastupdatedat = NOW()
                            WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                        """), ema34Data)
                
                # STEP 2: Batch update EMA states
                if emaStateUpdatedData:
                    emaStateData = []
                    for update in emaStateUpdatedData:
                        # Calculate next fetch time
                        timeframeSeconds = TradingActionUtil.getTimeframeSeconds(update['timeframe'])
                        nextFetchTime = update['latest_time'] + timeframeSeconds
                        
                        emaStateData.append((
                            float(update['latest_ema']),
                            update['latest_time'],
                            nextFetchTime,
                            update['status'],
                            update['token_address'],
                            update['timeframe'],
                            str(update['ema_period'])
                        ))
                    
                    cursor.executemany(text("""
                        UPDATE emastates 
                        SET emavalue = %s, lastupdatedunix = %s, nextfetchtime = %s, 
                            status = %s, lastupdatedat = NOW()
                        WHERE tokenaddress = %s AND timeframe = %s AND emakey = %s
                    """), emaStateData)
                
                return True
                
        except Exception as e:
            logger.error(f"Error in batch EMA update: {e}")
            return False


    def getAllCandlesFromAllTimeframes(self, tokenAddress: str, pairAddress: str) -> Dict[str, List[Dict]]:
        """Get ALL candles for all available timeframes in single database call"""
        try:
            with self.conn_manager.transaction() as cursor:
                # Get all candles for all available timeframes with JOIN
                cursor.execute(text("""
                    SELECT tm.timeframe, 
                           ohlcv.unixtime, ohlcv.openprice, ohlcv.highprice, 
                           ohlcv.lowprice, ohlcv.closeprice, ohlcv.volume
                    FROM timeframemetadata tm
                    INNER JOIN ohlcvdetails ohlcv ON (
                        tm.tokenaddress = ohlcv.tokenaddress AND 
                        tm.pairaddress = ohlcv.pairaddress AND 
                        tm.timeframe = ohlcv.timeframe
                    )
                    WHERE tm.tokenaddress = %s AND tm.pairaddress = %s 
                    AND tm.isactive = true
                    ORDER BY tm.timeframe, ohlcv.unixtime ASC
                """), (tokenAddress, pairAddress))
                
                results = cursor.fetchall()
                
                # Group candles by timeframe
                timeframeCandlesMap = {}
                for row in results:
                    timeframe = row['timeframe']
                    if timeframe not in timeframeCandlesMap:
                        timeframeCandlesMap[timeframe] = []
                    
                    timeframeCandlesMap[timeframe].append({
                        'unixtime': row['unixtime'],
                        'openprice': row['openprice'],
                        'highprice': row['highprice'],
                        'lowprice': row['lowprice'],
                        'closeprice': row['closeprice'],
                        'volume': row['volume']
                    })
                return timeframeCandlesMap
                
        except Exception as e:
            logger.error(f"Error getting all candles for all timeframes: {e}")
            return {}

    def recordVwapCandleUpdateAndVwapSessionUpdateFromAPI(self, tokenAddress: str, pairAddress: str, calcuatedVwapData: List[Dict]) -> Dict:
        """
        API FLOW DATABASE OPERATION: Execute all VWAP operations (candle updates + session creation) in single SQL transaction
        Properly updates individual candles with their corresponding VWAP values instead of using final VWAP for all candles
        """
        try:
            with self.conn_manager.transaction() as cursor:
                # Prepare all VWAP updates in single batch
                vwapCandleUpdateData = []
                vwapSessionUpdateData = []
                
                for operation in calcuatedVwapData:
                    timeframe = operation['timeframe']
                    calculatedVwap = operation['vwap_result']
                    nextFetchAtTime = operation['next_candle_fetch']
                    dayStart = operation['day_start']
                    
                    # FIXED: Update each candle with its corresponding VWAP value instead of final VWAP for all
                    for candle_vwap in calculatedVwap['candle_vwaps']:
                        vwapCandleUpdateData.append((
                            float(candle_vwap['vwap']),  # Use individual candle VWAP
                            tokenAddress,
                            pairAddress,
                            timeframe,
                            candle_vwap['unixtime']
                        ))
                    
                    # Collect VWAP session data for batch execution
                    dayEnd = dayStart + 86400
                    vwapSessionUpdateData.append((
                        tokenAddress, pairAddress, timeframe, dayStart, dayEnd,
                        float(calculatedVwap.get('cumulative_pv', 0)), 
                        float(calculatedVwap.get('cumulative_volume', 0)),
                        float(calculatedVwap.get('final_vwap', 0)), 
                        calculatedVwap.get('latest_candle_time', dayStart),
                        nextFetchAtTime
                    ))
                
                # Single batch update for all VWAP values across all timeframes
                if vwapCandleUpdateData:
                    cursor.executemany(text("""
                        UPDATE ohlcvdetails 
                        SET vwapvalue = %s
                        WHERE tokenaddress = %s AND pairaddress = %s AND timeframe = %s AND unixtime = %s
                    """), vwapCandleUpdateData)
                
                # Single batch insert/update for all VWAP sessions across all timeframes
                if vwapSessionUpdateData:
                    cursor.executemany(text("""
                        INSERT INTO vwapsessions 
                        (tokenaddress, pairaddress, timeframe, sessionstartunix, sessionendunix,
                         cumulativepv, cumulativevolume, currentvwap, lastcandleunix, nextcandlefetch)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, sessionstartunix) 
                        DO UPDATE SET 
                            cumulativepv = EXCLUDED.cumulativepv,
                            cumulativevolume = EXCLUDED.cumulativevolume,
                            currentvwap = EXCLUDED.currentvwap,
                            lastcandleunix = EXCLUDED.lastcandleunix,
                            nextcandlefetch = EXCLUDED.nextcandlefetch,
                            lastupdatedat = NOW()
                    """), vwapSessionUpdateData)
                
                logger.info(f"API VWAP operations completed: {len(vwapCandleUpdateData)} candle updates with individual VWAP values, {len(vwapSessionUpdateData)} session updates")
                return {'success': True}
                
        except Exception as e:
            logger.error(f"Error in API VWAP operations: {e}")
            return {'success': False, 'error': str(e)}



    def getAll15MinCandlesWithoutTimeRange(self, tokenAddress: str, pairAddress: str) -> List[Dict]:
        """Get all 15min candles for aggregation"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT unixtime, openprice, highprice, lowprice, closeprice, volume
                    FROM ohlcvdetails 
                    WHERE tokenaddress = %s AND pairaddress = %s AND timeframe = '15m' 
                    ORDER BY unixtime ASC
                """), (tokenAddress, pairAddress))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting 15min candles: {e}")
            return []



    def updateEMA(self, emaStateUpdatedData: List[Dict], emaCandleUpdatedData: List[Dict]):
        """
        API FLOW HELPER: Execute all EMA operations (state creation + candle updates) in single transaction
        Optimized batch operation for API flow's initial EMA state and value creation
        """
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                from sqlalchemy import text
                
                # Batch insert/update EMA states
                if emaStateUpdatedData:
                    emaStateUpdateQueryData = []
                    for ema_state in emaStateUpdatedData:
                        emaStateUpdateQueryData.append((
                            ema_state['tokenAddress'],
                            ema_state['pairAddress'],
                            ema_state['timeframe'],
                            ema_state['emaKey'],
                            ema_state['emaValue'],
                            ema_state['lastUpdatedUnix'],
                            ema_state['nextFetchTime'],
                            ema_state['emaAvailableTime'],
                            ema_state['pairCreatedTime'],
                            int(ema_state['status'])
                        ))
                    
                    cursor.executemany(text("""
                        INSERT INTO emastates 
                        (tokenaddress, pairaddress, timeframe, emakey, emavalue, 
                         lastupdatedunix, nextfetchtime, emaavailabletime, paircreatedtime, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, emakey) 
                        DO UPDATE SET 
                            emavalue = EXCLUDED.emavalue,
                            lastupdatedunix = EXCLUDED.lastupdatedunix,
                            nextfetchtime = EXCLUDED.nextfetchtime,
                            status = EXCLUDED.status,
                            lastupdatedat = NOW()
                    """), emaStateUpdateQueryData)
                    
                    logger.info(f"Batch inserted/updated {len(emaStateUpdateQueryData)} EMA states")
                
                # Batch update ALL EMA values in single SQL operation using CASE statements
                if emaCandleUpdatedData:
                    # Prepare data for single update with CASE statements
                    emaCandleUpdateQueryData = []
                    for update in emaCandleUpdatedData:
                        emaCandleUpdateQueryData.append((
                            update['ema_value'],
                            update['ema_period'],
                            update['ema_value'],
                            update['ema_period'],
                            update['tokenAddress'],
                            update['timeframe'],
                            update['unixtime']
                        ))
                    
                    # Single SQL call to update both EMA21 and EMA34 values using CASE
                    cursor.executemany(text("""
                        UPDATE ohlcvdetails 
                        SET ema21value = CASE WHEN %s = 21 THEN %s ELSE ema21value END,
                            ema34value = CASE WHEN %s = 34 THEN %s ELSE ema34value END
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """), emaCandleUpdateQueryData)
                    
                    logger.info(f"Batch updated {len(emaCandleUpdateQueryData)} EMA candle values in single operation")
                
                logger.info(f"All EMA operations completed in 2 SQL calls: {len(emaStateUpdatedData)} states, {len(emaCandleUpdatedData)} candle updates")
                
        except Exception as e:
            logger.error(f"Error in batch EMA operations: {e}")
            raise