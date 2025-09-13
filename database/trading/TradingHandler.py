from config.Config import get_config
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import json
from database.operations.BaseDBHandler import BaseDBHandler
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from logs.logger import get_logger
from sqlalchemy import text
from enum import IntEnum
from datetime import datetime, timezone, timedelta
from actions.TradingActionUtil import TradingActionUtil
from scheduler.SchedulerConstants import CandleDataKeys, Timeframes
from constants.TradingConstants import TimeframeConstants
from constants.VWAPConstants import *
from constants.EMAConstants import *
import time

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
                timeframe VARCHAR(10) NOT NULL,
                nextfetchat BIGINT NOT NULL,
                lastfetchedat BIGINT,
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
                timeframe VARCHAR(10) NOT NULL,
                unixtime BIGINT NOT NULL,
                timebucket BIGINT NOT NULL,
                openprice DECIMAL(20,8) NOT NULL,
                highprice DECIMAL(20,8) NOT NULL,
                lowprice DECIMAL(20,8) NOT NULL,
                closeprice DECIMAL(20,8) NOT NULL,
                volume DECIMAL(20,4) NOT NULL,
                trades INTEGER DEFAULT 0,
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
                timeframe VARCHAR(10),
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
                timeframe VARCHAR(10),
                sessionstartunix BIGINT,
                sessionendunix BIGINT,
                cumulativepv DECIMAL(30,8),
                cumulativevolume DECIMAL(30,8),
                currentvwap DECIMAL(20,8),
                lastcandleunix BIGINT,
                nextcandlefetch BIGINT,
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (tokenaddress, timeframe)
            )
        """))
        

    def getTableDocumentation(self, tableName: str) -> dict:
        """Get documentation for a specific table"""
        return self.schema.get(tableName, {})

    
    
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
                """), (fetchTime, next_fetch, tokenAddress, timeframe))
                
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
            now = datetime.now(timezone.utc)
            
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
            now = datetime.now(timezone.utc)
            
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
                          AND tm.nextfetchat <= EXTRACT(EPOCH FROM NOW())
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
                            COALESCE(tm1h.lastfetchedat, 0) as lastfetchedat_1h,
                            COALESCE(tm4h.lastfetchedat, 0) as lastfetchedat_4h,
                            COALESCE(tm1h.nextfetchat, 0) as nextfetchat_1h,
                            COALESCE(tm4h.nextfetchat, 0) as nextfetchat_4h,
                            LEAST(
                                COALESCE(tm1h.nextfetchat, 0),
                                COALESCE(tm4h.nextfetchat, 0)
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
                candle['lowprice'], candle['closeprice'], candle['volume'], 
                int(candle.get('trades', 0)), True  # trades, iscomplete=True
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
        cursor.executemany("""
            INSERT INTO ohlcvdetails 
            (tokenaddress, pairaddress, timeframe, unixtime, timebucket, openprice, 
             highprice, lowprice, closeprice, volume, trades, datasource, iscomplete, createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'aggregated', %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
        """, insertData)
    
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
            
        cursor.executemany("""
            INSERT INTO timeframemetadata 
            (tokenaddress, pairaddress, timeframe, nextfetchat, lastfetchedat, createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, pairaddress, timeframe)
            DO UPDATE SET 
                nextfetchat = %s,
                lastfetchedat = %s,
                lastupdatedat = NOW()
        """, updateData)
        
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
                           openprice, highprice, lowprice, closeprice, volume, trades
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
                        'volume': float(row['volume']),
                        'trades': int(row['trades'])
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"Error getting {days}-day historical data: {e}")
            return {}
    
    def batchPersistAllCandles(self, candleData: Dict[str, Dict]) -> int:
        """
        OPTIMIZED: Batch persist ALL candles from multiple tokens with multiple timeframes
        
        NEW FLOW COMPATIBLE:
        - Handles keys like "token_address_timeframe" format
        - Supports multiple timeframes per token (30min, 1h, 4h)
        - Updates timeframemetadata with proper lastfetchedat and nextfetchat
        
        Args:
            candleData: Dict mapping "token_address_timeframe" -> {
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
                # STEP 1: Parse data and build update structures
                timeframeUpdateData = []
                ohlcvDetailsData = []
                
                for key, data in candleData.items():
                    candles = data[CandleDataKeys.CANDLES]
                    if not candles:
                        continue
                    
                    # Extract token address and timeframe from key
                    tokenAddress, timeframe = self.findTokenAddressAndTimeframeFromKey(key)
                    if not tokenAddress or not timeframe:
                        logger.warning(f"Invalid candle data key format: {key}")
                        continue
                        
                    # Get metadata from first candle and data
                    firstCandle = candles[0]
                    pairAddress = firstCandle['pairaddress']
                    latestTime = data[CandleDataKeys.LATEST_TIME]
                    
                    # Calculate next fetch time based on timeframe
                    nextFetchTime = self.calculateNextFetchTimeForTimeframe(latestTime, timeframe)
                    
                    # Prepare timeframe update data
                    timeframeUpdateData.append((
                        tokenAddress, pairAddress, timeframe,
                        latestTime, nextFetchTime
                    ))
                    
                    # Prepare candle insert data
                    for candle in candles:
                        ohlcvDetailsData.append((
                            candle['tokenaddress'], candle['pairaddress'], candle['timeframe'],
                            candle['unixtime'], candle['openprice'], candle['highprice'],
                            candle['lowprice'], candle['closeprice'], candle['volume'], 
                            int(candle.get('trades', 0)), candle['datasource']
                        ))
                        totalCandles += 1
                
                # STEP 2: Update timeframe metadata and get IDs
                timeframePKIds = self.batchUpdateTimeframe(cursor, timeframeUpdateData)
                
                # STEP 3: Insert candles with timeframe IDs
                if ohlcvDetailsData:
                    self.batchRecordCandlesIntoOHLCV(cursor, ohlcvDetailsData, timeframePKIds)
            
            logger.info(f"Batch persisted {totalCandles} candles across {len(candleData)} token-timeframe combinations")
            return totalCandles
            
        except Exception as e:
            logger.error(f"Error in batch persist candles: {e}")
            return 0
    
    def findTokenAddressAndTimeframeFromKey(self, key: str) -> Tuple[str, str]:
        """Parse candle data key to extract token address and timeframe"""
        try:
            # Key format: "token_address_timeframe" 
            parts = key.rsplit('_', 1)  # Split from right to handle addresses with underscores
            if len(parts) == 2:
                return parts[0], parts[1]
            else:
                # Fallback: assume key is just token address (old format)
                return key, '15m'
        except Exception:
            return None, None
    
    def calculateNextFetchTimeForTimeframe(self, latestTime: int, timeframe: str) -> int:
        """Calculate next fetch time based on specific timeframe"""
        timeframe_seconds_map = { # needs to change
            '15m': 900,
            '30min': 1800, 
            '1h': 3600,
            '4h': 14400
        }
        
        seconds = timeframe_seconds_map.get(timeframe, 900)
        return latestTime + seconds
    
    def batchUpdateTimeframe(self, cursor, timeframeUpdateData: List[Tuple]) -> Dict[Tuple, int]:
        """Update timeframe metadata and return mapping of (token, timeframe) -> id"""
        timeframePKIds = {}
        
        if not timeframeUpdateData:
            return timeframePKIds
        
        for tokenAddress, pairAddress, timeframe, lastFetchTime, nextFetchTime in timeframeUpdateData:
            cursor.execute("""
                INSERT INTO timeframemetadata 
                (tokenaddress, pairaddress, timeframe, lastfetchedat, nextfetchat, createdat, lastupdatedat)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (tokenaddress, pairaddress, timeframe) 
                DO UPDATE SET 
                    lastfetchedat = %s,
                    nextfetchat = %s,
                    lastupdatedat = NOW()
                RETURNING id
            """, (
                tokenAddress, pairAddress, timeframe,
                lastFetchTime, nextFetchTime,
                lastFetchTime, nextFetchTime
            ))
            
            result = cursor.fetchone()
            timeframe_id = result['id']
            timeframePKIds[(tokenAddress, timeframe)] = timeframe_id
        
        return timeframePKIds
    
    def batchRecordCandlesIntoOHLCV(self, cursor, ohlcvDetailsData: List[Tuple], timeframeIds: Dict[Tuple, int]):
        """Insert candles with proper timeframe IDs and timebuckets"""
        if not ohlcvDetailsData:
            return
        
        candleInsertData = []
        
        for candle in ohlcvDetailsData:
            tokenAddress = candle[0]
            timeframe = candle[2]
            timeframePK = timeframeIds.get((tokenAddress, timeframe))
            
            if timeframePK:
                unixtime = candle[3]
                timebucket = self._calculateTimeBucket(unixtime, timeframe)
                
                candleInsertData.append((
                    timeframePK,    # timeframeid
                    candle[0],  # tokenaddress
                    candle[1],  # pairaddress
                    candle[2],  # timeframe
                    candle[3],  # unixtime
                    timebucket,      # timebucket
                    candle[4],  # openprice
                    candle[5],  # highprice
                    candle[6],  # lowprice
                    candle[7],  # closeprice
                    candle[8],  # volume
                    candle[9],  # trades
                    candle[10]   # datasource
                ))
        
        if candleInsertData:
            cursor.executemany("""
                INSERT INTO ohlcvdetails 
                (timeframeid, tokenaddress, pairaddress, timeframe, unixtime, timebucket, 
                 openprice, highprice, lowprice, closeprice, volume, trades, datasource)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
            """, candleInsertData)
    

    
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
    
    def getAllVWAPDataForScheduler(self) -> Dict[str, Dict[str, Dict]]:
        """
        Get all VWAP data for scheduler processing - all active tokens and timeframes.
        
        Returns:
            Dict: {token_address: {timeframe: vwap_data_dict}}
        """
        try:
            with self.conn_manager.transaction() as cursor:
                # Get all active tokens with their timeframes and VWAP session data
                cursor.execute(text("""
                    SELECT 
                        tt.tokenaddress,
                        tt.pairaddress,
                        tm.timeframe,
                        tm.lastfetchedat,
                        vs.sessionstartunix,
                        vs.sessionendunix,
                        vs.cumulativepv,
                        vs.cumulativevolume,
                        vs.currentvwap,
                        vs.lastcandleunix,
                        vs.nextcandlefetch,
                        ohlcv.unixtime,
                        ohlcv.openprice,
                        ohlcv.highprice,
                        ohlcv.lowprice,
                        ohlcv.closeprice,
                        ohlcv.volume
                    FROM trackedtokens tt
                    INNER JOIN timeframemetadata tm ON tt.tokenaddress = tm.tokenaddress
                    LEFT JOIN vwapsessions vs ON tt.tokenaddress = vs.tokenaddress AND tm.timeframe = vs.timeframe
                    LEFT JOIN ohlcvdetails ohlcv ON tt.tokenaddress = ohlcv.tokenaddress 
                        AND tm.timeframe = ohlcv.timeframe 
                        AND ohlcv.unixtime > COALESCE(vs.lastcandleunix, 0)
                    WHERE tt.status = 1
                    ORDER BY tt.tokenaddress, tm.timeframe, ohlcv.unixtime
                """))
                
                records = cursor.fetchall()
                
                # Group data by token and timeframe
                vwapDataByToken = {}
                
                for record in records:
                    tokenAddress = record['tokenaddress']
                    pairAddress = record['pairaddress']
                    timeframe = record['timeframe']
                    
                    if tokenAddress not in vwapDataByToken:
                        vwapDataByToken[tokenAddress] = {}
                    
                    if timeframe not in vwapDataByToken[tokenAddress]:
                        # Initialize timeframe data
                        vwapDataByToken[tokenAddress][timeframe] = {
                            'tokenAddress': tokenAddress,
                            'pairAddress': pairAddress,
                            'timeframe': timeframe,
                            'lastFetchedAt': record['lastfetchedat'],
                            'sessionStartUnix': record['sessionstartunix'],
                            'sessionEndUnix': record['sessionendunix'],
                            'cumulativePV': record['cumulativepv'],
                            'cumulativeVolume': record['cumulativevolume'],
                            'currentVWAP': record['currentvwap'],
                            'lastCandleUnix': record['lastcandleunix'] or 0,
                            'nextCandleFetch': record['nextcandlefetch'],
                            'candles': []
                        }
                    
                    # Add candle data if available
                    if record['unixtime'] is not None:
                        candle = {
                            'unixtime': record['unixtime'],
                            'openprice': record['openprice'],
                            'highprice': record['highprice'],
                            'lowprice': record['lowprice'],
                            'closeprice': record['closeprice'],
                            'volume': record['volume']
                        }
                        vwapDataByToken[tokenAddress][timeframe]['candles'].append(candle)
                
                logger.info(f"Retrieved VWAP data for {len(vwapDataByToken)} active tokens")
                return vwapDataByToken
                
        except Exception as e:
            logger.error(f"Error getting all VWAP data for scheduler: {e}")
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
                            float(update[VWAP_VALUE]),
                            update[TOKEN_ADDRESS],
                            update[TIMEFRAME], 
                            update[CANDLE_UNIX]
                        ))
                    
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET vwapvalue = %s, lastupdatedat = NOW()
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """, vwapCandleData)
                
                # STEP 2: Batch update/insert VWAP sessions
                if vwapSessionUpdatedData:
                    vwapSessionData = []
                    for update in vwapSessionUpdatedData:
                        vwapSessionData.append((
                            update[TOKEN_ADDRESS],
                            update[PAIR_ADDRESS],
                            update[TIMEFRAME],
                            update[SESSION_START_UNIX],
                            update[SESSION_END_UNIX],
                            float(update[CUMULATIVE_PV]),
                            float(update[CUMULATIVE_VOLUME]),
                            float(update[CURRENT_VWAP]),
                            update[LAST_CANDLE_UNIX],
                            update[NEXT_CANDLE_FETCH]
                        ))
                    
                    cursor.executemany("""
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
                    """, vwapSessionData)
                
                return True
                
        except Exception as e:
            logger.error(f"Error in batch VWAP update: {e}")
            return False
    
    def getAllEMADataWithCandlesForScheduler(self, timeframes: List[str], emaPeriods: List[int]) -> Dict[str, Dict]:
        """
        SINGLE OPTIMIZED QUERY: Get all EMA data with corresponding candles for scheduler
        
        This method implements the new optimized approach:
        1. JOIN emastates with trackedtokens to get only active tokens
        2. JOIN with timeframemetadata to get lastfetchedat for each timeframe
        3. JOIN with ohlcvdetails to get candles where unixtime > lastupdatedunix
        4. All in one highly optimized query for scalability
        
        Args:
            timeframes: List of timeframes to process (e.g., ['15m', '1h', '4h'])
            ema_periods: List of EMA periods to process (e.g., [21, 34])
            
        Returns:
            Dict: {
                token_address: {
                    pair_id: pair_address,
                    ema21: {
                        timeframe: {
                            ema_value: current_ema_value,
                            last_updated_at: last_updated_unix,
                            status: ema_status,
                            ema_available_at: ema_available_time,
                            last_fetched_at: last_fetched_time,
                            candles: [list_of_candles]
                        }
                    },
                    ema34: { ... }
                }
            }
        """
        try:
            if not timeframes or not emaPeriods:
                return {}
            
            emaPeriodList = [str(p) for p in emaPeriods]
            
            with self.conn_manager.transaction() as cursor:
                # Single optimized query with JOINs
                cursor.execute(text("""
                    WITH ema_data AS (
                        SELECT 
                            es.tokenaddress,
                            es.pairaddress,
                            es.timeframe,
                            es.emakey,
                            es.emavalue,
                            es.status,
                            es.lastupdatedunix,
                            es.emaavailabletime,
                            tmf.lastfetchedat,
                            CASE 
                                WHEN es.status = 2 THEN es.lastupdatedunix  -- AVAILABLE: get candles after last updated
                                WHEN es.status = 1 AND tmf.lastfetchedat >= es.emaavailabletime THEN es.emaavailabletime  -- NOT_AVAILABLE_READY: get candles from available time
                                ELSE 0  -- NOT_AVAILABLE_INSUFFICIENT: no candles needed
                            END as candle_from_time
                        FROM emastates es
                        INNER JOIN trackedtokens tt ON es.tokenaddress = tt.tokenaddress AND es.pairaddress = tt.pairaddress
                        INNER JOIN timeframemetadata tmf ON es.tokenaddress = tmf.tokenaddress AND es.timeframe = tmf.timeframe
                        WHERE es.timeframe = ANY(%s) 
                          AND es.emakey = ANY(%s)
                          AND tt.status = 1
                          AND tmf.isactive = TRUE
                    ),
                    candle_data AS (
                        SELECT 
                            ed.tokenaddress,
                            ed.pairaddress,
                            ed.timeframe,
                            ed.emakey,
                            o.unixtime,
                            o.closeprice
                        FROM ema_data ed
                        INNER JOIN ohlcvdetails o ON ed.tokenaddress = o.tokenaddress AND ed.timeframe = o.timeframe
                        WHERE ed.candle_from_time > 0 
                          AND o.unixtime > ed.candle_from_time
                          AND o.iscomplete = TRUE
                    )
                    SELECT 
                        ed.tokenaddress,
                        ed.pairaddress,
                        ed.timeframe,
                        ed.emakey,
                        ed.emavalue,
                        ed.status,
                        ed.lastupdatedunix,
                        ed.emaavailabletime,
                        ed.lastfetchedat,
                        cd.unixtime as candle_unixtime,
                        cd.closeprice as candle_closeprice
                    FROM ema_data ed
                    LEFT JOIN candle_data cd ON ed.tokenaddress = cd.tokenaddress 
                        AND ed.timeframe = cd.timeframe 
                        AND ed.emakey = cd.emakey
                    WHERE ed.candle_from_time > 0
                    ORDER BY ed.tokenaddress, ed.timeframe, ed.emakey, cd.unixtime ASC
                """), (timeframes, emaPeriodList))
                
                # Organize results into the required structure
                results = {}
                
                for row in cursor.fetchall():
                    tokenAddress = row['tokenaddress']
                    pairAddress = row['pairaddress']
                    timeframe = row['timeframe']
                    emaKey = row['emakey']
                    emaPeriod = int(emaKey)
                    
                    # Initialize token structure if not exists
                    if tokenAddress not in results:
                        results[tokenAddress] = {
                            'pair_id': pairAddress,
                            'ema21': {},
                            'ema34': {}
                        }
                    
                    # Initialize EMA structure if not exists
                    ema_key_lower = f"ema{emaPeriod}"
                    if timeframe not in results[tokenAddress][ema_key_lower]:
                        results[tokenAddress][ema_key_lower][timeframe] = {
                            'ema_value': float(row['emavalue']) if row['emavalue'] else None,
                            'last_updated_at': row['lastupdatedunix'],
                            'status': row['status'],
                            'ema_available_at': row['emaavailabletime'],
                            'last_fetched_at': row['lastfetchedat'],
                            'candles': []
                        }
                    
                    # Add candle if exists
                    if row['candle_unixtime']:
                        results[tokenAddress][ema_key_lower][timeframe]['candles'].append({
                            'unixtime': row['candle_unixtime'],
                            'closeprice': float(row['candle_closeprice'])
                        })
                
                return results
                
        except Exception as e:
            logger.error(f"Error getting EMA data with candles for scheduler: {e}")
            return {}

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
                            candle['lowprice'], candle['closeprice'], candle['volume'],
                            int(candle.get('trades', 0))
                        ))
                    
                    cursor.executemany("""
                        INSERT INTO ohlcvdetails 
                        (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
                         highprice, lowprice, closeprice, volume, trades)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                    """, insertQueryDataForHourlyCandles)
                    
                    # Create/update timeframe record with pre-calculated next fetch time in one operation
                    cursor.execute(text("""
                        INSERT INTO timeframemetadata 
                        (tokenaddress, pairaddress, timeframe, lastfetchedat, nextfetchat, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (tokenaddress, pairaddress, timeframe)
                        DO UPDATE SET 
                            lastfetchedat = %s,
                            nextfetchat = %s,
                            lastupdatedat = NOW()
                    """), (tokenAddress, pairAddress, '1h', 
                           latestFetchedTimeFor1Hr, nextFetchTimeFor1Hr,
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
                            candle['lowprice'], candle['closeprice'], candle['volume'],
                            int(candle.get('trades', 0))
                        ))
                    
                    cursor.executemany("""
                        INSERT INTO ohlcvdetails 
                        (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
                         highprice, lowprice, closeprice, volume, trades)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                    """, insertQueryDataForFourHourlyCandles)
                    
                    # Create/update timeframe record with pre-calculated next fetch time in one operation
                    cursor.execute(text("""
                        INSERT INTO timeframemetadata 
                        (tokenaddress, pairaddress, timeframe, lastfetchedat, nextfetchat, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (tokenaddress, pairaddress, timeframe)
                        DO UPDATE SET 
                            lastfetchedat = %s,
                            nextfetchat = %s,
                            lastupdatedat = NOW()
                    """), (tokenAddress, pairAddress, '4h', 
                           latestFetchedTimeFor4Hr, nextFetchTimeFor4Hr,
                           latestFetchedTimeFor4Hr, nextFetchTimeFor4Hr))
                    
                    logger.info(f"Inserted {len(fourHourCandles)} 4-hourly candles and updated timeframe in single operation")
                
                logger.info("All aggregation operations completed in optimized single transaction")
                return True
                
        except Exception as e:
            logger.error(f"Error in optimized aggregation transaction: {e}")
            return False

    def createTimeframeInitialRecords(self, tokenAddress: str, pairAddress: str, timeframes: List[str], 
                                    pairCreatedTime: int) -> bool:
        """
        Create initial timeframe metadata records for new tokens with proper nextfetchat calculation
        
        This method sets up the scheduling foundation for new tokens by calculating when
        the first complete candle should be available and when the scheduler should fetch next.
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Token pair address  
            timeframes: List of timeframes to create records for
            pairCreatedTime: Unix timestamp when pair was created
            
        Returns:
            bool: Success status
        """
        try:
            if not timeframes:
                logger.info(f"No timeframes provided for {tokenAddress}")
                return True
                
            with self.conn_manager.transaction() as cursor:
                timeframeRecords = self.collectDataForInitialTimeframeEntry(
                    tokenAddress, pairAddress, timeframes, pairCreatedTime
                )
                
                if timeframeRecords:
                    self.recordInitialTimeframeEntry(cursor, timeframeRecords)
                    logger.info(f"Created {len(timeframeRecords)} initial timeframe records for {tokenAddress}")
                
                return True
                
        except Exception as e:
            logger.error(f"Error creating initial timeframe records for {tokenAddress}: {e}")
            return False
    
    def collectDataForInitialTimeframeEntry(self, tokenAddress: str, pairAddress: str, 
                               timeframes: List[str], pairCreatedTime: int) -> List[Tuple]:
        """Build timeframe record data for batch insertion"""
        timeframeRecords = []
        
        for timeframe in timeframes:
            timeframeSeconds = TimeframeConstants.getSeconds(timeframe)
            if not timeframeSeconds:
                logger.warning(f"Unknown timeframe: {timeframe}")
                continue
            
            # Calculate scheduling times
            firstCandleTime, nextFetchTime = self.calcualteScheduling(
                pairCreatedTime, timeframeSeconds
            )
            
            timeframeRecords.append((
                tokenAddress, pairAddress, timeframe, nextFetchTime
            ))
            
            logger.debug(f"Timeframe {timeframe}: pair created at {pairCreatedTime}, "
                        f"first candle at {firstCandleTime}, next fetch at {nextFetchTime}")
        
        return timeframeRecords
    
    def calcualteScheduling(self, pairCreatedTime: int, timeframeSeconds: int) -> Tuple[int, int]:
        """
        Calculate when first candle completes and when scheduler should fetch next
        
        Logic: If pair created at 10:23 and timeframe is 1h:
        - First complete candle is at 10:00
        - Next fetch time is at 11:00 [this is when there will be a initial completed candle]
        """
        firstCandleTime = ((pairCreatedTime // timeframeSeconds)) * timeframeSeconds
        nextFetchTime = firstCandleTime + timeframeSeconds
        return firstCandleTime, nextFetchTime
    
    def recordInitialTimeframeEntry(self, cursor, timeframeRecords: List[Tuple]):
        """Insert timeframe records in batch"""
        cursor.executemany("""
            INSERT INTO timeframemetadata 
            (tokenaddress, pairaddress, timeframe, nextfetchat, createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, pairaddress, timeframe) DO NOTHING
        """, timeframeRecords)

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
                        if update[EMA_PERIOD] == EMA_21:
                            ema21Data.append((
                                float(update[EMA_VALUE]),
                                update[TOKEN_ADDRESS],
                                update[TIMEFRAME],
                                update[CANDLE_UNIX]
                            ))
                        elif update[EMA_PERIOD] == EMA_34:
                            ema34Data.append((
                                float(update[EMA_VALUE]),
                                update[TOKEN_ADDRESS],
                                update[TIMEFRAME],
                                update[CANDLE_UNIX]
                            ))
                    
                    # Batch update EMA21 values
                    if ema21Data:
                        cursor.executemany("""
                            UPDATE ohlcvdetails 
                            SET ema21value = %s, lastupdatedat = NOW()
                            WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                        """, ema21Data)
                    
                    # Batch update EMA34 values
                    if ema34Data:
                        cursor.executemany("""
                            UPDATE ohlcvdetails 
                            SET ema34value = %s, lastupdatedat = NOW()
                            WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                        """, ema34Data)
                
                # STEP 2: Batch update EMA states
                if emaStateUpdatedData:
                    emaStateData = []
                    for update in emaStateUpdatedData:
                        # Calculate next fetch time
                        timeframeSeconds = TradingActionUtil.getTimeframeSeconds(update[TIMEFRAME])
                        nextFetchTime = update[LAST_UPDATED_UNIX] + timeframeSeconds
                        
                        emaStateData.append((
                            float(update[EMA_VALUE]),
                            update[LAST_UPDATED_UNIX],
                            nextFetchTime,
                            update[STATUS],
                            update[TOKEN_ADDRESS],
                            update[TIMEFRAME],
                            str(update[EMA_PERIOD])
                        ))
                    
                    cursor.executemany("""
                        UPDATE emastates 
                        SET emavalue = %s, lastupdatedunix = %s, nextfetchtime = %s, 
                            status = %s, lastupdatedat = NOW()
                        WHERE tokenaddress = %s AND timeframe = %s AND emakey = %s
                    """, emaStateData)
                
                return True
                
        except Exception as e:
            logger.error(f"Error in batch EMA update: {e}")
            return False


    def getAllCandlesFromAllTimeframes(self, tokenAddress: str, pairAddress: str) -> Dict[str, List[Dict]]:
        """
        Get ALL candles for all timeframes in a single optimized database call.
        
        Uses LEFT JOIN to include timeframes with no candles (lastfetchedat IS NULL).
        Returns empty lists for timeframes without candle data, enabling indicator processors
        to create appropriate empty records.
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Pair contract address
            
        Returns:
            Dict mapping timeframe -> List of candle data
        """
        try:
            with self.conn_manager.transaction() as cursor:
                # Single optimized query using LEFT JOIN to get all timeframes with their candles
                cursor.execute(text("""
                    SELECT tm.timeframe, 
                           ohlcv.unixtime, ohlcv.openprice, ohlcv.highprice, 
                           ohlcv.lowprice, ohlcv.closeprice, ohlcv.volume, ohlcv.trades
                    FROM timeframemetadata tm
                    LEFT JOIN ohlcvdetails ohlcv ON (
                        tm.tokenaddress = ohlcv.tokenaddress AND 
                        tm.pairaddress = ohlcv.pairaddress AND 
                        tm.timeframe = ohlcv.timeframe
                    )
                    WHERE tm.tokenaddress = %s AND tm.pairaddress = %s 
                    AND tm.isactive = true
                    ORDER BY tm.timeframe, ohlcv.unixtime ASC
                """), (tokenAddress, pairAddress))
                
                results = cursor.fetchall()
                
                # Group candles by timeframe - empty lists for timeframes with no candles
                timeframeCandlesMap = {}
                for row in results:
                    timeframe = row['timeframe']
                    
                    # Initialize timeframe if not exists
                    if timeframe not in timeframeCandlesMap:
                        timeframeCandlesMap[timeframe] = []
                    
                    # Add candle data if it exists (LEFT JOIN may return NULL values)
                    if row['unixtime'] is not None:
                        timeframeCandlesMap[timeframe].append({
                            'unixtime': row['unixtime'],
                            'openprice': row['openprice'],
                            'highprice': row['highprice'],
                            'lowprice': row['lowprice'],
                            'closeprice': row['closeprice'],
                            'volume': row['volume'],
                            'trades': row['trades']
                        })
                
                logger.info(f"Retrieved candles for {len(timeframeCandlesMap)} timeframes: "
                          f"{sum(1 for candles in timeframeCandlesMap.values() if candles)} with data, "
                          f"{sum(1 for candles in timeframeCandlesMap.values() if not candles)} empty")
                
                return timeframeCandlesMap
                
        except Exception as e:
            logger.error(f"Error retrieving candles for all timeframes: {e}")
            return {}

    def getAllTimeframeRecordsReadyForFetching(self, buffer_seconds: int = 300) -> List[Dict]:
        """
        Get ALL timeframe records (not just 15m) that are ready for data fetching
        
        NEW SCHEDULER FLOW: This replaces the old 15m-only approach with multi-timeframe support.
        Returns records for all timeframes (30min, 1h, 4h) that need updates.
        
        Args:
            buffer_seconds: Buffer time for newly created tokens (default: 5 minutes)
            
        Returns:
            List of timeframe records ready for processing
        """
        try:
            currentTime = int(time.time())
            bufferTime = currentTime - buffer_seconds
            
            with self.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT tm.id as timeframeid,
                           tm.tokenaddress, 
                           tm.pairaddress,
                           tm.timeframe,
                           tm.nextfetchat,
                           tm.lastfetchedat,
                           tt.symbol,
                           tt.name,
                           tt.paircreatedtime,
                           tt.createdat,
                           tt.trackedtokenid
                    FROM timeframemetadata tm
                    INNER JOIN trackedtokens tt ON tm.tokenaddress = tt.tokenaddress
                    WHERE tm.isactive = TRUE 
                        AND tt.status = 1
                        AND tm.nextfetchat <= %s
                        AND tt.createdat <= to_timestamp(%s)
                    ORDER BY tm.nextfetchat ASC
                """), (currentTime, bufferTime))
                
                results = cursor.fetchall()
                
                # Convert to list of dicts for easier processing
                timeframeRecords = []
                for row in results:
                    timeframeRecords.append({
                        'timeframeid': row['timeframeid'],
                        'tokenaddress': row['tokenaddress'],
                        'pairaddress': row['pairaddress'], 
                        'timeframe': row['timeframe'],
                        'nextfetchat': row['nextfetchat'],
                        'lastfetchedat': row['lastfetchedat'],
                        'symbol': row['symbol'],
                        'name': row['name'],
                        'paircreatedtime': row['paircreatedtime'],
                        'createdat': row['createdat'],
                        'trackedtokenid': row['trackedtokenid']
                    })
                
                logger.info(f"Found {len(timeframeRecords)} timeframe records ready for fetching")
                return timeframeRecords
                
        except Exception as e:
            logger.error(f"Error getting timeframe records ready for fetching: {e}")
            return []

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
                    dayEnd = operation['day_end']  # Use pre-calculated dayEnd
                    
                    # FIXED: Update each candle with its corresponding VWAP value instead of final VWAP for all
                    for candle_vwap in calculatedVwap['candle_vwaps']:
                        vwapCandleUpdateData.append((
                            float(candle_vwap['vwap']),  # Use individual candle VWAP
                            tokenAddress,
                            pairAddress,
                            timeframe,
                            candle_vwap['unixtime']
                        ))
                    
                    # Collect VWAP session data for batch execution using pre-calculated dayEnd
                    vwapSessionUpdateData.append((
                        tokenAddress, pairAddress, timeframe, dayStart, dayEnd,
                        float(calculatedVwap.get('cumulative_pv', 0)), 
                        float(calculatedVwap.get('cumulative_volume', 0)),
                        float(calculatedVwap.get('final_vwap', 0)), 
                        calculatedVwap.get('latest_candle_time'),
                        nextFetchAtTime
                    ))
                
                # Single batch update for all VWAP values across all timeframes
                if vwapCandleUpdateData:
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET vwapvalue = %s
                        WHERE tokenaddress = %s AND pairaddress = %s AND timeframe = %s AND unixtime = %s
                    """, vwapCandleUpdateData)
                
                # Single batch insert/update for all VWAP sessions across all timeframes
                if vwapSessionUpdateData:
                    cursor.executemany("""
                        INSERT INTO vwapsessions 
                        (tokenaddress, pairaddress, timeframe, sessionstartunix, sessionendunix,
                         cumulativepv, cumulativevolume, currentvwap, lastcandleunix, nextcandlefetch)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    """, vwapSessionUpdateData)
                
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
                    SELECT unixtime, openprice, highprice, lowprice, closeprice, volume, trades
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
            with self.conn_manager.transaction() as cursor:
                from sqlalchemy import text
                
                # Batch insert/update EMA states
                if emaStateUpdatedData:
                    emaStateUpdateQueryData = []
                    for emaState in emaStateUpdatedData:
                        emaStateUpdateQueryData.append((
                            emaState['tokenAddress'],
                            emaState['pairAddress'],
                            emaState['timeframe'],
                            emaState['emaKey'],
                            emaState['emaValue'],
                            emaState['lastUpdatedUnix'],
                            emaState['nextFetchTime'],
                            emaState['emaAvailableTime'],
                            emaState['pairCreatedTime'],
                            int(emaState['status'])
                        ))
                    
                    cursor.executemany("""
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
                    """, emaStateUpdateQueryData)
                    
                    logger.info(f"Batch inserted/updated {len(emaStateUpdateQueryData)} EMA states")
                
                # Batch update EMA values - separate updates for EMA21 and EMA34
                if emaCandleUpdatedData:
                    # Separate data by EMA period
                    ema21Updates = []
                    ema34Updates = []
                    
                    for candle in emaCandleUpdatedData:
                        if candle['ema_period'] == 21:
                            ema21Updates.append((
                                candle['ema_value'],
                                candle['tokenAddress'],
                                candle['timeframe'],
                                candle['unixtime']
                            ))
                        elif candle['ema_period'] == 34:
                            ema34Updates.append((
                                candle['ema_value'],
                                candle['tokenAddress'],
                                candle['timeframe'],
                                candle['unixtime']
                            ))
                    
                    # Update EMA21 values
                    if ema21Updates:
                        cursor.executemany("""
                            UPDATE ohlcvdetails 
                            SET ema21value = %s
                            WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                        """, ema21Updates)
                        logger.info(f"Batch updated {len(ema21Updates)} EMA21 candle values")
                    
                    # Update EMA34 values
                    if ema34Updates:
                        cursor.executemany("""
                            UPDATE ohlcvdetails 
                            SET ema34value = %s
                            WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                        """, ema34Updates)
                        logger.info(f"Batch updated {len(ema34Updates)} EMA34 candle values")
                
                logger.info(f"All EMA operations completed in 2 SQL calls: {len(emaStateUpdatedData)} states, {len(emaCandleUpdatedData)} candle updates")
                
        except Exception as e:
            logger.error(f"Error in batch EMA operations: {e}")
            raise

    