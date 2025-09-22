from config.Config import get_config
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from datetime import datetime
import json
from database.operations.BaseDBHandler import BaseDBHandler
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from logs.logger import get_logger
from sqlalchemy import text
from enum import IntEnum
from datetime import datetime, timezone, timedelta
from actions.TradingActionUtil import TradingActionUtil
from scheduler.SchedulerConstants import CandleDataKeys
from constants.TradingConstants import TimeframeConstants

import time
from utils.CommonUtil import CommonUtil
from typing import Any
from constants.TradingHandlerConstants import TradingHandlerConstants
from utils.IndicatorConstants import IndicatorConstants
from api.trading.request import OHLCVDetails, VWAPSession
from api.trading.request import TimeframeRecord
from api.trading.request import TrackedToken
from api.trading.request import EMAState
from api.trading.request import AVWAPState


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
                avwapvalue DECIMAL(20,8),
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
        
        # 6. AVWAP States
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS avwapstates (
                tokenaddress CHAR(44),
                pairaddress CHAR(44),
                timeframe VARCHAR(10),
                avwap DECIMAL(20,8),
                cumulativepv DECIMAL(30,8),
                cumulativevolume DECIMAL(30,8),
                lastupdatedunix BIGINT,
                nextfetchtime BIGINT,
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (tokenaddress, timeframe)
            )
        """))
        

    def getTableDocumentation(self, tableName: str) -> dict:
        """Get documentation for a specific table"""
        return self.schema.get(tableName, {})

    
        

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
                # Use UPSERT (INSERT ... ON CONFLICT ... DO UPDATE)
                cursor.execute(
                    text("""
                        INSERT INTO trackedtokens 
                        (tokenaddress, symbol, name, pairaddress, paircreatedtime, additionsource, addedby, metadata, createdat, lastupdatedat, status, enabledat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s)
                        ON CONFLICT (tokenaddress) 
                        DO UPDATE SET
                            symbol = EXCLUDED.symbol,
                            name = EXCLUDED.name,
                            pairaddress = EXCLUDED.pairaddress,
                            paircreatedtime = EXCLUDED.paircreatedtime,
                            additionsource = EXCLUDED.additionsource,
                            addedby = EXCLUDED.addedby,
                            metadata = EXCLUDED.metadata,
                            lastupdatedat = EXCLUDED.lastupdatedat,
                            status = 1,
                            enabledat = EXCLUDED.enabledat,
                            disabledat = NULL
                        RETURNING trackedtokenid
                    """),
                    (tokenAddress, symbol, name, pairAddress, pairCreatedTime, int(additionSource), addedBy, 
                     json.dumps(metadata) if metadata else None, now, now, now)
                )
                result = cursor.fetchone()
                tokenId = result[TradingHandlerConstants.TrackedTokens.TRACKED_TOKEN_ID]
                logger.info(f"Upserted token {tokenAddress} with ID {tokenId}")
                return tokenId
                
        except Exception as e:
            logger.error(f"Error adding token {tokenAddress}: {e}")
            return None

    def disableToken(self, tokenAddress: str, disabledBy: str = None, reason: str = None) -> Dict[str, Any]:
        """
        Disable token tracking (soft delete)
        
        Args:
            tokenAddress: Token contract address
            disabledBy: User who disabled the token
            reason: Reason for disabling
            
        Returns:
            Dict containing success status and token info if successful
        """
        try:
            now = datetime.now(timezone.utc)
            
            with self.conn_manager.transaction() as cursor:
                # Update token status and return token info in one query
                cursor.execute(
                    text("""
                        UPDATE trackedtokens 
                        SET status = 2, disabledat = %s, disabledby = %s, lastupdatedat = %s
                        WHERE tokenaddress = %s AND status = 1
                        RETURNING trackedtokenid, symbol, name, tokenaddress
                    """),
                    (now, disabledBy, now, tokenAddress)
                )
                result = cursor.fetchone()
                
                if not result:
                    logger.warning(f"Token {tokenAddress} not found or already disabled")
                    return {
                        'success': False,
                        'error': 'Token not found or already disabled',
                        'tokenInfo': None
                    }

                
                tokenInfo = {
                    'trackedtokenid': result['trackedtokenid'],
                    'symbol': result['symbol'],
                    'name': result['name'],
                    'tokenaddress': result['tokenaddress']
                }
                
                logger.info(f"Disabled token {tokenInfo['symbol']} ({tokenAddress}) - reason: {reason}")
                return {
                    'success': True,
                    'tokenInfo': tokenInfo
                }
                
        except Exception as e:
            logger.error(f"Error disabling token {tokenAddress}: {e}")
            return {
                'success': False,
                'error': str(e),
                'tokenInfo': None
            }

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

    
    def _calculateTimeBucket(self, unixtime: int, timeframe: str) -> int:
        """Calculate timebucket based on timeframe - delegates to CommonUtil"""
        return CommonUtil.calculateInitialStartTime(unixtime, timeframe)

    
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
                    pairAddress = firstCandle[TradingHandlerConstants.OHLCVDetails.PAIR_ADDRESS]
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
                            candle[TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS], 
                            candle[TradingHandlerConstants.OHLCVDetails.PAIR_ADDRESS], 
                            candle[TradingHandlerConstants.OHLCVDetails.TIMEFRAME],
                            candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME], 
                            candle[TradingHandlerConstants.OHLCVDetails.OPEN_PRICE], 
                            candle[TradingHandlerConstants.OHLCVDetails.HIGH_PRICE],
                            candle[TradingHandlerConstants.OHLCVDetails.LOW_PRICE], 
                            candle[TradingHandlerConstants.OHLCVDetails.CLOSE_PRICE], 
                            candle[TradingHandlerConstants.OHLCVDetails.VOLUME], 
                            int(candle.get(TradingHandlerConstants.OHLCVDetails.TRADES, 0)), 
                            candle[TradingHandlerConstants.OHLCVDetails.DATA_SOURCE]
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
        except Exception:
            return None, None
    
    def calculateNextFetchTimeForTimeframe(self, latestTime: int, timeframe: str) -> int:
        """Calculate next fetch time based on specific timeframe - delegates to CommonUtil"""
        return CommonUtil.calculateNextFetchTimeForTimeframe(latestTime, timeframe)
    
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
    
    def getAllVWAPDataForScheduler(self) -> List['TrackedToken']:

        try:
            with self.conn_manager.transaction() as cursor:
                # Get all active tokens with their timeframes and VWAP session data
                cursor.execute(text("""
                    SELECT 
                        tt.tokenaddress,
                        tt.pairaddress,
                        tm.id as timeframeid,
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
                    LEFT JOIN vwapsessions vs ON tt.tokenaddress = vs.tokenaddress 
                        AND tt.pairaddress = vs.pairaddress 
                        AND tm.timeframe = vs.timeframe
                    LEFT JOIN ohlcvdetails ohlcv ON tt.tokenaddress = ohlcv.tokenaddress 
                        AND tm.id = ohlcv.timeframeid 
                        AND ohlcv.unixtime > COALESCE(vs.lastcandleunix, 0)
                    WHERE tt.status = 1
                    ORDER BY tt.tokenaddress, tm.timeframe, ohlcv.unixtime
                """))
                
                records = cursor.fetchall()
                
             
                trackedTokensMap = {}
                
                for record in records:
                    tokenAddress = record[TradingHandlerConstants.TrackedTokens.TOKEN_ADDRESS]
                    pairAddress = record[TradingHandlerConstants.TrackedTokens.PAIR_ADDRESS]
                    timeframe = record[TradingHandlerConstants.TimeframeMetadata.TIMEFRAME]
                    timeframeId = record['timeframeid']
                    
                    # Create or get existing TrackedToken
                    if tokenAddress not in trackedTokensMap:
                        trackedTokensMap[tokenAddress] = TrackedToken(
                            trackedTokenId=record.get(TradingHandlerConstants.TrackedTokens.TRACKED_TOKEN_ID, 0),
                            tokenAddress=tokenAddress,
                            symbol=record.get(TradingHandlerConstants.TrackedTokens.SYMBOL, ''),
                            name=record.get(TradingHandlerConstants.TrackedTokens.NAME, ''),
                            pairAddress=pairAddress,
                            pairCreatedTime=record.get(TradingHandlerConstants.TrackedTokens.PAIR_CREATED_TIME, 0),
                            addedBy='scheduler'
                        )
                    
                    # Get or create TimeframeRecord
                    timeframeRecord = trackedTokensMap[tokenAddress].getTimeframeRecord(timeframe)
                    if not timeframeRecord:
                        timeframeRecord = TimeframeRecord(
                            timeframeId=timeframeId,
                            tokenAddress=tokenAddress,
                            pairAddress=pairAddress,
                            timeframe=timeframe,
                            lastFetchedAt=record[TradingHandlerConstants.TimeframeMetadata.LAST_FETCHED_AT],
                            isActive=True
                        )
                        trackedTokensMap[tokenAddress].addTimeframeRecord(timeframeRecord)
                    
                    # Create VWAPSession POJO
                    if not timeframeRecord.vwapSession:
                        timeframeRecord.vwapSession = VWAPSession(
                            tokenAddress=tokenAddress,
                            pairAddress=pairAddress,
                            timeframe=timeframe,
                            sessionStartUnix=record[TradingHandlerConstants.VWAPSessions.SESSION_START_UNIX],
                            sessionEndUnix=record[TradingHandlerConstants.VWAPSessions.SESSION_END_UNIX],
                            cumulativePV=record[TradingHandlerConstants.VWAPSessions.CUMULATIVE_PV],
                            cumulativeVolume=record[TradingHandlerConstants.VWAPSessions.CUMULATIVE_VOLUME],
                            currentVWAP=record[TradingHandlerConstants.VWAPSessions.CURRENT_VWAP],
                            lastCandleUnix=record[TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX] or 0,
                            nextCandleFetch=record[TradingHandlerConstants.VWAPSessions.NEXT_CANDLE_FETCH]
                        )
                    
                    # Add candle data if available
                    if record[TradingHandlerConstants.OHLCVDetails.UNIX_TIME] is not None:
                        ohlcvDetail = OHLCVDetails(
                            tokenAddress=tokenAddress,
                            pairAddress=pairAddress,
                            timeframe=timeframe,
                            unixTime=record[TradingHandlerConstants.OHLCVDetails.UNIX_TIME],
                            timeBucket=CommonUtil.calculateInitialStartTime(record[TradingHandlerConstants.OHLCVDetails.UNIX_TIME], timeframe),
                            openPrice=record[TradingHandlerConstants.OHLCVDetails.OPEN_PRICE],
                            highPrice=record[TradingHandlerConstants.OHLCVDetails.HIGH_PRICE],
                            lowPrice=record[TradingHandlerConstants.OHLCVDetails.LOW_PRICE],
                            closePrice=record[TradingHandlerConstants.OHLCVDetails.CLOSE_PRICE],
                            volume=record[TradingHandlerConstants.OHLCVDetails.VOLUME],
                            trades=record.get(TradingHandlerConstants.OHLCVDetails.TRADES, 0),
                            isComplete=True,
                            dataSource=record.get(TradingHandlerConstants.OHLCVDetails.DATA_SOURCE, 'moralis')
                        )
                        timeframeRecord.addOHLCVDetail(ohlcvDetail)
                
                trackedTokens = list(trackedTokensMap.values())
                return trackedTokens
                
        except Exception as e:
            logger.error(f"Error getting all VWAP data for scheduler: {e}")
            return []
    
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
                            float(update[TradingHandlerConstants.OHLCVDetails.VWAP_VALUE]),
                            update[TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS],
                            update[TradingHandlerConstants.OHLCVDetails.TIMEFRAME], 
                            update[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
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
                            update[TradingHandlerConstants.VWAPSessions.TOKEN_ADDRESS],
                            update[TradingHandlerConstants.VWAPSessions.PAIR_ADDRESS],
                            update[TradingHandlerConstants.VWAPSessions.TIMEFRAME],
                            update[TradingHandlerConstants.VWAPSessions.SESSION_START_UNIX],
                            update[TradingHandlerConstants.VWAPSessions.SESSION_END_UNIX],
                            float(update[TradingHandlerConstants.VWAPSessions.CUMULATIVE_PV]),
                            float(update[TradingHandlerConstants.VWAPSessions.CUMULATIVE_VOLUME]),
                            float(update[TradingHandlerConstants.VWAPSessions.CURRENT_VWAP]),
                            update[TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX],
                            update[TradingHandlerConstants.VWAPSessions.NEXT_CANDLE_FETCH]
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
    
    def getAllEMADataWithCandlesForScheduler(self) -> List['TrackedToken']:
        """
        SINGLE OPTIMIZED QUERY: Get all EMA data with corresponding candles for scheduler
        
        This method implements the new optimized approach:
        1. JOIN emastates with trackedtokens to get only active tokens
        2. JOIN with timeframemetadata to get lastfetchedat for each timeframe
        3. JOIN with ohlcvdetails to get candles where unixtime > lastupdatedunix
        4. All in one highly optimized query for scalability
        
        Returns:
            List[TrackedToken]: List of tracked tokens with EMA data and candles
        """
        try:        
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
                            tmf.id as timeframeid,
                            tmf.lastfetchedat,
                            CASE 
                                WHEN es.status = 2 THEN es.lastupdatedunix  -- AVAILABLE: get candles after last updated
                                WHEN es.status = 1 AND tmf.lastfetchedat >= es.emaavailabletime THEN 0  -- NOT_AVAILABLE_READY: get ALL candles (for initial SMA calculation)
                                ELSE 0  -- NOT_AVAILABLE_INSUFFICIENT: no candles needed
                            END as candle_from_time
                        FROM emastates es
                        INNER JOIN trackedtokens tt ON es.tokenaddress = tt.tokenaddress AND es.pairaddress = tt.pairaddress
                        INNER JOIN timeframemetadata tmf ON es.tokenaddress = tmf.tokenaddress AND es.timeframe = tmf.timeframe
                        WHERE tt.status = 1
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
                        WHERE ed.candle_from_time >= 0 
                          AND (ed.candle_from_time = 0 OR o.unixtime > ed.candle_from_time)
                          AND o.iscomplete = TRUE
                    )
                    SELECT 
                        ed.tokenaddress,
                        ed.pairaddress,
                        ed.timeframe,
                        ed.timeframeid,
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
                    WHERE ed.candle_from_time >= 0
                    ORDER BY ed.tokenaddress, ed.timeframe, ed.emakey, cd.unixtime ASC
                """))
                
                # Organize results into POJOs
                trackedTokens = {}
                
                for row in cursor.fetchall():
                    tokenAddress = row[TradingHandlerConstants.EMAStates.TOKEN_ADDRESS]
                    pairAddress = row[TradingHandlerConstants.EMAStates.PAIR_ADDRESS]
                    timeframe = row[TradingHandlerConstants.EMAStates.TIMEFRAME]
                    timeframeId = row['timeframeid'] 
                    emaKey = row[TradingHandlerConstants.EMAStates.EMA_KEY]
                    emaPeriod = int(emaKey)
                    
                    # Initialize TrackedToken if not exists
                    if tokenAddress not in trackedTokens:
                        trackedTokens[tokenAddress] = TrackedToken(
                            trackedTokenId=0,  # Will be set from database if needed
                            tokenAddress=tokenAddress,
                            symbol='',  # Not needed for EMA processing
                            name='',    # Not needed for EMA processing
                            pairAddress=pairAddress,
                            addedBy='scheduler'
                        )
                    
                    # Get or create TimeframeRecord for this timeframe
                    timeframeRecord = trackedTokens[tokenAddress].getTimeframeRecord(timeframe)
                    if not timeframeRecord:
                        timeframeRecord = TimeframeRecord(
                            timeframeId=timeframeId,
                            tokenAddress=tokenAddress,
                            pairAddress=pairAddress,
                            timeframe=timeframe,
                            nextFetchAt=row[TradingHandlerConstants.TimeframeMetadata.LAST_FETCHED_AT] or 0,
                            lastFetchedAt=row[TradingHandlerConstants.TimeframeMetadata.LAST_FETCHED_AT],
                            isActive=True
                        )
                        trackedTokens[tokenAddress].addTimeframeRecord(timeframeRecord)
                    
                    # Create or update EMAState
                    emaState = EMAState(
                        tokenAddress=tokenAddress,
                        pairAddress=pairAddress,
                        timeframe=timeframe,
                        emaKey=emaKey,
                        emaValue=float(row[TradingHandlerConstants.EMAStates.EMA_VALUE]) if row[TradingHandlerConstants.EMAStates.EMA_VALUE] else None,
                        lastUpdatedUnix=row[TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX],
                        nextFetchTime=None,  # Will be calculated during processing
                        emaAvailableTime=row[TradingHandlerConstants.EMAStates.EMA_AVAILABLE_TIME],
                        pairCreatedTime=None,  # Not needed for EMA processing
                        status=row[TradingHandlerConstants.EMAStates.STATUS]
                    )
                    
                    # Set EMAState in TimeframeRecord
                    if emaPeriod == 21:
                        timeframeRecord.ema21State = emaState
                    elif emaPeriod == 34:
                        timeframeRecord.ema34State = emaState
                    
                    # Add candle data if exists (only close price needed for EMA)
                    if row[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME]:
                        # Create OHLCVDetails with only close price (EMA only needs close price)
                        candle = OHLCVDetails(
                            tokenAddress=tokenAddress,
                            pairAddress=pairAddress,
                            timeframe=timeframe,
                            unixTime=row[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME],
                            timeBucket=self._calculateTimeBucket(row[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME], timeframe),
                            openPrice=0.0,  # Not needed for EMA
                            highPrice=0.0,  # Not needed for EMA
                            lowPrice=0.0,   # Not needed for EMA
                            closePrice=float(row[IndicatorConstants.EMAStates.CANDLE_CLOSE_PRICE]),
                            volume=0.0,     # Not needed for EMA
                            trades=0,       # Not needed for EMA
                            isComplete=True,
                            dataSource='database'
                        )
                        timeframeRecord.addOHLCVDetail(candle)
                
                return list(trackedTokens.values())
                
        except Exception as e:
            logger.error(f"Error getting EMA data with candles for scheduler: {e}")
            return []


    def createTimeframeInitialRecords(self, tokenAddress: str, pairAddress: str, timeframes: List[str], 
                                    pairCreatedTime: int) -> List:
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
            List[TimeframeRecord]: List of TimeframeRecord POJOs
        """
        try:
            if not timeframes:
                logger.info(f"No timeframes provided for {tokenAddress}")
                return []
                
            # Collect data for timeframe records (outside transaction)
            timeframeRecords = self.collectDataForInitialTimeframeEntry(
                tokenAddress, pairAddress, timeframes, pairCreatedTime
            )
            
            if not timeframeRecords:
                logger.info(f"No timeframe records to create for {tokenAddress}")
                return []
            
            # Execute database operations in transaction
            with self.conn_manager.transaction() as cursor:
                persistedRecords = self.recordInitialTimeframeEntry(cursor, timeframeRecords)
            
            # Create TimeframeRecord POJOs from persisted data
            createdRecords = []
            for persistedRecord in persistedRecords:
                timeframeRecord = TimeframeRecord(
                    timeframeId=persistedRecord[TradingHandlerConstants.TimeframeMetadata.ID],
                    tokenAddress=persistedRecord[TradingHandlerConstants.TimeframeMetadata.TOKEN_ADDRESS],
                    pairAddress=persistedRecord[TradingHandlerConstants.TimeframeMetadata.PAIR_ADDRESS],
                    timeframe=persistedRecord[TradingHandlerConstants.TimeframeMetadata.TIMEFRAME],
                    nextFetchAt=persistedRecord[TradingHandlerConstants.TimeframeMetadata.NEXT_FETCH_AT],
                    lastFetchedAt=None,
                    isActive=True
                )
                createdRecords.append(timeframeRecord)
            
            logger.info(f"Created {len(createdRecords)} initial timeframe records for {tokenAddress}")
            return createdRecords
                
        except Exception as e:
            logger.error(f"Error creating initial timeframe records for {tokenAddress}: {e}")
            return []
    
    def collectDataForInitialTimeframeEntry(self, tokenAddress: str, pairAddress: str, 
                               timeframes: List[str], pairCreatedTime: int) -> List[Tuple]:
        """Build timeframe record data for batch insertion"""
        timeframeRecords = []
        
        for timeframe in timeframes:
            nextFetchTime = CommonUtil.calculateNextFetchTimeForInitialTimeframeRecord(pairCreatedTime, timeframe)
            
            timeframeRecords.append((
                tokenAddress, pairAddress, timeframe, nextFetchTime
            ))
        
        return timeframeRecords

    
    def recordInitialTimeframeEntry(self, cursor, timeframeRecords: List[Tuple]):
        """Insert timeframe records in batch and return persisted data"""
        # Insert records one by one to get RETURNING results
        persistedRecords = []
        for record in timeframeRecords:
            cursor.execute("""
                INSERT INTO timeframemetadata 
                (tokenaddress, pairaddress, timeframe, nextfetchat, createdat, lastupdatedat)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (tokenaddress, pairaddress, timeframe) 
                DO UPDATE SET 
                    nextfetchat = EXCLUDED.nextfetchat,
                    lastupdatedat = NOW()
                RETURNING id, tokenaddress, pairaddress, timeframe, nextfetchat
            """, record)
            
            result = cursor.fetchone()
            if result:
                persistedRecords.append(result)
        
        return persistedRecords
    

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
                    
                    for stateData in emaCandlesUpdatedData:
                        if stateData[IndicatorConstants.EMAStates.EMA_PERIOD] == IndicatorConstants.EMAStates.EMA_21:
                            ema21Data.append((
                                float(stateData[IndicatorConstants.EMAStates.EMA_VALUE]),
                                stateData[TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS],
                                stateData[TradingHandlerConstants.OHLCVDetails.TIMEFRAME],
                                stateData[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
                            ))
                        elif stateData[IndicatorConstants.EMAStates.EMA_PERIOD] == IndicatorConstants.EMAStates.EMA_34:
                            ema34Data.append((
                                float(stateData[IndicatorConstants.EMAStates.EMA_VALUE]),
                                stateData[TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS],
                                stateData[TradingHandlerConstants.OHLCVDetails.TIMEFRAME],
                                stateData[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
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
                    for stateData in emaStateUpdatedData:
                        # Calculate next fetch time
                        timeframeSeconds = CommonUtil.getTimeframeSeconds(stateData[TradingHandlerConstants.EMAStates.TIMEFRAME])
                        nextFetchTime = stateData[TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX] + timeframeSeconds
                        
                        emaStateData.append((
                            float(stateData[TradingHandlerConstants.EMAStates.EMA_VALUE]),
                            stateData[TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX],
                            nextFetchTime,
                            stateData[TradingHandlerConstants.EMAStates.STATUS],
                            stateData[TradingHandlerConstants.EMAStates.TOKEN_ADDRESS],
                            stateData[TradingHandlerConstants.EMAStates.TIMEFRAME],
                            str(stateData[IndicatorConstants.EMAStates.EMA_PERIOD])
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
                    timeframe = row[TradingHandlerConstants.TimeframeMetadata.TIMEFRAME]
                    
                    # Initialize timeframe if not exists
                    if timeframe not in timeframeCandlesMap:
                        timeframeCandlesMap[timeframe] = []

                    # Add candle data if it exists (LEFT JOIN may return NULL values)
                    if row[TradingHandlerConstants.OHLCVDetails.UNIX_TIME] is not None:
                        timeframeCandlesMap[timeframe].append({
                            TradingHandlerConstants.OHLCVDetails.UNIX_TIME: row[TradingHandlerConstants.OHLCVDetails.UNIX_TIME],
                            TradingHandlerConstants.OHLCVDetails.OPEN_PRICE: row[TradingHandlerConstants.OHLCVDetails.OPEN_PRICE],
                            TradingHandlerConstants.OHLCVDetails.HIGH_PRICE: row[TradingHandlerConstants.OHLCVDetails.HIGH_PRICE],
                            TradingHandlerConstants.OHLCVDetails.LOW_PRICE: row[TradingHandlerConstants.OHLCVDetails.LOW_PRICE],
                            TradingHandlerConstants.OHLCVDetails.CLOSE_PRICE: row[TradingHandlerConstants.OHLCVDetails.CLOSE_PRICE],
                            TradingHandlerConstants.OHLCVDetails.VOLUME: row[TradingHandlerConstants.OHLCVDetails.VOLUME],
                            TradingHandlerConstants.OHLCVDetails.TRADES: row[TradingHandlerConstants.OHLCVDetails.TRADES]
                        })
                
                logger.info(f"Retrieved candles for {len(timeframeCandlesMap)} timeframes: "
                          f"{sum(1 for candles in timeframeCandlesMap.values() if candles)} with data, "
                          f"{sum(1 for candles in timeframeCandlesMap.values() if not candles)} empty")
                
                return timeframeCandlesMap
                
        except Exception as e:
            logger.error(f"Error retrieving candles for all timeframes: {e}")
            return {}

    def getAllTimeframeRecordsReadyForFetching(self, buffer_seconds: int = 300) -> List['TrackedToken']:
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
                
                # Convert directly to TrackedToken POJOs
                from api.trading.request import TrackedToken, TimeframeRecord
                
                trackedTokensMap = {}
                
                for row in results:
                    tokenAddress = row[TradingHandlerConstants.TimeframeMetadata.TOKEN_ADDRESS]
                    
                    # Create or get existing TrackedToken
                    if tokenAddress not in trackedTokensMap:
                        trackedTokensMap[tokenAddress] = TrackedToken(
                            trackedTokenId=row[TradingHandlerConstants.TrackedTokens.TRACKED_TOKEN_ID],
                            tokenAddress=tokenAddress,
                            symbol=row[TradingHandlerConstants.TrackedTokens.SYMBOL],
                            name=row[TradingHandlerConstants.TrackedTokens.NAME],
                            pairAddress=row[TradingHandlerConstants.TimeframeMetadata.PAIR_ADDRESS],
                            pairCreatedTime=row[TradingHandlerConstants.TrackedTokens.PAIR_CREATED_TIME],
                            addedBy='scheduler'
                        )
                    
                    # Create TimeframeRecord POJO
                    timeframeRecord = TimeframeRecord(
                        timeframeId=row[TradingHandlerConstants.TimeframeMetadata.TIMEFRAME_ID],
                        tokenAddress=tokenAddress,
                        pairAddress=row[TradingHandlerConstants.TimeframeMetadata.PAIR_ADDRESS],
                        timeframe=row[TradingHandlerConstants.TimeframeMetadata.TIMEFRAME],
                        nextFetchAt=row[TradingHandlerConstants.TimeframeMetadata.NEXT_FETCH_AT],
                        lastFetchedAt=row[TradingHandlerConstants.TimeframeMetadata.LAST_FETCHED_AT],
                        isActive=True
                    )
                    
                    # Add to tracked token
                    trackedTokensMap[tokenAddress].addTimeframeRecord(timeframeRecord)
                
                trackedTokens = list(trackedTokensMap.values())
                logger.info(f"Found {len(trackedTokens)} tracked tokens with {sum(len(t.timeframeRecords) for t in trackedTokens)} timeframe records ready for fetching")
                return trackedTokens
                
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
                
                for vwapData in calcuatedVwapData:
                    timeframe = vwapData[TradingHandlerConstants.TimeframeMetadata.TIMEFRAME]
                    calculatedVwap = vwapData[TradingHandlerConstants.VWAPSessions.CURRENT_VWAP]
                    nextFetchAtTime = vwapData[TradingHandlerConstants.VWAPSessions.NEXT_CANDLE_FETCH]
                    dayStart = vwapData[TradingHandlerConstants.VWAPSessions.SESSION_START_UNIX]
                    dayEnd = vwapData[TradingHandlerConstants.VWAPSessions.SESSION_END_UNIX]  # Use pre-calculated dayEnd
                    
                    # FIXED: Update each candle with its corresponding VWAP value instead of final VWAP for all
                    for candleVWAP in calculatedVwap[IndicatorConstants.VWAPSessions.CANDLE_VWAPS]:
                        vwapCandleUpdateData.append((
                            float(candleVWAP[TradingHandlerConstants.OHLCVDetails.VWAP_VALUE]),  # Use individual candle VWAP
                            tokenAddress,
                            pairAddress,
                            timeframe,
                            candleVWAP[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
                        ))
                    
                    # Collect VWAP session data for batch execution using pre-calculated dayEnd
                    vwapSessionUpdateData.append((
                        tokenAddress, pairAddress, timeframe, dayStart, dayEnd,
                        float(calculatedVwap.get(TradingHandlerConstants.VWAPSessions.CUMULATIVE_PV, 0)), 
                        float(calculatedVwap.get(TradingHandlerConstants.VWAPSessions.CUMULATIVE_VOLUME, 0)),
                        float(calculatedVwap.get(TradingHandlerConstants.VWAPSessions.CURRENT_VWAP, 0)), 
                        calculatedVwap.get(TradingHandlerConstants.VWAPSessions.LAST_CANDLE_UNIX),
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
                            emaState[TradingHandlerConstants.EMAStates.TOKEN_ADDRESS],
                            emaState[TradingHandlerConstants.EMAStates.PAIR_ADDRESS],
                            emaState[TradingHandlerConstants.EMAStates.TIMEFRAME],
                            emaState[TradingHandlerConstants.EMAStates.EMA_KEY],
                            emaState[TradingHandlerConstants.EMAStates.EMA_VALUE],
                            emaState[TradingHandlerConstants.EMAStates.LAST_UPDATED_UNIX],
                            emaState[TradingHandlerConstants.EMAStates.NEXT_FETCH_TIME],
                            emaState[TradingHandlerConstants.EMAStates.EMA_AVAILABLE_TIME],
                            emaState[TradingHandlerConstants.EMAStates.PAIR_CREATED_TIME],
                            int(emaState[TradingHandlerConstants.EMAStates.STATUS])
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
                        if candle[IndicatorConstants.EMAStates.EMA_PERIOD] == IndicatorConstants.EMAStates.EMA_21:
                            ema21Updates.append((
                                candle[IndicatorConstants.EMAStates.EMA_VALUE],
                                candle[TradingHandlerConstants.EMAStates.TOKEN_ADDRESS],
                                candle[TradingHandlerConstants.EMAStates.TIMEFRAME],
                                candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
                            ))
                        elif candle[IndicatorConstants.EMAStates.EMA_PERIOD] == IndicatorConstants.EMAStates.EMA_34:
                            ema34Updates.append((
                                candle[IndicatorConstants.EMAStates.EMA_VALUE],
                                candle[TradingHandlerConstants.EMAStates.TOKEN_ADDRESS],
                                candle[TradingHandlerConstants.EMAStates.TIMEFRAME],
                                candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
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

    def batchUpdateAVWAPData(self, avwapStateData: List[Dict], avwapCandleUpdateData: List[Dict]) -> bool:
        """
        Execute batch AVWAP operations in database
        
        Args:
            avwapStateData: List of AVWAP state records to insert/update
            avwapCandleUpdateData: List of candle AVWAP updates
            
        Returns:
            bool: True if successful
        """
        try:
            with self.conn_manager.transaction() as cursor:
                # STEP 1: Batch insert/update AVWAP states
                if avwapStateData:
                    avwapStateQueryData = []
                    for avwapState in avwapStateData:
                        avwapStateQueryData.append((
                            avwapState[TradingHandlerConstants.AVWAPStates.TOKEN_ADDRESS],
                            avwapState[TradingHandlerConstants.AVWAPStates.PAIR_ADDRESS],
                            avwapState[TradingHandlerConstants.AVWAPStates.TIMEFRAME],
                            float(avwapState[TradingHandlerConstants.AVWAPStates.AVWAP]),
                            float(avwapState[TradingHandlerConstants.AVWAPStates.CUMULATIVE_PV]),
                            float(avwapState[TradingHandlerConstants.AVWAPStates.CUMULATIVE_VOLUME]),
                            avwapState[TradingHandlerConstants.AVWAPStates.LAST_UPDATED_UNIX],
                            avwapState[TradingHandlerConstants.AVWAPStates.NEXT_FETCH_TIME]
                        ))
                    
                    cursor.executemany("""
                        INSERT INTO avwapstates 
                        (tokenaddress, pairaddress, timeframe, avwap, cumulativepv, cumulativevolume, 
                         lastupdatedunix, nextfetchtime, createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (tokenaddress, timeframe) 
                        DO UPDATE SET 
                            avwap = EXCLUDED.avwap,
                            cumulativepv = EXCLUDED.cumulativepv,
                            cumulativevolume = EXCLUDED.cumulativevolume,
                            lastupdatedunix = EXCLUDED.lastupdatedunix,
                            nextfetchtime = EXCLUDED.nextfetchtime,
                            lastupdatedat = NOW()
                    """, avwapStateQueryData)
                    
                    logger.info(f"Batch inserted/updated {len(avwapStateQueryData)} AVWAP states")
                
                # STEP 2: Batch update AVWAP values in candles
                if avwapCandleUpdateData:
                    avwapCandleQueryData = []
                    for candle in avwapCandleUpdateData:
                        avwapCandleQueryData.append((
                            float(candle[TradingHandlerConstants.OHLCVDetails.AVWAP_VALUE]),
                            candle[TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS],
                            candle[TradingHandlerConstants.OHLCVDetails.TIMEFRAME],
                            candle[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
                        ))
                    
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET avwapvalue = %s, lastupdatedat = NOW()
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """, avwapCandleQueryData)
                    
                    logger.info(f"Batch updated {len(avwapCandleQueryData)} AVWAP candle values")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in batch AVWAP operations: {e}")
            raise

    def batchPersistOptimizedTokenData(self, timeframeRecords: List, maxCandlesPerTimeframe: int = None) -> int:
        
        try:
            totalCandlesInserted = 0
            
            with self.conn_manager.transaction() as cursor:
                # Collect all data for batch operations
                timeframeMetadataData = []
                candleData = []
                vwapSessionData = []
                emaStateData = []
                avwapStateData = []
                
                for timeframeRecord in timeframeRecords:
                    # Collect timeframe metadata
                    timeframeMetadataData.append((
                        timeframeRecord.tokenAddress,
                        timeframeRecord.pairAddress,
                        timeframeRecord.timeframe,
                        timeframeRecord.lastFetchedAt,
                        timeframeRecord.nextFetchAt
                    ))
                    
                    # Get candles for persistence using TimeframeRecord method
                    candlesToPersist = timeframeRecord.getCandlesForPersistence(maxCandlesPerTimeframe)
                    
                    for candle in candlesToPersist:
                        candleData.append((
                            timeframeRecord.timeframeId,  # Add timeframeid
                            timeframeRecord.tokenAddress,
                            timeframeRecord.pairAddress,
                            timeframeRecord.timeframe,
                            candle.unixTime,
                            self._calculateTimeBucket(candle.unixTime, timeframeRecord.timeframe),
                            candle.openPrice,
                            candle.highPrice,
                            candle.lowPrice,
                            candle.closePrice,
                            candle.volume,
                            candle.trades,
                            candle.vwapValue,
                            candle.avwapValue,
                            candle.ema21Value,
                            candle.ema34Value,
                            candle.isComplete,
                            candle.dataSource
                        ))
                        totalCandlesInserted += 1
                    
                    # Collect VWAP session data
                    if timeframeRecord.vwapSession:
                        vwapSessionData.append((
                            timeframeRecord.vwapSession.tokenAddress,
                            timeframeRecord.vwapSession.pairAddress,
                            timeframeRecord.vwapSession.timeframe,
                            timeframeRecord.vwapSession.sessionStartUnix,
                            timeframeRecord.vwapSession.sessionEndUnix,
                            timeframeRecord.vwapSession.cumulativePV,
                            timeframeRecord.vwapSession.cumulativeVolume,
                            timeframeRecord.vwapSession.currentVWAP,
                            timeframeRecord.vwapSession.lastCandleUnix,
                            timeframeRecord.vwapSession.nextCandleFetch
                        ))
                    
                    # Collect EMA state data
                    if timeframeRecord.ema21State:
                        emaStateData.append((
                            timeframeRecord.ema21State.tokenAddress,
                            timeframeRecord.ema21State.pairAddress,
                            timeframeRecord.ema21State.timeframe,
                            timeframeRecord.ema21State.emaKey,
                            timeframeRecord.ema21State.emaValue,
                            timeframeRecord.ema21State.lastUpdatedUnix,
                            timeframeRecord.ema21State.nextFetchTime,
                            timeframeRecord.ema21State.emaAvailableTime,
                            timeframeRecord.ema21State.pairCreatedTime,
                            timeframeRecord.ema21State.status
                        ))
                    
                    if timeframeRecord.ema34State:
                        emaStateData.append((
                            timeframeRecord.ema34State.tokenAddress,
                            timeframeRecord.ema34State.pairAddress,
                            timeframeRecord.ema34State.timeframe,
                            timeframeRecord.ema34State.emaKey,
                            timeframeRecord.ema34State.emaValue,
                            timeframeRecord.ema34State.lastUpdatedUnix,
                            timeframeRecord.ema34State.nextFetchTime,
                            timeframeRecord.ema34State.emaAvailableTime,
                            timeframeRecord.ema34State.pairCreatedTime,
                            timeframeRecord.ema34State.status
                        ))
                    
                    # Collect AVWAP state data
                    if timeframeRecord.avwapState:
                        avwapStateData.append((
                            timeframeRecord.avwapState.tokenAddress,
                            timeframeRecord.avwapState.pairAddress,
                            timeframeRecord.avwapState.timeframe,
                            timeframeRecord.avwapState.avwap,
                            timeframeRecord.avwapState.cumulativePV,
                            timeframeRecord.avwapState.cumulativeVolume,
                            timeframeRecord.avwapState.lastUpdatedUnix,
                            timeframeRecord.avwapState.nextFetchTime
                        ))
                
                # Execute all batch operations
                if timeframeMetadataData:
                    self._batchUpdateTimeframeMetadata(cursor, timeframeMetadataData)
                
                if candleData:
                    self._batchInsertCandles(cursor, candleData)
                
                if vwapSessionData:
                    self._batchInsertVWAPSessions(cursor, vwapSessionData)
                
                if emaStateData:
                    self._batchInsertEMAStates(cursor, emaStateData)
                
                if avwapStateData:
                    self._batchInsertAVWAPStates(cursor, avwapStateData)
            
            logger.info(f"Batch persisted {totalCandlesInserted} candles and all indicator data in single transaction")
            return totalCandlesInserted
            
        except Exception as e:
            logger.error(f"Error in batch persist optimized token data: {e}")
            return 0

    def batchPersistTrackedTokensData(self, trackedTokens: List['TrackedToken'], maxCandlesPerTimeframe: int = None) -> int:
        
        try:
            totalCandlesInserted = 0
            
            with self.conn_manager.transaction() as cursor:
                # Collect all data for batch operations
                timeframeMetadataData = []
                candleData = []
                vwapSessionData = []
                emaStateData = []
                avwapStateData = []
                
                for trackedToken in trackedTokens:
                    for timeframeRecord in trackedToken.timeframeRecords:
                        # Collect timeframe metadata
                        timeframeMetadataData.append((
                            timeframeRecord.tokenAddress,
                            timeframeRecord.pairAddress,
                            timeframeRecord.timeframe,
                            timeframeRecord.lastFetchedAt,
                            timeframeRecord.nextFetchAt
                        ))
                        
                        # Get candles for persistence using TimeframeRecord method
                        candlesToPersist = timeframeRecord.getCandlesForPersistence(maxCandlesPerTimeframe)
                        
                        for candle in candlesToPersist:
                            candleData.append((
                                timeframeRecord.timeframeId,  # Add timeframeid
                                timeframeRecord.tokenAddress,
                                timeframeRecord.pairAddress,
                                timeframeRecord.timeframe,
                                candle.unixTime,
                                self._calculateTimeBucket(candle.unixTime, timeframeRecord.timeframe),
                                candle.openPrice,
                                candle.highPrice,
                                candle.lowPrice,
                                candle.closePrice,
                                candle.volume,
                                candle.trades,
                                candle.vwapValue,
                                candle.avwapValue,
                                candle.ema21Value,
                                candle.ema34Value,
                                candle.isComplete,
                                candle.dataSource
                            ))
                            totalCandlesInserted += 1
                        
                        # Collect VWAP session data
                        if timeframeRecord.vwapSession:
                            vwapSessionData.append((
                                timeframeRecord.vwapSession.tokenAddress,
                                timeframeRecord.vwapSession.pairAddress,
                                timeframeRecord.vwapSession.timeframe,
                                timeframeRecord.vwapSession.sessionStartUnix,
                                timeframeRecord.vwapSession.sessionEndUnix,
                                timeframeRecord.vwapSession.cumulativePV,
                                timeframeRecord.vwapSession.cumulativeVolume,
                                timeframeRecord.vwapSession.currentVWAP,
                                timeframeRecord.vwapSession.lastCandleUnix,
                                timeframeRecord.vwapSession.nextCandleFetch
                            ))
                        
                        # Collect EMA state data
                        if timeframeRecord.ema21State:
                            emaStateData.append((
                                timeframeRecord.ema21State.tokenAddress,
                                timeframeRecord.ema21State.pairAddress,
                                timeframeRecord.ema21State.timeframe,
                                timeframeRecord.ema21State.emaKey,
                                timeframeRecord.ema21State.emaValue,
                                timeframeRecord.ema21State.lastUpdatedUnix,
                                timeframeRecord.ema21State.nextFetchTime,
                                timeframeRecord.ema21State.emaAvailableTime,
                                timeframeRecord.ema21State.pairCreatedTime,
                                timeframeRecord.ema21State.status
                            ))
                        
                        if timeframeRecord.ema34State:
                            emaStateData.append((
                                timeframeRecord.ema34State.tokenAddress,
                                timeframeRecord.ema34State.pairAddress,
                                timeframeRecord.ema34State.timeframe,
                                timeframeRecord.ema34State.emaKey,
                                timeframeRecord.ema34State.emaValue,
                                timeframeRecord.ema34State.lastUpdatedUnix,
                                timeframeRecord.ema34State.nextFetchTime,
                                timeframeRecord.ema34State.emaAvailableTime,
                                timeframeRecord.ema34State.pairCreatedTime,
                                timeframeRecord.ema34State.status
                            ))
                        
                        # Collect AVWAP state data
                        if timeframeRecord.avwapState:
                            avwapStateData.append((
                                timeframeRecord.avwapState.tokenAddress,
                                timeframeRecord.avwapState.pairAddress,
                                timeframeRecord.avwapState.timeframe,
                                timeframeRecord.avwapState.avwap,
                                timeframeRecord.avwapState.cumulativePV,
                                timeframeRecord.avwapState.cumulativeVolume,
                                timeframeRecord.avwapState.lastUpdatedUnix,
                                timeframeRecord.avwapState.nextFetchTime
                            ))
                
                # Execute all batch operations
                if timeframeMetadataData:
                    self._batchUpdateTimeframeMetadata(cursor, timeframeMetadataData)
                
                if candleData:
                    self._batchInsertCandles(cursor, candleData)
                
                if vwapSessionData:
                    self._batchInsertVWAPSessions(cursor, vwapSessionData)
                
                if emaStateData:
                    self._batchInsertEMAStates(cursor, emaStateData)
                
                if avwapStateData:
                    self._batchInsertAVWAPStates(cursor, avwapStateData)
            
            return totalCandlesInserted
            
        except Exception as e:
            logger.error(f"Error in batch persist tracked tokens data: {e}")
            return 0

    def batchPersistEMAData(self, trackedTokens: List['TrackedToken']) -> int:
        """
        OPTIMIZED: Batch persist only EMA data (EMA states + candle EMA values)
        
        Args:
            trackedTokens: List of TrackedToken POJOs with EMA data
            
        Returns:
            int: Number of EMA states updated
        """
        try:
            totalEMAStatesUpdated = 0
            
            with self.conn_manager.transaction() as cursor:
                # Collect EMA-specific data for batch operations
                emaStateData = []
                ema21CandleUpdates = []
                ema34CandleUpdates = []
                
                for trackedToken in trackedTokens:
                    for timeframeRecord in trackedToken.timeframeRecords:
                        # Collect EMA21 state data
                        if timeframeRecord.ema21State:
                            emaStateData.append((
                                timeframeRecord.ema21State.tokenAddress,
                                timeframeRecord.ema21State.pairAddress,
                                timeframeRecord.ema21State.timeframe,
                                timeframeRecord.ema21State.emaKey,
                                timeframeRecord.ema21State.emaValue,
                                timeframeRecord.ema21State.lastUpdatedUnix,
                                timeframeRecord.ema21State.nextFetchTime,
                                timeframeRecord.ema21State.emaAvailableTime,
                                timeframeRecord.ema21State.pairCreatedTime,
                                timeframeRecord.ema21State.status
                            ))
                            totalEMAStatesUpdated += 1
                            
                            # Collect EMA21 candle updates
                            for candle in timeframeRecord.ohlcvDetails:
                                if candle.ema21Value is not None:
                                    ema21CandleUpdates.append((
                                        candle.ema21Value,
                                        candle.tokenAddress,
                                        candle.timeframe,
                                        candle.unixTime
                                    ))
                        
                        # Collect EMA34 state data
                        if timeframeRecord.ema34State:
                            emaStateData.append((
                                timeframeRecord.ema34State.tokenAddress,
                                timeframeRecord.ema34State.pairAddress,
                                timeframeRecord.ema34State.timeframe,
                                timeframeRecord.ema34State.emaKey,
                                timeframeRecord.ema34State.emaValue,
                                timeframeRecord.ema34State.lastUpdatedUnix,
                                timeframeRecord.ema34State.nextFetchTime,
                                timeframeRecord.ema34State.emaAvailableTime,
                                timeframeRecord.ema34State.pairCreatedTime,
                                timeframeRecord.ema34State.status
                            ))
                            totalEMAStatesUpdated += 1
                            
                            # Collect EMA34 candle updates
                            for candle in timeframeRecord.ohlcvDetails:
                                if candle.ema34Value is not None:
                                    ema34CandleUpdates.append((
                                        candle.ema34Value,
                                        candle.tokenAddress,
                                        candle.timeframe,
                                        candle.unixTime
                                    ))
                
                # Execute EMA-specific batch operations
                if emaStateData:
                    self._batchInsertEMAStates(cursor, emaStateData)
                
                # Update EMA21 values
                if ema21CandleUpdates:
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET ema21value = %s
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """, ema21CandleUpdates)
                    logger.info(f"Batch updated {len(ema21CandleUpdates)} EMA21 candle values")
                
                # Update EMA34 values
                if ema34CandleUpdates:
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET ema34value = %s
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """, ema34CandleUpdates)
                    logger.info(f"Batch updated {len(ema34CandleUpdates)} EMA34 candle values")
                        
                
            return totalEMAStatesUpdated
            
        except Exception as e:
            logger.error(f"Error in batch persist EMA data: {e}")
            return 0

    def batchPersistVWAPData(self, trackedTokens: List['TrackedToken']) -> int:
       
        try:
            totalVWAPSessionsUpdated = 0
            
            with self.conn_manager.transaction() as cursor:
                # Collect VWAP-specific data for batch operations
                vwapSessionData = []
                vwapCandleUpdates = []
                
                for trackedToken in trackedTokens:
                    for timeframeRecord in trackedToken.timeframeRecords:
                        # Collect VWAP session data
                        if timeframeRecord.vwapSession:
                            vwapSessionData.append((
                                timeframeRecord.vwapSession.tokenAddress,
                                timeframeRecord.vwapSession.pairAddress,
                                timeframeRecord.vwapSession.timeframe,
                                timeframeRecord.vwapSession.sessionStartUnix,
                                timeframeRecord.vwapSession.sessionEndUnix,
                                timeframeRecord.vwapSession.cumulativePV,
                                timeframeRecord.vwapSession.cumulativeVolume,
                                timeframeRecord.vwapSession.currentVWAP,
                                timeframeRecord.vwapSession.lastCandleUnix,
                                timeframeRecord.vwapSession.nextCandleFetch
                            ))
                            totalVWAPSessionsUpdated += 1
                            
                            # Collect VWAP candle updates
                            for candle in timeframeRecord.ohlcvDetails:
                                if candle.vwapValue is not None:
                                    vwapCandleUpdates.append((
                                        candle.vwapValue,
                                        candle.tokenAddress,
                                        candle.timeframe,
                                        candle.unixTime
                                    ))
                
                # Execute VWAP-specific batch operations
                if vwapSessionData:
                    self._batchInsertVWAPSessions(cursor, vwapSessionData)
                
                if vwapCandleUpdates:
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET vwapvalue = %s
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """, vwapCandleUpdates)
                    logger.info(f"Batch updated {len(vwapCandleUpdates)} VWAP candle values")
                
                logger.info(f"Batch persisted {totalVWAPSessionsUpdated} VWAP sessions and {len(vwapCandleUpdates)} candle VWAP values")
                return totalVWAPSessionsUpdated
                
        except Exception as e:
            logger.error(f"Error in batch persist VWAP data: {e}")
            return 0

    def getAllAVWAPDataForScheduler(self) -> List['TrackedToken']:
        """
        SINGLE OPTIMIZED QUERY: Get all AVWAP data with corresponding candles for scheduler
        
        This method implements the new optimized approach:
        1. JOIN avwapstates with trackedtokens to get only active tokens
        2. JOIN with timeframemetadata to get lastfetchedat for each timeframe
        3. JOIN with ohlcvdetails to get candles where unixtime > lastupdatedunix
        4. All in one highly optimized query for scalability
        
        Returns:
            List[TrackedToken]: List of tracked tokens with AVWAP data and candles
        """
        try:        
            with self.conn_manager.transaction() as cursor:
                # Single optimized query with JOINs for AVWAP data
                cursor.execute(text("""
                    WITH avwap_data AS (
                        SELECT 
                            avs.tokenaddress,
                            avs.pairaddress,
                            avs.timeframe,
                            avs.avwap,
                            avs.cumulativepv,
                            avs.cumulativevolume,
                            avs.lastupdatedunix,
                            avs.nextfetchtime,
                            tmf.id as timeframeid,
                            tmf.lastfetchedat,
                            CASE 
                                WHEN avs.lastupdatedunix IS NOT NULL THEN avs.lastupdatedunix  -- Get candles after last updated
                                ELSE 0  -- Get all candles if no previous update
                            END as candle_from_time
                        FROM avwapstates avs
                        INNER JOIN trackedtokens tt ON avs.tokenaddress = tt.tokenaddress AND avs.pairaddress = tt.pairaddress
                        INNER JOIN timeframemetadata tmf ON avs.tokenaddress = tmf.tokenaddress AND avs.timeframe = tmf.timeframe
                        WHERE tt.status = 1
                          AND tmf.isactive = TRUE
                    ),
                    candle_data AS (
                        SELECT 
                            ad.tokenaddress,
                            ad.pairaddress,
                            ad.timeframe,
                            o.unixtime,
                            o.timebucket,
                            o.openprice,
                            o.highprice,
                            o.lowprice,
                            o.closeprice,
                            o.volume,
                            o.trades,
                            o.iscomplete,
                            o.datasource
                        FROM avwap_data ad
                        INNER JOIN ohlcvdetails o ON ad.tokenaddress = o.tokenaddress AND ad.timeframe = o.timeframe
                        WHERE o.unixtime > ad.candle_from_time
                          AND o.iscomplete = TRUE
                    )
                    SELECT 
                        ad.tokenaddress,
                        ad.pairaddress,
                        ad.timeframe,
                        ad.timeframeid,
                        ad.avwap,
                        ad.cumulativepv,
                        ad.cumulativevolume,
                        ad.lastupdatedunix,
                        ad.nextfetchtime,
                        ad.lastfetchedat,
                        cd.unixtime as candle_unixtime,
                        cd.timebucket as candle_timebucket,
                        cd.openprice as candle_openprice,
                        cd.highprice as candle_highprice,
                        cd.lowprice as candle_lowprice,
                        cd.closeprice as candle_closeprice,
                        cd.volume as candle_volume,
                        cd.trades as candle_trades,
                        cd.iscomplete as candle_iscomplete,
                        cd.datasource as candle_datasource
                    FROM avwap_data ad
                    LEFT JOIN candle_data cd ON ad.tokenaddress = cd.tokenaddress 
                        AND ad.timeframe = cd.timeframe
                    ORDER BY ad.tokenaddress, ad.timeframe, cd.unixtime ASC
                """))
                
                # Organize results into POJOs
                trackedTokens = {}
                
                for row in cursor.fetchall():
                    tokenAddress = row['tokenaddress']
                    pairAddress = row['pairaddress']
                    timeframe = row['timeframe']
                    timeframeId = row['timeframeid']
                    
                    # Initialize TrackedToken if not exists
                    if tokenAddress not in trackedTokens:
                        trackedTokens[tokenAddress] = TrackedToken(
                            trackedTokenId=0,  # Will be set from database if needed
                            tokenAddress=tokenAddress,
                            symbol='',  # Not needed for AVWAP processing
                            name='',    # Not needed for AVWAP processing
                            pairAddress=pairAddress,
                            addedBy='scheduler'
                        )
                    
                    # Get or create TimeframeRecord for this timeframe
                    timeframeRecord = trackedTokens[tokenAddress].getTimeframeRecord(timeframe)
                    if not timeframeRecord:
                        timeframeRecord = TimeframeRecord(
                            timeframeId=timeframeId,
                            tokenAddress=tokenAddress,
                            pairAddress=pairAddress,
                            timeframe=timeframe,
                            nextFetchAt=row['lastfetchedat'] or 0,
                            lastFetchedAt=row['lastfetchedat'],
                            isActive=True
                        )
                        trackedTokens[tokenAddress].addTimeframeRecord(timeframeRecord)
                    
                    # Create or update AVWAPState
                    avwapState = AVWAPState(
                        tokenAddress=tokenAddress,
                        pairAddress=pairAddress,
                        timeframe=timeframe,
                        avwap=float(row['avwap']) if row['avwap'] else None,
                        cumulativePV=float(row['cumulativepv']) if row['cumulativepv'] else None,
                        cumulativeVolume=float(row['cumulativevolume']) if row['cumulativevolume'] else None,
                        lastUpdatedUnix=row['lastupdatedunix'],
                        nextFetchTime=row['nextfetchtime']
                    )
                    
                    # Set AVWAPState in TimeframeRecord
                    timeframeRecord.avwapState = avwapState
                    
                    # Add candle data if exists
                    if row['candle_unixtime']:
                        # Create OHLCVDetails with all candle data (AVWAP needs full OHLCV data)
                        candle = OHLCVDetails(
                            tokenAddress=tokenAddress,
                            pairAddress=pairAddress,
                            timeframe=timeframe,
                            unixTime=row['candle_unixtime'],
                            timeBucket=row['candle_timebucket'],
                            openPrice=float(row['candle_openprice']),
                            highPrice=float(row['candle_highprice']),
                            lowPrice=float(row['candle_lowprice']),
                            closePrice=float(row['candle_closeprice']),
                            volume=float(row['candle_volume']),
                            trades=row['candle_trades'],
                            isComplete=row['candle_iscomplete'],
                            dataSource=row['candle_datasource']
                        )
                        timeframeRecord.addOHLCVDetail(candle)
                
                return list(trackedTokens.values())
                
        except Exception as e:
            logger.error(f"Error getting AVWAP data with candles for scheduler: {e}")
            return []

    def batchPersistAVWAPData(self, trackedTokens: List['TrackedToken']) -> int:
        """
        OPTIMIZED: Batch persist only AVWAP data (AVWAP states + candle AVWAP values)
        
        Args:
            trackedTokens: List of TrackedToken POJOs with AVWAP data
            
        Returns:
            int: Number of AVWAP states updated
        """
        try:
            totalAVWAPStatesUpdated = 0
            
            with self.conn_manager.transaction() as cursor:
                # Collect AVWAP-specific data for batch operations
                avwapStateData = []
                avwapCandleUpdates = []
                
                for trackedToken in trackedTokens:
                    for timeframeRecord in trackedToken.timeframeRecords:
                        # Collect AVWAP state data
                        if timeframeRecord.avwapState:
                            avwapStateData.append((
                                timeframeRecord.avwapState.tokenAddress,
                                timeframeRecord.avwapState.pairAddress,
                                timeframeRecord.avwapState.timeframe,
                                timeframeRecord.avwapState.avwap,
                                timeframeRecord.avwapState.cumulativePV,
                                timeframeRecord.avwapState.cumulativeVolume,
                                timeframeRecord.avwapState.lastUpdatedUnix,
                                timeframeRecord.avwapState.nextFetchTime
                            ))
                            totalAVWAPStatesUpdated += 1
                            
                            # Collect AVWAP candle updates
                            for candle in timeframeRecord.ohlcvDetails:
                                if candle.avwapValue is not None:
                                    avwapCandleUpdates.append((
                                        candle.avwapValue,
                                        candle.tokenAddress,
                                        candle.timeframe,
                                        candle.unixTime
                                    ))
                
                # Execute AVWAP-specific batch operations
                if avwapStateData:
                    self._batchInsertAVWAPStates(cursor, avwapStateData)
                
                if avwapCandleUpdates:
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET avwapvalue = %s
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """, avwapCandleUpdates)
                    logger.info(f"Batch updated {len(avwapCandleUpdates)} AVWAP candle values")
                
                logger.info(f"Batch persisted {totalAVWAPStatesUpdated} AVWAP states and {len(avwapCandleUpdates)} candle AVWAP values")
                return totalAVWAPStatesUpdated
                
        except Exception as e:
            logger.error(f"Error in batch persist AVWAP data: {e}")
            return 0

    def _batchUpdateTimeframeMetadata(self, cursor, timeframeMetadataData: List[Tuple]):
        """Batch update timeframe metadata"""
        cursor.executemany("""
            INSERT INTO timeframemetadata 
            (tokenaddress, pairaddress, timeframe, lastfetchedat, nextfetchat, createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, pairaddress, timeframe) 
            DO UPDATE SET 
                lastfetchedat = EXCLUDED.lastfetchedat,
                nextfetchat = EXCLUDED.nextfetchat,
                lastupdatedat = NOW()
        """, timeframeMetadataData)

    def _batchInsertCandles(self, cursor, candleData: List[Tuple]):
        """Batch insert candles with indicator values"""
        cursor.executemany("""
            INSERT INTO ohlcvdetails 
            (timeframeid, tokenaddress, pairaddress, timeframe, unixtime, timebucket, 
             openprice, highprice, lowprice, closeprice, volume, trades,
             vwapvalue, avwapvalue, ema21value, ema34value, iscomplete, datasource,
             createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, timeframe, unixtime) 
            DO UPDATE SET 
                vwapvalue = EXCLUDED.vwapvalue,
                avwapvalue = EXCLUDED.avwapvalue,
                ema21value = EXCLUDED.ema21value,
                ema34value = EXCLUDED.ema34value,
                lastupdatedat = NOW()
        """, candleData)

    def _batchInsertVWAPSessions(self, cursor, vwapSessionData: List[Tuple]):
        """Batch insert/update VWAP sessions"""
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

    def _batchInsertEMAStates(self, cursor, emaStateData: List[Tuple]):
        """Batch insert/update EMA states"""
        cursor.executemany("""
            INSERT INTO emastates 
            (tokenaddress, pairaddress, timeframe, emakey, emavalue, 
             lastupdatedunix, nextfetchtime, emaavailabletime, paircreatedtime, status,
             createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, timeframe, emakey) 
            DO UPDATE SET 
                emavalue = EXCLUDED.emavalue,
                lastupdatedunix = EXCLUDED.lastupdatedunix,
                nextfetchtime = EXCLUDED.nextfetchtime,
                status = EXCLUDED.status,
                lastupdatedat = NOW()
        """, emaStateData)

    def _batchInsertAVWAPStates(self, cursor, avwapStateData: List[Tuple]):
        """Batch insert/update AVWAP states"""
        cursor.executemany("""
            INSERT INTO avwapstates 
            (tokenaddress, pairaddress, timeframe, avwap, cumulativepv, cumulativevolume, 
             lastupdatedunix, nextfetchtime, createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, timeframe) 
            DO UPDATE SET 
                avwap = EXCLUDED.avwap,
                cumulativepv = EXCLUDED.cumulativepv,
                cumulativevolume = EXCLUDED.cumulativevolume,
                lastupdatedunix = EXCLUDED.lastupdatedunix,
                nextfetchtime = EXCLUDED.nextfetchtime,
                lastupdatedat = NOW()
        """, avwapStateData)

    