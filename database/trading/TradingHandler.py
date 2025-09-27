from config.Config import get_config
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from datetime import datetime
import json
from database.operations.BaseDBHandler import BaseDBHandler
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from logs.logger import get_logger
from sqlalchemy import text
from enum import IntEnum
from datetime import datetime, timezone


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
from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails, Alert
# Add EMA available times for processing
from api.trading.request import EMAState        


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
                ema12value DECIMAL(20,8),
                ema21value DECIMAL(20,8),
                ema34value DECIMAL(20,8),
                trend VARCHAR(20),
                status VARCHAR(50),
                trend12 VARCHAR(20),
                status12 VARCHAR(50),
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
        
        # 7. Alerts
        cursor.execute(text("""
            CREATE TABLE IF NOT EXISTS alerts (
                alertid BIGSERIAL PRIMARY KEY,
                tokenid BIGINT NOT NULL,
                tokenaddress CHAR(44) NOT NULL,
                pairaddress CHAR(44) NOT NULL,
                timeframe VARCHAR(10) NOT NULL,
                vwap DECIMAL(20,8),
                ema12 DECIMAL(20,8),
                ema21 DECIMAL(20,8),
                ema34 DECIMAL(20,8),
                avwap DECIMAL(20,8),
                lastupdatedunix BIGINT,
                trend VARCHAR(20),
                status VARCHAR(50),
                trend12 VARCHAR(20),
                status12 VARCHAR(50),
                touchcount INTEGER DEFAULT 0,
                latesttouchunix BIGINT,
                touchcount12 INTEGER DEFAULT 0,
                latesttouchunix12 BIGINT,
                createdat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                lastupdatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(tokenaddress, timeframe)
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
                
                # Track seen candle timestamps per timeframe to prevent duplicates (space and time efficient)
                seenCandles = {}  # {tokenAddress: {timeframe: set(unixTimes)}}
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
                        candleUnixTime = record[TradingHandlerConstants.OHLCVDetails.UNIX_TIME]
                        
                        # Initialize seenCandles structure if needed
                        if tokenAddress not in seenCandles:
                            seenCandles[tokenAddress] = {}
                        if timeframe not in seenCandles[tokenAddress]:
                            seenCandles[tokenAddress][timeframe] = set()
                        
                        # O(1) check if candle already exists using set
                        if candleUnixTime not in seenCandles[tokenAddress][timeframe]:
                            # Mark as seen
                            seenCandles[tokenAddress][timeframe].add(candleUnixTime)
                            
                            ohlcvDetail = OHLCVDetails(
                                tokenAddress=tokenAddress,
                                pairAddress=pairAddress,
                                timeframe=timeframe,
                                unixTime=candleUnixTime,
                                timeBucket=CommonUtil.calculateInitialStartTime(candleUnixTime, timeframe),
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
                # Track seen candle timestamps per timeframe to prevent duplicates (space and time efficient)
                seenCandles = {}  # {tokenAddress: {timeframe: set(unixTimes)}}

                records = cursor.fetchall()
                
                for row in records:
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
                    if emaPeriod == 12:
                        timeframeRecord.ema12State = emaState
                    elif emaPeriod == 21:
                        timeframeRecord.ema21State = emaState
                    elif emaPeriod == 34:
                        timeframeRecord.ema34State = emaState
                    
                    # Add candle data if exists (only close price needed for EMA)
                    if row[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME]:
                        candleUnixTime = row[IndicatorConstants.EMAStates.CANDLE_UNIX_TIME]
                        
                        # Initialize seenCandles structure if needed
                        if tokenAddress not in seenCandles:
                            seenCandles[tokenAddress] = {}
                        if timeframe not in seenCandles[tokenAddress]:
                            seenCandles[tokenAddress][timeframe] = set()
                        
                        # O(1) check if candle already exists using set
                        if candleUnixTime not in seenCandles[tokenAddress][timeframe]:
                            # Mark as seen
                            seenCandles[tokenAddress][timeframe].add(candleUnixTime)
                            
                            # Create OHLCVDetails with only close price (EMA only needs close price)
                            candle = OHLCVDetails(
                                tokenAddress=tokenAddress,
                                pairAddress=pairAddress,
                                timeframe=timeframe,
                                unixTime=candleUnixTime,
                                timeBucket=self._calculateTimeBucket(candleUnixTime, timeframe),
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
                            candle.ema12Value,
                            candle.ema21Value,
                            candle.ema34Value,
                            candle.trend,
                            candle.status,
                            candle.trend12,
                            candle.status12,
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
                    if timeframeRecord.ema12State:
                        emaStateData.append((
                            timeframeRecord.ema12State.tokenAddress,
                            timeframeRecord.ema12State.pairAddress,
                            timeframeRecord.ema12State.timeframe,
                            timeframeRecord.ema12State.emaKey,
                            timeframeRecord.ema12State.emaValue,
                            timeframeRecord.ema12State.lastUpdatedUnix,
                            timeframeRecord.ema12State.nextFetchTime,
                            timeframeRecord.ema12State.emaAvailableTime,
                            timeframeRecord.ema12State.pairCreatedTime,
                            timeframeRecord.ema12State.status
                        ))

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
                                candle.ema12Value,
                                candle.ema21Value,
                                candle.ema34Value,
                                candle.trend,
                                candle.status,
                                candle.trend12,
                                candle.status12,
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
                        
                        if timeframeRecord.ema12State:
                            emaStateData.append((
                                timeframeRecord.ema12State.tokenAddress,
                                timeframeRecord.ema12State.pairAddress,
                                timeframeRecord.ema12State.timeframe,
                                timeframeRecord.ema12State.emaKey,
                                timeframeRecord.ema12State.emaValue,
                                timeframeRecord.ema12State.lastUpdatedUnix,
                                timeframeRecord.ema12State.nextFetchTime,
                                timeframeRecord.ema12State.emaAvailableTime,
                                timeframeRecord.ema12State.pairCreatedTime,
                                timeframeRecord.ema12State.status
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
        try:
            totalEMAStatesUpdated = 0
            
            with self.conn_manager.transaction() as cursor:
                emaStateData = []
                ema12CandleUpdates = []
                ema21CandleUpdates = []
                ema34CandleUpdates = []
                
                for trackedToken in trackedTokens:
                    for timeframeRecord in trackedToken.timeframeRecords:
                        # Collect EMA12 state data
                        if timeframeRecord.ema12State:
                            emaStateData.append((
                                timeframeRecord.ema12State.tokenAddress,
                                timeframeRecord.ema12State.pairAddress,
                                timeframeRecord.ema12State.timeframe,
                                timeframeRecord.ema12State.emaKey,
                                timeframeRecord.ema12State.emaValue,
                                timeframeRecord.ema12State.lastUpdatedUnix,
                                timeframeRecord.ema12State.nextFetchTime,
                                timeframeRecord.ema12State.emaAvailableTime,
                                timeframeRecord.ema12State.pairCreatedTime,
                                timeframeRecord.ema12State.status
                            ))
                            totalEMAStatesUpdated += 1
                            
                            # Collect EMA12 candle updates
                            for candle in timeframeRecord.ohlcvDetails:
                                if candle.ema12Value is not None:
                                    ema12CandleUpdates.append((
                                        candle.ema12Value,
                                        candle.tokenAddress,
                                        candle.timeframe,
                                        candle.unixTime
                                    ))
                        
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
                
                # Update EMA12 values
                if ema12CandleUpdates:
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET ema12value = %s
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """, ema12CandleUpdates)
                    logger.info(f"Batch updated {len(ema12CandleUpdates)} EMA12 candle values")
                
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
                # Track seen candle timestamps per timeframe to prevent duplicates (space and time efficient)
                seenCandles = {}  # {tokenAddress: {timeframe: set(unixTimes)}}
                
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
                        candleUnixTime = row['candle_unixtime']
                        
                        # Initialize seenCandles structure if needed
                        if tokenAddress not in seenCandles:
                            seenCandles[tokenAddress] = {}
                        if timeframe not in seenCandles[tokenAddress]:
                            seenCandles[tokenAddress][timeframe] = set()
                        
                        # O(1) check if candle already exists using set
                        if candleUnixTime not in seenCandles[tokenAddress][timeframe]:
                            # Mark as seen
                            seenCandles[tokenAddress][timeframe].add(candleUnixTime)
                            
                            # Create OHLCVDetails with all candle data (AVWAP needs full OHLCV data)
                            candle = OHLCVDetails(
                                tokenAddress=tokenAddress,
                                pairAddress=pairAddress,
                                timeframe=timeframe,
                                unixTime=candleUnixTime,
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
             vwapvalue, avwapvalue, ema12value, ema21value, ema34value, trend, status, trend12, status12, iscomplete, datasource,
             createdat, lastupdatedat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, timeframe, unixtime) 
            DO UPDATE SET 
                vwapvalue = EXCLUDED.vwapvalue,
                avwapvalue = EXCLUDED.avwapvalue,
                ema12value = EXCLUDED.ema12value,
                ema21value = EXCLUDED.ema21value,
                ema34value = EXCLUDED.ema34value,
                trend = EXCLUDED.trend,
                status = EXCLUDED.status,
                trend12 = EXCLUDED.trend12,
                status12 = EXCLUDED.status12,
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
    
    def createInitialAlerts(self, tokenId: int, tokenAddress: str, pairAddress: str, 
                           timeframes: List[str]) -> bool:
        """
        Create initial alert entries for new token
        
        Args:
            tokenId: Tracked token ID
            tokenAddress: Token contract address
            pairAddress: Trading pair address
            timeframes: List of timeframes
            
        Returns:
            bool: True if successful
        """
        try:
            with self.conn_manager.transaction() as cursor:
                alertData = []
                for timeframe in timeframes:
                    alertData.append((
                        tokenId,
                        tokenAddress,
                        pairAddress,
                        timeframe,
                        'NEUTRAL'  # Initial trend
                    ))
                
                cursor.executemany("""
                    INSERT INTO alerts 
                    (tokenid, tokenaddress, pairaddress, timeframe, trend, 
                     touchcount, createdat, lastupdatedat)
                    VALUES (%s, %s, %s, %s, %s, 0, NOW(), NOW())
                    ON CONFLICT (tokenaddress, timeframe) DO NOTHING
                """, alertData)
                
                logger.info(f"Created {len(alertData)} initial alerts for token {tokenAddress}")
                return True
                
        except Exception as e:
            logger.error(f"Error creating initial alerts: {e}")
            return False
    
    def batchPersistAlerts(self, trackedTokens: List['TrackedToken']) -> int:
        """
        Batch persist alert data from tracked tokens
        
        Args:
            trackedTokens: List of TrackedToken POJOs with alert data
            
        Returns:
            int: Number of alerts updated
        """
        try:
            totalAlertsUpdated = 0
            
            with self.conn_manager.transaction() as cursor:
                # Collect alert data for batch operations
                alertData = []
                candleTrendStatusUpdates = []
                
                for trackedToken in trackedTokens:
                    for timeframeRecord in trackedToken.timeframeRecords:
                        if timeframeRecord.alert:
                            alert = timeframeRecord.alert
                            alertData.append((
                                alert.tokenId,
                                alert.tokenAddress,
                                alert.pairAddress,
                                alert.timeframe,
                                alert.vwap,
                                alert.ema12,
                                alert.ema21,
                                alert.ema34,
                                alert.avwap,
                                alert.lastUpdatedUnix,
                                alert.trend,
                                alert.status,
                                alert.trend12,
                                alert.status12,
                                alert.touchCount,
                                alert.latestTouchUnix
                            ))
                            totalAlertsUpdated += 1
                            
                            # Collect candle trend/status updates
                            for candle in timeframeRecord.ohlcvDetails:
                                if (candle.trend is not None or candle.status is not None or 
                                    candle.trend12 is not None or candle.status12 is not None):
                                    candleTrendStatusUpdates.append((
                                        candle.trend,
                                        candle.status,
                                        candle.trend12,
                                        candle.status12,
                                        candle.tokenAddress,
                                        candle.timeframe,
                                        candle.unixTime
                                    ))
                
                # Execute alert updates
                if alertData:
                    cursor.executemany("""
                        INSERT INTO alerts 
                        (tokenid, tokenaddress, pairaddress, timeframe, vwap, ema12, ema21, ema34, avwap,
                         lastupdatedunix, trend, status, trend12, status12, touchcount, latesttouchunix,
                         createdat, lastupdatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (tokenaddress, timeframe) 
                        DO UPDATE SET 
                            vwap = EXCLUDED.vwap,
                            ema12 = EXCLUDED.ema12,
                            ema21 = EXCLUDED.ema21,
                            ema34 = EXCLUDED.ema34,
                            avwap = EXCLUDED.avwap,
                            lastupdatedunix = EXCLUDED.lastupdatedunix,
                            trend = EXCLUDED.trend,
                            status = EXCLUDED.status,
                            trend12 = EXCLUDED.trend12,
                            status12 = EXCLUDED.status12,
                            touchcount = EXCLUDED.touchcount,
                            latesttouchunix = EXCLUDED.latesttouchunix,
                            lastupdatedat = NOW()
                    """, alertData)
                
                # Update candle trend/status
                if candleTrendStatusUpdates:
                    cursor.executemany("""
                        UPDATE ohlcvdetails 
                        SET trend = %s, status = %s, trend12 = %s, status12 = %s
                        WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                    """, candleTrendStatusUpdates)
                    logger.info(f"Updated trend/status for {len(candleTrendStatusUpdates)} candles")
                
                logger.info(f"Batch persisted {totalAlertsUpdated} alerts")
                return totalAlertsUpdated
                
        except Exception as e:
            logger.error(f"Error in batch persist alerts: {e}")
            return 0
    
    def getCurrentAlertStateAndNewCandles(self, tokenAddress: str = None) -> List['TrackedToken']:
        try:
            with self.conn_manager.transaction() as cursor:
                # Build where clause
                whereClause = "WHERE tt.status = 1"
                params = []
                if tokenAddress:
                    whereClause += " AND tt.tokenaddress = %s"
                    params.append(tokenAddress)
                
                # Get alerts and candles for processing
                query = text(f"""
                    WITH alert_data AS (
                        SELECT 
                            a.alertid,
                            a.tokenid,
                            a.tokenaddress,
                            a.pairaddress,
                            a.timeframe,
                            a.vwap as alert_vwap,
                            a.ema12 as alert_ema12,
                            a.ema21 as alert_ema21,
                            a.ema34 as alert_ema34,
                            a.avwap as alert_avwap,
                            a.lastupdatedunix,
                            a.trend as alert_trend,
                            a.status as alert_status,
                            a.trend12 as alert_trend12,
                            a.status12 as alert_status12,
                            a.touchcount,
                            a.latesttouchunix,
                            tt.trackedtokenid,
                            tt.symbol,
                            tt.name,
                            tm.id as timeframeid,
                            tm.lastfetchedat,
                            es12.emaavailabletime as ema12availabletime,
                            es21.emaavailabletime as ema21availabletime,
                            es34.emaavailabletime as ema34availabletime
                        FROM alerts a
                        INNER JOIN trackedtokens tt ON a.tokenid = tt.trackedtokenid
                        INNER JOIN timeframemetadata tm ON a.tokenaddress = tm.tokenaddress 
                            AND a.timeframe = tm.timeframe
                        LEFT JOIN emastates es12 ON a.tokenaddress = es12.tokenaddress 
                            AND a.timeframe = es12.timeframe AND es12.emakey = '12'
                        LEFT JOIN emastates es21 ON a.tokenaddress = es21.tokenaddress 
                            AND a.timeframe = es21.timeframe AND es21.emakey = '21'
                        LEFT JOIN emastates es34 ON a.tokenaddress = es34.tokenaddress 
                            AND a.timeframe = es34.timeframe AND es34.emakey = '34'
                        {whereClause}
                    )
                    SELECT 
                        ad.*,
                        o.unixtime,
                        o.timebucket,
                        o.openprice,
                        o.highprice,
                        o.lowprice,
                        o.closeprice,
                        o.volume,
                        o.trades,
                        o.vwapvalue,
                        o.avwapvalue,
                        o.ema12value,
                        o.ema21value,
                        o.ema34value,
                        o.trend as candle_trend,
                        o.status as candle_status,
                        o.trend12 as candle_trend12,
                        o.status12 as candle_status12
                    FROM alert_data ad
                    LEFT JOIN ohlcvdetails o ON ad.tokenaddress = o.tokenaddress 
                        AND ad.timeframe = o.timeframe
                        AND o.unixtime > COALESCE(ad.lastupdatedunix, 0)
                        AND o.vwapvalue IS NOT NULL
                        AND o.avwapvalue IS NOT NULL
                        AND (ad.ema12availabletime IS NULL OR o.unixtime < ad.ema12availabletime OR o.ema12value IS NOT NULL)
                        AND (ad.ema21availabletime IS NULL OR o.unixtime < ad.ema21availabletime OR o.ema21value IS NOT NULL)
                        AND (ad.ema34availabletime IS NULL OR o.unixtime < ad.ema34availabletime OR o.ema34value IS NOT NULL)
                    ORDER BY ad.tokenaddress, ad.timeframe, o.unixtime
                """)
                
                cursor.execute(query, params)
                records = cursor.fetchall()
                
                # Organize into POJOs
                trackedTokens = {}
                # Track seen candle timestamps per timeframe to prevent duplicates (space and time efficient)
                seenCandles = {}  # {tokenAddress: {timeframe: set(unixTimes)}}
                
                for row in records:
                    tokenAddress = row['tokenaddress']
                    
                    # Create or get TrackedToken
                    if tokenAddress not in trackedTokens:
                        trackedTokens[tokenAddress] = TrackedToken(
                            trackedTokenId=row['trackedtokenid'],
                            tokenAddress=tokenAddress,
                            symbol=row['symbol'],
                            name=row['name'],
                            pairAddress=row['pairaddress'],
                            addedBy='alert_processor'
                        )
                    
                    # Get or create TimeframeRecord
                    timeframe = row['timeframe']
                    timeframeRecord = trackedTokens[tokenAddress].getTimeframeRecord(timeframe)
                    if not timeframeRecord:
                        timeframeRecord = TimeframeRecord(
                            timeframeId=row['timeframeid'],
                            tokenAddress=tokenAddress,
                            pairAddress=row['pairaddress'],
                            timeframe=timeframe,
                            lastFetchedAt=row['lastfetchedat'],
                            isActive=True
                        )
                        
                        # Add alert data
                        timeframeRecord.alert = Alert(
                            alertId=row['alertid'],
                            tokenId=row['tokenid'],
                            tokenAddress=tokenAddress,
                            pairAddress=row['pairaddress'],
                            timeframe=timeframe,
                            vwap=row['alert_vwap'],
                            ema12=row['alert_ema12'],
                            ema21=row['alert_ema21'],
                            ema34=row['alert_ema34'],
                            avwap=row['alert_avwap'],
                            lastUpdatedUnix=row['lastupdatedunix'],
                            trend=row['alert_trend'],
                            status=row['alert_status'],
                            trend12=row['alert_trend12'],
                            status12=row['alert_status12'],
                            touchCount=row['touchcount'],
                            latestTouchUnix=row['latesttouchunix']
                        )
                        
                        if row['ema12availabletime']:
                            timeframeRecord.ema12State = EMAState(
                                tokenAddress=tokenAddress,
                                pairAddress=row['pairaddress'],
                                timeframe=timeframe,
                                emaKey='12',
                                emaAvailableTime=row['ema12availabletime']
                            )
                        if row['ema21availabletime']:
                            timeframeRecord.ema21State = EMAState(
                                tokenAddress=tokenAddress,
                                pairAddress=row['pairaddress'],
                                timeframe=timeframe,
                                emaKey='21',
                                emaAvailableTime=row['ema21availabletime']
                            )
                        if row['ema34availabletime']:
                            timeframeRecord.ema34State = EMAState(
                                tokenAddress=tokenAddress,
                                pairAddress=row['pairaddress'],
                                timeframe=timeframe,
                                emaKey='34',
                                emaAvailableTime=row['ema34availabletime']
                            )
                        
                        trackedTokens[tokenAddress].addTimeframeRecord(timeframeRecord)
                    
                    # Add candle data if exists
                    if row['unixtime']:
                        candleUnixTime = row['unixtime']
                        
                        # Initialize seenCandles structure if needed
                        if tokenAddress not in seenCandles:
                            seenCandles[tokenAddress] = {}
                        if timeframe not in seenCandles[tokenAddress]:
                            seenCandles[tokenAddress][timeframe] = set()
                        
                        # O(1) check if candle already exists using set
                        if candleUnixTime not in seenCandles[tokenAddress][timeframe]:
                            # Mark as seen
                            seenCandles[tokenAddress][timeframe].add(candleUnixTime)
                            
                            candle = OHLCVDetails(
                                tokenAddress=tokenAddress,
                                pairAddress=row['pairaddress'],
                                timeframe=timeframe,
                                unixTime=candleUnixTime,
                                timeBucket=row['timebucket'],
                                openPrice=float(row['openprice']),
                                highPrice=float(row['highprice']),
                                lowPrice=float(row['lowprice']),
                                closePrice=float(row['closeprice']),
                                volume=float(row['volume']),
                                trades=row['trades'],
                                vwapValue=float(row['vwapvalue']) if row['vwapvalue'] else None,
                                avwapValue=float(row['avwapvalue']) if row['avwapvalue'] else None,
                                ema12Value=float(row['ema12value']) if row['ema12value'] else None,
                                ema21Value=float(row['ema21value']) if row['ema21value'] else None,
                                ema34Value=float(row['ema34value']) if row['ema34value'] else None,
                                trend=row['candle_trend'],
                                status=row['candle_status'],
                                trend12=row['candle_trend12'],
                                status12=row['candle_status12'],
                                isComplete=True,
                                dataSource='database'
                            )
                            timeframeRecord.addOHLCVDetail(candle)
                
                return list(trackedTokens.values())
                
        except Exception as e:
            logger.error(f"Error getting alerts for processing: {e}")
            return []

    