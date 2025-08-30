from config.Config import get_config
from typing import Optional, Dict, Any, List, Union
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler, AdditionSource, EMAStatus
from database.trading.TradingModels import (
    TrackedToken, OHLCVCandle, BirdEyeOHLCVItem, BirdEyeOHLCVResponse,
    BackfillRequest, BackfillResult, EMACalculationInput, VWAPCalculationInput
)
import requests
from decimal import Decimal
import time
from datetime import datetime, timedelta
from logs.logger import get_logger
from database.auth.ServiceCredentialsEnum import ServiceCredentials, CredentialType
from actions.DexscrennerAction import DexScreenerAction
from actions.TradingActionUtil import TradingActionUtil
import pytz
import uuid
import json
from sqlalchemy import text

logger = get_logger(__name__)

class TradingActionEnhanced:
    """Enhanced Trading Action with comprehensive token processing flows"""
    
    def __init__(self, db: PortfolioDB):
        """Initialize with database handler"""
        self.db = db
        self.trading_handler = TradingHandler(db.conn_manager)
        
        # BirdEye API configuration using ServiceCredentials pattern
        self.service = ServiceCredentials.BIRDEYE
        self.baseUrl = self.service.metadata['base_url']
        self.creditsPerCall = self.service.metadata.get('credits_per_call', 40)

    # ===============================================================
    # PHASE 1: NEW TOKEN FLOW (≤7 days)
    # ===============================================================
    
    def addNewToken(self, tokenAddress: str, pairAddress: str, symbol: str, 
                   name: str, pairCreatedTime: int, addedBy: str) -> Dict[str, Any]:
        """Phase 1: Add new token with comprehensive processing"""
        try:
            current_time = int(time.time())
            
            # Step 1: Add token to trackedtokens table
            token_id = self.trading_handler.addToken(
                tokenAddress=tokenAddress,
                symbol=symbol,
                name=name,
                pairAddress=pairAddress,
                pairCreatedTime=pairCreatedTime,
                additionSource=AdditionSource.MANUAL if addedBy != "automatic_system" else AdditionSource.AUTOMATIC,
                addedBy=addedBy
            )
            
            if not token_id:
                return {'success': False, 'error': 'Failed to add token to database'}
            
            # Step 2: Create 15m timeframe record
            self.trading_handler._createTimeframeRecord(tokenAddress, pairAddress, '15m')
            
            # Step 3: Fetch all data from pair created time to now
            backfill_result = self._fetchHistoricalDataFromCreation(tokenAddress, pairAddress, symbol, name, pairCreatedTime, current_time)
            
            if not backfill_result['success']:
                # Rollback token addition
                self.trading_handler.disableToken(tokenAddress, addedBy, "Backfill failed")
                return {'success': False, 'error': f'Backfill failed: {backfill_result["error"]}'}
            
            # Step 5: Aggregate 15m -> 1h and 4h, create timeframe records
            self._aggregateAndCreateTimeframes(tokenAddress, pairAddress)
            
            # Step 6: Get ALL candles for all timeframes in single database call
            all_candles_data = self._getAllCandlesForAllTimeframes(tokenAddress, pairAddress)
            if not all_candles_data:
                logger.warning(f"No candles found after aggregation for {tokenAddress}")
                return {'success': True, 'tokenId': token_id, 'mode': 'new_token_no_indicators'}
            
            # Step 7: Process VWAP using filtered candles (no additional DB calls)
            vwap_result = self._processVWAPWithPreloadedCandles(tokenAddress, pairAddress, pairCreatedTime, all_candles_data)
            if not vwap_result['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"VWAP processing failed: {vwap_result['error']}")
                return vwap_result
            
            # Step 8: Process EMA using filtered candles (no additional DB calls)
            ema_result = self._processEMAWithPreloadedCandles(tokenAddress, pairAddress, pairCreatedTime, all_candles_data)
            if not ema_result['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"EMA processing failed: {ema_result['error']}")
                return ema_result
            
            return {
                'success': True,
                'tokenId': token_id,
                'mode': 'new_token_full_processing',
                'candlesInserted': backfill_result.get('candlesInserted', 0),
                'creditsUsed': backfill_result.get('creditsUsed', 0)
            }
            
        except Exception as e:
            logger.error(f"Error in new token addition: {e}")
            try:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"Addition failed: {str(e)}")
            except:
                pass
            return {'success': False, 'error': str(e)}

    # ===============================================================
    # PHASE 2: OLD TOKEN FLOW (>7 days)
    # ===============================================================
    
    def addOldToken(self, tokenAddress: str, pairAddress: str, symbol: str, name: str,
                   pairCreatedTime: int, ema21Values: Dict, ema34Values: Dict, 
                   referenceUnixTime: int, addedBy: str) -> Dict[str, Any]:
        """Phase 2: Add old token with limited backfill and EMA initialization"""
        try:
            current_time = int(time.time())
            
            # Step 1: Add token to trackedtokens table
            token_id = self.trading_handler.addToken(
                tokenAddress=tokenAddress,
                symbol=symbol,
                name=name,
                pairAddress=pairAddress,
                pairCreatedTime=pairCreatedTime,
                additionSource=AdditionSource.MANUAL,
                addedBy=addedBy
            )
            
            if not token_id:
                return {'success': False, 'error': 'Failed to add token to database'}
            
            # Step 2: Create 15m timeframe record
            self.trading_handler._createTimeframeRecord(tokenAddress, pairAddress, '15m')
            
            # Step 3: Fetch 2 days of 15m data from BirdEye API
            backfill_result = self._fetchHistoricalData(tokenAddress, pairAddress, symbol, name, 48)
            
            if not backfill_result['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, "Backfill failed")
                return {'success': False, 'error': f'Backfill failed: {backfill_result["error"]}'}
            
            # Step 4: Timeframe fetch status updated in pagination method
            
            # Step 5: Aggregate 15m -> 1h and 4h, create timeframe records
            self._aggregateAndCreateTimeframes(tokenAddress, pairAddress)
            
            # Step 6: Get ALL candles for all timeframes in single database call
            all_candles_data = self._getAllCandlesForAllTimeframes(tokenAddress, pairAddress)
            if not all_candles_data:
                logger.warning(f"No candles found after aggregation for {tokenAddress}")
                return {'success': True, 'tokenId': token_id, 'mode': 'old_token_no_indicators'}
            
            # Step 7: Process VWAP using filtered candles (no additional DB calls)
            vwap_result = self._processVWAPWithPreloadedCandles(tokenAddress, pairAddress, pairCreatedTime, all_candles_data)
            if not vwap_result['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"VWAP processing failed: {vwap_result['error']}")
                return vwap_result
            
            # Step 8: Process EMA with user-provided values using pre-loaded candles (no additional DB calls)
            ema_result = self._processEMAForOldTokenWithPreloadedCandles(tokenAddress, pairAddress, pairCreatedTime, 
                                                                       ema21Values, ema34Values, referenceUnixTime, all_candles_data)
            if not ema_result['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"EMA processing failed: {ema_result['error']}")
                return ema_result
            
            return {
                'success': True,
                'tokenId': token_id,
                'mode': 'old_token_limited_backfill',
                'candlesInserted': backfill_result.get('candlesInserted', 0),
                'creditsUsed': backfill_result.get('creditsUsed', 0)
            }
            
        except Exception as e:
            logger.error(f"Error in old token addition: {e}")
            try:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"Addition failed: {str(e)}")
            except:
                pass
            return {'success': False, 'error': str(e)}

    # ===============================================================
    # HELPER METHODS FOR DATA PROCESSING
    # ===============================================================
    
    def _fetchHistoricalData(self, tokenAddress: str, pairAddress: str, 
                           symbol: str, name: str, hours: int) -> Dict:
        """Fetch historical data from BirdEye API (for old tokens - 2 days backfill)"""
        try:
            # Calculate time range
            to_time = int(time.time())
            from_time = to_time - (hours * 3600)
            
            # Fetch all data with proper pagination
            result = self._fetchCandleDataWithPagination(tokenAddress, pairAddress, from_time, to_time)
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return {'success': False, 'error': str(e)}
    
    def _fetchHistoricalDataFromCreation(self, tokenAddress: str, pairAddress: str, 
                                       symbol: str, name: str, pairCreatedTime: int, currentTime: int) -> Dict:
        """Fetch all historical data from pair creation to now (for new tokens)"""
        try:
            # Fetch all data with proper pagination
            result = self._fetchCandleDataWithPagination(tokenAddress, pairAddress, pairCreatedTime, currentTime)
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching historical data from creation: {e}")
            return {'success': False, 'error': str(e)}

    def _aggregateAndCreateTimeframes(self, tokenAddress: str, pairAddress: str):
        """Aggregate 15m data into 1h and 4h with batched database operations"""
        try:
            # Get all 15min candles once
            all_15min_candles = self._get15minCandles(tokenAddress, pairAddress)
            if not all_15min_candles:
                logger.info(f"No 15min candles found for {tokenAddress}")
                return
            
            logger.info(f"Processing {len(all_15min_candles)} 15min candles for aggregation")
            
            # Aggregate to 1 hour using in-memory data - returns candles and latest time
            hourly_result = TradingActionUtil.aggregateToHourlyInMemory(all_15min_candles)
            
            # Aggregate to 4 hour using the in-memory hourly data - returns candles and latest time
            four_hourly_result = TradingActionUtil.aggregateTo4HourlyInMemory(hourly_result['candles'])
            
            # Prepare aggregation results with latest times calculated during aggregation
            aggregation_results = {
                'hourly_candles': hourly_result['candles'],
                'four_hourly_candles': four_hourly_result['candles'],
                'latest_1h_time': hourly_result['latest_time'],
                'latest_4h_time': four_hourly_result['latest_time']
            }
            
            # Batch all database operations in single transaction
            self._persistAggregationResultsInSingleTransaction(
                tokenAddress, pairAddress, aggregation_results
            )
            
            logger.info(f"Aggregation completed: {len(hourly_result['candles'])} hourly, {len(four_hourly_result['candles'])} 4-hourly candles")
            
        except Exception as e:
            logger.error(f"Error in aggregation: {e}")
            raise

    

    

    



    # ===============================================================
    # UTILITY METHODS
    # ===============================================================
    

    def _callBirdEyeAPI(self, pairAddress: str, start_time: int, end_time: int, api_key: str) -> List:
        """Make API call to BirdEye"""
        try:
            url = f"{self.baseUrl}/ohlcv/pair"
            params = {
                'address': pairAddress,
                'type': '15m',
                'time_from': start_time,
                'time_to': end_time
            }
            headers = {
                'X-API-KEY': api_key
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('data', {}).get('items', [])
            else:
                logger.error(f"BirdEye API error: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error calling BirdEye API: {e}")
            return []

    
    

    

    

    

    def _updateEMAInOHLCVForTimestamp(self, tokenAddress: str, timeframe: str, 
                                    ema_period: int, timestamp: int, ema_value: float):
        """Update specific EMA value in OHLCV table for a timestamp"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                column_name = f'ema{ema_period}value'
                
                cursor.execute(text(f"""
                    UPDATE ohlcvdetails 
                    SET {column_name} = %s
                    WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                """), (Decimal(str(ema_value)), tokenAddress, timeframe, timestamp))
                
        except Exception as e:
            logger.error(f"Error updating EMA{ema_period} for timestamp: {e}")

    # ===============================================================
    # API INTEGRATION METHODS
    # ===============================================================
    
    def processTokenAddition(self, tokenAddress: str, pairAddress: str = None, 
                           ema21Values: Dict = None, ema34Values: Dict = None,
                           referenceUnixTime: int = None, addedBy: str = None) -> Dict[str, Any]:
        """Main API integration method - determines flow based on token age"""
        try:
            # Get token info from DexScreener
            dex_action = DexScreenerAction()
            token_info = dex_action.getTokenPrice(tokenAddress)
            
            if not token_info:
                return {'success': False, 'error': 'Token not found on DexScreener'}
            
            # Use DexScreener data as fallback
            symbol = token_info.symbol
            name = token_info.name
            pair_address = pairAddress or token_info.pairAddress
            
            # Calculate pair age from DexScreener pairCreatedAt field
            pair_created_time = token_info.pairCreatedAt // 1000  # ms to seconds
            current_time = int(time.time())
            pair_age_days = (current_time - pair_created_time) / 86400
            
            logger.info(f"Processing token {symbol} (age: {pair_age_days:.1f} days)")
            
            # Route based on pair age
            if pair_age_days <= 7:
                # Phase 1: New token flow
                return self.addNewToken(
                    tokenAddress, pair_address, symbol, name, 
                    pair_created_time, addedBy or "api_user"
                )
            else:
                # Phase 2: Old token flow
                if not all([ema21Values, ema34Values, referenceUnixTime]):
                    return {
                        'success': False,
                        'error': f'Token is {pair_age_days:.1f} days old. For old tokens, please provide: ema21, ema34, referenceUnixTime',
                        'tokenAge': round(pair_age_days, 1),
                        'requiresEMA': True
                    }
                
                return self.addOldToken(
                    tokenAddress, pair_address, symbol, name, pair_created_time,
                    ema21Values, ema34Values, referenceUnixTime, addedBy or "api_user"
                )
                
        except Exception as e:
            logger.error(f"Error processing token addition: {e}")
            return {'success': False, 'error': str(e)}
    
    # ===============================================================
    # AGGREGATION HELPER METHODS
    # ===============================================================
    
    def _get15minCandles(self, tokenAddress: str, pairAddress: str) -> List[Dict]:
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
    
    def _get1hCandles(self, tokenAddress: str, pairAddress: str) -> List[Dict]:
        """Get all 1h candles for aggregation to 4h"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT unixtime, openprice, highprice, lowprice, closeprice, volume
                    FROM ohlcvdetails 
                    WHERE tokenaddress = %s AND pairaddress = %s AND timeframe = '1h' 
                    ORDER BY unixtime ASC
                """), (tokenAddress, pairAddress))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting 1h candles: {e}")
            return []
    
    def _aggregateToHourly(self, tokenAddress: str, pairAddress: str, candles_15m: List[Dict]) -> int:
        """Aggregate 15min candles to hourly with proper validation"""
        try:
            # Group candles by hour periods
            hour_groups = {}
            for candle in candles_15m:
                hour_bucket = (candle['unixtime'] // 3600) * 3600
                if hour_bucket not in hour_groups:
                    hour_groups[hour_bucket] = []
                hour_groups[hour_bucket].append(candle)
            
            # Process each hour group
            candles_created = 0
            hourly_candles = []
            
            for hour_start, candles in hour_groups.items():
                if len(candles) == 4:  # Must have exactly 4 candles
                    # Verify they are the right times (:00, :15, :30, :45)
                    expected_minutes = [0, 15, 30, 45]
                    actual_minutes = [datetime.fromtimestamp(c['unixtime']).minute for c in candles]
                    actual_minutes.sort()
                    
                    if actual_minutes == expected_minutes:
                        # Create 1h candle from 4 complete 15min candles
                        sorted_candles = sorted(candles, key=lambda x: x['unixtime'])
                        
                        hourly_candle = {
                            'unixtime': hour_start,
                            'openprice': sorted_candles[0]['openprice'],
                            'closeprice': sorted_candles[-1]['closeprice'],
                            'highprice': max(c['highprice'] for c in sorted_candles),
                            'lowprice': min(c['lowprice'] for c in sorted_candles),
                            'volume': sum(c['volume'] for c in sorted_candles)
                        }
                        
                        hourly_candles.append(hourly_candle)
                        candles_created += 1
            
            # Bulk insert hourly candles
            if hourly_candles:
                self._bulkInsertCandles(tokenAddress, pairAddress, '1h', hourly_candles)
            
            return candles_created
            
        except Exception as e:
            logger.error(f"Error aggregating to hourly: {e}")
            return 0
    
    def _aggregateTo4Hourly(self, tokenAddress: str, pairAddress: str, candles_1h: List[Dict]) -> int:
        """Aggregate 1h candles to 4-hourly with proper validation"""
        try:
            # Group candles by 4-hour periods (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
            four_hour_groups = {}
            for candle in candles_1h:
                # Calculate 4-hour bucket (aligned to 00:00, 04:00, 08:00, etc.)
                four_hour_bucket = (candle['unixtime'] // 14400) * 14400
                if four_hour_bucket not in four_hour_groups:
                    four_hour_groups[four_hour_bucket] = []
                four_hour_groups[four_hour_bucket].append(candle)
            
            # Process each 4-hour group
            candles_created = 0
            four_hourly_candles = []
            
            for four_hour_start, candles in four_hour_groups.items():
                if len(candles) == 4:  # Must have exactly 4 hourly candles
                    # Verify they are the right times (00, 01, 02, 03 or 04, 05, 06, 07, etc.)
                    expected_hours = [(four_hour_start // 3600 + i) % 24 for i in range(4)]
                    actual_hours = [datetime.fromtimestamp(c['unixtime']).hour for c in candles]
                    actual_hours.sort()
                    expected_hours.sort()
                    
                    if actual_hours == expected_hours:
                        # Create 4h candle from 4 complete 1h candles
                        sorted_candles = sorted(candles, key=lambda x: x['unixtime'])
                        
                        four_hourly_candle = {
                            'unixtime': four_hour_start,
                            'openprice': sorted_candles[0]['openprice'],
                            'closeprice': sorted_candles[-1]['closeprice'],
                            'highprice': max(c['highprice'] for c in sorted_candles),
                            'lowprice': min(c['lowprice'] for c in sorted_candles),
                            'volume': sum(c['volume'] for c in sorted_candles)
                        }
                        
                        four_hourly_candles.append(four_hourly_candle)
                        candles_created += 1
            
            # Bulk insert 4-hourly candles
            if four_hourly_candles:
                self._bulkInsertCandles(tokenAddress, pairAddress, '4h', four_hourly_candles)
            
            return candles_created
            
        except Exception as e:
            logger.error(f"Error aggregating to 4-hourly: {e}")
            return 0
    
    def _getLatestCandleTime(self, tokenAddress: str, timeframe: str) -> int:
        """Get the latest candle time for a specific timeframe"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT MAX(unixtime) as latest_time
                    FROM ohlcvdetails
                    WHERE tokenaddress = %s AND timeframe = %s
                """), (tokenAddress, timeframe))
                
                result = cursor.fetchone()
                return result['latest_time'] if result and result['latest_time'] else None
                
        except Exception as e:
            logger.error(f"Error getting latest candle time: {e}")
            return None
    
    def _bulkInsertCandles(self, tokenAddress: str, pairAddress: str, timeframe: str, candles: List[Dict]):
        """Bulk insert aggregated candles"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                insert_data = []
                for candle in candles:
                    insert_data.append((
                        tokenAddress, pairAddress, timeframe,
                        candle['unixtime'], candle['openprice'], candle['highprice'],
                        candle['lowprice'], candle['closeprice'], candle['volume'],
                        None, None, None  # vwapvalue, ema21value, ema34value will be filled later
                    ))
                
                cursor.executemany(text("""
                    INSERT INTO ohlcvdetails 
                    (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
                     highprice, lowprice, closeprice, volume, vwapvalue, ema21value, ema34value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                """), insert_data)
                
                logger.info(f"Bulk inserted {len(candles)} {timeframe} candles for {tokenAddress}")
                
        except Exception as e:
            logger.error(f"Error bulk inserting candles: {e}")
            raise
    
    
    def _persistAggregationResultsInSingleTransaction(self, tokenAddress: str, pairAddress: str, 
                                                    aggregation_results: Dict):
        """Persist all aggregation results and create timeframe records in single transaction"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                hourly_candles = aggregation_results['hourly_candles']
                four_hourly_candles = aggregation_results['four_hourly_candles']
                latest_1h_time = aggregation_results['latest_1h_time']
                latest_4h_time = aggregation_results['latest_4h_time']
                
                # 1. Insert hourly candles if any
                if hourly_candles and latest_1h_time:
                    self._bulkInsertCandlesInTransaction(cursor, tokenAddress, pairAddress, '1h', hourly_candles)
                    
                    # Create 1h timeframe record
                    self._createTimeframeRecordInTransaction(cursor, tokenAddress, pairAddress, '1h')
                    
                    # Update 1h timeframe fetch status using pre-calculated latest time
                    self._updateTimeframeFetchStatusInTransaction(cursor, tokenAddress, pairAddress, '1h', latest_1h_time)
                    
                    logger.info(f"Inserted {len(hourly_candles)} hourly candles")
                
                # 2. Insert 4-hourly candles if any
                if four_hourly_candles and latest_4h_time:
                    self._bulkInsertCandlesInTransaction(cursor, tokenAddress, pairAddress, '4h', four_hourly_candles)
                    
                    # Create 4h timeframe record
                    self._createTimeframeRecordInTransaction(cursor, tokenAddress, pairAddress, '4h')
                    
                    # Update 4h timeframe fetch status using pre-calculated latest time
                    self._updateTimeframeFetchStatusInTransaction(cursor, tokenAddress, pairAddress, '4h', latest_4h_time)
                    
                    logger.info(f"Inserted {len(four_hourly_candles)} 4-hourly candles")
                
                logger.info("All aggregation operations completed in single transaction")
                
        except Exception as e:
            logger.error(f"Error in single transaction aggregation: {e}")
            raise
    
    def _bulkInsertCandlesInTransaction(self, cursor, tokenAddress: str, pairAddress: str, 
                                      timeframe: str, candles: List[Dict]):
        """Bulk insert candles within existing transaction"""
        insert_data = []
        for candle in candles:
            insert_data.append((
                tokenAddress, pairAddress, timeframe,
                candle['unixtime'], candle['openprice'], candle['highprice'],
                candle['lowprice'], candle['closeprice'], candle['volume'],
                None, None, None  # vwapvalue, ema21value, ema34value will be filled later
            ))
        
        cursor.executemany(text("""
            INSERT INTO ohlcvdetails 
            (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
             highprice, lowprice, closeprice, volume, vwapvalue, ema21value, ema34value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
        """), insert_data)
    
    def _createTimeframeRecordInTransaction(self, cursor, tokenAddress: str, pairAddress: str, timeframe: str):
        """Create timeframe record within existing transaction"""
        cursor.execute(text("""
            INSERT INTO timeframemetadata 
            (tokenaddress, pairaddress, timeframe, nextfetchat, lastfetchedat, isactive, createdat, lastupdatedat)
            VALUES (%s, %s, %s, NULL, NULL, %s, NOW(), NOW())
            ON CONFLICT (tokenaddress, pairaddress, timeframe) DO NOTHING
        """), (tokenAddress, pairAddress, timeframe, True))
    
    def _updateTimeframeFetchStatusInTransaction(self, cursor, tokenAddress: str, pairAddress: str, 
                                               timeframe: str, latest_unix_time: int):
        """Update timeframe fetch status within existing transaction"""
        timeframe_seconds = {'15m': 900, '1h': 3600, '4h': 14400}.get(timeframe, 900)
        next_fetch_time = latest_unix_time + timeframe_seconds
        
        cursor.execute(text("""
            UPDATE timeframemetadata 
            SET lastfetchedat = %s, nextfetchat = %s, lastupdatedat = NOW()
            WHERE tokenaddress = %s AND pairaddress = %s AND timeframe = %s
        """), (datetime.fromtimestamp(latest_unix_time), 
               datetime.fromtimestamp(next_fetch_time),
               tokenAddress, pairAddress, timeframe))
    
    # ===============================================================
    # ADDITIONAL HELPER METHODS
    # ===============================================================
    
    def _getAvailableTimeframes(self, tokenAddress: str, pairAddress: str) -> List[str]:
        """Get timeframes that have actual data for the token"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                cursor.execute(text("""
                    SELECT DISTINCT timeframe 
                    FROM timeframemetadata 
                    WHERE tokenaddress = %s AND pairaddress = %s AND isactive = true
                """), (tokenAddress, pairAddress))
                
                results = cursor.fetchall()
                return [row['timeframe'] for row in results]
                
        except Exception as e:
            logger.error(f"Error getting available timeframes: {e}")
            return ['15m']  # Default fallback
    
    def _getTodaysCandlesForAllTimeframes(self, tokenAddress: str, pairAddress: str, day_start: int) -> Dict[str, List[Dict]]:
        """Get today's candles for all available timeframes in single database call"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                day_end = day_start + 86400  # End of current day
                
                # Get all today's candles for all timeframes with JOIN
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
                    AND ohlcv.unixtime >= %s AND ohlcv.unixtime < %s
                    ORDER BY tm.timeframe, ohlcv.unixtime ASC
                """), (tokenAddress, pairAddress, day_start, day_end))
                
                results = cursor.fetchall()
                
                # Group candles by timeframe
                timeframe_candles = {}
                for row in results:
                    timeframe = row['timeframe']
                    if timeframe not in timeframe_candles:
                        timeframe_candles[timeframe] = []
                    
                    timeframe_candles[timeframe].append({
                        'unixtime': row['unixtime'],
                        'openprice': row['openprice'],
                        'highprice': row['highprice'],
                        'lowprice': row['lowprice'],
                        'closeprice': row['closeprice'],
                        'volume': row['volume']
                    })
                
                return timeframe_candles
                
        except Exception as e:
            logger.error(f"Error getting today's candles for all timeframes: {e}")
            return {}
    
    def _executeVWAPOperationsInSingleTransaction(self, tokenAddress: str, pairAddress: str, 
                                                vwap_operations: List[Dict]):
        """Execute all VWAP operations (candle updates + session creation) in single SQL call"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                # Prepare all VWAP updates in single batch
                vwap_update_data = []
                vwap_session_data = []
                
                for operation in vwap_operations:
                    timeframe = operation['timeframe']
                    vwap_result = operation['vwap_result']
                    next_candle_fetch = operation['next_candle_fetch']
                    day_start = operation['day_start']
                    
                    # Collect VWAP updates for batch execution
                    for vwap_data in vwap_result['vwap_values']:
                        vwap_update_data.append((
                            vwap_data['vwap'],
                            tokenAddress,
                            pairAddress,
                            timeframe,
                            vwap_data['unixtime']
                        ))
                    
                    # Collect VWAP session data for batch execution
                    day_end = day_start + 86400
                    vwap_session_data.append((
                        tokenAddress, pairAddress, timeframe, day_start, day_end,
                        vwap_result.get('cumulative_pv', 0), vwap_result.get('cumulative_volume', 0),
                        vwap_result.get('final_vwap', 0), vwap_result.get('last_candle_time', day_start),
                        next_candle_fetch
                    ))
                
                # Single batch update for all VWAP values across all timeframes
                if vwap_update_data:
                    cursor.executemany(text("""
                        UPDATE ohlcvdetails 
                        SET vwapvalue = %s
                        WHERE tokenaddress = %s AND pairaddress = %s AND timeframe = %s AND unixtime = %s
                    """), vwap_update_data)
                
                # Single batch insert/update for all VWAP sessions across all timeframes
                if vwap_session_data:
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
                    """), vwap_session_data)
                
                logger.info(f"Batch VWAP operations completed: {len(vwap_update_data)} candle updates, {len(vwap_session_data)} session updates")
                
        except Exception as e:
            logger.error(f"Error in batch VWAP operations: {e}")
            raise
    
    def _getAllCandlesForAllTimeframes(self, tokenAddress: str, pairAddress: str) -> Dict[str, List[Dict]]:
        """Get ALL candles for all available timeframes in single database call"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
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
                timeframe_candles = {}
                for row in results:
                    timeframe = row['timeframe']
                    if timeframe not in timeframe_candles:
                        timeframe_candles[timeframe] = []
                    
                    timeframe_candles[timeframe].append({
                        'unixtime': row['unixtime'],
                        'openprice': row['openprice'],
                        'highprice': row['highprice'],
                        'lowprice': row['lowprice'],
                        'closeprice': row['closeprice'],
                        'volume': row['volume']
                    })
                
                logger.info(f"Loaded {sum(len(candles) for candles in timeframe_candles.values())} total candles across {len(timeframe_candles)} timeframes")
                return timeframe_candles
                
        except Exception as e:
            logger.error(f"Error getting all candles for all timeframes: {e}")
            return {}
    
    def _processVWAPWithPreloadedCandles(self, tokenAddress: str, pairAddress: str, pairCreatedTime: int, 
                                       all_candles_data: Dict[str, List[Dict]]) -> Dict:
        """Process VWAP using pre-loaded candles (filter for today's data)"""
        try:
            current_time = int(time.time())
            day_start = (current_time // 86400) * 86400  # Start of current day UTC
            day_end = day_start + 86400
            
            # Filter today's candles from pre-loaded data
            today_candles_by_timeframe = {}
            for timeframe, candles in all_candles_data.items():
                today_candles = [c for c in candles if day_start <= c['unixtime'] < day_end]
                if today_candles:
                    today_candles_by_timeframe[timeframe] = today_candles
            
            if not today_candles_by_timeframe:
                logger.info(f"No today's candles found for VWAP processing for {tokenAddress}")
                return {'success': True}
            
            # Process all timeframes and prepare batch operations
            vwap_operations = []
            for timeframe, today_candles in today_candles_by_timeframe.items():
                logger.info(f"Processing VWAP for {tokenAddress} {timeframe}: {len(today_candles)} candles")
                
                # Calculate VWAP for all today's candles
                vwap_result = TradingActionUtil.calculateVWAPForCandles(today_candles)
                
                # Calculate next candle fetch time
                next_candle_fetch = TradingActionUtil.calculateNextCandleFetch(timeframe, today_candles[-1]['unixtime'])
                
                # Prepare VWAP operation data
                vwap_operations.append({
                    'timeframe': timeframe,
                    'today_candles': today_candles,
                    'vwap_result': vwap_result,
                    'next_candle_fetch': next_candle_fetch,
                    'day_start': day_start
                })
            
            # Execute all VWAP operations in single transaction
            self._executeVWAPOperationsInSingleTransaction(tokenAddress, pairAddress, vwap_operations)
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error processing VWAP with preloaded candles: {e}")
            return {'success': False, 'error': str(e)}
    
    def _processEMAWithPreloadedCandles(self, tokenAddress: str, pairAddress: str, pairCreatedTime: int,
                                      all_candles_data: Dict[str, List[Dict]]) -> Dict:
        """Process EMA using pre-loaded candles - batch all database operations"""
        try:
            current_time = int(time.time())
            
            # Collect all EMA operations in memory
            ema_state_operations = []
            ema_candle_updates = []
            
            # Process EMA for each available timeframe using pre-loaded data
            for timeframe, candles in all_candles_data.items():
                for ema_period in [21, 34]:
                    # Calculate when EMA becomes available: pairCreatedTime + (ema_period-1) * timeframe_seconds
                    timeframe_seconds = TradingActionUtil.getTimeframeSeconds(timeframe)
                    ema_available_time = pairCreatedTime + ((ema_period - 1) * timeframe_seconds)
                    
                    logger.info(f"Processing EMA{ema_period} state for {tokenAddress} {timeframe}: available at {ema_available_time}")
                    
                    # Prepare EMA state data for batch operation
                    ema_state_data = {
                        'tokenAddress': tokenAddress,
                        'pairAddress': pairAddress,
                        'timeframe': timeframe,
                        'emaKey': str(ema_period),
                        'pairCreatedTime': pairCreatedTime,
                        'emaAvailableTime': ema_available_time,
                        'emaValue': None,
                        'status': EMAStatus.NOT_AVAILABLE,
                        'lastUpdatedUnix': None,
                        'nextFetchTime': None
                    }
                    
                    # Check if we have enough data to calculate EMA (ema_available_time <= current_time)
                    if ema_available_time <= current_time:
                        logger.info(f"EMA{ema_period} data available for {tokenAddress} {timeframe}, calculating...")
                        
                        # Calculate EMA from pre-loaded candles (filter as needed)
                        ema_result = self._calculateEMAFromPreloadedCandles(tokenAddress, timeframe, ema_period, 
                                                                          ema_available_time, candles)
                        
                        if ema_result['success']:
                            # Calculate next fetch time
                            next_fetch_time = ema_result['last_candle_time'] + timeframe_seconds
                            
                            # Update EMA state data with calculated values
                            ema_state_data.update({
                                'emaValue': ema_result['final_ema'],
                                'status': EMAStatus.AVAILABLE,
                                'lastUpdatedUnix': ema_result['last_candle_time'],
                                'nextFetchTime': next_fetch_time
                            })
                            
                            # Collect EMA candle updates for batch operation
                            for ema_value in ema_result['ema_values']:
                                if ema_value['ema'] is not None:  # Skip None values
                                    ema_candle_updates.append({
                                        'tokenAddress': tokenAddress,
                                        'timeframe': timeframe,
                                        'ema_period': ema_period,
                                        'unixtime': ema_value['unixtime'],
                                        'ema_value': ema_value['ema']
                                    })
                            
                            logger.info(f"EMA{ema_period} calculated for {tokenAddress} {timeframe}: final value {ema_result['final_ema']}")
                        else:
                            logger.warning(f"Failed to calculate EMA{ema_period} for {tokenAddress} {timeframe}: {ema_result.get('error')}")
                    else:
                        logger.info(f"EMA{ema_period} not yet available for {tokenAddress} {timeframe} (needs {ema_period} candles)")
                    
                    # Add to batch operations
                    ema_state_operations.append(ema_state_data)
            
            # Execute all EMA operations in single transaction
            self._executeEMAOperationsInSingleTransaction(tokenAddress, pairAddress, ema_state_operations, ema_candle_updates)
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error processing EMA with preloaded candles: {e}")
            return {'success': False, 'error': str(e)}
    
    def _calculateEMAFromPreloadedCandles(self, tokenAddress: str, timeframe: str, 
                                        ema_period: int, ema_available_time: int, candles: List[Dict]) -> Dict:
        """Calculate EMA from pre-loaded candles using standard approach"""
        try:
            # Filter candles up to current time for EMA calculation
            if len(candles) < ema_period:
                return {'success': False, 'error': 'Not enough data for EMA calculation'}
            
            ema_values = []
            current_ema = Decimal('0')
            multiplier = Decimal('2') / (Decimal(str(ema_period)) + Decimal('1'))
            
            for i, candle in enumerate(candles):
                close_price = Decimal(str(candle['closeprice']))
                
                if i < ema_period - 1:
                    # First 19 candles for EMA21 are empty
                    ema_values.append({
                        'unixtime': candle['unixtime'],
                        'ema': None
                    })
                elif i == ema_period - 1:
                    # 20th candle = SMA of first 20 candles
                    sma_sum = sum(Decimal(str(candles[j]['closeprice'])) for j in range(ema_period))
                    current_ema = sma_sum / Decimal(str(ema_period))
                    ema_values.append({
                        'unixtime': candle['unixtime'],
                        'ema': current_ema
                    })
                else:
                    # EMA formula: EMA = (Close * Multiplier) + (Previous_EMA * (1 - Multiplier))
                    current_ema = (close_price * multiplier) + (current_ema * (Decimal('1') - multiplier))
                    ema_values.append({
                        'unixtime': candle['unixtime'],
                        'ema': current_ema
                    })
            
            return {
                'success': True,
                'ema_values': ema_values,
                'final_ema': current_ema,
                'last_candle_time': candles[-1]['unixtime']
            }
            
        except Exception as e:
            logger.error(f"Error calculating EMA from preloaded candles: {e}")
            return {'success': False, 'error': str(e)}
    
    def _executeEMAOperationsInSingleTransaction(self, tokenAddress: str, pairAddress: str,
                                               ema_state_operations: List[Dict], ema_candle_updates: List[Dict]):
        """Execute all EMA operations (state creation + candle updates) in single transaction"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                # Batch insert/update EMA states
                if ema_state_operations:
                    ema_state_data = []
                    for ema_state in ema_state_operations:
                        ema_state_data.append((
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
                    """), ema_state_data)
                    
                    logger.info(f"Batch inserted/updated {len(ema_state_data)} EMA states")
                
                # Batch update ALL EMA values in single SQL operation using CASE statements
                if ema_candle_updates:
                    # Prepare data for single update with CASE statements
                    update_data = []
                    for update in ema_candle_updates:
                        update_data.append((
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
                    """), [(val, period, val, period, token, tf, unix_time) 
                           for val, period, token, tf, unix_time in update_data])
                    
                    logger.info(f"Batch updated {len(update_data)} EMA candle values in single operation")
                
                logger.info(f"All EMA operations completed in 2 SQL calls: {len(ema_state_operations)} states, {len(ema_candle_updates)} candle updates")
                
        except Exception as e:
            logger.error(f"Error in batch EMA operations: {e}")
            raise
    
    def _processEMAForOldTokenWithPreloadedCandles(self, tokenAddress: str, pairAddress: str, pairCreatedTime: int,
                                                 ema21Values: Dict, ema34Values: Dict, referenceUnixTime: int,
                                                 all_candles_data: Dict[str, List[Dict]]) -> Dict:
        """Process EMA for old token using pre-loaded candles with user-provided values - batch all operations"""
        try:
            # ====================================================================
            # DATA COLLECTION PHASE: Build in-memory collections for batch operations
            # - ema_state_operations: List of EMA state records to insert/update in emastates table
            # - ema_candle_updates: List of candle updates to apply EMA values in ohlcvdetails table
            # ====================================================================
            ema_state_operations = []  # Will contain: tokenAddress, pairAddress, timeframe, emaKey, emaValue, status, timestamps
            ema_candle_updates = []    # Will contain: tokenAddress, timeframe, ema_period, unixtime, ema_value
            
            # Process each timeframe that has candle data available
            for timeframe, candles in all_candles_data.items():
                self._processEMAForSingleTimeframe(
                    timeframe, candles, tokenAddress, pairAddress, pairCreatedTime, 
                    referenceUnixTime, ema21Values, ema34Values,
                    ema_state_operations, ema_candle_updates
                )
            
            # ====================================================================
            # DATABASE PERSISTENCE PHASE: Execute all collected operations in single transaction
            # - Batch 1: INSERT/UPDATE all EMA states in emastates table (1 SQL call)
            # - Batch 2: UPDATE all EMA values in ohlcvdetails table using CASE statements (1 SQL call)
            # Total: 2 SQL calls for entire EMA processing across all timeframes
            # ====================================================================
            logger.info(f"Executing batch EMA operations: {len(ema_state_operations)} state records, {len(ema_candle_updates)} candle updates")
            self._executeEMAOperationsInSingleTransaction(tokenAddress, pairAddress, ema_state_operations, ema_candle_updates)
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error processing EMA for old token with preloaded candles: {e}")
            return {'success': False, 'error': str(e)}
    
    def _processEMAForSingleTimeframe(self, timeframe: str, candles: List[Dict], tokenAddress: str, 
                                    pairAddress: str, pairCreatedTime: int, referenceUnixTime: int,
                                    ema21Values: Dict, ema34Values: Dict, 
                                    ema_state_operations: List[Dict], ema_candle_updates: List[Dict]):
        """
        Process EMA for a single timeframe - extracts nested loop logic for better readability
        
        DATA FLOW:
        1. INPUT: timeframe candles + user EMA values for EMA21 and EMA34
        2. PROCESSING: Create EMA state records and candle update records for valid user values
        3. OUTPUT: Appends to ema_state_operations and ema_candle_updates lists (passed by reference)
        
        NEXT STEPS: Collected data will be batch-persisted in _executeEMAOperationsInSingleTransaction
        """
        # Define EMA periods to process - keeps the logic flexible for future periods
        ema_configurations = [
            (21, ema21Values, 'EMA21'),
            (34, ema34Values, 'EMA34')
        ]
        
        # Process each EMA period for this timeframe
        for ema_period, ema_values_dict, ema_name in ema_configurations:
            # Skip if user didn't provide value for this timeframe and EMA period
            if timeframe not in ema_values_dict:
                logger.debug(f"Skipping {ema_name} for {timeframe} - no user value provided")
                continue
                
            # Convert user-provided EMA value to Decimal for precision
            user_ema_value = Decimal(str(ema_values_dict[timeframe]))
            
            # Calculate timing for next EMA fetch
            timeframe_seconds = TradingActionUtil.getTimeframeSeconds(timeframe)
            next_fetch_time = referenceUnixTime + timeframe_seconds
            
            logger.info(f"Setting {ema_name} for {tokenAddress} {timeframe}: value={user_ema_value} at timestamp {referenceUnixTime}")
            
            # ====================================================================
            # PREPARE EMA STATE RECORD: Will be inserted/updated in emastates table
            # ====================================================================
            ema_state_data = TradingActionUtil.prepareEMAStateData(
                tokenAddress, pairAddress, timeframe, ema_period, user_ema_value,
                pairCreatedTime, referenceUnixTime, EMAStatus.AVAILABLE
            )
            ema_state_operations.append(ema_state_data)
            
            # ====================================================================
            # PREPARE CANDLE UPDATE RECORD: Will update EMA value in ohlcvdetails table
            # ====================================================================
            reference_candle = next((c for c in candles if c['unixtime'] == referenceUnixTime), None)
            if reference_candle:
                ema_candle_update = TradingActionUtil.prepareEMACandleUpdate(
                    tokenAddress, timeframe, ema_period, referenceUnixTime, user_ema_value
                )
                ema_candle_updates.append(ema_candle_update)
                logger.info(f"Prepared {ema_name} candle update for {tokenAddress} {timeframe} at {referenceUnixTime}")
            else:
                logger.warning(f"Reference candle at timestamp {referenceUnixTime} not found for {tokenAddress} {timeframe} - skipping {ema_name} candle update")
    
    
    def _createVWAPSession(self, tokenAddress: str, pairAddress: str, timeframe: str, 
                          day_start: int, vwap_result: Dict, next_candle_fetch: int):
        """Create or update VWAP session record"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                day_end = day_start + 86400  # End of day
                
                cursor.execute(text("""
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
                """), (
                    tokenAddress, pairAddress, timeframe, day_start, day_end,
                    vwap_result.get('cumulative_pv', 0), vwap_result.get('cumulative_volume', 0),
                    vwap_result.get('final_vwap', 0), vwap_result.get('last_candle_time', day_start),
                    next_candle_fetch
                ))
                
        except Exception as e:
            logger.error(f"Error creating VWAP session: {e}")
    
    def _updateEMAInOHLCVForTimestamp(self, tokenAddress: str, timeframe: str, 
                                    ema_period: int, timestamp: int, ema_value: Decimal):
        """Update EMA value in OHLCV table for specific timestamp"""
        try:
            with self.trading_handler.conn_manager.transaction() as cursor:
                column_name = f"ema{ema_period}value"
                
                cursor.execute(text(f"""
                    UPDATE ohlcvdetails 
                    SET {column_name} = %s
                    WHERE tokenaddress = %s AND timeframe = %s AND unixtime = %s
                """), (ema_value, tokenAddress, timeframe, timestamp))
                
                if cursor.rowcount == 0:
                    logger.warning(f"No OHLCV record found for {tokenAddress} {timeframe} at timestamp {timestamp}")
                    
        except Exception as e:
            logger.error(f"Error updating EMA in OHLCV for timestamp: {e}")
    
    def _fetchCandleDataWithPagination(self, tokenAddress: str, pairAddress: str, 
                                     from_time: int, to_time: int) -> Dict:
        """Fetch candle data with pagination, credit management, and batch DB operations"""
        logger.info(f"Fetching candle data for {tokenAddress} from {from_time} to {to_time}")
        
        fetch_state = self._initializeFetchState(from_time)
        
        try:
            # Collect all candle data from API calls
            self._collectAllCandleData(pairAddress, to_time, fetch_state)
            
            # Perform single database operation
            candles_inserted = self._persistCandleDataToDatabase(
                tokenAddress, pairAddress, fetch_state['all_candle_data'], fetch_state['latest_unix_time']
            )
            
            logger.info(f"Completed: {candles_inserted} candles inserted, {fetch_state['total_credits_used']} credits used")
            
            return {
                'success': True,
                'candlesInserted': candles_inserted,
                'creditsUsed': fetch_state['total_credits_used'],
                'latestUnixTime': fetch_state['latest_unix_time']
            }
            
        except Exception as e:
            logger.error(f"Error in candle data pagination: {e}")
            self._handleFetchError(fetch_state)
            return {'success': False, 'error': str(e)}
    
    def _initializeFetchState(self, from_time: int) -> Dict:
        """Initialize state for candle data fetching"""
        return {
            'all_candle_data': [],
            'total_credits_used': 0,
            'latest_unix_time': from_time,
            'current_from_time': from_time,
            'api_key_data': None
        }
    
    def _collectAllCandleData(self, pairAddress: str, to_time: int, fetch_state: Dict):
        """Collect all candle data from API with pagination - NO database operations"""
        # Get initial API key once
        if not self._getInitialApiKey(fetch_state):
            raise Exception('No valid BirdEye API key available')
        
        while fetch_state['current_from_time'] < to_time:
            # Fetch data from API (no DB operations)
            ohlcv_data = self._fetchSinglePageData(pairAddress, fetch_state, to_time)
            if not ohlcv_data:
                break
            
            # Process the response (no DB operations)
            self._processFetchedData(ohlcv_data, fetch_state)
            
            # Check if more data is needed (no DB operations)
            if not self._shouldContinueFetching(ohlcv_data, fetch_state, to_time):
                break
        
        # Deduct ALL credits in single DB operation after collection complete
        self._deductAllCreditsInBatch(fetch_state)
    
    def _getInitialApiKey(self, fetch_state: Dict) -> bool:
        """Get initial API key once at the beginning"""
        fetch_state['api_key_data'] = self.db.credentials.getNextValidApiKey(
            serviceName=self.service.service_name,
            requiredCredits=self.creditsPerCall
        )
        return fetch_state['api_key_data'] is not None
    
    def _fetchSinglePageData(self, pairAddress: str, fetch_state: Dict, to_time: int) -> List[Dict]:
        """Fetch a single page of data from the API"""
        current_from = fetch_state['current_from_time']
        logger.info(f"Calling BirdEye API from {current_from} to {to_time}")
        
        ohlcv_data = self._callBirdEyeAPI(pairAddress, current_from, to_time, fetch_state['api_key_data']['apikey'])
        
        if not ohlcv_data:
            logger.warning(f"No data returned from API for range {current_from} to {to_time}")
        
        return ohlcv_data
    
    def _processFetchedData(self, ohlcv_data: List[Dict], fetch_state: Dict):
        """Process fetched data and update state"""
        fetch_state['all_candle_data'].extend(ohlcv_data)
        fetch_state['total_credits_used'] += self.creditsPerCall
        
        # Update latest unix time
        if ohlcv_data:
            api_latest_time = max([candle['unixtime'] for candle in ohlcv_data])
            fetch_state['latest_unix_time'] = max(fetch_state['latest_unix_time'], api_latest_time)
        
        logger.info(f"Retrieved {len(ohlcv_data)} candles, latest time: {fetch_state['latest_unix_time']}")
    
    def _shouldContinueFetching(self, ohlcv_data: List[Dict], fetch_state: Dict, to_time: int) -> bool:
        """Determine if we should continue fetching more data - NO database operations"""
        record_count = len(ohlcv_data)
        latest_time = fetch_state['latest_unix_time']
        
        # Continue if we got max records and haven't reached end time
        if record_count == 1000 and latest_time < to_time:
            fetch_state['current_from_time'] = latest_time + 1
            
            # Track credits needed (no DB operation yet)
            self._trackCreditUsage(fetch_state)
            
            logger.info(f"More data available, continuing from {fetch_state['current_from_time']}")
            return True
        else:
            logger.info(f"Data fetch complete - retrieved {record_count} records")
            return False
    
    def _trackCreditUsage(self, fetch_state: Dict):
        """Track credit usage without database operations"""
        # Just track the usage - will deduct all at once later
        remaining_credits = fetch_state['api_key_data'].get('remaining_credits', 0) - fetch_state['total_credits_used']
        
        # If we need more credits, we'll handle it in the batch operation later
        if remaining_credits < self.creditsPerCall:
            logger.info(f"Credits running low, will need new API key for future calls")
    
    def _deductAllCreditsInBatch(self, fetch_state: Dict):
        """Deduct ALL credits used in a single database operation"""
        if fetch_state['api_key_data'] and fetch_state['total_credits_used'] > 0:
            logger.info(f"Deducting {fetch_state['total_credits_used']} credits in batch operation")
            self.db.credentials.deductAPICredits(fetch_state['api_key_data']['id'], fetch_state['total_credits_used'])
    
    def _persistCandleDataToDatabase(self, tokenAddress: str, pairAddress: str, 
                                   all_candle_data: List[Dict], latest_unix_time: int) -> int:
        """Persist all collected candle data to database in single transaction"""
        if not all_candle_data:
            return 0
        
        logger.info(f"Performing batch insert of {len(all_candle_data)} candles with timeframe update")
        
        candles_inserted = self._storeCandlesAndUpdateTimeframeInSingleTransaction(
            tokenAddress, pairAddress, all_candle_data, latest_unix_time
        )
        
        logger.info(f"Batch operation completed: {candles_inserted} candles inserted")
        return candles_inserted
    
    def _handleFetchError(self, fetch_state: Dict):
        """Handle errors by ensuring credits are deducted in batch"""
        if fetch_state['api_key_data'] and fetch_state['total_credits_used'] > 0:
            try:
                logger.info(f"Error occurred, deducting {fetch_state['total_credits_used']} credits")
                self.db.credentials.deductAPICredits(fetch_state['api_key_data']['id'], fetch_state['total_credits_used'])
            except Exception:
                pass  # Ignore errors in error handling
    
    def _storeCandlesAndUpdateTimeframeInSingleTransaction(self, tokenAddress: str, pairAddress: str, 
                                                         all_candle_data: List[Dict], latest_unix_time: int) -> int:
        """Store all candles and update timeframe status in a single database transaction"""
        try:
            if not all_candle_data:
                return 0
            
            with self.trading_handler.conn_manager.transaction() as cursor:
                # 1. Batch insert all candles
                insert_data = []
                for candle in all_candle_data:
                    insert_data.append((
                        tokenAddress,
                        pairAddress,
                        '15m',  # All fetched data is 15m timeframe
                        candle['unixtime'],
                        candle['openprice'],
                        candle['highprice'],
                        candle['lowprice'],
                        candle['closeprice'],
                        candle['volume'],
                        None,  # vwapvalue - will be calculated later
                        None,  # ema21value - will be calculated later
                        None   # ema34value - will be calculated later
                    ))
                
                cursor.executemany(text("""
                    INSERT INTO ohlcvdetails 
                    (tokenaddress, pairaddress, timeframe, unixtime, openprice, 
                     highprice, lowprice, closeprice, volume, vwapvalue, ema21value, ema34value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tokenaddress, timeframe, unixtime) DO NOTHING
                """), insert_data)
                
                candles_inserted = cursor.rowcount
                
                # 2. Update timeframe fetch status in same transaction
                timeframe_seconds = 900  # 15m = 900 seconds
                next_fetch_time = latest_unix_time + timeframe_seconds
                
                cursor.execute(text("""
                    UPDATE timeframemetadata 
                    SET lastfetchedat = %s, nextfetchat = %s, lastupdatedat = NOW()
                    WHERE tokenaddress = %s AND pairaddress = %s AND timeframe = %s
                """), (datetime.fromtimestamp(latest_unix_time), 
                       datetime.fromtimestamp(next_fetch_time),
                       tokenAddress, pairAddress, '15m'))
                
                logger.info(f"Single transaction completed: {candles_inserted} candles inserted, timeframe updated")
                
                return candles_inserted
                
        except Exception as e:
            logger.error(f"Error in single transaction operation: {e}")
            return 0