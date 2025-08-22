


from config.Config import get_config
from typing import Optional, Dict, Any, List, Union
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler, AdditionSource
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
import pytz
import uuid
import json


logger = get_logger(__name__)


class TradingAction:
   """Handles complete crypto trading system workflow"""
  
   def __init__(self, db: PortfolioDB):
       """
       Initialize action with required parameters
       Args:
           db: Database handler for persistence
       """
       self.db = db
       self.trading_handler = TradingHandler(db.conn_manager)
      
       # BirdEye API configuration using ServiceCredentials pattern
       self.service = ServiceCredentials.BIRDEYE
       self.baseUrl = self.service.metadata['base_url']
       self.creditsPerCall = self.service.metadata.get('credits_per_call', 40)


   def queueBackfillJob(self, token_address: str, pair_address: str, symbol: str,
                       name: str, hours: int = 168) -> Dict[str, Any]:
       """
       Queue a backfill job for historical data
      
       Args:
           token_address: Token contract address
           pair_address: DEX pair address 
           symbol: Trading symbol
           name: Token name
           hours: Hours of historical data to fetch
          
       Returns:
           Dict: Job result with success status and job ID
       """
       try:
           # Generate unique job ID
           job_id = f"backfill_{token_address[:8]}_{int(time.time())}"
          
           # In a production system, you would queue this with Celery or similar
           # For now, we'll execute it immediately
           logger.info(f"Starting immediate backfill for {symbol} ({token_address})")
          
           backfill_result = self.executeBackfill(
               token_address=token_address,
               pair_address=pair_address,
               symbol=symbol,
               name=name,
               hours=hours
           )
          
           if backfill_result.success:
               logger.info(f"Backfill completed successfully for {symbol}: {backfill_result.candlesinserted} candles inserted")
               return {
                   'success': True,
                   'jobId': job_id,
                   'status': 'COMPLETED',
                   'candlesInserted': backfill_result.candlesinserted,
                   'creditsUsed': backfill_result.apicreditsused
               }
           else:
               logger.error(f"Backfill failed for {symbol}: {backfill_result.errordetails}")
               return {
                   'success': False,
                   'jobId': job_id,
                   'status': 'FAILED',
                   'error': backfill_result.errordetails
               }
              
       except Exception as e:
           logger.error(f"Error queuing backfill job for {symbol}: {e}")
           return {
               'success': False,
               'error': f'Failed to queue backfill job: {str(e)}'
           }


   def executeBackfill(self, token_address: str, pair_address: str, symbol: str,
                      name: str, hours: int) -> BackfillResult:
       """
       Execute backfill operation for a token
      
       Args:
           token_address: Token contract address
           pair_address: DEX pair address
           symbol: Trading symbol 
           name: Token name
           hours: Hours of historical data to fetch
          
       Returns:
           BackfillResult: Result of backfill operation
       """
       try:
           # Calculate time range
           now = int(time.time())
           start_time = now - (hours * 3600)  # hours ago
          
           logger.info(f"Backfilling {hours} hours of data for {symbol} from {start_time} to {now}")
          
           total_candles_inserted = 0
           total_credits_used = 0
          
           # Fetch data in chunks (1000 candles per API call)
           current_time = start_time
           chunk_size = 3600 * 250  # 250 hours per chunk (1000 15m candles)
          
           while current_time < now:
               chunk_end = min(current_time + chunk_size, now)
              
               # Fetch OHLCV data from BirdEye
               ohlcv_data = self.fetchBirdEyeOHLCV(
                   pair_address=pair_address,
                   timeframe="15m",
                   time_from=current_time,
                   time_to=chunk_end
               )
              
               if not ohlcv_data:
                   logger.warning(f"No data received for {symbol} in chunk {current_time} to {chunk_end}")
                   current_time = chunk_end
                   continue
              
               # Convert to OHLCVCandle objects and insert
               candles = []
               for item in ohlcv_data:
                   candle = OHLCVCandle(
                       tokenaddress=token_address,
                       pairaddress=pair_address,
                       timeframe="15m",
                       unixtime=item.unixTime,
                       openprice=Decimal(str(item.o)),
                       highprice=Decimal(str(item.h)),
                       lowprice=Decimal(str(item.l)),
                       closeprice=Decimal(str(item.c)),
                       volume=Decimal(str(item.v)),
                       datasource="api"
                   )
                   candles.append(candle)
              
               # Batch insert candles
               if candles:
                   # Convert to dict format for batch insert
                   candle_dicts = []
                   for candle in candles:
                       # Get timeframe metadata ID
                       tokens_due = self.trading_handler.getTokensDueForFetch(limit=1000)
                       timeframe_id = None
                       for token_due in tokens_due:
                           if (token_due['tokenaddress'] == token_address and
                               token_due['timeframe'] == '15m'):
                               timeframe_id = token_due['id']
                               break
                      
                       if timeframe_id:
                           candle_dict = {
                               'timeframeid': timeframe_id,
                               'tokenaddress': candle.tokenaddress,
                               'pairaddress': candle.pairaddress,
                               'timeframe': candle.timeframe,
                               'unixtime': candle.unixtime,
                               'openprice': candle.openprice,
                               'highprice': candle.highprice,
                               'lowprice': candle.lowprice,
                               'closeprice': candle.closeprice,
                               'volume': candle.volume,
                               'datasource': candle.datasource
                           }
                           candle_dicts.append(candle_dict)
                  
                   inserted_count = self.trading_handler.batchInsertCandles(candle_dicts)
                   total_candles_inserted += inserted_count
                  
                   logger.info(f"Inserted {inserted_count} candles for {symbol} in chunk {current_time} to {chunk_end}")
              
               total_credits_used += 40  # Each API call costs 40 credits
               current_time = chunk_end
              
               # Rate limiting - wait 1 second between API calls
               time.sleep(1)
          
           logger.info(f"Backfill completed for {symbol}: {total_candles_inserted} candles, {total_credits_used} credits")
          
           return BackfillResult(
               success=True,
               candlesinserted=total_candles_inserted,
               totalcandlesprocessed=total_candles_inserted,
               apicreditsused=total_credits_used,
               timecomplete=datetime.now(pytz.timezone('Asia/Kolkata'))
           )
          
       except Exception as e:
           logger.error(f"Error in backfill execution for {symbol}: {e}")
           return BackfillResult(
               success=False,
               errordetails=f"Backfill execution failed: {str(e)}"
           )


   def fetchBirdEyeOHLCV(self, pair_address: str, timeframe: str = "15m",
                        time_from: int = None, time_to: int = None) -> List[BirdEyeOHLCVItem]:
       """
       Fetch OHLCV data from BirdEye API using credentials system
      
       Args:
           pair_address: DEX pair address
           timeframe: Data timeframe ("15m", "1h", "4h")
           time_from: Start timestamp (Unix)
           time_to: End timestamp (Unix)
          
       Returns:
           List[BirdEyeOHLCVItem]: OHLCV data items
       """
       try:
           # Get valid API key using credentials system
           apiKeyData = self.db.credentials.getNextValidApiKey(
               serviceName=self.service.service_name,
               requiredCredits=self.creditsPerCall
           )
           if not apiKeyData:
               logger.error("No valid BirdEye API key available")
               return []
          
           # Default time range (last 24 hours)
           if not time_from:
               time_from = int(time.time()) - 86400
           if not time_to:
               time_to = int(time.time())
          
           url = f"{self.baseUrl}/defi/ohlcv/pair"
          
           headers = {
               "X-API-KEY": apiKeyData['apikey'],
               "accept": "application/json",
               "x-chain": "solana"
           }
          
           params = {
               "address": pair_address,
               "type": timeframe,
               "time_from": time_from,
               "time_to": time_to
           }
          
           logger.debug(f"BirdEye API request: {url} with params: {params}")
          
           response = requests.get(url, headers=headers, params=params, timeout=30)
           response.raise_for_status()
          
           data = response.json()
          
           # Deduct credits after successful API call
           self.db.credentials.deductAPIKeyCredits(apiKeyData['id'], self.creditsPerCall)
          
           if not data.get("success", False):
               logger.error(f"BirdEye API returned error: {data}")
               return []
          
           items = data.get("data", {}).get("items", [])
          
           # Convert to BirdEyeOHLCVItem objects
           ohlcv_items = []
           for item in items:
               ohlcv_item = BirdEyeOHLCVItem(
                   address=item.get("address", pair_address),
                   c=item.get("c", 0),
                   h=item.get("h", 0),
                   l=item.get("l", 0),
                   o=item.get("o", 0),
                   type=item.get("type", timeframe),
                   unixTime=item.get("unixTime", 0),
                   v=item.get("v", 0)
               )
               ohlcv_items.append(ohlcv_item)
          
           logger.info(f"Fetched {len(ohlcv_items)} OHLCV items from BirdEye for {pair_address}")
           return ohlcv_items
          
       except requests.exceptions.RequestException as e:
           logger.error(f"BirdEye API request failed: {e}")
           return []
       except Exception as e:
           logger.error(f"Error fetching BirdEye OHLCV data: {e}")
           return []


   def processSingleToken(self, token_address: str) -> bool:
       """
       Process a single token - fetch latest data and update indicators
      
       Args:
           token_address: Token contract address
          
       Returns:
           bool: Success status
       """
       try:
           # Get token metadata
           active_tokens = self.trading_handler.getActiveTokens()
           token_info = None
          
           for token in active_tokens:
               if token['tokenaddress'] == token_address:
                   token_info = token
                   break
          
           if not token_info:
               logger.error(f"Token {token_address} not found in active tokens")
               return False
          
           # Get latest candle to determine next fetch time
           latest_candles = self.trading_handler.getLatestCandles(token_address, "15m", limit=1)
          
           # Calculate time range for fetching (last 2 hours to ensure we don't miss any)
           now = int(time.time())
           time_from = now - 7200  # 2 hours ago
          
           if latest_candles:
               # Start from the last candle time
               time_from = max(time_from, latest_candles[0]['unixtime'])
          
           # Fetch new data
           ohlcv_data = self.fetchBirdEyeOHLCV(
               pair_address=token_info['pairaddress'],
               timeframe="15m",
               time_from=time_from,
               time_to=now
           )
          
           if not ohlcv_data:
               logger.warning(f"No new data for {token_info['symbol']}")
               # Update fetch status as failed
               self.trading_handler.updateFetchStatus(
                   tokenAddress=token_address,
                   timeframe="15m",
                   success=False
               )
               return False
          
           # Process each new candle
           new_candles_count = 0
           for item in ohlcv_data:
               # Skip if we already have this candle
               if latest_candles and item.unixTime <= latest_candles[0]['unixtime']:
                   continue
              
               # Insert new candle
               candle_id = self.trading_handler.insertOHLCVCandle(
                   tokenAddress=token_address,
                   pairAddress=token_info['pairaddress'],
                   timeframe="15m",
                   unixTime=item.unixTime,
                   openPrice=Decimal(str(item.o)),
                   highPrice=Decimal(str(item.h)),
                   lowPrice=Decimal(str(item.l)),
                   closePrice=Decimal(str(item.c)),
                   volume=Decimal(str(item.v))
               )
              
               if candle_id:
                   new_candles_count += 1
                  
                   # TODO: Calculate indicators (EMA, VWAP) for this candle
                   # TODO: Check for aggregation triggers (1h, 4h)
                   # TODO: Check for alert conditions
          
           # Update fetch status as successful
           next_fetch_time = datetime.now(pytz.timezone('Asia/Kolkata')) + timedelta(minutes=15)
           self.trading_handler.updateFetchStatus(
               tokenAddress=token_address,
               timeframe="15m",
               success=True,
               nextFetchTime=next_fetch_time
           )
          
           logger.info(f"Processed {new_candles_count} new candles for {token_info['symbol']}")
           return True
          
       except Exception as e:
           logger.error(f"Error processing token {token_address}: {e}")
          
           # Update fetch status as failed
           self.trading_handler.updateFetchStatus(
               tokenAddress=token_address,
               timeframe="15m",
               success=False
           )
           return False


   def processScheduledTokensBatch(self) -> Dict[str, Any]:
       """
       High-performance batch processing of scheduled tokens
       Uses optimized batch queries and bulk operations
       """
       try:
           # Get tokens due for fetching with optimized query
           tokens_due = self.trading_handler.getTokensDueForFetch(limit=50)
          
           if not tokens_due:
               logger.info("No tokens due for fetching")
               return {
                   'success': True,
                   'tokensProcessed': 0,
                   'tokensSuccessful': 0,
                   'tokensFailed': 0
               }
          
           logger.info(f"Batch processing {len(tokens_due)} tokens due for fetching")
          
           # Extract token addresses for batch operations 
           token_addresses = [token['tokenaddress'] for token in tokens_due]
          
           # Batch fetch indicator states and latest candles
           indicator_states = self.trading_handler.getBatchIndicatorStates(token_addresses, '15m')
           latest_candles = self.trading_handler.getBatchLatestCandles(token_addresses, '15m')
          
           # Process tokens and collect results
           all_new_candles = []
           all_indicator_updates = []
           token_results = []
          
           for token_metadata in tokens_due:
               try:
                   token_address = token_metadata['tokenaddress']
                  
                   # Process token with pre-fetched data
                   new_candles, indicator_updates, success = self.processSingleTokenBatch(
                       token_metadata,
                       indicator_states.get(token_address, {}),
                       latest_candles.get(token_address)
                   )
                  
                   # Collect data for bulk operations
                   all_new_candles.extend(new_candles)
                   all_indicator_updates.extend(indicator_updates)
                  
                   token_results.append({
                       'token_address': token_address,
                       'success': success,
                       'current_failures': token_metadata['consecutivefailures']
                   })
                  
               except Exception as e:
                   logger.error(f"Error processing token {token_metadata.get('tokenaddress', 'unknown')}: {e}")
                   token_results.append({
                       'token_address': token_metadata.get('tokenaddress', 'unknown'),
                       'success': False,
                       'current_failures': token_metadata.get('consecutivefailures', 0)
                   })
          
           # Execute bulk operations
           inserted_count = 0
           updated_count = 0
          
           if all_new_candles:
               inserted_count = self.trading_handler.batchInsertCandles(all_new_candles)
               logger.info(f"Bulk inserted {inserted_count} candles")
          
           if all_indicator_updates:
               updated_count = self.trading_handler.bulkUpdateIndicators(all_indicator_updates)
               logger.info(f"Bulk updated {updated_count} indicator values")
          
           # Batch update fetch status
           status_updated = self.trading_handler.updateFetchStatusBatch(token_results)
          
           # Calculate results
           tokens_successful = sum(1 for result in token_results if result['success'])
           tokens_failed = len(token_results) - tokens_successful
          
           logger.info(f"Batch processing completed: {tokens_successful} successful, {tokens_failed} failed")
          
           return {
               'success': True,
               'tokensProcessed': len(token_results),
               'tokensSuccessful': tokens_successful,
               'tokensFailed': tokens_failed,
               'candlesInserted': inserted_count,
               'indicatorsUpdated': updated_count,
               'statusUpdated': status_updated,
               'summary': f"Batch processed {len(token_results)} tokens: {tokens_successful} successful, {tokens_failed} failed"
           }
          
       except Exception as e:
           logger.error(f"Error in batch scheduled token processing: {e}")
           return {
               'success': False,
               'error': f"Batch processing failed: {str(e)}"
           }


   def processSingleTokenBatch(self, token_metadata: Dict, indicator_states: Dict, latest_candle: Optional[Dict]) -> Tuple[List[Dict], List[Dict], bool]:
       """
       Process single token optimized for batch operations
       Returns data for bulk operations instead of executing immediately
       """
       try:
           token_address = token_metadata['tokenaddress']
           pair_address = token_metadata['pairaddress']
          
           # Calculate time range for fetching
           now = int(time.time())
           time_from = now - 7200  # 2 hours ago
          
           if latest_candle:
               time_from = max(time_from, latest_candle['unixtime'])
          
           # Fetch new data from BirdEye
           ohlcv_data = self.fetchBirdEyeOHLCV(
               pair_address=pair_address,
               timeframe="15m",
               time_from=time_from,
               time_to=now
           )
          
           if not ohlcv_data:
               return [], [], False
          
           # Prepare data for bulk operations
           new_candles = []
           indicator_updates = []
          
           for item in ohlcv_data:
               # Skip if we already have this candle
               if latest_candle and item.unixTime <= latest_candle['unixtime']:
                   continue
              
               # Get timeframe metadata ID
               timeframe_id = token_metadata.get('id')  # From tokens due query
              
               # Prepare candle data for bulk insert
               candle_data = {
                   'timeframeid': timeframe_id,
                   'tokenaddress': token_address,
                   'pairaddress': pair_address,
                   'timeframe': '15m',
                   'unixtime': item.unixTime,
                   'openprice': item.o,
                   'highprice': item.h,
                   'lowprice': item.l,
                   'closeprice': item.c,
                   'volume': item.v,
                   'datasource': 'api'
               }
              
               new_candles.append(candle_data)
              
               # Calculate indicators for this candle
               indicator_update = self.calculateIndicatorsBatch(
                   token_address, candle_data, indicator_states
               )
              
               if indicator_update:
                   # Note: candle_id will need to be resolved after bulk insert
                   indicator_update['token_address'] = token_address
                   indicator_update['unixtime'] = item.unixTime
                   indicator_updates.append(indicator_update)
          
           success = len(new_candles) > 0
           return new_candles, indicator_updates, success
          
       except Exception as e:
           logger.error(f"Error in batch single token processing: {e}")
           return [], [], False


   def calculateIndicatorsBatch(self, token_address: str, candle_data: Dict, indicator_states: Dict) -> Optional[Dict]:
       """
       Calculate indicators efficiently using flexible indicator framework
       Supports any EMA period combinations and timeframes
       """
       try:
           timeframe = candle_data.get('timeframe', '15m')
          
           # Use flexible indicator framework for dynamic calculation
           calculated_values = self.trading_handler.calculateDynamicIndicators(
               tokenAddress=token_address,
               timeframe=timeframe,
               latestCandle=candle_data
           )
          
           if not calculated_values:
               logger.debug(f"No indicators calculated for {token_address} {timeframe}")
               return None
          
           # Convert to expected format for bulk update
           update_data = {}
           for indicator_key, value in calculated_values.items():
               if indicator_key.startswith('ema_21'):
                   update_data['ema21'] = value
               elif indicator_key.startswith('ema_34'):
                   update_data['ema34'] = value
               elif indicator_key == 'vwap':
                   update_data['vwap'] = value
               # Support for any other configured indicators
               else:
                   update_data[indicator_key] = value
          
           return update_data if update_data else None
          
       except Exception as e:
           logger.error(f"Error calculating indicators for batch: {e}")
           return None


   def calculateEMA(self, input_data: EMACalculationInput) -> Decimal:
       """
       Calculate EMA for a token/timeframe/period
      
       Args:
           input_data: EMA calculation input parameters
          
       Returns:
           Decimal: Calculated EMA value
       """
       try:
           # Get current indicator state
           indicator_key = f"ema_{input_data.period}"
           state = self.trading_handler.getIndicatorState(
               tokenAddress=input_data.tokenaddress,
               timeframe=input_data.timeframe,
               indicatorKey=indicator_key
           )
          
           # Get historical candles for initialization if needed
           if not state or not state['iswarmedup']:
               # Need to initialize EMA with SMA
               historical_candles = self.trading_handler.getLatestCandles(
                   tokenAddress=input_data.tokenaddress,
                   timeframe=input_data.timeframe,
                   limit=input_data.period
               )
              
               if len(historical_candles) < input_data.period:
                   # Not enough data yet
                   logger.debug(f"Not enough data for EMA{input_data.period} initialization: {len(historical_candles)}/{input_data.period}")
                   return None
              
               # Calculate SMA as initial EMA value
               close_prices = [Decimal(str(candle['closeprice'])) for candle in historical_candles]
               sma = sum(close_prices) / len(close_prices)
              
               # Update indicator state with initial value
               self.trading_handler.updateIndicatorState(
                   tokenAddress=input_data.tokenaddress,
                   timeframe=input_data.timeframe,
                   indicatorKey=indicator_key,
                   currentValue=sma,
                   candleCount=input_data.period,
                   isWarmedUp=True,
                   lastUpdatedUnix=input_data.unixtime
               )
              
               return sma
          
           # Calculate EMA using formula
           multiplier = Decimal(2) / (input_data.period + 1)
           prev_ema = Decimal(str(state['currentvalue']))
          
           new_ema = (input_data.closeprice * multiplier) + (prev_ema * (1 - multiplier))
          
           # Update indicator state
           self.trading_handler.updateIndicatorState(
               tokenAddress=input_data.tokenaddress,
               timeframe=input_data.timeframe,
               indicatorKey=indicator_key,
               currentValue=new_ema,
               previousValue=prev_ema,
               candleCount=state['candlecount'] + 1,
               isWarmedUp=True,
               lastUpdatedUnix=input_data.unixtime
           )
          
           return new_ema
          
       except Exception as e:
           logger.error(f"Error calculating EMA{input_data.period}: {e}")
           return None


   def calculateVWAP(self, input_data: VWAPCalculationInput) -> Decimal:
       """
       Calculate VWAP for a token/timeframe
      
       Args:
           input_data: VWAP calculation input parameters
          
       Returns:
           Decimal: Calculated VWAP value
       """
       try:
           # Calculate typical price
           typical_price = (input_data.highprice + input_data.lowprice + input_data.closeprice) / 3
          
           # Determine session boundaries (daily sessions at 00:00 UTC)
           session_start = (input_data.unixtime // 86400) * 86400
           session_end = session_start + 86399
          
           # Get or create VWAP session
           session = self.trading_handler.getVWAPSession(
               tokenAddress=input_data.tokenaddress,
               timeframe=input_data.timeframe,
               sessionStart=session_start
           )
          
           if not session:
               # New session - initialize
               cumulative_pv = typical_price * input_data.volume
               cumulative_volume = input_data.volume
               vwap = typical_price
               candle_count = 1
           else:
               # Update existing session
               cumulative_pv = Decimal(str(session['cumulativepv'])) + (typical_price * input_data.volume)
               cumulative_volume = Decimal(str(session['cumulativevolume'])) + input_data.volume
               vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else typical_price
               candle_count = session['candlecount'] + 1
          
           # Update VWAP session
           self.trading_handler.updateVWAPSession(
               tokenAddress=input_data.tokenaddress,
               timeframe=input_data.timeframe,
               sessionStart=session_start,
               sessionEnd=session_end,
               cumulativePV=cumulative_pv,
               cumulativeVolume=cumulative_volume,
               currentVWAP=vwap,
               highVWAP=vwap if not session else max(Decimal(str(session.get('highvwap', vwap))), vwap),
               lowVWAP=vwap if not session else min(Decimal(str(session.get('lowvwap', vwap))), vwap),
               lastCandleUnix=input_data.unixtime,
               candleCount=candle_count
           )
          
           return vwap
          
       except Exception as e:
           logger.error(f"Error calculating VWAP: {e}")
           return None


   # ===============================================================
   # FLEXIBLE INDICATOR CONFIGURATION METHODS
   # ===============================================================
  
   def setupIndicatorConfigurations(self, token_address: str = None) -> bool:
       """Setup default indicator configurations"""
       try:
           # Standard EMA configurations
           for timeframe in ['15m', '1h', '4h']:
               # EMA 21/34 cross
               self.trading_handler.addIndicatorConfig(
                   tokenAddress=token_address,
                   timeframe=timeframe,
                   indicatorType='ema',
                   parameters={'periods': [21, 34]},
                   configName='EMA_21_34_Cross',
                   priority=50
               )
              
               # VWAP
               self.trading_handler.addIndicatorConfig(
                   tokenAddress=token_address,
                   timeframe=timeframe,
                   indicatorType='vwap',
                   parameters={'session_type': 'daily'},
                   configName='Daily_VWAP',
                   priority=100
               )
          
           # Initialize states if specific token
           if token_address:
               for timeframe in ['15m', '1h', '4h']:
                   self.trading_handler.initializeIndicatorStates(token_address, timeframe)
          
           return True
          
       except Exception as e:
           logger.error(f"Error setting up indicator configurations: {e}")
           return False


   def addCustomIndicatorConfig(self, token_address: str, timeframe: str,
                               ema_periods: List[int], config_name: str = None) -> bool:
       """Add custom EMA configuration for specific token/timeframe"""
       try:
           if not ema_periods:
               return False
          
           if not config_name:
               config_name = f"Custom_EMA_{'_'.join(map(str, ema_periods))}"
          
           config_id = self.trading_handler.addIndicatorConfig(
               tokenAddress=token_address,
               timeframe=timeframe,
               indicatorType='ema',
               parameters={'periods': ema_periods},
               configName=config_name,
               priority=10
           )
          
           if config_id:
               self.trading_handler.initializeIndicatorStates(token_address, timeframe)
               return True
           return False
              
       except Exception as e:
           logger.error(f"Error adding custom indicator config: {e}")
           return False


   def detectIndicatorCrosses(self, token_address: str, timeframe: str) -> List[Dict]:
       """Detect crosses for all configured EMA pairs"""
       try:
           return self.trading_handler.detectCrosses(token_address, timeframe)
       except Exception as e:
           logger.error(f"Error detecting crosses: {e}")
           return []


   def getIndicatorSummary(self, token_address: str, timeframe: str) -> Dict:
       """
       Get summary of all indicators for a token/timeframe
      
       Args:
           token_address: Token contract address
           timeframe: Timeframe to summarize
          
       Returns:
           Dict: Indicator summary with current values and states
       """
       try:
           # Get configurations
           configs = self.trading_handler.getIndicatorConfigs(token_address, timeframe)
          
           # Get current states organized by type
           states = self.trading_handler.getDynamicIndicatorStates(token_address, timeframe)
          
           # Get latest candle for context
           latest_candles = self.trading_handler.getLatestCandles(token_address, timeframe, 1)
           latest_candle = latest_candles[0] if latest_candles else None
          
           summary = {
               'token_address': token_address,
               'timeframe': timeframe,
               'configurations': len(configs),
               'latest_candle': latest_candle,
               'indicators': {}
           }
          
           # Organize indicators by type
           for indicator_type, type_states in states.items():
               if isinstance(type_states, dict):
                   # Multiple periods (like EMA)
                   summary['indicators'][indicator_type] = {}
                   for period, state in type_states.items():
                       summary['indicators'][indicator_type][period] = {
                           'current_value': float(state['currentvalue']),
                           'previous_value': float(state['previousvalue']),
                           'is_warmed_up': state['iswarmedup'],
                           'candle_count': state['candlecount']
                       }
               else:
                   # Single indicator (like VWAP)
                   summary['indicators'][indicator_type] = {
                       'current_value': float(type_states['currentvalue']),
                       'previous_value': float(type_states['previousvalue']),
                       'is_warmed_up': type_states['iswarmedup'],
                       'candle_count': type_states['candlecount']
                   }
          
           return summary
          
       except Exception as e:
           logger.error(f"Error getting indicator summary: {e}")
           return {}


   # ===============================================================
   # ENHANCED TOKEN ADDITION METHODS
   # ===============================================================
  
   def addTokenManual(self, tokenAddress: str, pairAddress: str = None, symbol: str = None,
                     name: str = None, ema21Values: Dict = None, ema34Values: Dict = None,
                     referenceUnixTime: int = None, addedBy: str = None) -> Dict[str, Any]:
       """Manual token addition with age-based backfill strategy"""
       try:
           # Fetch token info and pair creation time from DexScreener
           dex_action = DexScreenerAction()
           token_info = dex_action.getTokenPrice(tokenAddress)
          
           if not token_info:
               return {'success': False, 'error': 'Token not found on DexScreener'}
          
           # Use DexScreener data as fallback
           symbol = symbol or token_info.symbol
           name = name or token_info.name
           pairAddress = pairAddress or token_info.pairAddress
          
           # Calculate pair age from DexScreener pairCreatedAt field
           pair_created_time = token_info.pairCreatedAt // 1000  # ms to seconds
           pair_age_days = (int(time.time()) - pair_created_time) / 86400
          
           # Route based on pair age: ≤7 days = full backfill, >7 days = limited + EMA init
           if pair_age_days <= 7:
               return self._addNewTokenWithFullBackfill(
                   tokenAddress, pairAddress, symbol, name, pair_created_time, addedBy
               )
           else:
               return self._addOldTokenWithLimitedBackfill(
                   tokenAddress, pairAddress, symbol, name, pair_created_time,
                   ema21Values, ema34Values, referenceUnixTime, addedBy
               )
              
       except Exception as e:
           logger.error(f"Error in manual token addition: {e}")
           return {'success': False, 'error': str(e)}


   def addTokensAutomatic(self, tokenAddresses: List[str]) -> Dict[str, Any]:
       """Batch automatic token addition with 10-day age filtering"""
       try:
           results = {'total': len(tokenAddresses), 'added': 0, 'skipped': 0, 'failed': 0, 'details': []}
           dex_action = DexScreenerAction()
           current_time = int(time.time())
          
           for token_address in tokenAddresses:
               try:
                   # Get token info and pair age from DexScreener
                   token_info = dex_action.getTokenPrice(token_address)
                   if not token_info:
                       results['failed'] += 1
                       results['details'].append({'tokenAddress': token_address, 'status': 'failed', 'reason': 'Not found on DexScreener'})
                       continue
                  
                   # Filter by age: only add tokens ≤10 days old
                   pair_age_days = (current_time - (token_info.pairCreatedAt // 1000)) / 86400
                   if pair_age_days > 10:
                       results['skipped'] += 1
                       results['details'].append({'tokenAddress': token_address, 'status': 'skipped', 'reason': f'Pair too old ({pair_age_days:.1f} days)'})
                       continue
                  
                   # Add token with full backfill
                   add_result = self._addNewTokenWithFullBackfill(
                       token_address, token_info.pairAddress, token_info.symbol,
                       token_info.name, token_info.pairCreatedAt // 1000, "automatic_system"
                   )
                  
                   if add_result['success']:
                       results['added'] += 1
                       results['details'].append({'tokenAddress': token_address, 'status': 'added', 'tokenId': add_result['tokenId']})
                   else:
                       results['failed'] += 1
                       results['details'].append({'tokenAddress': token_address, 'status': 'failed', 'reason': add_result.get('error')})
                      
               except Exception as e:
                   results['failed'] += 1
                   results['details'].append({'tokenAddress': token_address, 'status': 'failed', 'reason': str(e)})
          
           return results
          
       except Exception as e:
           logger.error(f"Error in automatic token addition: {e}")
           return {'success': False, 'error': str(e)}


   def _addNewTokenWithFullBackfill(self, tokenAddress: str, pairAddress: str,
                                  symbol: str, name: str, pairCreatedTime: int, addedBy: str) -> Dict:
       """Add new token (<=7 days) with full historical backfill"""
       try:
           # Step 1: Add token to database
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
          
           # Step 2: Backfill all data from pair creation to now
           current_time = int(time.time())
           hours_to_backfill = int((current_time - pairCreatedTime) / 3600) + 1
          
           backfill_result = self.executeBackfill(
               token_address=tokenAddress,
               pair_address=pairAddress,
               symbol=symbol,
               name=name,
               hours=hours_to_backfill
           )
          
           if not backfill_result.success:
               return {'success': False, 'error': f'Backfill failed: {backfill_result.errordetails}'}
          
           # Step 3: Setup indicator configurations
           self.setupIndicatorConfigurations(tokenAddress)
          
           # Step 4: Calculate indicators for all historical data
           self._calculateHistoricalIndicators(tokenAddress)
          
           return {
               'success': True,
               'tokenId': token_id,
               'mode': 'full_backfill',
               'candlesInserted': backfill_result.candlesinserted,
               'creditsUsed': backfill_result.apicreditsused
           }
          
       except Exception as e:
           logger.error(f"Error in new token addition: {e}")
           return {'success': False, 'error': str(e)}


   def _addOldTokenWithLimitedBackfill(self, tokenAddress: str, pairAddress: str,
                                     symbol: str, name: str, pairCreatedTime: int,
                                     ema21Values: Dict, ema34Values: Dict,
                                     referenceUnixTime: int, addedBy: str) -> Dict:
       """Add old token (>7 days) with limited backfill and EMA initialization"""
       try:
           # Step 1: Add token to database
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
          
           # Step 2: Backfill last 2 days of data
           backfill_result = self.executeBackfill(
               token_address=tokenAddress,
               pair_address=pairAddress,
               symbol=symbol,
               name=name,
               hours=48  # 2 days
           )
          
           if not backfill_result.success:
               return {'success': False, 'error': f'Backfill failed: {backfill_result.errordetails}'}
          
           # Step 3: Setup indicator configurations
           self.setupIndicatorConfigurations(tokenAddress)
          
           # Step 4: Initialize EMA values with provided data
           if ema21Values and ema34Values and referenceUnixTime:
               self._initializeEMAValues(tokenAddress, ema21Values, ema34Values, referenceUnixTime)
          
           # Step 5: Calculate VWAP for current day
           self._calculateCurrentDayVWAP(tokenAddress)
          
           return {
               'success': True,
               'tokenId': token_id,
               'mode': 'limited_backfill',
               'candlesInserted': backfill_result.candlesinserted,
               'creditsUsed': backfill_result.apicreditsused
           }
          
       except Exception as e:
           logger.error(f"Error in old token addition: {e}")
           return {'success': False, 'error': str(e)}


   def _calculateHistoricalIndicators(self, tokenAddress: str):
       """Calculate indicators for all historical data"""
       try:
           for timeframe in ['15m', '1h', '4h']:
               candles = self.trading_handler.getLatestCandles(tokenAddress, timeframe, 1000)
              
               for candle in reversed(candles):  # Process chronologically
                   self.trading_handler.calculateDynamicIndicators(tokenAddress, timeframe, candle)
                  
       except Exception as e:
           logger.error(f"Error calculating historical indicators: {e}")


   def _initializeEMAValues(self, tokenAddress: str, ema21Values: Dict, ema34Values: Dict, referenceUnixTime: int):
       """Initialize EMA values with provided reference data"""
       try:
           for timeframe in ['15m', '1h', '4h']:
               if timeframe in ema21Values:
                   self.trading_handler.updateIndicatorState(
                       tokenAddress, timeframe, 'ema_21',
                       currentValue=Decimal(str(ema21Values[timeframe])),
                       candleCount=21, isWarmedUp=True, lastUpdatedUnix=referenceUnixTime
                   )
              
               if timeframe in ema34Values:
                   self.trading_handler.updateIndicatorState(
                       tokenAddress, timeframe, 'ema_34',
                       currentValue=Decimal(str(ema34Values[timeframe])),
                       candleCount=34, isWarmedUp=True, lastUpdatedUnix=referenceUnixTime
                   )
                  
       except Exception as e:
           logger.error(f"Error initializing EMA values: {e}")


   def _calculateCurrentDayVWAP(self, tokenAddress: str):
       """Calculate VWAP for current day data"""
       try:
           current_time = int(time.time())
           day_start = (current_time // 86400) * 86400
          
           for timeframe in ['15m', '1h', '4h']:
               candles = self.trading_handler.getCandlesForAggregation(
                   tokenAddress, timeframe, day_start, current_time
               )
              
               for candle in candles:
                   self.trading_handler._calculateVWAPIndicator(
                       tokenAddress, timeframe, {}, candle
                   )
                  
       except Exception as e:
           logger.error(f"Error calculating current day VWAP: {e}")

