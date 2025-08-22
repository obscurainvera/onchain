"""
VWAPProcessor - Handles VWAP calculations and session management

STEP-BY-STEP VWAP CALCULATION PROCESS:
=====================================

EXAMPLE: Token ABC, 1hr timeframe, Current time: 15:00

STEP 1: Session Detection
- Check if vwapsessions table has record for ABC-1h
- Get lastfetchedat from timeframemetadata table
- Compare: lastfetchedat vs sessionendunix

STEP 2: Determine Action
Case A - NEW_SESSION (no existing record):
  → Calculate VWAP for ALL candles since pair creation
  → Example: Get candles [09:00, 10:00, 11:00, 12:00, 13:00, 14:00, 15:00]
  → Create new vwapsessions record

Case B - SAME_DAY_UPDATE (lastfetchedat <= sessionendunix):
  → Example: lastfetchedat=15:00, sessionendunix=23:59:59 (same day)
  → Get existing: cumulativepv=220250, cumulativevolume=6300, lastcandleunix=12:00
  → Get NEW candles after 12:00: [13:00, 14:00, 15:00]
  → Add new data to existing cumulative totals (EFFICIENT!)

Case C - NEW_DAY_RESET (lastfetchedat > sessionendunix):
  → Example: lastfetchedat=09:00 next day, sessionendunix=23:59:59 previous day
  → Reset session boundaries to current day (00:00 to 23:59)
  → Calculate fresh VWAP for current day candles only
  → Update session with new boundaries and reset cumulative data

STEP 3: VWAP Calculation
- For each candle: typical_price = (high + low + close) / 3
- Cumulative: total_pv += (typical_price × volume)
- Cumulative: total_volume += volume  
- VWAP = total_pv / total_volume

STEP 4: Database Updates (ATOMIC)
- Update ohlcvdetails.vwapvalue for each processed candle
- Update/Insert vwapsessions with new cumulative totals
- All operations in single transaction (success/failure together)

KEY OPTIMIZATION: Same-day updates use existing cumulative data instead of
recalculating entire day, making it extremely fast for frequent updates.
"""

from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timezone
from logs.logger import get_logger
from database.trading.TradingHandler import TradingHandler
from scheduler.SchedulerConstants import CandleDataKeys, Timeframes
import time
from actions.TradingActionUtil import TradingActionUtil

logger = get_logger(__name__)

class VWAPSessionResult:
    """Result container for VWAP session operations"""
    NEW_SESSION = 'new_session'
    SAME_DAY_UPDATE = 'same_day_update' 
    NEW_DAY_RESET = 'new_day_reset'

class VWAPProcessor:
    """
    Production-ready VWAP processor with atomic transactions and session management
    """
    
    def __init__(self, trading_handler: TradingHandler):
        self.trading_handler = trading_handler
        self.TIMEFRAMES = [Timeframes.FIFTEEN_MIN, Timeframes.ONE_HOUR, Timeframes.FOUR_HOUR]
    
    def processVWAPForAllTokens(self, successfulTokensList: List[Dict[str, Any]]) -> bool:
        """
        PRODUCTION-READY: Process VWAP calculations with TRUE batch optimization
        
        OPTIMIZATION STRATEGY:
        1. Fetch all metadata in 2 queries (sessions + fetch times)
        2. Analyze all operations and batch candle requirements  
        3. Fetch ALL required candles in 1 UNION query
        4. Calculate VWAP for all tokens in memory
        5. Batch update ALL VWAP values and sessions in 2 queries
        
        Total DB queries: 5 (regardless of token count)
        """
        try:
            if not successfulTokensList:
                return True
                
            logger.info(f"Processing VWAP for {len(successfulTokensList)} tokens")
            
            # STEP 1: Get all metadata in batch (2 queries)
            tokenAddresses = [token['tokenaddress'] for token in successfulTokensList]
            timeframes = self.TIMEFRAMES
            
            allVwapSessions = self.trading_handler.getAllVWAPSessionInfo(tokenAddresses, timeframes)
            allLastFetchedAtTimes = self.trading_handler.getAllLastFetchTimes(tokenAddresses, timeframes)
            
            # STEP 2: Analyze all operations and prepare candle requirements
            vwapOperations = []
            operationMap = {}  # Maps operation_id to (token, timeframe, action_type)
            
            for token in successfulTokensList:
                for timeframe in timeframes:
                    vwapSession = allVwapSessions.get(token['tokenaddress'], {}).get(timeframe)
                    lastFetchedAtTime = allLastFetchedAtTimes.get(token['tokenaddress'], {}).get(timeframe)
                    
                    if not lastFetchedAtTime:
                        continue
                    
                    sessionType = self._determineSessionType(vwapSession, lastFetchedAtTime)
                    operationId = f"{token['tokenaddress']}_{timeframe}_{sessionType}"
                    operationMap[operationId] = (token, timeframe, sessionType, vwapSession)
                    
                    # Prepare candle operation based on action type
                    if sessionType == VWAPSessionResult.NEW_SESSION:
                        vwapOperations.append({
                            'token_address': token['tokenaddress'],
                            'timeframe': timeframe,
                            'operation_type': 'new_session',
                            'from_time': token.get('paircreatedtime', lastFetchedAtTime)
                        })
                    elif sessionType == VWAPSessionResult.SAME_DAY_UPDATE:
                        vwapOperations.append({
                            'token_address': token['tokenaddress'],
                            'timeframe': timeframe,
                            'operation_type': 'same_day_update',
                            'from_time': vwapSession['lastcandleunix']
                        })
                    elif sessionType == VWAPSessionResult.NEW_DAY_RESET:
                        sessionStartTime, sessionEndTime = self.calculateVwapSessionRange(lastFetchedAtTime)
                        vwapOperations.append({
                            'token_address': token['tokenaddress'],
                            'timeframe': timeframe,
                            'operation_type': 'new_day_reset',
                            'from_time': sessionStartTime,
                            'to_time': lastFetchedAtTime
                        })
            
            # STEP 3: Fetch ALL required candles in single UNION query
            allCandlesNeededToCalculateVwap = self.trading_handler.getBatchVWAPCandles(vwapOperations)
            
            # STEP 4: Process all VWAP calculations in memory
            updateExsistingVwap = []
            createNewVwapSession = []
            
            for operationId, (token, timeframe, sessionType, vwapSession) in operationMap.items():
                candlesNeededToCalculateVwap = allCandlesNeededToCalculateVwap.get(operationId, [])
                if not candlesNeededToCalculateVwap:
                    continue
                
                lastFetchedAtTime = allLastFetchedAtTimes[token['tokenaddress']][timeframe]
                vwapCandleUpdatedData, vwapSessionUpdatedData = self.calculateVwap(
                    token, timeframe, sessionType, vwapSession, candlesNeededToCalculateVwap, lastFetchedAtTime
                )
                
                if vwapCandleUpdatedData: # tp update the vwap in the candles in the ohclvdetails table
                    updateExsistingVwap.extend(vwapCandleUpdatedData)
                if vwapSessionUpdatedData: # to create a new vwap session in the vwapsessions table
                    createNewVwapSession.append(vwapSessionUpdatedData)
            
            # STEP 5: Batch update ALL VWAP data in 2 queries
            success = self.trading_handler.batchUpdateVWAPData(updateExsistingVwap, createNewVwapSession)
            
            logger.info(f"VWAP processing completed: {len(updateExsistingVwap)} VWAP updates, {len(createNewVwapSession)} session updates")
            return success
            
        except Exception as e:
            logger.error(f"Error in batch VWAP processing: {e}")
            return False
    
    def calculateVwap(self, token: Dict[str, Any], timeframe: str, actionType: str,
                               exsistingVwapSessionInfo: Optional[Dict], candlesNeededToCalculateVwap: List[Dict], latestFetchedAt: int) -> Tuple[Optional[List[Dict]], Optional[Dict]]:
        """
        Calculate VWAP for specific action type using pre-fetched candles
        
        Returns:
            Tuple: (vwap_updates_list, session_update_dict)
        """
        try:
            if not candlesNeededToCalculateVwap:
                return None, None
            
            tokenAddress = token['tokenaddress']
            pairAddress = token['pairaddress']
            
            if actionType == VWAPSessionResult.NEW_SESSION:
                # Calculate VWAP for all candles (new session)
                vwapSessionStart, vwapSessionEnd = self.calculateVwapSessionRange(latestFetchedAt)
                calculatedVwapData = self._calculateVWAPForResetOrNew(candlesNeededToCalculateVwap)
                
                # Prepare VWAP updates
                vwapCandleUpdatedData = []
                for candle_vwap in calculatedVwapData['candle_vwaps']:
                    vwapCandleUpdatedData.append({
                        'token_address': tokenAddress,
                        'timeframe': timeframe,
                        'unixtime': candle_vwap['unixtime'],
                        'vwap': candle_vwap['vwap']
                    })
                
                # Prepare session update
                vwapSessionUpdatedData = {
                    'token_address': tokenAddress,
                    'pair_address': pairAddress,
                    'timeframe': timeframe,
                    'session_start': vwapSessionStart,
                    'session_end': vwapSessionEnd,
                    'cumulative_pv': calculatedVwapData['cumulative_pv'],
                    'cumulative_volume': calculatedVwapData['cumulative_volume'],
                    'final_vwap': calculatedVwapData['final_vwap'],
                    'latest_candle_time': calculatedVwapData['latest_candle_time'],
                    'next_candle_fetch': latestFetchedAt
                }
                
                return vwapCandleUpdatedData, vwapSessionUpdatedData
            
            elif actionType == VWAPSessionResult.SAME_DAY_UPDATE:
                # Calculate incremental VWAP using existing session data
                calculatedVwapData = self._calculateIncrementalVWAP(exsistingVwapSessionInfo, candlesNeededToCalculateVwap)
                
                # Prepare VWAP updates
                vwapCandleUpdatedData = []
                for candle_vwap in calculatedVwapData['candle_vwaps']:
                    vwapCandleUpdatedData.append({
                        'token_address': tokenAddress,
                        'timeframe': timeframe,
                        'unixtime': candle_vwap['unixtime'],
                        'vwap': candle_vwap['vwap']
                    })
                
                # Prepare session update
                vwapSessionUpdatedData = {
                    'token_address': tokenAddress,
                    'pair_address': pairAddress,
                    'timeframe': timeframe,
                    'session_start': exsistingVwapSessionInfo['sessionstartunix'],
                    'session_end': exsistingVwapSessionInfo['sessionendunix'],
                    'cumulative_pv': calculatedVwapData['cumulative_pv'],
                    'cumulative_volume': calculatedVwapData['cumulative_volume'],
                    'final_vwap': calculatedVwapData['final_vwap'],
                    'latest_candle_time': calculatedVwapData['latest_candle_time'],
                    'next_candle_fetch': latestFetchedAt
                }
                
                return vwapCandleUpdatedData, vwapSessionUpdatedData
            
            elif actionType == VWAPSessionResult.NEW_DAY_RESET:
                # Calculate fresh VWAP for new day
                vwapSessionStart, vwapSessionEnd = self.calculateVwapSessionRange(latestFetchedAt)
                calculatedVwapData = self._calculateVWAPForResetOrNew(candlesNeededToCalculateVwap)
                
                # Prepare VWAP updates
                vwapCandleUpdatedData = []
                for candle_vwap in calculatedVwapData['candle_vwaps']:
                    vwapCandleUpdatedData.append({
                        'token_address': tokenAddress,
                        'timeframe': timeframe,
                        'unixtime': candle_vwap['unixtime'],
                        'vwap': candle_vwap['vwap']
                    })
                
                # Prepare session update
                vwapSessionUpdatedData = {
                    'token_address': tokenAddress,
                    'pair_address': pairAddress,
                    'timeframe': timeframe,
                    'session_start': vwapSessionStart,
                    'session_end': vwapSessionEnd,
                    'cumulative_pv': calculatedVwapData['cumulative_pv'],
                    'cumulative_volume': calculatedVwapData['cumulative_volume'],
                    'final_vwap': calculatedVwapData['final_vwap'],
                    'latest_candle_time': calculatedVwapData['latest_candle_time'],
                    'next_candle_fetch': latestFetchedAt
                }
                
                return vwapCandleUpdatedData, vwapSessionUpdatedData
            
            return None, None
            
        except Exception as e:
            logger.error(f"Error calculating VWAP for action {actionType}: {e}")
            return None, None
    
    
    def _determineSessionType(self, session: Optional[Dict], lastFetchedAtTime: int) -> str:
        """
        CRITICAL LOGIC: Determine what VWAP action to take based on session state
        
        Returns:
            - NEW_SESSION: No existing session found
            - SAME_DAY_UPDATE: lastfetchedat <= sessionendunix (same day)
            - NEW_DAY_RESET: lastfetchedat > sessionendunix (new day)
        """
        if not session:
            return VWAPSessionResult.NEW_SESSION
        
        sessionEndUnix = session['sessionendunix']
        
        if lastFetchedAtTime <= sessionEndUnix:
            return VWAPSessionResult.SAME_DAY_UPDATE
        else:
            return VWAPSessionResult.NEW_DAY_RESET
    
    
    def calculateVwapSessionRange(self, timestamp: int) -> Tuple[int, int]:
        """Calculate session start (00:00:00) and end (23:59:59) for given timestamp"""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        session_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        session_end = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        return int(session_start.timestamp()), int(session_end.timestamp())
    
    
    def _calculateVWAPForResetOrNew(self, candles: List[Dict]) -> Dict:
        """
        Calculate VWAP for list of candles
        Returns cumulative data and individual candle VWAPs
        """
        if not candles:
            return {
                'cumulative_pv': Decimal('0'),
                'cumulative_volume': Decimal('0'),
                'final_vwap': Decimal('0'),
                'candle_vwaps': [],
                'latest_candle_time': 0
            }
        
        cumulative_pv = Decimal('0')
        cumulative_volume = Decimal('0')
        candle_vwaps = []
        
        for candle in candles:
            # Calculate typical price (HLC/3)
            high = Decimal(str(candle['highprice']))
            low = Decimal(str(candle['lowprice']))
            close = Decimal(str(candle['closeprice']))
            volume = Decimal(str(candle['volume']))
            
            typical_price = (high + low + close) / Decimal('3')
            price_volume = typical_price * volume
            
            cumulative_pv += price_volume
            cumulative_volume += volume
            
            # Calculate running VWAP
            vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else Decimal('0')
            
            candle_vwaps.append({
                'unixtime': candle['unixtime'],
                'vwap': vwap
            })
        
        return {
            'cumulative_pv': cumulative_pv,
            'cumulative_volume': cumulative_volume, 
            'final_vwap': vwap,
            'candle_vwaps': candle_vwaps,
            'latest_candle_time': candles[-1]['unixtime']
        }
    
    def _calculateIncrementalVWAP(self, exsistingVwapSession: Dict, newCandlesToUpdateVwap: List[Dict]) -> Dict:
        """
        Calculate incremental VWAP using existing cumulative data
        More efficient than recalculating from scratch
        """
        cumulative_pv = exsistingVwapSession['cumulativepv']
        cumulative_volume = exsistingVwapSession['cumulativevolume']
        candle_vwaps = []
        
        for candle in newCandlesToUpdateVwap:
            # Calculate typical price (HLC/3)
            high = Decimal(str(candle['highprice']))
            low = Decimal(str(candle['lowprice']))
            close = Decimal(str(candle['closeprice']))
            volume = Decimal(str(candle['volume']))
            
            typical_price = (high + low + close) / Decimal('3')
            price_volume = typical_price * volume
            
            cumulative_pv += price_volume
            cumulative_volume += volume
            
            # Calculate running VWAP
            vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else Decimal('0')
            
            candle_vwaps.append({
                'unixtime': candle['unixtime'],
                'vwap': vwap
            })
        
        return {
            'cumulative_pv': cumulative_pv,
            'cumulative_volume': cumulative_volume,
            'final_vwap': vwap if newCandlesToUpdateVwap else exsistingVwapSession['currentvwap'],
            'candle_vwaps': candle_vwaps,
            # Get the timestamp of the most recent candle ([-1] gets the last element in the list)
            # If no new candles, use the existing session's last candle time
            'latest_candle_time': newCandlesToUpdateVwap[-1]['unixtime'] if newCandlesToUpdateVwap else exsistingVwapSession['lastcandleunix']
        }
    

    def calculateVwapFromAPI(self, tokenAddress: str, pairAddress: str, pairCreatedTime: int, allCandles: Dict[str, List[Dict]]) -> Dict:
        """
        API FLOW ONLY: Process VWAP for new token addition using pre-loaded candles

        Idea behind this : when we are adding a new token or old token, we only need to calculate vwap for all timeframes for the current day.
        we usually get the past 2 days candles but only take the current day candles and update the vwap values candle wise and session wise
        
        NOTE: This function exists for API flow because:
        - API flow has candles already loaded in memory (no need to fetch from DB)
        - Processes all timeframes at once during token addition
        - Different from scheduler's incremental VWAP updates
        """
        try:
            
            currentTime = int(time.time())
            dayStart = (currentTime // 86400) * 86400  # Start of current day UTC
            dayEnd = dayStart + 86400
            
            # Filter today's candles from pre-loaded data
            todaysCandlesCategorizedByTimeframe = {}
            for timeframe, candles in allCandles.items():
                todayCandles = [c for c in candles if dayStart <= c['unixtime'] < dayEnd] #get all candles for the current day
                if todayCandles:
                    todaysCandlesCategorizedByTimeframe[timeframe] = todayCandles #categorize the candles by timeframe
            
            if not todaysCandlesCategorizedByTimeframe:
                logger.info(f"No today's candles found for VWAP processing for {tokenAddress}")
                return {'success': True}
            
            # Process all timeframes and prepare batch operations
            calculatedVwapData = []
            for timeframe, todayCandles in todaysCandlesCategorizedByTimeframe.items():
                logger.info(f"Processing VWAP for {tokenAddress} {timeframe}: {len(todayCandles)} candles")
                
                calculatedVwap = self._calculateVWAPForResetOrNew(todayCandles)
                
                # Calculate next candle fetch time
                nextCandleFetchTime = TradingActionUtil.calculateNextCandleFetch(timeframe, calculatedVwap['latest_candle_time'])
                
                # Prepare VWAP operation data
                calculatedVwapData.append({
                    'timeframe': timeframe,
                    'today_candles': todayCandles,
                    'vwap_result': calculatedVwap,
                    'next_candle_fetch': nextCandleFetchTime,
                    'day_start': dayStart
                })
            
            # Execute all VWAP operations using TradingHandler
            return self.trading_handler.recordVwapCandleUpdateAndVwapSessionUpdateFromAPI(tokenAddress, pairAddress, calculatedVwapData)
            
        except Exception as e:
            logger.error(f"Error processing VWAP with preloaded candles: {e}")
            return {'success': False, 'error': str(e)}
    
    
