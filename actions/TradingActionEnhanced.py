from config.Config import get_config
from typing import Optional, Dict, Any, List, Union
from database.operations.PortfolioDB import PortfolioDB
from database.trading.TradingHandler import TradingHandler, AdditionSource, EMAStatus
from database.trading.TradingModels import (
    TrackedToken, OHLCVCandle, BirdEyeOHLCVItem, BirdEyeOHLCVResponse,
    BackfillRequest, BackfillResult, EMACalculationInput, VWAPCalculationInput
)
from decimal import Decimal
import time
from datetime import datetime, timedelta
from logs.logger import get_logger
from actions.DexscrennerAction import DexScreenerAction
from actions.TradingActionUtil import TradingActionUtil
from services.BirdEyeServiceHandler import BirdEyeServiceHandler
from scheduler.VWAPProcessor import VWAPProcessor
from scheduler.EMAProcessor import EMAProcessor
import pytz
import uuid
import json
from sqlalchemy import text
from scheduler.SchedulerConstants import CandleDataKeys

logger = get_logger(__name__)

class TradingActionEnhanced:
    """Enhanced Trading Action with comprehensive token processing flows"""
    
    def __init__(self, db: PortfolioDB):
        """Initialize with database handler"""
        self.db = db
        self.trading_handler = TradingHandler(db.conn_manager)
        
        # Use unified BirdEye service handler
        self.birdeye_handler = BirdEyeServiceHandler(db)
        
        # Initialize processor instances
        self.vwap_processor = VWAPProcessor(self.trading_handler)
        self.ema_processor = EMAProcessor(self.trading_handler)

       
    def addNewToken(self, tokenAddress: str, pairAddress: str, symbol: str, name: str, pairCreatedTime: int, addedBy: str) -> Dict[str, Any]:
        """Phase 1: Add new token with comprehensive processing"""
        try:
            currentTime = int(time.time())
            
            # Step 1: Add token to trackedtokens table
            tokenId = self.trading_handler.addToken(
                tokenAddress=tokenAddress,
                symbol=symbol,
                name=name,
                pairAddress=pairAddress,
                pairCreatedTime=pairCreatedTime,
                additionSource=AdditionSource.MANUAL if addedBy != "automatic_system" else AdditionSource.AUTOMATIC,
                addedBy=addedBy
            )
            
            if not tokenId:
                return {'success': False, 'error': 'Failed to add token to database'}
            
            # Step 2: Create 15m timeframe record
            self.trading_handler.createEmptyTimeFrameRecord(tokenAddress, pairAddress, '15m')
            
            # Step 3: Fetch all data from pair created time to now as its a new token
            all15MCandles = self.recordAll15MCandlesFromPairCreatedTime(tokenAddress, pairAddress, pairCreatedTime, currentTime)
            
            if not all15MCandles['success']:
                # Rollback token addition
                self.trading_handler.disableToken(tokenAddress, addedBy, "Backfill failed")
                return {'success': False, 'error': f'Backfill failed: {all15MCandles["error"]}'}
            
            # Step 5: Aggregate 15m -> 1h and 4h using the candles we just fetched (no DB query!)
            self.aggregate15MInto1HrAnd4Hr(tokenAddress, pairAddress, all15MCandles.get('fetchedCandles', []))
            
            # Step 6: Get ALL candles for all timeframes in single database call
            allCandles = self.trading_handler.getAllCandlesFromAllTimeframes(tokenAddress, pairAddress)
            if not allCandles:
                logger.warning(f"No candles found after aggregation for {tokenAddress}")
                return {'success': True, 'tokenId': tokenId, 'mode': 'new_token_no_indicators'}
            
            # Step 7: Process VWAP using filtered candles (no additional DB calls)
            calculatedVwap = self.vwap_processor.calculateVwapFromAPI(tokenAddress, pairAddress, pairCreatedTime, allCandles)
            if not calculatedVwap['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"VWAP processing failed: {calculatedVwap['error']}")
                return calculatedVwap
            
            # Step 8: Process EMA using filtered candles (no additional DB calls)
            calculatedEMA = self.ema_processor.calcualteEMAForNewTokenFromAPI(tokenAddress, pairAddress, pairCreatedTime, allCandles)
            if not calculatedEMA['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"EMA processing failed: {calculatedEMA['error']}")
                return calculatedEMA
            
            return {
                'success': True,
                'tokenId': tokenId,
                'mode': 'new_token_full_processing',
                'candlesInserted': all15MCandles.get('candlesInserted', 0),
                'creditsUsed': all15MCandles.get('creditsUsed', 0)
            }
            
        except Exception as e:
            logger.error(f"Error in new token addition: {e}")
            try:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"Addition failed: {str(e)}")
            except:
                pass
            return {'success': False, 'error': str(e)}    

    def addOldToken(self, tokenAddress: str, pairAddress: str, symbol: str, name: str,
                                     pairCreatedTime: int, perTimeframeEMAData: Dict, addedBy: str) -> Dict[str, Any]:
        """Phase 2: Add old token with per-timeframe EMA initialization"""
        try:
            
            # Step 1: Add token to trackedtokens table
            tokenId = self.trading_handler.addToken(
                tokenAddress=tokenAddress,
                symbol=symbol,
                name=name,
                pairAddress=pairAddress,
                pairCreatedTime=pairCreatedTime,
                additionSource=AdditionSource.MANUAL,
                addedBy=addedBy
            )
            
            if not tokenId:
                return {'success': False, 'error': 'Failed to add token to database'}
            
            # Step 2: Create 15m timeframe record
            self.trading_handler.createEmptyTimeFrameRecord(tokenAddress, pairAddress, '15m')
            
            # Step 3: Fetch 2 days of 15m data from BirdEye API
            candlesFromAPI = self.fetchCandlesFromAPIForTheGivenTimeRange(tokenAddress, pairAddress, 48)
            
            if not candlesFromAPI['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, "Backfill failed")
                return {'success': False, 'error': f'Backfill failed: {candlesFromAPI["error"]}'}
            
            # Step 4: Aggregate 15m -> 1h and 4h, create timeframe records
            self.aggregate15MInto1HrAnd4Hr(tokenAddress, pairAddress, candlesFromAPI.get('fetchedCandles', []))
            
            # Step 5: Get ALL candles for all timeframes in single database call
            allCandles = self.trading_handler.getAllCandlesFromAllTimeframes(tokenAddress, pairAddress)
            if not allCandles:
                logger.warning(f"No candles found after aggregation for {tokenAddress}")
                return {'success': True, 'tokenId': tokenId, 'mode': 'old_token_no_indicators'}
            
            # Step 6: Process VWAP using filtered candles (no additional DB calls)
            calculatedVwap = self.vwap_processor.calculateVwapFromAPI(tokenAddress, pairAddress, pairCreatedTime, allCandles)
            if not calculatedVwap['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"VWAP processing failed: {calculatedVwap['error']}")
                return calculatedVwap
            
            # Step 7: just set the EMA from the API in the ema states table and update the corresponding candle values in the ohlcv table
            calculatedEMA = self.ema_processor.setEMAForOldTokenFromAPI(
                tokenAddress, pairAddress, pairCreatedTime, perTimeframeEMAData, allCandles
            )
            if not calculatedEMA['success']:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"EMA processing failed: {calculatedEMA['error']}")
                return calculatedEMA
            
            return {
                'success': True,
                'tokenId': tokenId,
                'mode': 'old_token_per_timeframe_ema',
                'candlesInserted': candlesFromAPI.get('candlesInserted', 0),
                'creditsUsed': candlesFromAPI.get('creditsUsed', 0)
            }
            
        except Exception as e:
            logger.error(f"Error in old token addition with per-timeframe EMA: {e}")
            try:
                self.trading_handler.disableToken(tokenAddress, addedBy, f"Addition failed: {str(e)}")
            except:
                pass
            return {'success': False, 'error': str(e)}
    
    def fetchCandlesFromAPIForTheGivenTimeRange(self, tokenAddress: str, pairAddress: str, hours: int) -> Dict:
        """Fetch historical data using unified BirdEye service (for old tokens - 2 days backfill)"""
        try:
            # Calculate time range
            toTime = int(time.time())
            fromTime = toTime - (hours * 3600)
            
            # Use unified BirdEye service handler - just fetch, don't store
            candleDataFromAPI = self.birdeye_handler.getAllCandleDataFromAPI(tokenAddress, pairAddress, fromTime, toTime)
            
            if candleDataFromAPI['success']:
                # Format data for existing batchPersistAllCandles method
                from scheduler.SchedulerConstants import CandleDataKeys
                
                allCandleData = {
                    tokenAddress: {
                        CandleDataKeys.CANDLES: candleDataFromAPI['candles'],
                        CandleDataKeys.LATEST_TIME: candleDataFromAPI['latestTime'],
                        CandleDataKeys.COUNT: candleDataFromAPI['candleCount']
                    }
                }
                
                # Use existing batch persist method from scheduler flow
                candlesInserted = self.trading_handler.batchPersistAllCandles(allCandleData)
                
                return {
                    'success': True,
                    'candlesInserted': candlesInserted,
                    'creditsUsed': candleDataFromAPI['creditsUsed'],
                    'latestUnixTime': candleDataFromAPI['latestTime']
                }
            else:
                return candleDataFromAPI
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return {'success': False, 'error': str(e)}
    
    def recordAll15MCandlesFromPairCreatedTime(self, tokenAddress: str, pairAddress: str, pairCreatedTime: int, currentTime: int) -> Dict:
        """Fetch all historical data from pair creation to now using unified BirdEye service (for new tokens)"""
        try:
            # Use unified BirdEye service handler - just fetch, don't store
            candleDataFromAPI = self.birdeye_handler.getAllCandleDataFromAPI(tokenAddress, pairAddress, pairCreatedTime, currentTime)
            
            if candleDataFromAPI['success']:
                # Format data for existing batchPersistAllCandles method
                processedCandleData = {
                    tokenAddress: {
                        CandleDataKeys.CANDLES: candleDataFromAPI['candles'],
                        CandleDataKeys.LATEST_TIME: candleDataFromAPI['latestTime'],
                        CandleDataKeys.COUNT: candleDataFromAPI['candleCount']
                    }
                }
                
                # Use existing batch persist method from scheduler flow
                insertedCandles = self.trading_handler.batchPersistAllCandles(processedCandleData)
                
                return {
                    'success': True,
                    'candlesInserted': insertedCandles,
                    'creditsUsed': candleDataFromAPI['creditsUsed'],
                    'latestUnixTime': candleDataFromAPI['latestTime'],
                    'fetchedCandles': candleDataFromAPI['candles']
                }
            else:
                return candleDataFromAPI
            
        except Exception as e:
            logger.error(f"Error fetching historical data from creation: {e}")
            return {'success': False, 'error': str(e)}

    def aggregate15MInto1HrAnd4Hr(self, tokenAddress: str, pairAddress: str, all15MinCandlesFromAPI: List = None):
        """Aggregate 15m data into 1h and 4h with batched database operations"""
        try:
            # Use pre-fetched candles if available, otherwise get from database
            if all15MinCandlesFromAPI:
                all15MinCandles = all15MinCandlesFromAPI
                logger.info(f"Using {len(all15MinCandles)} pre-fetched 15min candles for aggregation")
            else:
                # Get all 15min candles from database
                all15MinCandles = self.trading_handler.getAll15MinCandlesWithoutTimeRange(tokenAddress, pairAddress)
                if not all15MinCandles:
                    logger.info(f"No 15min candles found for {tokenAddress}")
                    return
                logger.info(f"Fetched {len(all15MinCandles)} 15min candles from database for aggregation")
            
            # Aggregate to 1 hour using in-memory data - returns candles and latest time
            hourlyAggregatedCandles = TradingActionUtil.aggregateToHourlyInMemory(all15MinCandles)
            
            # Aggregate to 4 hour using the in-memory hourly data - returns candles and latest time
            fourHourltAggregatedCandles = TradingActionUtil.aggregateToFourHourlyInMemory(hourlyAggregatedCandles['candles'])
            
            # Prepare aggregation results with latest times and next fetch times calculated during aggregation
            aggregatedCandles = {
                'hourly_candles': hourlyAggregatedCandles['candles'],
                'four_hourly_candles': fourHourltAggregatedCandles['candles'],
                'latest_1h_time': hourlyAggregatedCandles['latest_time'],
                'latest_4h_time': fourHourltAggregatedCandles['latest_time'],
                'next_fetch_1h_time': hourlyAggregatedCandles['next_fetch_time'],
                'next_fetch_4h_time': fourHourltAggregatedCandles['next_fetch_time']
            }
            
            # Use optimized TradingHandler method for single transaction
            success = self.trading_handler.batchInsertAggregatedCandlesWithTimeframeUpdate(
                aggregatedCandles, tokenAddress, pairAddress
            )
            
            if not success:
                raise Exception("Failed to persist aggregation results")
            
            logger.info(f"Aggregation completed: {len(hourlyAggregatedCandles['candles'])} hourly, {len(fourHourltAggregatedCandles['candles'])} 4-hourly candles")
            
        except Exception as e:
            logger.error(f"Error in aggregation: {e}")
            raise
    
    
    
