"""
AVWAPProcessor - Handles AVWAP (Anchored Volume Weighted Average Price) processing for trading tokens

This processor manages AVWAP state and value persistence based on user-provided data from the API.
Unlike VWAP which is calculated from candle data, AVWAP values are provided by users and stored
as reference points for trading decisions.

FUNCTIONALITY:
==============

1. setAVWAPForTokenFromAPI: Process user-provided AVWAP data and persist to database
   - Validates AVWAP data structure and values
   - Creates avwapstates records with proper timestamps
   - Updates ohlcvdetails.avwapvalue for reference candles
   - Handles both new and old token flows

2. Data Structure Expected:
   avwap: {
       "1h": {"value": "10.5", "referenceTime": "1234567890"},
       "30min": {"value": "10.3", "referenceTime": "1234567890"},
       "4h": {"value": "10.7", "referenceTime": "1234567890"}
   }

3. Database Operations:
   - INSERT/UPDATE avwapstates table with AVWAP values and timestamps
   - UPDATE ohlcvdetails.avwapvalue for the reference candle
   - Calculate nextfetchtime based on timeframe intervals

USAGE:
======
- Called from TradingActionEnhanced after EMA processing
- Processes both new and old token AVWAP data uniformly
- Maintains consistency with existing processor patterns
"""

from typing import Dict, Any, List
from decimal import Decimal
import time
from logs.logger import get_logger
from constants.TradingHandlerConstants import TradingHandlerConstants
from constants.TradingAPIConstants import TradingAPIConstants
from actions.TradingActionUtil import TradingActionUtil
from utils.CommonUtil import CommonUtil

logger = get_logger(__name__)


class AVWAPProcessor:
    """Processor for AVWAP (Anchored Volume Weighted Average Price) operations"""
    
    def __init__(self, trading_handler):
        """
        Initialize AVWAP processor with trading handler
        
        Args:
            trading_handler: TradingHandler instance for database operations
        """
        self.trading_handler = trading_handler
    
    def setAVWAPForTokenFromAPI(self, tokenAddress: str, pairAddress: str, 
                               avwapData: Dict, allCandles: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        Process user-provided AVWAP data and persist to database
        
        This method handles AVWAP data from the API request and:
        1. Validates the AVWAP data structure
        2. Creates avwapstates records for each timeframe
        3. Updates ohlcvdetails.avwapvalue for reference candles
        4. Calculates proper next fetch times
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Pair contract address
            avwapData: AVWAP data from API request in format:
                {
                    "1h": {"value": "10.5", "referenceTime": "1234567890"},
                    "30min": {"value": "10.3", "referenceTime": "1234567890"}
                }
            allCandles: Pre-loaded candles for all timeframes
            
        Returns:
            Dict with success status and any error messages
        """
        try:
            logger.info(f"Processing AVWAP data for token {tokenAddress}")
            
            if not avwapData:
                logger.warning(f"No AVWAP data provided for {tokenAddress}")
                return {'success': True, 'message': 'No AVWAP data to process'}
            
            # Collections for batch operations
            avwapStateData = []
            avwapCandleUpdateData = []
            
            # Process each timeframe that has both candle data and user-provided AVWAP data
            for timeframe in avwapData.keys():
                if timeframe not in allCandles:
                    logger.debug(f"Skipping {timeframe} - no candle data available")
                    continue
                    
                candles = allCandles[timeframe]
                timeframeAVWAPData = avwapData[timeframe]
                
                logger.info(f"Processing AVWAP for timeframe {timeframe} with {len(candles)} candles")
                
                # Extract AVWAP value and reference time
                try:
                    avwapValue = Decimal(str(timeframeAVWAPData[TradingAPIConstants.RequestParameters.VALUE]))
                    referenceTime = int(timeframeAVWAPData[TradingAPIConstants.RequestParameters.REFERENCE_TIME])
                except (KeyError, ValueError, TypeError) as e:
                    logger.error(f"Invalid AVWAP data for {timeframe}: {e}")
                    continue
                
                logger.info(f"Setting AVWAP for {tokenAddress} {timeframe}: value={avwapValue} at timestamp {referenceTime}")
                
                # Prepare AVWAP state record
                avwapStateRecord = self.collectDataForAVWAPStateQuery(
                    tokenAddress, pairAddress, timeframe, avwapValue, referenceTime
                )
                avwapStateData.append(avwapStateRecord)
                
                # Prepare AVWAP candle update (UPDATE will handle if candle exists or not)
                avwapCandleUpdate = self.collectDataForAVWAPCandleUpdate(
                    tokenAddress, timeframe, referenceTime, avwapValue
                )
                avwapCandleUpdateData.append(avwapCandleUpdate)
                logger.info(f"Prepared AVWAP candle update for {tokenAddress} {timeframe} at {referenceTime}")
            
            # Execute batch database operations via handler
            if avwapStateData or avwapCandleUpdateData:
                logger.info(f"Executing AVWAP operations: {len(avwapStateData)} state records, {len(avwapCandleUpdateData)} candle updates")
                self.trading_handler.batchUpdateAVWAPData(avwapStateData, avwapCandleUpdateData)
            else:
                logger.warning(f"No valid AVWAP data to persist for {tokenAddress}")
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error processing AVWAP data for token {tokenAddress}: {e}")
            return {'success': False, 'error': str(e)}
    
    def collectDataForAVWAPStateQuery(self, tokenAddress: str, pairAddress: str, 
                                      timeframe: str, avwapValue: Decimal, 
                                      referenceUnixTime: int) -> Dict:
        """
        Prepare AVWAP state data dictionary for database operations
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Pair contract address  
            timeframe: Timeframe (30min, 1h, 4h)
            avwapValue: AVWAP value as Decimal
            referenceUnixTime: Reference timestamp
            
        Returns:
            Dict formatted for database insertion
        """
        timeframeInSeconds = CommonUtil.getTimeframeSeconds(timeframe)
        nextFetchTime = referenceUnixTime + timeframeInSeconds
        
        return {
            TradingHandlerConstants.AVWAPStates.TOKEN_ADDRESS: tokenAddress,
            TradingHandlerConstants.AVWAPStates.PAIR_ADDRESS: pairAddress,
            TradingHandlerConstants.AVWAPStates.TIMEFRAME: timeframe,
            TradingHandlerConstants.AVWAPStates.AVWAP: avwapValue,
            TradingHandlerConstants.AVWAPStates.LAST_UPDATED_UNIX: referenceUnixTime,
            TradingHandlerConstants.AVWAPStates.NEXT_FETCH_TIME: nextFetchTime
        }
    
    def collectDataForAVWAPCandleUpdate(self, tokenAddress: str, timeframe: str, 
                                        unixtime: int, avwapValue: Decimal) -> Dict:
        """
        Prepare AVWAP candle update data dictionary
        
        Args:
            tokenAddress: Token contract address
            timeframe: Timeframe (30min, 1h, 4h)
            unixtime: Unix timestamp of the candle
            avwapValue: AVWAP value as Decimal
            
        Returns:
            Dict formatted for candle update
        """
        return {
            TradingHandlerConstants.OHLCVDetails.TOKEN_ADDRESS: tokenAddress,
            TradingHandlerConstants.OHLCVDetails.TIMEFRAME: timeframe,
            TradingHandlerConstants.OHLCVDetails.UNIX_TIME: unixtime,
            TradingHandlerConstants.OHLCVDetails.AVWAP_VALUE: avwapValue
        }
    
