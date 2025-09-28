from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional
from constants.TradingAPIConstants import TradingAPIConstants
from logs.logger import get_logger
from constants.TradingConstants import TimeframeConstants, ValidationMessages

logger = get_logger(__name__)


class TradingAPIUtil:
    """Utility class for Trading API operations"""
    
    @staticmethod
    def parseUserFriendlyTime(timeString: str) -> Tuple[bool, str, Optional[int]]:
        """
        Parse user-friendly time strings to Unix timestamps
        
        Supported formats:
        - "10 AM", "10:30 AM", "2 PM", "14:30"
        - "10:00", "14:30", "23:45"
        - "Today 10 AM", "Yesterday 2 PM"
        
        Returns:
            Tuple: (success, error_message, unix_timestamp)
        """
        try:
            timeString = timeString.strip().upper()
            
            todaysDate = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Handle relative day indicators
            targetDate = todaysDate
            if timeString.startswith('TODAY'):
                timeString = timeString.replace('TODAY', '').strip()
            elif timeString.startswith('YESTERDAY'):
                timeString = timeString.replace('YESTERDAY', '').strip()
                targetDate = todaysDate - timedelta(days=1)
            
            # Parse AM/PM format
            if 'AM' in timeString or 'PM' in timeString:
                time_part = timeString.replace('AM', '').replace('PM', '').strip()
                is_pm = 'PM' in timeString
                
                if ':' in time_part:
                    hourStr, minuteStr = time_part.split(':')
                    hour = int(hourStr.strip())
                    minute = int(minuteStr.strip())
                else:
                    hour = int(time_part.strip())
                    minute = 0
                
                # Convert to 24-hour format
                if is_pm and hour != 12:
                    hour += 12
                elif not is_pm and hour == 12:
                    hour = 0
                    
            else:
                # Handle 24-hour format like "14:30" or "23:45"
                if ':' in timeString:
                    hourStr, minuteStr = timeString.split(':')
                    hour = int(hourStr.strip())
                    minute = int(minuteStr.strip())
                else:
                    hour = int(timeString.strip())
                    minute = 0
            
            # Validate hour and minute
            if not (0 <= hour <= 23):
                return False, f"Invalid hour: {hour}. Must be between 0-23", None
            
            if not (0 <= minute <= 59):
                return False, f"Invalid minute: {minute}. Must be between 0-59", None
            
            # Create target datetime
            targetDateTime = targetDate.replace(hour=hour, minute=minute, second=0, microsecond=0)
            unixTimestamp = int(targetDateTime.timestamp())
            
            return True, '', unixTimestamp
            
        except ValueError as e:
            return False, f"Invalid time format: {timeString}. Use formats like '10 AM', '14:30', or 'Today 2 PM'", None
        except Exception as e:
            logger.error(f"Error parsing time string '{timeString}': {e}")
            return False, f"Failed to parse time: {str(e)}", None

    @staticmethod
    def validateRequestData(data: dict) -> Tuple[bool, str, dict]:
        """
        Validate request data
        
        New format supports:
        - tokenAddress, pairAddress (required)
        - timeframes (required) - must be valid timeframes from VALID_NEW_TOKEN_TIMEFRAMES
        - ema21, ema34 with per-timeframe reference times for old tokens, but its not done in this function
        """
        if not data:
            return False, 'No JSON data provided', {}

        # Extract and validate required fields
        tokenAddress = data.get(TradingAPIConstants.RequestParameters.TOKEN_ADDRESS, '').strip()
        pairAddress = data.get(TradingAPIConstants.RequestParameters.PAIR_ADDRESS, '').strip()
        ema21Data = data.get(TradingAPIConstants.Log.EMA_21_TYPE)
        ema34Data = data.get(TradingAPIConstants.Log.EMA_34_TYPE)
        avwapData = data.get(TradingAPIConstants.Log.AVWAP_TYPE)
        addedBy = data.get(TradingAPIConstants.RequestParameters.ADDED_BY, 'api_user')
        timeframes = data.get(TradingAPIConstants.RequestParameters.TIMEFRAMES, [])

        # Validate basic required fields
        if not all([tokenAddress, pairAddress]):
            return False, 'Missing required fields: tokenAddress, pairAddress', {}

        isTimeframeCorrect = TradingAPIUtil.checkCorrectTimeframe(timeframes)
        if isTimeframeCorrect:
            return isTimeframeCorrect

        return True, '', {
            TradingAPIConstants.RequestParameters.TOKEN_ADDRESS: tokenAddress,
            TradingAPIConstants.RequestParameters.PAIR_ADDRESS: pairAddress,
            TradingAPIConstants.Log.EMA_21_TYPE: ema21Data,
            TradingAPIConstants.Log.EMA_34_TYPE: ema34Data,
            TradingAPIConstants.Log.AVWAP_TYPE: avwapData,
            TradingAPIConstants.RequestParameters.ADDED_BY: addedBy,
            TradingAPIConstants.RequestParameters.TIMEFRAMES: timeframes
        }

    @staticmethod
    def validatePerTimeframeEMAData(emaData: Dict, emaType: str) -> Tuple[bool, str]:
        """
        Validate per-timeframe EMA data structure
        
        Expected format:
        {
            "30m": {"value": 1.25, "referenceTime": "10:30 AM"},
            "1h": {"value": 1.28, "referenceTime": "10 AM"},
            "4h": {"value": 1.30, "referenceTime": "8 AM"}
        }
        """
       
    
        
        for timeframe in TradingAPIConstants.Values.REQUIRED_TIMEFRAMES:
            if timeframe not in emaData:
                return False, f'Missing {timeframe} timeframe data in {emaType}'
            
            timeframeData = emaData[timeframe]
    
            # Validate EMA value
            if 'value' not in timeframeData:
                return False, f'Missing "value" field in {emaType}.{timeframe}'
                        
            # Validate reference time
            if 'referenceTime' not in timeframeData:
                return False, f'Missing "referenceTime" field in {emaType}.{timeframe}'
        
        return True, ''

    @staticmethod
    def validateAndProcessEMAData(ema21Data, ema34Data) -> Tuple[bool, str, Optional[Dict]]:
        """
        Core function to validate and process EMA data
        
        Returns:
            Tuple: (success, error_message, processed_ema_data)
        """
        if not ema21Data or not ema34Data:
            return False, 'EMA21 and EMA34 data are required', None
        
        # Validate EMA21 data structure
        isValid, errorMsg = TradingAPIUtil.validatePerTimeframeEMAData(ema21Data, TradingAPIConstants.Log.EMA_21_TYPE)
        if not isValid:
            return False, errorMsg, None

        # Validate EMA34 data structure  
        isValid, errorMsg = TradingAPIUtil.validatePerTimeframeEMAData(ema34Data, TradingAPIConstants.Log.EMA_34_TYPE)
        if not isValid:
            return False, errorMsg, None

        # Parse and validate reference times for all timeframes
        perTimeframeEMAData = {}

        for timeframe in TradingAPIConstants.Values.REQUIRED_TIMEFRAMES:
            # Parse EMA21 reference time
            ema21TimeStr = ema21Data[timeframe][TradingAPIConstants.RequestParameters.REFERENCE_TIME]    
            success, errorMsg, unixTime = TradingAPIUtil.parseUserFriendlyTime(ema21TimeStr)
            if not success:
                return False, f'Invalid ema21 referenceTime for {timeframe}: {errorMsg}', None
            
            # Parse EMA34 reference time
            ema34TimeStr = ema34Data[timeframe][TradingAPIConstants.RequestParameters.REFERENCE_TIME] 
            success, errorMsg, unixTime34 = TradingAPIUtil.parseUserFriendlyTime(ema34TimeStr)
            if not success:
                return False, f'Invalid ema34 referenceTime for {timeframe}: {errorMsg}', None
            
            # Build per-timeframe format directly
            perTimeframeEMAData[timeframe] = {
                TradingAPIConstants.Log.EMA_21_TYPE: {
                    TradingAPIConstants.RequestParameters.VALUE: ema21Data[timeframe][TradingAPIConstants.RequestParameters.VALUE],
                    TradingAPIConstants.RequestParameters.REFERENCE_TIME: unixTime
                },
                TradingAPIConstants.Log.EMA_34_TYPE: {
                    TradingAPIConstants.RequestParameters.VALUE: ema34Data[timeframe][TradingAPIConstants.RequestParameters.VALUE],
                    TradingAPIConstants.RequestParameters.REFERENCE_TIME: unixTime34
                }
            }

        return True, '', perTimeframeEMAData

    @staticmethod
    def validateNewTokenRequirements(avwapData) -> Tuple[bool, str, Optional[Dict]]:
        """
        Wrapper function for new token validation - only validates AVWAP data
        
        Args:
            avwapData: AVWAP data (required for all tokens)
        
        Returns:
            Tuple: (success, error_message, processed_avwap_data)
        """
        # Validate AVWAP data (mandatory for new tokens)
        return TradingAPIUtil.validateAndProcessAVWAPData(avwapData)
    
    @staticmethod
    def validateOldTokenRequirements(ema21Data, ema34Data, avwapData) -> Tuple[bool, str, Optional[Dict], Optional[Dict]]:
        """
        Wrapper function for old token validation - validates both EMA and AVWAP data
        
        Args:
            ema21Data: EMA21 data (required for old tokens)
            ema34Data: EMA34 data (required for old tokens)
            avwapData: AVWAP data (required for all tokens)
        
        Returns:
            Tuple: (success, error_message, processed_ema_data, processed_avwap_data)
        """
        # Validate AVWAP data first
        isValid, errorMsg, processedAVWAPData = TradingAPIUtil.validateAndProcessAVWAPData(avwapData)
        if not isValid:
            return False, errorMsg, None, None
        
        # Validate EMA data
        isValid, errorMsg, processedEMAData = TradingAPIUtil.validateAndProcessEMAData(ema21Data, ema34Data)
        if not isValid:
            return False, errorMsg, None, None
        
        return True, '', processedEMAData, processedAVWAPData

    @staticmethod
    def validateOldTokenRequirementsAndProcessEMAData(pairAgeInDays: float, ema21Data, ema34Data) -> Tuple[bool, str, Optional[Dict]]:
        """
        Legacy function for backward compatibility - validates only EMA data for old tokens
        
        Returns:
            Tuple: (success, error_message, processed_ema_data)
        """
        return TradingAPIUtil.validateAndProcessEMAData(ema21Data, ema34Data)

    @staticmethod
    def formatSuccessResponse(tokenAddition: Dict, tokenAddress: str, pairAddress: str, 
                            pairAgeInDays: float) -> Dict:
        """Format successful token addition response"""
        return {
            'success': True,
            'tokenId': tokenAddition['tokenId'],
            'tokenAddress': tokenAddress,
            'pairAddress': pairAddress,
            'tokenAge': round(pairAgeInDays, 1),
            'candlesInserted': tokenAddition.get('candlesInserted', 0),
            'creditsUsed': tokenAddition.get('creditsUsed', 0)
        }

    @staticmethod
    def formatErrorResponse(error: str, statusCode: int = 500) -> Tuple[Dict, int]:
        """Format error response"""
        return {
            'success': False,
            'error': error
        }, statusCode

    @staticmethod
    def formatOldTokenErrorResponse(pairAgeInDays: float, customMessage: str = None) -> Tuple[Dict, int]:
        """Format error response for old tokens requiring EMA data"""
        message = customMessage or f'Token is {pairAgeInDays:.1f} days old. For old tokens, please provide EMA data with per-timeframe reference times'
        
        return {
            'success': False,
            'error': message,
            'tokenAge': round(pairAgeInDays, 1),
            'requiresEMA': True,
            'expectedFormat': {
                'ema21': {
                    '15m': {'value': 1.25, 'referenceTime': '10:30 AM'},
                    '1h': {'value': 1.28, 'referenceTime': '10 AM'},
                    '4h': {'value': 1.30, 'referenceTime': '8 AM'}
                },
                'ema34': {
                    '15m': {'value': 1.22, 'referenceTime': '10:30 AM'},
                    '1h': {'value': 1.24, 'referenceTime': '10 AM'},
                    '4h': {'value': 1.26, 'referenceTime': '8 AM'}
                }
            }
        }, 400

    @staticmethod
    def checkCorrectTimeframe(timeframes: list) -> dict:
        """
        Validate timeframes array for new token flow
    
        Args:
        timeframes: List of timeframes to validate
        
        Returns:
        dict: Error response if validation fails, None if valid
    """
        if not timeframes:
            return {
            'success': False,
            'error': ValidationMessages.TIMEFRAMES_REQUIRED,
            'validTimeframes': TimeframeConstants.VALID_NEW_TOKEN_TIMEFRAMES
        }   
    
    # Check for invalid timeframes
        invalidTimeframes = [tf for tf in timeframes if not TimeframeConstants.isCorrectTimeframe(tf)]   
        if invalidTimeframes:
            return {
            'success': False,
            'error': ValidationMessages.constructInvalidTimeframeMessage(invalidTimeframes),
            'validTimeframes': TimeframeConstants.VALID_NEW_TOKEN_TIMEFRAMES
            }
        return None  # No validation errors

    @staticmethod
    def validatePerTimeframeAVWAPData(avwapData: Dict) -> Tuple[bool, str]:
        """
        Validate per-timeframe AVWAP data structure
        
        Expected format:
        {
            "30min": {"value": "1.25", "referenceTime": "10:30 AM"},
            "1h": {"value": "1.28", "referenceTime": "10 AM"},
            "4h": {"value": "1.30", "referenceTime": "8 AM"}
        }
        """
        if not avwapData or not isinstance(avwapData, dict):
            return False, 'AVWAP data is required and must be a dictionary'
        
        # Check for all required timeframes
        for timeframe in TradingAPIConstants.Values.REQUIRED_TIMEFRAMES:
            if timeframe not in avwapData:
                return False, f'Missing AVWAP data for {timeframe} timeframe'
            
            timeframeData = avwapData[timeframe]
            if not isinstance(timeframeData, dict):
                return False, f'AVWAP {timeframe} data must be a dictionary'
            
            # Validate required fields
            if TradingAPIConstants.RequestParameters.VALUE not in timeframeData:
                return False, f'Missing AVWAP value for {timeframe} timeframe'
            
            if TradingAPIConstants.RequestParameters.REFERENCE_TIME not in timeframeData:
                return False, f'Missing AVWAP referenceTime for {timeframe} timeframe'
            
            # Validate value is numeric
            try:
                float(timeframeData[TradingAPIConstants.RequestParameters.VALUE])
            except (ValueError, TypeError):
                return False, f'AVWAP value for {timeframe} must be a valid number'
            
            # Validate referenceTime is a string
            if not isinstance(timeframeData[TradingAPIConstants.RequestParameters.REFERENCE_TIME], str):
                return False, f'AVWAP referenceTime for {timeframe} must be a string'
        
        return True, ''
    
    @staticmethod
    def validateAndProcessAVWAPData(avwapData: Dict) -> Tuple[bool, str, Optional[Dict]]:
        """
        Validate and process AVWAP data, converting reference times to Unix timestamps
        
        Returns:
            Tuple: (success, error_message, processed_avwap_data)
        """
        if not avwapData:
            return False, 'AVWAP data is required for all tokens', None
        
        # Validate AVWAP data structure
        isValid, errorMsg = TradingAPIUtil.validatePerTimeframeAVWAPData(avwapData)
        if not isValid:
            return False, errorMsg, None
        
        # Parse and validate reference times for all timeframes
        processedAVWAPData = {}
        
        for timeframe in TradingAPIConstants.Values.REQUIRED_TIMEFRAMES:
            if timeframe not in avwapData:
                continue
                
            # Parse AVWAP reference time
            avwapTimeStr = avwapData[timeframe][TradingAPIConstants.RequestParameters.REFERENCE_TIME]
            success, errorMsg, unixTime = TradingAPIUtil.parseUserFriendlyTime(avwapTimeStr)
            if not success:
                return False, f'Invalid AVWAP referenceTime for {timeframe}: {errorMsg}', None
            
            # Build processed format
            processedAVWAPData[timeframe] = {
                TradingAPIConstants.RequestParameters.VALUE: avwapData[timeframe][TradingAPIConstants.RequestParameters.VALUE],
                TradingAPIConstants.RequestParameters.REFERENCE_TIME: unixTime
            }
        
        return True, '', processedAVWAPData