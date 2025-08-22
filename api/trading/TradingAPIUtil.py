from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional
from logs.logger import get_logger

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
        - ema21, ema34 with per-timeframe reference times for old tokens, but its not done in this function
        """
        if not data:
            return False, 'No JSON data provided', {}

        # Extract and validate required fields
        tokenAddress = data.get('tokenAddress', '').strip()
        pairAddress = data.get('pairAddress', '').strip()
        ema21Data = data.get('ema21')
        ema34Data = data.get('ema34')
        addedBy = data.get('addedBy', 'api_user')

        # Validate basic required fields
        if not all([tokenAddress, pairAddress]):
            return False, 'Missing required fields: tokenAddress, pairAddress', {}

        return True, '', {
            'tokenAddress': tokenAddress,
            'pairAddress': pairAddress,
            'ema21Data': ema21Data,
            'ema34Data': ema34Data,
            'addedBy': addedBy
        }

    @staticmethod
    def validatePerTimeframeEMAData(emaData: Dict, emaType: str) -> Tuple[bool, str]:
        """
        Validate per-timeframe EMA data structure
        
        Expected format:
        {
            "15m": {"value": 1.25, "referenceTime": "10:30 AM"},
            "1h": {"value": 1.28, "referenceTime": "10 AM"},
            "4h": {"value": 1.30, "referenceTime": "8 AM"}
        }
        """
        if not isinstance(emaData, dict):
            return False, f'{emaType} must be an object with per-timeframe data'
        
        requiredTimeframes = ['15m', '1h', '4h']
        
        for timeframe in requiredTimeframes:
            if timeframe not in emaData:
                return False, f'Missing {timeframe} timeframe data in {emaType}'
            
            timeframeData = emaData[timeframe]
            
            if not isinstance(timeframeData, dict):
                return False, f'{emaType}.{timeframe} must be an object with "value" and "referenceTime"'
            
            # Validate EMA value
            if 'value' not in timeframeData:
                return False, f'Missing "value" field in {emaType}.{timeframe}'
            
            if not isinstance(timeframeData['value'], (int, float)) or timeframeData['value'] <= 0:
                return False, f'Invalid EMA value in {emaType}.{timeframe}. Must be a positive number'
            
            # Validate reference time
            if 'referenceTime' not in timeframeData:
                return False, f'Missing "referenceTime" field in {emaType}.{timeframe}'
            
            if not isinstance(timeframeData['referenceTime'], str) or not timeframeData['referenceTime'].strip():
                return False, f'Invalid referenceTime in {emaType}.{timeframe}. Must be a non-empty string'
        
        return True, ''

    @staticmethod
    def validateOldTokenRequirements(pairAgeInDays: float, ema21Data, ema34Data) -> Tuple[bool, str, Optional[Dict]]:
        """
        Validate and process per-timeframe EMA requirements for old tokens
        
        Returns:
            Tuple: (success, error_message, processed_ema_data)
        """
        if not all([ema21Data, ema34Data]):
            return False, f'Token is {pairAgeInDays:.1f} days old. For old tokens, please provide EMA data with per-timeframe reference times', None

        # Validate EMA21 data structure
        isValid, errorMsg = TradingAPIUtil.validatePerTimeframeEMAData(ema21Data, 'ema21')
        if not isValid:
            return False, errorMsg, None

        # Validate EMA34 data structure  
        isValid, errorMsg = TradingAPIUtil.validatePerTimeframeEMAData(ema34Data, 'ema34')
        if not isValid:
            return False, errorMsg, None

        # Parse and validate reference times for all timeframes
        processedEMAData = {
            'ema21Values': {},
            'ema34Values': {},
            'referenceTimes': {}
        }

        for timeframe in ['15m', '1h', '4h']:
            # Parse EMA21 reference time
            ema21TimeStr = ema21Data[timeframe]['referenceTime']
            success, errorMsg, unixTime = TradingAPIUtil.parseUserFriendlyTime(ema21TimeStr)
            if not success:
                return False, f'Invalid ema21 referenceTime for {timeframe}: {errorMsg}', None
            
            processedEMAData['ema21Values'][timeframe] = ema21Data[timeframe]['value']
            processedEMAData['referenceTimes'][f'{timeframe}_ema21'] = unixTime
            
            # Parse EMA34 reference time
            ema34TimeStr = ema34Data[timeframe]['referenceTime'] 
            success, errorMsg, unixTime = TradingAPIUtil.parseUserFriendlyTime(ema34TimeStr)
            if not success:
                return False, f'Invalid ema34 referenceTime for {timeframe}: {errorMsg}', None
            
            processedEMAData['ema34Values'][timeframe] = ema34Data[timeframe]['value']
            processedEMAData['referenceTimes'][f'{timeframe}_ema34'] = unixTime

        return True, '', processedEMAData

    @staticmethod
    def convertToPerTimeframeFormat(processedEMAData: Dict) -> Dict:
        """
        Convert processed EMA data to the per-timeframe format expected by EMAProcessor
        
        Input format (from validateOldTokenRequirements):
        {
            'ema21Values': {'15m': 1.25, '1h': 1.28, '4h': 1.30},
            'ema34Values': {'15m': 1.22, '1h': 1.24, '4h': 1.26},
            'referenceTimes': {'15m_ema21': 1234567890, '15m_ema34': 1234567890, ...}
        }
        
        Output format (for EMAProcessor):
        {
            '15m': {'ema21': {'value': 1.25, 'referenceTime': 1234567890}, 'ema34': {'value': 1.22, 'referenceTime': 1234567890}},
            '1h': {'ema21': {'value': 1.28, 'referenceTime': 1234567890}, 'ema34': {'value': 1.24, 'referenceTime': 1234567890}},
            '4h': {'ema21': {'value': 1.30, 'referenceTime': 1234567890}, 'ema34': {'value': 1.26, 'referenceTime': 1234567890}}
        }
        """
        perTimeframeFormat = {}
        
        for timeframe in ['15m', '1h', '4h']:
            timeframeData = {}
            
            # Add EMA21 data if available
            if timeframe in processedEMAData['ema21Values']:
                timeframeData['ema21'] = {
                    'value': processedEMAData['ema21Values'][timeframe],
                    'referenceTime': processedEMAData['referenceTimes'][f'{timeframe}_ema21']
                }
            
            # Add EMA34 data if available
            if timeframe in processedEMAData['ema34Values']:
                timeframeData['ema34'] = {
                    'value': processedEMAData['ema34Values'][timeframe],
                    'referenceTime': processedEMAData['referenceTimes'][f'{timeframe}_ema34']
                }
            
            if timeframeData:  # Only add if we have data for this timeframe
                perTimeframeFormat[timeframe] = timeframeData
        
        return perTimeframeFormat

    @staticmethod
    def formatSuccessResponse(tokenAddition: Dict, tokenAddress: str, pairAddress: str, 
                            pairAgeInDays: float, tokenInfoFromAPI) -> Dict:
        """Format successful token addition response"""
        return {
            'success': True,
            'tokenId': tokenAddition['tokenId'],
            'tokenAddress': tokenAddress,
            'pairAddress': pairAddress,
            'tokenAge': round(pairAgeInDays, 1),
            'mode': tokenAddition['mode'],
            'candlesInserted': tokenAddition.get('candlesInserted', 0),
            'creditsUsed': tokenAddition.get('creditsUsed', 0),
            'symbol': tokenInfoFromAPI.symbol,
            'name': tokenInfoFromAPI.name,
            'message': f'Token {tokenInfoFromAPI.symbol} added successfully ({tokenAddition["mode"]})'
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