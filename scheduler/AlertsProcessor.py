"""
AlertsProcessor - Handles alert calculations, trend detection, and status encoding

This processor implements sophisticated logic for:
1. Trend calculation (bullish/bearish crosses)
2. Status encoding (band order and price position)
3. Touch detection and counting
4. Alert state management
"""

from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from decimal import Decimal
from database.auth.ChatCredentialsEnum import ChatCredentials
from logs.logger import get_logger
from api.trading.request import Alert, TrendType
from utils.CommonUtil import CommonUtil
from scheduler.AlertsProcessorTypes import BandInfo, PriceInterval, IntervalType, BandType, PositionType
from notification.handlers.BullishCrossNotification import BullishCrossNotification
from notification.handlers.BearishCrossNotification import BearishCrossNotification
from notification.handlers.BandTouchNotification import BandTouchNotification
from notification.handlers.AVWAPBreakoutNotification import AVWAPBreakoutNotification
from notification.handlers.AVWAPBreakdownNotification import AVWAPBreakdownNotification
from notification.handlers.StochRSIOversoldNotification import StochRSIOversoldNotification
from notification.handlers.StochRSIOverboughtNotification import StochRSIOverboughtNotification
from config.AVWAPPricePositionEnum import AVWAPPricePosition
from constants.BullishCrossConstants import StochRSIOversoldDefaults, StochRSIOverboughtDefaults
from notification.NotificationType import NotificationType

if TYPE_CHECKING:
    from database.trading.TradingHandler import TradingHandler
    from api.trading.request import TrackedToken, TimeframeRecord, OHLCVDetails

logger = get_logger(__name__)


class AlertsProcessor:
    """
    Production-ready alerts processor for technical indicator signals
    """
    
    def __init__(self, tradingHandler: 'TradingHandler'):
        self.tradingHandler = tradingHandler
        self.TOUCH_THRESHOLD_SECONDS = 7200  # 2 hours
    
    def calculateTrend(self, fastEMA: Optional[float], slowEMA: Optional[float]) -> str:
        if fastEMA is None:
            return TrendType.NEUTRAL.value
        
        if (fastEMA >= slowEMA) or (slowEMA is None):
            return TrendType.BULLISH.value
        elif fastEMA < slowEMA:
            return TrendType.BEARISH.value
        else:
            return TrendType.NEUTRAL.value
    
    def processEMANotification(self, existingAlert: 'Alert', candle: 'OHLCVDetails', 
                                previousTrend: Optional[str], currentTrend: Optional[str],
                                previousTrend12: Optional[str], currentTrend12: Optional[str],
                                trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord',
                                trendType: str = 'ema23') -> None:
    
        tokenAddress = trackedToken.tokenAddress
        # Process EMA 21/34 notifications (default)
        if trendType == 'ema23' and previousTrend and currentTrend:
            if existingAlert.isBullishCross(previousTrend, currentTrend):
                # Bullish cross detected - send notification (EMA 21/34)
                existingAlert.latestTouchUnix = candle.unixTime
                existingAlert.touchCount = 0
                logger.info(f"TRADING SCHEDULER :: Bullish cross detected for {trackedToken.symbol} - {timeframeRecord.timeframe} (EMA 21/34)")
                self.sendBullishCrossNotification(ChatCredentials.BULLISH_CROSS_CHAT.value, trackedToken, timeframeRecord, candle, 21, 34)
            
            elif existingAlert.isBearishCross(previousTrend, currentTrend):
                # Bearish cross detected - send notification (EMA 21/34)
                existingAlert.resetTouch()
                logger.info(f"TRADING SCHEDULER :: Bearish cross detected for {trackedToken.symbol} - {timeframeRecord.timeframe} (EMA 21/34)")
                self.sendBearishCrossNotification(ChatCredentials.BEARISH_CROSS_CHAT.value, trackedToken, timeframeRecord, candle, 21, 34)
            
            elif currentTrend == TrendType.BULLISH.value and previousTrend != TrendType.BEARISH.value:
                # Check for EMA touches during bullish trend
                if self.isEMATouched(candle, 'EMA21', 'EMA34') and existingAlert.shouldRecordTouch(candle.unixTime, self.TOUCH_THRESHOLD_SECONDS):
                    existingAlert.recordTouch(candle.unixTime)
                    logger.info(f"TRADING SCHEDULER :: EMA touch recorded for {trackedToken.symbol} - {timeframeRecord.timeframe}")
                    # Send band touch notification (only for first and second touches)
                    self.sendBandTouchNotification(ChatCredentials.BAND_TOUCH_CHAT.value, trackedToken, timeframeRecord, candle, existingAlert, 'EMA21', 'EMA34')
        
        # Process EMA 12/21 notifications
        elif trendType == 'ema12' and previousTrend12 and currentTrend12:
            if existingAlert.isBullishCross(previousTrend12, currentTrend12):
                # Bullish cross detected for EMA 12/21
                existingAlert.latestTouchUnix12 = candle.unixTime
                existingAlert.touchCount12 = 0
                logger.info(f"TRADING SCHEDULER :: EMA 12/21 Bullish cross detected for {trackedToken.symbol} - {timeframeRecord.timeframe}")
                self.sendBullishCrossNotification(ChatCredentials.BULLISH_CROSS_CHAT.value, trackedToken, timeframeRecord, candle, 12, 21)
            
            elif existingAlert.isBearishCross(previousTrend12, currentTrend12):
                # Bearish cross detected for EMA 12/21 - send notification
                existingAlert.resetTouch12()
                logger.info(f"TRADING SCHEDULER :: EMA 12/21 Bearish cross detected for {trackedToken.symbol} - {timeframeRecord.timeframe}")
                self.sendBearishCrossNotification(ChatCredentials.BEARISH_CROSS_CHAT.value, trackedToken, timeframeRecord, candle, 12, 21)
            
            elif currentTrend12 == TrendType.BULLISH.value and previousTrend12 != TrendType.BEARISH.value:
                # Check for EMA 12/21 touches during bullish trend
                if self.isEMATouched(candle, 'EMA12', 'EMA21') and existingAlert.shouldRecordTouch(candle.unixTime, self.TOUCH_THRESHOLD_SECONDS):
                    existingAlert.recordTouch12(candle.unixTime)
                    logger.info(f"TRADING SCHEDULER :: EMA 12/21 touch recorded for {trackedToken.symbol} - {timeframeRecord.timeframe}")
                    self.sendBandTouchNotification(ChatCredentials.BAND_TOUCH_CHAT.value, trackedToken, timeframeRecord, candle, existingAlert, 'EMA12', 'EMA21')
    
    def processAVWAPNotification(self, existingAlert: 'Alert', candle: 'OHLCVDetails',
                                  trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord') -> None:
        """
        Process AVWAP breakout alert
        
        Logic:
        - Send alert once when price closes above AVWAP for the first time
        - Track state using avwapPricePosition enum
        - Reset flag when price goes back below AVWAP
        
        Args:
            existingAlert: Current alert state
            candle: Current candle being processed
            trackedToken: Token being tracked
            timeframeRecord: Timeframe record
        """
        try:
            closePrice = candle.closePrice
            avwapValue = candle.avwapValue
            
            if avwapValue is None:
                return
            
            # Check if we should send breakout alert
            if existingAlert.shouldSendAVWAPBreakoutAlert(closePrice, avwapValue):
                # Price crossed above AVWAP - send alert
                existingAlert.markPriceAboveAVWAP()
                logger.info(f"TRADING SCHEDULER :: AVWAP Breakout detected for {trackedToken.symbol} - {timeframeRecord.timeframe}: "
                           f"Close={closePrice:.8f}, AVWAP={avwapValue:.8f}")
                self.sendAVWAPBreakoutNotification(ChatCredentials.AVWAP_BREAKOUT_CHAT.value, trackedToken, timeframeRecord, candle)
            
            elif closePrice < avwapValue and existingAlert.avwapPricePosition == AVWAPPricePosition.ABOVE_AVWAP.positionCode:
                # Price went back below AVWAP - send breakdown alert and reset flag
                existingAlert.markPriceBelowAVWAP()
                logger.info(f"TRADING SCHEDULER :: AVWAP Breakdown detected for {trackedToken.symbol} - {timeframeRecord.timeframe}: "
                           f"Close={closePrice:.8f}, AVWAP={avwapValue:.8f}")
                self.sendAVWAPBreakdownNotification(ChatCredentials.AVWAP_BREAKDOWN_CHAT.value, trackedToken, timeframeRecord, candle)
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error processing AVWAP breakout alert: {e}", exc_info=True)
    
    def processStochRSIOversoldAlert(self, candle: 'OHLCVDetails', 
                                     currentTrend: str, trackedToken: 'TrackedToken', 
                                     timeframeRecord: 'TimeframeRecord', 
                                     shortEmaLabel: str, longEmaLabel: str) -> None:
        """
        Process Stochastic RSI oversold confluence alert
        
        Conditions:
        1. Must be in bullish trend
        2. Price must touch either short or long EMA
        3. Both %K and %D must be below oversold thresholds
        
        Args:
            candle: OHLCV candle with price and indicator data
            currentTrend: Current trend for the EMA pair being checked
            trackedToken: Token being tracked
            timeframeRecord: Timeframe record
            shortEmaLabel: Label for short EMA (e.g., "EMA12", "EMA21")
            longEmaLabel: Label for long EMA (e.g., "EMA21", "EMA34")
        """
        try:
            # Condition 1: Must be in bullish trend
            if currentTrend != TrendType.BULLISH.value:
                return
            
            # Condition 3: Check if RSI indicators are available and oversold
            if candle.stochRSIK is None or candle.stochRSID is None:
                return
            
            kValue = float(candle.stochRSIK)
            dValue = float(candle.stochRSID)
            
            # Check if both %K and %D are below thresholds
            isOversold = (kValue < StochRSIOversoldDefaults.K_OVERSOLD_THRESHOLD and 
                         dValue < StochRSIOversoldDefaults.D_OVERSOLD_THRESHOLD)
            
            if not isOversold:
                return
            
            # Get EMA values from candle using labels
            shortEmaValue = self.getEmaValueFromLabel(candle, shortEmaLabel)
            longEmaValue = self.getEmaValueFromLabel(candle, longEmaLabel)
            
            # Condition 2: Check if price touched either band
            lowPrice = float(candle.lowPrice)
            highPrice = float(candle.highPrice)
            
            touchedBand = None
            bandValue = None
            
            # Check short EMA touch
            if shortEmaValue is not None:
                if self.didPriceTouch(lowPrice, highPrice, shortEmaValue):
                    touchedBand = shortEmaLabel
                    bandValue = shortEmaValue
            
            # Check long EMA touch (if short EMA wasn't touched)
            if touchedBand is None and longEmaValue is not None:
                if self.didPriceTouch(lowPrice, highPrice, longEmaValue):
                    touchedBand = longEmaLabel
                    bandValue = longEmaValue
            
            if touchedBand is None:
                return
            
            # All 3 conditions met - send alert!
            logger.info(f"TRADING SCHEDULER :: Stochastic RSI Oversold Setup detected for {trackedToken.symbol} - {timeframeRecord.timeframe}: "
                       f"Trend=BULLISH, Touched={touchedBand}, K={kValue:.2f}, D={dValue:.2f}")
            self.sendStochRSIOversoldNotification(
                ChatCredentials.STOCH_RSI_OVERSOLD_CHAT.value, 
                trackedToken, 
                timeframeRecord, 
                candle, 
                touchedBand, 
                bandValue,
                shortEmaLabel,
                longEmaLabel
            )
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error processing Stochastic RSI oversold alert: {e}", exc_info=True)
    
    def processStochRSIOverboughtAlert(self, candle: 'OHLCVDetails', 
                                       currentTrend: str, trackedToken: 'TrackedToken', 
                                       timeframeRecord: 'TimeframeRecord', 
                                       shortEmaLabel: str, longEmaLabel: str) -> None:
        """
        Process Stochastic RSI overbought confluence alert
        
        Conditions:
        1. Must be in bullish trend
        2. Price must touch either short or long EMA
        3. Both %K and %D must be above overbought thresholds
        
        Args:
            candle: OHLCV candle with price and indicator data
            currentTrend: Current trend for the EMA pair being checked
            trackedToken: Token being tracked
            timeframeRecord: Timeframe record
            shortEmaLabel: Label for short EMA (e.g., "EMA12", "EMA21")
            longEmaLabel: Label for long EMA (e.g., "EMA21", "EMA34")
        """
        try:
            # Condition 1: Must be in bullish trend
            if currentTrend != TrendType.BULLISH.value:
                return
            
            # Condition 3: Check if RSI indicators are available and overbought
            if candle.stochRSIK is None or candle.stochRSID is None:
                return
            
            kValue = float(candle.stochRSIK)
            dValue = float(candle.stochRSID)
            
            # Check if both %K and %D are above thresholds
            isOverbought = (kValue > StochRSIOverboughtDefaults.K_OVERBOUGHT_THRESHOLD and 
                           dValue > StochRSIOverboughtDefaults.D_OVERBOUGHT_THRESHOLD)
            
            if not isOverbought:
                return
            
            # Get EMA values from candle using labels
            shortEmaValue = self.getEmaValueFromLabel(candle, shortEmaLabel)
            longEmaValue = self.getEmaValueFromLabel(candle, longEmaLabel)
            
            # Condition 2: Check if price touched either band
            lowPrice = float(candle.lowPrice)
            highPrice = float(candle.highPrice)
            
            touchedBand = None
            bandValue = None
            
            # Check short EMA touch
            if shortEmaValue is not None:
                if self.didPriceTouch(lowPrice, highPrice, shortEmaValue):
                    touchedBand = shortEmaLabel
                    bandValue = shortEmaValue
            
            # Check long EMA touch (if short EMA wasn't touched)
            if touchedBand is None and longEmaValue is not None:
                if self.didPriceTouch(lowPrice, highPrice, longEmaValue):
                    touchedBand = longEmaLabel
                    bandValue = longEmaValue
            
            if touchedBand is None:
                return
            
            # All 3 conditions met - send alert!
            logger.info(f"TRADING SCHEDULER :: Stochastic RSI Overbought Setup detected for {trackedToken.symbol} - {timeframeRecord.timeframe}: "
                       f"Trend=BULLISH, Touched={touchedBand}, K={kValue:.2f}, D={dValue:.2f}")
            self.sendStochRSIOverboughtNotification(
                ChatCredentials.STOCH_RSI_OVERBOUGHT_CHAT.value, 
                trackedToken, 
                timeframeRecord, 
                candle, 
                touchedBand, 
                bandValue,
                shortEmaLabel,
                longEmaLabel
            )
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error processing Stochastic RSI overbought alert: {e}", exc_info=True)
    
    def getEmaValueFromLabel(self, candle: 'OHLCVDetails', emaLabel: str) -> Optional[float]:
        """
        Get EMA value from candle using EMA label
        
        Args:
            candle: OHLCV candle with EMA values
            emaLabel: EMA label like "EMA12", "EMA21", "EMA34"
            
        Returns:
            EMA value or None if not available
        """
        emaMap = {
            'EMA12': candle.ema12Value,
            'EMA21': candle.ema21Value,
            'EMA34': candle.ema34Value
        }
        return emaMap.get(emaLabel)

    def calculateStatus(self, candle: 'OHLCVDetails', emaFastValue: Optional[float] = None, emaSlowValue: Optional[float] = None, 
                       emaFastLabel: str = 'EMA21', emaSlowLabel: str = 'EMA34') -> str:
        """
        Format: {order_of_bands}_{current_band}{position}
        
        Examples:
        - AV23_AA: Bands ordered AVWAP>VWAP>EMA21>EMA34, price closed above AVWAP
        - AV23_VB: Bands ordered AVWAP>VWAP>EMA21>EMA34, price closed below VWAP
        - VA23_2A: Bands ordered VWAP>AVWAP>EMA21>EMA34, price touched EMA21
        """
        try:
            # Extract values
            closePrice = float(candle.closePrice)
            lowPrice = float(candle.lowPrice)
            highPrice = float(candle.highPrice)
            
            # Create band list with available indicators
            bands = []
            if candle.avwapValue is not None:
                bands.append(BandInfo('AVWAP', float(candle.avwapValue)))
            if candle.vwapValue is not None:
                bands.append(BandInfo('VWAP', float(candle.vwapValue)))
            if emaFastValue is not None:
                bands.append(BandInfo(emaFastLabel, float(emaFastValue)))
            if emaSlowValue is not None:
                bands.append(BandInfo(emaSlowLabel, float(emaSlowValue)))
            
            if not bands:
                return "NONE_NA"
            
            # Sort bands by value (descending)
            bands.sort(key=lambda x: x.value, reverse=True)
            
            # Generate order code
            orderCode = ''.join([band.shortCode for band in bands])
            
            # Find price position and touches
            positionCode = self.calculatePositionCode(closePrice, lowPrice, highPrice, bands)
            
            return f"{orderCode}_{positionCode}"
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error calculating status for candle at {candle.unixTime}: {e}")
            return "ERROR_NA"
    
    def calculatePositionCode(self, closePrice: float, lowPrice: float, highPrice: float, 
                              bands: List[BandInfo]) -> str:
        """
        Calculate position code based on clear algorithm:
        1. Find which bands the price is between (higher band and lower band)
        2. Check touches in specific order: lower band first, then higher band, then neither
        3. Encode position based on touch pattern
        """
        if not bands:
            return "NA"
        
        # Step 1: Find the bands that enclose the price
        bandsEnclosingPrice = self.findBandsEnclosingPrice(closePrice, bands)
        
        # Step 2 & 3: Check touches and encode position
        return self.encodePositionBasedOnTouches(lowPrice, highPrice, bandsEnclosingPrice)
    
    def findBandsEnclosingPrice(self, closePrice: float, bands: List[BandInfo]) -> PriceInterval:
        """
        Find which bands enclose the price based on the clear algorithm:
        - If price closes above highest band: higher=infinity, lower=highest_band
        - If price closes below lowest band: higher=lowest_band, lower=0/infinity  
        - If price closes between bands: higher=upper_band, lower=lower_band
        
        Returns:
            PriceInterval: POJO with higher and lower band information
        """
        # Case 1: Price closes above all bands (higher=infinity, lower=highest_band)
        if closePrice > bands[0].value or closePrice == bands[0].value:  # Treat exact as above
            return PriceInterval(IntervalType.ABOVE_ALL, upperBand=None, lowerBand=bands[0])
        
        # Case 2: Price closes below all bands (higher=lowest_band, lower=0/infinity)
        if closePrice < bands[-1].value:
            return PriceInterval(IntervalType.BELOW_ALL, upperBand=bands[-1], lowerBand=None)
        
        # Case 3: Price closes between two bands (higher=upper_band, lower=lower_band)
        for i in range(len(bands) - 1):
            higherBand = bands[i]
            lowerBand = bands[i + 1]
            
            if lowerBand.value < closePrice <= higherBand.value:  # Exact on band treated as above
                return PriceInterval(IntervalType.BETWEEN, upperBand=higherBand, lowerBand=lowerBand)
        
        # Fallback - shouldn't happen
        return PriceInterval(IntervalType.UNKNOWN)
    
    def encodePositionBasedOnTouches(self, lowPrice: float, highPrice: float, 
                                   bandsEnclosingPrice: PriceInterval) -> str:
        """
        Encode position based on touch pattern following the clear algorithm:
        
        Case 1: Price touched lower band -> {lower_band}A (closed above lower band)
        Case 2: Price didn't touch lower band but touched higher band -> {higher_band}B (closed below higher band)
        Case 3: Price didn't touch either band -> {higher_band}BC (between bands, no touch)
        Edge case: Above all bands, no touch -> {lower_band}AC (above highest band)
        
        Args:
            lowPrice: Low price of candle
            highPrice: High price of candle  
            bandsEnclosingPrice: PriceInterval with higher and lower bands
            
        Returns:
            str: Position code
        """
        higherBand = bandsEnclosingPrice.upperBand
        lowerBand = bandsEnclosingPrice.lowerBand
        
        # Case 1: Check if price touched the lower band
        if lowerBand and self.didPriceTouch(lowPrice, highPrice, lowerBand.value):
            return f"{lowerBand.shortCode}A"
        
        # Case 2: Check if price touched the higher band  
        if higherBand and self.didPriceTouch(lowPrice, highPrice, higherBand.value):
            return f"{higherBand.shortCode}B"
        
        # Case 3: No touches
        if bandsEnclosingPrice.isAboveAll():
            # Edge case: Above all bands, no touch -> {lower_band}AC
            return f"{lowerBand.shortCode}AC"
        elif bandsEnclosingPrice.isBelowAll():
            # Below all bands, no touch -> {higher_band}BC  
            return f"{higherBand.shortCode}BC"
        elif bandsEnclosingPrice.isBetween():
            # Between bands, no touch -> {higher_band}BC
            return f"{higherBand.shortCode}BC"
        
        # Fallback
        return "NA"
    
    def didPriceTouch(self, lowPrice: float, highPrice: float, bandValue: float) -> bool:
        return lowPrice <= bandValue <= highPrice
    
    def sendBullishCrossNotification(self, chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails', shortMa: int, longMa: int) -> None:
        try:
            BullishCrossNotification.sendAlert(chatName, trackedToken, timeframeRecord, candle, shortMa, longMa)                
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in sendBullishCrossNotification for {trackedToken.symbol} - {NotificationType.BULLISH_CROSS.value}: {e}")
    
    def sendBearishCrossNotification(self, chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails', shortMa: int, longMa: int) -> None:
        try:
            BearishCrossNotification.sendAlert(chatName, trackedToken, timeframeRecord, candle, shortMa, longMa)                
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in sendBearishCrossNotification for {trackedToken.symbol} - {NotificationType.BEARISH_CROSS.value}: {e}")
    
    def sendBandTouchNotification(self, chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails', alert: 'Alert', shortEmaLabel: str, longEmaLabel: str) -> None:
        try:
            BandTouchNotification.sendAlert(chatName, trackedToken, timeframeRecord, candle, alert, shortEmaLabel, longEmaLabel)                
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in sendBandTouchNotification for {trackedToken.symbol} - {NotificationType.BAND_TOUCH.value}: {e}")
    
    def sendAVWAPBreakoutNotification(self, chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails') -> None:
        try:
            AVWAPBreakoutNotification.sendAlert(chatName, trackedToken, timeframeRecord, candle)                
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in sendAVWAPBreakoutNotification for {trackedToken.symbol} - {NotificationType.AVWAP_BREAKOUT.value}: {e}")
    
    def sendAVWAPBreakdownNotification(self, chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails') -> None:
        try:
            AVWAPBreakdownNotification.sendAlert(chatName, trackedToken, timeframeRecord, candle)                
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in sendAVWAPBreakdownNotification for {trackedToken.symbol} - {NotificationType.AVWAP_BREAKDOWN.value}: {e}")
    
    def sendStochRSIOversoldNotification(self, chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails', touchedBand: str, bandValue: float, shortEmaLabel: str, longEmaLabel: str) -> None:
        try:
            StochRSIOversoldNotification.sendAlert(chatName, trackedToken, timeframeRecord, candle, touchedBand, bandValue, shortEmaLabel, longEmaLabel)                
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in sendStochRSIOversoldNotification for {trackedToken.symbol} - {NotificationType.STOCH_RSI_OVERSOLD.value}: {e}")
    
    def sendStochRSIOverboughtNotification(self, chatName: str, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord', candle: 'OHLCVDetails', touchedBand: str, bandValue: float, shortEmaLabel: str, longEmaLabel: str) -> None:
        try:
            StochRSIOverboughtNotification.sendAlert(chatName, trackedToken, timeframeRecord, candle, touchedBand, bandValue, shortEmaLabel, longEmaLabel)                
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error in sendStochRSIOverboughtNotification for {trackedToken.symbol} - {NotificationType.STOCH_RSI_OVERBOUGHT.value}: {e}")
    
    
    def processAlertsForToken(self, trackedToken: 'TrackedToken') -> None:
        try:
            logger.info(f"TRADING SCHEDULER :: Processing alerts for token {trackedToken.symbol} started")
            
            for timeframeRecord in trackedToken.timeframeRecords: #processing timeframes in a token one by one
                alert = self.processTimeframeAlert(trackedToken,timeframeRecord)
                if alert:
                    timeframeRecord.alert = alert
            
            logger.info(f"TRADING SCHEDULER :: Processing alerts for token {trackedToken.symbol} completed")
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error processing alerts for token {trackedToken.symbol}: {e}")
    
    def processTimeframeAlert(self, trackedToken: 'TrackedToken', timeframeRecord: 'TimeframeRecord') -> Optional[Alert]:
        try:
            if not timeframeRecord.ohlcvDetails:
                logger.info(f"TRADING SCHEDULER :: No candles available for alert processing {trackedToken.symbol} - {timeframeRecord.timeframe}")
                return None
            
            logger.info(f"TRADING SCHEDULER :: Processing timeframe alert {trackedToken.symbol} - {timeframeRecord.timeframe} with {len(timeframeRecord.ohlcvDetails)} candles")
            
            existingAlert = timeframeRecord.alert if hasattr(timeframeRecord, 'alert') else None
            
            # Extract token information
            tokenAddress = trackedToken.tokenAddress
            pairAddress = trackedToken.pairAddress
            tokenId = trackedToken.trackedTokenId
            
            if not existingAlert:
                existingAlert = Alert(
                    tokenId=tokenId,
                    tokenAddress=tokenAddress,
                    pairAddress=pairAddress,
                    timeframe=timeframeRecord.timeframe
                )
            
            previousTrend = existingAlert.trend
            previousTrend12 = existingAlert.trend12
            
            # Process candles chronologically
            for candle in timeframeRecord.ohlcvDetails:
                if not self.areIndicatorsReady(candle, timeframeRecord):
                    logger.info(f"TRADING SCHEDULER :: Indicators not ready for {trackedToken.symbol} - {timeframeRecord.timeframe} - {candle.unixTime}")
                    continue
                
                
                currentTrend = self.calculateTrend(candle.ema21Value, candle.ema34Value)
                currentTrend12 = self.calculateTrend(candle.ema12Value, candle.ema21Value)
                currentStatus = self.calculateStatus(candle, candle.ema21Value, candle.ema34Value, 'EMA21', 'EMA34')
                currentStatus12 = self.calculateStatus(candle, candle.ema12Value, candle.ema21Value, 'EMA12', 'EMA21')
                
                
            
                self.processEMANotification(
                    existingAlert, candle, previousTrend, currentTrend, 
                    previousTrend12, currentTrend12, trackedToken, timeframeRecord, 'ema23'
                )
                
                
                self.processEMANotification(
                    existingAlert, candle, previousTrend, currentTrend, 
                    previousTrend12, currentTrend12, trackedToken, timeframeRecord, 'ema12'
                )

               
                self.processAVWAPNotification(existingAlert, candle, trackedToken, timeframeRecord)
                
               
                self.processStochRSIOversoldAlert(
                    candle, currentTrend, trackedToken, timeframeRecord,
                    'EMA21', 'EMA34'
                )
                
                # Process Stochastic RSI oversold confluence alert for EMA 12/21
                self.processStochRSIOversoldAlert(
                    candle, currentTrend12, trackedToken, timeframeRecord,
                    'EMA12', 'EMA21'
                )
                
                # Process Stochastic RSI overbought confluence alert for EMA 21/34
                self.processStochRSIOverboughtAlert(
                    candle, currentTrend, trackedToken, timeframeRecord,
                    'EMA21', 'EMA34'
                )
                
                # Process Stochastic RSI overbought confluence alert for EMA 12/21
                self.processStochRSIOverboughtAlert(
                    candle, currentTrend12, trackedToken, timeframeRecord,
                    'EMA12', 'EMA21'
                )
                
                # Update indicator values in alert
                existingAlert.updateIndicatorValues(
                    vwap=candle.vwapValue,
                    ema12=candle.ema12Value,
                    ema21=candle.ema21Value,
                    ema34=candle.ema34Value,
                    avwap=candle.avwapValue,
                    rsiValue=candle.rsiValue,
                    stochRSIValue=candle.stochRSIValue,
                    stochRSIK=candle.stochRSIK,
                    stochRSID=candle.stochRSID
                )
                # Update trend and status
                existingAlert.updateTrendAndStatus(currentTrend, currentStatus, candle.unixTime)
                existingAlert.updateTrendAndStatus12(currentTrend12, currentStatus12, candle.unixTime)
                
                # Update lastUpdatedUnix only if there's a change
                existingAlert.updateLastUpdatedUnix(candle.unixTime)
                
                # Update candle with trend and status
                candle.trend = currentTrend
                candle.status = currentStatus
                candle.trend12 = currentTrend12
                candle.status12 = currentStatus12
                previousTrend = currentTrend
                previousTrend12 = currentTrend12
            
            logger.info(f"TRADING SCHEDULER :: Completed timeframe alert processing {trackedToken.symbol} - {timeframeRecord.timeframe}")
            return existingAlert
            
        except Exception as e:
            logger.info(f"TRADING SCHEDULER :: Error processing timeframe alert for {trackedToken.symbol} - {timeframeRecord.timeframe}: {e}")
            return None
    
    def areIndicatorsReady(self, candle: 'OHLCVDetails', timeframeRecord: 'TimeframeRecord') -> bool:
        
        # VWAP and AVWAP must be available
        if candle.vwapValue is None or candle.avwapValue is None:
            return False

        if timeframeRecord.ema12State and timeframeRecord.ema12State.emaAvailableTime:
            if candle.unixTime >= timeframeRecord.ema12State.emaAvailableTime:
                if candle.ema12Value is None:
                    return False
        
        # Check EMA21 availability
        if timeframeRecord.ema21State and timeframeRecord.ema21State.emaAvailableTime:
            if candle.unixTime >= timeframeRecord.ema21State.emaAvailableTime:
                if candle.ema21Value is None:
                    return False
        
        # Check EMA34 availability
        if timeframeRecord.ema34State and timeframeRecord.ema34State.emaAvailableTime:
            if candle.unixTime >= timeframeRecord.ema34State.emaAvailableTime:
                if candle.ema34Value is None:
                    return False
        
        return True
    
    def isEMATouched(self, candle: 'OHLCVDetails', shortEmaLabel: str, longEmaLabel: str) -> bool:
        """
        Check if price touched either of the specified EMA bands
        
        Args:
            candle: OHLCVDetails object containing price and EMA data
            shortEmaLabel: Short EMA label (e.g., "EMA12", "EMA21")
            longEmaLabel: Long EMA label (e.g., "EMA21", "EMA34")
        
        Returns:
            bool: True if price touched either EMA band
        """
        lowPrice = float(candle.lowPrice)
        highPrice = float(candle.highPrice)
        
        # Get EMA values using the helper method
        shortEmaValue = self.getEmaValueFromLabel(candle, shortEmaLabel)
        longEmaValue = self.getEmaValueFromLabel(candle, longEmaLabel)
        
        # Check if price touched short EMA
        if shortEmaValue is not None:
            if self.didPriceTouch(lowPrice, highPrice, shortEmaValue):
                return True
        
        # Check if price touched long EMA
        if longEmaValue is not None:
            if self.didPriceTouch(lowPrice, highPrice, longEmaValue):
                return True
        
        return False
    
    def processAlertsFromScheduler(self, trackedTokens: List['TrackedToken']) -> None:
       
        logger.info(f"TRADING SCHEDULER :: Processing alerts for {len(trackedTokens)} tokens started")
        
        for trackedToken in trackedTokens: #processing tokens one by one
            self.processAlertsForToken(trackedToken)
        
        logger.info(f"TRADING SCHEDULER :: Processing alerts for {len(trackedTokens)} tokens completed")
    
    def createInitialAlerts(self, tokenAddress: str, pairAddress: str, 
                           tokenId: int, timeframes: List[str]) -> List[Alert]:
        """
        Create initial empty alerts for new token
        
        Args:
            tokenAddress: Token contract address
            pairAddress: Trading pair address
            tokenId: Tracked token ID
            timeframes: List of timeframes
            
        Returns:
            List[Alert]: Initial alerts for each timeframe
        """
        alerts = []
        
        for timeframe in timeframes:
            alert = Alert(
                tokenId=tokenId,
                tokenAddress=tokenAddress,
                pairAddress=pairAddress,
                timeframe=timeframe,
                trend=TrendType.NEUTRAL.value,
                trend12=TrendType.NEUTRAL.value,
                status=None,
                status12=None
            )
            alerts.append(alert)
        
        return alerts
