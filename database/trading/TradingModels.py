from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict
from decimal import Decimal
from enum import IntEnum

# ===============================================================
# ENUMS FOR TRADING SYSTEM
# ===============================================================

class TokenStatus(IntEnum):
    """Status of tracked tokens"""
    ACTIVE = 1
    DISABLED = 2

class TimeframeEnum:
    """Supported timeframes"""
    FIFTEEN_MIN = "15m"
    ONE_HOUR = "1h"
    FOUR_HOUR = "4h"

class DataSourceEnum:
    """Source of OHLCV data"""
    API = "api"
    AGGREGATED = "aggregated"

class IndicatorTypeEnum:
    """Supported indicator types"""
    EMA_21 = "ema_21"
    EMA_34 = "ema_34"
    VWAP = "vwap"

# ===============================================================
# CORE DATA MODELS
# ===============================================================

@dataclass
class TrackedToken:
    """Tracked token data structure"""
    
    # Required fields
    tokenaddress: str  # 44 character Solana address
    symbol: str       # Trading symbol (e.g., "SOL")
    name: str         # Full token name
    pairaddress: str  # DEX pair address for price data
    
    # Optional fields with defaults
    trackedtokenid: Optional[int] = None
    status: int = TokenStatus.ACTIVE
    enabledat: Optional[datetime] = None
    disabledat: Optional[datetime] = None
    createdat: Optional[datetime] = None
    lastupdatedat: Optional[datetime] = None
    addedby: Optional[str] = None
    disabledby: Optional[str] = None
    metadata: Optional[Dict] = field(default_factory=dict)

@dataclass
class TimeframeMetadata:
    """Scheduler metadata for each token/timeframe combination"""
    
    # Required fields
    tokenaddress: str
    pairaddress: str
    timeframe: str  # "15m", "1h", "4h"
    nextfetchat: datetime
    
    # Optional fields
    id: Optional[int] = None
    lastfetchedat: Optional[datetime] = None
    lastsuccessfullfetchat: Optional[datetime] = None
    fetchintervalseconds: int = 900  # 15 minutes default
    consecutivefailures: int = 0
    isactive: bool = True
    createdat: Optional[datetime] = None
    lastupdatedat: Optional[datetime] = None

@dataclass
class OHLCVCandle:
    """OHLCV candle data with validation columns"""
    
    # Required fields
    tokenaddress: str
    pairaddress: str
    timeframe: str
    unixtime: int
    openprice: Decimal
    highprice: Decimal
    lowprice: Decimal
    closeprice: Decimal
    volume: Decimal
    
    # Optional fields
    id: Optional[int] = None
    timeframeid: Optional[int] = None
    timebucket: Optional[int] = None
    vwapvalue: Optional[Decimal] = None    # Validation column
    ema21value: Optional[Decimal] = None   # Validation column
    ema34value: Optional[Decimal] = None   # Validation column
    iscomplete: bool = True
    datasource: str = DataSourceEnum.API
    createdat: Optional[datetime] = None
    lastupdatedat: Optional[datetime] = None

@dataclass
class IndicatorState:
    """Current and previous indicator values for cross detection"""
    
    # Required fields
    tokenaddress: str
    timeframe: str
    indicatorkey: str  # "ema_21", "ema_34", etc.
    
    # Optional fields
    currentvalue: Optional[Decimal] = None
    previousvalue: Optional[Decimal] = None
    candlecount: int = 0
    lastupdatedunix: Optional[int] = None
    iswarmedup: bool = False

@dataclass
class VWAPSession:
    """VWAP session data with accumulation values"""
    
    # Required fields
    tokenaddress: str
    timeframe: str
    sessionstartunix: int
    sessionendunix: int
    
    # Optional fields
    cumulativepv: Decimal = Decimal('0')       # Cumulative price × volume
    cumulativevolume: Decimal = Decimal('0')   # Cumulative volume
    lastcandleunix: Optional[int] = None
    currentvwap: Optional[Decimal] = None
    highvwap: Optional[Decimal] = None         # Session high VWAP
    lowvwap: Optional[Decimal] = None          # Session low VWAP
    candlecount: int = 0

@dataclass
class IndicatorConfig:
    """User-configurable indicator parameters per token"""
    
    # Required fields
    tokenaddress: str
    timeframe: str
    indicatortype: str  # "ema_cross"
    
    # Optional fields
    configid: Optional[int] = None
    shortperiod: int = 21
    longperiod: int = 34
    isactive: bool = True
    createdat: Optional[datetime] = None

@dataclass
class AlertCondition:
    """Alert conditions and notification settings"""
    
    # Required fields
    tokenaddress: str
    timeframe: str
    conditionname: str
    conditiontype: str  # "ema_cross_bullish", "complex"
    alertmessage: str
    
    # Optional fields
    conditionid: Optional[int] = None
    conditionjson: Optional[Dict] = field(default_factory=dict)
    telegramchatid: Optional[str] = None
    cooldownminutes: int = 60
    lasttriggeredat: Optional[datetime] = None
    isactive: bool = True
    createdat: Optional[datetime] = None

@dataclass
class AlertHistory:
    """Historical record of triggered alerts"""
    
    # Required fields
    tokenaddress: str
    timeframe: str
    message: str
    
    # Optional fields
    alertid: Optional[int] = None
    conditionid: Optional[int] = None
    triggeredat: Optional[datetime] = None
    indicatorvalues: Optional[Dict] = field(default_factory=dict)
    sentstatus: bool = False
    errordetails: Optional[str] = None

@dataclass
class APICreditTracker:
    """API usage and credit consumption tracking"""
    
    # Required fields
    apiname: str  # "birdeye"
    creditsused: int
    endpoint: str
    
    # Optional fields
    id: Optional[int] = None
    tokencount: Optional[int] = None
    usedat: Optional[datetime] = None
    dailytotal: Optional[int] = None
    monthlytotal: Optional[int] = None

@dataclass
class AggregationTracking:
    """Monitor aggregation completeness and gaps"""
    
    # Required fields
    tokenaddress: str
    sourcetimeframe: str  # "15m"
    targettimeframe: str  # "1h" or "4h"
    periodstartunix: int
    candlesrequired: int
    candlescollected: int
    
    # Optional fields
    id: Optional[int] = None
    iscomplete: bool = False
    aggregatedat: Optional[datetime] = None
    createdat: Optional[datetime] = None

# ===============================================================
# BIRDEYE API RESPONSE MODELS
# ===============================================================

@dataclass
class BirdEyeOHLCVItem:
    """Single OHLCV item from BirdEye API response"""
    
    address: str
    c: float      # close
    h: float      # high
    l: float      # low
    o: float      # open
    type: str     # "15m"
    unixTime: int
    v: float      # volume

@dataclass
class BirdEyeOHLCVResponse:
    """Complete BirdEye API response structure"""
    
    success: bool
    data: Dict[str, List[BirdEyeOHLCVItem]]

# ===============================================================
# AGGREGATION MODELS
# ===============================================================

@dataclass
class AggregationRequest:
    """Request for aggregating candles to higher timeframe"""
    
    tokenaddress: str
    sourcetimeframe: str  # "15m"
    targettimeframe: str  # "1h" or "4h" 
    periodstart: int      # Unix timestamp of period start
    periodend: int        # Unix timestamp of period end
    requiredcandles: int  # Number of candles needed (4 for 1h, 16 for 4h)

@dataclass
class AggregationResult:
    """Result of aggregation operation"""
    
    success: bool
    aggregatedcandle: Optional[OHLCVCandle] = None
    candlesfound: int = 0
    candlesrequired: int = 0
    missingcandles: List[int] = field(default_factory=list)
    errordetails: Optional[str] = None

# ===============================================================
# INDICATOR CALCULATION MODELS
# ===============================================================

@dataclass
class EMACalculationInput:
    """Input for EMA calculation"""
    
    tokenaddress: str
    timeframe: str
    period: int           # 21 or 34
    closeprice: Decimal
    unixtime: int

@dataclass
class EMACalculationResult:
    """Result of EMA calculation"""
    
    emavalue: Optional[Decimal] = None
    previousvalue: Optional[Decimal] = None
    candlecount: int = 0
    iswarmedup: bool = False
    isfirstcalculation: bool = False
    errordetails: Optional[str] = None

@dataclass
class VWAPCalculationInput:
    """Input for VWAP calculation"""
    
    tokenaddress: str
    timeframe: str
    highprice: Decimal
    lowprice: Decimal
    closeprice: Decimal
    volume: Decimal
    unixtime: int

@dataclass
class VWAPCalculationResult:
    """Result of VWAP calculation"""
    
    vwapvalue: Optional[Decimal] = None
    sessionstart: Optional[int] = None
    sessionend: Optional[int] = None
    cumulativepv: Optional[Decimal] = None
    cumulativevolume: Optional[Decimal] = None
    errordetails: Optional[str] = None

# ===============================================================
# CROSS DETECTION MODELS
# ===============================================================

@dataclass
class IndicatorCross:
    """Detected indicator cross (bullish/bearish)"""
    
    tokenaddress: str
    timeframe: str
    crosstype: str        # "bullish_cross", "bearish_cross", "no_cross"
    ema21current: Decimal
    ema21previous: Decimal
    ema34current: Decimal
    ema34previous: Decimal
    unixtime: int

@dataclass
class CrossDetectionResult:
    """Result of cross detection analysis"""
    
    crosses: List[IndicatorCross] = field(default_factory=list)
    alertsgenerated: int = 0
    errordetails: Optional[str] = None

# ===============================================================
# BACKFILL MODELS
# ===============================================================

@dataclass
class BackfillRequest:
    """Request for historical data backfill"""
    
    tokenaddress: str
    pairaddress: str
    symbol: str
    name: str
    hours: int = 168  # 7 days default
    timeframe: str = "15m"

@dataclass
class BackfillResult:
    """Result of backfill operation"""
    
    success: bool
    candlesinserted: int = 0
    candlesduplicated: int = 0
    totalcandlesprocessed: int = 0
    apicreditsused: int = 0
    timecomplete: Optional[datetime] = None
    errordetails: Optional[str] = None

# ===============================================================
# SCHEDULER CONFIGURATION
# ===============================================================

@dataclass
class SchedulerConfig:
    """Production Configuration Constants for Trading Scheduler"""
    
    # API Configuration
    API_DELAY_SECONDS: int = 2
    API_RETRY_ATTEMPTS: int = 3
    API_TIMEOUT_SECONDS: int = 30
    
    # Batch Processing Configuration
    MAX_TOKENS_PER_BATCH: int = 100
    MAX_CANDLES_MEMORY_LIMIT: int = 10000
    
    # Database Configuration
    DB_QUERY_TIMEOUT: int = 60
    DB_RETRY_ATTEMPTS: int = 2
    
    # Time Configuration
    NEW_TOKEN_BUFFER_SECONDS: int = 300  # 5 minutes
    HISTORICAL_DATA_DAYS: int = 2
    
    # Aggregation Configuration
    AGGREGATION_TIMEFRAMES: List[str] = field(default_factory=lambda: ['1h', '4h'])
    MIN_CANDLES_FOR_AGGREGATION: int = 1


