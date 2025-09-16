# VWAP Calculation Guide: Step-by-Step Process

## Overview
VWAP (Volume Weighted Average Price) calculation in our scheduler follows a sophisticated session management approach that optimizes for performance while maintaining accuracy across daily trading sessions.

## Core Logic Flow

### 1. **Session Detection Logic**
For each token-timeframe pair, we determine the processing approach:

```
lastfetchedat vs sessionendunix comparison:
- lastfetchedat <= sessionendunix → SAME_DAY_UPDATE (incremental)
- lastfetchedat > sessionendunix  → NEW_DAY_RESET (fresh calculation)  
- No existing session           → NEW_SESSION (first time)
```

### 2. **VWAP Calculation Scenarios**

#### **Scenario A: NEW_SESSION (First Time)**
**Example:** New token ABC just got its first 1hr candle

```
Token: ABC, Timeframe: 1h
Situation: No existing VWAP session
Action: Calculate VWAP for ALL available candles

Steps:
1. Get all candles for ABC-1h since pair creation
2. Calculate VWAP for all candles: Σ(typical_price × volume) / Σ(volume)
3. Update ohlcvdetails with VWAP values
4. Create new vwapsessions record

Example Data:
Candles: [09:00, 10:00, 11:00, 12:00]
VWAP Calculation:
- 09:00: (10+9+9.5)/3 * 1000 = 28500, Vol=1000 → VWAP=9.5
- 10:00: Previous + (11+10+10.5)/3 * 1500 = 75750, Vol=2500 → VWAP=10.1  
- 11:00: Previous + (12+11+11.5)/3 * 2000 = 152750, Vol=4500 → VWAP=10.4
- 12:00: Previous + (13+12+12.5)/3 * 1800 = 220250, Vol=6300 → VWAP=10.5

Result: Create vwapsessions with cumulativepv=220250, cumulativevolume=6300, currentvwap=10.5
```

#### **Scenario B: SAME_DAY_UPDATE (Incremental)**
**Example:** Same day, ABC gets new 1hr candles

```
Token: ABC, Timeframe: 1h  
Current Time: 15:00
lastfetchedat: 15:00 (3 PM)
sessionendunix: 23:59:59 (same day)
Situation: 15:00 <= 23:59:59 → SAME_DAY_UPDATE

Existing Session Data:
- cumulativepv: 220250
- cumulativevolume: 6300  
- currentvwap: 10.5
- lastcandleunix: 12:00 (noon)

Steps:
1. Get NEW candles after 12:00 → [13:00, 14:00, 15:00]
2. Use EXISTING cumulative data + new candles
3. Update ohlcvdetails with new VWAP values
4. Update vwapsessions with new cumulative totals

Example Calculation:
Starting: cumulativepv=220250, cumulativevolume=6300

- 13:00: 220250 + (14+13+13.5)/3 * 1200 = 236450, Vol=7500 → VWAP=10.49
- 14:00: 236450 + (15+14+14.5)/3 * 1100 = 252425, Vol=8600 → VWAP=10.48  
- 15:00: 252425 + (16+15+15.5)/3 * 1300 = 272700, Vol=9900 → VWAP=10.48

Result: Update vwapsessions with cumulativepv=272700, cumulativevolume=9900, currentvwap=10.48
```

#### **Scenario C: NEW_DAY_RESET (Day Boundary)**
**Example:** Next day, ABC gets first candles of new trading session

```
Token: ABC, Timeframe: 1h
Current Time: Next day 09:00  
lastfetchedat: Next day 09:00
sessionendunix: Previous day 23:59:59
Situation: 09:00 > 23:59:59 → NEW_DAY_RESET

Steps:  
1. Calculate new session boundaries (00:00 to 23:59 of current day)
2. Get ALL candles for current day only
3. Calculate fresh VWAP (ignore previous day's cumulative data)
4. Update session boundaries and reset cumulative values

Example:
Previous Day Final: cumulativepv=272700, cumulativevolume=9900 (IGNORED)
New Day Candles: [09:00] 

Fresh Calculation:
- 09:00: (11+10+10.5)/3 * 1500 = 15750, Vol=1500 → VWAP=10.5

Result: Update vwapsessions with NEW session boundaries and fresh totals:
- sessionstartunix: Current day 00:00
- sessionendunix: Current day 23:59  
- cumulativepv: 15750
- cumulativevolume: 1500
- currentvwap: 10.5
```

### 3. **Database Updates**

#### **OHLCV Table Updates**
```sql
UPDATE ohlcvdetails 
SET vwapvalue = calculated_vwap, lastupdatedat = NOW()
WHERE unixtime = candle_timestamp AND iscomplete = TRUE
```

#### **VWAP Sessions Table Updates**
```sql
-- New Session
INSERT INTO vwapsessions (tokenaddress, timeframe, cumulativepv, cumulativevolume, currentvwap, ...)

-- Same Day Update  
UPDATE vwapsessions SET cumulativepv = new_total, cumulativevolume = new_vol, currentvwap = new_vwap, ...

-- New Day Reset
UPDATE vwapsessions SET sessionstartunix = new_start, sessionendunix = new_end, 
                        cumulativepv = fresh_total, cumulativevolume = fresh_vol, ...
```

### 4. **Key Performance Optimizations**

#### **Incremental Calculation (Same Day)**
```python
# EFFICIENT: Use existing cumulative data
cumulative_pv = existing_session['cumulativepv']  # Start with existing total
cumulative_volume = existing_session['cumulativevolume']

for new_candle in new_candles_only:
    typical_price = (high + low + close) / 3
    cumulative_pv += typical_price * volume  # Add only new data
    cumulative_volume += volume
    vwap = cumulative_pv / cumulative_volume
```

vs

```python
# INEFFICIENT: Recalculate everything from scratch
for all_candles_since_start_of_day:  # Much more data to process
    # Recalculate entire day's VWAP...
```

### 5. **Real-World Example Timeline**

```
Day 1:
09:00 - NEW_SESSION: ABC gets first 1hr candle → Calculate from scratch
10:00 - SAME_DAY_UPDATE: Add 10:00 candle → Use cumulative + new candle  
11:00 - SAME_DAY_UPDATE: Add 11:00 candle → Use cumulative + new candle
...
23:59 - End of Day 1

Day 2:
09:00 - NEW_DAY_RESET: Detect day change → Reset session, fresh calculation
10:00 - SAME_DAY_UPDATE: Add 10:00 candle → Use Day 2 cumulative + new candle
...
```

### 6. **Error Handling & Edge Cases**

#### **Missing Candles**
```python
if not new_candles:
    logger.debug("No new candles to process")
    return True  # Skip processing but don't fail
```

#### **Database Consistency**
```python
with transaction():  # All-or-nothing approach
    update_ohlcv_records()
    update_vwap_session()
    # If any step fails, entire transaction rolls back
```

#### **New Token Handling**
```python
if no_existing_session and first_aggregated_candle:
    # Special case: New token just got enough 15m candles for first 1hr candle
    create_new_session_with_single_candle()
```

## Summary

The VWAP calculation system optimizes performance by:
1. **Detecting session state** to choose appropriate calculation method
2. **Using incremental calculations** for same-day updates (most common case)
3. **Resetting sessions cleanly** at day boundaries
4. **Maintaining cumulative state** to avoid recalculating historical data
5. **Updating both tables atomically** to ensure data consistency

This approach handles millions of candles efficiently while maintaining mathematical accuracy and data integrity.