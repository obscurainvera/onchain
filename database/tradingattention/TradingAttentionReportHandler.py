from config.Config import get_config
from database.operations.BaseDBHandler import BaseDBHandler
from database.operations.DatabaseConnectionManager import DatabaseConnectionManager
from typing import List, Dict, Optional, Any
from decimal import Decimal
import sqlite3
from logs.logger import get_logger
from datetime import datetime, timedelta
import json
from sqlalchemy import text

logger = get_logger(__name__)

class TradingAttentionReportHandler(BaseDBHandler):
    """
    Highly optimized handler for trading attention report operations.
    Creates reports showing latest tokens in attention with 7-day history.
    Optimized for cloud deployment with minimal egress and maximum performance.
    """
    
    def __init__(self, conn_manager=None):
        if conn_manager is None:
            conn_manager = DatabaseConnectionManager()
        super().__init__(conn_manager)
    
    def getTradingAttentionReport(self) -> List[Dict[str, Any]]:
        """
        Get highly optimized trading attention report with 7-day history.
        
        Single mega-query approach for maximum performance:
        1. Find latest date from trading attention table
        2. Get all tokens for that date with their attention data
        3. Get 7-day history with min/max scores and prices per day
        4. Calculate price differences and percentages
        
        Returns:
            List of dictionaries with the exact format specified:
            [
                {
                    "name": coin name,
                    "token_id": token id,
                    "market_cap": market cap,
                    "token_age": token age,
                    "attention_data": [
                        {
                            "day": 1,
                            "date": "01-08-2025",
                            "min_score": min score,
                            "max_score": max score,
                            "min_score_time": time using createdat,
                            "max_score_time": time using createdat,
                            "low_price": low price,
                            "high_price": high price,
                            "low_price_time": low price time,
                            "high_price_time": high price time,
                            "difference": price difference between low and high
                        }
                    ]
                }
            ]
        """
        try:
            config = get_config()
            logger.info(f"Starting getTradingAttentionReport with DB_TYPE: {config.DB_TYPE}")
            
            with self.transaction() as cursor:
                # Fixed and optimized query without portsummary dependency
                if config.DB_TYPE == 'postgres':
                    mega_query = """
                    WITH latest_date AS (
                        -- Get the latest date from trading attention
                        SELECT MAX(date) as max_date
                        FROM tradingattention
                    ),
                    latest_tokens AS (
                        -- Get all tokens that are in attention for the latest date
                        SELECT ta.tokenid, ta.name
                        FROM tradingattention ta
                        CROSS JOIN latest_date ld
                        WHERE ta.date = ld.max_date
                    ),
                    date_series AS (
                        -- Generate 7-day date series from latest date as strings
                        SELECT 
                            ld.max_date as date_str,
                            1 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            TO_CHAR(TO_DATE(ld.max_date, 'YYYY-MM-DD') - INTERVAL '1 day', 'YYYY-MM-DD') as date_str,
                            2 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            TO_CHAR(TO_DATE(ld.max_date, 'YYYY-MM-DD') - INTERVAL '2 days', 'YYYY-MM-DD') as date_str,
                            3 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            TO_CHAR(TO_DATE(ld.max_date, 'YYYY-MM-DD') - INTERVAL '3 days', 'YYYY-MM-DD') as date_str,
                            4 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            TO_CHAR(TO_DATE(ld.max_date, 'YYYY-MM-DD') - INTERVAL '4 days', 'YYYY-MM-DD') as date_str,
                            5 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            TO_CHAR(TO_DATE(ld.max_date, 'YYYY-MM-DD') - INTERVAL '5 days', 'YYYY-MM-DD') as date_str,
                            6 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            TO_CHAR(TO_DATE(ld.max_date, 'YYYY-MM-DD') - INTERVAL '6 days', 'YYYY-MM-DD') as date_str,
                            7 as day_num
                        FROM latest_date ld
                    ),
                    all_data_points AS (
                        -- Collect all data points for each token-day combination
                        SELECT 
                            lt.tokenid,
                            lt.name,
                            ds.date_str,
                            ds.day_num,
                            ta_current.score,
                            ta_current.createdat,
                            ta_current.currentprice
                        FROM latest_tokens lt
                        CROSS JOIN date_series ds
                        LEFT JOIN tradingattention ta_current ON (
                            lt.tokenid = ta_current.tokenid 
                            AND ta_current.date = ds.date_str
                        )
                        
                        UNION ALL
                        
                        SELECT 
                            lt.tokenid,
                            lt.name,
                            ds.date_str,
                            ds.day_num,
                            ta_hist.score,
                            ta_hist.createdat,
                            ta_hist.currentprice
                        FROM latest_tokens lt
                        CROSS JOIN date_series ds
                        LEFT JOIN tradingattentionhistory ta_hist ON (
                            lt.tokenid = ta_hist.tokenid 
                            AND ta_hist.date = ds.date_str
                        )
                    ),
                    daily_aggregates AS (
                        -- Find min/max prices and scores with their corresponding times
                        SELECT 
                            tokenid,
                            name,
                            date_str,
                            day_num,
                            -- Score aggregations
                            COALESCE(MIN(score), 0) as min_score,
                            COALESCE(MAX(score), 0) as max_score,
                            -- Find when min/max scores occurred (earliest time if tied)
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.score = MIN(adp.score) 
                             ORDER BY adp2.createdat LIMIT 1) as min_score_time,
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.score = MAX(adp.score) 
                             ORDER BY adp2.createdat LIMIT 1) as max_score_time,
                            -- Latest score and time (most recent record for the day)
                            (SELECT score FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.score IS NOT NULL
                             ORDER BY adp2.createdat DESC LIMIT 1) as latest_score,
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.score IS NOT NULL
                             ORDER BY adp2.createdat DESC LIMIT 1) as latest_score_time,
                            -- Price aggregations  
                            COALESCE(MIN(currentprice), 0) as low_price,
                            COALESCE(MAX(currentprice), 0) as high_price,
                            -- Find when min/max prices occurred (earliest time if tied)
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.currentprice = MIN(adp.currentprice) 
                             ORDER BY adp2.createdat LIMIT 1) as low_price_time,
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.currentprice = MAX(adp.currentprice) 
                             ORDER BY adp2.createdat LIMIT 1) as high_price_time
                        FROM all_data_points adp
                        GROUP BY tokenid, name, date_str, day_num
                    )
                    SELECT 
                        da.tokenid,
                        da.name,
                        0 as market_cap,  -- Will be filled from DexScreener
                        0 as token_age,   -- Will be filled from DexScreener
                        da.day_num,
                        da.date_str,
                        da.min_score,
                        da.max_score,
                        da.min_score_time,
                        da.max_score_time,
                        da.latest_score,
                        da.latest_score_time,
                        da.low_price,
                        da.high_price,
                        da.low_price_time,
                        da.high_price_time,
                        (da.high_price - da.low_price) as difference
                    FROM daily_aggregates da
                    ORDER BY da.tokenid, da.day_num
                    """
                    
                    logger.info("Executing PostgreSQL query...")
                    cursor.execute(text(mega_query))
                    
                else:
                    # SQLite optimized version without portsummary
                    mega_query = """
                    WITH latest_date AS (
                        SELECT MAX(date) as max_date
                        FROM tradingattention
                    ),
                    latest_tokens AS (
                        SELECT ta.tokenid, ta.name
                        FROM tradingattention ta, latest_date ld
                        WHERE ta.date = ld.max_date
                    ),
                    date_series AS (
                        SELECT 
                            ld.max_date as date_str,
                            1 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            date(ld.max_date, '-1 days') as date_str,
                            2 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            date(ld.max_date, '-2 days') as date_str,
                            3 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            date(ld.max_date, '-3 days') as date_str,
                            4 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            date(ld.max_date, '-4 days') as date_str,
                            5 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            date(ld.max_date, '-5 days') as date_str,
                            6 as day_num
                        FROM latest_date ld
                        UNION ALL
                        SELECT 
                            date(ld.max_date, '-6 days') as date_str,
                            7 as day_num
                        FROM latest_date ld
                    ),
                    all_data_points AS (
                        -- Collect all data points for each token-day combination
                        SELECT 
                            lt.tokenid,
                            lt.name,
                            ds.date_str,
                            ds.day_num,
                            ta_current.score,
                            ta_current.createdat,
                            ta_current.currentprice
                        FROM latest_tokens lt
                        CROSS JOIN date_series ds
                        LEFT JOIN tradingattention ta_current ON (
                            lt.tokenid = ta_current.tokenid 
                            AND ta_current.date = ds.date_str
                        )
                        
                        UNION ALL
                        
                        SELECT 
                            lt.tokenid,
                            lt.name,
                            ds.date_str,
                            ds.day_num,
                            ta_hist.score,
                            ta_hist.createdat,
                            ta_hist.currentprice
                        FROM latest_tokens lt
                        CROSS JOIN date_series ds
                        LEFT JOIN tradingattentionhistory ta_hist ON (
                            lt.tokenid = ta_hist.tokenid 
                            AND ta_hist.date = ds.date_str
                        )
                    ),
                    daily_aggregates AS (
                        -- Find min/max prices and scores with their corresponding times (SQLite version)
                        SELECT 
                            tokenid,
                            name,
                            date_str,
                            day_num,
                            -- Score aggregations
                            COALESCE(MIN(score), 0) as min_score,
                            COALESCE(MAX(score), 0) as max_score,
                            -- Find when min/max scores occurred (earliest time if tied)
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.score = MIN(adp.score) 
                             ORDER BY adp2.createdat LIMIT 1) as min_score_time,
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.score = MAX(adp.score) 
                             ORDER BY adp2.createdat LIMIT 1) as max_score_time,
                            -- Latest score and time (most recent record for the day)
                            (SELECT score FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.score IS NOT NULL
                             ORDER BY adp2.createdat DESC LIMIT 1) as latest_score,
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.score IS NOT NULL
                             ORDER BY adp2.createdat DESC LIMIT 1) as latest_score_time,
                            -- Price aggregations
                            COALESCE(MIN(currentprice), 0) as low_price,
                            COALESCE(MAX(currentprice), 0) as high_price,
                            -- Find when min/max prices occurred (earliest time if tied)
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.currentprice = MIN(adp.currentprice) 
                             ORDER BY adp2.createdat LIMIT 1) as low_price_time,
                            (SELECT createdat FROM all_data_points adp2 
                             WHERE adp2.tokenid = adp.tokenid AND adp2.date_str = adp.date_str 
                             AND adp2.currentprice = MAX(adp.currentprice) 
                             ORDER BY adp2.createdat LIMIT 1) as high_price_time
                        FROM all_data_points adp
                        GROUP BY tokenid, name, date_str, day_num
                    )
                    SELECT 
                        da.tokenid,
                        da.name,
                        0 as market_cap,  -- Will be filled from DexScreener
                        0 as token_age,   -- Will be filled from DexScreener  
                        da.day_num,
                        da.date_str,
                        da.min_score,
                        da.max_score,
                        da.min_score_time,
                        da.max_score_time,
                        da.latest_score,
                        da.latest_score_time,
                        da.low_price,
                        da.high_price,
                        da.low_price_time,
                        da.high_price_time,
                        (da.high_price - da.low_price) as difference
                    FROM daily_aggregates da
                    ORDER BY da.tokenid, da.day_num
                    """
                    
                    logger.info("Executing SQLite query...")
                    cursor.execute(mega_query)
                
                results = cursor.fetchall()
                logger.info(f"Cursor.fetchall() returned: {len(results) if results else 0} results")
                
                # Process results into required format
                if not results:
                    logger.warning("No trading attention data found - results is empty or None")
                    return []
                
                logger.info(f"Query returned {len(results)} raw results - proceeding to process")
                
                # Define helper functions outside the loop
                def format_time(dt):
                    if dt is None:
                        return None
                    if isinstance(dt, str):
                        try:
                            dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
                        except:
                            return dt
                    return dt.strftime("%I:%M %p").lstrip('0')
                
                def format_date_with_time(date_str, latest_time):
                    """Format date and latest score time as 'Aug 1 - 2AM'"""
                    try:
                        if not date_str:
                            return ""
                        
                        # Format date part
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                        formatted_date = date_obj.strftime('%b %d').replace(' 0', ' ')
                        
                        # Format time part
                        if latest_time is None:
                            return formatted_date
                        
                        if isinstance(latest_time, str):
                            try:
                                latest_time = datetime.fromisoformat(latest_time.replace('Z', '+00:00'))
                            except:
                                return formatted_date
                        
                        formatted_time = latest_time.strftime("%I%p").lstrip('0').replace('M', 'M')
                        return f"{formatted_date} - {formatted_time}"
                        
                    except Exception as e:
                        logger.warning(f"Error formatting date with time {date_str}, {latest_time}: {e}")
                        return date_str if date_str else ""
                
                def format_market_cap(value):
                    if not value or value == 0:
                        return "0"
                    
                    if value >= 1_000_000_000:  # Billions
                        formatted = value / 1_000_000_000
                        return f"{formatted:.1f}B".rstrip('0').rstrip('.')
                    elif value >= 1_000_000:  # Millions
                        formatted = value / 1_000_000
                        return f"{formatted:.1f}M".rstrip('0').rstrip('.')
                    elif value >= 1_000:  # Thousands
                        formatted = value / 1_000
                        return f"{formatted:.1f}K".rstrip('0').rstrip('.')
                    else:
                        return f"{value:.0f}"
                
                def calculate_percentage_increase(low, high):
                    if not low or low == 0:
                        return 0
                    return round(((high - low) / low) * 100, 2)
                
                # Group by token_id
                token_data = {}
                logger.info(f"Starting to process {len(results)} rows")
                for i, row in enumerate(results):
                    try:
                        # Add debug logging for the first few rows
                        if i < 3:
                            logger.info(f"Processing row {i}: {row}")
                        
                        # Access by column name since it's a RealDictRow
                        # Handle Decimal and other types properly
                        token_id = str(row['tokenid']) if row['tokenid'] is not None else ""
                        name = str(row['name']) if row['name'] is not None else ""
                        market_cap = float(row['market_cap']) if row['market_cap'] is not None else 0
                        token_age = int(row['token_age']) if row['token_age'] is not None else 0
                        day_num = int(row['day_num']) if row['day_num'] is not None else 0
                        date_str = str(row['date_str']) if row['date_str'] is not None else ""
                        
                        # Handle Decimal types from database
                        min_score = float(row['min_score']) if row['min_score'] is not None else 0
                        max_score = float(row['max_score']) if row['max_score'] is not None else 0
                        low_price = float(row['low_price']) if row['low_price'] is not None else 0
                        high_price = float(row['high_price']) if row['high_price'] is not None else 0
                        
                        # Handle datetime objects
                        min_score_time = row['min_score_time']
                        max_score_time = row['max_score_time']
                        latest_score = float(row['latest_score']) if row['latest_score'] is not None else max_score
                        latest_score_time = row['latest_score_time']
                        low_price_time = row['low_price_time']
                        high_price_time = row['high_price_time']
                        
                        # Skip rows with invalid data
                        if not token_id or not name:
                            logger.warning(f"Skipping row {i} due to missing token_id or name")
                            continue
                        
                        if token_id not in token_data:
                            token_data[token_id] = {
                                "name": name,
                                "token_id": token_id,
                                "market_cap": market_cap,
                                "market_cap_formatted": format_market_cap(market_cap),
                                "token_age": token_age,
                                "attention_data": []
                            }
                        
                        # Format date to DD-MM-YYYY (original format)
                        try:
                            if date_str:
                                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                                formatted_date = date_obj.strftime('%d-%m-%Y')
                            else:
                                formatted_date = ""
                        except Exception as date_error:
                            logger.warning(f"Error formatting date {date_str}: {date_error}")
                            formatted_date = date_str
                        
                        # Format latest_score_time with date as "Aug 1 - 2AM"
                        formatted_latest_time = format_date_with_time(date_str, latest_score_time)
                        
                        token_data[token_id]["attention_data"].append({
                            "day": day_num,
                            "date": formatted_date,
                            "min_score": min_score,
                            "max_score": max_score,
                            "min_score_time": format_time(min_score_time),
                            "max_score_time": format_time(max_score_time),
                            "latest_score": latest_score,
                            "latest_score_time": formatted_latest_time,
                            "low_price": low_price,
                            "high_price": high_price,
                            "low_price_time": format_time(low_price_time),
                            "high_price_time": format_time(high_price_time),
                            "difference": calculate_percentage_increase(low_price, high_price)
                        })
                        
                    except Exception as row_error:
                        logger.error(f"Error processing row {i}: {row_error}. Row data: {dict(row)}", exc_info=True)
                        continue
                
                # Convert to list and sort attention_data by day
                result_list = []
                for token_id, data in token_data.items():
                    try:
                        data["attention_data"].sort(key=lambda x: x["day"])
                        result_list.append(data)
                    except Exception as sort_error:
                        logger.error(f"Error sorting data for token {token_id}: {sort_error}")
                        continue
                
                logger.info(f"Generated trading attention report for {len(result_list)} tokens with 7-day history")
                logger.info(f"Final result contains {len(result_list)} tokens")
                return result_list
                
        except Exception as e:
            logger.error(f"Error generating trading attention report: {str(e)}", exc_info=True)
            return []
    
    def getAllTokenIds(self) -> List[str]:
        """
        Get all token IDs from the latest attention data.
        All tokens will need market cap data from DexScreener since we don't use portsummary.
        
        Returns:
            List of token IDs that need market cap from external source
        """
        try:
            with self.transaction() as cursor:
                query = """
                WITH latest_date AS (
                    SELECT MAX(date) as max_date
                    FROM tradingattention
                )
                SELECT DISTINCT ta.tokenid
                FROM tradingattention ta
                CROSS JOIN latest_date ld
                WHERE ta.date = ld.max_date
                """
                
                cursor.execute(text(query))
                results = cursor.fetchall()
                token_ids = [row[0] for row in results]
                
                logger.info(f"Found {len(token_ids)} tokens needing market cap data")
                return token_ids
                
        except Exception as e:
            logger.error(f"Error getting token IDs: {str(e)}")
            return []