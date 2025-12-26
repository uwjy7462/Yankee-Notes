import sys
import os
import time
import datetime
import argparse
from loguru import logger

# Add backend directory to sys.path to ensure imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from whop_summary import summary_run
from utils.market_date import get_trading_window_cn_offset
from utils import check_summary_exists_by_date

def backfill(days: int, start_offset: int = 1, force: bool = False):
    """
    Backfill summaries for the past 'days' days.
    
    Args:
        days: Number of days to backfill.
        start_offset: Start backfilling from this offset (1 = yesterday).
        force: If True, regenerate even if summary exists.
    """
    logger.info(f"Starting backfill for {days} days (offset {start_offset} to {start_offset + days - 1})...")
    
    success_count = 0
    fail_count = 0
    
    for i in range(days):
        offset = start_offset + i
        
        # Calculate window for this offset
        start_ms, end_ms, desc = get_trading_window_cn_offset(offset)
        
        start_str = datetime.datetime.fromtimestamp(start_ms/1000).strftime('%Y-%m-%d %H:%M')
        end_str = datetime.datetime.fromtimestamp(end_ms/1000).strftime('%Y-%m-%d %H:%M')
        
        # Check if already exists
        date_str_for_check = datetime.datetime.fromtimestamp(start_ms/1000).strftime('%Y.%m.%d')
        if not force and check_summary_exists_by_date(date_str_for_check):
            logger.warning(f"Offset {offset}: Summary for {date_str_for_check} already exists. Skipping (use --force to override).")
            continue

        logger.info(f"Processing Offset {offset}: {start_str} -> {end_str} ({desc})")
        
        try:
            # Run summary generation
            # We use a slightly modified title to indicate it's a backfill or just standard
            result = summary_run(start_ms, end_ms, "美股交易日复盘 (补全)", desc)
            
            if result:
                logger.success(f"Offset {offset} completed successfully.")
                success_count += 1
            else:
                logger.warning(f"Offset {offset} skipped (no data or other reason).")
                fail_count += 1
                
        except Exception as e:
            logger.error(f"Offset {offset} failed with error: {e}")
            fail_count += 1
            
        # Sleep briefly to avoid hitting rate limits too hard if any
        time.sleep(2)
        
    logger.info(f"Backfill finished. Success: {success_count}, Skipped/Failed: {fail_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Whop Summaries")
    parser.add_argument("--days", type=int, default=30, help="Number of days to backfill (default: 30)")
    parser.add_argument("--offset", type=int, default=1, help="Start offset (default: 1, i.e., yesterday)")
    parser.add_argument("--force", action="store_true", help="Force regenerate even if exists")
    
    args = parser.parse_args()
    
    backfill(args.days, args.offset, args.force)
