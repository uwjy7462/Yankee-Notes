import argparse
import datetime
from loguru import logger
from utils.message_utils import get_universal_posts, get_latest_universal_db_timestamp

def sync_universal_run(since_date: str = None, before_date: str = None):
    """
    同步 Universal Posts 历史数据
    :param since_date: 截止日期 (YYYY-MM-DD HH:MM:SS)，拉取到该时间为止。
    :param before_date: 起始日期 (YYYY-MM-DD HH:MM:SS)，从该时间开始往回拉取。
    """
    stop_at = 0
    # 注意：Universal Posts 使用 Cursor 分页，但我们仍然可以用 stop_at_timestamp 来控制停止
    
    if since_date:
        logger.info(f"开始同步 Universal 历史数据: 目标截止到 {since_date}")
        try:
            try:
                dt = datetime.datetime.strptime(since_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                dt = datetime.datetime.strptime(since_date, "%Y-%m-%d")
            
            if dt.tzinfo is None:
                dt = dt.astimezone(datetime.timezone.utc)
            
            stop_at = int(dt.timestamp() * 1000)
        except ValueError:
            logger.error("截止日期格式错误，请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
            return
    else:
        logger.info("未指定截止日期，进入 [自动追赶模式] (Catch-up Mode)")
        latest_ts = get_latest_universal_db_timestamp()
        if latest_ts > 0:
            stop_at = latest_ts + 1
            t_readable = datetime.datetime.fromtimestamp(stop_at/1000.0).strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"检测到数据库最新 Universal 消息时间: {t_readable}，将同步在此之后的所有新消息...")
        else:
            logger.warning("数据库为空，默认同步最近 7 天数据...")
            stop_at = int((datetime.datetime.now() - datetime.timedelta(days=7)).timestamp() * 1000)

    HUGE_LIMIT = 1000000
    MAX_PAGES = 10000
    
    try:
        items = get_universal_posts(
            limit=HUGE_LIMIT, 
            stop_at_timestamp=stop_at, 
            max_api_requests=MAX_PAGES,
            accumulate_results=False
        )
        
        logger.info(f"Universal 同步完成，本次共处理 {len(items)} 条消息")
            
    except Exception as e:
        logger.error(f"Universal 同步过程中发生错误: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Whop Universal Posts 数据同步工具')
    parser.add_argument('--since', type=str, required=False, help='截止日期 (YYYY-MM-DD HH:MM:SS)。如果不传，则自动同步 DB 最新数据之后的所有消息。')
    parser.add_argument('--before', type=str, required=False, help='起始日期 (YYYY-MM-DD HH:MM:SS)。目前 Universal 接口主要支持从最新往回拉。')
    
    args = parser.parse_args()
    
    sync_universal_run(since_date=args.since, before_date=args.before)
