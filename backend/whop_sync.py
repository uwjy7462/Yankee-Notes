
import argparse
import time
import datetime
from loguru import logger
from utils.message_utils import get_history_posts, get_latest_db_timestamp

def sync_run(since_date: str = None, before_date: str = None):
    """
    同步历史数据
    :param since_date: 截止日期 (YYYY-MM-DD HH:MM:SS)，拉取到该时间为止。
    :param before_date: 起始日期 (YYYY-MM-DD HH:MM:SS)，从该时间开始往回拉取。
    """
    stop_at = 0
    start_before_ts = None

    if before_date:
        try:
            try:
                dt_before = datetime.datetime.strptime(before_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                dt_before = datetime.datetime.strptime(before_date, "%Y-%m-%d")
            
            if dt_before.tzinfo is None:
                dt_before = dt_before.astimezone(datetime.timezone.utc)
            
            start_before_ts = int(dt_before.timestamp() * 1000)
            logger.info(f"指定起始时间: {before_date} (UTC: {dt_before})")
        except ValueError:
            logger.error("起始日期格式错误，请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
            return

    if since_date:
        logger.info(f"开始同步历史数据: 目标截止到 {since_date}")
        try:
            # 尝试解析完整时间
            try:
                dt = datetime.datetime.strptime(since_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # 回退到仅日期
                dt = datetime.datetime.strptime(since_date, "%Y-%m-%d")
            
            # 假设输入是本地时间，转换为 UTC 时间戳
            # 如果是 naive (无时区)，认为是本地时间 -> 转 UTC
            if dt.tzinfo is None:
                dt = dt.astimezone(datetime.timezone.utc)
            
            stop_at = int(dt.timestamp() * 1000)
        except ValueError:
            logger.error("截止日期格式错误，请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
            return
    else:
        # Catch-up Mode
        logger.info("未指定截止日期，进入 [自动追赶模式] (Catch-up Mode)")
        latest_ts = get_latest_db_timestamp()
        if latest_ts > 0:
            # +1ms 以避免重复抓取数据库中已存在的最新那条消息
            stop_at = latest_ts + 1
            t_readable = datetime.datetime.fromtimestamp(stop_at/1000.0).strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"检测到数据库最新消息时间: {t_readable}，将同步在此之后的所有新消息...")
        else:
            logger.warning("数据库为空或查询失败，无法自动追赶。默认同步最近 24 小时数据...")
            stop_at = int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp() * 1000)

    # 使用 get_history_posts 进行拉取
    # limit 设置非常大，因为我们主要依赖 stop_at 来停止
    # max_api_requests 也设置很大，依赖 Smart Jump 和 stop_at
    HUGE_LIMIT = 1000000
    MAX_PAGES = 100000
    
    from utils.local_secrets import whop_feeds
    
    for feed_config in whop_feeds:
        feed_id = feed_config.get("feed_id")
        allowed_usernames = feed_config.get("allowed_usernames")
        
        if not feed_id:
            logger.warning("跳过无效的 feed 配置 (缺少 feed_id)")
            continue
            
        logger.info(f"==================================================")
        logger.info(f"开始同步 Feed: {feed_id}")
        if allowed_usernames:
            logger.info(f"启用用户过滤: 仅同步 {allowed_usernames}")
        else:
            logger.info(f"同步所有用户消息")
        logger.info(f"==================================================")
        
        try:
            items, _ = get_history_posts(
                limit=HUGE_LIMIT, 
                stop_at_timestamp=stop_at, 
                before=start_before_ts,
                max_api_requests=MAX_PAGES,
                accumulate_results=False,  # 内存优化：不累积结果
                feed_id=feed_id,
                allowed_usernames=allowed_usernames
            )
            
            logger.info(f"Feed {feed_id} 同步完成，本次共处理 {len(items)} 条消息")
            if items:
                oldest = items[-1]
                oldest_time = datetime.datetime.fromtimestamp(int(oldest.get('createdAt', 0))/1000)
                logger.info(f"本次同步最远到达: {oldest_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
        except Exception as e:
            logger.error(f"Feed {feed_id} 同步过程中发生错误: {e}")
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Whop 数据同步工具')
    parser.add_argument('--since', type=str, required=False, help='截止日期 (YYYY-MM-DD HH:MM:SS)。如果不传，则自动同步 DB 最新数据之后的所有消息。')
    parser.add_argument('--before', type=str, required=False, help='起始日期 (YYYY-MM-DD HH:MM:SS)。如果不传，则从最新消息开始。')
    
    args = parser.parse_args()
    
    sync_run(since_date=args.since, before_date=args.before)
