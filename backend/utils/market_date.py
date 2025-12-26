import pytz
from datetime import timedelta
import datetime
from typing import Tuple


def get_trading_window_cn() -> Tuple[int, int, str]:
    """
    计算当前对应的“中国交易日窗口” (北京时间 09:00 - 次日 09:00)
    
    逻辑：
    - 如果当前时间 (北京时间) < 10:00:
      窗口 = [昨天 09:00, 今天 09:00] (即刚刚结束或正在进行的那个窗口)
      注意：这里设为 10:00 切换，是为了让 09:00-10:00 期间能检测到 "now > end"，从而生成最终报告。
    - 如果当前时间 (北京时间) >= 10:00:
      窗口 = [今天 09:00, 明天 09:00] (即新的一天)
      
    返回:
        (start_ms, end_ms, description)
    """
    # 1. 获取当前北京时间
    tz_cn = pytz.timezone("Asia/Shanghai")
    now_cn = datetime.datetime.now(tz_cn)
    
    # 2. 确定基准日期
    # 切换点设为 10:00
    if now_cn.hour < 10:
        # 对应“昨天”开始的盘
        base_date = now_cn.date() - timedelta(days=1)
    else:
        # 对应“今天”开始的盘
        base_date = now_cn.date()
        
    # 3. 构造时间窗口
    # start: base_date 09:00
    start_dt = tz_cn.localize(datetime.datetime.combine(base_date, datetime.time(9, 0, 0)))
    
    # end: base_date + 1 day 09:00
    end_dt = tz_cn.localize(datetime.datetime.combine(base_date + timedelta(days=1), datetime.time(9, 0, 0)))
    
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    
    date_str = (base_date + timedelta(days=1)).strftime('%m月%d日')
    desc = f"美股复盘 ({date_str})"
    
    return start_ms, end_ms, desc


def get_trading_window_cn_offset(offset: int = 0) -> Tuple[int, int, str]:
    """
    获取指定偏移量的"中国交易日窗口" (北京时间 09:00 - 次日 09:00)
    
    Args:
        offset: 向前偏移的天数，0 = 当前窗口，1 = 上一个窗口，以此类推
        
    返回:
        (start_ms, end_ms, description)
    """
    # 1. 获取当前北京时间
    tz_cn = pytz.timezone("Asia/Shanghai")
    now_cn = datetime.datetime.now(tz_cn)
    
    # 2. 确定基准日期
    if now_cn.hour < 10:
        # 对应"昨天"开始的盘
        base_date = now_cn.date() - timedelta(days=1)
    else:
        # 对应"今天"开始的盘
        base_date = now_cn.date()
    
    # 3. 应用偏移
    base_date = base_date - timedelta(days=offset)
        
    # 4. 构造时间窗口
    # start: base_date 09:00
    start_dt = tz_cn.localize(datetime.datetime.combine(base_date, datetime.time(9, 0, 0)))
    
    # end: base_date + 1 day 09:00
    end_dt = tz_cn.localize(datetime.datetime.combine(base_date + timedelta(days=1), datetime.time(9, 0, 0)))
    
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    
    date_str = (base_date + timedelta(days=1)).strftime('%m月%d日')
    desc = f"美股复盘 ({date_str})"
    
    return start_ms, end_ms, desc


if __name__ == "__main__":
    import datetime
    
    s, e, d = get_trading_window_cn()
    print(f"Trading Window: {d}")
    print(f"Start: {datetime.datetime.fromtimestamp(s/1000)}")
    print(f"End:   {datetime.datetime.fromtimestamp(e/1000)}")