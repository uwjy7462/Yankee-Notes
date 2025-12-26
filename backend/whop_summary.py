import datetime
import pytz
import os
import time
from loguru import logger
from utils import history_list_to_text, get_knowledge_macro_prompt, get_timeline_views_prompt, get_metadata_prompt, get_response, save_summary_to_db, get_trading_window_cn, get_trading_window_cn_offset
from utils.local_secrets import supabase_url, supabase_key
from supabase import create_client, Client
import json
import re

# 初始化 Supabase
try:
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    logger.error(f"Supabase 初始化失败: {e}")
    supabase = None


def _iso_to_ms(iso: str) -> int:
    if iso:
        try:
            dt = datetime.datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except:
            return 0
    return 0

def fetch_vip_data(start_ms: int, end_ms: int):
    """
    从 whop_vip_posts 表获取指定时间窗口内的 VIP 数据，并重建上下文。
    注意：whop_vip_posts 已包含 whop_posts 和 whop_universal_posts 中的 VIP 内容。
    """
    if not supabase:
        return [], {}

    try:
        # 1. 查询 VIP 帖子 (whop_vip_posts)
        start_iso = datetime.datetime.fromtimestamp(start_ms / 1000.0, tz=datetime.timezone.utc).isoformat()
        end_iso = datetime.datetime.fromtimestamp(end_ms / 1000.0, tz=datetime.timezone.utc).isoformat()
        
        query = supabase.table("whop_vip_posts").select("*").order("posted_at", desc=True)
        query = query.gt("posted_at", start_iso).lt("posted_at", end_iso)
            
        resp = query.execute()
        vip_rows = resp.data
        
        if not vip_rows:
            return [], {}
            
        # 2. 收集需要补充的 Parent ID 和 User ID
        parent_ids = set()
        user_ids = set()
        
        for row in vip_rows:
            if row.get('user_id'):
                user_ids.add(row['user_id'])
            if row.get('reply_to_post_id'):
                parent_ids.add(row['reply_to_post_id'])
                
        # 3. 获取 Parent Posts (Context)
        parent_map = {}
        if parent_ids:
            p_resp = supabase.table("whop_posts").select("*").in_("id", list(parent_ids)).execute()
            for p in p_resp.data:
                parent_map[p['id']] = p
                if p.get('user_id'):
                    user_ids.add(p['user_id'])
                    
        # 4. 获取用户信息
        user_map = {}  # id -> username/name
        if user_ids:
            u_resp = supabase.table("whop_users").select("id, username, display_name").in_("id", list(user_ids)).execute()
            for u in u_resp.data:
                name = u.get('display_name') or u.get('username') or "Unknown"
                user_map[u['id']] = name

        # 5. 格式化为 history_list_to_text 所需的结构
        formatted_items = []
        
        for row in vip_rows:
            item = {
                'id': row['id'],
                'userId': row['user_id'],
                'content': row['content'],
                'createdAt': _iso_to_ms(row['posted_at']),
                'attachments': row.get('attachments') or [],
                'isPosterAdmin': False,
                'is_vip_related': True,
            }
            
            u_name = user_map.get(row['user_id'], "Unknown")
            item['user'] = {'username': u_name, 'name': u_name}
            
            # 补充 Reply Context
            reply_id = row.get('reply_to_post_id')
            if reply_id and reply_id in parent_map:
                parent = parent_map[reply_id]
                p_user_id = parent.get('user_id')
                p_name = user_map.get(p_user_id, "Unknown")
                
                item['replyingToPost'] = {
                    'id': reply_id,
                    'content': parent.get('content'),
                    'user': {
                        'username': p_name,
                        'name': p_name
                    }
                }
            
            formatted_items.append(item)
            
        return formatted_items, user_map

    except Exception as e:
        logger.error(f"Fetch VIP Data Failed: {e}")
        return [], {}

def get_last_summary_time() -> int:
    """
    获取最近一次总结的时间戳（毫秒）- 从数据库查询
    """
    if not supabase:
        return 0
    
    try:
        # 查询最新的一条记录
        resp = supabase.table("whop_summaries").select("created_at").order("created_at", desc=True).limit(1).execute()
        if resp.data:
            iso_time = resp.data[0]['created_at']
            # ISO -> 毫秒
            dt = datetime.datetime.fromisoformat(iso_time.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
    except Exception as e:
        logger.error(f"查询上次总结时间失败: {e}")
        
    return 0

def summary_run(start_ms: int, end_ms: int, title: str, description: str) -> bool:
    """
    生成并保存总结 (双 Agent 模式 + 动态元数据)
    
    Returns:
        bool: True 表示成功生成总结，False 表示跳过（无有效数据）
    """
    logger.info(f"开始生成总结: {title} ({description})")
    
    # 拉取指定窗口内的所有数据
    history_items, username_dict = fetch_vip_data(start_ms=start_ms, end_ms=end_ms)
    
    # 前置检查：如果没有 VIP 聊天内容，跳过 LLM 调用
    if not history_items:
        # 检查是否为周末 (周六=5, 周日=6)
        # 注意：start_ms 是北京时间 09:00
        tz_cn = pytz.timezone("Asia/Shanghai")
        start_dt = datetime.datetime.fromtimestamp(start_ms / 1000.0, tz=tz_cn)
        weekday = start_dt.weekday()
        
        if weekday in [5, 6]:
            logger.info(f"当前窗口起始于周{'六' if weekday == 5 else '日'} ({start_dt.strftime('%Y-%m-%d')})，通常无美股交易数据，正常跳过。")
        else:
            logger.warning("该时间窗口内没有 VIP 聊天内容，跳过 LLM 总结生成以节省 Token。")
        return False
    
    vip_username = "xiaozhaolucky"
    # 全量回顾模式，last_summary_time 设为 0
    big_text = history_list_to_text(history_items, username_dict, last_summary_time=0, vip_username=vip_username)
    
    # 内容有效性检查：如果转换后的文本过短，说明没有有意义的内容
    if not big_text or len(big_text.strip()) < 100:
        logger.warning(f"聊天内容过少 (长度: {len(big_text.strip()) if big_text else 0} 字符)，跳过 LLM 总结生成。")
        return False
    
    logger.info(f"获取到 {len(history_items)} 条 VIP 相关消息，文本长度: {len(big_text)} 字符，开始调用 LLM...")
    
    # 计算日期字符串 (YYYY-MM-DD)
    date_str = datetime.datetime.fromtimestamp(start_ms / 1000.0).strftime('%Y-%m-%d')
    
    from utils.local_secrets import ai_model_name
    model = ai_model_name
    
    import concurrent.futures

    # 定义并行任务函数
    def call_agent_1():
        logger.info("Agent 1: 生成知识库与宏观分析...")
        prompt_1 = get_knowledge_macro_prompt(date_str) + big_text
        return get_response(prompt_1, model=model)

    def call_agent_2():
        logger.info("Agent 2: 生成核心观点与时间线...")
        prompt_2 = get_timeline_views_prompt(date_str) + big_text
        return get_response(prompt_2, model=model)

    # 并行执行 Agent 1 和 Agent 2
    logger.info("🚀 启动并行任务: Agent 1 & Agent 2...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_1 = executor.submit(call_agent_1)
        future_2 = executor.submit(call_agent_2)
        
        summary_1 = future_1.result()
        summary_2 = future_2.result()
        
    logger.info("✅ Agent 1 & Agent 2 任务完成")

    # 合并结果
    # 用户要求：Timeline (summary_2) 在前，Knowledge (summary_1) 在后
    # 但是 summary_1 包含主标题 (# ...)，我们需要把标题提取出来放在最前面
    
    final_summary = ""
    title_line = ""
    body_1 = summary_1
    
    # 尝试提取 summary_1 的第一行作为标题
    # 仅当第一行是 H1 (# ) 时才提取，避免误伤 H2 (## )
    if summary_1 and summary_1.strip().startswith("# "):
        parts = summary_1.strip().split("\n", 1)
        if len(parts) >= 1:
            title_line = parts[0]
            if len(parts) > 1:
                body_1 = parts[1]
            else:
                body_1 = ""
    
    if title_line:
        # 丢弃原有的 title_line，只保留 body
        final_summary = f"{summary_2}\n\n{body_1}"
    else:
        # 如果提取失败，直接拼接，Timeline 在前
        final_summary = f"{summary_2}\n\n{summary_1}"

    # 生成动态元数据 (Title & Description)
    logger.info("生成动态元数据 (Title & Description)...")
    try:
        # 计算标题日期 (YYYY.MM.DD)，使用窗口开始时间作为锚点 (通常是美股交易日当天)
        title_date = datetime.datetime.fromtimestamp(start_ms / 1000.0).strftime('%Y.%m.%d')
        
        meta_prompt = get_metadata_prompt(title_date) + f"\n\n{final_summary}"
        meta_response = get_response(meta_prompt, model=model)
        
        # 尝试解析 JSON
        # 有时候 LLM 会包裹在 ```json ... ``` 中
        json_str = meta_response
        if "```json" in meta_response:
            match = re.search(r"```json(.*?)```", meta_response, re.DOTALL)
            if match:
                json_str = match.group(1)
        elif "```" in meta_response:
             match = re.search(r"```(.*?)```", meta_response, re.DOTALL)
             if match:
                json_str = match.group(1)
                
        meta_data = json.loads(json_str)
        
        new_title = meta_data.get("title", title)
        new_description = meta_data.get("description", description)
        new_tags = meta_data.get("tags", [])
        
        logger.info(f"动态元数据生成成功: Title='{new_title}', Desc='{new_description}', Tags={new_tags}")
        
        # 将动态生成的 Title 加到文档最前面
        # 先保存 body 部分
        body_content = final_summary
        
        final_summary = f"# {new_title}\n\n"
        
        # 显性化展示 Tags (用户要求移除)
        # if new_tags:
        #     tags_str = " ".join([f"#{t}" for t in new_tags])
        #     final_summary += f"**Tags:** {tags_str}\n\n"
            
        final_summary += f"{body_content}"
        
    except Exception as e:
        logger.error(f"动态元数据生成失败，使用默认值: {e}")
        new_title = title
        new_description = description
        new_tags = []
        # 失败时使用传入的默认 title
        final_summary = f"# {new_title}\n\n{final_summary}"

    # 保存到数据库
    save_summary_to_db(
        summary=final_summary,
        title=new_title,
        description=new_description,
        model=model,
        raw_chat_text=big_text,
        tags=new_tags,
    )
    
    logger.info("总结生成并保存成功！")
    return True

if __name__ == "__main__":
    import sys
    is_force_run = "--force" in sys.argv

    # 1. 计算时间窗口 (北京时间 18:00 - 10:00)
    start_ms, end_ms, title_desc = get_trading_window_cn()
    
    start_str = datetime.datetime.fromtimestamp(start_ms/1000).strftime('%Y-%m-%d %H:%M')
    end_str = datetime.datetime.fromtimestamp(end_ms/1000).strftime('%Y-%m-%d %H:%M')
    logger.info(f"当前交易日窗口: {start_str} -> {end_str} ({title_desc})")
    
    # 2. 重复性检查 (Deduplication)
    last_time_ms = get_last_summary_time()
    last_time_str = datetime.datetime.fromtimestamp(last_time_ms/1000).strftime('%Y-%m-%d %H:%M:%S')
    
    now_ms = int(time.time() * 1000)
    
    should_run = False
    trigger_reason = ""
    
    # 逻辑 A: 如果上次总结时间 晚于 窗口结束时间，说明这个窗口的总结已经做过了
    if last_time_ms >= end_ms:
        if is_force_run:
            logger.warning(f"该窗口总结已存在 (上次: {last_time_str})，但检测到强制执行参数，继续...")
            should_run = True
            trigger_reason = "强制执行 (--force)"
        else:
            logger.info(f"该窗口总结已存在 (上次: {last_time_str} >= 窗口结束 {end_str})，跳过以节省 Token。")
            exit(0)
            
    # 逻辑 B: 如果上次总结时间 早于 窗口开始时间，说明这是该窗口的第一次运行
    elif last_time_ms < start_ms:
        should_run = True
        trigger_reason = "新交易日首次运行"
        
    # 逻辑 C: 如果上次总结时间 在 窗口中间 (部分总结)
    else:
        # 如果当前时间已经过了窗口结束时间 (补全最终报告)
        if now_ms > end_ms:
            should_run = True
            trigger_reason = "窗口结束，生成最终报告"
        else:
            # 还在窗口期内，检查是否有足够的新消息
            # 这里为了节省 Token，我们可以设置一个较高的阈值，或者直接跳过（除非 force）
            # 用户说 "其他时间没有需求"，暗示只有最终报告重要？
            # 但如果用户在 09:00 运行，可能想看截止目前的。
            # 策略：检查新消息数量
            logger.info("检测到窗口内已有部分总结，正在检查增量消息...")
            # 预检一下数据量
            new_items, _ = fetch_vip_data(start_ms=last_time_ms, end_ms=end_ms)
            new_count = len(new_items)
            
            if is_force_run:
                should_run = True
                trigger_reason = f"强制更新 (增量 {new_count} 条)"
            elif new_count > 50: # 只有当增量大于 50 条才更新，避免频繁浪费
                should_run = True
                trigger_reason = f"增量消息积累 ({new_count} > 50)"
            else:
                logger.info(f"增量消息不足 ({new_count} <= 50)，跳过。")
                exit(0)

    if should_run:
        logger.info(f"触发总结生成，原因: {trigger_reason}")
        result = summary_run(start_ms, end_ms, "美股交易日复盘", title_desc)
        
        # --force 模式下，如果当前窗口没有数据，尝试回退到上一个窗口
        if not result and is_force_run:
            logger.info("当前窗口无有效数据，尝试回退到上一个交易日窗口...")
            
            # 最多回退 3 个窗口（避免无限循环）
            for offset in range(1, 4):
                prev_start_ms, prev_end_ms, prev_title_desc = get_trading_window_cn_offset(offset)
                prev_start_str = datetime.datetime.fromtimestamp(prev_start_ms/1000).strftime('%Y-%m-%d %H:%M')
                prev_end_str = datetime.datetime.fromtimestamp(prev_end_ms/1000).strftime('%Y-%m-%d %H:%M')
                logger.info(f"尝试窗口 (offset={offset}): {prev_start_str} -> {prev_end_str} ({prev_title_desc})")
                
                result = summary_run(prev_start_ms, prev_end_ms, "美股交易日复盘", prev_title_desc)
                if result:
                    logger.info(f"成功使用窗口 (offset={offset}) 生成总结")
                    break
            else:
                logger.warning("回退 3 个窗口后仍未找到有效数据")
        elif not result:
            logger.info("由于没有有效数据，本次未生成总结。")
    else:
        logger.info("未满足触发条件，跳过")

