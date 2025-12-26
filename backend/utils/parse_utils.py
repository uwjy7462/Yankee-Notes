
from datetime import datetime
from typing import List, Dict, Any, Optional

def format_timestamp(ts_ms: str | int) -> str:
    """
    把毫秒时间戳转成可读时间字符串 (北京时间)，例如：2025-11-26 09:30 BJT
    """
    import pytz
    ts_ms_int = int(ts_ms)
    dt_utc = datetime.fromtimestamp(ts_ms_int / 1000, tz=pytz.utc)
    dt_cn = dt_utc.astimezone(pytz.timezone('Asia/Shanghai'))
    return dt_cn.strftime("%Y-%m-%d %H:%M BJT")


def get_user_name(post: Dict[str, Any], username_dict: Dict[str, str]) -> str:
    """
    优先用 post['user']['name']，没有的话退回 userId。
    """
    user = post.get("user")
    if isinstance(user, dict):
        return user.get("name") or user.get("username") or username_dict.get(post.get("userId"), "未知用户")
    return username_dict.get(post.get("userId"), "未知用户")


def get_reply_target(post: Dict[str, Any]) -> Optional[str]:
    """
    从 replyingToPost 里取一个“回复对象”的描述。
    优先用原帖用户名字，其次内容（可按需截断）。
    """
    replying = post.get("replyingToPost")
    if not replying:
        return None

    # 先尝试用户名
    user = replying.get("user")
    if isinstance(user, dict):
        name = user.get("name") or user.get("username")
        if name:
            return name

    # 再退回到内容（这里简单截断下，避免太长）
    content = replying.get("content")
    if content:
        content = content.strip()
        max_len = 20  # 你可以按需要调整
        return content if len(content) <= max_len else content[:max_len] + "..."
    return None


def history_list_to_text(items: List[Dict[str, Any]], username_dict: Dict[str, str], last_summary_time: int = 0, vip_username: str = "xiaozhaolucky") -> str:
    """
    把一整个 history list 转成一大段文本，供 LLM 做 summaries 使用。
    
    策略升级 (Suggestion 1):
    将文本分为两个核心区域：
    1. [VIP 核心上下文]: 包含 VIP 的发言、VIP 回复的消息、回复 VIP 的消息。这是 LLM 的重点分析对象。
    2. [市场情绪参考]: 其他人的闲聊，作为背景噪音或情绪参考。
    """
    # 尽量按时间排序
    items_sorted = sorted(
        items,
        key=lambda x: int(x.get("createdAt", 0))
    )

    vip_context_lines: List[str] = []
    market_context_lines: List[str] = []
    
    # 统一转小写处理
    target_vip = vip_username.lower()
    
    # 找到 VIP 的 User ID (可能有多个 ID 对应同一个 username，虽然少见但防守一下)
    vip_user_ids = set()
    for uid, u_name in username_dict.items():
        if u_name and u_name.lower() == target_vip:
            vip_user_ids.add(uid)

    for post in items_sorted:
        created_at = int(post.get("createdAt", 0))
        
        # 仅处理新增消息 (或者是全量模式下的所有消息)
        # 注意：如果 last_summary_time > 0，我们只关心新增的。
        # 但为了上下文完整性，如果某条旧消息是 VIP 上下文的一部分，是否要包含？
        # 目前逻辑是：history_items 已经是根据 limit/time 筛选过的集合。
        # 如果是增量更新，history_items 可能包含一些旧消息作为 context (取决于 get_history_posts 的实现，目前看它主要返回 limit 条)
        # 简单起见，我们对传入的所有 items 进行分类。
        
        # 格式化单行
        time_str = format_timestamp(created_at)
        is_admin = post.get("isPosterAdmin", False)
        admin_tag = "[管理员]" if is_admin else ""
        name = get_user_name(post, username_dict)
        
        reply_target = get_reply_target(post)
        reply_part = f"(回复 {reply_target})" if reply_target else ""
        
        content = (post.get("content") or "").strip()
        
        # 处理图片/附件标识
        attachments = post.get("attachments", [])
        if attachments:
            content += f" [包含 {len(attachments)} 张图片/附件]"

        line = f"{time_str} {admin_tag}{name} 说{reply_part}: {content}"
        
        # 判断是否属于 VIP 上下文
        is_vip_related = post.get("is_vip_related", False)
        
        # 1. VIP 本人发言
        poster_id = post.get("userId")
        # 名字也转小写比较
        if not is_vip_related and (poster_id in vip_user_ids or (name and name.lower() == target_vip)):
            is_vip_related = True
        
        # 2. 回复 VIP 的消息 (需要检查 replyingToPost 的 user)
        # post['replyingToPost'] 结构: {'user': {'username': ...}}
        if not is_vip_related:
            replying_to = post.get("replyingToPost")
            if replying_to:
                r_user = replying_to.get("user")
                if r_user:
                    r_username = r_user.get("username")
                    r_name = r_user.get("name")
                    
                    if (r_username and r_username.lower() == target_vip) or \
                       (r_name and r_name.lower() == target_vip):
                        is_vip_related = True
        
        # 3. VIP 回复别人的消息 (已经在 case 1 中涵盖，因为 poster 是 VIP)
        
        if is_vip_related:
            vip_context_lines.append(line)
        else:
            market_context_lines.append(line)

    # 拼接最终文本
    result = ""
    
    result += f"=== 核心内参：{vip_username} 专属上下文 (高权重) ===\n"
    result += "> 请重点分析以下内容，提取交易思路、信号和逻辑。\n\n"
    if vip_context_lines:
        result += "\n".join(vip_context_lines)
    else:
        result += "(该时段内 VIP 无直接互动)"
    result += "\n\n"
        
    result += "=== 市场情绪参考 (低权重) ===\n"
    result += "> 以下是群友闲聊，仅供判断市场情绪，若无实质内容可忽略。\n\n"
    if market_context_lines:
        result += "\n".join(market_context_lines)
    else:
        result += "(无其他消息)"
        
    return result