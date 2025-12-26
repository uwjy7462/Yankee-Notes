import datetime
import os
import time
from typing import Optional, List, Dict, Any
from openai import OpenAI
from loguru import logger
import pytz
from .local_secrets import openai_api_key, openai_base_url, supabase_url, supabase_key
from supabase import create_client, Client

client = OpenAI(
    api_key=openai_api_key,
    base_url=openai_base_url
)

# 初始化 Supabase
try:
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    logger.error(f"Supabase 初始化失败: {e}")
    supabase = None


def _determine_provider(base_url: str, model: str) -> str:
    """根据 base_url 或 model 简单推断 provider"""
    base_url = base_url.lower()
    model = model.lower()
    
    if 'deepseek' in base_url or 'deepseek' in model:
        return 'deepseek'
    if 'openai' in base_url:
        return 'openai'
    if 'gemini' in model or 'google' in base_url:
        return 'google'
    if 'qwen' in model:
        return 'qwen'
    return 'other'


def insert_llm_log(
    model_name: str,
    input_params: Dict[str, Any],
    output_result: Any,
    start_time: float,
    end_time: float,
    base_url: str,
    error: Optional[Exception] = None
) -> None:
    """记录 LLM 调用日志"""
    if not supabase:
        return

    try:
        latency_ms = int((end_time - start_time) * 1000)
        provider = _determine_provider(base_url, model_name)
        
        # 提取 Token 使用情况
        total_tokens = None
        output_json = None
        
        if error:
            status = 'error'
            error_message = str(error)
        else:
            status = 'success'
            error_message = None
            
            # 尝试提取 usage 和序列化响应
            if hasattr(output_result, 'usage') and output_result.usage:
                total_tokens = output_result.usage.total_tokens
            
            if hasattr(output_result, 'model_dump'):
                output_json = output_result.model_dump()
            elif isinstance(output_result, (dict, list, str, int, float, bool)):
                output_json = output_result
            else:
                output_json = str(output_result)

        data = {
            "model_name": model_name,
            "provider": provider,
            "base_url": base_url,
            "input_params": input_params,
            "output_result": output_json,
            "total_tokens": total_tokens,
            "latency_ms": latency_ms,
            "status": status,
            "error_message": error_message,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
        supabase.table("llm_logs").insert(data).execute()
        
    except Exception as e:
        # 日志记录失败不应影响主流程，仅打印错误
        logger.error(f"写入 LLM 日志失败: {e}")


def get_response(to_summary_text: str, model: str = "gemini-2.5-pro") -> str:
    logger.info(f"正在使用模型 {model} 生成总结...")
    
    messages = [{"role": "user", "content": to_summary_text}]
    base_url = str(client.base_url)
    
    start_time = time.time()
    response = None
    error = None
    
    try:
        kwargs = {
            "model": model,
            "messages": messages
        }
        
        if "qwen" in model.lower():
            kwargs["extra_body"] = {"enable_thinking": True}

        response = client.chat.completions.create(**kwargs)
        logger.info("模型生成总结完成。")
        return_val = ""
        
        if isinstance(response, str):
            logger.warning("API 返回的是字符串格式（非标准 OpenAI 响应对象）")
            return_val = response
        else:
            try:
                return_val = response.choices[0].message.content
            except (AttributeError, IndexError, TypeError) as e:
                logger.error(f"解析响应失败: {e}")
                logger.error(f"响应类型: {type(response)}")
                logger.error(f"响应内容: {response}")
                raise e
                
        return return_val

    except Exception as e:
        error = e
        raise e
    finally:
        end_time = time.time()
        # 无论成功失败都记录日志
        insert_llm_log(
            model_name=model,
            input_params={"messages": messages},
            output_result=response,
            start_time=start_time,
            end_time=end_time,
            base_url=base_url,
            error=error
        )


def save_summary_to_db(
    summary: str,
    description: str,
    model: str,
    title: Optional[str] = None,
    raw_chat_text: str = "",
    tags: Optional[List[str]] = None,
) -> None:
    """
    将总结保存到 Supabase 数据库
    """
    if not supabase:
        logger.error("Supabase 未配置，无法保存总结")
        return

    # 构造数据
    data = {
        "title": title or f"{description} 总结",
        "description": description,
        "content": summary,
        "raw_chat_text": raw_chat_text, # 视需求决定是否存原始文本，可能会很大
        "model_name": model,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "tags": tags,
    }
    
    try:
        supabase.table("whop_summaries").insert(data).execute()
        logger.info("总结已成功保存到数据库")
    except Exception as e:
        logger.error(f"保存总结到数据库失败: {e}")


def check_summary_exists_by_date(date_str: str) -> bool:
    """
    检查指定日期是否已存在总结 (通过标题前缀匹配)
    
    Args:
        date_str: 日期字符串，格式 "YYYY.MM.DD"
        
    Returns:
        bool: True 表示已存在
    """
    if not supabase:
        return False
        
    try:
        # 标题格式通常为 "YYYY.MM.DD | ..."
        # 使用 ilike 匹配前缀
        pattern = f"{date_str}%"
        resp = supabase.table("whop_summaries").select("id").ilike("title", pattern).limit(1).execute()
        
        if resp.data:
            return True
            
    except Exception as e:
        logger.error(f"检查总结是否存在失败: {e}")
        
    return False
