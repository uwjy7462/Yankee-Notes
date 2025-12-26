
import os
import sys
from typing import List, Dict, Any
from loguru import logger
from dotenv import load_dotenv
from supabase import create_client, Client

# Add backend to path to allow imports if needed, though we use relative imports or direct execution
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Try to import secrets
try:
    # Adjust import based on where the script is run from
    sys.path.append(os.getcwd())
    from backend.utils.local_secrets import supabase_url, supabase_key
except ImportError:
    load_dotenv()
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    logger.error("Supabase credentials not found")
    exit(1)

supabase: Client = create_client(supabase_url, supabase_key)

VIP_USERNAME = "xiaozhaolucky"
TABLE_NAME = "whop_vip_posts"
SOURCE_TABLE = "whop_posts"

def get_vip_user_id(username: str) -> str:
    logger.info(f"Looking up user ID for username: {username}...")
    try:
        # Try to find by username
        resp = supabase.table("whop_users").select("id").eq("username", username).execute()
        if resp.data:
            return resp.data[0]['id']
        
        # Fallback: Try to find by display_name if username fails (sometimes they are mixed)
        resp = supabase.table("whop_users").select("id").eq("display_name", username).execute()
        if resp.data:
            return resp.data[0]['id']
            
    except Exception as e:
        logger.error(f"Error looking up user: {e}")
    return None

def fetch_all_vip_posts(user_id: str) -> List[Dict[str, Any]]:
    logger.info(f"Fetching VIP posts for user_id: {user_id}...")
    all_posts = []
    page_size = 1000
    offset = 0
    
    while True:
        try:
            resp = supabase.table(SOURCE_TABLE)\
                .select("*")\
                .eq("user_id", user_id)\
                .range(offset, offset + page_size - 1)\
                .execute()
            
            data = resp.data
            if not data:
                break
                
            all_posts.extend(data)
            logger.info(f"Fetched {len(data)} posts (Total: {len(all_posts)})")
            
            if len(data) < page_size:
                break
                
            offset += page_size
        except Exception as e:
            logger.error(f"Error fetching VIP posts: {e}")
            break
            
    return all_posts

def fetch_all_universal_posts(user_id: str) -> List[Dict[str, Any]]:
    """
    从 whop_universal_posts 表获取指定用户的所有帖子，
    并转换为与 whop_posts 兼容的格式。
    """
    logger.info(f"Fetching Universal posts for user_id: {user_id}...")
    all_posts = []
    page_size = 1000
    offset = 0
    
    while True:
        try:
            resp = supabase.table("whop_universal_posts")\
                .select("*")\
                .eq("user_id", user_id)\
                .range(offset, offset + page_size - 1)\
                .execute()
            
            data = resp.data
            if not data:
                break
            
            # 转换为 whop_posts 兼容格式
            for row in data:
                # 合并 title 和 content
                title = row.get('title') or ''
                content = row.get('content') or ''
                if title and content:
                    combined_content = f"【{title}】\n{content}"
                elif title:
                    combined_content = f"【{title}】"
                else:
                    combined_content = content
                
                converted = {
                    'id': row['id'],
                    'feed_id': row.get('feed_id'),
                    'user_id': row.get('user_id'),
                    'content': combined_content,
                    'rich_content': row.get('rich_content'),
                    'reply_to_post_id': None,  # Universal posts 没有回复关系
                    'mentioned_user_ids': row.get('mentioned_user_ids'),
                    'attachments': row.get('attachments'),
                    'link_embeds': None,
                    'gifs': None,
                    'reaction_counts': row.get('reaction_counts'),
                    'view_count': row.get('view_count'),
                    'is_pinned': row.get('is_pinned'),
                    'is_edited': row.get('is_edited'),
                    'is_deleted': row.get('is_deleted'),
                    'posted_at': row.get('posted_at'),
                    'edited_at': None,
                    'crawled_at': row.get('crawled_at'),
                }
                all_posts.append(converted)
                
            logger.info(f"Fetched {len(data)} Universal posts (Total: {len(all_posts)})")
            
            if len(data) < page_size:
                break
                
            offset += page_size
        except Exception as e:
            logger.error(f"Error fetching Universal posts: {e}")
            break
            
    return all_posts

def fetch_posts_by_ids(ids: List[str]) -> List[Dict[str, Any]]:
    if not ids:
        return []
        
    logger.info(f"Fetching {len(ids)} context posts...")
    all_posts = []
    # Supabase 'in' filter might have limits, chunk it
    chunk_size = 200
    
    # Remove duplicates and None
    unique_ids = list(set([i for i in ids if i]))
    
    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i:i + chunk_size]
        try:
            resp = supabase.table(SOURCE_TABLE)\
                .select("*")\
                .in_("id", chunk)\
                .execute()
            
            if resp.data:
                all_posts.extend(resp.data)
        except Exception as e:
            logger.error(f"Error fetching context posts chunk: {e}")
            
    return all_posts

def clear_target_table():
    logger.info(f"Clearing target table '{TABLE_NAME}' to ensure clean sync...")
    try:
        # Delete all rows. We use a condition that covers all IDs (e.g., ID is not empty string)
        # Assuming IDs are non-empty strings.
        supabase.table(TABLE_NAME).delete().neq("id", "").execute()
        logger.info("Table cleared.")
    except Exception as e:
        logger.error(f"Error clearing table: {e}")

def transform_and_upsert(vip_posts: List[Dict], context_posts: List[Dict], vip_user_id: str):
    logger.info("Transforming and upserting data...")
    
    upsert_data = []
    
    # Process VIP posts
    for p in vip_posts:
        p_copy = p.copy()
        p_copy['relation_type'] = 'vip'
        upsert_data.append(p_copy)
        
    # Process Context posts
    # We only want context posts that are NOT by the VIP themselves (self-replies)
    # Although we already filter by ID, checking user_id is safer.
    
    vip_post_ids = {p['id'] for p in vip_posts}
    
    for p in context_posts:
        # 1. Skip if it's already in the VIP list (it's a VIP post)
        if p['id'] in vip_post_ids:
            continue
            
        # 2. Skip if the author is the VIP (redundant check but good for safety)
        if p.get('user_id') == vip_user_id:
            continue
            
        p_copy = p.copy()
        p_copy['relation_type'] = 'context'
        upsert_data.append(p_copy)
        
    if not upsert_data:
        logger.info("No data to upsert.")
        return

    # Upsert in chunks
    chunk_size = 100
    total = len(upsert_data)
    
    for i in range(0, total, chunk_size):
        chunk = upsert_data[i:i + chunk_size]
        try:
            # We need to make sure the target table exists and has the relation_type column
            supabase.table(TABLE_NAME).upsert(chunk).execute()
            logger.info(f"Upserted batch {i//chunk_size + 1}/{(total-1)//chunk_size + 1}")
        except Exception as e:
            logger.error(f"Error upserting batch: {e}")
            if "relation" in str(e) and "does not exist" in str(e):
                logger.critical(f"Table '{TABLE_NAME}' does not exist. Please run the SQL creation script in Supabase Dashboard.")
                return

def main():
    # 1. Get User ID
    vip_user_id = get_vip_user_id(VIP_USERNAME)
    if not vip_user_id:
        # Fallback to hardcoded if lookup fails (for safety)
        logger.warning(f"Could not find user ID for {VIP_USERNAME}, trying hardcoded ID...")
        vip_user_id = "user_4yeplXgbguTu4" # Known ID for xiaozhaolucky
    
    logger.info(f"Target VIP User ID: {vip_user_id}")

    # 2. Fetch VIP Posts from whop_posts
    vip_posts = fetch_all_vip_posts(vip_user_id)
    logger.info(f"Found {len(vip_posts)} VIP posts from whop_posts.")
    
    # 3. Fetch Universal Posts (也是 VIP 内容)
    universal_posts = fetch_all_universal_posts(vip_user_id)
    logger.info(f"Found {len(universal_posts)} Universal posts from whop_universal_posts.")
    
    # 4. 合并 VIP Posts
    all_vip_posts = vip_posts + universal_posts
    logger.info(f"Total VIP posts (combined): {len(all_vip_posts)}")
    
    if not all_vip_posts:
        logger.warning("No VIP posts found. Exiting.")
        return
    
    # 5. Fetch Context Posts (只从 whop_posts 的回复获取)
    context_ids = [p['reply_to_post_id'] for p in vip_posts if p.get('reply_to_post_id')]
    context_posts = fetch_posts_by_ids(context_ids)
    logger.info(f"Found {len(context_posts)} context posts.")
    
    # 6. Clear Table (Only if we have data to replace it with)
    clear_target_table()
    
    # 7. Upsert
    transform_and_upsert(all_vip_posts, context_posts, vip_user_id)
    logger.info("Sync complete.")

if __name__ == "__main__":
    main()
