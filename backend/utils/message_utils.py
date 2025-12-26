import json
import os
import time
import random
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import requests
from loguru import logger
from supabase import create_client, Client
from .local_secrets import whom_headers as headers
from .local_secrets import supabase_url, supabase_key

try:
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    logger.error(f"Supabase 初始化失败: {e}")
    supabase = None

url = 'https://whop.com/api/graphql/MessagesFetchFeedPosts/'

def _ms_to_iso(ms: Any) -> Optional[str]:
    if ms is not None:
        try:
            return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).isoformat()
        except:
            return None
    return None

def _iso_to_ms(iso: Any) -> int:
    if iso:
        try:
            dt = datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except:
            return 0
    return 0

def _record_to_post(r: Dict) -> Dict:
    return {
        'id': r['id'],
        'feedId': r['feed_id'],
        'userId': r['user_id'],
        'content': r['content'],
        'richContent': r['rich_content'],
        'replyingToPostId': r['reply_to_post_id'],
        'mentionedUserIds': r['mentioned_user_ids'],
        'attachments': r['attachments'],
        'linkEmbeds': r['link_embeds'],
        'gifs': r['gifs'],
        'reactionCounts': r['reaction_counts'],
        'viewCount': r['view_count'],
        'isPinned': r['is_pinned'],
        'isEdited': r['is_edited'],
        'isDeleted': r['is_deleted'],
        'createdAt': _iso_to_ms(r['posted_at']),
        'updatedAt': _iso_to_ms(r['edited_at']),
        'messageType': 'text'
    }

def _upsert_users_to_db(users_list: List[Dict]) -> None:
    if not users_list or not supabase:
        return
    
    # 全局去重：防止同一个批次中出现重复的 user_id 导致 PostgreSQL 报错
    unique_users = {}
    for u in users_list:
        uid = u.get('id')
        if uid:
            unique_users[uid] = u
            
    data = []
    for u in unique_users.values():
        data.append({
            'id': u.get('id'),
            'username': u.get('username'),
            'display_name': u.get('name'),
            'avatar_url': u.get('profilePicSm', {}).get('double') or u.get('profilePicLg', {}).get('double'),
            'roles': u.get('roles'),
            'updated_at': datetime.now(timezone.utc).isoformat()
        })
        
    if data:
        try:
            supabase.table('whop_users').upsert(data).execute()
        except Exception as e:
            logger.error(f"Supabase 用户写入失败: {e}")

def _upsert_posts_to_db(posts: List[Dict]) -> None:
    if not posts or not supabase:
        return
        
    data = []
    for p in posts:
        pid = p.get('id')
        if not pid: continue
        
        try:
            data.append({
                'id': pid,
                'feed_id': p.get('feedId'),
                'user_id': p.get('userId'),
                'content': p.get('content'),
                'rich_content': p.get('richContent'),
                'reply_to_post_id': p.get('replyingToPostId'),
                'mentioned_user_ids': p.get('mentionedUserIds', []),
                'attachments': p.get('attachments', []),
                'link_embeds': p.get('linkEmbeds', []),
                'gifs': p.get('gifs', []),
                'reaction_counts': p.get('reactionCounts', []),
                'view_count': p.get('viewCount', 0),
                'is_pinned': p.get('isPinned', False),
                'is_edited': p.get('isEdited', False),
                'is_deleted': p.get('isDeleted', False),
                'posted_at': _ms_to_iso(p.get('createdAt')),
                'edited_at': _ms_to_iso(p.get('updatedAt')),
                'crawled_at': datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.warning(f"跳过格式错误的帖子 {pid}: {e}")
            continue
            
    if data:
        try:
            supabase.table('whop_posts').upsert(data).execute()
        except Exception as e:
            logger.error(f"Supabase 帖子写入失败: {e}")

def _fetch_posts_from_db(limit: int, before_ms: Optional[int]) -> List[Dict]:
    if not supabase:
        return []
        
    try:
        query = supabase.table('whop_posts').select('*').order('posted_at', desc=True).limit(limit)
        
        if before_ms:
            before_iso = _ms_to_iso(before_ms)
            if before_iso:
                query = query.lt('posted_at', before_iso)
                
        resp = query.execute()
        return [_record_to_post(r) for r in resp.data]
    except Exception as e:
        logger.error(f"Supabase 查询失败: {e}")
        return []

def _get_users_map_from_db(user_ids: List[str]) -> Dict[str, str]:
    if not supabase or not user_ids:
        return {}
        
    try:
        resp = supabase.table('whop_users').select('id, username').in_('id', user_ids).execute()
        return {r['id']: r['username'] for r in resp.data}
    except Exception as e:
        logger.error(f"Supabase 用户查询失败: {e}")
        return {}

def get_payload(limit: int, before: int = None, feed_id: str = "chat_feed_1CTr5VAdNHtbZAFaTitvoT") -> str:
    if before is not None:
        before_str = str(before)
    else:
        before_str = "null"
        
    return '{"query":"query MessagesFetchFeedPosts($feedType: FeedTypes!, $after: BigInt, $before: BigInt, $aroundId: ID, $feedId: ID!, $includeDeleted: Boolean, $includeReactions: Boolean, $limit: Int, $direction: Direction) {\\n  feedPosts(\\n    feedType: $feedType\\n    after: $after\\n    before: $before\\n    aroundId: $aroundId\\n    feedId: $feedId\\n    includeDeleted: $includeDeleted\\n    includeReactions: $includeReactions\\n    limit: $limit\\n    direction: $direction\\n  ) {\\n    posts {\\n      __typename\\n      ...DmsPostFragment\\n    }\\n    users {\\n      ...BasicUserProfileDetails\\n    }\\n    reactions {\\n      ...ReactionFragment\\n    }\\n  }\\n}\\n\\nfragment DmsPostFragment on DmsPost {\\n  id\\n  createdAt\\n  updatedAt\\n  isDeleted\\n  sortKey\\n  isPosterAdmin\\n  mentionedUserIds\\n  content\\n  feedId\\n  feedType\\n  attachments {\\n    ...Attachment\\n  }\\n  gifs {\\n    height\\n    provider\\n    originalUrl\\n    previewUrl\\n    provider\\n    slug\\n    title\\n    width\\n  }\\n  isEdited\\n  isEveryoneMentioned\\n  isPinned\\n  linkEmbeds {\\n    description\\n    favicon\\n    image\\n    processing\\n    title\\n    url\\n    footer {\\n      title\\n      description\\n      icon\\n    }\\n  }\\n  richContent\\n  userId\\n  viewCount\\n  reactionCounts {\\n    reactionType\\n    userCount\\n    value\\n  }\\n  messageType\\n  embed\\n  replyingToPostId\\n  replyingToPost {\\n    id\\n    richContent\\n    content\\n    gifs {\\n      __typename\\n    }\\n    isDeleted\\n    linkEmbeds {\\n      __typename\\n    }\\n    mentionedUserIds\\n    isEveryoneMentioned\\n    messageType\\n    attachments {\\n      contentType\\n    }\\n    user {\\n      id\\n      name\\n      username\\n      roles\\n      profilePicSm: profileImageSrcset(style: s32) {\\n        double\\n      }\\n    }\\n  }\\n  poll {\\n    options {\\n      id\\n      text\\n    }\\n  }\\n  customAuthor {\\n    displayName\\n    profilePicture {\\n      sourceUrl\\n    }\\n  }\\n}\\n\\nfragment Attachment on AttachmentInterface {\\n  __typename\\n  id\\n  signedId\\n  analyzed\\n  byteSizeV2\\n  filename\\n  contentType\\n  source(variant: original) {\\n    url\\n  }\\n  ... on ImageAttachment {\\n    height\\n    width\\n    blurhash\\n    aspectRatio\\n  }\\n  ... on VideoAttachment {\\n    height\\n    width\\n    duration\\n    aspectRatio\\n    preview(variant: original) {\\n      url\\n    }\\n  }\\n  ... on AudioAttachment {\\n    duration\\n    waveformUrl\\n  }\\n}\\n\\nfragment BasicUserProfileDetails on PublicProfileUser {\\n  id\\n  name\\n  createdAt\\n  bannerImageLg: bannerImageSrcset(style: s600x200) {\\n    double\\n  }\\n  profilePicLg: profileImageSrcset(style: s128) {\\n    double\\n  }\\n  profilePicSm: profileImageSrcset(style: s32) {\\n    double\\n  }\\n  username\\n  createdAt\\n  roles\\n  lastSeenAt\\n  isPlatformPolice\\n}\\n\\nfragment ReactionFragment on Reaction {\\n  id\\n  isDeleted\\n  createdAt\\n  updatedAt\\n  feedId\\n  feedType\\n  postId\\n  postType\\n  userId\\n  reactionType\\n  score\\n  value\\n}","variables":{"feedId":"' + feed_id + '","feedType":"chat_feed","limit":' + str(limit) + ',"before":' + before_str + ',"direction":"desc","includeDeleted":false}}'

def get_universal_payload(limit: int, before_cursor: str = None) -> Dict:
    query = """
    query coreFetchUniversalPosts($feedType: UniversalPostFeedTypes!, $accessPassId: ID, $experienceId: ID, $limit: Int, $beforeCursor: ID, $afterCursor: ID, $appIds: [ID!], $internalOnlyShowGlobalFeed: Boolean) {
  universalPosts(
    feedType: $feedType
    accessPassId: $accessPassId
    experienceId: $experienceId
    limit: $limit
    beforeCursor: $beforeCursor
    afterCursor: $afterCursor
    appIds: $appIds
    internalOnlyShowGlobalFeed: $internalOnlyShowGlobalFeed
  ) {
    universalPosts {
      ...FeedUniversalPost
    }
    beforeCursor
    afterCursor
  }
}
    
    fragment FeedUniversalPost on UniversalPost {
  __typename
  experience {
    id
  }
  app {
    id
  }
  resource {
    __typename
    ... on FeedForumPostUniversalPost {
      forumPost {
        ...UniversalForumPost
      }
    }
    ... on FeedLivestreamFeedUniversalPost {
      livestreamFeed {
        ...UniversalLivestreamFeed
      }
    }
  }
}
    
    fragment UniversalForumPost on ForumPost {
  ...UniversalForumPostContent
  commentUsers(first: 4) {
    nodes {
      id
    }
    totalCount
  }
  comments(first: 2, depth: 1) {
    nodes {
      ...UniversalForumPostContent
    }
    totalCount
  }
}
    
    fragment UniversalForumPostContent on ForumPost {
  id
  createdAt
  title
  content
  richContent
  feedId
  commentCount
  viewCount
  pinned
  reactionCounts {
    reactionType
    userCount
    value
  }
  ownEmojiReactions: ownReactions(first: 1, reactionType: emoji) {
    nodes {
      value
    }
  }
  ownVoteReactions: ownReactions(first: 1, reactionType: vote) {
    nodes {
      value
    }
  }
  gifs {
    originalUrl
    url
    previewUrl
    width
    height
    slug
    title
    provider
  }
  lineItem {
    id
    amount
    redirectUrl
    baseCurrency
  }
  poll {
    options {
      id
      text
    }
  }
  muxAssets {
    id
    status
    signedPlaybackId
    playbackId
    signedThumbnailPlaybackToken
    signedVideoPlaybackToken
    signedStoryboardPlaybackToken
    durationSeconds
  }
  attachments {
    ...Attachment
  }
  userId
  isPosterAdmin
  parentId
  mentionedUserIds
  isDeleted
  isEdited
  user {
    id
    name
    username
    profilePicSm: profileImageSrcset(style: s32) {
        double
    }
  }
}
    
    fragment Attachment on AttachmentInterface {
  __typename
  id
  signedId
  analyzed
  byteSizeV2
  filename
  contentType
  source(variant: original) {
    url
  }
  ... on ImageAttachment {
    height
    width
    blurhash
    aspectRatio
  }
  ... on VideoAttachment {
    height
    width
    duration
    aspectRatio
    preview(variant: original) {
      url
    }
  }
  ... on AudioAttachment {
    duration
    waveformUrl
  }
}
    
    fragment UniversalLivestreamFeed on LivestreamFeed {
  title
  id
  thumbnailUrl
  startedAt
  endedAt
  host {
    id
  }
}
    """
    return {
        "query": query,
        "variables": {
            "appIds": ["app_dYfm2IdXhDMquv"],
            "feedType": "home",
            "limit": limit,
            "experienceId": "exp_JG1I58S5zTHbxs",
            "beforeCursor": before_cursor
        },
        "operationName": "coreFetchUniversalPosts"
    }

def _get_db_min_timestamp_after(after_ms: int) -> int:
    """获取数据库中在指定时间之后的最早一条消息的时间戳"""
    if not supabase:
        return 0
    try:
        # 查找 posted_at > after_ms 的消息，按 posted_at 升序排列，取第一条
        after_iso = _ms_to_iso(after_ms)
        resp = supabase.table('whop_posts')\
            .select('posted_at')\
            .gt('posted_at', after_iso)\
            .order('posted_at', desc=False)\
            .limit(1)\
            .execute()
            
        if resp.data:
            return _iso_to_ms(resp.data[0]['posted_at'])
    except Exception as e:
        logger.error(f"查询 DB Min Timestamp 失败: {e}")
    return 0

def get_latest_db_timestamp() -> int:
    """获取数据库中最新一条消息的时间戳"""
    if not supabase:
        return 0
    try:
        resp = supabase.table('whop_posts')\
            .select('posted_at')\
            .order('posted_at', desc=True)\
            .limit(1)\
            .execute()
            
        if resp.data:
            return _iso_to_ms(resp.data[0]['posted_at'])
    except Exception as e:
        logger.error(f"查询 DB Latest Timestamp 失败: {e}")
    return 0

def get_latest_universal_db_timestamp() -> int:
    """获取数据库中最新一条 Universal Post 的时间戳"""
    if not supabase:
        return 0
    try:
        resp = supabase.table('whop_universal_posts')\
            .select('posted_at')\
            .order('posted_at', desc=True)\
            .limit(1)\
            .execute()
            
        if resp.data:
            return _iso_to_ms(resp.data[0]['posted_at'])
    except Exception as e:
        logger.error(f"查询 Universal DB Latest Timestamp 失败: {e}")
    return 0

def _upsert_universal_posts_to_db(posts: List[Dict]) -> None:
    if not posts or not supabase:
        return
        
    data = []
    users_to_upsert = []
    
    for p_item in posts:
        resource = p_item.get('resource', {})
        p = resource.get('forumPost') or resource.get('forum_post')
        if not p: continue
        
        pid = p.get('id')
        if not pid: continue
        
        # Extract user info for whop_users table
        user_data = p.get('user')
        if user_data:
            users_to_upsert.append(user_data)
        
        try:
            data.append({
                'id': pid,
                'title': p.get('title'),
                'content': p.get('content'),
                'rich_content': p.get('richContent'),
                'feed_id': p.get('feedId'),
                'user_id': p.get('userId'),
                'comment_count': p.get('commentCount', 0),
                'view_count': p.get('viewCount', 0),
                'is_pinned': p.get('pinned', False),
                'reaction_counts': p.get('reactionCounts', []),
                'attachments': p.get('attachments', []),
                'mentioned_user_ids': p.get('mentionedUserIds', []),
                'is_deleted': p.get('isDeleted', False),
                'is_edited': p.get('isEdited', False),
                'posted_at': _ms_to_iso(p.get('createdAt')),
                'crawled_at': datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.warning(f"跳过格式错误的 Universal Post {pid}: {e}")
            continue
            
    if users_to_upsert:
        _upsert_users_to_db(users_to_upsert)
        
    if data:
        try:
            supabase.table('whop_universal_posts').upsert(data).execute()
        except Exception as e:
            logger.error(f"Supabase Universal Post 写入失败: {e}")

def get_universal_posts(
    limit: int, 
    before_cursor: Optional[str] = None, 
    stop_at_timestamp: Optional[int] = None,
    max_api_requests: int = 20,
    accumulate_results: bool = True
) -> List[Dict]:
    logger.info(f"准备获取 Universal 历史消息，limit={limit}，before_cursor={before_cursor}, stop_at={stop_at_timestamp}, max_req={max_api_requests}")
    
    history_items = []
    seen_ids = set()
    request_count = 0
    
    # Anti-Ban: Coffee Break Logic
    requests_since_break = 0
    next_break_threshold = random.randint(20, 40)
    
    next_before_cursor = before_cursor
    session_start_ts = None
    total_saved_count = 0
    
    target_str = datetime.fromtimestamp(stop_at_timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S') if stop_at_timestamp else '无限制'
    logger.info(f"🚀 开始同步 Universal 任务... (目标: {target_str})")

    universal_url = "https://whop.com/api/graphql/coreFetchUniversalPosts/"
    
    while len(history_items) < limit:
        if request_count >= max_api_requests:
            logger.warning(f"达到 API 请求次数上限 ({max_api_requests})，停止 API 拉取")
            break
            
        remaining = limit - len(history_items)
        page_limit = min(50, remaining)
        
        try:
            max_retries = 3
            for retry_attempt in range(max_retries):
                try:
                    payload = get_universal_payload(page_limit, next_before_cursor)
                    resp = requests.post(universal_url, headers=headers, json=payload, timeout=30)
                    
                    if resp.status_code == 429:
                        logger.critical("⚠️ 触发 API 限流 (429)！强制休眠 10 分钟...")
                        time.sleep(600)
                        raise Exception("API Rate Limit Hit (429) - Safety Stop")

                    resp.raise_for_status()
                    request_count += 1
                    requests_since_break += 1
                    break
                except Exception as e:
                    if retry_attempt < max_retries - 1:
                        wait_time = (retry_attempt + 1) * 5
                        logger.warning(f"API请求失败 ({e})，正在重试 ({retry_attempt + 1}/{max_retries})，等待 {wait_time} 秒...")
                        time.sleep(wait_time)
                    else:
                        raise e
            
            resp_json = resp.json()
            if 'errors' in resp_json:
                logger.error(f"GraphQL 错误: {resp_json['errors']}")
                break
                
            data = resp_json.get('data', {}).get('universalPosts', {})
            posts_page = data.get('universalPosts', [])
            next_before_cursor = data.get('beforeCursor')
            
            if not posts_page:
                logger.warning("API未返回更多 Universal 消息")
                break
                
            _upsert_universal_posts_to_db(posts_page)
            
            stop_fetch_signal = False
            for p_item in posts_page:
                resource = p_item.get('resource', {})
                post = resource.get('forumPost') or resource.get('forum_post')
                if not post: continue
                
                pid = post.get('id')
                created = int(post.get('createdAt', 0))
                
                if session_start_ts is None:
                    session_start_ts = created
                
                if stop_at_timestamp and created < stop_at_timestamp:
                    t_created = datetime.fromtimestamp(created/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                    t_stop = datetime.fromtimestamp(stop_at_timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                    logger.success(f"✅ 任务目标达成！当前消息时间 ({t_created}) 已早于设定目标 ({t_stop})，停止拉取。")
                    stop_fetch_signal = True
                    break
                
                if pid not in seen_ids:
                    if accumulate_results:
                        history_items.append(p_item)
                    else:
                        history_items.append({'id': pid, 'createdAt': created})
                    total_saved_count += 1
                    seen_ids.add(pid)
                
                if len(history_items) >= limit:
                    stop_fetch_signal = True
                    break
            
            # Progress Dashboard
            if (request_count % 5 == 0 or request_count == 1):
                try:
                    last_created = int((posts_page[-1].get('resource', {}).get('forumPost') or {}).get('createdAt', 0))
                    progress_pct = 0.0
                    if stop_at_timestamp and session_start_ts and session_start_ts > stop_at_timestamp:
                        total_range = session_start_ts - stop_at_timestamp
                        current_progress = session_start_ts - last_created
                        progress_pct = max(0.0, min(100.0, (current_progress / total_range) * 100))
                    
                    bar = '▓' * int(20 * progress_pct / 100) + '░' * (20 - int(20 * progress_pct / 100))
                    logger.info(f"\n [Universal 同步进度] {progress_pct:.1f}% {bar}\n 📊 累计保存: {total_saved_count} 条 | 请求: {request_count} 次")
                except: pass

            if stop_fetch_signal or not next_before_cursor:
                break
                
            # Anti-Ban
            if requests_since_break >= next_break_threshold:
                break_duration = random.randint(60, 180)
                logger.info(f"☕ Coffee Break: {break_duration}s...")
                time.sleep(break_duration)
                requests_since_break = 0
                next_break_threshold = random.randint(20, 40)
            else:
                time.sleep(random.uniform(4, 8))
            
        except Exception as e:
            logger.error(f"Universal API请求失败: {e}")
            break
            
    return history_items


def get_history_posts(
    limit: int, 
    before: Optional[int] = None, 
    is_whole_day: bool = False,
    stop_at_timestamp: Optional[int] = None,
    max_api_requests: int = 20,
    accumulate_results: bool = True,
    feed_id: str = "chat_feed_1CTr5VAdNHtbZAFaTitvoT",
    allowed_usernames: Optional[List[str]] = None
) -> Tuple[List[Dict], Dict[str, str]]:
    logger.info(f"准备获取历史消息 [{feed_id}]，limit={limit}，before={before}, stop_at={stop_at_timestamp}, max_req={max_api_requests}, allowed_users={allowed_usernames}")
    
    history_items = []
    seen_ids = set()
    
    should_fetch_api = True
    should_fetch_api = True
    request_count = 0
    
    # Anti-Ban: Coffee Break Logic
    requests_since_break = 0
    next_break_threshold = random.randint(20, 40)
    
    next_before = before
    session_start_ts = None # 用于计算进度百分比
    total_saved_count = 0 # 用于显示累计保存数量
    
    target_str = datetime.fromtimestamp(stop_at_timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S') if stop_at_timestamp else '无限制'
    logger.info(f"🚀 开始同步任务... (目标: {target_str})")

    
    while len(history_items) < limit and should_fetch_api:
        # 1. 安全检查
        if request_count >= max_api_requests:
            logger.warning(f"达到 API 请求次数上限 ({max_api_requests})，停止 API 拉取")
            break
            
        remaining = limit - len(history_items)
        # 模仿浏览器行为，使用 51 作为分页大小
        page_limit = min(51, remaining)
        
        # 转换 next_before 为可读时间
        next_before_str = "Latest"
        if next_before:
            try:
                next_before_str = datetime.fromtimestamp(next_before / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
            except:
                next_before_str = str(next_before)
        
        # 减少日志噪音：移除请求前的日志，改为请求后汇总
        # if request_count % 5 == 0:
        #    logger.info(f"请求API获取消息 (第 {request_count + 1} 次)，page_limit={page_limit}，当前进度: {next_before_str}")
        
        # Retry mechanism for API requests
        try:
            max_retries = 3
            for retry_attempt in range(max_retries):
                try:
                    payload = get_payload(page_limit, next_before, feed_id=feed_id)
                    resp = requests.request('POST', url, headers=headers, data=payload, timeout=30)
                    
                    if resp.status_code == 429:
                        logger.critical("⚠️ 触发 API 限流 (429 Too Many Requests)！")
                        logger.critical("为了账号安全，脚本将强制休眠 10 分钟...")
                        time.sleep(600)
                        # 休眠后抛出异常结束本次运行，人工检查更安全
                        raise Exception("API Rate Limit Hit (429) - Safety Stop")

                    resp.raise_for_status()
                    request_count += 1
                    requests_since_break += 1
                    break # Success, exit retry loop
                except (requests.exceptions.RequestException, requests.exceptions.SSLError) as e:
                    if "429" in str(e):
                         # Double check if 429 was caught as exception
                        logger.critical("⚠️ 触发 API 限流 (429)！强制休眠 10 分钟...")
                        time.sleep(600)
                        raise e

                    if retry_attempt < max_retries - 1:
                        wait_time = (retry_attempt + 1) * 5 # 5s, 10s, 15s
                        logger.warning(f"API请求失败 ({e})，正在重试 ({retry_attempt + 1}/{max_retries})，等待 {wait_time} 秒...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"API请求失败，已达到最大重试次数: {e}")
                        raise e # Re-raise to be caught by outer try-except
            
            try:
                json_response = resp.json()
                if 'errors' in json_response:
                    logger.error(f"GraphQL Errors: {json_response['errors']}")
                data = json_response['data']['feedPosts']
            except Exception as e:
                logger.error(f"解析响应失败: {e}, Response: {resp.text[:500]}")
                raise e
            user_json = data['users']
            posts_page = data['posts']
            
            if not posts_page:
                logger.warning("API未返回更多消息")
                break
                
            _upsert_users_to_db(user_json)
            
            # Filter posts if allowed_usernames is set
            filtered_posts_page = []
            if allowed_usernames:
                # Create a map of user_id -> username
                uid_to_username = {}
                for u in user_json:
                    uid = u.get('id')
                    uname = u.get('username')
                    if uid and uname:
                        uid_to_username[uid] = uname
                
                for post in posts_page:
                    uid = post.get('userId')
                    username = uid_to_username.get(uid)
                    if username and username in allowed_usernames:
                        filtered_posts_page.append(post)
                    else:
                        # Optional: Log filtered out posts
                        # logger.debug(f"Filtered out post from user {username} ({uid})")
                        pass
            else:
                filtered_posts_page = posts_page

            _upsert_posts_to_db(filtered_posts_page)
            
            min_created_this_page = None
            stop_fetch_signal = False
            
            for post in posts_page:
                pid = post.get('id')
                if not pid: continue
                
                created = int(post.get('createdAt', 0))
                
                if min_created_this_page is None or created < min_created_this_page:
                    min_created_this_page = created
                
                if before is not None and created >= before:
                    continue
                    
                if pid in seen_ids:
                    continue
                
                if stop_at_timestamp and created < stop_at_timestamp:
                    t_created = datetime.fromtimestamp(created/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                    t_stop = datetime.fromtimestamp(stop_at_timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                    logger.success(f"✅ 任务目标达成！当前消息时间 ({t_created}) 已早于设定目标 ({t_stop})，停止拉取。")
                    stop_fetch_signal = True
                    break
                
                if accumulate_results:
                    history_items.append(post)
                # 即使不累积，也需要记录 ID 以避免重复处理（如果 limit 较小）
                # 但对于大数据量同步，seen_ids 也可能过大，这里暂且保留，
                # 因为 seen_ids 仅存 ID 字符串，百万级也就几十 MB，可接受。
                
                # 如果不累积，我们需要手动维护一个计数器来判断是否达到 limit
                # 但外层循环是用 len(history_items) 判断的。
                # 修正：如果不累积，我们无法用 len(history_items) 准确控制 limit。
                # 但通常不累积模式下，limit 都是设得极大，主要靠 stop_at 停止。
                # 为了兼容，如果 accumulate_results=False，我们往 history_items 放一个空占位符或者仅放 ID？
                # 不，为了内存最优化，我们最好改用 total_processed_count 计数。
                # 简单起见，如果 accumulate_results=False，我们就不 append 到 history_items，
                # 但是为了让 `while len(history_items) < limit` 循环继续，我们需要一种方式。
                # 方案：引入 total_fetched 变量。
                
                # 修正逻辑：
                # 1. 始终维护 seen_ids
                # 2. 始终维护 total_fetched (在外部) -> 实际上 len(history_items) 就是。
                # 如果 accumulate_results=False，我们就不 append full object。
                # 我们可以 append 一个极小的占位符，比如 1。
                if accumulate_results:
                    history_items.append(post)
                else:
                    # 仅计数，为了让循环条件 len(history_items) < limit 正常工作
                    # 同时为了最后能返回点东西（虽然没用），append 一个轻量级对象
                    history_items.append({'id': pid, 'createdAt': created}) # 最小化存储
                    total_saved_count += 1
                
                seen_ids.add(pid)
                
                if len(history_items) >= limit:
                    stop_fetch_signal = True
                    break
            
            # --- Smart Jump 核心逻辑 (Gap Detection Strategy) ---
            if not stop_fetch_signal and min_created_this_page and stop_at_timestamp:
                try:
                    # 检查 min_created_this_page (本页最老) 是否在 DB 中有“接续”
                    # 定义“接续”的阈值：比如 12 小时内有消息
                    CHECK_RANGE_HOURS = 12
                    check_limit_ts = min_created_this_page - (CHECK_RANGE_HOURS * 3600 * 1000)
                    
                    if check_limit_ts < stop_at_timestamp:
                        check_limit_ts = stop_at_timestamp
                        
                    # 查询 DB: 在 (check_limit_ts, min_created_this_page) 范围内是否有消息？
                    # 策略：
                    # 1. 从 min_created_this_page 开始，向后（更旧）扫描 DB。
                    # 2. 寻找第一个“时间断层”（Gap）。
                    #    Gap 定义：两条相邻消息的时间差 > GAP_THRESHOLD (例如 6 小时)
                    # 3. 如果找到 Gap，跳到 Gap 的开始时间（即较晚的那条消息的时间）。
                    # 4. 如果没找到 Gap（扫描了一定范围），说明这一段都很密集，直接跳到扫描到的最远端。
                    
                    GAP_THRESHOLD_MS = 6 * 3600 * 1000  # 6 小时
                    SCAN_LIMIT = 5000 # 提高扫描效率：每次扫描 5000 条 DB 记录
                    
                    # 获取一批更旧的消息的时间戳
                    scan_query = supabase.table('whop_posts')\
                        .select('posted_at')\
                        .lt('posted_at', _ms_to_iso(min_created_this_page))\
                        .gt('posted_at', _ms_to_iso(stop_at_timestamp))\
                        .order('posted_at', desc=True)\
                        .limit(SCAN_LIMIT)
                        
                    scan_resp = scan_query.execute()
                    scan_data = scan_resp.data
                    
                    if scan_data:
                        # 找到了更旧的数据，开始寻找 Gap
                        jump_target_ts = None
                        last_ts = min_created_this_page
                        
                        for row in scan_data:
                            curr_ts = _iso_to_ms(row['posted_at'])
                            delta = last_ts - curr_ts
                            
                            if delta > GAP_THRESHOLD_MS:
                                # 发现断层！
                                jump_target_ts = last_ts
                                t_readable = datetime.fromtimestamp(jump_target_ts/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                                logger.info(f"Smart Jump: 发现时间断层 ({delta/3600000:.1f}h)，准备跳跃到断层边缘: {t_readable}")
                                break
                            
                            last_ts = curr_ts
                            
                        if jump_target_ts:
                            min_created_this_page = jump_target_ts
                        else:
                            # 没发现断层，说明这 5000 条数据是连续的。
                            deepest_ts = _iso_to_ms(scan_data[-1]['posted_at'])
                            t_readable = datetime.fromtimestamp(deepest_ts/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                            logger.info(f"Smart Jump: 未发现断层，安全跳过 {len(scan_data)} 条本地数据，到达: {t_readable}")
                            min_created_this_page = deepest_ts
                            
                except Exception as e:
                    logger.error(f"Smart Jump Check Failed: {e}")

            # --- Log Progress Dashboard ---
            if posts_page:
                # Update session start on first batch
                p_newest = int(posts_page[0].get('createdAt', 0))
                if session_start_ts is None:
                    session_start_ts = p_newest

                if (request_count % 5 == 0 or request_count == 1):
                    try:
                        p_oldest = int(posts_page[-1].get('createdAt', 0))
                        
                        # Calculate Progress
                        progress_pct = 0.0
                        if stop_at_timestamp and session_start_ts and session_start_ts > stop_at_timestamp:
                            total_range = session_start_ts - stop_at_timestamp
                            current_progress = session_start_ts - p_oldest
                            progress_pct = (current_progress / total_range) * 100
                            progress_pct = max(0.0, min(100.0, progress_pct))
                        
                        # Progress Bar
                        bar_len = 20
                        filled_len = int(bar_len * progress_pct / 100)
                        bar = '▓' * filled_len + '░' * (bar_len - filled_len)
                        
                        fmt = '%Y-%m-%d %H:%M'
                        t_current = datetime.fromtimestamp(p_oldest/1000.0).strftime(fmt)
                        t_target = datetime.fromtimestamp(stop_at_timestamp/1000.0).strftime(fmt) if stop_at_timestamp else "Inf"
                        
                        break_countdown = next_break_threshold - requests_since_break
                        
                        msg = (
                            f"\n{'='*50}\n"
                            f" [同步进度] {progress_pct:.1f}% {bar}\n"
                            f" 📅 当前位置: {t_current}  -->  目标: {t_target}\n"
                            f" 📊 累计保存: {total_saved_count} 条 | 本次请求: {request_count} 次\n"
                            f" ☕ 状态: 正常抓取中 (再过 {break_countdown} 次请求休息)\n"
                            f"{'='*50}"
                        )
                        logger.info(msg)
                    except Exception as e:
                        logger.warning(f"日志打印出错: {e}")


            if stop_fetch_signal:
                break
                
            next_before = min_created_this_page
            if next_before is None:
                break
                
            # --- Anti-Ban: Random Delay & Coffee Break ---
            
            # 1. Check Coffee Break
            if requests_since_break >= next_break_threshold:
                break_duration = random.randint(60, 180) # 1-3 minutes
                logger.info(f"☕ 喝咖啡时间 (Coffee Break): 已连续请求 {requests_since_break} 次，休息 {break_duration} 秒...")
                time.sleep(break_duration)
                requests_since_break = 0
                next_break_threshold = random.randint(20, 40) # Reset threshold
            else:
                # 2. Normal Random Delay (Increased for safety)
                sleep_time = random.uniform(4, 8) # 4-8 seconds
                time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"API请求失败: {e}")
            break
    
    # 补齐逻辑 (Smart Jump 模式下通常不需要补齐，因为我们是跳跃式拉取)
    # 但如果最后一段是在 DB 里，我们跳到了 stop_at，循环结束。
    # 如果最后一段不在 DB 里，我们拉到了 stop_at，循环结束。
    # 所以这里不需要额外的 DB 补齐逻辑，除非是为了满足 limit (但现在 limit 很大)。
    # 为了保持兼容性，我们保留简单的排序返回。
            
    history_items = sorted(history_items, key=lambda p: int(p.get('createdAt', 0)), reverse=True)
    
    user_ids = {p.get('userId') for p in history_items if p.get('userId')}
    users_cache = _get_users_map_from_db(list(user_ids))
    
    if is_whole_day:
        # 这个逻辑在 Smart Sync 下可能不太适用，但保留
        pass
        
    logger.info(f"最终返回消息数量：{len(history_items)}")
    return history_items, users_cache
