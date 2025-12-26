import os
from dotenv import load_dotenv

# Load environment variables from .env file
# We look for .env in the project root (parent of backend) or current directory
load_dotenv()

# OpenAI / Gemini Configuration
openai_api_key = os.getenv("OPENAI_API_KEY")
openai_base_url = os.getenv("OPENAI_BASE_URL")
ai_model_name = os.getenv("AI_MODEL_NAME", "gemini-2.5-pro")

# Whop Configuration
whop_authorization = os.getenv("WHOP_AUTHORIZATION")
whop_cookie = os.getenv("WHOP_COOKIE")

import json
whop_feeds_env = os.getenv("WHOP_FEEDS")
if whop_feeds_env:
    try:
        whop_feeds = json.loads(whop_feeds_env)
    except Exception as e:
        print(f"Error parsing WHOP_FEEDS: {e}")
        whop_feeds = []
else:
    # Default configuration if env var is not set
    whop_feeds = [
        {
            "feed_id": "chat_feed_1CTr5VAdNHtbZAFaTitvoT",
            "allowed_usernames": None  # All users
        },
        {
            "feed_id": "chat_feed_1CU95KbtifP1JtuqTiVXZb",
            "allowed_usernames": ["xiaozhaolucky"]
        }
    ]

whom_headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "authorization": whop_authorization,
    "content-type": "application/json",
    "cookie": whop_cookie,
    "origin": "https://whop.com",
    "referer": "https://whop.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

# Supabase Configuration
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
