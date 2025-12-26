import os
import sys
from loguru import logger
from supabase import create_client, Client
from utils.local_secrets import supabase_url, supabase_key

# Initialize Supabase
try:
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    logger.error(f"Supabase init failed: {e}")
    sys.exit(1)

def list_summaries():
    try:
        resp = supabase.table("whop_summaries").select("title, created_at").order("created_at", desc=True).limit(20).execute()
        print(f"{'Created At':<30} | {'Title'}")
        print("-" * 80)
        for row in resp.data:
            print(f"{row['created_at']:<30} | {row['title']}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_summaries()
