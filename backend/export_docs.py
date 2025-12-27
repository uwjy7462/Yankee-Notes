
import os
import datetime
import re
from loguru import logger
from utils.local_secrets import supabase_url, supabase_key
from supabase import create_client, Client

# Initialize Supabase
supabase: Client = create_client(supabase_url, supabase_key)

DOCS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../docs"))

def sanitize_filename(name):
    """Sanitize the string to be safe for filenames."""
    if not name:
        return "untitled"
    # Remove invalid characters
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    # Replace spaces with underscores or hyphens
    return name[:100]  # Limit length

def export_summaries():
    logger.info("Starting export of summaries to Markdown...")
    
    try:
        # Fetch all summaries
        # Note: If there are many rows, we might need pagination. 
        # For now, assuming < 1000 rows or adjusting limit.
        resp = supabase.table("whop_summaries").select("*").order("created_at", desc=True).execute()
        summaries = resp.data
        
        logger.info(f"Fetched {len(summaries)} summaries.")
        
        for summary in summaries:
            created_at_str = summary.get("created_at")
            if not created_at_str:
                continue
                
            # Parse date
            dt = datetime.datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            date_folder = dt.strftime('%Y-%m')
            
            # Create directory
            target_dir = os.path.join(DOCS_DIR, date_folder)
            os.makedirs(target_dir, exist_ok=True)
            
            # Prepare filename
            title = summary.get("title") or "summary"
            filename = f"{sanitize_filename(title)}.md"
            file_path = os.path.join(target_dir, filename)
            
            # Prepare content
            content = summary.get("content") or ""
            
            # Optional: Add Frontmatter for Jekyll/Hugo/MkDocs
            frontmatter = f"""---
title: "{title}"
date: {created_at_str}
description: "{summary.get('description', '')}"
tags: {summary.get('tags', [])}
---

"""
            full_content = frontmatter + content
            
            # Write to file
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(full_content)
                
        logger.info(f"Successfully exported summaries to {DOCS_DIR}")
        
    except Exception as e:
        logger.error(f"Export failed: {e}")

if __name__ == "__main__":
    export_summaries()
