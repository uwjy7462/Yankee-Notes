import os
import subprocess
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")

supabase: Client = create_client(url, key)

DOCS_DIR = "docs"

def fetch_summaries():
    """Fetch summaries from the database sorted by created_at descending."""
    response = supabase.table("whop_summaries").select("*").order("created_at", desc=True).execute()
    return response.data

def generate_markdown(summary):
    """Generate Markdown content from a summary object."""
    title = summary.get("title") or "Untitled"
    description = summary.get("description") or ""
    content = summary.get("content") or ""
    created_at_str = summary.get("created_at")
    
    # Parse created_at to get date for filename and folder
    if created_at_str:
        try:
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        except ValueError:
            created_at = datetime.now()
    else:
        created_at = datetime.now()

    date_str = created_at.strftime("%Y-%m-%d")
    month_str = created_at.strftime("%Y-%m")
    
    # Sanitize title for filename
    # User requested to use the DB title as is.
    # We only replace / and : which are problematic on filesystems/Finder.
    safe_title = title.replace('/', '-').replace(':', '-').strip()
    
    filename = f"{safe_title}.md"
    
    markdown_content = f"""
> {description}

{content}
"""
    return filename, month_str, markdown_content

def save_file(filename, month_str, content):
    """Save the markdown content to a file in a month-based directory."""
    directory = os.path.join(DOCS_DIR, month_str)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    filepath = os.path.join(directory, filename)
    
    # Only write if file doesn't exist or content is different (optional optimization, 
    # but here we just overwrite to ensure latest version)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    
    return filepath

def git_commit_and_push():
    """Commit and push changes to Git."""
    try:
        # Add changes
        subprocess.run(["git", "add", DOCS_DIR], check=True)
        
        # Check if there are changes to commit
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not status.stdout.strip():
            print("No changes to commit.")
            return

        # Commit
        date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subprocess.run(["git", "commit", "-m", f"Update summaries: {date_str}"], check=True)
        
        # Push
        subprocess.run(["git", "push"], check=True)
        print("Successfully pushed changes to remote.")
        
    except subprocess.CalledProcessError as e:
        print(f"Git operation failed: {e}")

def clean_orphaned_files(valid_files):
    """Delete files in DOCS_DIR that are not in the valid_files list."""
    print("Checking for orphaned files...")
    deleted_count = 0
    
    # Walk through the docs directory
    for root, dirs, files in os.walk(DOCS_DIR):
        for file in files:
            if not file.endswith(".md") or file == "index.md":
                continue
                
            filepath = os.path.join(root, file)
            # Check if this file was generated in this run
            if filepath not in valid_files:
                try:
                    os.remove(filepath)
                    print(f"Deleted orphaned file: {filepath}")
                    deleted_count += 1
                except OSError as e:
                    print(f"Error deleting {filepath}: {e}")
                    
    # Clean up empty directories
    for root, dirs, files in os.walk(DOCS_DIR, topdown=False):
        for name in dirs:
            dir_path = os.path.join(root, name)
            if not os.listdir(dir_path):  # Check if directory is empty
                try:
                    os.rmdir(dir_path)
                    print(f"Removed empty directory: {dir_path}")
                except OSError as e:
                    print(f"Error removing directory {dir_path}: {e}")

    print(f"Cleanup complete. Deleted {deleted_count} files.")

def main():
    print("Fetching summaries...")
    summaries = fetch_summaries()
    
    print(f"Found {len(summaries)} summaries. Generating Markdown files...")
    
    # Keep track of all files generated in this run
    generated_files = set()
    
    for summary in summaries:
        filename, month_str, content = generate_markdown(summary)
        filepath = save_file(filename, month_str, content)
        generated_files.add(filepath)
        
    # Clean up files that exist locally but are no longer in the DB
    clean_orphaned_files(generated_files)
        
    print("Files generated. Committing to Git...")
    git_commit_and_push()

if __name__ == "__main__":
    main()
