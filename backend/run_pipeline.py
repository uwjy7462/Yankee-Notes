import subprocess
import sys
import os
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:MM:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")

def run_script(script_name, args=None):
    """Run a python script and check for errors."""
    if args is None:
        args = []
    
    # Get the absolute path to the script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(base_dir, script_name)
    
    cmd = [sys.executable, script_path] + args
    
    logger.info(f"🚀 Starting: {script_name} {' '.join(args)}")
    
    try:
        result = subprocess.run(cmd, check=True, text=True)
        logger.info(f"✅ Finished: {script_name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed: {script_name} (Exit Code: {e.returncode})")
        sys.exit(e.returncode)
    except Exception as e:
        logger.error(f"❌ Error running {script_name}: {e}")
        sys.exit(1)

def main():
    logger.info("==================================================")
    logger.info("       Starting Whop Data Pipeline")
    logger.info("==================================================")

    # Pass any arguments from this script to the child scripts if needed
    # For now, we just pass them to whop_summary.py if it's the --force flag
    extra_args = sys.argv[1:]
    
    # 1. Sync Raw Data (Parallelizable in theory, but sequential is safer for logs)
    run_script("whop_sync.py")
    run_script("sync_universal_posts.py")

    # 2. Transform & Aggregate Data (Depends on 1)
    run_script("sync_vip_posts.py")

    # 3. Generate Summary (Depends on 2)
    # We pass any extra arguments (like --force) to the summary script
    run_script("whop_summary.py", args=extra_args)

    logger.info("==================================================")
    logger.info("       🎉 Pipeline Completed Successfully")
    logger.info("==================================================")

if __name__ == "__main__":
    main()
