"""Entry point for target-vendit."""

import logging
import sys
import os
from pathlib import Path
from target_vendit.target import TargetVendit

# Set up logging to see all messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    stream=sys.stderr
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting target-vendit")
    logger.info("=" * 80)
    
    # Check if stdin is a TTY (interactive) or empty
    if sys.stdin.isatty():
        logger.warning("stdin is a TTY (interactive terminal) - no input will be available")
    else:
        logger.info("stdin is available (piped input)")
    
    # Log environment info
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Python executable: {sys.executable}")
    logger.info(f"Command line args: {sys.argv}")
    
    try:
        TargetVendit.cli()
    except Exception as e:
        logger.error(f"Fatal error in target-vendit: {e}", exc_info=True)
        raise
    finally:
        logger.info("=" * 80)
        logger.info("target-vendit completed")
        logger.info("=" * 80)
