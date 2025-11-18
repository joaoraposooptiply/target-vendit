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
    stdin_is_tty = sys.stdin.isatty()
    if stdin_is_tty:
        logger.warning("stdin is a TTY (interactive terminal) - no input will be available from stdin")
    else:
        logger.info("stdin is available (piped input)")
    
    # Log environment info
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Python executable: {sys.executable}")
    logger.info(f"Command line args: {sys.argv}")
    
    # If stdin is empty, try to find and use input file
    if stdin_is_tty:
        logger.info("stdin is empty, checking for input file...")
        
        # First, try to get input_path from config by parsing it manually
        # (We can't access config before cli() runs, so we'll check common paths)
        # Try common locations for data.singer
        common_paths = [
            Path("etl-output/data.singer"),
            Path("etl-output/data.singer/data.singer"),
            Path("../etl-output/data.singer"),
            Path("../etl-output/data.singer/data.singer"),
            Path("data.singer"),
            Path("etl-output"),
        ]
        
        input_file = None
        for path in common_paths:
            if path.exists():
                if path.is_file():
                    logger.info(f"Found data.singer at: {path}")
                    logger.info(f"Input file size: {path.stat().st_size} bytes")
                    input_file = path
                    break
                elif path.is_dir():
                    # Check if it's a directory containing data.singer
                    data_singer = path / "data.singer"
                    if data_singer.exists():
                        logger.info(f"Found data.singer at: {data_singer}")
                        logger.info(f"Input file size: {data_singer.stat().st_size} bytes")
                        input_file = data_singer
                        break
        
        if input_file:
            # Redirect stdin to the file
            logger.info(f"Reading from file: {input_file}")
            logger.info(f"File absolute path: {input_file.absolute()}")
            with open(input_file, 'r') as f:
                sys.stdin = f
                try:
                    TargetVendit.cli()
                finally:
                    # Restore stdin
                    sys.stdin = sys.__stdin__
        else:
            logger.error("stdin is empty and could not find data.singer in any common location")
            logger.error("Tried paths:")
            for path in common_paths:
                abs_path = path.absolute()
                exists = path.exists()
                logger.error(f"  - {abs_path} (exists: {exists})")
            # Still try to run cli() - it will process 0 lines but at least we'll see the logs
            logger.warning("Running target with empty stdin - will process 0 lines")
            TargetVendit.cli()
    else:
        # Normal case: stdin was piped
        try:
            TargetVendit.cli()
        except Exception as e:
            logger.error(f"Fatal error in target-vendit: {e}", exc_info=True)
            raise
    
    logger.info("=" * 80)
    logger.info("target-vendit completed")
    logger.info("=" * 80)
