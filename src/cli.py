#!/usr/bin/env python3
\"\"\"CLI argument parsing and validation.\"\"\"

import argparse
from typing import Namespace
from pathlib import Path
from .config_manager import ConfigManager
from .logging import log

def parse_args() -> Namespace:
    \"\"\"Parse and validate CLI arguments.\"\"\"

    parser = argparse.ArgumentParser(
        description='S3 ES Downloader & Parser v3.1',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s config.json output.ts
  %(prog)s config.json output.ts --gofile
  %(prog)s config.json output.ts --sendgb --debug
        '''
    )
    
    parser.add_argument('config', type=str, help='Path to config.json')
    parser.add_argument('output', type=str, help='Output TS file path')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--gofile', action='store_true', help='Upload to GoFile')
    group.add_argument('--sendgb', action='store_true', help='Upload to SendGB (GoFile fallback)')
    
    parser.add_argument('--sendgb-wait', type=int, default=600, help='SendGB wait timeout (s)')
    parser.add_argument('--workers', type=int, help='Max concurrent downloads')
    parser.add_argument('--temp-dir', type=str, help='Custom temp directory')
    parser.add_argument('--resume-state', type=str, help='Resume from state file')
    parser.add_argument('--dry-run', action='store_true', help='List files only')
    parser.add_argument('-d', '--debug', action='store_true', help='Debug logging')
    
    args = parser.parse_args()
    
    # Validation
    if not Path(args.config).exists():
        log.error(f"Config not found: {args.config}")
        raise SystemExit(1)
    
    return args