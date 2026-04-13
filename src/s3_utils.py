#!/usr/bin/env python3
\"\""
S3 utility functions for path construction and timestamp extraction.
\"\"\"

import os
import re
from typing import Tuple
from .logging import log

def get_bucket_name_path_from_url(s3_url: str) -> Tuple[str, str]:
    \"\"\"
    Extracts S3 bucket name and object path from an S3 URL.
    
    Args:
        s3_url: S3 URL in format s3://bucket-name/path/to/object
        
    Returns:
        Tuple of (bucket_name, object_path)
        
    Raises:
        ValueError: Invalid URL format
    \"\"\"
    if not s3_url.startswith(\"s3://\"):
        raise ValueError(f\"Invalid S3 URL format: {s3_url}\")
    parts = s3_url[5:].split('/', 1)
    bucket_name = parts[0]
    bucket_path = parts[1] if len(parts) > 1 else \"\"
    return bucket_name, bucket_path

def get_s3_path(s3_prefix: str, rel_path: str) -> str:
    \"\"\"Constructs full S3 object path.\"\"\"

    if not s3_prefix.endswith('/'):
        s3_prefix += \"/\"
    if rel_path.startswith('/'):
        rel_path = rel_path[1:]
    return s3_prefix + rel_path

def get_file_duration(file_path_or_name: str) -> int:
    \"\"\"Estimates file duration from filename pattern 'start-end.es'. Returns 4s default.\"\"\"

    filename = os.path.basename(file_path_or_name)
    match = re.match(r'(\\d+)-(\\d+)\\.es$', filename)
    if match:
        try:
            start_epoch = int(match.group(1))
            end_epoch = int(match.group(2))
            duration = end_epoch - start_epoch
            if duration <= 0:
                log.warning(f\"Invalid duration ({duration}s) from {filename}. Using 4s.\")
                return 4
            return duration
        except (ValueError, OverflowError) as e:
            log.warning(f\"Parse error in {filename}: {e}. Using 4s.\")
            return 4
    log.debug(f\"No pattern match for {filename}. Using 4s.\")
    return 4

def get_file_path_to_read(base_utc: int) -> str:
    \"\"\"Calculates expected relative file path for UTC timestamp (4s chunks).\"\"\"

    seconds_past_hour = base_utc % 3600
    seconds_into_interval = seconds_past_hour % 4
    interval_start_utc = base_utc - seconds_into_interval
    
    start_epoch = interval_start_utc
    end_epoch = start_epoch + 4
    
    from datetime import datetime, timezone
    dt_obj = datetime.fromtimestamp(start_epoch, timezone.utc)
    date_str = dt_obj.strftime(\"%d%m%Y\")
    hour_str = dt_obj.strftime(\"%H\")
    
    return f\"{date_str}/{hour_str}/{start_epoch}-{end_epoch}.es\"

def get_start_utc_from_filename(filename: str) -> int:
    \"\"\"Extracts start UTC epoch from filename like '.../12345-67890.es'.\"\"\"

    basename = os.path.basename(filename)
    match = re.match(r'(\\d+)-(\\d+)\\.es$', basename)
    if match:
        try:
            timestamp = int(match.group(1))
            if timestamp <= 0:
                log.warning(f\"Invalid timestamp ({timestamp}) in {basename}\")
                return 0
            return timestamp
        except (ValueError, OverflowError) as e:
            log.error(f\"Parse error in {basename}: {e}\")
            return 0
    log.debug(f\"No pattern match for {basename}\")
    return 0

def convert_pcr_27mhz_to_pcr_ns(pcr_27mhz: int) -> int:
    \"\"\"Converts PCR 27MHz value to nanoseconds.\"\"\"

    if pcr_27mhz < 0:
        return 0
    return (int(pcr_27mhz) * 1000) // 27

def format_datetime(utc_timestamp: int) -> str:
    \"\"\"Formats UTC timestamp to human-readable string.\"\"\"

    from datetime import datetime, timezone
    try:
        return datetime.fromtimestamp(utc_timestamp, timezone.utc).strftime(\"%Y-%m-%d %H:%M:%S UTC\")
    except (ValueError, OSError, OverflowError) as e:
        from .logging import log
        log.warning(f\"Invalid timestamp {utc_timestamp}: {e}\")
        return f\"Invalid Timestamp ({utc_timestamp})\"