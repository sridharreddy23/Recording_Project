#!/usr/bin/env python3
"""
Utility functions for the ES Downloader and Parser.
"""
import os
import re
import datetime as dt
import logging
import sys
import json
from typing import Tuple, Dict, Any, List
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

def format_datetime(utc_timestamp: int) -> str:
    """
    Formats a UTC timestamp into a human-readable string.
    
    Args:
        utc_timestamp: Unix timestamp in seconds
        
    Returns:
        Formatted datetime string in UTC
        
    Examples:
        >>> format_datetime(1672567200)
        '2023-01-01 12:00:00 UTC'
    """
    try:
        return dt.datetime.fromtimestamp(utc_timestamp, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, OSError, OverflowError) as e:
        log.warning(f"Invalid timestamp {utc_timestamp}: {e}")
        return f"Invalid Timestamp ({utc_timestamp})"

def get_bucket_name_path_from_url(s3_url: str) -> Tuple[str, str]:
    """
    Extracts S3 bucket name and object path from an S3 URL.
    
    Args:
        s3_url: S3 URL in the format s3://bucket-name/path/to/object
        
    Returns:
        Tuple of (bucket_name, object_path)
        
    Raises:
        ValueError: If the URL format is invalid
    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 URL format: {s3_url}")
    parts = s3_url[5:].split('/', 1)
    bucket_name = parts[0]
    bucket_path = parts[1] if len(parts) > 1 else ""
    return bucket_name, bucket_path

def get_s3_path(s3_prefix: str, rel_path: str) -> str:
    """
    Constructs the full S3 object path.
    
    Args:
        s3_prefix: S3 prefix (s3://bucket-name/path)
        rel_path: Relative path to append
        
    Returns:
        Full S3 path
    """
    # Ensure prefix ends with a slash
    if not s3_prefix.endswith('/'):
        s3_prefix += "/"
    # Ensure relative path doesn't start with a slash
    if rel_path.startswith('/'):
        rel_path = rel_path[1:]
    return s3_prefix + rel_path

def get_file_duration(file_path_or_name: str) -> int:
    """
    Estimates file duration in seconds based on filename pattern 'start-end.es'.
    
    Args:
        file_path_or_name: Path or filename to parse
        
    Returns:
        Duration in seconds, or default (4 seconds) if parsing fails
    """
    filename = os.path.basename(file_path_or_name)
    match = re.match(r'(\d+)-(\d+)\.es$', filename)
    if match:
        try:
            start_epoch = int(match.group(1))
            end_epoch = int(match.group(2))
            duration = end_epoch - start_epoch
            if duration <= 0:
                log.warning(f"Invalid duration ({duration}s) from filename: {filename}. Using default duration 4s.")
                return 4
            return duration
        except (ValueError, OverflowError) as e:
            log.warning(f"Could not parse epochs from filename {filename}: {e}. Using default duration 4s.")
            return 4
    else:
        log.debug(f"Filename '{filename}' does not match expected pattern 'start-end.es'. Using default duration 4s.")
        return 4  # Default duration if pattern doesn't match

def get_file_path_to_read(base_utc: int) -> str:
    """
    Calculates the expected relative file path based on a UTC timestamp.
    Assumes 4-second file chunks aligned to multiples of 4 seconds.
    
    Args:
        base_utc: UTC timestamp in seconds
        
    Returns:
        Relative file path (e.g., "20230101/12/1672567200-1672567204.es")
    """
    # Find the start of the 4-second interval the timestamp falls into
    seconds_past_hour = base_utc % 3600
    seconds_into_interval = seconds_past_hour % 4
    interval_start_utc = base_utc - seconds_into_interval

    # Use the *start* of the interval to determine the file name
    start_epoch = interval_start_utc
    end_epoch = start_epoch + 4  # Assuming 4 second files

    dt_obj = dt.datetime.fromtimestamp(start_epoch, dt.timezone.utc)
    date_str = dt_obj.strftime("%d%m%Y")
    hour_str = dt_obj.strftime("%H")  # Zero-padded hour

    file_path = f"{date_str}/{hour_str}/{start_epoch}-{end_epoch}.es"
    return file_path

def get_start_utc_from_filename(filename: str) -> int:
    """
    Extracts the start UTC epoch second from a filename like '.../12345-67890.es'.
    
    Args:
        filename: Path or filename to parse
        
    Returns:
        Start UTC timestamp in seconds, or 0 if parsing fails
    """
    basename = os.path.basename(filename)
    match = re.match(r'(\d+)-(\d+)\.es$', basename)
    if match:
        try:
            timestamp = int(match.group(1))
            if timestamp <= 0:
                log.warning(f"Invalid timestamp ({timestamp}) from filename: {basename}")
                return 0
            return timestamp
        except (ValueError, OverflowError) as e:
            log.error(f"Could not parse start epoch from filename {basename}: {e}")
            return 0
    log.debug(f"Filename pattern not matched for start epoch extraction: {basename}")
    return 0

def convert_pcr_27mhz_to_pcr_ns(pcr_27mhz: int) -> int:
    """
    Converts a 27MHz PCR value to nanoseconds.
    
    Args:
        pcr_27mhz: PCR value at 27MHz
        
    Returns:
        PCR value in nanoseconds
    """
    if pcr_27mhz < 0: 
        return 0  # Handle potential weird values
    return (int(pcr_27mhz) * 1000) // 27  # Integer division for nanoseconds

def print_banner():
    """Prints a formatted application banner."""
    banner = f"""
{Fore.CYAN}╔═══════════════════════════════════════════════════╗
{Fore.CYAN}║ {Fore.YELLOW}    _      _                                   {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   | |    (_)                                  {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   | |     _ _   _____  ___                    {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   | |    | | | / / _ \\/ _ \\                   {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   | |____| | |/ /  __/  __/                   {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   |______|_|___/ \\___|\\___/                   {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}      S3 ES Downloader & Parser                {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.GREEN}             Enhanced Version v3.0              {Fore.CYAN} ║
{Fore.CYAN}╚═══════════════════════════════════════════════════╝
{Fore.YELLOW}[Press Ctrl+C at any time to exit the program]
"""
    print(banner)

def print_section_header(title: str):
    """
    Prints a formatted section header.
    
    Args:
        title: Header title text
    """
    print(f"\n{Fore.CYAN}╔{'═' * (len(title) + 8)}╗")
    print(f"{Fore.CYAN}║    {Fore.YELLOW}{title}{Fore.CYAN}    ║")
    print(f"{Fore.CYAN}╚{'═' * (len(title) + 8)}╝{Style.RESET_ALL}")

def print_final_success():
    """Prints a final success message box."""
    print(f"\n{Fore.GREEN}╔{'═' * 45}╗")
    print(f"{Fore.GREEN}║{' ' * 45}║")
    print(f"{Fore.GREEN}║   {Fore.YELLOW}Process completed successfully!{' ' * 10}{Fore.GREEN}║")
    print(f"{Fore.GREEN}║{' ' * 45}║")
    print(f"{Fore.GREEN}╚{'═' * 45}╝{Style.RESET_ALL}")

def print_progress(current: int, total: int, prefix: str = "", suffix: str = "", length: int = 50) -> None:
    """
    Prints or updates a console progress bar.
    
    Args:
        current: Current progress value
        total: Total value for 100% progress
        prefix: Text before the progress bar
        suffix: Text after the progress bar
        length: Width of the progress bar in characters (default: 50)
        
    Note:
        This function overwrites the current line. Call with current >= total
        to finalize and print a newline.
    """
    if total <= 0:  # Avoid division by zero and handle negative totals
        percent = 100
        bar_fill = length
    else:
        percent = min(100, int(100 * (current / float(total))))
        bar_fill = min(length, int(length * current // total))

    bar = f"{Fore.GREEN}{'█' * bar_fill}{Fore.WHITE}{'░' * (length - bar_fill)}"
    # Use \r to return to the start of the line, overwrite previous progress
    sys.stdout.write(f"\r{prefix} |{bar}| {percent}% {suffix}")
    sys.stdout.flush()
    if current >= total:  # Print newline when done
        sys.stdout.write("\n")
        sys.stdout.flush()

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validates the configuration dictionary.

    Args:
        config: Configuration dictionary

    Returns:
        True if configuration is valid

    Raises:
        ValueError: If configuration is invalid
        TypeError: If configuration has wrong types
    """
    if not isinstance(config, dict):
        raise TypeError(f"Configuration must be a dictionary, got {type(config)}")
    
    # Check required keys
    required_keys = ["start_utc", "end_utc", "s3_prefix", "aws_conf"]
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
    
    # Check AWS configuration
    aws_conf = config.get("aws_conf", {})
    if not isinstance(aws_conf, dict):
        raise TypeError(f"AWS configuration must be a dictionary, got {type(aws_conf)}")
    
    if "s3_bucket" not in aws_conf:
        raise ValueError("Missing required AWS configuration key: s3_bucket")
    
    if not isinstance(aws_conf["s3_bucket"], str) or not aws_conf["s3_bucket"]:
        raise ValueError("AWS s3_bucket must be a non-empty string")
    
    # Validate time range
    try:
        start_utc = int(config["start_utc"])
        end_utc = int(config["end_utc"])
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid timestamp format: {e}") from e
    
    if start_utc <= 0 or end_utc <= 0:
        raise ValueError(f"Timestamps must be positive integers (got start={start_utc}, end={end_utc})")
    
    if start_utc >= end_utc:
        raise ValueError(f"Invalid time range: start_utc ({start_utc}) must be less than end_utc ({end_utc})")
    
    # Validate S3 prefix format
    s3_prefix = config["s3_prefix"]
    if not isinstance(s3_prefix, str):
        raise TypeError(f"S3 prefix must be a string, got {type(s3_prefix)}")

    normalized_prefix = s3_prefix.strip()
    if not normalized_prefix:
        raise ValueError("S3 prefix must be a non-empty string")

    if normalized_prefix.startswith("s3://"):
        raise ValueError("S3 prefix should be a key prefix only (without s3://bucket)")
    
    # Check for credentials in config (warning only)
    if aws_conf.get("access_key") or aws_conf.get("secret_key"):
        log.warning("AWS credentials found in config file. Consider using environment variables, "
                   "~/.aws/credentials, or IAM roles instead for better security.")

    return True

def save_progress_state(state_file: str, progress_data: Dict[str, Any]) -> bool:
    """
    Saves the current progress state to a file for potential resume.
    
    Args:
        state_file: Path to the state file
        progress_data: Dictionary of progress data to save
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure parent directory exists
        state_dir = os.path.dirname(os.path.abspath(state_file))
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, indent=2)
        log.debug(f"Progress state saved to {state_file}")
        return True
    except (OSError, IOError) as e:
        log.error(f"Failed to save progress state to {state_file}: {e}")
        return False
    except (TypeError, ValueError) as e:
        log.error(f"Failed to serialize progress data: {e}")
        return False

def load_progress_state(state_file: str) -> Dict[str, Any]:
    """
    Loads the progress state from a file.
    
    Args:
        state_file: Path to the state file
        
    Returns:
        Dictionary of progress data, or empty dict if file not found or invalid
    """
    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            log.debug(f"Progress state loaded from {state_file}")
            return data
    except FileNotFoundError:
        log.debug(f"No progress state file found at {state_file}")
        return {}
    except json.JSONDecodeError as e:
        log.warning(f"Invalid JSON in progress state file {state_file}: {e}")
        return {}
    except (OSError, IOError) as e:
        log.warning(f"Error reading progress state file {state_file}: {e}")
        return {}
    except Exception as e:
        log.error(f"Unexpected error loading progress state: {e}")
        return {}
