#!/usr/bin/env python3
"""
Main entry for ES Downloader & Parser with SendGB (Selenium) + authenticated GoFile fallback.
"""
import os
import sys
import argparse
import logging
import tempfile
import signal
import json
import shutil
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Tuple, Optional, List
from botocore.exceptions import ClientError
import boto3

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# --- Import project modules ---
try:
    from .utils import print_banner, print_section_header, print_final_success, format_datetime, load_progress_state, log
    from .config_manager import ConfigManager
    from .s3_reader import S3Reader
    from .es_parser import ESParser
except Exception:
    from utils import print_banner, print_section_header, print_final_success, format_datetime, load_progress_state, log
    from config_manager import ConfigManager
    from s3_reader import S3Reader
    from es_parser import ESParser

# --- SendGB (Selenium) uploader ---
try:
    from .sendgb_selenium_uploader import upload_with_selenium
except Exception:
    try:
        from sendgb_selenium_uploader import upload_with_selenium  # type: ignore
    except Exception:
        upload_with_selenium = None

# --- GoFile authenticated uploader ---
try:
    from .gofile_uploader import upload_to_gofile
except Exception:
    from gofile_uploader import upload_to_gofile  # type: ignore

# --- Helpers for link handling ---
try:
    from .sendgb_helpers import is_sendgb_link, validate_link_http, save_sendgb_link
except Exception:
    def is_sendgb_link(u: str) -> bool:
        return bool(u and "sendgb.com" in u and "payment.sendgb.com" not in u)
    def validate_link_http(u: str, timeout: int = 10):
        return (True, 200)
    def save_sendgb_link(out: str, link: str, filename_suffix: str = ".link.txt") -> str:
        """Fallback implementation for saving upload links."""
        path = out + filename_suffix
        try:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(link.strip() + "\n")
            log.debug(f"Link saved to {path}")
        except (OSError, IOError) as e:
            log.warning(f"Failed to save link to {path}: {e}")
        return path


# Global flag for graceful shutdown
_shutdown_requested = False

# --- Signal handler ---
def signal_handler(sig, frame):
    """Handle interruption signals for graceful shutdown."""
    global _shutdown_requested
    try:
        signal_name = signal.Signals(sig).name
    except (AttributeError, ValueError):
        signal_name = f"SIG{sig}"
    log.warning("\nProcess interrupted by user (%s). Cleaning up and exiting...", signal_name)
    _shutdown_requested = True
    # Allow some time for cleanup before force exit
    import time
    time.sleep(1)
    sys.exit(130 if sig == signal.SIGINT else 128 + sig)


# --- Environment setup ---
def load_environment_from_dotenv(config_path: str) -> List[str]:
    """
    Load environment variables from .env files without overriding existing env vars.

    Lookup order:
      1) Project root/current working directory `.env`
      2) Directory containing the provided config file `.env`

    Args:
        config_path: Path to configuration JSON file

    Returns:
        List of `.env` file paths that were loaded
    """
    if load_dotenv is None:
        log.debug("python-dotenv is not available; skipping .env loading")
        return []

    loaded_files: List[str] = []
    candidate_paths: List[str] = []

    cwd_env = os.path.join(os.getcwd(), ".env")
    candidate_paths.append(cwd_env)

    config_dir = os.path.dirname(os.path.abspath(config_path))
    config_env = os.path.join(config_dir, ".env")
    if config_env not in candidate_paths:
        candidate_paths.append(config_env)

    for env_path in candidate_paths:
        if os.path.isfile(env_path) and load_dotenv(env_path, override=False):
            loaded_files.append(env_path)
            log.info("Loaded environment variables from %s", env_path)

    return loaded_files


def print_runtime_summary(args: argparse.Namespace, start_utc: int, end_utc: int, s3_prefix: str) -> None:
    """Print a concise, human-friendly run summary."""
    upload_mode = "none"
    if args.gofile:
        upload_mode = "gofile"
    elif args.sendgb:
        upload_mode = "sendgb-with-fallback"

    log.info("🚀 Starting processing pipeline")
    log.info("📄 Config file: %s", args.config)
    log.info("🎯 Output file: %s", args.output)
    log.info("🕒 Range: %s -> %s", format_datetime(start_utc), format_datetime(end_utc))
    log.info("🗂️ S3 prefix: %s", s3_prefix)
    log.info("☁️ Upload mode: %s", upload_mode)


def calculate_expected_segments(start_utc: int, end_utc: int, segment_seconds: int = 4) -> int:
    """Estimate number of expected ES segments for the configured time window."""
    if end_utc <= start_utc:
        return 0
    return (max(0, end_utc - start_utc) + segment_seconds - 1) // segment_seconds


def calculate_recommended_space_bytes(expected_segments: int, avg_segment_size_bytes: int = 2 * 1024 * 1024) -> int:
    """Estimate required temp disk footprint (download + final TS + safety margin)."""
    if expected_segments <= 0:
        return 0
    estimated_data = expected_segments * avg_segment_size_bytes
    return int(estimated_data * 2.2)


def parse_cli_time(value: str) -> int:
    """
    Parse a CLI time value as UTC epoch seconds.

    Supported formats:
      - Epoch seconds (e.g. "1711939200")
      - ISO-8601 date/time (e.g. "2026-04-01T00:00:00Z")
      - Date only (e.g. "2026-04-01", interpreted as 00:00:00 UTC)
    """
    if value is None:
        raise ValueError("Time value cannot be None")

    raw = str(value).strip()
    if not raw:
        raise ValueError("Time value cannot be empty")

    if raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit()):
        return int(raw)

    iso_candidate = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_candidate)
    except ValueError as exc:
        raise ValueError(
            f"Invalid time value '{value}'. Use epoch seconds or ISO-8601 (e.g. 2026-04-01T12:00:00Z)."
        ) from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return int(dt.timestamp())


def run_preflight_checks(start_utc: int, end_utc: int, output_path: str) -> dict:
    """Run quick reliability-oriented checks before expensive operations begin."""
    expected_segments = calculate_expected_segments(start_utc, end_utc)
    required_space = calculate_recommended_space_bytes(expected_segments)
    output_dir = os.path.dirname(os.path.abspath(output_path)) or os.getcwd()
    free_space = shutil.disk_usage(output_dir).free
    disk_ok = free_space >= required_space if required_space else True

    return {
        "expected_segments": expected_segments,
        "recommended_space_bytes": required_space,
        "free_space_bytes": free_space,
        "disk_ok": disk_ok,
        "output_dir": output_dir,
    }


def write_run_report(report_path: str, report_payload: dict) -> None:
    """Write a structured run report for auditability and appraisal evidence."""
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report_payload, fh, indent=2)
        fh.write("\n")


# --- AWS setup ---
def setup_aws_credentials(cfg: ConfigManager) -> None:
    """
    Configure AWS credentials from ConfigManager.
    
    Args:
        cfg: ConfigManager instance containing AWS configuration
        
    Note:
        This sets environment variables for boto3. Credentials in config file
        will be used only if not already set in environment.
    """
    # Only set credentials from config if not already in environment
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        creds = cfg.get_aws_credentials()
        if creds:
            if creds.get("access_key"):
                os.environ["AWS_ACCESS_KEY_ID"] = creds["access_key"]
            if creds.get("secret_key"):
                os.environ["AWS_SECRET_ACCESS_KEY"] = creds["secret_key"]
            if creds.get("session_token"):
                os.environ["AWS_SESSION_TOKEN"] = creds["session_token"]
    
    # Set region from config if not already set
    if not os.environ.get("AWS_DEFAULT_REGION"):
        region = cfg.get_aws_region()
        if region:
            os.environ["AWS_DEFAULT_REGION"] = region
            log.debug(f"AWS region set to: {region}")


# --- Upload logic (SendGB → GoFile fallback) ---
def attempt_sendgb_then_fallback(output_path: str, wait_timeout: int = 600) -> Tuple[str, str]:
    """
    Attempt SendGB upload; if fails or payment link, fallback to GoFile.
    
    Args:
        output_path: Path to the file to upload
        wait_timeout: Maximum seconds to wait for SendGB upload (default: 600)
    
    Returns:
        Tuple of (provider_name, download_link)
        
    Raises:
        RuntimeError: If both upload methods fail or GOFILE_TOKEN is missing
        FileNotFoundError: If output_path doesn't exist
    """
    if not os.path.isfile(output_path):
        raise FileNotFoundError(f"Output file not found: {output_path}")
    
    # 1️⃣ Try SendGB
    if upload_with_selenium:
        try:
            log.info("Attempting SendGB upload...")
            link = upload_with_selenium(output_path, wait_timeout=wait_timeout)
            if is_sendgb_link(link):
                ok, code = validate_link_http(link)
                if ok:
                    save_sendgb_link(output_path, link, filename_suffix=".sendgb_link.txt")
                    log.info(f"SendGB link validated (HTTP {code})")
                    return ("sendgb", link)
                else:
                    log.warning(f"SendGB link validation failed (HTTP {code}), falling back to GoFile.")
            else:
                log.warning("SendGB returned a payment or invalid link, falling back to GoFile.")
        except KeyboardInterrupt:
            raise  # Re-raise keyboard interrupts
        except Exception as e:
            log.warning("SendGB upload failed: %s", e, exc_info=log.isEnabledFor(logging.DEBUG))

    # 2️⃣ Fallback → GoFile
    token = os.environ.get("GOFILE_TOKEN")
    if not token:
        raise RuntimeError("Missing GOFILE_TOKEN in environment. Export it before running.")
    try:
        log.info("Falling back to GoFile upload (authenticated).")
        gofile_link = upload_to_gofile(output_path, api_token=token)
        save_sendgb_link(output_path, gofile_link, filename_suffix=".gofile_link.txt")
        return ("gofile", gofile_link)
    except Exception as e:
        log.exception("GoFile upload failed:")
        raise RuntimeError(f"GoFile upload failed: {e}") from e


def validate_arguments(args: argparse.Namespace) -> None:
    """
    Validate command-line arguments.
    
    Args:
        args: Parsed command-line arguments
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If arguments are invalid
    """
    # Validate config file exists
    if not os.path.isfile(args.config):
        raise FileNotFoundError(f"Configuration file not found: {args.config}")
    
    # Validate output path directory exists and is writable
    output_dir = os.path.dirname(os.path.abspath(args.output))
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            log.info(f"Created output directory: {output_dir}")
        except (OSError, PermissionError) as e:
            raise ValueError(f"Cannot create output directory {output_dir}: {e}") from e
    elif output_dir and not os.access(output_dir, os.W_OK):
        raise ValueError(f"Output directory is not writable: {output_dir}")
    
    # Validate sendgb-wait timeout
    if args.sendgb_wait <= 0:
        raise ValueError(f"--sendgb-wait must be positive, got {args.sendgb_wait}")

    # Validate optional CLI time overrides
    if args.start_utc is not None:
        parse_cli_time(args.start_utc)
    if args.end_utc is not None:
        parse_cli_time(args.end_utc)

    if args.workers is not None and args.workers <= 0:
        raise ValueError(f"--workers must be positive, got {args.workers}")

    if args.resume and not args.resume_state:
        raise ValueError("--resume requires --resume-state to be provided")
    if args.resume and not args.temp_dir:
        raise ValueError("--resume requires --temp-dir so downloaded files can persist across runs")


def print_download_link(provider: str, link: str) -> None:
    """
    Print download link prominently to terminal.
    
    Args:
        provider: Name of the upload provider (e.g., "GoFile", "SendGB")
        link: The download link URL
    """
    try:
        from colorama import Fore, Style
    except ImportError:
        # Fallback if colorama is not available
        Fore = type('Fore', (), {'GREEN': '', 'YELLOW': '', 'CYAN': ''})()
        Style = type('Style', (), {'RESET_ALL': ''})()
    
    # Calculate spacing for centered display
    link_len = len(link)
    padding = max(0, 68 - link_len - 2)
    
    print(f"\n{Fore.GREEN}{'=' * 70}")
    print(f"{Fore.GREEN}╔{'═' * 68}╗")
    print(f"{Fore.GREEN}║{Fore.YELLOW}  {provider} Download Link:{' ' * (68 - len(provider) - 18)}{Fore.GREEN}║")
    print(f"{Fore.GREEN}║{' ' * 68}║")
    print(f"{Fore.GREEN}║{Fore.CYAN}  {link}{' ' * padding}{Fore.GREEN}║")
    print(f"{Fore.GREEN}╚{'═' * 68}╝")
    print(f"{Fore.GREEN}{'=' * 70}{Style.RESET_ALL}\n")
    # Also print just the link for easy copying
    print(f"{Fore.CYAN}{link}{Style.RESET_ALL}\n")


def handle_upload(output_path: str, args: argparse.Namespace) -> Optional[Tuple[str, str]]:
    """
    Handle file upload based on command-line arguments.
    
    Args:
        output_path: Path to the file to upload
        args: Parsed command-line arguments
        
    Returns:
        Tuple of (provider, link) if upload succeeded, None otherwise
    """
    if args.gofile:
        token = os.environ.get("GOFILE_TOKEN")
        if not token:
            raise RuntimeError("Missing GOFILE_TOKEN in environment.")
        print_section_header("GoFile Upload")
        link = upload_to_gofile(output_path, api_token=token)
        # Print prominently to terminal
        print_download_link("GoFile", link)
        # Optionally save to file (user can disable if needed)
        try:
            save_sendgb_link(output_path, link, filename_suffix=".gofile_link.txt")
            log.debug(f"Link also saved to {output_path}.gofile_link.txt")
        except Exception as e:
            log.debug(f"Could not save link to file: {e}")
        return ("gofile", link)
    elif args.sendgb:
        print_section_header("SendGB Upload")
        provider, link = attempt_sendgb_then_fallback(output_path, wait_timeout=args.sendgb_wait)
        # Print prominently to terminal
        print_download_link(provider.capitalize(), link)
        # Optionally save to file
        try:
            save_sendgb_link(output_path, link, filename_suffix=f".{provider.lower()}_link.txt")
            log.debug(f"Link also saved to {output_path}.{provider.lower()}_link.txt")
        except Exception as e:
            log.debug(f"Could not save link to file: {e}")
        return (provider, link)
    else:
        log.info("No upload option requested (--sendgb or --gofile).")
        return None


# --- Main function ---
def main() -> int:
    """
    Main entry point for the ES Downloader & Parser.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)
    
    parser = argparse.ArgumentParser(
        description="S3 ES Downloader & Parser + SendGB/GoFile uploader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s config.json output.ts --gofile
  %(prog)s config.json output.ts --sendgb --sendgb-wait 300

Tip:
  Put AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, and GOFILE_TOKEN
  in a .env file to avoid re-exporting them every terminal session.
        """
    )
    parser.add_argument("config", help="Path to configuration JSON file")
    parser.add_argument("output", help="Final TS output file path")
    upload_group = parser.add_mutually_exclusive_group()
    upload_group.add_argument("--sendgb", action="store_true", 
                             help="Try SendGB upload (fallback to GoFile on fail)")
    upload_group.add_argument("--gofile", action="store_true", 
                             help="Upload final TS directly to GoFile")
    parser.add_argument("--sendgb-wait", type=int, default=600, 
                       help="Seconds to wait for SendGB upload (default: 600)")
    parser.add_argument("--debug", "-d", action="store_true", 
                       help="Enable debug logs")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--preflight-only", action="store_true",
                            help="Run smart preflight checks and exit without downloading")
    parser.add_argument("--report-file", default=None,
                       help="Optional JSON report path (default: <output>.run_report.json)")
    parser.add_argument("--start-utc", default=None,
                       help="Optional start override (epoch seconds or ISO-8601 UTC)")
    parser.add_argument("--end-utc", default=None,
                       help="Optional end override (epoch seconds or ISO-8601 UTC)")
    mode_group.add_argument("--list-expected-files", action="store_true",
                            help="Print expected S3 file paths for the selected range and exit")
    mode_group.add_argument("--dry-run", action="store_true",
                            help="Run preflight + expected-file manifest summary and exit")
    parser.add_argument("--workers", type=int, default=None,
                       help="Optional number of parallel S3 download workers (default: 10)")
    parser.add_argument("--resume-state", default=None,
                       help="Path to JSON state file for saving/resuming download progress")
    parser.add_argument("--resume", action="store_true",
                       help="Resume from --resume-state if available")
    parser.add_argument("--temp-dir", default=None,
                       help="Optional persistent temp directory for downloaded ES files")
    args = parser.parse_args()

    # Setup logging
    if args.debug:
        log.setLevel(logging.DEBUG)
        logging.getLogger("boto3").setLevel(logging.DEBUG)
        logging.getLogger("botocore").setLevel(logging.DEBUG)
        log.info("Debug logging enabled")

    loaded_env_files = load_environment_from_dotenv(args.config)
    if not loaded_env_files:
        log.info("No .env file found (or no new values loaded); using existing environment/config values")

    print_banner()

    try:
        started_at = time.time()
        report_payload = {
            "status": "started",
            "config": args.config,
            "output": args.output,
            "upload_mode": "sendgb" if args.sendgb else "gofile" if args.gofile else "none",
        }

        # Validate arguments
        validate_arguments(args)
        
        # Load and validate configuration
        cfg = ConfigManager(args.config)
        setup_aws_credentials(cfg)

        start_utc, end_utc = cfg.get_start_utc(), cfg.get_end_utc()
        if args.start_utc is not None:
            start_utc = parse_cli_time(args.start_utc)
        if args.end_utc is not None:
            end_utc = parse_cli_time(args.end_utc)
        if end_utc <= start_utc:
            raise ValueError(f"Invalid time range: start_utc={start_utc}, end_utc={end_utc}")
        s3_prefix = cfg.get_s3_prefix()
        output_path = args.output

        print_section_header("Setup")
        print_runtime_summary(args, start_utc, end_utc, s3_prefix)

        preflight = run_preflight_checks(start_utc, end_utc, output_path)
        report_payload["preflight"] = preflight
        log.info("🧠 Preflight: estimated %s segments for selected range", preflight["expected_segments"])
        log.info("💽 Preflight: free disk %.2f GiB (recommended %.2f GiB)",
                 preflight["free_space_bytes"] / (1024 ** 3),
                 preflight["recommended_space_bytes"] / (1024 ** 3))

        if not preflight["disk_ok"]:
            raise RuntimeError(
                "Preflight failed: not enough free disk space for reliable processing "
                f"(need ~{preflight['recommended_space_bytes']} bytes)."
            )

        if args.preflight_only:
            report_payload["status"] = "preflight_only_success"
            report_payload["duration_seconds"] = round(time.time() - started_at, 2)
            report_file = args.report_file or f"{output_path}.run_report.json"
            write_run_report(report_file, report_payload)
            log.info("Preflight-only mode complete. Report saved to %s", report_file)
            return 0

        if args.list_expected_files or args.dry_run:
            with tempfile.TemporaryDirectory(prefix="s3_es_manifest_") as temp_dir:
                manifest_reader = S3Reader(start_utc, end_utc, s3_prefix, temp_dir, None)
                expected_paths = sorted(manifest_reader.files_to_download_map.keys())
            if args.list_expected_files:
                for path in expected_paths:
                    print(path)
            if args.dry_run:
                log.info("Dry-run summary: expected_files=%s expected_segments=%s",
                         len(expected_paths), preflight["expected_segments"])
            report_payload["status"] = "dry_run_success" if args.dry_run else "list_expected_files_success"
            report_payload["expected_files_count"] = len(expected_paths)
            report_payload["duration_seconds"] = round(time.time() - started_at, 2)
            report_file = args.report_file or f"{output_path}.run_report.json"
            write_run_report(report_file, report_payload)
            if args.dry_run:
                log.info("Dry-run complete. Report saved to %s", report_file)
            else:
                log.info("Listed %s expected files. Report saved to %s", len(expected_paths), report_file)
            return 0

        # Check for shutdown request
        if _shutdown_requested:
            log.warning("Shutdown requested before processing")
            return 130

        if args.temp_dir:
            os.makedirs(args.temp_dir, exist_ok=True)
            temp_dir_context = nullcontext(args.temp_dir)
        else:
            temp_dir_context = tempfile.TemporaryDirectory(prefix="s3_es_parser_")

        with temp_dir_context as temp_dir:
            log.debug(f"Using temporary directory: {temp_dir}")
            
            # Download
            print_section_header("Downloading from S3")
            s3_reader = S3Reader(
                start_utc,
                end_utc,
                s3_prefix,
                temp_dir,
                resume_state_file=args.resume_state,
                max_download_workers=args.workers,
            )
            if args.resume and args.resume_state:
                state_data = load_progress_state(args.resume_state)
                resumed = s3_reader.resume_from_state(state_data)
                if resumed:
                    log.info("Resume state loaded from %s", args.resume_state)
            files = s3_reader.download_files_parallel()
            report_payload["downloaded_files"] = len(files)
            if not files:
                log.error("No files available for parsing. Exiting.")
                return 1

            if _shutdown_requested:
                log.warning("Shutdown requested after download")
                return 130

            # Parse
            print_section_header("Parsing ES Files")
            es_parser = ESParser(start_utc, end_utc, output_path, 1024 * 1024, None)
            es_parser.process_files(files, cleanup_after_processing=False)
            report_payload["parser"] = {
                "files_processed": es_parser.total_files_processed,
                "files_failed": es_parser.total_files_failed,
                "packets_processed": es_parser.total_packets_processed,
                "bytes_written": es_parser.output_bytes_written,
            }
            
            if not os.path.isfile(output_path):
                raise FileNotFoundError(f"Expected output file not created: {output_path}")

            if _shutdown_requested:
                log.warning("Shutdown requested after parsing")
                return 130

            # Upload
            upload_result = handle_upload(output_path, args)
            if upload_result:
                report_payload["upload"] = {
                    "provider": upload_result[0],
                    "link": upload_result[1],
                }

        print_final_success()
        log.info("All done successfully.")
        report_payload["status"] = "success"
        report_payload["duration_seconds"] = round(time.time() - started_at, 2)
        report_file = args.report_file or f"{output_path}.run_report.json"
        write_run_report(report_file, report_payload)
        log.info("Run report written to %s", report_file)
        return 0

    except KeyboardInterrupt:
        log.warning("\nProcess interrupted by user")
        return 130
    except (FileNotFoundError, ValueError) as e:
        log.error(f"Configuration error: {e}")
        return 1
    except RuntimeError as e:
        log.error(f"Runtime error: {e}")
        return 1
    except Exception as e:
        log.exception("Fatal error:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
