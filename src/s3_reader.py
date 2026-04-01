#!/usr/bin/env python3
"""
S3 Reader module for downloading ES files from S3.
"""
import os
import time
import logging
import threading
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig

from .utils import (
    get_bucket_name_path_from_url, get_s3_path, get_file_path_to_read,
    get_file_duration, get_start_utc_from_filename, print_progress,
    format_datetime, log
)

# S3 download configuration
MAX_DOWNLOAD_WORKERS = 10
S3_DOWNLOAD_RETRIES = 3
S3_DOWNLOAD_RETRY_DELAY_S = 5
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=16 * 1024 * 1024,  # 16MB
    max_concurrency=10,
    use_threads=True
)

class S3Reader:
    """
    Handles listing and downloading files from S3.
    
    Attributes:
        start_utc_s: Start time in UTC seconds
        end_utc_s: End time in UTC seconds
        s3_prefix: S3 prefix URL (s3://bucket/path)
        local_temp_dir: Local directory for downloaded files
        files_to_download_map: Map of S3 paths to local paths
        downloaded_files: List of successfully downloaded files
        files_skipped: Count of skipped files (already exist locally)
        files_failed: Count of failed downloads
        files_found_locally: Count of files found locally
        resume_state_file: Path to the resume state file
    """
    
    def __init__(self, start_utc_s: int, end_utc_s: int, s3_prefix: str,
                 local_temp_dir: str, resume_state_file: Optional[str] = None,
                 max_download_workers: Optional[int] = None):
        """
        Initialize the S3Reader.
        
        Args:
            start_utc_s: Start time in UTC seconds
            end_utc_s: End time in UTC seconds
            s3_prefix: S3 prefix URL (s3://bucket/path)
            local_temp_dir: Local directory for downloaded files
            resume_state_file: Optional path to the resume state file
        """
        self.start_utc_s = start_utc_s
        self.end_utc_s = end_utc_s
        self.s3_prefix = s3_prefix
        self.local_temp_dir = local_temp_dir
        self.resume_state_file = resume_state_file
        self.max_download_workers = max_download_workers or MAX_DOWNLOAD_WORKERS
        self.files_to_download_map: Dict[str, str] = {}  # s3_path -> local_path
        self.downloaded_files: List[str] = []
        self._resumed_local_files: Set[str] = set()
        self.files_skipped = 0  # Already exists locally
        self.files_failed = 0  # Failed after retries
        self.files_found_locally = 0
        self._s3_client = None  # Lazy initialization

        log.info("S3 Reader Initialized:")
        log.info(f"  Time Range: {format_datetime(start_utc_s)} -> {format_datetime(end_utc_s)}")
        log.info(f"  S3 Prefix: {s3_prefix}")
        log.info(f"  Local Temp Storage: {local_temp_dir}")
        
        # Create the local temp directory if it doesn't exist
        os.makedirs(local_temp_dir, exist_ok=True)
        
        self._prepare_file_list()
    
    @property
    def s3_client(self):
        """
        Lazy initialization of S3 client.
        
        Returns:
            boto3.client: S3 client
        """
        if self._s3_client is None:
            session = boto3.session.Session()
            self._s3_client = session.client('s3')
        return self._s3_client

    def _prepare_file_list(self):
        """
        Generates the list of S3 files expected within the time range.
        """
        log.info("Preparing list of required S3 files...")
        current_utc = self.start_utc_s
        expected_files = set()

        while current_utc < self.end_utc_s:
            rel_path = get_file_path_to_read(current_utc)
            # Avoid adding duplicates if calculation yields same file multiple times
            if rel_path not in expected_files:
                s3_path = get_s3_path(self.s3_prefix, rel_path)
                local_path = os.path.join(self.local_temp_dir, rel_path)
                self.files_to_download_map[s3_path] = local_path
                expected_files.add(rel_path)

            # Move to the next potential file interval start time
            # Use duration parsed from *this* file's name for accuracy
            duration = get_file_duration(rel_path)
            if duration <= 0:
                log.warning(f"Got non-positive duration {duration} for {rel_path}, advancing by 1s to avoid infinite loop.")
                duration = 1  # Safety break
            current_utc += duration

        log.info(f"Identified {len(self.files_to_download_map)} unique potential files in the time range.")
    
    def _download_file_from_s3(self, s3_path: str, local_path: str) -> bool:
        """
        Downloads a single file from S3 with retries.
        
        Args:
            s3_path: S3 path (s3://bucket/path)
            local_path: Local path to save the file
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            FileNotFoundError: If the file is not found in S3
            PermissionError: If access is denied
        """
        log.info(f"Attempting download: {os.path.basename(local_path)} from {s3_path}")
        bucket_name, bucket_path = get_bucket_name_path_from_url(s3_path)

        attempt = 0
        while attempt < S3_DOWNLOAD_RETRIES:
            attempt += 1
            try:
                # Ensure local directory exists
                dir_path = os.path.dirname(local_path)
                os.makedirs(dir_path, exist_ok=True)

                self.s3_client.download_file(
                    Bucket=bucket_name,
                    Key=bucket_path,
                    Filename=local_path,
                    Config=S3_TRANSFER_CONFIG
                )
                log.info(f"Successfully downloaded {os.path.basename(local_path)}")
                return True  # Success

            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code')
                if error_code == '404':
                    log.error(f"File not found in S3 (Attempt {attempt}/{S3_DOWNLOAD_RETRIES}): {s3_path}")
                    raise FileNotFoundError(f"S3 object not found: {s3_path}")  # Don't retry 404
                elif error_code == '403':
                    log.error(f"Permission denied for S3 object (Attempt {attempt}/{S3_DOWNLOAD_RETRIES}): {s3_path}")
                    raise PermissionError(f"Permission denied for S3 object: {s3_path}")  # Don't retry 403
                else:
                    # General S3 client error, potentially retryable
                    log.warning(f"S3 ClientError on attempt {attempt}/{S3_DOWNLOAD_RETRIES} for {os.path.basename(local_path)}: {e}")
            except S3UploadFailedError as e:  # download_file might raise this too
                log.warning(f"S3 Transfer Error on attempt {attempt}/{S3_DOWNLOAD_RETRIES} for {os.path.basename(local_path)}: {e}")
            except Exception as e:
                # Catch other potential errors (network, disk space?)
                log.warning(f"Non-S3 Error downloading on attempt {attempt}/{S3_DOWNLOAD_RETRIES} for {os.path.basename(local_path)}: {e}", exc_info=False)

            # If we haven't returned or raised an exception, wait before retrying
            if attempt < S3_DOWNLOAD_RETRIES:
                log.info(f"Retrying download of {os.path.basename(local_path)} in {S3_DOWNLOAD_RETRY_DELAY_S} seconds...")
                time.sleep(S3_DOWNLOAD_RETRY_DELAY_S)
            else:
                log.error(f"Download failed after {S3_DOWNLOAD_RETRIES} attempts: {os.path.basename(local_path)}")
                return False  # Failed after all retries

        return False  # Should not be reached, but safety return

    def download_files_parallel(self) -> List[str]:
        """
        Downloads required files in parallel, skipping existing ones.
        
        Returns:
            List of paths to downloaded files
        """
        from .utils import print_section_header
        
        print_section_header("S3 Parallel Download Process")
        self.downloaded_files = []
        self.files_found_locally = 0
        self.files_failed = 0
        seen_downloaded_files: Set[str] = set()

        # Check which files already exist locally
        files_needing_download = {}
        for s3_path, local_path in self.files_to_download_map.items():
            if os.path.exists(local_path):
                log.info(f"File already exists locally, skipping download: {os.path.basename(local_path)}")
                if local_path not in seen_downloaded_files:
                    self.downloaded_files.append(local_path)  # Add existing file to list once
                    seen_downloaded_files.add(local_path)
                self.files_found_locally += 1
            else:
                files_needing_download[s3_path] = local_path

        total_to_attempt = len(files_needing_download)
        log.info(f"Attempting to download {total_to_attempt} files.")
        files_actually_downloaded = 0
        download_lock = threading.Lock()  # For thread-safe counter updates

        with ThreadPoolExecutor(max_workers=self.max_download_workers) as executor:
            # Submit download tasks
            futures = {
                executor.submit(self._download_file_from_s3, s3_path, local_path): (s3_path, local_path)
                for s3_path, local_path in files_needing_download.items()
            }

            print_progress(0, total_to_attempt, prefix="Downloading: ", suffix="(0 / {})".format(total_to_attempt))

            for i, future in enumerate(as_completed(futures)):
                s3_path, local_path = futures[future]
                try:
                    success = future.result()  # Get result (True/False) or raise exception
                    if success:
                        with download_lock:
                            if local_path not in seen_downloaded_files:
                                self.downloaded_files.append(local_path)
                                seen_downloaded_files.add(local_path)
                            files_actually_downloaded += 1
                    else:
                        # Failure after retries already logged in download_file_from_s3
                        with download_lock:
                            self.files_failed += 1
                except FileNotFoundError:
                    # Logged in download_file_from_s3
                    with download_lock:
                        self.files_failed += 1  # Treat 404 as failure for summary
                except PermissionError:
                    # Logged in download_file_from_s3
                    with download_lock:
                        self.files_failed += 1  # Treat 403 as failure for summary
                except Exception as e:
                    log.error(f"Unexpected error during download future processing for {s3_path}: {e}", exc_info=True)
                    with download_lock:
                        self.files_failed += 1

                # Update progress bar after each file completes (or fails)
                processed_count = files_actually_downloaded + self.files_failed
                print_progress(processed_count, total_to_attempt,
                              prefix="Downloading: ",
                              suffix=f"({processed_count} / {total_to_attempt})")

        # Final Summary
        print_section_header("S3 Download Summary")
        log.info(f"Total files identified in range: {len(self.files_to_download_map)}")
        log.info(f"Files found locally (skipped download): {self.files_found_locally}")
        log.info(f"Files attempted download: {total_to_attempt}")
        log.info(f"Files successfully downloaded: {files_actually_downloaded}")
        log.info(f"Files failed download (after retries): {self.files_failed}")
        log.info(f"Total files available for parsing: {len(self.downloaded_files)}")

        if not self.downloaded_files:
            log.warning("No files were successfully downloaded or found locally. Cannot proceed.")
            return []

        # IMPORTANT: Sort the list of available files chronologically before returning
        try:
            self.downloaded_files.sort(key=get_start_utc_from_filename)
            log.info("Downloaded file list sorted chronologically.")
        except Exception as e:
            log.error("Failed to sort downloaded files based on start UTC. Parsing order may be incorrect.", exc_info=True)
            # Proceeding with unsorted list - user beware!

        # Save progress state if resume file is specified
        if self.resume_state_file:
            from .utils import save_progress_state
            progress_data = {
                "downloaded_files": self.downloaded_files,
                "files_found_locally": self.files_found_locally,
                "files_failed": self.files_failed,
                "timestamp": time.time()
            }
            save_progress_state(self.resume_state_file, progress_data)

        return self.downloaded_files
    
    def resume_from_state(self, state_data: Dict) -> bool:
        """
        Resume download from a saved state.
        
        Args:
            state_data: Dictionary containing saved state
            
        Returns:
            True if state was successfully loaded, False otherwise
        """
        if not state_data:
            return False
            
        try:
            downloaded_files = state_data.get("downloaded_files", [])
            expected_local_paths = set(self.files_to_download_map.values())
            
            # Validate that resumed files both exist and belong to the current manifest.
            valid_files = set()
            for file_path in downloaded_files:
                if file_path not in expected_local_paths:
                    log.debug(f"Ignoring resumed file outside current manifest: {file_path}")
                elif os.path.exists(file_path):
                    valid_files.add(file_path)
                else:
                    log.warning(f"File in resume state not found: {file_path}")
            
            self._resumed_local_files = valid_files
            self.downloaded_files = []
            
            log.info(
                "Loaded resume state with %s local manifest file(s); "
                "actual parse list will be rebuilt from current manifest scan.",
                len(self._resumed_local_files)
            )
            return True
        except Exception as e:
            log.error(f"Failed to resume from state: {e}")
            return False
