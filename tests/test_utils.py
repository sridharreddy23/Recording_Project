#!/usr/bin/env python3
"""
Unit tests for utility functions.
"""
import unittest
import os
import tempfile
import json
from datetime import datetime, timezone

from src.utils import (
    format_datetime, get_bucket_name_path_from_url, get_s3_path,
    get_file_duration, get_file_path_to_read, get_start_utc_from_filename,
    convert_pcr_27mhz_to_pcr_ns, validate_config, save_progress_state,
    load_progress_state
)

class TestUtils(unittest.TestCase):
    """Test cases for utility functions."""
    
    def test_format_datetime(self):
        """Test formatting UTC timestamps."""
        # Test valid timestamp
        timestamp = 1609459200  # 2021-01-01 00:00:00 UTC
        expected = "2021-01-01 00:00:00 UTC"
        self.assertEqual(format_datetime(timestamp), expected)
        
        # Test invalid timestamp (should return error message)
        self.assertTrue("Invalid Timestamp" in format_datetime(-1e20))
    
    def test_get_bucket_name_path_from_url(self):
        """Test extracting bucket name and path from S3 URL."""
        # Test valid URL with path
        url = "s3://my-bucket/path/to/object"
        bucket, path = get_bucket_name_path_from_url(url)
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(path, "path/to/object")
        
        # Test valid URL without path
        url = "s3://my-bucket"
        bucket, path = get_bucket_name_path_from_url(url)
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(path, "")
        
        # Test invalid URL
        with self.assertRaises(ValueError):
            get_bucket_name_path_from_url("https://my-bucket")
    
    def test_get_s3_path(self):
        """Test constructing S3 paths."""
        # Test with trailing slash in prefix
        prefix = "s3://my-bucket/prefix/"
        rel_path = "path/to/object"
        self.assertEqual(get_s3_path(prefix, rel_path), "s3://my-bucket/prefix/path/to/object")
        
        # Test without trailing slash in prefix
        prefix = "s3://my-bucket/prefix"
        rel_path = "path/to/object"
        self.assertEqual(get_s3_path(prefix, rel_path), "s3://my-bucket/prefix/path/to/object")
        
        # Test with leading slash in rel_path
        prefix = "s3://my-bucket/prefix"
        rel_path = "/path/to/object"
        self.assertEqual(get_s3_path(prefix, rel_path), "s3://my-bucket/prefix/path/to/object")
    
    def test_get_file_duration(self):
        """Test extracting duration from filenames."""
        # Test valid filename
        filename = "1609459200-1609459204.es"
        self.assertEqual(get_file_duration(filename), 4)
        
        # Test valid filename with path
        filename = "/path/to/1609459200-1609459204.es"
        self.assertEqual(get_file_duration(filename), 4)
        
        # Test invalid filename (should return default 4)
        filename = "invalid.es"
        self.assertEqual(get_file_duration(filename), 4)
        
        # Test negative duration (should return default 4)
        filename = "1609459204-1609459200.es"
        self.assertEqual(get_file_duration(filename), 4)
    
    def test_get_file_path_to_read(self):
        """Test generating file paths from timestamps."""
        # Test timestamp on boundary
        timestamp = 1609459200  # 2021-01-01 00:00:00 UTC
        dt_obj = datetime.fromtimestamp(timestamp, timezone.utc)
        date_str = dt_obj.strftime("%d%m%Y")
        hour_str = dt_obj.strftime("%H")
        expected = f"{date_str}/{hour_str}/1609459200-1609459204.es"
        self.assertEqual(get_file_path_to_read(timestamp), expected)
        
        # Test timestamp not on boundary
        timestamp = 1609459203  # 2021-01-01 00:00:03 UTC
        dt_obj = datetime.fromtimestamp(timestamp - 3, timezone.utc)  # Should align to 1609459200
        date_str = dt_obj.strftime("%d%m%Y")
        hour_str = dt_obj.strftime("%H")
        expected = f"{date_str}/{hour_str}/1609459200-1609459204.es"
        self.assertEqual(get_file_path_to_read(timestamp), expected)
    
    def test_get_start_utc_from_filename(self):
        """Test extracting start UTC from filenames."""
        # Test valid filename
        filename = "1609459200-1609459204.es"
        self.assertEqual(get_start_utc_from_filename(filename), 1609459200)
        
        # Test valid filename with path
        filename = "/path/to/1609459200-1609459204.es"
        self.assertEqual(get_start_utc_from_filename(filename), 1609459200)
        
        # Test invalid filename (should return 0)
        filename = "invalid.es"
        self.assertEqual(get_start_utc_from_filename(filename), 0)
    
    def test_convert_pcr_27mhz_to_pcr_ns(self):
        """Test converting PCR values to nanoseconds."""
        # Test valid PCR value
        pcr = 27000000  # 1 second at 27MHz
        self.assertEqual(convert_pcr_27mhz_to_pcr_ns(pcr), 1000000000)  # 1 second in ns
        
        # Test negative PCR value (should return 0)
        self.assertEqual(convert_pcr_27mhz_to_pcr_ns(-1), 0)
    
    def test_validate_config(self):
        """Test configuration validation."""
        # Test valid config
        config = {
            "start_utc": 1609459200,
            "end_utc": 1609459300,
            "s3_prefix": "path/to/prefix",
            "aws_conf": {
                "s3_bucket": "my-bucket"
            }
        }
        self.assertTrue(validate_config(config))
        
        # Test missing required key
        config = {
            "start_utc": 1609459200,
            "end_utc": 1609459300,
            "s3_prefix": "path/to/prefix"
        }
        with self.assertRaises(ValueError):
            validate_config(config)
        
        # Test missing s3_bucket
        config = {
            "start_utc": 1609459200,
            "end_utc": 1609459300,
            "s3_prefix": "path/to/prefix",
            "aws_conf": {}
        }
        with self.assertRaises(ValueError):
            validate_config(config)
        
        # Test invalid time range
        config = {
            "start_utc": 1609459300,
            "end_utc": 1609459200,
            "s3_prefix": "path/to/prefix",
            "aws_conf": {
                "s3_bucket": "my-bucket"
            }
        }
        with self.assertRaises(ValueError):
            validate_config(config)

        # Test empty prefix
        config = {
            "start_utc": 1609459200,
            "end_utc": 1609459300,
            "s3_prefix": "   ",
            "aws_conf": {
                "s3_bucket": "my-bucket"
            }
        }
        with self.assertRaises(ValueError):
            validate_config(config)

        # Test full S3 URL in prefix (should not include bucket)
        config = {
            "start_utc": 1609459200,
            "end_utc": 1609459300,
            "s3_prefix": "s3://my-bucket/path/to/prefix",
            "aws_conf": {
                "s3_bucket": "my-bucket"
            }
        }
        with self.assertRaises(ValueError):
            validate_config(config)
    
    def test_save_load_progress_state(self):
        """Test saving and loading progress state."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Test saving state
            state = {
                "downloaded_files": ["/path/to/file1", "/path/to/file2"],
                "files_found_locally": 2,
                "files_failed": 1,
                "timestamp": 1609459200
            }
            self.assertTrue(save_progress_state(temp_path, state))
            
            # Test loading state
            loaded_state = load_progress_state(temp_path)
            self.assertEqual(loaded_state, state)
            
            # Test loading non-existent state
            self.assertEqual(load_progress_state("/non/existent/path"), {})
            
            # Test loading invalid state
            with open(temp_path, "w") as f:
                f.write("invalid json")
            self.assertEqual(load_progress_state(temp_path), {})
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)

if __name__ == "__main__":
    unittest.main()