#!/usr/bin/env python3
"""
Unit tests for ConfigManager.
"""
import unittest
import os
import tempfile
import json

from src.config_manager import ConfigManager

class TestConfigManager(unittest.TestCase):
    """Test cases for ConfigManager."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary config file
        self.config_data = {
            "start_utc": 1609459200,
            "end_utc": 1609459300,
            "s3_prefix": "path/to/prefix",
            "aws_conf": {
                "aws_region": "us-east-1",
                "s3_bucket": "my-bucket",
                "access_key": "test-access-key",
                "secret_key": "test-secret-key"
            },
            "extra_param": "extra_value"
        }
        
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
            json.dump(self.config_data, temp_file)
            self.config_path = temp_file.name
    
    def tearDown(self):
        """Tear down test fixtures."""
        # Remove the temporary config file
        if os.path.exists(self.config_path):
            os.unlink(self.config_path)
    
    def test_load_config(self):
        """Test loading configuration."""
        config_manager = ConfigManager(self.config_path)
        self.assertEqual(config_manager.config, self.config_data)
    
    def test_get_start_utc(self):
        """Test getting start UTC."""
        config_manager = ConfigManager(self.config_path)
        self.assertEqual(config_manager.get_start_utc(), 1609459200)
    
    def test_get_end_utc(self):
        """Test getting end UTC."""
        config_manager = ConfigManager(self.config_path)
        self.assertEqual(config_manager.get_end_utc(), 1609459300)
    
    def test_get_s3_prefix(self):
        """Test getting S3 prefix."""
        config_manager = ConfigManager(self.config_path)
        self.assertEqual(config_manager.get_s3_prefix(), "s3://my-bucket/path/to/prefix")
    
    def test_get_aws_region(self):
        """Test getting AWS region."""
        config_manager = ConfigManager(self.config_path)
        self.assertEqual(config_manager.get_aws_region(), "us-east-1")
    
    def test_get_aws_credentials(self):
        """Test getting AWS credentials."""
        config_manager = ConfigManager(self.config_path)
        credentials = config_manager.get_aws_credentials()
        self.assertEqual(credentials["access_key"], "test-access-key")
        self.assertEqual(credentials["secret_key"], "test-secret-key")
    
    def test_get_value(self):
        """Test getting a value."""
        config_manager = ConfigManager(self.config_path)
        self.assertEqual(config_manager.get_value("extra_param"), "extra_value")
        self.assertEqual(config_manager.get_value("non_existent", "default"), "default")
    
    def test_get_nested_value(self):
        """Test getting a nested value."""
        config_manager = ConfigManager(self.config_path)
        self.assertEqual(config_manager.get_nested_value(["aws_conf", "aws_region"]), "us-east-1")
        self.assertEqual(config_manager.get_nested_value(["aws_conf", "non_existent"], "default"), "default")
        self.assertEqual(config_manager.get_nested_value(["non_existent", "key"], "default"), "default")
    
    def test_get_nested_value_with_non_dict_intermediate(self):
        """Test nested lookup gracefully handles non-dict intermediate values."""
        config_manager = ConfigManager(self.config_path)
        self.assertEqual(
            config_manager.get_nested_value(["extra_param", "missing"], "default"),
            "default"
        )

    def test_invalid_config_file(self):
        """Test handling invalid config file."""
        # Create an invalid JSON file
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
            temp_file.write("invalid json")
            invalid_path = temp_file.name
        
        try:
            with self.assertRaises(json.JSONDecodeError):
                ConfigManager(invalid_path)
        finally:
            if os.path.exists(invalid_path):
                os.unlink(invalid_path)
    
    def test_missing_config_file(self):
        """Test handling missing config file."""
        with self.assertRaises(FileNotFoundError):
            ConfigManager("/non/existent/path")
    
    def test_invalid_config_content(self):
        """Test handling invalid config content."""
        # Create a config file with invalid content
        invalid_data = {
            "start_utc": 1609459200,
            "end_utc": 1609459100,  # end < start
            "s3_prefix": "path/to/prefix",
            "aws_conf": {
                "s3_bucket": "my-bucket"
            }
        }
        
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
            json.dump(invalid_data, temp_file)
            invalid_path = temp_file.name
        
        try:
            with self.assertRaises(ValueError):
                ConfigManager(invalid_path)
        finally:
            if os.path.exists(invalid_path):
                os.unlink(invalid_path)

if __name__ == "__main__":
    unittest.main()