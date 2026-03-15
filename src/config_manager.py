#!/usr/bin/env python3
"""
Configuration manager for the ES Downloader and Parser.
"""
import os
import json
import logging
from typing import Dict, Any, Optional

from .utils import validate_config, log

class ConfigManager:
    """
    Manages configuration loading and validation.
    
    Attributes:
        config_path: Path to the configuration file
        config: Loaded configuration dictionary
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the ConfigManager.
        
        Args:
            config_path: Path to the configuration file
            
        Raises:
            FileNotFoundError: If the configuration file is not found
            json.JSONDecodeError: If the configuration file is not valid JSON
            ValueError: If the configuration is invalid
        """
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """
        Load and validate the configuration file.
        
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If the configuration file is not found
            json.JSONDecodeError: If the configuration file is not valid JSON
            ValueError: If the configuration is invalid
        """
        log.info(f"Loading configuration from: {self.config_path}")
        try:
            with open(self.config_path, 'r') as f:
                config_dict = json.load(f)
            log.info("Configuration loaded successfully.")
            
            # Validate the configuration
            validate_config(config_dict)
            
            return config_dict
        except FileNotFoundError:
            log.exception(f"Configuration file not found: {self.config_path}")
            raise
        except json.JSONDecodeError:
            log.exception(f"Invalid JSON format in configuration file: {self.config_path}")
            raise
        except ValueError as ve:
            log.exception(f"Configuration validation failed: {ve}")
            raise
        except Exception as e:
            log.exception(f"An unexpected error occurred while loading configuration: {e}")
            raise
    
    def get_start_utc(self) -> int:
        """
        Get the start UTC timestamp.
        
        Returns:
            Start UTC timestamp in seconds
        """
        return int(self.config["start_utc"])
    
    def get_end_utc(self) -> int:
        """
        Get the end UTC timestamp.
        
        Returns:
            End UTC timestamp in seconds
        """
        return int(self.config["end_utc"])
    
    def get_s3_prefix(self) -> str:
        """
        Get the full S3 prefix URL.
        
        Returns:
            S3 prefix URL (s3://bucket/path)
        """
        s3_base_prefix = self.config["s3_prefix"]
        s3_bucket = self.config["aws_conf"]["s3_bucket"]
        return f"s3://{s3_bucket}/{s3_base_prefix.strip('/')}"
    
    def get_aws_region(self) -> Optional[str]:
        """
        Get the AWS region.
        
        Returns:
            AWS region, or None if not specified
        """
        return self.config.get("aws_conf", {}).get("aws_region")
    
    def get_aws_credentials(self) -> Dict[str, str]:
        """
        Get AWS credentials from the configuration.
        
        Returns:
            Dictionary containing AWS credentials
        """
        aws_conf = self.config.get("aws_conf", {})
        credentials = {}
        
        if "access_key" in aws_conf:
            credentials["access_key"] = aws_conf["access_key"]
            log.warning("Using access key from configuration file. Consider using environment variables instead.")
        
        if "secret_key" in aws_conf:
            credentials["secret_key"] = aws_conf["secret_key"]
            log.warning("Using secret key from configuration file. Consider using environment variables instead.")
        
        if "session_token" in aws_conf:
            credentials["session_token"] = aws_conf["session_token"]
            log.warning("Using session token from configuration file. Consider using environment variables instead.")
        
        return credentials
    
    def get_value(self, key: str, default: Any = None) -> Any:
        """
        Get a value from the configuration.
        
        Args:
            key: Configuration key
            default: Default value if key is not found
            
        Returns:
            Configuration value, or default if not found
        """
        return self.config.get(key, default)
    
    def get_nested_value(self, keys: list, default: Any = None) -> Any:
        """
        Get a nested value from the configuration.
        
        Args:
            keys: List of keys to traverse
            default: Default value if key is not found
            
        Returns:
            Configuration value, or default if not found
        """
        current = self.config
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current