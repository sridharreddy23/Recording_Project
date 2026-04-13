#!/usr/bin/env python3
\"\""
Validation and progress state utilities.
\"\"\"

import os
import json
from typing import Dict, Any
from .logging import log

def validate_config(config: Dict[str, Any]) -> bool:
    \"\"\"Validates configuration dictionary. Raises ValueError on invalid config.\"\"\"

    if not isinstance(config, dict):
        raise TypeError(f\"Config must be dict, got {type(config)}\")
    
    required_keys = [\"start_utc\", \"end_utc\", \"s3_prefix\", \"aws_conf\"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise ValueError(f\"Missing keys: {', '.join(missing)}\")
    
    aws_conf = config.get(\"aws_conf\", {})
    if not isinstance(aws_conf, dict):
        raise TypeError(f\"aws_conf must be dict, got {type(aws_conf)}\")
    
    if \"s3_bucket\" not in aws_conf:
        raise ValueError(\"Missing aws_conf.s3_bucket\")
    
    if not isinstance(aws_conf[\"s3_bucket\"], str) or not aws_conf[\"s3_bucket\"]:
        raise ValueError(\"s3_bucket must be non-empty str\")
    
    try:
        start_utc = int(config[\"start_utc\"])
        end_utc = int(config[\"end_utc\"])
    except (ValueError, TypeError) as e:
        raise ValueError(f\"Invalid timestamps: {e}\") from e
    
    if start_utc <= 0 or end_utc <= 0:
        raise ValueError(f\"Timestamps must be positive (start={start_utc}, end={end_utc})\")
    if start_utc >= end_utc:
        raise ValueError(f\"start_utc ({start_utc}) < end_utc ({end_utc})\")
    
    s3_prefix = config[\"s3_prefix\"]
    if not isinstance(s3_prefix, str):
        raise TypeError(f\"s3_prefix must be str, got {type(s3_prefix)}\")
    
    normalized = s3_prefix.strip()
    if not normalized:
        raise ValueError(\"s3_prefix must be non-empty\")
    
    if normalized.startswith(\"s3://\"):
        raise ValueError(\"s3_prefix should not include s3://bucket\")
    
    if aws_conf.get(\"access_key\") or aws_conf.get(\"secret_key\"):
        log.warning(\"AWS creds in config.json. Use env vars instead.\")
    
    return True

def save_progress_state(state_file: str, progress_data: Dict[str, Any]) -> bool:
    \"\"\"Saves progress state to JSON file.\"\"\"

    try:
        state_dir = os.path.dirname(os.path.abspath(state_file))
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, indent=2)
        log.debug(f\"Progress saved: {state_file}\")
        return True
    except Exception as e:
        log.error(f\"Save progress failed {state_file}: {e}\")
        return False

def load_progress_state(state_file: str) -> Dict[str, Any]:
    \"\"\"Loads progress state from JSON file.\"\"\"

    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        log.debug(f\"Progress loaded: {state_file}\")
        return data
    except FileNotFoundError:
        log.debug(f\"No state file: {state_file}\")
        return {}
    except Exception as e:
        log.warning(f\"Load state failed {state_file}: {e}\")
        return {}