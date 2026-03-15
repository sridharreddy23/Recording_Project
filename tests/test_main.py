#!/usr/bin/env python3
"""Unit tests for main module helpers."""
import os
import tempfile
import unittest
from argparse import Namespace

from src.main import load_environment_from_dotenv, print_runtime_summary


class TestMainHelpers(unittest.TestCase):
    """Test cases for helper functions in main.py."""

    def test_load_environment_from_dotenv_loads_values(self):
        """Should load env vars from config directory .env when present."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = os.path.join(tmp_dir, "config.json")
            env_path = os.path.join(tmp_dir, ".env")

            with open(config_path, "w", encoding="utf-8") as fh:
                fh.write("{}")

            with open(env_path, "w", encoding="utf-8") as fh:
                fh.write("AWS_DEFAULT_REGION=us-west-2\n")

            original = os.environ.get("AWS_DEFAULT_REGION")
            os.environ.pop("AWS_DEFAULT_REGION", None)
            try:
                loaded = load_environment_from_dotenv(config_path)
                self.assertIn(env_path, loaded)
                self.assertEqual(os.environ.get("AWS_DEFAULT_REGION"), "us-west-2")
            finally:
                if original is None:
                    os.environ.pop("AWS_DEFAULT_REGION", None)
                else:
                    os.environ["AWS_DEFAULT_REGION"] = original

    def test_load_environment_from_dotenv_does_not_override_existing(self):
        """Existing environment vars should not be overridden by .env values."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = os.path.join(tmp_dir, "config.json")
            env_path = os.path.join(tmp_dir, ".env")

            with open(config_path, "w", encoding="utf-8") as fh:
                fh.write("{}")

            with open(env_path, "w", encoding="utf-8") as fh:
                fh.write("AWS_ACCESS_KEY_ID=from-dotenv\n")

            original = os.environ.get("AWS_ACCESS_KEY_ID")
            os.environ["AWS_ACCESS_KEY_ID"] = "from-existing-env"
            try:
                loaded = load_environment_from_dotenv(config_path)
                self.assertIn(env_path, loaded)
                self.assertEqual(os.environ.get("AWS_ACCESS_KEY_ID"), "from-existing-env")
            finally:
                if original is None:
                    os.environ.pop("AWS_ACCESS_KEY_ID", None)
                else:
                    os.environ["AWS_ACCESS_KEY_ID"] = original

    def test_print_runtime_summary_smoke(self):
        """Runtime summary helper should run without errors."""
        args = Namespace(config="config.json", output="output.ts", gofile=False, sendgb=False)
        print_runtime_summary(args, 1609459200, 1609459300, "s3://bucket/path")


if __name__ == "__main__":
    unittest.main()
