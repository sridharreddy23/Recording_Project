#!/usr/bin/env python3
"""Unit tests for main module helpers."""
import os
import json
import tempfile
import unittest
from argparse import Namespace

from src.main import (
    calculate_expected_segments,
    calculate_recommended_space_bytes,
    load_environment_from_dotenv,
    parse_cli_time,
    print_runtime_summary,
    run_preflight_checks,
    validate_arguments,
    write_run_report,
)


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

    def test_calculate_expected_segments(self):
        """Segment estimate should use 4-second chunk math."""
        self.assertEqual(calculate_expected_segments(0, 0), 0)
        self.assertEqual(calculate_expected_segments(100, 101), 1)
        self.assertEqual(calculate_expected_segments(100, 108), 2)
        self.assertEqual(calculate_expected_segments(100, 109), 3)

    def test_calculate_recommended_space_bytes(self):
        """Space estimate should include processing safety multiplier."""
        self.assertEqual(calculate_recommended_space_bytes(0), 0)
        expected = int(3 * (2 * 1024 * 1024) * 2.2)
        self.assertEqual(calculate_recommended_space_bytes(3), expected)

    def test_parse_cli_time_epoch(self):
        """Should parse epoch second input directly."""
        self.assertEqual(parse_cli_time("1711939200"), 1711939200)

    def test_parse_cli_time_iso_z(self):
        """Should parse ISO-8601 timestamp with Z suffix."""
        self.assertEqual(parse_cli_time("2026-04-01T00:00:00Z"), 1775001600)

    def test_parse_cli_time_date_only(self):
        """Should parse date-only input as UTC midnight."""
        self.assertEqual(parse_cli_time("2026-04-01"), 1775001600)

    def test_parse_cli_time_invalid(self):
        """Invalid timestamp strings should raise ValueError."""
        with self.assertRaises(ValueError):
            parse_cli_time("not-a-time")

    def test_run_preflight_checks(self):
        """Preflight should return expected structure and valid disk check."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = os.path.join(tmp_dir, "out.ts")
            result = run_preflight_checks(100, 116, output_path)
            self.assertEqual(result["expected_segments"], 4)
            self.assertIn("free_space_bytes", result)
            self.assertIn("recommended_space_bytes", result)
            self.assertIn("disk_ok", result)
            self.assertTrue(result["disk_ok"])

    def test_write_run_report(self):
        """Report writer should persist valid JSON to disk."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_path = os.path.join(tmp_dir, "report.json")
            payload = {"status": "success", "downloaded_files": 5}
            write_run_report(report_path, payload)
            with open(report_path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
            self.assertEqual(loaded, payload)

    def test_validate_arguments_rejects_non_positive_workers(self):
        """Workers must be positive when explicitly provided."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = os.path.join(tmp_dir, "config.json")
            with open(config_path, "w", encoding="utf-8") as fh:
                fh.write("{}")
            args = Namespace(
                config=config_path,
                output=os.path.join(tmp_dir, "out.ts"),
                sendgb_wait=1,
                start_utc=None,
                end_utc=None,
                workers=0,
                resume=False,
                resume_state=None,
                temp_dir=None,
            )
            with self.assertRaises(ValueError):
                validate_arguments(args)

    def test_validate_arguments_requires_resume_state_with_resume(self):
        """Resume mode should require a resume state file path."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = os.path.join(tmp_dir, "config.json")
            with open(config_path, "w", encoding="utf-8") as fh:
                fh.write("{}")
            args = Namespace(
                config=config_path,
                output=os.path.join(tmp_dir, "out.ts"),
                sendgb_wait=1,
                start_utc=None,
                end_utc=None,
                workers=None,
                resume=True,
                resume_state=None,
                temp_dir=None,
            )
            with self.assertRaises(ValueError):
                validate_arguments(args)

    def test_validate_arguments_requires_temp_dir_with_resume(self):
        """Resume mode should require a persistent temp dir."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = os.path.join(tmp_dir, "config.json")
            with open(config_path, "w", encoding="utf-8") as fh:
                fh.write("{}")
            args = Namespace(
                config=config_path,
                output=os.path.join(tmp_dir, "out.ts"),
                sendgb_wait=1,
                start_utc=None,
                end_utc=None,
                workers=None,
                resume=True,
                resume_state=os.path.join(tmp_dir, "resume.json"),
                temp_dir=None,
            )
            with self.assertRaises(ValueError):
                validate_arguments(args)


if __name__ == "__main__":
    unittest.main()
