#!/usr/bin/env python3
"""Unit tests for S3Reader resume/download interactions."""
import os
import tempfile
import unittest

from src.s3_reader import S3Reader


class TestS3ReaderResumeBehavior(unittest.TestCase):
    """Verify resume state does not seed stale/duplicate parse inputs."""

    def test_resume_state_does_not_preseed_or_include_stale_files(self):
        """State should be filtered to current manifest and rebuilt during scan."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            first = os.path.join(tmp_dir, "01012026", "00", "100-104.es")
            second = os.path.join(tmp_dir, "01012026", "00", "104-108.es")
            stale = os.path.join(tmp_dir, "01012026", "00", "108-112.es")

            os.makedirs(os.path.dirname(first), exist_ok=True)
            open(first, "w", encoding="utf-8").close()
            open(second, "w", encoding="utf-8").close()
            open(stale, "w", encoding="utf-8").close()

            reader = S3Reader(100, 108, "s3://bucket/prefix", tmp_dir)
            reader.files_to_download_map = {
                "s3://bucket/prefix/01012026/00/100-104.es": first,
                "s3://bucket/prefix/01012026/00/104-108.es": second,
            }
            resumed = reader.resume_from_state(
                {
                    "downloaded_files": [first, first, stale],
                    "files_found_locally": 3,
                    "files_failed": 0,
                }
            )

            self.assertTrue(resumed)
            self.assertEqual([], reader.downloaded_files)

            downloaded = reader.download_files_parallel()
            self.assertEqual([first, second], downloaded)


if __name__ == "__main__":
    unittest.main()
