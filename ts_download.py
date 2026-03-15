import boto3
import json
import os
import sys
import subprocess
import re
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"


def styled(label, message, color=Colors.CYAN, emoji="🍽️"):
    print(f"{color}{Colors.BOLD}{emoji} {label}{Colors.RESET} {message}")


def banner(title):
    garnish = "🍊" * 3
    print(f"\n{Colors.MAGENTA}{Colors.BOLD}{garnish} {title} {garnish}{Colors.RESET}")

# Define IST timezone offset (UTC+5:30)
IST_OFFSET = timedelta(hours=5, minutes=30)

# Load config
try:
    with open("config.json") as f:
        config = json.load(f)
except Exception as e:
    styled("[ERROR]", f"Failed to load config.json: {e}", Colors.RED, "🔥")
    sys.exit(1)

start_utc = config["start_utc"]
end_utc = config["end_utc"]
bucket = config["aws_conf"]["s3_bucket"]
region = config["aws_conf"]["aws_region"]
# Optimize prefix for September 26, 2025
prefix = "kcdok_001/abscbn-kcdok-001-dd/2025/09/26/"

banner("CONFIG MENU")
styled("Bucket", bucket, Colors.BLUE, "🪣")
styled("Prefix", prefix, Colors.BLUE, "🧭")
styled("Region", region, Colors.BLUE, "🌍")
styled(
    "Start Time",
    f"{start_utc} ({datetime.fromtimestamp(start_utc, tz=timezone.utc)} UTC / "
    f"{datetime.fromtimestamp(start_utc, tz=timezone.utc) + IST_OFFSET} IST)",
    Colors.BLUE,
    "⏱️",
)
styled(
    "End Time",
    f"{end_utc} ({datetime.fromtimestamp(end_utc, tz=timezone.utc)} UTC / "
    f"{datetime.fromtimestamp(end_utc, tz=timezone.utc) + IST_OFFSET} IST)",
    Colors.BLUE,
    "⏱️",
)

# Initialize S3 client
try:
    s3 = boto3.client("s3", region_name=region)
except Exception as e:
    styled("[ERROR]", f"Could not initialize S3 client: {e}", Colors.RED, "🔥")
    sys.exit(1)

# Create temp dir for chunks
os.makedirs("chunks", exist_ok=True)

# List objects using aws s3 ls
banner("SCANNING S3 PANTRY")
styled("[INFO]", "Scanning S3 objects with aws s3 ls…", Colors.CYAN, "🔎")
try:
    # Construct the S3 URI
    s3_uri = f"s3://{bucket}/{prefix}"
    # Run aws s3 ls command
    cmd = ["aws", "s3", "ls", s3_uri, "--recursive"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        styled("[ERROR]", f"Failed to list objects with aws s3 ls: {result.stderr}", Colors.RED, "🔥")
        sys.exit(1)
    
    # Parse output
    ts_files = []
    lines = result.stdout.splitlines()
    for line in lines:
        # Example line: "2025-09-26 08:29:05      12345 kcdok_001/abscbn-kcdok-001-dd/2025/09/26/02/1758894545_5005.ts"
        parts = line.strip().split(maxsplit=3)
        if len(parts) != 4:
            continue  # Skip malformed lines
        date_str, time_str, size, key = parts
        if not key.endswith(".ts"):
            continue  # Skip non-.ts files
        # Extract epoch timestamp from file name
        try:
            # Match epoch timestamp (e.g., 1758894545 from 1758894545_5005.ts)
            match = re.search(r"(\d{10})_", os.path.basename(key))
            if not match:
                styled("[WARN]", f"Skipping {key}: No epoch timestamp in file name", Colors.YELLOW, "⚠️")
                continue
            file_epoch = int(match.group(1))
            # Filter based on file name epoch timestamp
            if start_utc <= file_epoch <= end_utc:
                # Get LastModified for logging
                last_modified_utc = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                last_modified_utc = last_modified_utc.replace(tzinfo=timezone.utc)
                last_modified_ist = last_modified_utc + IST_OFFSET
                ts_files.append((file_epoch, key))
                styled(
                    "[DEBUG]",
                    f"Included {key}: FileEpoch={file_epoch} ({datetime.fromtimestamp(file_epoch, tz=timezone.utc)} UTC / "
                    f"{datetime.fromtimestamp(file_epoch, tz=timezone.utc) + IST_OFFSET} IST), "
                    f"LastModified={last_modified_utc} UTC / {last_modified_ist} IST",
                    Colors.GREEN,
                    "✅",
                )
            else:
                styled(
                    "[DEBUG]",
                    f"Skipped {key}: FileEpoch={file_epoch} ({datetime.fromtimestamp(file_epoch, tz=timezone.utc)} UTC / "
                    f"{datetime.fromtimestamp(file_epoch, tz=timezone.utc) + IST_OFFSET} IST) "
                    f"(outside {start_utc}-{end_utc})",
                    Colors.YELLOW,
                    "🥡",
                )
        except (ValueError, re.error) as e:
            styled("[WARN]", f"Skipping {key}: Invalid epoch timestamp in file name: {e}", Colors.YELLOW, "⚠️")
            continue
except subprocess.CalledProcessError as e:
    styled("[ERROR]", f"Failed to execute aws s3 ls: {e}", Colors.RED, "🔥")
    sys.exit(1)

# Sort chunks by file epoch timestamp
ts_files.sort()
if not ts_files:
    styled("[WARN]", "No TS chunks found in given time range.", Colors.YELLOW, "🫙")
    sys.exit(0)

styled("[INFO]", f"Found {len(ts_files)} chunks in range.", Colors.CYAN, "📦")

# Download chunks
local_files = []
for idx, (file_epoch, key) in enumerate(ts_files, start=1):
    utc_time = datetime.fromtimestamp(file_epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ist_time = (datetime.fromtimestamp(file_epoch, tz=timezone.utc) + IST_OFFSET).strftime("%Y-%m-%d %H:%M:%S")
    local_path = os.path.join("chunks", os.path.basename(key))
    try:
        styled(
            "[INFO]",
            f"Downloading {idx}/{len(ts_files)}: {key} "
            f"(FileEpoch={file_epoch} ({utc_time} UTC / {ist_time} IST))",
            Colors.CYAN,
            "⬇️",
        )
        s3.download_file(bucket, key, local_path)
        local_files.append(local_path)
    except ClientError as e:
        styled("[ERROR]", f"Failed to download {key}: {e}", Colors.RED, "🔥")
        continue  # Continue with next file

# Concatenate into single TS file
output_file = "output.ts"
try:
    with open(output_file, "wb") as outfile:
        for fname in local_files:
            with open(fname, "rb") as infile:
                outfile.write(infile.read())
    banner("SERVING COMPLETE")
    styled("[SUCCESS]", f"Concatenated {len(local_files)} chunks into {output_file}", Colors.GREEN, "🍽️")
except Exception as e:
    styled("[ERROR]", f"Failed to concatenate files: {e}", Colors.RED, "🔥")
    sys.exit(1)
