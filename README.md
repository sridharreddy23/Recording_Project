# INPUT_REC - S3 ES Downloader & Parser with Upload Support

A robust Python tool for downloading Elementary Stream (ES) files from AWS S3, parsing them into Transport Stream (TS) files, and optionally uploading to cloud storage services (SendGB/GoFile).

## Overview

This tool automates the process of downloading ES media segment files from AWS S3 within a specified time range, parsing and concatenating segments into a single TS output file, and optionally uploading the final TS file to SendGB (with GoFile fallback) or directly to GoFile.

It features parallel downloads, robust error handling, progress tracking, and supports resumable operations.

## Features

- Parallel S3 Downloads: Efficiently download multiple files concurrently
- Resume Capability: Resume interrupted downloads and parsing operations
- Progress Tracking: Visual progress indicators and detailed logging
- Memory Efficient: Buffered I/O for handling large files
- Configurable: Extensive configuration options via JSON
- Upload Support: Automatic upload to SendGB or GoFile with fallback
- Robust Error Handling: Comprehensive error handling and retry mechanisms

## Requirements

- Python 3.7 or higher
- pip package manager
- AWS credentials (environment variables or config file)
- GoFile API token (for uploads, optional)
- Selenium + ChromeDriver (for SendGB uploads, optional)

## Installation

First, clone the repository and navigate to the project directory.

Install all dependencies by running pip install with the requirements.txt file. This will install boto3, colorama, Flask, and other necessary packages.

Set up AWS credentials using one of three methods. You can use environment variables (recommended) by exporting AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION. Alternatively, you can add credentials to the config.json file, or use the standard AWS credentials file located at ~/.aws/credentials.

For uploads to work, you'll need to set up a GoFile token by exporting GOFILE_TOKEN as an environment variable. You can obtain your token from the GoFile.io API after logging in.

## Quick Start for Persistent Environment Variables

To avoid re-exporting AWS and GoFile variables every time you open a new terminal, create a `.env` file in the project root (or in the same directory as your config JSON):

```env
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1
GOFILE_TOKEN=your-gofile-token
```

The CLI now auto-loads `.env` values on startup (without overriding variables already set in your shell).

If you also want these variables globally in your shell, add them to `~/.bashrc` or `~/.zshrc` and reload your shell.

## Configuration

Create a config.json file in the project root. The configuration file requires several parameters:

- start_utc: Start time in Unix epoch seconds (UTC format)
- end_utc: End time in Unix epoch seconds (UTC format)
- s3_prefix: S3 path prefix where ES files are stored (without the s3://bucket/ prefix)

The aws_conf section contains:
- aws_region: AWS region such as us-east-1
- s3_bucket: Your S3 bucket name
- access_key: AWS access key ID (optional if using environment variables)
- secret_key: AWS secret access key (optional if using environment variables)
- session_token: AWS session token (optional, for temporary credentials)

## Usage

### Basic Usage

You can run the tool using the run.sh convenience script, passing the config file and output file as arguments. Alternatively, you can run it directly with Python using the src.main module.

### Upload Options

To upload directly to GoFile, use the --gofile flag when running the command.

To upload to SendGB with automatic GoFile fallback, use the --sendgb flag. You can also specify a custom wait timeout using --sendgb-wait followed by the number of seconds (default is 600 seconds).

### Command-Line Options

The tool accepts a config file path and output file path as required positional arguments. Optional arguments include --sendgb to try SendGB upload with GoFile fallback, --gofile to upload directly to GoFile (these are now mutually exclusive), --sendgb-wait to specify seconds to wait for SendGB upload, and --debug or -d to enable debug logs. Use --help or -h to display the full help message.

The CLI also prints a clearer run summary (config, output target, time range, S3 prefix, and upload mode) before download starts for better readability and presentation in terminal logs.

### Examples

For basic download and parse operations, run the command with just the config file and output file.

To download, parse, and upload to GoFile, first export your GOFILE_TOKEN as an environment variable, then run the command with the --gofile flag.

For SendGB uploads with fallback, export the GOFILE_TOKEN and use the --sendgb flag. You can adjust the wait timeout as needed.

To enable debug logging for troubleshooting, add the --debug flag to any command.

## How It Works

### Process Flow

The tool follows a five-stage process:

First, it loads configuration from the config.json file, sets up AWS credentials from environment variables or the config file, and validates all parameters.

Next, it discovers files in S3 by listing files in the bucket matching the specified prefix, filtering files by timestamp (between start_utc and end_utc), and identifying ES files in Elementary Stream format.

Then it performs parallel downloads, downloading matching ES files concurrently (up to 10 simultaneous downloads), saving files to a temporary directory, and handling retries and error recovery automatically.

During ES parsing, it reads ES files sequentially, extracts TS (Transport Stream) payloads, concatenates them into a single output TS file, and uses buffered I/O for memory efficiency.

Finally, if upload is requested, it can upload via SendGB (attempts upload using Selenium automation, validates the link, and falls back to GoFile if a payment link is detected or upload fails) or directly via GoFile (uploads using authenticated API, shows upload progress, and saves download link to a text file).

### Architecture

The project is structured into modular components for maintainability:

The main entry point and orchestration logic is in src/main.py. Configuration management is handled by src/config_manager.py, which loads and validates the configuration. S3 operations including file listing and parallel downloading are managed by src/s3_reader.py. ES file parsing and TS extraction are handled by src/es_parser.py. GoFile API integration with progress tracking is implemented in src/gofile_uploader.py. Various utility functions and helpers are located in src/utils.py.

## Output

The tool produces two types of output:

The primary output is the final concatenated Transport Stream file at the path you specify.

If upload succeeds, the tool also creates link files containing the download URLs. These are saved as output.ts.gofile_link.txt for GoFile uploads, or output.ts.sendgb_link.txt for SendGB uploads.

## Error Handling

The tool includes comprehensive error handling. AWS errors trigger automatic retry with exponential backoff. Network errors use configurable retry attempts. Upload failures automatically fallback from SendGB to GoFile when applicable. When uploads fail, debug information is saved to disk for post-mortem analysis.

## Troubleshooting

### Common Issues

If you encounter an AWS credentials error, set the appropriate environment variables or add credentials to your config.json file.

If you see a missing GOFILE_TOKEN error, export the token as an environment variable before running the command.

For S3 access denied errors, verify that your AWS credentials have read access to the specified bucket.

If no files are found for parsing, double-check your start_utc, end_utc, and s3_prefix configuration values to ensure they match files that exist in your S3 bucket.

### Debug Mode

Enable detailed logging by adding the --debug flag to your command. This will display AWS request and response details, file processing progress information, upload status and progress updates, and detailed error information when issues occur.

## Dependencies

Core dependencies listed in requirements.txt include boto3 (AWS SDK for Python, version 1.26.0 or higher), colorama (for colored terminal output, version 0.4.6 or higher), requests (HTTP library, version 2.25.0 or higher), Flask (web framework, version 2.0.1 or higher), python-dotenv (environment variable management, version 0.19.0 or higher), and gunicorn (WSGI HTTP server, version 20.1.0 or higher).

Optional dependencies that enhance functionality include tqdm (progress bars, version 4.60.0 or higher), requests-toolbelt (multipart encoder for accurate upload progress, version 0.9.0 or higher), and selenium (for SendGB uploads if needed).

## License

[Specify your license here]

## Contributing

[Add contribution guidelines if applicable]

## Support

For issues and questions, please open an issue on the repository.
# Recording_Project
