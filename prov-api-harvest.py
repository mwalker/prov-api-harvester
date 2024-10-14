# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests",
#     "xattr",
#     "zstandard",
# ]
# ///

"""
PROV API Harvester with Optional zstd Compression

This script harvests data from the Public Record Office Victoria (PROV) API,
with streaming output, resume capability, and optional zstd compression. It allows for
custom queries, adjustable batch sizes, and can resume interrupted downloads.

Usage:
    python prov-api-harvest.py [options]

Options:
    --query      Custom query to replace the default q parameter
    --rows       Number of rows to fetch per request
    --resume     Resume from last saved progress
    --output     Output file name (default: output.json or output.json.zst if compressed)
    --compress   Enable zstd compression for output
    --debug      Enable debug mode to print additional information
    --version    Show the version number and exit

The script uses rate limiting, error handling, and optional zstd compression to ensure
reliable and efficient data retrieval and storage.
"""

import json
import sys
import time
import argparse
import os
from urllib.parse import urlencode

import requests
import xattr
import zstandard as zstd

VERSION = "0.7.0"  # Updated version number

BASE_URL = "https://api.prov.vic.gov.au/search/query"
PARAMS = {
    "rows": "1000",
    "start": "0",
    "sort": "identifier.PROV_ACM.id asc",
    "wt": "json",
    "q": "*:*"
}

MAX_CONSECUTIVE_FAILURES = 6
BASE_WAIT_TIME = 63  # seconds
PROGRESS_XATTR_NAME = "org.gunzel.prov-api-harvester.progress"


class TooManyFailedRequestsError(Exception):
    """
    Custom exception raised when the maximum number of consecutive
    failed requests is reached.
    """


def fetch_data(url, debug=False):
    """
    Fetch data from the given URL with error handling and retries.

    Args:
        url (str): The URL to fetch data from.
        debug (bool): If True, print debug information.

    Returns:
        tuple: A tuple containing the JSON response, headers, and content length.

    Raises:
        TooManyFailedRequestsError: If the maximum number of retries is exceeded.
    """
    consecutive_failures = 0
    while consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        try:
            if debug:
                print(f"Fetching data from {url}", file=sys.stderr)
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json(), response.headers, len(response.content)
        except requests.exceptions.RequestException as e:
            consecutive_failures += 1
            wait_time = BASE_WAIT_TIME * consecutive_failures

            if isinstance(
                    e,
                    requests.exceptions.HTTPError) and e.response.status_code == 429:
                print(
                    f"Rate limit exceeded (429). Attempt {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}. Waiting {wait_time} seconds before retrying...",
                    file=sys.stderr)
            elif isinstance(e, requests.exceptions.Timeout):
                print(
                    f"Request timed out. Attempt {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}. Waiting {wait_time} seconds before retrying...",
                    file=sys.stderr)
            else:
                print(
                    f"An error occurred: {e}. Attempt {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}. Waiting {wait_time} seconds before retrying...",
                    file=sys.stderr)

            time.sleep(wait_time)
            print("Retrying request...", file=sys.stderr)

    raise TooManyFailedRequestsError(
        f"Failed to fetch data after {MAX_CONSECUTIVE_FAILURES} consecutive attempts. Exiting.")


def check_rate_limit(headers):
    """
    Check the rate limit from the response headers and sleep if necessary.

    Args:
        headers (dict): The response headers containing rate limit information.
    """
    remaining = int(headers.get('x-ratelimit-remaining-minute', 20))
    time.sleep(6)
    if remaining < 20:
        print(
            f"Rate limit approaching. Remaining: {remaining}. Sleeping for 2 seconds...",
            file=sys.stderr)
        time.sleep(2)


def load_progress(output_file):
    """
    Load the progress from the output file's extended attribute if it exists.

    Args:
        output_file (str): The path to the output file.

    Returns:
        dict or None: The progress data if found, None otherwise.
    """
    if not os.path.exists(output_file):
        return None

    try:
        progress_data = xattr.getxattr(
            output_file, PROGRESS_XATTR_NAME).decode('utf-8')
        return json.loads(progress_data)
    except (OSError, ValueError):
        return None


def save_progress(output_file, progress_data):
    """
    Save the progress data to the output file's extended attribute.

    Args:
        output_file (str): The path to the output file.
        progress_data (dict): The progress data to save.
    """
    progress_json = json.dumps(progress_data)
    xattr.setxattr(
        output_file,
        PROGRESS_XATTR_NAME,
        progress_json.encode('utf-8'))


def prepare_output_file(output_file, resume, compress):
    """
    Prepare the output file for writing, either by truncating or appending.

    Args:
        output_file (str): The path to the output file.
        resume (bool): If True, prepare for resuming; otherwise, start fresh.
        compress (bool): If True, use zstd compression; otherwise, use plain text.
    """
    if not resume:
        if compress:
            with zstd.open(output_file, 'wb') as f:
                f.write(b"[")
        else:
            with open(output_file, 'wb') as f:
                f.write(b"[")


def stream_records(
        resume=False,
        output_file='output.json',
        compress=False,
        debug=False):
    """
    Stream records from the PROV API, optionally compressing and writing them to the output file.

    Args:
        resume (bool): If True, resume from the last saved progress.
        output_file (str): The path to the output file.
        compress (bool): If True, use zstd compression; otherwise, use plain text.
        debug (bool): If True, print debug information.
    """
    progress = load_progress(output_file) if resume else None
    start = progress['start'] if progress else 0
    total_bytes = progress['total_bytes'] if progress else 0
    total_docs = progress['total_docs'] if progress else float('inf')
    first_record = not resume

    if resume:
        print(f"Resuming from record {start}", file=sys.stderr)

    prepare_output_file(output_file, resume, compress)

    overall_start_time = time.time()
    total_fetched = 0

    try:
        file_opener = zstd.open if compress else open

        with file_opener(output_file, 'ab') as file:
            while start < total_docs:
                PARAMS['start'] = str(start)
                url = f"{BASE_URL}?{urlencode(PARAMS)}"

                fetch_start_time = time.time()
                data, headers, content_size = fetch_data(url, debug)
                fetch_end_time = time.time()

                docs = data['response']['docs']
                total_docs = data['response']['numFound']

                for doc in docs:
                    if not first_record:
                        file.write(b",\n")
                    else:
                        first_record = False
                    json_data = json.dumps(doc, ensure_ascii=False).encode()
                    file.write(json_data)
                    file.flush()

                start += len(docs)
                total_fetched += len(docs)
                total_bytes += content_size

                # Save progress after each batch
                save_progress(output_file, {
                    'start': start,
                    'total_bytes': total_bytes,
                    'total_docs': total_docs
                })

                fetch_duration = fetch_end_time - fetch_start_time
                overall_duration = fetch_end_time - overall_start_time
                overall_rate = total_fetched / overall_duration if overall_duration > 0 else 0

                print(f"Fetched {len(docs)} documents in {fetch_duration:.2f} seconds. "
                      f"Total: {start}/{total_docs}. "
                      f"Overall rate: {overall_rate:.2f} rows/second. "
                      f"Downloaded: {content_size} bytes (Total: {total_bytes} bytes)", file=sys.stderr)

                if start < total_docs:
                    check_rate_limit(headers)
                else:
                    file.write(b"]")  # End of JSON array

    except TooManyFailedRequestsError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print(
            "\nInterrupted. Progress saved. You can resume later using the --resume flag.",
            file=sys.stderr)
        sys.exit(1)


def main():
    """
    Main function to parse command-line arguments and initiate the data harvesting process.
    """
    parser = argparse.ArgumentParser(
        description='Harvest data from PROV API with streaming output, resume capability, and optional zstd compression.')
    parser.add_argument(
        '--query',
        type=str,
        help='Custom query to replace the default q parameter')
    parser.add_argument(
        '--rows',
        type=int,
        help='Number of rows to fetch per request')
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from last saved progress')
    parser.add_argument(
        '--output',
        type=str,
        help='Output file name (default: output.json or output.json.zst if compressed)')
    parser.add_argument(
        '--compress',
        action='store_true',
        help='Enable zstd compression for output')
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode to print additional information')
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {VERSION}')
    args = parser.parse_args()

    if args.query:
        PARAMS['q'] = args.query

    if args.rows:
        PARAMS['rows'] = str(args.rows)

    if args.output:
        output_file = args.output
    else:
        output_file = 'output.json.zst' if args.compress else 'output.json'

    stream_records(
        resume=args.resume,
        output_file=output_file,
        compress=args.compress,
        debug=args.debug)


if __name__ == "__main__":
    main()
