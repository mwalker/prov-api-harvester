import json
import sys
import time
import argparse
import os
from urllib.parse import urlparse, parse_qs, urlencode

import requests

VERSION = "0.3.2"

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
PROGRESS_FILE = "harvest_progress.json"


class TooManyFailedRequestsError(Exception):
    pass


def fetch_data(url):
    consecutive_failures = 0
    while consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json(), response.headers
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
    remaining = int(headers.get('x-ratelimit-remaining-minute', 20))
    if remaining < 20:
        print(
            f"Rate limit approaching. Remaining: {remaining}. Sleeping for 2 seconds...",
            file=sys.stderr)
        time.sleep(2)


def save_progress(start, total_docs):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({"start": start, "total_docs": total_docs}, f)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return None


def remove_trailing_bracket(filename):
    with open(filename, 'rb+') as f:
        f.seek(-1, os.SEEK_END)
        if f.read(1) == b']':
            f.seek(-1, os.SEEK_END)
            f.truncate()


def stream_records(resume=False, output_file='output.json'):
    progress = load_progress() if resume else None
    start = progress['start'] if progress else 0
    total_docs = progress['total_docs'] if progress else float('inf')
    first_record = not resume

    if resume:
        print(f"Resuming from record {start}", file=sys.stderr)
        remove_trailing_bracket(output_file)
    else:
        with open(output_file, 'w') as f:
            f.write("[")  # Start of JSON array

    overall_start_time = time.time()
    total_fetched = 0

    try:
        while start < total_docs:
            PARAMS['start'] = str(start)
            url = f"{BASE_URL}?{urlencode(PARAMS)}"

            print(f"Fetching data from {url}", file=sys.stderr)
            fetch_start_time = time.time()
            data, headers = fetch_data(url)
            fetch_end_time = time.time()

            docs = data['response']['docs']
            total_docs = data['response']['numFound']

            with open(output_file, 'a') as f:
                for doc in docs:
                    if not first_record:
                        f.write(",")  # Add comma between records
                    else:
                        first_record = False
                    json.dump(doc, f)
                    f.flush()  # Ensure the output is written immediately

            start += len(docs)
            total_fetched += len(docs)

            fetch_duration = fetch_end_time - fetch_start_time
            overall_duration = fetch_end_time - overall_start_time
            overall_rate = total_fetched / overall_duration if overall_duration > 0 else 0

            print(f"Fetched {len(docs)} documents in {fetch_duration:.2f} seconds. "
                  f"Total: {start}/{total_docs}. "
                  f"Overall rate: {overall_rate:.2f} rows/second", file=sys.stderr)

            save_progress(start, total_docs)

            if start < total_docs:
                check_rate_limit(headers)

    except TooManyFailedRequestsError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print(
            "\nInterrupted. Progress saved. You can resume later using the --resume flag.",
            file=sys.stderr)
        sys.exit(1)
    finally:
        with open(output_file, 'a') as f:
            f.write("]")  # End of JSON array


def main():
    parser = argparse.ArgumentParser(
        description='Harvest data from PROV API with streaming output and resume capability.')
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
        default='output.json',
        help='Output file name')
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {VERSION}')
    args = parser.parse_args()

    if args.query:
        PARAMS['q'] = args.query

    if args.rows:
        PARAMS['rows'] = str(args.rows)

    stream_records(resume=args.resume, output_file=args.output)


if __name__ == "__main__":
    main()
