import json
import sys
import time
import argparse
import os
from urllib.parse import urlparse, parse_qs, urlencode

import requests

VERSION = "0.4.1"  # Updated version number

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


def load_progress(output_file):
    if not os.path.exists(output_file):
        return None
    with open(output_file, 'rb') as f:
        try:
            f.seek(-1024, os.SEEK_END)  # Look at the last 1KB of the file
        except IOError:
            f.seek(0)
        last_lines = f.read().decode('utf-8').split('\n')
        for line in reversed(last_lines):
            if line.startswith("#RESUME:"):
                return json.loads(line[8:])
    return None


def prepare_output_file(output_file, resume):
    if resume:
        with open(output_file, 'r+b') as f:
            f.seek(-1, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
            f.truncate()
    else:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("[")


def stream_records(resume=False, output_file='output.json'):
    progress = load_progress(output_file) if resume else None
    start = progress['start'] if progress else 0
    total_docs = progress['total_docs'] if progress else float('inf')
    first_record = not resume

    if resume:
        print(f"Resuming from record {start}", file=sys.stderr)

    prepare_output_file(output_file, resume)

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

            with open(output_file, 'a', encoding='utf-8') as f:
                for doc in docs:
                    if not first_record:
                        f.write(",\n")  # Add comma (JSON) and \n (readability) between records
                    else:
                        first_record = False
                    json.dump(doc, f, ensure_ascii=False)
                    f.flush()  # Ensure the output is written immediately

            start += len(docs)
            total_fetched += len(docs)

            fetch_duration = fetch_end_time - fetch_start_time
            overall_duration = fetch_end_time - overall_start_time
            overall_rate = total_fetched / overall_duration if overall_duration > 0 else 0

            print(f"Fetched {len(docs)} documents in {fetch_duration:.2f} seconds. "
                  f"Total: {start}/{total_docs}. "
                  f"Overall rate: {overall_rate:.2f} rows/second", file=sys.stderr)

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
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write("]")  # End of JSON array
            if start < total_docs:
                f.write(
                    f"\n#RESUME:{json.dumps({'start': start, 'total_docs': total_docs})}")


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
