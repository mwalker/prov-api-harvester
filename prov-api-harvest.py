import json
import sys
import requests
import time
import argparse
from urllib.parse import urlparse, parse_qs, urlencode

VERSION = "0.2.3"

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
            response = requests.get(url, timeout=30)  # Adding a timeout
            response.raise_for_status()
            return response.json(), response.headers
        except requests.exceptions.RequestException as e:
            consecutive_failures += 1
            wait_time = BASE_WAIT_TIME * consecutive_failures

            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                print(f"Rate limit exceeded (429). Attempt {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}. Waiting {wait_time} seconds before retrying...", file=sys.stderr)
            elif isinstance(e, requests.exceptions.Timeout):
                print(f"Request timed out. Attempt {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}. Waiting {wait_time} seconds before retrying...", file=sys.stderr)
            else:
                print(f"An error occurred: {e}. Attempt {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}. Waiting {wait_time} seconds before retrying...", file=sys.stderr)

            time.sleep(wait_time)
            print("Retrying request...", file=sys.stderr)

    raise TooManyFailedRequestsError(f"Failed to fetch data after {MAX_CONSECUTIVE_FAILURES} consecutive attempts. Exiting.")

def check_rate_limit(headers):
    remaining = int(headers.get('x-ratelimit-remaining-minute', 20))
    if remaining < 20:
        print(f"Rate limit approaching. Remaining: {remaining}. Sleeping for 2 seconds...", file=sys.stderr)
        time.sleep(2)

def stream_records():
    start = 0
    total_docs = float('inf')
    first_record = True

    print("[")  # Start of JSON array

    try:
        while start < total_docs:
            PARAMS['start'] = str(start)
            url = f"{BASE_URL}?{urlencode(PARAMS)}"

            print(f"Fetching data from {url}", file=sys.stderr)
            data, headers = fetch_data(url)

            docs = data['response']['docs']
            total_docs = data['response']['numFound']

            for doc in docs:
                if not first_record:
                    print(",")  # Add comma between records
                else:
                    first_record = False
                json.dump(doc, sys.stdout)
                sys.stdout.flush()  # Ensure the output is written immediately

            start += len(docs)

            print(f"Fetched {len(docs)} documents. Total: {start}/{total_docs}", file=sys.stderr)

            if start < total_docs:
                check_rate_limit(headers)

    except TooManyFailedRequestsError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        print("]")  # End of JSON array

def main():
    parser = argparse.ArgumentParser(description='Harvest data from PROV API with streaming output.')
    parser.add_argument('--query', type=str, help='Custom query to replace the default q parameter')
    parser.add_argument('--rows', type=int, help='Number of rows to fetch per request')
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    args = parser.parse_args()

    if args.query:
        PARAMS['q'] = args.query

    if args.rows:
        PARAMS['rows'] = str(args.rows)

    stream_records()

if __name__ == "__main__":
    main()
