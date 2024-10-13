import json
import sys
import requests
import time
import argparse
from urllib.parse import urlparse, parse_qs, urlencode

VERSION = "0.1.0"

BASE_URL = "https://api.prov.vic.gov.au/search/query"
PARAMS = {
    "rows": "1000",
    "start": "0",
    "sort": "identifier.PROV_ACM.id asc",
    "wt": "json",
    "q": "*:*"
}

def normalize_keys(docs):
    all_keys = set()
    for doc in docs:
        all_keys.update(doc.keys())

    normalized_docs = []
    for doc in docs:
        normalized_doc = {key: doc.get(key, None) for key in all_keys}
        normalized_docs.append(normalized_doc)

    return normalized_docs

def fetch_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json(), response.headers

def check_rate_limit(headers):
    remaining = int(headers.get('x-ratelimit-remaining-minute', 20))
    if remaining < 20:
        print(f"Rate limit approaching. Remaining: {remaining}. Sleeping for 2 seconds...", file=sys.stderr)
        time.sleep(2)

def main():
    parser = argparse.ArgumentParser(description='Harvest data from PROV API with optional key normalization and custom query.')
    parser.add_argument('--normalize', action='store_true', help='Normalize keys across all documents')
    parser.add_argument('--rows', type=int, help='Number of rows to fetch per request')
    parser.add_argument('--query', type=str, help='Custom query to replace the default q parameter')
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    args = parser.parse_args()

    if args.query:
        PARAMS['q'] = args.query

    if args.rows:
        PARAMS['rows'] = str(args.rows)

    all_docs = []
    start = 0
    total_docs = float('inf')

    while start < total_docs:
        PARAMS['start'] = str(start)
        url = f"{BASE_URL}?{urlencode(PARAMS)}"

        print(f"Fetching data from {url}", file=sys.stderr)
        data, headers = fetch_data(url)

        docs = data['response']['docs']
        all_docs.extend(docs)

        total_docs = data['response']['numFound']
        start += len(docs)

        print(f"Fetched {len(docs)} documents. Total: {len(all_docs)}/{total_docs}", file=sys.stderr)

        if start < total_docs:
            check_rate_limit(headers)

    if args.normalize:
        print("Normalizing keys...", file=sys.stderr)
        output_docs = normalize_keys(all_docs)
    else:
        output_docs = all_docs

    print(json.dumps(output_docs, indent=2))

if __name__ == "__main__":
    main()
