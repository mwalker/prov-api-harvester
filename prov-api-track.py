# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests",
# ]
# ///

"""
This script harvests data from the Public Record Office Victoria (PROV) API
in a way that makes it easy to track changes over time.

It fetches data for series, functions, agencies, or consignments based on user input and outputs
the results as JSON. The script handles pagination and rate limiting to ensure
all data is collected while respecting API usage limits. It also implements
retry logic for failed API requests.
"""

import json
import sys
import time
import argparse
from urllib.parse import urlencode
from datetime import date

import requests

VERSION = "0.1.6"

BASE_URL = "https://api.prov.vic.gov.au/search/query"
PARAMS = {
    "rows": "1000",
    "start": "0",
    "wt": "json",
}

MAX_RETRIES = 6
BASE_WAIT_TIME = 63


def normalise_keys(docs):
    """
    Normalize the keys across all documents to ensure consistent structure and order.

    Args:
        docs (list): List of dictionaries representing documents.

    Returns:
        list: List of dictionaries with normalized and alphabetically sorted keys.
    """
    all_keys = set()
    for doc in docs:
        all_keys.update(doc.keys())

    sorted_keys = sorted(all_keys)

    normalised_docs = []
    for doc in docs:
        normalised_doc = {key: doc.get(key, None) for key in sorted_keys}
        normalised_docs.append(normalised_doc)

    return normalised_docs


def fetch_data(url):
    """
    Fetch data from the given URL with retry logic.

    Args:
        url (str): The URL to fetch data from.

    Returns:
        tuple: A tuple containing the JSON response and headers.

    Raises:
        requests.RequestException: If all retry attempts fail.
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json(), response.headers
        except requests.RequestException as e:
            wait_time = BASE_WAIT_TIME * (attempt + 1)
            print(
                f"Request failed. Error: {e}. Retrying in {wait_time} seconds... (Attempt {
                    attempt + 1}/{MAX_RETRIES})",
                file=sys.stderr)
            time.sleep(wait_time)

    raise requests.RequestException(
        f"Max retries reached. Unable to fetch data. Last error: {e}")


def check_rate_limit(headers):
    """
    Check the rate limit and pause if necessary.

    Args:
        headers (dict): The response headers containing rate limit information.
    """
    remaining = int(headers.get('x-ratelimit-remaining-minute', 20))
    if remaining < 20:
        print(
            f"Rate limit approaching. Remaining: {remaining}. Sleeping for 2 seconds...",
            file=sys.stderr)
        time.sleep(2)


def custom_sort_key(doc):
    """
    Custom sorting function for documents.

    Args:
        doc (dict): A document dictionary.

    Returns:
        tuple: A tuple for sorting, containing the alphabetical part and numerical parts.
    """
    id_value = doc.get("identifier.PROV_ACM.id", "")
    parts = id_value.split(" ", 1)

    if len(parts) == 2:
        alpha_part = parts[0].upper()
        num_parts = parts[1].split("/")

        try:
            num_part1 = int(num_parts[0])
        except ValueError:
            num_part1 = float('inf')

        if len(num_parts) > 1:
            num_part2 = num_parts[1].upper()
        else:
            num_part2 = ""
    else:
        alpha_part = id_value.upper()
        num_part1 = float('inf')
        num_part2 = ""

    return (alpha_part, num_part1, num_part2)


def get_plural(type_name):
    """
    Get the plural form of the given type name.

    Args:
        type_name (str): The singular form of the type name.

    Returns:
        str: The plural form of the type name.
    """
    if type_name == "series":
        return "series"
    if type_name == "agency":
        return "agencies"
    return f"{type_name}s"


def fetch_all_docs(type_query):
    """
    Fetch all documents for a given type query.

    Args:
        type_query (str): The query string for the document type.

    Returns:
        list: A list of all fetched documents.
    """
    all_docs = []
    start = 0
    total_docs = float('inf')

    while start < total_docs:
        PARAMS['start'] = str(start)
        PARAMS['q'] = type_query
        url = f"{BASE_URL}?{urlencode(PARAMS)}"

        print(f"Fetching data from {url}", file=sys.stderr)
        try:
            data, headers = fetch_data(url)
        except requests.RequestException as e:
            print(
                f"Failed to fetch data after multiple retries: {e}",
                file=sys.stderr)
            sys.exit(1)

        docs = data['response']['docs']
        all_docs.extend(docs)

        total_docs = data['response']['numFound']
        start += len(docs)

        print(
            f"Fetched {
                len(docs)} documents. Total: {
                len(all_docs)}/{total_docs}",
            file=sys.stderr)

        if start < total_docs:
            check_rate_limit(headers)

    return all_docs


def main():
    """
    Main function to run the PROV API data harvester.

    Parses command line arguments, fetches data from the API, and outputs the results as JSON.
    """
    parser = argparse.ArgumentParser(
        description='Harvest data from PROV API for easy tracking.')
    parser.add_argument(
        '--type',
        type=str,
        choices=['series', 'function', 'agency', 'consignment'],
        required=True,
        help='Type of data to fetch (series, function, agency, or consignment)')
    parser.add_argument(
        '--output',
        type=str,
        help='Output file name for JSON data (default: prov-{plural_type}-{date}.json)')
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {VERSION}')
    args = parser.parse_args()

    type_queries = {
        'series': 'category:(Series)',
        'function': 'category:(Function)',
        'agency': 'category:(Agency)',
        'consignment': 'category:(Consignment)'
    }

    all_docs = fetch_all_docs(type_queries[args.type])

    print("Normalising keys...", file=sys.stderr)
    output_docs = normalise_keys(all_docs)

    print("Sorting documents...", file=sys.stderr)
    sorted_docs = sorted(output_docs, key=custom_sort_key)

    # Determine the output file name
    if args.output:
        output_file = args.output
    else:
        today = date.today().isoformat()
        plural_type = get_plural(args.type)
        output_file = f"prov-{plural_type}-{today}.json"

    # Write the JSON data to the file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(sorted_docs, f, indent=2)

    print(f"Data written to {output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
