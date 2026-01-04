# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests",
#     "zstandard",
# ]
# ///

"""
PROV API Harvester with Optional zstd Compression

This script harvests data from the Public Record Office Victoria (PROV) API,
with streaming output and optional zstd compression.

Usage:
    python prov-api-harvest.py [options]

Options:
    --query      Custom query to replace the default q parameter
    --series     One or more positive integers representing series IDs to query
    --series-batch  Enable batch processing of series (batch size determined by --max-per-batch and --max-series-per-batch)
    --series-min    Minimum series ID to include (default: 1, used with --series-batch)
    --series-max    Maximum series ID to include (used with --series-batch)
    --max-per-batch   Maximum results per batch before splitting (default: 200000, used with --series-batch)
    --max-series-per-batch  Maximum series per batch to prevent overlength request errors (default: 200, used with --series-batch)
    --include-related-entities  Always include relatedEntity records (otherwise only if >1M total records)
    --iiif       Retrieve only records with IIIF metadata
    --output     Output file name (default: harvest.json or harvest.json.zst if compressed)
    --rows       Number of rows to fetch per request (default: 1000)
    --compress   Enable zstd compression for output (also inferred from output file name)
    --wait       Additional wait time between requests in seconds (default: 0)
    --sort       Sorting option for the query results (score, title or default: identifier)
    --debug      Debug mode to print additional information, specify more to get more
    --version    Show the version number and exit

Examples:
    # Process all existing series in batches
    python prov-api-harvest.py --series-batch --compress

    # Process series 1-100 in batches for testing
    python prov-api-harvest.py --series-batch --series-min 1 --series-max 100 --output test.json

    # Use a higher threshold for larger batches (300k results before splitting)
    python prov-api-harvest.py --series-batch --max-per-batch 300000 --compress

    # Use smaller series batches to avoid very long URLs (100 series max per batch)
    python prov-api-harvest.py --series-batch --max-series-per-batch 100 --compress

    # Always include relatedEntity records regardless of harvest size
    python prov-api-harvest.py --series-batch --include-related-entities --compress

The script uses rate limiting, error handling, and optional zstd compression.
"""

import json
import sys
import time
import argparse
import os
from urllib.parse import urlencode

import requests
import zstandard as zstd

VERSION = "0.8.5"

BASE_URL = "https://api.prov.vic.gov.au/search/query"
PARAMS = {
    "start": "0",
    "sort": "identifier.PROV_ACM.id asc",
    "wt": "json",
    "q": "*:*"
}

MAX_CONSECUTIVE_FAILURES = 6
BASE_WAIT_TIME = 63  # seconds


class TooManyFailedRequestsError(Exception):
    """
    Custom exception raised when the maximum number of consecutive
    failed requests is reached.
    """


def fetch_data(url, debug_level=0):
    """
    Fetch data from the given URL with error handling and retries.

    Args:
        url (str): The URL to fetch data from.
        debug_level (int): Debug level (0: no debug, 1: basic debug, 2: verbose debug with headers).

    Returns:
        tuple: A tuple containing the JSON response, headers, and content length.

    Raises:
        TooManyFailedRequestsError: If the maximum number of retries is exceeded.
    """
    consecutive_failures = 0
    while consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        try:
            if debug_level >= 1:
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
            if debug_level >= 1:
                print("Retrying request...", file=sys.stderr)
    raise TooManyFailedRequestsError(
        f"Failed to fetch data after {MAX_CONSECUTIVE_FAILURES} consecutive attempts. Exiting.")


def check_rate_limit(headers, wait_time):
    """
    Check the rate limit from the response headers and sleep if necessary.

    Args:
        headers (dict): The response headers containing rate limit information.
        wait_time (int): The wait time between requests in seconds.
    """
    remaining = int(headers.get('x-ratelimit-remaining-minute', 20))
    time.sleep(wait_time)
    if remaining < 20:
        print(
            f"Rate limit approaching. Remaining: {remaining}. Sleeping for 2 seconds...",
            file=sys.stderr)
        time.sleep(2)


class FileManager:
    """
    Simple file management using .incomplete extension during processing.
    """

    @classmethod
    def from_args(cls, args):
        """
        Create FileManager from command line arguments, processing output file and compression.

        Args:
            args: Command line arguments

        Returns:
            FileManager: Configured file manager instance
        """
        if args.output:
            output_file = args.output
            # Infer compression from file extension if not explicitly set
            if not args.compress and (output_file.endswith(
                    '.zst') or output_file.endswith('.zstd')):
                args.compress = True
                print(
                    f"  Inferred compression from file extension: {output_file}",
                    file=sys.stderr)
        else:
            # Auto-name output file based on series number if --series is provided
            if hasattr(args, 'series') and args.series and len(args.series) == 1:
                series_number = args.series[0]
                output_file = f'{series_number}.json'
            else:
                output_file = 'harvest.json'

        # Add .zst extension if compress is True and extension is not already
        # present
        if args.compress and not (output_file.endswith(
                '.zst') or output_file.endswith('.zstd')):
            output_file += '.zst'

        # Update args with final output file
        args.output = output_file

        return cls(output_file, args.compress)

    def __init__(self, output_file, compress=False):
        """
        Initialise file manager.

        Args:
            output_file (str): Path to the final output file
            compress (bool): Whether to use compression
        """
        self.output_file = output_file
        self.incomplete_file = f"{output_file}.incomplete"
        self.compress = compress
        self.file_opener = zstd.open if compress else open

    def check_existing_files(self):
        """
        Check if final output file already exists.

        Raises:
            SystemExit: If final output file already exists
        """
        if os.path.exists(self.output_file):
            print(
                f"Error: Output file '{
                    self.output_file}' already exists. Use a different filename or remove the existing file.",
                file=sys.stderr)
            sys.exit(1)

    def prepare_for_writing(self):
        """
        Prepare the incomplete file for writing.
        """
        with self.file_opener(self.incomplete_file, 'wb') as f:
            f.write(b"[")

    def open_for_writing(self):
        """
        Open the incomplete file for appending.

        Returns:
            File handle for writing
        """
        return self.file_opener(self.incomplete_file, 'ab')

    def finalise(self):
        """
        Finalise the file by renaming from .incomplete to final name.
        """
        with self.file_opener(self.incomplete_file, 'ab') as f:
            f.write(b"]")
        os.rename(self.incomplete_file, self.output_file)


def process_paginated_query(file_handle, query_params, description="", debug_level=0, wait_time=0,
                            first_record=True, overall_start_time=time.time(), overall_fetched=0):
    """
    Process a paginated query by fetching all results and writing them to the file.

    Args:
        file_handle: Open file handle to write JSON data to
        query_params (dict): Query parameters (will be modified with 'start')
        description (str): Description for logging (e.g., "Functions and Agencies")
        debug_level (int): Debug level for output
        wait_time (int): Wait time between requests
        first_record (boolean): first record for this file?
        overall_start_time (float): Overall start time for rate calculations

    Returns:
        tuple: (total_fetched, total_bytes, total_docs)
    """
    start = int(query_params.get('start'))
    total_docs = float('inf')
    total_fetched = 0
    total_bytes = 0

    while start < total_docs:
        query_params['start'] = str(start)
        url = f"{BASE_URL}?{urlencode(query_params)}"

        fetch_start_time = time.time()
        data, headers, content_size = fetch_data(url, debug_level)
        fetch_end_time = time.time()

        docs = data['response']['docs']
        total_docs = data['response']['numFound']

        # Log total docs on first iteration
        if start == 0 and total_docs > 0 and description:
            print(
                f"  Found {total_docs} {description} records",
                file=sys.stderr)

        # Write documents to file
        for doc in docs:
            if first_record:
                first_record = False
            else:
                file_handle.write(b",\n")

            json_data = json.dumps(doc, ensure_ascii=False).encode()
            file_handle.write(json_data)
            file_handle.flush()

        start += len(docs)
        total_fetched += len(docs)
        total_bytes += content_size

        # Log progress
        fetch_duration = fetch_end_time - fetch_start_time
        overall_duration = fetch_end_time - overall_start_time
        overall_rate = (overall_fetched + total_fetched) / overall_duration if overall_duration > 0 else 0
        print(f"Fetched {len(docs)} documents in {fetch_duration:.2f} seconds. "
              f"Total: {start}/{total_docs}. "
              f"Overall rate: {overall_rate:.2f} rows/second. "
              f"Downloaded: {content_size} bytes (Total: {total_bytes} bytes)", file=sys.stderr)

        # Debug output
        if debug_level >= 1:
            print("Debug information:", file=sys.stderr)
        if debug_level >= 2:
            print(
                f"x-ratelimit-remaining-minute: {
                    headers.get(
                        'x-ratelimit-remaining-minute',
                        'N/A')}",
                file=sys.stderr)
            print(
                f"x-ratelimit-remaining-hour: {
                    headers.get(
                        'x-ratelimit-remaining-hour',
                        'N/A')}",
                file=sys.stderr)
            print(
                f"x-kong-upstream-latency: {
                    headers.get(
                        'x-kong-upstream-latency',
                        'N/A')}",
                file=sys.stderr)
            print(
                f"x-kong-proxy-latency: {
                    headers.get(
                        'x-kong-proxy-latency',
                        'N/A')}",
                file=sys.stderr)

        # Rate limit handling
        if start < total_docs:
            check_rate_limit(headers, wait_time)

    return total_fetched, total_bytes, total_docs


def stream_records(args, file_manager):
    """
    Stream records from the PROV API, optionally compressing, and then writing them to the output file.

    Args:
        args: Command line arguments containing all configuration options.
        file_manager: An initialised FileManager.
    """
    debug_level = args.debug
    wait_time = args.wait

    file_manager.prepare_for_writing()

    total_fetched = 0

    with file_manager.open_for_writing() as file:
        # Set up parameters for the query
        query_params = PARAMS.copy()

        # Process the query using unified function
        fetched, bytes_downloaded, final_total = process_paginated_query(
            file, query_params, "", debug_level, wait_time,
            True
        )

        total_fetched = fetched

    file_manager.finalise()
    print(
        f"Download complete. Output saved to {file_manager.output_file}",
        file=sys.stderr)


def get_series_estimated_counts(args):
    """
    Fetch series IDs and their record counts using a facet query.

    Args:
        args (Namespace): Command line arguments.

    Returns:
        dict: Dictionary mapping series_id -> record_count.
    """
    print("Fetching series IDs and record counts using facets...", file=sys.stderr)

    # Build the facet query
    facet_params = PARAMS.copy()
    facet_params['q'] = "*:*"  # Query all records to get facet counts
    if hasattr(args, 'iiif') and args.iiif:
        # Apply IIIF filter if specified
        facet_params['q'] = "(iiif-manifest:(*))"

    # We don't need the actual documents, just facets
    facet_params['rows'] = "0"
    facet_params['facet'] = "true"
    facet_params['facet.limit'] = "-1"  # Get all facet values
    # Only return series with at least 1 record
    facet_params['facet.mincount'] = "1"

    # We need to facet on both series_id AND parents.ids because:
    # - series_id captures Series, Items, and Images that have a direct series_id field
    # - parents.ids captures Consignments that link to series via parents.ids field (e.g., "VPRS1234")
    facet_fields = ['series_id', 'parents.ids']

    # Build URL with multiple facet.field parameters using proper encoding
    from urllib.parse import urlencode
    params_list = []
    for key, value in facet_params.items():
        if key == 'facet.field':
            continue  # Skip the single facet.field we may have set
        params_list.append((key, value))

    # Add multiple facet.field parameters
    for field in facet_fields:
        params_list.append(('facet.field', field))

    debug_level = args.debug
    if debug_level >= 1:
        print(f"  Query: {facet_params['q']}", file=sys.stderr)
        print(f"  Facet fields: {facet_fields}", file=sys.stderr)
        print(f"  Facet limit: {facet_params['facet.limit']}", file=sys.stderr)

    url = f"{BASE_URL}?{urlencode(params_list)}"

    if debug_level >= 2:
        print(f"  URL: {url}", file=sys.stderr)

    try:
        data, headers, _ = fetch_data(url, debug_level)

        # Extract facet data
        facet_fields = data.get('facet_counts', {}).get('facet_fields', {})
        series_facets = facet_fields.get('series_id', [])
        parents_facets = facet_fields.get('parents.ids', [])

        if debug_level >= 2:
            print(
                f"  Raw series_id facet results length: {
                    len(series_facets)}",
                file=sys.stderr)
            print(
                f"  Raw parents.ids facet results length: {
                    len(parents_facets)}",
                file=sys.stderr)
            if len(series_facets) <= 20:  # Show all if small
                print(
                    f"  Raw series_id facet results: {series_facets}",
                    file=sys.stderr)
            else:  # Show first 10 pairs for large results
                sample = series_facets[:20]  # First 10 value/count pairs
                print(
                    f"  Raw series_id facet results (first 10): {sample}...",
                    file=sys.stderr)

            if len(parents_facets) <= 20:  # Show all if small
                print(
                    f"  Raw parents.ids facet results: {parents_facets}",
                    file=sys.stderr)
            else:  # Show first 10 pairs for large results
                sample = parents_facets[:20]  # First 10 value/count pairs
                print(
                    f"  Raw parents.ids facet results (first 10): {sample}...",
                    file=sys.stderr)

        # Parse series_id facet results (they come as [value1, count1, value2,
        # count2, ...])
        all_series_counts = {}
        for i in range(0, len(series_facets), 2):
            if i + 1 < len(series_facets):
                series_id_str = series_facets[i]
                count = series_facets[i + 1]

                # Convert to int if it's a valid series ID
                if str(series_id_str).isdigit():
                    series_id = int(series_id_str)
                    all_series_counts[series_id] = count

        # Parse parents.ids facet results (consignments)
        for i in range(0, len(parents_facets), 2):
            if i + 1 < len(parents_facets):
                parent_id_str = parents_facets[i]
                count = parents_facets[i + 1]

                # parents.ids contains values like "VPRS1234", extract the
                # numeric part
                if parent_id_str.startswith(
                        'VPRS') and parent_id_str[4:].isdigit():
                    series_id = int(parent_id_str[4:])
                    # Take the maximum count since our OR query returns the
                    # union, not sum
                    if series_id in all_series_counts:
                        all_series_counts[series_id] = max(
                            all_series_counts[series_id], count)
                    else:
                        all_series_counts[series_id] = count

        # Apply client-side filtering for series range
        min_series = args.series_min
        max_series = args.series_max

        if min_series > 1 or max_series is not None:
            series_counts = {}
            for sid, count in all_series_counts.items():
                if min_series <= sid and (
                        max_series is None or sid <= max_series):
                    series_counts[sid] = count

            if max_series is not None:
                print(
                    f"  Filtered to series {min_series}-{max_series}: {
                        len(series_counts)} of {
                        len(all_series_counts)} series",
                    file=sys.stderr)
            else:
                print(
                    f"  Filtered to series >={min_series}: {
                        len(series_counts)} of {
                        len(all_series_counts)} series",
                    file=sys.stderr)
        else:
            series_counts = all_series_counts
            print(
                f"  Found {
                    len(series_counts)} series with records",
                file=sys.stderr)

        total_records = sum(series_counts.values())
        print(
            f"  Total records across all series: {
                total_records:,}", file=sys.stderr)

        if debug_level >= 1:
            if series_counts:
                sorted_series = sorted(series_counts.keys())
                print(
                    f"  Series ID range: {sorted_series[0]} to {sorted_series[-1]}", file=sys.stderr)
                if debug_level >= 2:
                    # Show top 10 series by record count
                    top_series = sorted(
                        series_counts.items(),
                        key=lambda x: x[1],
                        reverse=True)[
                        :10]
                    print(
                        f"  Top series by record count: {top_series}",
                        file=sys.stderr)

        return series_counts

    except Exception as e:
        print(f"Error fetching series estimated counts: {e}", file=sys.stderr)
        sys.exit(1)


def create_optimal_batches_from_counts(
        series_counts, max_per_batch, max_series_per_batch):
    """
    Create optimal batches from series counts data to stay under max_per_batch and max_series_per_batch.

    Args:
        series_counts (dict): Dictionary mapping series_id -> record_count.
        max_per_batch (int): Maximum results per batch.
        max_series_per_batch (int): Maximum number of series per batch (to prevent HTTP 414 errors).

    Returns:
        list: List of batches, where each batch is a list of series IDs.
    """
    # Sort series by ID for consistent processing
    sorted_series = sorted(series_counts.items())  # [(series_id, count), ...]

    batches = []
    current_batch = []
    current_count = 0

    i = 0
    while i < len(sorted_series):
        series_id, record_count = sorted_series[i]

        # If a single series exceeds max_per_batch, it gets its own batch
        if record_count > max_per_batch:
            # Finish current batch if it has items
            if current_batch:
                batches.append(current_batch)
                current_batch = []
                current_count = 0

            # Add the large series as its own batch
            batches.append([series_id])
            i += 1
            continue

        # If adding this series would exceed max_per_batch OR
        # max_series_per_batch, finish current batch
        if current_batch and ((current_count + record_count > max_per_batch) or
                              (len(current_batch) >= max_series_per_batch)):
            batches.append(current_batch)
            current_batch = []
            current_count = 0

        # Add series to current batch
        current_batch.append(series_id)
        current_count += record_count
        i += 1

    # Add any remaining series as the final batch
    if current_batch:
        batches.append(current_batch)

    return batches


def get_batch_result_count_from_counts(series_batch, series_counts):
    """
    Get the total number of results for a batch using pre-calculated counts.

    Args:
        series_batch (list): List of series IDs.
        series_counts (dict): Dictionary mapping series_id -> record_count.

    Returns:
        int: Total number of results for this batch.
    """
    return sum(series_counts.get(sid, 0) for sid in series_batch)


def get_lowercase_parents_series(args):
    """
    Get a set of series IDs that have consignments with lowercase parents.ids values.

    Args:
        args (Namespace): Command line arguments to respect series-min and series-max filters.

    Returns:
        set: Set of series IDs (as integers) that have lowercase parents.ids.
    """
    # Query to find all consignments with lowercase "vprs" in parents.ids
    check_params = PARAMS.copy()
    check_params['q'] = 'category:(Consignment) AND parents.ids:vprs*'

    # Get series range for client-side filtering
    min_series = args.series_min
    max_series = args.series_max

    check_params['rows'] = str(args.rows)  # Use configured batch size
    check_params['fl'] = 'parents.ids'  # Only return the parents.ids field

    # Add debug output
    debug_level = args.debug
    if debug_level >= 1:
        print(
            f"  Lowercase parents.ids query: {
                check_params['q']}",
            file=sys.stderr)
        if min_series > 1 or max_series is not None:
            if max_series is not None:
                print(
                    f"  Will filter client-side to series {min_series}-{max_series}",
                    file=sys.stderr)
            else:
                print(
                    f"  Will filter client-side to series >={min_series}",
                    file=sys.stderr)

    lowercase_series = set()
    start = 0

    while True:
        check_params['start'] = str(start)
        url = f"{BASE_URL}?{urlencode(check_params)}"

        try:
            data, headers, _ = fetch_data(url, debug_level)
            docs = data['response']['docs']
            total_docs = data['response']['numFound']

            if debug_level >= 1:
                print(
                    f"  Found {total_docs} total consignments with lowercase 'vprs', fetched {
                        len(docs)} in this batch", file=sys.stderr)

            if not docs:
                break

            for doc in docs:
                parents_ids = doc.get('parents.ids', [])
                if isinstance(parents_ids, str):
                    parents_ids = [parents_ids]

                if debug_level >= 2:
                    print(
                        f"    Processing doc with parents.ids: {parents_ids}",
                        file=sys.stderr)

                for parent_id in parents_ids:
                    # Look for lowercase vprs entries
                    if isinstance(parent_id, str) and parent_id.lower(
                    ).startswith('vprs'):
                        if debug_level >= 2:
                            print(
                                f"      Found lowercase parent_id: '{parent_id}'",
                                file=sys.stderr)

                        # Extract series number from formats like "vprs 1234"
                        # or "vprs1234"
                        import re
                        match = re.search(r'vprs\s*(\d+)', parent_id.lower())
                        if match:
                            series_id = int(match.group(1))
                            # Apply client-side series range filtering
                            if min_series <= series_id and (
                                    max_series is None or series_id <= max_series):
                                lowercase_series.add(series_id)
                                if debug_level >= 2:
                                    print(
                                        f"        Extracted series_id: {series_id} (in range)",
                                        file=sys.stderr)
                            elif debug_level >= 2:
                                print(
                                    f"        Extracted series_id: {series_id} (out of range, skipped)",
                                    file=sys.stderr)

            start += len(docs)
            if start >= total_docs:
                break

        except Exception as e:
            print(f"Error getting lowercase parents.ids: {e}", file=sys.stderr)
            break

    return lowercase_series


def create_series_query(series_ids, debug_level=0, lowercase_series_set=None):
    """
    Create a query string for the given series IDs.

    Args:
        series_ids (list): List of series IDs.
        debug_level (int): Debug level for output.
        lowercase_series_set (set): Set of series IDs that have lowercase parents.ids.

    Returns:
        str: Query string for the series IDs.
    """
    series_str = " ".join(map(str, series_ids))

    # Build parent string with both uppercase and lowercase variants as needed
    parent_terms = []
    for sid in series_ids:
        # Always add the standard uppercase format
        parent_terms.append(f"VPRS{sid}")

        # Add lowercase variants if this series is known to have them
        if lowercase_series_set and sid in lowercase_series_set:
            parent_terms.extend([
                f"vprs{sid}",      # lowercase without space
                f"vprs\\ {sid}"    # lowercase with space (escaped)
            ])

    parent_str = " ".join(parent_terms)
    query = f"(series_id:({series_str}) OR (category:(Consignment) AND parents.ids:({parent_str})))"

    if debug_level >= 1:
        print(f"    Query: {query}", file=sys.stderr)
        if lowercase_series_set:
            lowercase_in_batch = [
                sid for sid in series_ids if sid in lowercase_series_set]
            if lowercase_in_batch:
                print(
                    f"    Added lowercase variants for series: {lowercase_in_batch}",
                    file=sys.stderr)

    return query


def process_query_arguments(args):
    """
    Process the query-related arguments (series, query, and iiif) and generate the appropriate query string.

    Args:
        args (Namespace): The parsed command-line arguments.

    Returns:
        str: The query string for the API request.
    """
    debug_level = args.debug
    query_parts = []

    if args.series:
        # Use the same logic as create_series_query
        series_query = create_series_query(args.series, debug_level)
        query_parts.append(series_query)
    elif args.query:
        query_parts.append(f"({args.query})")

    if args.iiif:
        query_parts.append("(iiif-manifest:(*))")

    if not query_parts:
        final_query = "*:*"
    else:
        final_query = " AND ".join(query_parts)

    if debug_level >= 1:
        print(f"Final query: {final_query}", file=sys.stderr)

    return final_query


def stream_records_in_series_batches(args, file_manager):
    """
    Stream records from the PROV API in batches based on series, optionally compressing, and then writing
    them to the output file.

    This overcomes issues with slow responses from the API when the start (offset) param in the query is large,
    things start to slow around 200,000 and get really slow over 3,000,000.

    Args:
        args (Namespace): The parsed command-line arguments.
        file_manager: An initialised FileManager.
    """

    # Get series that have lowercase parents.ids for consignments
    print("Checking for series with lowercase parents.ids...", file=sys.stderr)
    lowercase_series_set = get_lowercase_parents_series(args)
    if lowercase_series_set:
        print(
            f"Found {
                len(lowercase_series_set)} series with lowercase parents.ids: {
                sorted(
                    list(lowercase_series_set))}",
            file=sys.stderr)
    else:
        print("No series with lowercase parents.ids found", file=sys.stderr)

    series_counts = get_series_estimated_counts(args)

    if not series_counts:
        print("No series with records found. Exiting.", file=sys.stderr)
        sys.exit(1)

    all_batches = create_optimal_batches_from_counts(
        series_counts, args.max_per_batch, args.max_series_per_batch)

    print(
        f"Created {
            len(all_batches)} optimal batches from {
            len(series_counts)} series",
        file=sys.stderr)

    file_manager.prepare_for_writing()

    overall_start_time = time.time()
    total_fetched = 0
    total_bytes = 0

    with file_manager.open_for_writing() as file:
        # First, query for Functions and Agencies
        print("Fetching Functions and Agencies...", file=sys.stderr)

        category_params = PARAMS.copy()
        category_params['q'] = "category:(Function OR Agency)"

        debug_level = args.debug
        if debug_level >= 1:
            print(
                f"  Query: {
                    category_params['q']}",
                file=sys.stderr)

        fetched, bytes_downloaded, final_total = process_paginated_query(
            file, category_params, "Function/Agency", debug_level, args.wait,
            True, overall_start_time
        )

        total_fetched += fetched
        total_bytes += bytes_downloaded
        print(
            f"Completed Functions and Agencies. Total documents so far: {total_fetched}",
            file=sys.stderr)

        # Process the pre-calculated optimal batches
        total_batches = len(all_batches)

        for batch_idx in range(0, total_batches):
            batch_number = batch_idx + 1
            series_batch = all_batches[batch_idx]

            if not series_batch:
                continue

            # Calculate the result count for this batch
            batch_result_count = get_batch_result_count_from_counts(
                series_batch, series_counts)

            print(
                f"Processing batch {batch_number}/{total_batches}: series {series_batch[0]}-{series_batch[-1]} ({len(series_batch)} series, {batch_result_count:,} records)", file=sys.stderr)

            # Create query for this batch
            debug_level = args.debug
            query = create_series_query(
                series_batch, debug_level, lowercase_series_set)
            if args.iiif:
                query += " AND (iiif-manifest:(*))"

            # Set up parameters for this batch
            batch_params = PARAMS.copy()
            batch_params['q'] = query

            fetched, bytes_downloaded, final_total = process_paginated_query(
                file, batch_params, "batch", debug_level, args.wait,
                False, overall_start_time, total_fetched
            )

            total_fetched += fetched
            total_bytes += bytes_downloaded

            # Log discrepancy if significant
            actual_count = final_total
            estimated_count = batch_result_count
            if abs(actual_count - estimated_count) > max(10,
                                                         estimated_count * 0.05):  # 5% or 10 records threshold
                print(
                    f"  Note: Count discrepancy for batch {batch_number}: estimated {
                        estimated_count:,}, actual {
                        actual_count:,} ({
                        actual_count - estimated_count:+,})", file=sys.stderr)

            overall_duration = time.time() - overall_start_time
            overall_rate = total_fetched / overall_duration if overall_duration > 0 else 0
            remaining_batches = total_batches - (batch_idx + 1)
            print(f"Completed batch {batch_number}/{total_batches}. "
                  f"Total documents: {total_fetched}. "
                  f"Overall rate: {overall_rate:.2f} rows/second. "
                  f"Batches remaining: {remaining_batches}", file=sys.stderr)

        # Only fetch related entities if:
        # 1. Flag is explicitly specified, OR
        # 2. We're doing a full harvest (no series range limits)
        include_related = args.include_related_entities
        full_harvest = args.series_min == 1 and args.series_max is None
        if include_related or full_harvest:
            reason = "flag specified" if include_related else "full harvest"
            print(
                f"Fetching relatedEntity records ({reason})...",
                file=sys.stderr)

            related_params = PARAMS.copy()
            related_params['q'] = "category:(relatedEntity)"
            # Double the rows for relatedEntities since they're typically
            # lighter records
            related_params['rows'] = str(args.rows * 2)

            debug_level = args.debug
            if debug_level >= 1:
                print(
                    f"  Querying relatedEntity records (rows={
                        related_params['rows']})...",
                    file=sys.stderr)
                print(f"    Query: {related_params['q']}", file=sys.stderr)

            fetched, bytes_downloaded, final_total = process_paginated_query(
                file, related_params, "relatedEntity", debug_level, args.wait,
                False, overall_start_time, total_fetched
            )

            total_fetched += fetched
            total_bytes += bytes_downloaded

            print(
                f"Completed relatedEntities. Final total: {total_fetched} documents",
                file=sys.stderr)
        else:
            print(
                f"Skipping relatedEntity records (series range specified, use --include-related-entities to force)", file=sys.stderr)

    file_manager.finalise()
    print(
        f"Batch processing complete. Output saved to {
            file_manager.output_file}",
        file=sys.stderr)
    print(f"Total documents processed: {total_fetched}", file=sys.stderr)


def main():
    """
    Main function to parse command-line arguments and initiate the data harvesting process.
    """
    parser = argparse.ArgumentParser(
        description='Harvest data from PROV API with streaming output and optional zstd compression.')
    parser.add_argument(
        '--query',
        type=str,
        help='Custom query to replace the default q parameter, overridden by --series')
    parser.add_argument(
        '--series',
        type=int,
        nargs='+',
        help='One or more positive integers representing series IDs to query, overrides --query')
    parser.add_argument(
        '--series-batch',
        action='store_true',
        help='Enable batch processing of series (batch size determined by --max-per-batch and --max-series-per-batch), overrides --query and --series')
    parser.add_argument(
        '--series-min',
        type=int,
        default=1,
        metavar='MIN_ID',
        help='Minimum series ID to include (default: 1, used with --series-batch)')
    parser.add_argument(
        '--series-max',
        type=int,
        metavar='MAX_ID',
        help='Maximum series ID to include (used with --series-batch)')
    parser.add_argument(
        '--max-per-batch',
        type=int,
        default=200000,
        metavar='MAX_COUNT',
        help='Maximum results per batch before splitting (default: 200000, used with --series-batch)')
    parser.add_argument(
        '--max-series-per-batch',
        type=int,
        default=200,
        metavar='MAX_SERIES',
        help='Maximum series per batch to prevent request size errors (default: 200, used with --series-batch)')
    parser.add_argument(
        '--include-related-entities',
        action='store_true',
        help='Always include relatedEntity records (otherwise only included if >1M total records harvested)')
    parser.add_argument(
        '--iiif',
        action='store_true',
        help='Retrieve only records with IIIF metadata')
    parser.add_argument(
        '--output',
        type=str,
        help='Output file name (default: harvest.json or harvest.json.zst if compressed)')
    parser.add_argument(
        '--rows',
        type=int,
        default=1000,
        help='Number of rows to fetch per request (default: 1000)')
    parser.add_argument(
        '--compress',
        action='store_true',
        help='Enable zstd compression for output')
    parser.add_argument(
        '--wait',
        type=int,
        default=0,
        help='Additional wait time between requests in seconds (default: 0)')
    parser.add_argument(
        '--sort',
        choices=['identifier', 'score', 'title'],
        default='identifier',
        help='Sorting option for the query results (default: identifier)')
    parser.add_argument(
        '--debug',
        action='count',
        default=0,
        help='Enable debug mode. Use more for more verbose information.')
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {VERSION}')
    args = parser.parse_args()

    PARAMS['rows'] = str(args.rows)

    # Validate mutually exclusive options
    if sum(bool(x) for x in [args.series, args.series_batch, args.query]) > 1:
        parser.error(
            "Only one of --series, --series-batch, or --query can be specified")

    # Validate series-min/max are only used with series-batch
    if (args.series_min != 1 or args.series_max is not None) and not args.series_batch:
        parser.error(
            "--series-min and --series-max can only be used with --series-batch")

    # Validate series range
    if args.series_batch and args.series_max is not None and args.series_min > args.series_max:
        parser.error("--series-min cannot be greater than --series-max")

    file_manager = FileManager.from_args(args)
    file_manager.check_existing_files()

    try:
        if args.series_batch:
            stream_records_in_series_batches(args, file_manager)
            return

        PARAMS['q'] = process_query_arguments(args)

        if args.sort == 'identifier':
            # Keep the default sort parameter
            pass
        elif args.sort == 'title':
            PARAMS['sort'] = 'Series_title asc'
        elif args.sort == 'score':
            # Remove the sort parameter
            PARAMS.pop('sort', None)

        stream_records(args, file_manager)

    except TooManyFailedRequestsError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print(
            f"\nInterrupted. File saved as {
                file_manager.output_file}.incomplete",
            file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
