# /// script
# requires-python = ">=3.13"
# dependencies = []
# ///

"""
PROV Post-Processor: Extract Agency-Series Subsets

This script post-processes prov-series.json and prov-agencies.json to extract
a subset of series and agencies related to one or more input agency IDs.

Algorithm:
    1. Parse the input agency IDs (e.g., VA2876, VA 2876, or 2876)
    2. Find all series where any input agency appears as a creating or
       responsible agent
    3. From those series, collect ALL related agency IDs (creating and
       responsible)
    4. Extract the full agency records for all collected agency IDs
    5. Write filtered series and agencies to output JSON files

Usage:
    python prov-postprocess.py VA2876
    python prov-postprocess.py VA2876 VA2877 VA421
    python prov-postprocess.py --agencies-file prov-agencies.json --series-file prov-series.json VA2876

Options:
    --agencies-file  Path to prov-agencies.json (default: prov-agencies.json)
    --series-file    Path to prov-series.json (default: prov-series.json)
    --output-prefix  Prefix for output files (default: derived from agency IDs)
    --debug          Enable debug output
"""

import json
import sys
import argparse
import re


def parse_agency_id(raw_id):
    """
    Parse an agency ID from various formats into a numeric ID.

    Accepts: VA2876, VA 2876, va2876, 2876
    Returns: int agency ID, or None if unparseable.
    """
    raw_id = raw_id.strip()

    # Try plain integer
    if raw_id.isdigit():
        return int(raw_id)

    # Try VA prefix with optional space
    match = re.match(r'^[Vv][Aa]\s*(\d+)$', raw_id)
    if match:
        return int(match.group(1))

    return None


def extract_agency_ids_from_series(series_record):
    """
    Extract all agency IDs referenced by a series record.

    Looks at creating_agents.creating_agency_id, resp_agency_id,
    and responsible_agents.resp_agency_id fields.

    Returns: set of int agency IDs
    """
    ids = set()

    # creating_agents.creating_agency_id is a list of ints
    for val in (series_record.get('creating_agents.creating_agency_id') or []):
        ids.add(int(val))

    # resp_agency_id is a list of strings
    for val in (series_record.get('resp_agency_id') or []):
        if str(val).isdigit():
            ids.add(int(val))

    # responsible_agents.resp_agency_id is a list of ints
    for val in (series_record.get('responsible_agents.resp_agency_id') or []):
        ids.add(int(val))

    return ids


def agency_id_from_citation(citation):
    """
    Extract the numeric agency ID from a citation like "VA 2876".

    Returns: int agency ID, or None if unparseable.
    """
    match = re.match(r'^VA\s+(\d+)$', citation)
    if match:
        return int(match.group(1))
    return None


def main():
    parser = argparse.ArgumentParser(
        description='Extract agency-series subsets from PROV harvest data.')
    parser.add_argument(
        'agency_ids',
        nargs='+',
        metavar='AGENCY_ID',
        help='One or more agency IDs (e.g., VA2876, VA 2876, or 2876)')
    parser.add_argument(
        '--agencies-file',
        default='prov-agencies.json',
        help='Path to prov-agencies.json (default: prov-agencies.json)')
    parser.add_argument(
        '--series-file',
        default='prov-series.json',
        help='Path to prov-series.json (default: prov-series.json)')
    parser.add_argument(
        '--output-prefix',
        default=None,
        help='Prefix for output files (default: derived from agency IDs)')
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug output')
    args = parser.parse_args()

    # Parse input agency IDs
    input_agency_ids = set()
    for raw_id in args.agency_ids:
        parsed = parse_agency_id(raw_id)
        if parsed is None:
            print(f"Error: Could not parse agency ID '{raw_id}'", file=sys.stderr)
            print("  Expected formats: VA2876, VA 2876, or 2876", file=sys.stderr)
            sys.exit(1)
        input_agency_ids.add(parsed)

    print(f"Input agency IDs: {sorted(input_agency_ids)}", file=sys.stderr)

    # Determine output prefix
    if args.output_prefix:
        prefix = args.output_prefix
    else:
        sorted_ids = sorted(input_agency_ids)
        prefix = '_'.join(f'VA{aid}' for aid in sorted_ids)

    # Load series data
    print(f"Loading series from {args.series_file}...", file=sys.stderr)
    with open(args.series_file) as f:
        all_series = json.load(f)
    print(f"  Loaded {len(all_series)} series records", file=sys.stderr)

    # Load agencies data
    print(f"Loading agencies from {args.agencies_file}...", file=sys.stderr)
    with open(args.agencies_file) as f:
        all_agencies = json.load(f)
    print(f"  Loaded {len(all_agencies)} agency records", file=sys.stderr)

    # Step 1: Find all series related to the input agencies
    matched_series = []
    for series in all_series:
        series_agency_ids = extract_agency_ids_from_series(series)
        if series_agency_ids & input_agency_ids:
            matched_series.append(series)

    print(f"Found {len(matched_series)} series related to input agencies", file=sys.stderr)

    if args.debug:
        for s in matched_series[:10]:
            print(f"  {s['citation']}: {s['title']}", file=sys.stderr)
        if len(matched_series) > 10:
            print(f"  ... and {len(matched_series) - 10} more", file=sys.stderr)

    # Step 2: Collect all agency IDs from matched series
    all_related_agency_ids = set()
    for series in matched_series:
        all_related_agency_ids |= extract_agency_ids_from_series(series)

    print(f"Found {len(all_related_agency_ids)} unique agency IDs across matched series", file=sys.stderr)

    if args.debug:
        print(f"  Agency IDs: {sorted(all_related_agency_ids)}", file=sys.stderr)

    # Step 3: Extract agency records for all related agency IDs
    # Build lookup from numeric ID to agency record
    matched_agencies = []
    matched_agency_ids = set()
    for agency in all_agencies:
        agency_num = agency_id_from_citation(agency.get('citation', ''))
        if agency_num is not None and agency_num in all_related_agency_ids:
            matched_agencies.append(agency)
            matched_agency_ids.add(agency_num)

    print(f"Found {len(matched_agencies)} agency records", file=sys.stderr)

    # Check for agency IDs referenced in series but not found in agencies file
    missing = all_related_agency_ids - matched_agency_ids
    if missing:
        print(f"  Warning: {len(missing)} agency IDs referenced in series but not found in agencies file:",
              file=sys.stderr)
        for mid in sorted(missing):
            print(f"    VA {mid}", file=sys.stderr)

    # Write output files
    series_output = f"{prefix}-series.json"
    agencies_output = f"{prefix}-agencies.json"

    print(f"Writing {len(matched_series)} series to {series_output}...", file=sys.stderr)
    with open(series_output, 'w') as f:
        json.dump(matched_series, f, ensure_ascii=False, indent=2)

    print(f"Writing {len(matched_agencies)} agencies to {agencies_output}...", file=sys.stderr)
    with open(agencies_output, 'w') as f:
        json.dump(matched_agencies, f, ensure_ascii=False, indent=2)

    # Summary
    print(f"\nSummary:", file=sys.stderr)
    print(f"  Input agencies:    {len(input_agency_ids)}", file=sys.stderr)
    print(f"  Matched series:    {len(matched_series)}", file=sys.stderr)
    print(f"  Related agencies:  {len(matched_agencies)}", file=sys.stderr)
    print(f"  Output files:", file=sys.stderr)
    print(f"    {series_output}", file=sys.stderr)
    print(f"    {agencies_output}", file=sys.stderr)


if __name__ == "__main__":
    main()
