#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "ijson",
#     "zstandard",
# ]
# ///
# -*- coding: utf-8 -*-

"""
Generate stats for data harvested from the PROV API

Dependencies:
    - Python 3.6+
    - ijson (install with: pip install ijson)
    - zstandard (install with: pip install zstandard)

Usage:
    1. Process from stdin:
       cat your_large_file.json | python prov-harvest-stats.py

    2. Process from file (automatically detects Zstandard compression):
       python prov-harvest-stats.py --input your_large_file.json
       python prov-harvest-stats.py --input your_compressed_file.json.zst
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import re
import sys
import os

import ijson
import zstandard as zstd

VERSION = "0.1.0"


def is_zstandard_compressed(file_path):
    """Check if the file is Zstandard compressed."""
    try:
        with open(file_path, 'rb') as file:
            return file.read(4) == b'\x28\xB5\x2F\xFD'
    except IOError:
        return False


def get_input_stream(input_source):
    """Get the input stream for processing."""
    if isinstance(input_source, str):  # File path
        if not os.path.exists(input_source):
            raise FileNotFoundError(
                f"The file '{input_source}' does not exist.")
        if not os.access(input_source, os.R_OK):
            raise PermissionError(
                f"You do not have permission to read the file '{input_source}'.")

        if is_zstandard_compressed(input_source):
            try:
                dctx = zstd.ZstdDecompressor()
                return dctx.stream_reader(open(input_source, 'rb'))
            except Exception as e:
                raise RuntimeError(
                    f"Error opening compressed file '{input_source}': {
                        str(e)}") from e
        else:
            try:
                return open(input_source, 'rb')
            except Exception as e:
                raise RuntimeError(
                    f"Error opening file '{input_source}': {
                        str(e)}") from e
    else:  # Assume it's already a file-like object (e.g., sys.stdin.buffer)
        return input_source


def process_json_stream(input_source):
    """Process the JSON stream and generate statistics."""
    stats = {
        'category': Counter(),
        'series': defaultdict(lambda: {
            'agencies': set(),
            'consignments': 0,
            'iiif_manifests': 0,
            'images': 0,
            'items': 0,
            'related_entities': 0,
            'title': '',
            'units': 0,
            'years': Counter()
        }),
        'agencies': defaultdict(lambda: {
            'title': '',
            'consignments': 0,
            'iiif_manifests': 0,
            'images': 0,
            'items': 0,
            'series': set(),
            'units': 0,
            'years': Counter()
        }),
        'year': Counter(),
        'iiif_manifests': 0,
        'objects': 0,
        'related_entities': 0,
        'units': 0
    }

    try:
        input_stream = get_input_stream(input_source)
        parser = ijson.parse(input_stream)

        in_array = ''
        current_array = []

        for prefix, event, value in parser:
            if prefix == 'item' and event == 'start_map':
                obj = {}
            elif prefix.startswith('item.') and event == 'start_array':
                in_array = prefix.removeprefix('item.')
                current_array = []
            elif prefix.startswith('item.') and event == 'end_array':
                obj[in_array] = current_array
                in_array = ''
            elif in_array != '':
                current_array.append(value)
            elif prefix.startswith('item.'):
                key = prefix.split('.', 1)[1]
                obj[key] = value
            elif prefix == 'item' and event == 'end_map':
                process_object(obj, stats)
                stats['objects'] += 1
                if stats['objects'] % 10000 == 0:
                    sys.stderr.write(
                        f"\rProcessed {
                            stats['objects']} objects...")
                    sys.stderr.flush()

        sys.stderr.write(
            f"\rProcessed {
                stats['objects']} objects. Generating final stats...\n")
        sys.stderr.flush()
        print_stats_json(stats)

    except FileNotFoundError as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        print(
            "Please check if the file exists and the path is correct.",
            file=sys.stderr)
    except PermissionError as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        print(
            "Please check if you have the necessary permissions to read the file.",
            file=sys.stderr)
    except RuntimeError as e:
        print(f"Error: {str(e)}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}", file=sys.stderr)
    finally:
        if isinstance(input_source, str) and 'input_stream' in locals():
            input_stream.close()


def extract_series_id_from_entity_id(entity_id):
    """Extract series ID from entity ID."""
    match = re.match(r'VPRS(\d+)/', entity_id)
    return match.group(1) if match else None


def extract_series_id_from_identifier(identifier):
    """Extract series ID from identifier."""
    match = re.match(r'VPRS (\d+)/P', identifier)
    return match.group(1) if match else None


def process_object(obj, stats):
    """Process a single object and update statistics."""
    category = obj.get('category', 'Unknown')

    stats['category'][category] += 1

    if category == 'Agency':
        agency_id = obj.get('identifier.PROV_ACM.id', '').replace(' ', '')
        if agency_id != '':
            agency_stats = stats['agencies'][agency_id]
            agency_stats['title'] = obj.get('title', '')
        return

    # Extract series_id
    if category == 'Consignment':
        identifier = obj.get('identifier.PROV_ACM.id')
        series_id = extract_series_id_from_identifier(
            identifier) if identifier else 'Unknown'
    elif category == 'relatedEntity':
        entity_id = obj.get('_id')
        series_id = extract_series_id_from_entity_id(entity_id)
    else:
        series_id = obj.get('series_id', 'Unknown')

    # Process year stats for all objects
    if 'timestamp' in obj:
        try:
            timestamp = int(obj['timestamp'])
            year = datetime.fromtimestamp(timestamp, tz=timezone.utc).year
            year_str = str(year)
            stats['year'][year_str] += 1

            # Only update series year stats if category is not 'Series'
            if category != 'Series':
                stats['series'][series_id]['years'][year_str] += 1
        except (ValueError, OverflowError):
            stats['year']['Invalid'] += 1

    series_stats = stats['series'][series_id]

    # Stats for each series
    if category == 'Consignment':
        series_stats['consignments'] += 1
    elif category == 'Image':
        series_stats['images'] += 1
    elif category == 'Item':
        series_stats['items'] += 1
        # Check if the item is a unit
        if obj.get('barcode') == obj.get('box_barcode'):
            series_stats['units'] += 1
            stats['units'] += 1
    elif category == 'relatedEntity':
        series_stats['related_entities'] += 1
    elif category == 'Series':
        series_stats['title'] = obj.get('title', '')

    # Count items with 'iiif-manifest' key
    if 'iiif-manifest' in obj:
        series_stats['iiif_manifests'] += 1
        stats['iiif_manifests'] += 1

    # Process agency information from items
    # (we may have a harvest with no Agency records)
    agencies_ids = obj.get('agencies.ids', [])
    if agencies_ids:
        series_stats['agencies'].update(agencies_ids)
    agencies_titles = obj.get('agencies.titles', [])
    for i, agency_id in enumerate(agencies_ids):
        agency_stats = stats['agencies'][agency_id]
        if i < len(agencies_titles):
            agency_stats['title'] = agencies_titles[i]
        agency_stats['series'].add(series_id)


def sort_dict(d):
    """Sort a dictionary recursively."""
    return {
        k: sort_dict(v) if isinstance(
            v, dict) else v for k, v in sorted(
            d.items())}


def print_stats_json(stats):
    """Print the final statistics in JSON format."""
    overall = {
        "categories": dict(sorted(stats['category'].items())),
        "iiif_manifests": stats['iiif_manifests'],
        "objects": stats['objects'],
        "units": stats['units'],
        "years": dict(sorted(stats['year'].items()))
    }

    def agency_sort_key(agency_id):
        match = re.search(r'VA(\d+)', agency_id)
        return int(match.group(1)) if match else float('inf')

    def series_sort_key(item):
        series_id = item[0]
        try:
            return int(series_id)
        except ValueError:
            return float('inf')

    valid_series = [
        (sid, sstats) for sid, sstats in stats['series'].items() if sid not in (
            None, "Unknown")]
    sorted_series = sorted(valid_series, key=series_sort_key)

    series_data = []
    for series_id, series_stats in sorted_series:
        series_item = {
            "id": series_id,
            "title": series_stats['title'],
            "agencies": sorted(series_stats['agencies'], key=agency_sort_key),
            "consignments": series_stats['consignments'],
            "iiif_manifests": series_stats['iiif_manifests'],
            "images": series_stats['images'],
            "items": series_stats['items'],
            "related_entities": series_stats['related_entities'],
            "units": series_stats['units'],
            "years": dict(sorted(series_stats['years'].items()))
        }
        series_data.append(series_item)

    sorted_agencies = sorted(
        stats['agencies'].items(),
        key=lambda x: agency_sort_key(
            x[0]))

    agency_data = []
    for agency_id, agency_stats in sorted_agencies:
        for series_id in agency_stats['series']:
            series_stats = stats['series'].get(series_id)
            if series_stats:
                agency_stats['consignments'] += series_stats['consignments']
                agency_stats['iiif_manifests'] += series_stats['iiif_manifests']
                agency_stats['images'] += series_stats['images']
                agency_stats['items'] += series_stats['items']
                agency_stats['units'] += series_stats['units']
                agency_stats['years'].update(series_stats['years'])

        agency_item = {
            "id": agency_id,
            "title": agency_stats['title'],
            'consignments': agency_stats['consignments'],
            'iiif_manifests': agency_stats['iiif_manifests'],
            'images': agency_stats['images'],
            "items": agency_stats['items'],
            "series": sorted(agency_stats['series'], key=int),
            'units': agency_stats['units'],
            'years': dict(sorted(agency_stats['years'].items()))
        }
        agency_data.append(agency_item)

    output = {
        "overall": overall,
        "agencies": agency_data,
        "series": series_data
    }

    class SortedDict(dict):
        """A dictionary that maintains sorted keys."""

        def __iter__(self):
            return iter(sorted(super().__iter__()))

    def dict_to_sorted_dict(d):
        if isinstance(d, dict):
            return SortedDict((k, dict_to_sorted_dict(v))
                              for k, v in d.items())
        if isinstance(d, list):
            return [dict_to_sorted_dict(v) for v in d]
        return d

    sorted_output = dict_to_sorted_dict(output)

    print(json.dumps(sorted_output, indent=2))


def main():
    """Main function to process JSON data and generate statistics."""
    parser = argparse.ArgumentParser(
        description="Process JSON data and generate statistics in JSON format.")
    parser.add_argument(
        '--input',
        type=str,
        help='Input JSON file path. If not provided, reads from stdin. Automatically detects Zstandard compression.')
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {VERSION}')
    args = parser.parse_args()

    if args.input:
        process_json_stream(args.input)
    else:
        process_json_stream(sys.stdin.buffer)


if __name__ == "__main__":
    main()
