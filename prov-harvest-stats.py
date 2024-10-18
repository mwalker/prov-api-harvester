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
JSON Stream Processor for PROV Functions

Dependencies:
    - Python 3.6+
    - ijson (install with: pip install ijson)
    - zstandard (install with: pip install zstandard)

Usage:
    1. Process from stdin:
       cat your_large_file.json | python json_stream_processor.py

    2. Process from file (automatically detects Zstandard compression):
       python json_stream_processor.py --input your_large_file.json
       python json_stream_processor.py --input your_compressed_file.json.zst
"""

import json
import sys
import argparse
import re
from collections import Counter, defaultdict
import os

from datetime import datetime, timezone
import ijson
import zstandard as zstd


def is_zstandard_compressed(file_path):
    try:
        with open(file_path, 'rb') as file:
            return file.read(4) == b'\x28\xB5\x2F\xFD'
    except IOError:
        return False


def get_input_stream(input_source):
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
                        str(e)}")
        else:
            try:
                return open(input_source, 'rb')
            except Exception as e:
                raise RuntimeError(
                    f"Error opening file '{input_source}': {
                        str(e)}")
    else:  # Assume it's already a file-like object (e.g., sys.stdin.buffer)
        return input_source


def process_json_stream(input_source):
    stats = {
        'category': Counter(),
        'series': defaultdict(
            lambda: {
                'agencies': set(),
                'consignments': 0,
                'iiif_manifests': 0,
                'images': 0,
                'items': 0,
                'related_entities': 0,
                'title': '',
                'units': 0,
                'years': Counter()}),
        'year': Counter(),
        'iiif_manifests': 0,
        'objects': 0,
        'related_entities': 0,
        'units': 0}

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
    match = re.match(r'VPRS(\d+)/', entity_id)
    return match.group(1) if match else None


def extract_series_id_from_identifier(identifier):
    match = re.match(r'VPRS (\d+)/P', identifier)
    return match.group(1) if match else None


def process_object(obj, stats):
    category = obj.get('category', 'Unknown')

    stats['category'][category] += 1

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

    # Collect agency IDs
    agencies_ids = obj.get('agencies.ids', [])
    if agencies_ids:
        series_stats['agencies'].update(agencies_ids)

    # Count items with 'iiif-manifest' key
    if 'iiif-manifest' in obj:
        series_stats['iiif_manifests'] += 1
        stats['iiif_manifests'] += 1


def sort_dict(d):
    return {
        k: sort_dict(v) if isinstance(
            v, dict) else v for k, v in sorted(
            d.items())}


def print_stats_json(stats):
    overall = {
        "categories": dict(sorted(stats['category'].items())),
        "iiif_manifests": stats['iiif_manifests'],
        "objects": stats['objects'],
        "units": stats['units'],
        "years": dict(sorted(stats['year'].items()))
    }

    # Filter out None and "Unknown" series, then sort numerically
    def series_sort_key(item):
        series_id = item[0]
        try:
            return int(series_id)
        except ValueError:
            return float('inf')  # Place non-numeric strings at the end

    valid_series = [
        (sid, sstats) for sid, sstats in stats['series'].items() if sid not in (
            None, "Unknown")]
    sorted_series = sorted(valid_series, key=series_sort_key)

    # Construct the series data maintaining the sorted order
    series_data = []
    for series_id, series_stats in sorted_series:
        series_item = {
            "id": series_id,
            "title": series_stats['title'],
            # Add sorted list of agency IDs
            "agencies": sorted(series_stats['agencies']),
            "consignments": series_stats['consignments'],
            "iiif_manifests": series_stats['iiif_manifests'],
            "images": series_stats['images'],
            "items": series_stats['items'],
            "related_entities": series_stats['related_entities'],
            "units": series_stats['units'],
            "years": dict(sorted(series_stats['years'].items()))
        }
        series_data.append(series_item)

    # Construct the final output
    output = {
        "overall": overall,
        "series": series_data
    }

    # Use a custom JSON encoder to sort dictionary keys
    class SortedDict(dict):
        def __iter__(self):
            return iter(sorted(super().__iter__()))

    def dict_to_sorted_dict(d):
        if isinstance(d, dict):
            return SortedDict((k, dict_to_sorted_dict(v))
                              for k, v in d.items())
        elif isinstance(d, list):
            return [dict_to_sorted_dict(v) for v in d]
        else:
            return d

    sorted_output = dict_to_sorted_dict(output)

    print(json.dumps(sorted_output, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="Process JSON data and generate statistics in JSON format.")
    parser.add_argument(
        '--input',
        type=str,
        help='Input JSON file path. If not provided, reads from stdin. Automatically detects Zstandard compression.')
    args = parser.parse_args()

    if args.input:
        process_json_stream(args.input)
    else:
        process_json_stream(sys.stdin.buffer)


if __name__ == "__main__":
    main()
