# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "csv-diff",
# ]
# ///

"""
Generate improved commit messages for PROV API git-scraping workflow.

This script creates commit messages that focus on new additions with human-readable
summaries at the top, followed by the detailed csv-diff output.

Usage:
    uv run generate-commit-message.py [--date YYYY-MM-DD]

Arguments:
    --date YYYY-MM-DD    Date to use for file comparison (default: today)
"""

import argparse
import csv_diff
import json
from datetime import datetime
from pathlib import Path


def compare_files(old_file, new_file, key="citation"):
    """Compare two JSON files using csv_diff library."""
    try:
        with open(old_file) as fp:
            old_data = csv_diff.load_json(fp, key=key)

        with open(new_file) as fp:
            new_data = csv_diff.load_json(fp, key=key)

        result = csv_diff.compare(old_data, new_data)
        human_output = csv_diff.human_text(result, key=key)

        return result, human_output
    except Exception as e:
        return None, f"csv-diff failed for '{old_file}': {e}"


def format_function_summary(added_records):
    """Format summary for new functions."""
    if not added_records:
        return ""

    summaries = []
    for record in added_records:
        citation = record.get("citation", "Unknown")
        title = record.get("title", "Untitled")
        summaries.append(f"{citation}: {title}")

    return "\n".join(summaries)


def format_agency_summary(added_records):
    """Format summary for new agencies."""
    if not added_records:
        return ""

    summaries = []
    for record in added_records:
        citation = record.get("citation", "Unknown")
        title = record.get("title", "Untitled")
        summaries.append(f"{citation}: {title}")

    return "\n".join(summaries)


def format_series_summary(added_records):
    """Format summary for new series."""
    if not added_records:
        return ""

    summaries = []
    for record in added_records:
        citation = record.get("citation", "Unknown")
        title = record.get("title", "Untitled")

        # Extract creating agent title (first one if multiple)
        creating_agents = record.get("creating_agents.title", [])

        # Handle case where csv-diff returns JSON string instead of parsed list
        if isinstance(creating_agents, str):
            try:
                import json
                creating_agents = json.loads(creating_agents)
            except json.JSONDecodeError:
                creating_agents = []

        if creating_agents and isinstance(
                creating_agents, list) and len(creating_agents) > 0:
            # Strip whitespace/newlines
            creating_agent = creating_agents[0].strip()
        else:
            creating_agent = "Unknown Agency"

        # Extract rights_status
        rights_status = record.get("rights_status", [])
        if isinstance(rights_status, str):
            try:
                import json
                rights_status = json.loads(rights_status)
            except json.JSONDecodeError:
                rights_status = []

        # Format rights_status for display
        if isinstance(rights_status, list) and rights_status:
            status_display = ", ".join(rights_status)
        else:
            status_display = str(rights_status) if rights_status else "Unknown"

        summaries.append(
            f"{citation}: {title} ({creating_agent}) [{status_display}]")

    return "\n".join(summaries)


def format_consignment_summary(added_records):
    """Format summary for new consignments."""
    if not added_records:
        return ""

    summaries = []
    for record in added_records:
        citation = record.get("citation", "Unknown")

        # Extract parent title (first one if multiple)
        parent_titles = record.get("parents.titles", [])

        # Handle case where csv-diff returns JSON string instead of parsed list
        if isinstance(parent_titles, str):
            try:
                import json
                parent_titles = json.loads(parent_titles)
            except json.JSONDecodeError:
                parent_titles = []

        if parent_titles and isinstance(
                parent_titles, list) and len(parent_titles) > 0:
            # Strip whitespace/newlines
            parent_title = parent_titles[0].strip()
        else:
            parent_title = "Unknown Series"

        # Extract rights_status
        rights_status = record.get("rights_status", [])
        if isinstance(rights_status, str):
            try:
                import json
                rights_status = json.loads(rights_status)
            except json.JSONDecodeError:
                rights_status = []

        # Format rights_status for display
        if isinstance(rights_status, list) and rights_status:
            status_display = ", ".join(rights_status)
        else:
            status_display = str(rights_status) if rights_status else "Unknown"

        # Only add non-empty entries
        if parent_title and parent_title.strip():
            summaries.append(f"{citation}: {parent_title} [{status_display}]")

    return "\n".join(summaries)


def format_opening_summary(changed_records, data_type, target_date):
    """Format summary for records that have newly opened (rights_status changed to or includes 'Open').
    Only includes existing records that changed status, not newly added records.
    Returns a list of opening records for this data type."""
    openings = []

    # Load the current JSON file to get full record details
    if data_type == "Series":
        json_file = f"prov-series-{target_date}.json"
    elif data_type == "Consignments":
        json_file = f"prov-consignments-{target_date}.json"
    else:
        return ""

    # Load the JSON data
    try:
        with open(json_file, 'r') as f:
            json_data = json.load(f)

        # Create a lookup dictionary by citation
        records_by_citation = {
            record.get("citation"): record for record in json_data}
    except (FileNotFoundError, json.JSONDecodeError):
        return ""

    # Check changed records for rights_status changes to "Open"
    for record in changed_records:
        citation = record.get("key", "Unknown")
        changes = record.get("changes", {})

        # Look for rights_status changes
        if "rights_status" in changes:
            old_status, new_status = changes["rights_status"]

            # Check if status changed to include "Open"
            if "Open" in str(new_status) and "Open" not in str(old_status):
                # Get the full record from the JSON data
                full_record = records_by_citation.get(citation)
                if not full_record:
                    continue

                # Format the rights status change for display
                old_status_display = str(old_status).replace(
                    "'",
                    "").replace(
                    "[",
                    "").replace(
                    "]",
                    "").replace(
                    '"',
                    '')
                new_status_display = str(new_status).replace(
                    "'",
                    "").replace(
                    "[",
                    "").replace(
                    "]",
                    "").replace(
                    '"',
                    '')
                status_change = f"{old_status_display} → {new_status_display}"

                # For series, get title and creating agency
                if data_type == "Series":
                    title = full_record.get("title", "Untitled")
                    creating_agents = full_record.get(
                        "creating_agents.title", [])

                    # Handle case where csv-diff returns JSON string instead of
                    # parsed list
                    if isinstance(creating_agents, str):
                        try:
                            creating_agents = json.loads(creating_agents)
                        except json.JSONDecodeError:
                            creating_agents = []

                    if creating_agents and isinstance(
                            creating_agents, list) and len(creating_agents) > 0:
                        creating_agent = creating_agents[0].strip()
                    else:
                        creating_agent = "Unknown Agency"

                    openings.append(
                        f"{citation}: {title} ({creating_agent}) [{status_change}]")

                # For consignments, get parent title
                elif data_type == "Consignments":
                    parent_titles = full_record.get("parents.titles", [])

                    # Handle case where csv-diff returns JSON string instead of
                    # parsed list
                    if isinstance(parent_titles, str):
                        try:
                            parent_titles = json.loads(parent_titles)
                        except json.JSONDecodeError:
                            parent_titles = []

                    if parent_titles and isinstance(
                            parent_titles, list) and len(parent_titles) > 0:
                        parent_title = parent_titles[0].strip()
                    else:
                        parent_title = "Unknown Series"

                    if parent_title and parent_title.strip():
                        openings.append(
                            f"{citation}: {parent_title} [{status_change}]")

    # Note: We exclude newly added records from openings since they're already shown in "New Additions"
    # with their access status clearly marked in square brackets

    return "\n".join(openings)


def generate_commit_message(target_date=None, original_timestamp=None):
    """Generate the complete commit message."""
    if original_timestamp:
        timestamp = original_timestamp
    else:
        timestamp = datetime.utcnow().strftime("%a %b %d %H:%M:%S UTC %Y")

    # Define file pairs
    data_types = [
        ("Functions", "prov-functions.json", format_function_summary),
        ("Agencies", "prov-agencies.json", format_agency_summary),
        ("Series", "prov-series.json", format_series_summary),
        ("Consignments", "prov-consignments.json", format_consignment_summary)
    ]

    today = target_date or datetime.now().strftime("%Y-%m-%d")

    # Start building the commit message
    message_parts = [timestamp, ""]

    # Collect summaries and detailed outputs
    summaries = {}
    detailed_outputs = {}
    openings_by_type = {}

    for data_type, filename, formatter in data_types:
        old_file = filename
        # Create new filename by inserting date before .json extension
        base_name = Path(filename).stem
        new_file = f"{base_name}-{today}.json"

        # Skip if files don't exist
        if not Path(old_file).exists() or not Path(new_file).exists():
            detailed_outputs[data_type] = f"File not found: {old_file} or {new_file}"
            continue

        result, human_output = compare_files(old_file, new_file)
        detailed_outputs[data_type] = human_output

        # Generate custom summary for new additions
        if result and result['added']:
            summaries[data_type] = formatter(result['added'])

        # Generate openings summary for Series and Consignments only
        if data_type in ["Series", "Consignments"] and result:
            changed_records = result.get('changed', [])

            opening_summary = format_opening_summary(
                changed_records, data_type, today)
            if opening_summary:
                openings_by_type[data_type] = opening_summary

    # Add human-readable summary section if there are any new additions
    if any(summaries.values()):
        message_parts.append("## New Additions")
        message_parts.append("")

        for data_type, _, _ in data_types:
            if data_type in summaries and summaries[data_type]:
                message_parts.append(f"### New {data_type}")
                message_parts.append("")
                message_parts.append(summaries[data_type])
                message_parts.append("")

    # Add openings section if there are any newly opened records
    if openings_by_type:
        message_parts.append("## Openings")
        message_parts.append("")

        for data_type in ["Series", "Consignments"]:
            if data_type in openings_by_type:
                message_parts.append(f"### {data_type} Opened")
                message_parts.append("")
                message_parts.append(openings_by_type[data_type])
                message_parts.append("")

    # Add detailed csv-diff output
    message_parts.append("## Detailed Changes")
    message_parts.append("")

    for data_type, _, _ in data_types:
        if data_type in detailed_outputs:
            message_parts.append(f"### {data_type}")
            message_parts.append("")
            output = detailed_outputs[data_type].strip()
            if output:
                message_parts.append(output)
            else:
                message_parts.append("no changes")
            message_parts.append("")

    return "\n".join(message_parts).rstrip()


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            description='Generate improved commit messages for PROV API git-scraping workflow.'
        )
        parser.add_argument(
            '--date',
            type=str,
            help='Date to use for file comparison in YYYY-MM-DD format (default: today)'
        )
        parser.add_argument(
            '--timestamp',
            type=str,
            help='Original timestamp to preserve in the commit message'
        )

        args = parser.parse_args()

        commit_message = generate_commit_message(
            target_date=args.date, original_timestamp=args.timestamp)
        print(commit_message)
    except BrokenPipeError:
        # Handle pipe being closed (e.g., when piping to less/head/grep and
        # exiting early)
        import sys
        sys.exit(0)
    except KeyboardInterrupt:
        import sys
        sys.exit(1)
