#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "csv-diff",
# ]
# ///

"""
Test the improved commit message generation by simulating a specific commit.

This script uses git to temporarily checkout files from commits to test
what the commit message would have looked like with the new format.

Usage:
    uv run test-commit-message.py <commit-hash>
"""

import subprocess
import sys
import tempfile
from pathlib import Path


def run_git_command(cmd):
    """Run a git command and return the result."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Git command failed: {' '.join(cmd)}", file=sys.stderr)
        print(f"Error: {e.stderr}", file=sys.stderr)
        return None


def get_commit_date(commit_hash):
    """Get the date of a commit in YYYY-MM-DD format."""
    return run_git_command([
        "git", "show", "-s", "--format=%cd", "--date=format:%Y-%m-%d", commit_hash
    ])


def get_commit_subject(commit_hash):
    """Get the subject line (first line) of a commit message."""
    return run_git_command([
        "git", "show", "-s", "--format=%s", commit_hash
    ])


def get_parent_commit(commit_hash):
    """Get the parent commit hash."""
    return run_git_command(["git", "rev-parse", f"{commit_hash}^"])


def checkout_files_at_commit(commit_hash, target_date):
    """Checkout the 'new' files at target commit with dated names."""
    files = [
        ("prov-functions.json", f"prov-functions-{target_date}.json"),
        ("prov-agencies.json", f"prov-agencies-{target_date}.json"),
        ("prov-series.json", f"prov-series-{target_date}.json"),
        ("prov-consignments.json", f"prov-consignments-{target_date}.json")
    ]

    created_files = []
    for current_name, dated_name in files:
        try:
            content = run_git_command(
                ["git", "show", f"{commit_hash}:{current_name}"])
            if content is not None:
                with open(dated_name, 'w') as f:
                    f.write(content)
                created_files.append(dated_name)
        except Exception as e:
            print(
                f"Warning: Could not create {dated_name}: {e}",
                file=sys.stderr)

    return created_files


def test_commit_message(commit_hash):
    """Test what the commit message would have been for a specific commit."""

    parent_hash = get_parent_commit(commit_hash)
    if not parent_hash:
        print(f"Error: Could not find parent of commit {commit_hash}")
        return

    commit_date = get_commit_date(commit_hash)
    if not commit_date:
        print(f"Error: Could not get date for commit {commit_hash}")
        return

    # Get the original timestamp from the commit subject line
    original_subject = get_commit_subject(commit_hash)
    if not original_subject:
        print(f"Error: Could not get subject for commit {commit_hash}")
        return

    print(
        f"Testing commit message for {commit_hash} (date: {commit_date})",
        file=sys.stderr)
    print(f"Original subject: {original_subject}", file=sys.stderr)
    print(f"Comparing against parent {parent_hash}", file=sys.stderr)

    try:
        # Checkout the 'old' files (parent commit)
        print("Checking out old files...", file=sys.stderr)
        subprocess.run([
            "git", "checkout", parent_hash, "--",
            "prov-functions.json", "prov-agencies.json",
            "prov-series.json", "prov-consignments.json"
        ], check=True)

        # Create the 'new' files (target commit) with dated names
        print("Creating new files with dated names...", file=sys.stderr)
        created_files = checkout_files_at_commit(commit_hash, commit_date)

        if not created_files:
            print("Error: No files could be created for comparison")
            return

        # Run the script with the target date and original timestamp
        print("Running commit message generator...", file=sys.stderr)
        try:
            result = subprocess.run([
                "uv", "run", "scripts/generate-commit-message.py",
                "--date", commit_date,
                "--timestamp", original_subject
            ], capture_output=True, text=True, check=True)

            print(result.stdout)

        except subprocess.CalledProcessError as e:
            print(f"Error running script: {e}")
            if e.stderr:
                print("STDERR:", e.stderr)
        finally:
            # Clean up created files
            for file_path in created_files:
                Path(file_path).unlink(missing_ok=True)

    finally:
        # Restore current state
        print("Restoring current files...", file=sys.stderr)
        subprocess.run([
            "git", "checkout", "HEAD", "--",
            "prov-functions.json", "prov-agencies.json",
            "prov-series.json", "prov-consignments.json"
        ])


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            print("Usage: uv run test-commit-message.py <commit-hash>")
            print("\nExample:")
            print("  uv run test-commit-message.py 02d3466")
            sys.exit(1)

        commit_hash = sys.argv[1]
        test_commit_message(commit_hash)
    except BrokenPipeError:
        # Handle pipe being closed (e.g., when piping to less/head/grep and
        # exiting early)
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(1)
