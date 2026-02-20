# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "textual",
#     "tomli-w",
# ]
# ///

"""
PROV Config TUI: Configure Agency-Series Tracking

Interactive TUI for discovering agencies related to a set of seed agencies
(via shared series) and configuring which to track.

Usage:
    uv run prov-config-tui.py VA2876
    uv run prov-config-tui.py VA2876 VA2877 --config my-tracking.toml
    uv run prov-config-tui.py --config my-tracking.toml   # reload existing config

The tool:
    1. Takes seed agency IDs (or loads them from an existing config)
    2. Loads prov-series.json and prov-agencies.json
    3. Discovers all agencies that share series with the seeds
    4. Presents a TUI to toggle tracking and series inclusion per agency
    5. Saves selections to a TOML config file
"""

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path

import tomli_w
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Static


VERSION = "0.1.0"


# ── Data helpers (shared with prov-postprocess.py) ──────────────────────────


def parse_agency_id(raw_id: str) -> int | None:
    raw_id = raw_id.strip()
    if raw_id.isdigit():
        return int(raw_id)
    match = re.match(r"^[Vv][Aa]\s*(\d+)$", raw_id)
    if match:
        return int(match.group(1))
    return None


def agency_id_from_citation(citation: str) -> int | None:
    match = re.match(r"^VA\s+(\d+)$", citation)
    if match:
        return int(match.group(1))
    return None


def extract_agency_ids_from_series(series_record: dict) -> set[int]:
    ids: set[int] = set()
    for val in series_record.get("creating_agents.creating_agency_id") or []:
        ids.add(int(val))
    for val in series_record.get("resp_agency_id") or []:
        if str(val).isdigit():
            ids.add(int(val))
    for val in series_record.get("responsible_agents.resp_agency_id") or []:
        ids.add(int(val))
    return ids


# ── Discovery logic ─────────────────────────────────────────────────────────


def discover_related_agencies(
    seed_ids: set[int],
    all_series: list[dict],
    all_agencies: list[dict],
) -> tuple[list[dict], dict[int, list[str]]]:
    """
    Find all agencies related to the seed agencies via shared series.

    Returns:
        agencies: list of agency records (sorted by citation)
        shared_series_map: {agency_numeric_id: [list of series citations]}
    """
    # Find series that involve any seed agency
    matched_series = []
    for series in all_series:
        series_agency_ids = extract_agency_ids_from_series(series)
        if series_agency_ids & seed_ids:
            matched_series.append(series)

    # Collect all agency IDs from matched series + map which series each agency appears in
    shared_series_map: dict[int, list[str]] = {}
    for series in matched_series:
        agency_ids = extract_agency_ids_from_series(series)
        citation = series.get("citation", "?")
        for aid in agency_ids:
            shared_series_map.setdefault(aid, []).append(citation)

    all_related_ids = set(shared_series_map.keys())

    # Extract agency records
    matched_agencies = []
    for agency in all_agencies:
        agency_num = agency_id_from_citation(agency.get("citation", ""))
        if agency_num is not None and agency_num in all_related_ids:
            matched_agencies.append(agency)

    # Sort by citation numerically
    def sort_key(a):
        num = agency_id_from_citation(a.get("citation", ""))
        return num if num is not None else 999999

    matched_agencies.sort(key=sort_key)
    return matched_agencies, shared_series_map


# ── Config I/O ───────────────────────────────────────────────────────────────


def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def save_config(path: Path, seed_ids: list[int], tracked: list[dict]) -> None:
    doc = {
        "seed": {"agencies": [f"VA {aid}" for aid in sorted(seed_ids)]},
        "tracked": tracked,
    }
    with open(path, "wb") as f:
        tomli_w.dump(doc, f)


# ── TUI ──────────────────────────────────────────────────────────────────────


class AgencyRow:
    """In-memory state for one row in the table."""

    def __init__(
        self,
        agency_id: int,
        citation: str,
        title: str,
        date_range: str,
        shared_count: int,
        is_seed: bool,
        tracked: bool = False,
        include_series: bool = False,
    ):
        self.agency_id = agency_id
        self.citation = citation
        self.title = title
        self.date_range = date_range
        self.shared_count = shared_count
        self.is_seed = is_seed
        self.tracked = tracked
        self.include_series = include_series


class ProvConfigApp(App):
    CSS = """
    #status-bar {
        dock: top;
        height: 3;
        padding: 0 1;
        background: $primary-background;
        color: $text;
    }
    #filter-row {
        dock: top;
        height: 3;
        padding: 0 1;
    }
    #filter-input {
        width: 1fr;
    }
    #filter-label {
        width: auto;
        padding: 1 1 0 0;
    }
    #help-text {
        dock: bottom;
        height: 3;
        padding: 0 1;
        color: $text-muted;
    }
    DataTable {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("s", "save", "Save config"),
        Binding("t", "toggle_tracked", "Toggle tracked", show=True),
        Binding("i", "toggle_series", "Toggle series", show=True),
        Binding("space", "toggle_tracked_and_advance", "Toggle+next", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("g", "cursor_top", "Top", show=False),
        Binding("G", "cursor_bottom", "Bottom", show=False, key_display="shift+g"),
        Binding("/", "focus_filter", "Filter", show=True),
        Binding("escape", "clear_filter", "Clear filter", show=False),
    ]

    def __init__(
        self,
        seed_ids: set[int],
        agencies: list[dict],
        shared_series_map: dict[int, list[str]],
        config_path: Path,
        existing_config: dict | None = None,
    ):
        super().__init__()
        self.seed_ids = seed_ids
        self.config_path = config_path
        self.filter_text = ""
        self._dirty = False

        # Build existing config lookup
        prev_tracked: dict[str, dict] = {}
        if existing_config and "tracked" in existing_config:
            for entry in existing_config["tracked"]:
                prev_tracked[entry["citation"]] = entry

        # Build row state
        self.rows: list[AgencyRow] = []
        for agency in agencies:
            citation = agency.get("citation", "")
            agency_num = agency_id_from_citation(citation)
            if agency_num is None:
                continue
            is_seed = agency_num in seed_ids
            shared_count = len(shared_series_map.get(agency_num, []))
            date_range = agency.get("start_dt") or ""
            end = agency.get("end_dt")
            if date_range and end:
                date_range = f"{date_range}–{end}"
            elif date_range:
                date_range = f"{date_range}–"

            # Restore previous config or default seeds to tracked
            if citation in prev_tracked:
                tracked = True
                include_series = prev_tracked[citation].get("include_series", False)
            elif is_seed:
                tracked = True
                include_series = True
            else:
                tracked = False
                include_series = False

            self.rows.append(
                AgencyRow(
                    agency_id=agency_num,
                    citation=citation,
                    title=agency.get("title", ""),
                    date_range=date_range,
                    shared_count=shared_count,
                    is_seed=is_seed,
                    tracked=tracked,
                    include_series=include_series,
                )
            )

    def compose(self) -> ComposeResult:
        yield Header()
        seed_str = ", ".join(f"VA {sid}" for sid in sorted(self.seed_ids))
        tracked_count = sum(1 for r in self.rows if r.tracked)
        yield Static(
            f"Seeds: {seed_str}  |  {len(self.rows)} related agencies  |  {tracked_count} tracked",
            id="status-bar",
        )
        with Horizontal(id="filter-row"):
            yield Static("Filter:", id="filter-label")
            yield Input(placeholder="Type to filter agencies...", id="filter-input")
        yield DataTable(id="agency-table")
        yield Static(
            "[space] toggle+next  [t] toggle tracked  [i] toggle series  [j/k] nav  [s] save  [/] filter  [q] quit",
            id="help-text",
        )
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#agency-table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_column("Tracked", key="tracked")
        table.add_column("Series", key="series")
        table.add_column("Citation", key="citation")
        table.add_column("Title", key="title")
        table.add_column("Dates", key="dates")
        table.add_column("Shared", key="shared")
        table.add_column("Seed", key="seed")
        self._populate_table()

    def _populate_table(self) -> None:
        table = self.query_one("#agency-table", DataTable)
        table.clear()
        filter_lower = self.filter_text.lower()
        for row in self.rows:
            if filter_lower and filter_lower not in row.citation.lower() and filter_lower not in row.title.lower():
                continue
            table.add_row(
                "✓" if row.tracked else " ",
                "✓" if row.include_series else " ",
                row.citation,
                row.title[:60],
                row.date_range,
                str(row.shared_count),
                "●" if row.is_seed else "",
                key=row.citation,
            )

    def _get_selected_row(self) -> AgencyRow | None:
        table = self.query_one("#agency-table", DataTable)
        if table.row_count == 0:
            return None
        try:
            cell_key = table.coordinate_to_cell_key((table.cursor_row, 0))
            citation = str(cell_key.row_key.value)
        except Exception:
            return None
        for row in self.rows:
            if row.citation == citation:
                return row
        return None

    def _update_status(self) -> None:
        seed_str = ", ".join(f"VA {sid}" for sid in sorted(self.seed_ids))
        tracked_count = sum(1 for r in self.rows if r.tracked)
        series_count = sum(1 for r in self.rows if r.tracked and r.include_series)
        dirty = " [unsaved]" if self._dirty else ""
        self.query_one("#status-bar", Static).update(
            f"Seeds: {seed_str}  |  {len(self.rows)} related agencies  |  "
            f"{tracked_count} tracked  |  {series_count} with series{dirty}"
        )

    def _refresh_current_row(self, row: AgencyRow) -> None:
        table = self.query_one("#agency-table", DataTable)
        table.update_cell(row.citation, "tracked", "✓" if row.tracked else " ")
        table.update_cell(row.citation, "series", "✓" if row.include_series else " ")

    def action_toggle_tracked(self) -> None:
        row = self._get_selected_row()
        if row is None:
            return
        row.tracked = not row.tracked
        if not row.tracked:
            row.include_series = False
        self._dirty = True
        self._refresh_current_row(row)
        self._update_status()

    def action_toggle_tracked_and_advance(self) -> None:
        self.action_toggle_tracked()
        table = self.query_one("#agency-table", DataTable)
        table.action_cursor_down()

    def action_cursor_down(self) -> None:
        self.query_one("#agency-table", DataTable).action_cursor_down()

    def action_cursor_up(self) -> None:
        self.query_one("#agency-table", DataTable).action_cursor_up()

    def action_cursor_top(self) -> None:
        table = self.query_one("#agency-table", DataTable)
        table.move_cursor(row=0)

    def action_cursor_bottom(self) -> None:
        table = self.query_one("#agency-table", DataTable)
        table.move_cursor(row=table.row_count - 1)

    def action_toggle_series(self) -> None:
        row = self._get_selected_row()
        if row is None:
            return
        row.include_series = not row.include_series
        if row.include_series:
            row.tracked = True
        self._dirty = True
        self._refresh_current_row(row)
        self._update_status()

    def action_save(self) -> None:
        tracked = []
        for row in self.rows:
            if row.tracked:
                tracked.append(
                    {
                        "citation": row.citation,
                        "title": row.title,
                        "include_series": row.include_series,
                    }
                )
        save_config(self.config_path, list(self.seed_ids), tracked)
        self._dirty = False
        self._update_status()
        self.notify(f"Saved {len(tracked)} tracked agencies to {self.config_path}")

    def action_focus_filter(self) -> None:
        self.query_one("#filter-input", Input).focus()

    def action_clear_filter(self) -> None:
        inp = self.query_one("#filter-input", Input)
        if inp.has_focus:
            inp.value = ""
            self.filter_text = ""
            self._populate_table()
            self.query_one("#agency-table", DataTable).focus()
        else:
            self.filter_text = ""
            inp.value = ""
            self._populate_table()

    @on(Input.Changed, "#filter-input")
    def on_filter_changed(self, event: Input.Changed) -> None:
        self.filter_text = event.value
        self._populate_table()

    @on(Input.Submitted, "#filter-input")
    def on_filter_submitted(self, event: Input.Submitted) -> None:
        self.query_one("#agency-table", DataTable).focus()

    def action_quit(self) -> None:
        if self._dirty:
            self.notify("Unsaved changes! Press [s] to save or [q] again to quit.", severity="warning")
            self._dirty = False  # allow second q to quit
            return
        self.exit()


# ── CLI ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="TUI for configuring agency-series tracking from PROV data."
    )
    parser.add_argument(
        "agency_ids",
        nargs="*",
        metavar="AGENCY_ID",
        help="Seed agency IDs (e.g., VA2876, VA 2876, or 2876)",
    )
    parser.add_argument(
        "--config",
        default="prov-tracking.toml",
        help="Config file path (default: prov-tracking.toml)",
    )
    parser.add_argument(
        "--agencies-file",
        default="prov-agencies.json",
        help="Path to prov-agencies.json (default: prov-agencies.json)",
    )
    parser.add_argument(
        "--series-file",
        default="prov-series.json",
        help="Path to prov-series.json (default: prov-series.json)",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    args = parser.parse_args()

    config_path = Path(args.config)
    existing_config = None

    # Determine seed agencies
    seed_ids: set[int] = set()

    # Load from existing config if it exists
    if config_path.exists():
        print(f"Loading existing config from {config_path}...", file=sys.stderr)
        existing_config = load_config(config_path)
        for raw_id in existing_config.get("seed", {}).get("agencies", []):
            parsed = parse_agency_id(raw_id)
            if parsed is not None:
                seed_ids.add(parsed)

    # Add any CLI-provided seed agencies
    for raw_id in args.agency_ids:
        parsed = parse_agency_id(raw_id)
        if parsed is None:
            print(f"Error: Could not parse agency ID '{raw_id}'", file=sys.stderr)
            sys.exit(1)
        seed_ids.add(parsed)

    if not seed_ids:
        print("Error: No seed agencies specified.", file=sys.stderr)
        print("  Provide agency IDs on the command line or via --config", file=sys.stderr)
        sys.exit(1)

    print(f"Seed agencies: {', '.join(f'VA {s}' for s in sorted(seed_ids))}", file=sys.stderr)

    # Load data
    print(f"Loading series from {args.series_file}...", file=sys.stderr)
    with open(args.series_file) as f:
        all_series = json.load(f)
    print(f"  {len(all_series)} series records", file=sys.stderr)

    print(f"Loading agencies from {args.agencies_file}...", file=sys.stderr)
    with open(args.agencies_file) as f:
        all_agencies = json.load(f)
    print(f"  {len(all_agencies)} agency records", file=sys.stderr)

    # Discover relationships
    print("Discovering related agencies...", file=sys.stderr)
    agencies, shared_series_map = discover_related_agencies(
        seed_ids, all_series, all_agencies
    )
    print(f"  Found {len(agencies)} related agencies", file=sys.stderr)

    # Launch TUI
    app = ProvConfigApp(
        seed_ids=seed_ids,
        agencies=agencies,
        shared_series_map=shared_series_map,
        config_path=config_path,
        existing_config=existing_config,
    )
    app.title = "PROV Agency Tracker"
    app.sub_title = f"Seeds: {', '.join(f'VA {s}' for s in sorted(seed_ids))}"
    app.run()


if __name__ == "__main__":
    main()
