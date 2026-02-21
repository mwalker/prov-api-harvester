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
    4. Agency view: toggle which agencies to track
    5. Series view: toggle which series to include (Tab to switch)
    6. Saves selections to a TOML config file
"""

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path

import tomli_w
from rich.markup import escape as rich_escape
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import DataTable, Footer, Header, Input, Static


VERSION = "0.2.0"


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


def strip_html(text: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


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
        shared_series_map: {agency_numeric_id: [list of shared series citations]}
    """
    # Find series that involve any seed agency
    matched_series = []
    for series in all_series:
        series_agency_ids = extract_agency_ids_from_series(series)
        if series_agency_ids & seed_ids:
            matched_series.append(series)

    # Build agency→shared-series map (only seed-related series, for the shared count)
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

    def agency_sort_key(a):
        num = agency_id_from_citation(a.get("citation", ""))
        return num if num is not None else 999999

    matched_agencies.sort(key=agency_sort_key)

    return matched_agencies, shared_series_map


def build_series_index(
    all_series: list[dict],
) -> tuple[dict[int, list[str]], dict[str, dict]]:
    """
    Build indexes over the full series dataset.

    Returns:
        agency_to_series: {agency_numeric_id: [series citations]}
        series_by_citation: {series_citation: series_record}
    """
    agency_to_series: dict[int, list[str]] = {}
    series_by_citation: dict[str, dict] = {}
    for series in all_series:
        citation = series.get("citation", "")
        if not citation:
            continue
        series_by_citation[citation] = series
        for aid in extract_agency_ids_from_series(series):
            agency_to_series.setdefault(aid, []).append(citation)
    return agency_to_series, series_by_citation


# ── Config I/O ───────────────────────────────────────────────────────────────


def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def save_config(
    path: Path,
    seed_ids: list[int],
    tracked_agencies: list[dict],
    excluded_series: list[str],
    included_series: list[str] | None = None,
) -> None:
    series_section: dict = {"excluded": sorted(excluded_series)}
    if included_series:
        series_section["included"] = sorted(included_series)
    doc = {
        "seed": {"agencies": [f"VA {aid}" for aid in sorted(seed_ids)]},
        "tracked": tracked_agencies,
        "series": series_section,
    }
    with open(path, "wb") as f:
        tomli_w.dump(doc, f)


# ── TUI ──────────────────────────────────────────────────────────────────────


class AgencyRow:
    """In-memory state for one agency in the table."""

    def __init__(
        self,
        agency_id: int,
        citation: str,
        title: str,
        date_range: str,
        shared_count: int,
        total_series_count: int,
        shared_series: list[str],
        is_seed: bool,
        tracked: bool = False,
    ):
        self.agency_id = agency_id
        self.citation = citation
        self.title = title
        self.date_range = date_range
        self.shared_count = shared_count
        self.total_series_count = total_series_count
        self.shared_series = shared_series
        self.is_seed = is_seed
        self.tracked = tracked


class SeriesRow:
    """In-memory state for one series in the table."""

    def __init__(
        self,
        citation: str,
        title: str,
        date_range: str,
        agency_citations: list[str],
        included: bool = True,
    ):
        self.citation = citation
        self.title = title
        self.date_range = date_range
        self.agency_citations = agency_citations
        self.included = included


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
    #detail-panel {
        dock: bottom;
        height: auto;
        max-height: 7;
        padding: 0 1;
        background: $surface;
        border-top: solid $primary;
        overflow-y: auto;
    }
    #detail-panel.expanded {
        max-height: 50%;
    }
    DataTable {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("s", "save", "Save config"),
        Binding("tab", "switch_view", "All series", show=True, priority=True),
        Binding("d", "view_agency_series", "Drill down", show=True),
        Binding("t", "toggle_item", "Toggle", show=True),
        Binding("space", "toggle_and_advance", "Toggle+next", show=True),
        Binding("j", "cursor_down", "Down", show=True),
        Binding("k", "cursor_up", "Up", show=True),
        Binding("g", "cursor_top", "Top", show=False),
        Binding("G", "cursor_bottom", "Bottom", show=False, key_display="shift+g"),
        Binding("/", "focus_filter", "Filter", show=True),
        Binding("escape", "clear_or_back", "Back/Clear", show=False),
    ]

    def __init__(
        self,
        seed_ids: set[int],
        agencies: list[dict],
        shared_series_map: dict[int, list[str]],
        agency_to_series: dict[int, list[str]],
        series_by_citation: dict[str, dict],
        config_path: Path,
        existing_config: dict | None = None,
    ):
        super().__init__()
        self.seed_ids = seed_ids
        self.config_path = config_path
        self.filter_text = ""
        self._dirty = False
        self.view_mode = "agencies"  # "agencies", "series", or "series_detail"
        self.series_filter_agency: int | None = None  # set when drilling into one agency
        self._last_agency_key: str | None = None  # agency to re-select on back
        self._last_series_key: str | None = None  # series to re-select on back
        self._detail_series_citation: str | None = None  # series being inspected

        # Store indexes for dynamic series lookup
        self.agency_to_series = agency_to_series
        self.series_by_citation = series_by_citation

        # Build existing config lookups
        prev_tracked: set[str] = set()
        self.excluded_series: set[str] = set()
        self.included_series: set[str] = set()
        if existing_config:
            if "tracked" in existing_config:
                for entry in existing_config["tracked"]:
                    prev_tracked.add(entry["citation"])
            if "series" in existing_config:
                self.excluded_series = set(
                    existing_config["series"].get("excluded", [])
                )
                self.included_series = set(
                    existing_config["series"].get("included", [])
                )

        has_prev_config = bool(existing_config and "tracked" in existing_config)

        # Build agency rows
        self.agency_rows: list[AgencyRow] = []
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

            if has_prev_config:
                tracked = citation in prev_tracked
            elif is_seed:
                tracked = True
            else:
                tracked = False

            series_list = shared_series_map.get(agency_num, [])
            total_series_count = len(agency_to_series.get(agency_num, []))
            self.agency_rows.append(
                AgencyRow(
                    agency_id=agency_num,
                    citation=citation,
                    title=agency.get("title", ""),
                    date_range=date_range,
                    shared_count=shared_count,
                    total_series_count=total_series_count,
                    shared_series=sorted(set(series_list)),
                    is_seed=is_seed,
                    tracked=tracked,
                )
            )

        # Build set of shared series citations (series connecting to seed agencies)
        self.shared_series_set: set[str] = set()
        for series_list in shared_series_map.values():
            self.shared_series_set.update(series_list)

        # Build agency id→citation lookup from agency_rows
        self._agency_id_to_citation: dict[int, str] = {
            r.agency_id: r.citation for r in self.agency_rows
        }

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="status-bar")
        with Horizontal(id="filter-row"):
            yield Static("Filter:", id="filter-label")
            yield Input(placeholder="Type to filter...", id="filter-input")
        yield DataTable(id="data-table")
        yield Static("", id="detail-panel")
        yield Footer()

    def on_mount(self) -> None:
        self._setup_agency_view()

    def _setup_agency_view(self) -> None:
        self.query_one("#detail-panel", Static).remove_class("expanded")
        table = self.query_one("#data-table", DataTable)
        table.clear(columns=True)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_column("Tracked", key="col0")
        table.add_column("Citation", key="col1")
        table.add_column("Title", key="col2")
        table.add_column("Dates", key="col3")
        table.add_column("Total", key="col4")
        table.add_column("Selected", key="col5")
        table.add_column("Shared", key="col6")
        table.add_column("Seed", key="col7")
        self._populate_table()
        self._update_status()

    def _setup_series_view(self) -> None:
        self.query_one("#detail-panel", Static).remove_class("expanded")
        table = self.query_one("#data-table", DataTable)
        table.clear(columns=True)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_column("Included", key="col0")
        table.add_column("Citation", key="col1")
        table.add_column("Title", key="col2")
        table.add_column("Dates", key="col3")
        table.add_column("Agencies", key="col4")
        self._populate_table()
        self._update_status()

    def _setup_series_detail_view(self) -> None:
        self.query_one("#detail-panel", Static).add_class("expanded")
        table = self.query_one("#data-table", DataTable)
        table.clear(columns=True)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_column("Role", key="col0")
        table.add_column("Citation", key="col1")
        table.add_column("Title", key="col2")
        table.add_column("Dates", key="col3")
        self._populate_table()
        self._update_status()

    def _is_series_included(self, cit: str, tracked_ids: set[int]) -> bool:
        """Determine if a series should be included.

        Priority: explicit exclusion > explicit inclusion > default.
        Default: tracked agencies get all series, others only shared.
        """
        if cit in self.excluded_series:
            return False
        if cit in self.included_series:
            return True
        # Series from a tracked agency: included by default
        series = self.series_by_citation.get(cit)
        if series is not None:
            if extract_agency_ids_from_series(series) & tracked_ids:
                return True
        # Shared series (connects to seed agencies): included by default
        if cit in self.shared_series_set:
            return True
        return False

    def _get_visible_series(self) -> list[SeriesRow]:
        """Get series for the current view scope."""
        tracked_ids = {r.agency_id for r in self.agency_rows if r.tracked}

        if self.series_filter_agency is not None:
            # Drill-down: only series for one specific agency
            scope_ids = {self.series_filter_agency}
        else:
            # All tracked agencies
            scope_ids = tracked_ids

        # Collect all unique series citations across scoped agencies
        seen: set[str] = set()
        for aid in scope_ids:
            for cit in self.agency_to_series.get(aid, []):
                seen.add(cit)

        # Build SeriesRow objects from the full index
        visible = []
        for cit in sorted(seen, key=lambda c: (
            int(m.group(1)) if (m := re.match(r"^VPRS\s+(\d+)$", c)) else 999999
        )):
            series = self.series_by_citation.get(cit)
            if series is None:
                continue

            date_range = series.get("start_dt") or ""
            end = series.get("end_dt")
            if date_range and end:
                date_range = f"{date_range}–{end}"
            elif date_range:
                date_range = f"{date_range}–"

            # Which tracked agencies have this series
            series_agency_ids = extract_agency_ids_from_series(series)
            agency_cites = sorted(
                self._agency_id_to_citation.get(aid, f"VA {aid}")
                for aid in series_agency_ids
                if aid in tracked_ids
            )

            visible.append(
                SeriesRow(
                    citation=cit,
                    title=series.get("title", ""),
                    date_range=date_range,
                    agency_citations=agency_cites,
                    included=self._is_series_included(cit, tracked_ids),
                )
            )
        return visible

    def _populate_table(self) -> None:
        table = self.query_one("#data-table", DataTable)
        table.clear()
        filter_lower = self.filter_text.lower()

        if self.view_mode == "agencies":
            tracked_ids = {r.agency_id for r in self.agency_rows if r.tracked}
            for row in self.agency_rows:
                if filter_lower and filter_lower not in row.citation.lower() and filter_lower not in row.title.lower():
                    continue
                series_cits = self.agency_to_series.get(row.agency_id, [])
                selected = sum(
                    1 for c in series_cits
                    if self._is_series_included(c, tracked_ids)
                )
                table.add_row(
                    "✓" if row.tracked else " ",
                    row.citation,
                    row.title[:60],
                    row.date_range,
                    str(row.total_series_count),
                    str(selected),
                    str(row.shared_count),
                    "●" if row.is_seed else "",
                    key=row.citation,
                )
        elif self.view_mode == "series":
            for row in self._get_visible_series():
                if filter_lower and filter_lower not in row.citation.lower() and filter_lower not in row.title.lower():
                    continue
                table.add_row(
                    "✓" if row.included else " ",
                    row.citation,
                    row.title[:60],
                    row.date_range,
                    ", ".join(row.agency_citations[:3])
                    + (f" +{len(row.agency_citations) - 3}" if len(row.agency_citations) > 3 else ""),
                    key=row.citation,
                )
        elif self.view_mode == "series_detail":
            series = self.series_by_citation.get(self._detail_series_citation or "")
            if series is None:
                return
            # Creating agencies
            ca_ids = series.get("creating_agents.creating_agency_id") or []
            ca_titles = series.get("creating_agents.title") or []
            ca_dates = series.get("creating_agents.date_ranges") or []
            for i, aid in enumerate(ca_ids):
                cite = f"VA {aid}"
                title = (ca_titles[i] if i < len(ca_titles) else "")[:60]
                dates = ca_dates[i] if i < len(ca_dates) else ""
                if filter_lower and filter_lower not in cite.lower() and filter_lower not in title.lower():
                    continue
                table.add_row("Creating", cite, title, dates, key=f"ca-{aid}")
            # Responsible agencies
            ra_ids = series.get("responsible_agents.resp_agency_id") or []
            ra_titles = series.get("responsible_agents.title") or []
            ra_dates = series.get("responsible_agents.date_ranges") or []
            for i, aid in enumerate(ra_ids):
                cite = f"VA {aid}"
                title = (ra_titles[i] if i < len(ra_titles) else "")[:60]
                dates = ra_dates[i] if i < len(ra_dates) else ""
                if filter_lower and filter_lower not in cite.lower() and filter_lower not in title.lower():
                    continue
                table.add_row("Responsible", cite, title, dates, key=f"ra-{aid}")

    def _get_selected_key(self) -> str | None:
        table = self.query_one("#data-table", DataTable)
        if table.row_count == 0:
            return None
        try:
            cell_key = table.coordinate_to_cell_key((table.cursor_row, 0))
            return str(cell_key.row_key.value)
        except Exception:
            return None

    def _move_cursor_to_key(self, key: str, center: bool = False) -> None:
        """Move the DataTable cursor to the row with the given key."""
        table = self.query_one("#data-table", DataTable)
        for idx in range(table.row_count):
            try:
                cell_key = table.coordinate_to_cell_key((idx, 0))
                if str(cell_key.row_key.value) == key:
                    table.move_cursor(row=idx)
                    if center:
                        self.set_timer(
                            0.05, lambda: self._scroll_cursor_center()
                        )
                    return
            except Exception:
                continue

    def _scroll_cursor_center(self) -> None:
        """Scroll so the current cursor row is vertically centred."""
        table = self.query_one("#data-table", DataTable)
        row = table.cursor_coordinate.row
        visible = table.scrollable_content_region.height
        target = row - visible // 2
        table.scroll_to(y=max(0, target), animate=False)

    def _update_detail(self) -> None:
        key = self._get_selected_key()
        panel = self.query_one("#detail-panel", Static)

        if key is None:
            panel.update("")
            return

        if self.view_mode == "agencies":
            row = next((r for r in self.agency_rows if r.citation == key), None)
            if row is None:
                panel.update("")
                return
            seed_label = " (seed)" if row.is_seed else ""
            series_preview = ", ".join(row.shared_series[:8])
            if len(row.shared_series) > 8:
                series_preview += f", ... (+{len(row.shared_series) - 8} more)"
            title = rich_escape(row.title)
            panel.update(
                f"[bold]{row.citation}[/bold]{seed_label}  {row.date_range}\n"
                f"{title}\n"
                f"Shared series ({row.shared_count}): {series_preview}"
            )
        elif self.view_mode == "series":
            series = self.series_by_citation.get(key)
            if series is None:
                panel.update("")
                return
            title = rich_escape(series.get("title", ""))
            date_range = series.get("start_dt") or ""
            end = series.get("end_dt")
            if date_range and end:
                date_range = f"{date_range}–{end}"
            elif date_range:
                date_range = f"{date_range}–"
            agency_ids = extract_agency_ids_from_series(series)
            agencies_str = ", ".join(sorted(
                self._agency_id_to_citation.get(aid, f"VA {aid}")
                for aid in agency_ids
            ))
            tracked_ids = {r.agency_id for r in self.agency_rows if r.tracked}
            included = "included" if self._is_series_included(key, tracked_ids) else "excluded"
            panel.update(
                f"[bold]{key}[/bold]  {date_range}  [{included}]\n"
                f"{title}\n"
                f"Agencies: {agencies_str}"
            )
        elif self.view_mode == "series_detail":
            series = self.series_by_citation.get(self._detail_series_citation or "")
            if series is None:
                panel.update("")
                return
            title = rich_escape(series.get("title", ""))
            func_raw = series.get("function_content") or []
            func_text = rich_escape(strip_html(" ".join(func_raw))) if func_raw else "(none)"
            use_raw = series.get("how_to_use") or []
            use_text = rich_escape(strip_html(" ".join(use_raw))) if use_raw else "(none)"
            panel.update(
                f"[bold]{self._detail_series_citation}[/bold]  {title}\n\n"
                f"[bold]Function/Content:[/bold]\n{func_text}\n\n"
                f"[bold]How to Use:[/bold]\n{use_text}"
            )

    @on(DataTable.RowHighlighted)
    def on_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        self._update_detail()

    def _update_status(self) -> None:
        seed_str = ", ".join(f"VA {sid}" for sid in sorted(self.seed_ids))
        tracked_count = sum(1 for r in self.agency_rows if r.tracked)
        dirty = " [unsaved]" if self._dirty else ""

        if self.view_mode == "series_detail":
            cit = self._detail_series_citation or ""
            series = self.series_by_citation.get(cit)
            title = (series.get("title", "") if series else "")[:50]
            self.query_one("#status-bar", Static).update(
                f"[bold]Series Detail[/bold]  |  {cit}  {title}{dirty}  "
                f"[dim]Esc→Back[/dim]"
            )
            return

        visible_series = self._get_visible_series()
        included_count = sum(1 for r in visible_series if r.included)

        if self.view_mode == "agencies":
            self.query_one("#status-bar", Static).update(
                f"[bold]Agencies[/bold]  |  Seeds: {seed_str}  |  "
                f"{len(self.agency_rows)} related  |  {tracked_count} tracked  |  "
                f"{included_count}/{len(visible_series)} series{dirty}  [dim]Tab→Series  Enter→Drill[/dim]"
            )
        elif self.series_filter_agency is not None:
            agency_cite = self._agency_id_to_citation.get(
                self.series_filter_agency, f"VA {self.series_filter_agency}"
            )
            self.query_one("#status-bar", Static).update(
                f"[bold]Series[/bold] for [bold]{agency_cite}[/bold]  |  "
                f"{len(visible_series)} series  |  "
                f"{included_count} included{dirty}  [dim]Esc→Back  Tab→All series[/dim]"
            )
        else:
            self.query_one("#status-bar", Static).update(
                f"[bold]Series[/bold]  |  {len(visible_series)} from {tracked_count} tracked agencies  |  "
                f"{included_count} included{dirty}  [dim]Esc→Back  Tab→Agencies[/dim]"
            )

    def _refresh_current_row_toggle(self, is_on: bool) -> None:
        key = self._get_selected_key()
        if key is None:
            return
        table = self.query_one("#data-table", DataTable)
        table.update_cell(key, "col0", "✓" if is_on else " ")

    def action_switch_view(self) -> None:
        """Tab: toggle between agencies and all-tracked-agencies series."""
        self.filter_text = ""
        self.query_one("#filter-input", Input).value = ""
        if self.view_mode == "series_detail":
            # Back to series first
            restore_key = self._last_series_key
            self._detail_series_citation = None
            self.view_mode = "series"
            self._setup_series_view()
            if restore_key is not None:
                self._move_cursor_to_key(restore_key, center=True)
        elif self.view_mode == "agencies":
            self.view_mode = "series"
            self.series_filter_agency = None
            self._setup_series_view()
        else:
            restore_key = self._last_agency_key
            self.view_mode = "agencies"
            self.series_filter_agency = None
            self._setup_agency_view()
            if restore_key is not None:
                self._move_cursor_to_key(restore_key)

    def action_view_agency_series(self) -> None:
        """d: drill into the selected item."""
        key = self._get_selected_key()
        if key is None:
            return
        if self.view_mode == "agencies":
            row = next((r for r in self.agency_rows if r.citation == key), None)
            if row is None:
                return
            self.filter_text = ""
            self.query_one("#filter-input", Input).value = ""
            self._last_agency_key = row.citation
            self.series_filter_agency = row.agency_id
            self.view_mode = "series"
            self._setup_series_view()
        elif self.view_mode == "series":
            if key not in self.series_by_citation:
                return
            self.filter_text = ""
            self.query_one("#filter-input", Input).value = ""
            self._last_series_key = key
            self._detail_series_citation = key
            self.view_mode = "series_detail"
            self._setup_series_detail_view()

    def action_toggle_item(self) -> None:
        key = self._get_selected_key()
        if key is None:
            return

        if self.view_mode == "series_detail":
            return
        if self.view_mode == "agencies":
            row = next((r for r in self.agency_rows if r.citation == key), None)
            if row is None:
                return
            row.tracked = not row.tracked
            self._refresh_current_row_toggle(row.tracked)
        else:
            tracked_ids = {r.agency_id for r in self.agency_rows if r.tracked}
            if self._is_series_included(key, tracked_ids):
                # Currently included → exclude it
                self.included_series.discard(key)
                self.excluded_series.add(key)
                self._refresh_current_row_toggle(False)
            else:
                # Currently excluded → include it
                self.excluded_series.discard(key)
                self.included_series.add(key)
                self._refresh_current_row_toggle(True)

        self._dirty = True
        self._update_status()

    def action_toggle_and_advance(self) -> None:
        self.action_toggle_item()
        table = self.query_one("#data-table", DataTable)
        table.action_cursor_down()

    def action_cursor_down(self) -> None:
        self.query_one("#data-table", DataTable).action_cursor_down()

    def action_cursor_up(self) -> None:
        self.query_one("#data-table", DataTable).action_cursor_up()

    def action_cursor_top(self) -> None:
        table = self.query_one("#data-table", DataTable)
        table.move_cursor(row=0)

    def action_cursor_bottom(self) -> None:
        table = self.query_one("#data-table", DataTable)
        table.move_cursor(row=table.row_count - 1)

    def action_save(self) -> None:
        tracked_agencies = []
        for row in self.agency_rows:
            if row.tracked:
                tracked_agencies.append(
                    {"citation": row.citation, "title": row.title}
                )

        # Only save overrides for series that are currently visible
        visible_citations = {r.citation for r in self._get_visible_series()}
        excluded = sorted(self.excluded_series & visible_citations)
        included = sorted(self.included_series & visible_citations)

        save_config(
            self.config_path,
            list(self.seed_ids),
            tracked_agencies,
            excluded,
            included,
        )
        self._dirty = False
        self._update_status()
        included_count = len(visible_citations) - len(excluded)
        self.notify(
            f"Saved {len(tracked_agencies)} agencies, "
            f"{included_count}/{len(visible_citations)} series "
            f"to {self.config_path}"
        )

    def action_focus_filter(self) -> None:
        self.query_one("#filter-input", Input).focus()

    def action_clear_or_back(self) -> None:
        inp = self.query_one("#filter-input", Input)
        if inp.has_focus:
            inp.value = ""
            self.filter_text = ""
            self._populate_table()
            self.query_one("#data-table", DataTable).focus()
        elif self.filter_text:
            self.filter_text = ""
            inp.value = ""
            self._populate_table()
        elif self.view_mode == "series_detail":
            restore_key = self._last_series_key
            self._detail_series_citation = None
            self.view_mode = "series"
            self.filter_text = ""
            inp.value = ""
            self._setup_series_view()
            if restore_key is not None:
                self._move_cursor_to_key(restore_key, center=True)
        elif self.view_mode == "series":
            restore_key = self._last_agency_key
            self.view_mode = "agencies"
            self.series_filter_agency = None
            self.filter_text = ""
            inp.value = ""
            self._setup_agency_view()
            if restore_key is not None:
                self._move_cursor_to_key(restore_key)

    @on(Input.Changed, "#filter-input")
    def on_filter_changed(self, event: Input.Changed) -> None:
        self.filter_text = event.value
        self._populate_table()

    @on(Input.Submitted, "#filter-input")
    def on_filter_submitted(self, event: Input.Submitted) -> None:
        self.query_one("#data-table", DataTable).focus()

    def action_quit(self) -> None:
        if self._dirty:
            self.notify(
                "Unsaved changes! Press [s] to save or [q] again to quit.",
                severity="warning",
            )
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

    # Build full series index
    print("Indexing series...", file=sys.stderr)
    agency_to_series, series_by_citation = build_series_index(all_series)
    print(f"  Indexed {len(series_by_citation)} series", file=sys.stderr)

    # Launch TUI
    app = ProvConfigApp(
        seed_ids=seed_ids,
        agencies=agencies,
        shared_series_map=shared_series_map,
        agency_to_series=agency_to_series,
        series_by_citation=series_by_citation,
        config_path=config_path,
        existing_config=existing_config,
    )
    app.title = "PROV Agency Tracker"
    app.sub_title = f"Seeds: {', '.join(f'VA {s}' for s in sorted(seed_ids))}"
    app.run()


if __name__ == "__main__":
    main()
