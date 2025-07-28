"""
Excel writer for waterfall reports (current + history).

The workbook layout is:

1. Sheet "waterfall" – consolidated report identical to the original format
   (all groups side-by-side).
2. One additional sheet *per group* containing:
      • a full table for the *current* run
      • a full table for the *previous* run within the configured look-back
        window (if no prior data → note displayed instead).

This module has **no legacy compatibility shims** – it is only used by
WaterfallEngine v2 and tests inside this repository.
"""

from __future__ import annotations

from typing import List, Tuple, Dict, Iterable, Any

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment

# ────────────────────────────────────────────────────────────────────────────────
# Constants / helper data -------------------------------------------------------
# ────────────────────────────────────────────────────────────────────────────────


_BASE_TITLES = ["Section", "Template", "#", "Criteria", "Description"]

# Column order & friendly display names for metrics
_METRIC_ORDER: List[Tuple[str, str]] = [
    ("unique_drops", "Drop If Only This Scrub"),
    ("regain", "Regain If No Scrub"),
    ("incremental_drops", "Drop Incremental"),
    ("cumulative_drops", "Drop Cumulative"),
    ("remaining", "Remaining"),
]

_HEADER_FILL = PatternFill(start_color="87CEEB", end_color="87CEEB", fill_type="solid")


# ────────────────────────────────────────────────────────────────────────────────
# Public API --------------------------------------------------------------------
# ────────────────────────────────────────────────────────────────────────────────


def write_waterfall_excel(
    conditions: pd.DataFrame,
    compiled_current: List[Tuple[str, List[Tuple[str, pd.DataFrame]]]],
    output_path: str,
    *,
    previous: Dict[str, List[Tuple[str, pd.DataFrame]]] | None = None,
    offer_code: str = "",
    campaign_planner: str = "",
    lead: str = "",
    current_date: str = "",
    starting_pops: Dict[str, int] | None = None,
) -> None:
    """Write the Excel workbook for a Waterfall run.

    Parameters
    ----------
    conditions
        DataFrame indexed by *check_name* with extra descriptive columns.
    compiled_current
        Output of WaterfallEngine for the current run: list of
        ``(group_name, compiled)`` where *compiled* == list of
        ``(section_name, pivoted_df)``.
    output_path
        Destination ``.xlsx`` file.
    previous
        Optional mapping: ``{group_name: compiled_previous}`` following the
        same internal *compiled* structure. Used to render comparison tables
        on individual group sheets.
    starting_pops
        Mapping ``group_name -> starting_population``. If omitted the
        *Starting Population* row is left blank.
    """

    if previous is None:
        previous = {}
    if starting_pops is None:
        starting_pops = {}

    wb = Workbook()
    ws_cons = wb.active
    ws_cons.title = "waterfall"

    _write_header(ws_cons, offer_code, campaign_planner, lead, current_date)
    _write_consolidated_table(ws_cons, conditions, compiled_current, starting_pops)

    # ------------------------------------------------------------------
    # Per-group sheets ---------------------------------------------------
    # ------------------------------------------------------------------
    for group_name, compiled in compiled_current:
        sheet_name = group_name[:31]  # Excel sheet name limit
        ws_grp = wb.create_sheet(title=sheet_name)

        _write_header(ws_grp, offer_code, campaign_planner, lead, current_date)

        # Current run table ------------------------------------------------
        next_row = 2
        next_row = _write_group_table(
            ws_grp,
            start_row=next_row,
            group_name=group_name,
            compiled=compiled,
            conditions=conditions,
            start_pop=starting_pops.get(group_name),
        )

        # Spacer row and label
        ws_grp.cell(row=next_row, column=1, value="Previous").font = Font(size=12, bold=True)
        next_row += 1

        prev_compiled = previous.get(group_name)
        if prev_compiled:
            _write_group_table(
                ws_grp,
                start_row=next_row,
                group_name=group_name,
                compiled=prev_compiled,
                conditions=conditions,
                start_pop=None,
            )
        else:
            ws_grp.cell(row=next_row, column=1, value="No prior data available within look-back window").font = Font(size=10)

    wb.save(output_path)


# ────────────────────────────────────────────────────────────────────────────────
# Private helpers ----------------------------------------------------------------
# ────────────────────────────────────────────────────────────────────────────────


def _write_header(ws, offer_code: str, campaign_planner: str, lead: str, current_date: str) -> None:
    """Write the big header row (row 1)."""
    header_txt = f"[[{offer_code}] [CP: {campaign_planner}] [LEAD: {lead}] [DATE: {current_date}]"
    ws["A1"] = header_txt
    ws["A1"].font = Font(size=18)


def _write_consolidated_table(ws, conditions: pd.DataFrame, compiled_groups: List[Tuple[str, List[Tuple[str, pd.DataFrame]]]], starting_pops: Dict[str, int]) -> None:
    """Render the classic side-by-side consolidated sheet (rows begin at 2)."""

    # Column titles – row 2
    for cidx, title in enumerate(_BASE_TITLES, start=1):
        cell = ws.cell(row=2, column=cidx, value=title)
        cell.font = Font(size=12)
        cell.alignment = Alignment(wrap_text=True, horizontal="center", vertical="center")

    start_col = len(_BASE_TITLES) + 1  # first metric column

    # Metric header rows (rows 2-3)
    col_ptr = start_col
    for group_name, _ in compiled_groups:
        ws.merge_cells(start_row=2, start_column=col_ptr, end_row=2, end_column=col_ptr + len(_METRIC_ORDER) - 1)
        gcell = ws.cell(row=2, column=col_ptr, value=group_name)
        gcell.font = Font(size=10, bold=True)
        gcell.alignment = Alignment(horizontal="center", vertical="center")

        for m_idx, (_, disp) in enumerate(_METRIC_ORDER):
            cell = ws.cell(row=3, column=col_ptr + m_idx, value=disp)
            cell.font = Font(size=9)
            cell.fill = _HEADER_FILL
            cell.alignment = Alignment(horizontal="center", vertical="center")
            ws.column_dimensions[cell.column_letter].width = 14

        col_ptr += len(_METRIC_ORDER) + 1  # +1 spacer

    # Build lookup: group -> check_name -> metrics dict
    group_lookup: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for grp, compiled in compiled_groups:
        sub: Dict[str, Dict[str, Any]] = {}
        for _, df in compiled:
            for _, row in df.iterrows():
                sub[row["check_name"]] = row.to_dict()
        group_lookup[grp] = sub

    # Starting population row (row 4)
    row_idx = 4
    ws.cell(row=row_idx, column=5, value="Starting Population").font = Font(size=10, bold=True)

    col_ptr = len(_BASE_TITLES) + _metric_remaining_offset()  # Remaining column within block
    for grp, _ in compiled_groups:
        val = starting_pops.get(grp, "")
        ws.cell(row=row_idx, column=col_ptr, value=val).font = Font(size=10)
        col_ptr += len(_METRIC_ORDER) + 1

    # Conditions rows – begin at row 5
    for _, cond_row in conditions.reset_index().iterrows():
        row_idx += 1
        # Base descriptive columns
        ws.cell(row=row_idx, column=1, value=cond_row["Section"]).font = Font(size=10)
        ws.cell(row=row_idx, column=2, value=cond_row["Template"]).font = Font(size=10)
        ws.cell(row=row_idx, column=3, value=cond_row["#"]).font = Font(size=10)
        ws.cell(row=row_idx, column=4, value=cond_row["sql"]).font = Font(size=10)
        ws.cell(row=row_idx, column=5, value=cond_row["description"]).font = Font(size=10)

        col_ptr = len(_BASE_TITLES) + 1
        for grp, _ in compiled_groups:
            metrics = group_lookup.get(grp, {}).get(cond_row["check_name"], {})
            for metric_key, _ in _METRIC_ORDER:
                val = metrics.get(metric_key, "")
                ws.cell(row=row_idx, column=col_ptr, value=val).font = Font(size=10)
                col_ptr += 1
            # spacer
            col_ptr += 1


def _write_group_table(
    ws,
    *,
    start_row: int,
    group_name: str,
    compiled: List[Tuple[str, pd.DataFrame]],
    conditions: pd.DataFrame,
    start_pop: int | None,
) -> int:
    """Render *compiled* metrics for **one** group starting at *start_row*.

    Returns the next free row index after the table.
    """

    # Title rows ---------------------------------------------------------
    # Row with base titles
    for cidx, title in enumerate(_BASE_TITLES, start=1):
        cell = ws.cell(row=start_row, column=cidx, value=title)
        cell.font = Font(size=12)
        cell.alignment = Alignment(wrap_text=True, horizontal="center", vertical="center")

    # Group title merge
    ws.merge_cells(start_row=start_row, start_column=len(_BASE_TITLES) + 1,
                   end_row=start_row, end_column=len(_BASE_TITLES) + len(_METRIC_ORDER))
    gcell = ws.cell(row=start_row, column=len(_BASE_TITLES) + 1, value=group_name)
    gcell.font = Font(size=10, bold=True)
    gcell.alignment = Alignment(horizontal="center", vertical="center")

    # Row with metric headers
    row_hdr2 = start_row + 1
    for m_idx, (_, disp) in enumerate(_METRIC_ORDER):
        cell = ws.cell(row=row_hdr2, column=len(_BASE_TITLES) + 1 + m_idx, value=disp)
        cell.font = Font(size=9)
        cell.fill = _HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center")
        ws.column_dimensions[cell.column_letter].width = 14

    # Build lookup for metrics
    metrics_lookup: Dict[str, Dict[str, Any]] = {}
    for _, df in compiled:
        for _, row in df.iterrows():
            metrics_lookup[row["check_name"]] = row.to_dict()

    # Starting population row
    row_ptr = row_hdr2 + 1
    ws.cell(row=row_ptr, column=5, value="Starting Population").font = Font(size=10, bold=True)
    if start_pop is not None:
        pop_col = len(_BASE_TITLES) + _metric_remaining_offset()
        ws.cell(row=row_ptr, column=pop_col, value=start_pop).font = Font(size=10)

    # Condition rows
    for _, cond_row in conditions.reset_index().iterrows():
        row_ptr += 1
        ws.cell(row=row_ptr, column=1, value=cond_row["Section"]).font = Font(size=10)
        ws.cell(row=row_ptr, column=2, value=cond_row["Template"]).font = Font(size=10)
        ws.cell(row=row_ptr, column=3, value=cond_row["#"]).font = Font(size=10)
        ws.cell(row=row_ptr, column=4, value=cond_row["sql"]).font = Font(size=10)
        ws.cell(row=row_ptr, column=5, value=cond_row["description"]).font = Font(size=10)

        for m_idx, (mkey, _) in enumerate(_METRIC_ORDER):
            val = metrics_lookup.get(cond_row["check_name"], {}).get(mkey, "")
            ws.cell(row=row_ptr, column=len(_BASE_TITLES) + 1 + m_idx, value=val).font = Font(size=10)

    return row_ptr + 2  # leave a blank row after table


def _metric_remaining_offset() -> int:
    """Return 1-based offset (within metric block) of 'remaining' column."""
    for idx, (key, _) in enumerate(_METRIC_ORDER, start=1):
        if key == "remaining":
            return idx
    return 1
