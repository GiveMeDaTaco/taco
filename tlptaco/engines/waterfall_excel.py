"""
Excel writer for waterfall reports, preserving the old formatting and layout.
"""
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd


def write_waterfall_excel(
    conditions: pd.DataFrame,
    compiled_groups,
    output_path: str,
    group_name=None,
    offer_code: str = '',
    campaign_planner: str = '',
    lead: str = '',
    current_date: str = '',
    starting_pops: dict | None = None,
) -> None:
    """Write a single Excel workbook containing *all* waterfall groupings.

    Parameters
    ----------
    conditions
        DataFrame indexed by ``check_name`` with added columns:
        [Section, Template, #, sql, description]
    compiled_groups
        List of tuples ``(group_name, compiled)`` where *compiled* is the
        list returned by WaterfallEngine (section_name, pivoted_df).
    output_path
        Destination ``.xlsx`` file.
    offer_code, campaign_planner, lead, current_date
        Metadata for header row.
    """

    # ------------------------------------------------------------------
    # Backwards-compatibility shim:
    #   compiled_groups  – originally a flat list of (section, DataFrame).
    #   group_name param – now sometimes transports the *real* grouped data
    #                      when called from WaterfallEngine.
    # ------------------------------------------------------------------
    import pandas as _pd_check  # type: ignore

    if isinstance(group_name, list) and group_name and isinstance(group_name[0], tuple):
        # group_name actually carries the grouped structure; override
        compiled_groups = group_name

    # If we still have flat structure, wrap into single group
    if compiled_groups and isinstance(compiled_groups, list):
        first = compiled_groups[0]
        if isinstance(first, tuple) and isinstance(first[1], _pd_check.DataFrame):
            grp_label = 'group'
            compiled_groups = [(grp_label, compiled_groups)]

    wb = Workbook()
    ws = wb.active
    ws.title = 'waterfall'

    # ------------------------------------------------------------------
    # Header Row (row 1)
    # ------------------------------------------------------------------
    header = f'[[{offer_code}] [CP: {campaign_planner}] [LEAD: {lead}] [DATE: {current_date}]'
    ws['A1'] = header
    ws['A1'].font = Font(size=18)

    # ------------------------------------------------------------------
    # Column Titles (row 2)
    # ------------------------------------------------------------------
    base_titles = ['Section', 'Template', '#', 'Criteria', 'Description']
    for col_idx, title in enumerate(base_titles, start=1):
        cell = ws.cell(row=2, column=col_idx, value=title)
        cell.font = Font(size=12)
        cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')

    # ------------------------------------------------------------------
    # Pre-compute metric header mapping and order
    # ------------------------------------------------------------------
    metric_order = [
        ('unique_drops', 'Drop If Only This Scrub'),
        ('regain', 'Regain If No Scrub'),
        ('incremental_drops', 'Drop Incremental'),
        ('cumulative_drops', 'Drop Cumulative'),
        ('remaining', 'Remaining'),
    ]

    # Calculate starting column index for first group metrics
    start_col = len(base_titles) + 1  # first metric-column position

    header_fill = PatternFill(start_color='87CEEB', end_color='87CEEB', fill_type='solid')

    # ------------------------------------------------------------------
    # Write metric header rows for each group (rows 2 and 3)
    # ------------------------------------------------------------------
    for grp_name, _ in compiled_groups:
        col_start = start_col

        # Merge cells on row 2 for the group title
        ws.merge_cells(start_row=2, start_column=col_start,
                       end_row=2, end_column=col_start + len(metric_order) - 1)
        gcell = ws.cell(row=2, column=col_start, value=grp_name)
        gcell.font = Font(size=10, bold=True)
        gcell.alignment = Alignment(horizontal='center', vertical='center')

        # Row 3 – metric headers
        col_idx = col_start
        for mkey, display in metric_order:
            cell = ws.cell(row=3, column=col_idx, value=display)
            cell.font = Font(size=9)
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            ws.column_dimensions[cell.column_letter].width = 14
            col_idx += 1

        # spacer column
        start_col = col_idx + 1

    # ------------------------------------------------------------------
    # Write condition rows (starting Population + each filter row)
    # ------------------------------------------------------------------
    # Reset pointer to first metric column again
    first_group_start = len(base_titles) + 1

    # Build mapping: group_name -> metrics_by_check_name (dict)
    compiled_lookup: dict[str, dict[str, dict[str, int]]] = {}
    for group_name, compiled in compiled_groups:
        grp_dict: dict[str, dict[str, int]] = {}
        for _, df in compiled:
            for _, row in df.iterrows():
                grp_dict[row['check_name']] = row.to_dict()
        compiled_lookup[group_name] = grp_dict

    # ------------------------------------------------------------------
    # Starting Population row (row 4)
    # ------------------------------------------------------------------
    row_idx = 4
    ws.cell(row=row_idx, column=5, value='Starting Population').font = Font(size=10, bold=True)

    # Fill starting population numbers per group under the *Remaining* metric
    pop_col_base = len(base_titles) + 1 + metric_order.index(('remaining', 'Remaining'))
    spacer = 1  # plus spacer cols between groups
    if starting_pops is None:
        starting_pops = {}
    col_ptr = pop_col_base
    for grp_name, _ in compiled_groups:
        val = starting_pops.get(grp_name, '')
        ws.cell(row=row_idx, column=col_ptr, value=val).font = Font(size=10)
        # advance to next group's remaining column: block width len(metric_order)+1
        col_ptr += len(metric_order) + spacer

    # ------------------------------------------------------------------
    # Iterate through conditions rows – begin at row 5
    # ------------------------------------------------------------------
    row_idx = 4
    for _, cond_row in conditions.reset_index().iterrows():
        row_idx += 1
        # Base columns
        ws.cell(row=row_idx, column=1, value=cond_row['Section']).font = Font(size=10)
        ws.cell(row=row_idx, column=2, value=cond_row['Template']).font = Font(size=10)
        ws.cell(row=row_idx, column=3, value=cond_row['#']).font = Font(size=10)
        ws.cell(row=row_idx, column=4, value=cond_row['sql']).font = Font(size=10)
        ws.cell(row=row_idx, column=5, value=cond_row['description']).font = Font(size=10)

        # Metrics for each group
        col_base = len(base_titles) + 1
        for group_name, _ in compiled_groups:
            metrics = compiled_lookup.get(group_name, {}).get(cond_row['check_name'], {})
            # First column (spacer) stay blank
            col_idx = col_base
            col_idx += 1  # move to first metric
            for metric_key, _ in metric_order:
                val = metrics.get(metric_key, '')
                ws.cell(row=row_idx, column=col_idx, value=val).font = Font(size=10)
                col_idx += 1
            # Skip spacer column
            col_idx += 1
            col_base = col_idx - 1

    wb.save(output_path)