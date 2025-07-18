"""
Excel writer for waterfall reports, preserving the old formatting and layout.
"""
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd


def write_waterfall_excel(
    conditions: pd.DataFrame,
    compiled: list[tuple[str, pd.DataFrame]],
    output_path: str,
    group_name: str,
    offer_code: str,
    campaign_planner: str,
    lead: str,
    current_date: str,
) -> None:
    """
    Writes a formatted waterfall Excel workbook.

    conditions: DataFrame with index=check_name and columns ['sql', 'description'].
    compiled: Ordered dict of section name -> waterfall DataFrame (wide format).
    output_path: path to save the .xlsx file.
    offer_code, campaign_planner, lead, current_date: metadata for header.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = 'waterfall'

    # Header line
    header = f'[[{offer_code}] [CP: {campaign_planner}] [LEAD: {lead}] [DATE: {current_date}]'
    ws['A1'] = header
    ws['A1'].font = Font(size=18)

    # Column titles
    titles = ['Checks', 'Criteria', 'Description']
    for col_idx, title in enumerate(titles, start=1):
        cell = ws.cell(row=2, column=col_idx, value=title)
        cell.font = Font(size=12)
        cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')

    # Starting Population label
    cell = ws.cell(row=3, column=3, value='Starting Population')
    cell.font = Font(size=12)
    cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')

    # Write conditions SQL+description
    cond_df = conditions.reset_index().rename(columns={'index': 'check_name'})
    for r_idx, row in enumerate(dataframe_to_rows(cond_df, index=False, header=False), start=4):
        for c_idx, val in enumerate(row, start=1):
            ws.cell(row=r_idx, column=c_idx, value=val)

    # Write each waterfall section side-by-side, aligned with conditions
    start_col = 5
    header_fill = PatternFill(start_color='87CEEB', end_color='87CEEB', fill_type='solid')
    # compiled is a list of (section_name, DataFrame)
    # Mapping of stat_name to friendly column headers
    header_map = {
        'unique_drops': 'Drop If Only This Scrub',
        'incremental_drops': 'Drop Incremental',
        'cumulative_drops': 'Drop Cumulative',
        'regain': 'Regain If No Scrub',
        'remaining': 'Remaining'
    }
    for section, df in compiled:
        # Header row: first cell shows section name (the grouping identifier), then stat columns
        for offset, col_name in enumerate(df.columns, start=0):
            col_idx = start_col + offset
            # First column shows the section name; subsequent use friendly headers
            if offset == 0:
                display = section
            else:
                display = header_map.get(col_name, col_name)
            cell = ws.cell(row=3, column=col_idx, value=display)
            cell.font = Font(size=9)
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            ws.column_dimensions[cell.column_letter].width = 11

        # Data rows: start at row 4 to align under condition rows
        for r_offset, row in enumerate(dataframe_to_rows(df, index=False, header=False), start=0):
            row_idx = 4 + r_offset
            for offset, val in enumerate(row, start=0):
                col_idx = start_col + offset
                ws.cell(row=row_idx, column=col_idx, value=val).font = Font(size=10)

        start_col += len(df.columns) + 1

    wb.save(output_path)