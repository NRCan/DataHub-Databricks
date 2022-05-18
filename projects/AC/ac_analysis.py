""" CA Analysis Main """

import sys
import os
from typing import Callable

from openpyxl import load_workbook
from openpyxl.styles import PatternFill

from ac_analysis_engine import Analizer
from ac_analysis_utils import ACRow, ACContext

def _get_output_name(file_name: str):
    split = os.path.splitext(file_name)
    return f"{split[0]}_results{split[1]}"

def _highlight_row(row, color):
    for cell in row:
        cell.fill = color

def _load_set_column(sheet, from_row, col) -> set[str]:
    values = set()
    for row in sheet.iter_rows(min_row = from_row, max_col = col, max_row = sheet.max_row):
        if row[col - 1].value:
            values.add(row[col - 1].value.strip().upper())
    return values

def _load_fps_chs(sheet) -> set[str]:
    return _load_set_column(sheet, 2, 2)

def _load_rcm_chs(sheet) -> set[str]:
    return _load_set_column(sheet, 2, 3)

def _load_dsc_list(sheet) -> set[str]:
    return _load_set_column(sheet, 2, 3)

def _load_filter_colors(sheet) -> dict:
    colors = {}
    for row in sheet.iter_rows(min_row = 5, max_col = 3, max_row = sheet.max_row):
        cell = row[2]
        if cell.value:
            color = f"{cell.fill.start_color.rgb}" 
            colors[cell.value.strip()] = PatternFill(start_color=color, end_color=color, fill_type="solid")
    return colors

def _get_context(workbook) -> ACContext:

    fps_chs = _load_fps_chs(workbook["FPS CHs"])
    rcm_chs = _load_rcm_chs(workbook["RCM - CH Listing "]) #notice there is a space at the end in the sheet name
    dsc_list = _load_dsc_list(workbook["DSC"])
    colors = _load_filter_colors(workbook["Filters"])

    return ACContext(fps_cardholders=fps_chs, rcm_cardholders=rcm_chs, dsc_list=dsc_list, 
        highlight_colors=colors, result_column="AI")

def run_analysis(argvs):

    if len(argvs) != 2:
        _, tail = os.path.split(argvs[0])
        print(f'Usage:\n\t{tail} <excel_file_name>')
        return 0

    input_file = argvs[1]

    workbook = load_workbook(input_file, read_only=False, data_only=True)
    master_sheet = workbook.worksheets[0]
    
    context = _get_context(workbook)
    last_column = master_sheet.max_column + 1

    master_sheet.cell(row=1, column=last_column).value = "CRITERIA"

    analyzer = Analizer(context)

    # iterates the master sheet
    row_index = 2
    for excel_row in master_sheet.iter_rows(min_row = row_index, max_col = last_column, max_row = master_sheet.max_row):

        row = ACRow(row=excel_row, index=row_index)

        if row["A"] is not None:
            analyzer.analize(row)

        row_index += 1

    # highlight the rows
    analyzer.highlight_rows()

    #print(analyzer)
    
    # save the file
    output_file = _get_output_name(input_file)
    workbook.save(output_file)

    print(f"Output file: {output_file}\n")

    return 0

if __name__ == '__main__':
    sys.exit(run_analysis(sys.argv))


