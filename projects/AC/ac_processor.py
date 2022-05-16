""" CA Processor Main """

import sys
import os
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from ac_processor_rules import apply_criterias
from ac_processor_utils import ACRow, ACContext

def _get_output_name(file_name: str):
    split = os.path.splitext(file_name)
    return f"{split[0]}_results{split[1]}"

def _highlight_row(row, color):
    for cell in row:
        cell.fill = color

def _load_fps_chs(sheet) -> set[str]:
    values = set()
    for row in sheet.iter_rows(min_row = 2, max_col = 2, max_row = sheet.max_row):
        if row[1].value:
            values.add(row[1].value.strip().upper())
    return values

def _load_rcm_chs(sheet) -> set[str]:
    values = set()
    for row in sheet.iter_rows(min_row = 2, max_col = 3, max_row = sheet.max_row):
        if row[2].value:
            values.add(row[2].value.strip().upper())
    return values

def _load_filter_colors(sheet) -> dict:
    colors = {}
    for row in sheet.iter_rows(min_row = 5, max_col = 3, max_row = sheet.max_row):
        cell = row[2]
        if cell.value:
            color = f"{cell.fill.start_color.rgb}" 
            colors[cell.value.strip()] = PatternFill(start_color=color, end_color=color, fill_type="solid")
    return colors

def _run_processor(argvs):
    if len(argvs) != 2:
        _, tail = os.path.split(argvs[0])
        print(f'Usage:\n\t{tail} <excel_file_name>')
        return 0

    input_file = argvs[1]

    workbook = load_workbook(input_file, read_only=False, data_only=True)
    fps_chs = _load_fps_chs(workbook["FPS CHs"])
    rcm_chs = _load_rcm_chs(workbook["RCM - CH Listing "]) #notice there is a space at the end in the sheet name

    context = ACContext(fps_cardholders=fps_chs, rcm_cardholders=rcm_chs)
    colors = _load_filter_colors(workbook["Filters"])

    master_sheet = workbook.worksheets[0]

    mod_rows = 0
    for excel_row in master_sheet.iter_rows(min_row = 2, max_col = master_sheet.max_column,
        max_row = master_sheet.max_row):

        row = ACRow(row=excel_row)
        if row["A"] is not None:
            criteria = apply_criterias(row, context) #result = apply_rules(row, context)
            if criteria is not None:
                pattern = colors[criteria.name]
                _highlight_row(excel_row, pattern)
                mod_rows += 1

    output_file = _get_output_name(input_file)
    workbook.save(output_file)

    print(f"Output file: {output_file}\nModified rows: {mod_rows}")

    return 0

def main():
    #sys.exit(run_processor(sys.argv))
    sys.exit(_run_processor([sys.argv[0], "./data/AC_sample.xlsx"]))

if __name__ == '__main__':
    main()
