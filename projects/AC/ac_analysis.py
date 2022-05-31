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

def _get_pattern(rgb: str) -> PatternFill:
    return PatternFill(start_color=rgb, end_color=rgb, fill_type="solid")

def _get_highlight_colors() -> dict:
    return {
        "Cornsilk": _get_pattern("FFF8DC"),
        "NavajoWhite": _get_pattern("FFDEAD"), 
        "Salmon": _get_pattern("FA8072"), 
        "DarkSalmon": _get_pattern("E9967A"),
        "RosyBrown": _get_pattern("BC8F8F"),
        "Pink": _get_pattern("FFC0CB"),
        "HotPink": _get_pattern("FF69B4"),
        "Coral": _get_pattern("FF7F50"),
        "OrangeRed": _get_pattern("FF4500"),
        "DarkSeaGreen": _get_pattern("8FBC8F"),
        "Aqua": _get_pattern("00FFFF"),
        "Violet": _get_pattern("EE82EE"),
        "Olive": _get_pattern("808000"),
        "Gold": _get_pattern("FFD700")
    }

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

def _get_context(workbook) -> ACContext:

    fps_chs = _load_fps_chs(workbook["FPS CHs"])
    rcm_chs = _load_rcm_chs(workbook["RCM CHs"])
    dsc_list = _load_dsc_list(workbook["DSC"])
    colors = _get_highlight_colors()

    return ACContext(fps_cardholders=fps_chs, rcm_cardholders=rcm_chs, dsc_list=dsc_list, 
        highlight_colors=colors, result_column="AI", spend_max=10000.0)

def _validate_sheets(names: list[str]) -> bool:
    return "Master" in names and "FPS CHs" in names and "RCM CHs" in names and "DSC" in names

def run_analysis(argvs):

    if len(argvs) != 2:
        _, tail = os.path.split(argvs[0])
        print(f'Usage:\n\t{tail} <excel_file_name>')
        return 0

    input_file = argvs[1]

    print(f"Loading file...")

    workbook = load_workbook(input_file, read_only=False, data_only=True)

    print(f"Validating sheets...")

    if not _validate_sheets(workbook.sheetnames):
        print("One of these sheets was not found: 'Master', 'FPS CHs', 'RCM CHs', 'DSC'.\nPlease rename the sheets to be able to run the analysis.")
        return 0

    master_sheet = workbook["Master"]
    
    print(f"Analysing...")

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

    print(f"Highlighting rows...")

    # highlight the rows
    analyzer.highlight_rows()

    #print(analyzer)
    
    # save the file
    output_file = _get_output_name(input_file)
    workbook.save(output_file)

    print(f"Process completed...\nOutput file: {output_file}\n")

    return 0

if __name__ == '__main__':
    sys.exit(run_analysis(sys.argv))
