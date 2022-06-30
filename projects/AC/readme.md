# Acquisition Card Processing Tool

## Requirements
- The tool will only works with **.xlsx** files. Before using the tool, the original file must be saved as a **.xlsx** file type.
- The Excel workbook must have the following sheets with the exact names or the tool will not generate the Results file: **Master**, **FPS CHs**, **RCM CHs**, **DSC**. 
- The Excel column letters MUST match the following header names: 

```
    B  = CARDHOLDER
    C  = MERCHANT_NAME
    D  = DOC_DESC
    E  = TRANS_DATE
    F  = AMOUNT
    O  = BT_MCC_CODE
    AF = SAKNR_GL_ACCT
    AJ = CRITERIA_COL
```

## Usage - Drag and drop file
- Just drag and drop the Excel file on top of the **ac_analysis.exe** and the app will process the file.
- A new file, appended with **_results**, will be generated in the same folder as the original Excel file.

## Updating the Python Code
### To add a new Rule:
- Use Ctrl+F and enter **filter rules - start** to go to the filter rules section (line 240 of the code file named: **ac_analysis_tool.py**).
``` 
###### filter rules - start ######

def mcc_9399(row: ACRow, context: ACContext) -> bool:
    """ Restricted IS/OGD transactions => column O = 9399 """
    return row[BT_MCC_CODE] == '9399'

...
``` 
- Add a new rule function after the last rule.
```
def new_rule_name(row: ACRow, context: ACContext) -> bool:
    <rule code>

```
- Next add a new highlight color for the new rule.
- Use Ctrl + F and enter **_get_highlight_colors** (line 375 of the code).
- Create a new color pattern at the end of the list.
``` 

def _get_highlight_colors() -> dict:
    return {
        ...
        "Gold": _get_pattern("FFD700"),
        "NewColor": _get_pattern("FFD755"),
    }

```
- Next define the filter for the new rule.
- Use Ctrl + F and enter **get_filters**  (line 293 of the code).
- Add the following **Filter(name="new_rule_desc", rule=new_rule_name, color="NewColor")** replacing: **new_rule_desc**, **new_rule_name** and **NewColor** with the newly created values.

```
def get_filters():
    filters = [
        ...
        Filter(name="new_rule_desc", rule=new_rule_name, color="NewColor")
    ]
    return filters
```
