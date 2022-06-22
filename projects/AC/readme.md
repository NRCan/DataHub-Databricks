# Acquisition Card Processing Tool

## Requirements
- The tool will only works with .xlsx files. Before using the tool, the original file must be saved as a .xlsx file type.
- The Excel workbook must have the following sheets with the exact names or the tool will not generate the Results file: Master, FPS CHs, RCM CHs, DSC. 

## Usage - Drag and drop file
- Just drag and drop the Excel file on top of the ac_analysis.exe and the app will process the file.
- A new file, appended with "_results", will be generated in the same folder as the original Excel file.

## Updating the Python Code
### To add a new Rule:
- Find (Ctrl + F) "filter rules - start" (line# 245 )
``` 
###### filter rules - start ######

def mcc_9399(row: ACRow, context: ACContext) -> bool:
    """ Restricted IS/OGD transactions => column O = 9399 """
    return row[BT_MCC_CODE] == '9399'

...
``` 
- Create a new rule function (after the last rule)
```
def new_rule_name(row: ACRow, context: ACContext) -> bool:
    <rule code>

```
- Add a new Color (Ctrl + F) "_get_highlight_colors" (Line 375)
``` 

def _get_highlight_colors() -> dict:
    return {
        ...
        "Gold": _get_pattern("FFD700"),
        "NewColor": _get_pattern("FFD755"),
    }

```
- Add it to the list in the function (Ctrl + F) "get_filters" (line# 311)

```
def get_filters():
    filters = [
        ...
        Filter(name="<new rule name>", rule=new_rule_name, color="NewColor")
    ]
    return filters
```

### 2. Check the Columns match the names (line# 24-31). 

```
CARDHOLDER    = "B"
MERCHANT_NAME = "C"
DOC_DESC      = "D"
TRANS_DATE    = "E"
AMMOUNT       = "F"
BT_MCC_CODE   = "O"
SAKNR_GL_ACCT = "AF"
CRITERIA_COL  = "AJ"  

```
