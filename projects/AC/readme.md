# Acquisition Card Processing Tool

## Requirements
- The tool will only works with **.xlsx** files. Before using the tool, the original file must be saved as a **.xlsx** file type.
- The Excel workbook must have the following sheets with the exact names or the tool will not generate the Results file: **Master**, **FPS CHs**, **RCM CHs**, **DSC**. 
- The following Column Headers **MUST** match exactly the listed below: 

| Header Description | Exact Column Header Name |
--- | --- | ---
Card Holder | CARDHOLDER | 
Mechant Name | MERCHANT_NAME |
Document Description | DOC_DESC |
Transaction Date | TRANS_DATE |
Transaction Ammount | AMOUNT
Merchant Code | BT_MCC_CODE |
GL Account | SAKNR_GL_ACCT |

## Usage - Drag and drop file
- Just drag and drop the Excel file on top of the **ac_analysis.exe** and the app will process the file.
- A new file, appended with **_results**, will be generated in the same folder as the original Excel file.

## Updating the Python Code

### Changing the dollar value of an existing threshold 
- In this example we are updating the maximun spend threshold amount from 5000.00 to 10000.00.
- In the python script, use Ctrl+F for the search function and type **SPEND_MAX =** 
- Once the SPEND_MAX line of code is found, modify the amount by replacing the value. 

Original threshold amount:
``
SPEND_MAX = 5000.00
``
New threshold amount:
``
SPEND_MAX = 10000.00
``
- Use Ctrl+S to save the python script.
- You **MUST** compile the AC tool again. (See instructions at the end).

### To add a new Rule:
- In the python script, use Ctrl+F for the search function and type **filter rules - start** to go to the filter rules section.
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
- Use Ctrl + F and enter **_get_highlight_colors**.
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
- Use Ctrl + F and enter **get_filters**
- Add the following **Filter(name="new_rule_desc", rule=new_rule_name, color="NewColor")** replacing: **new_rule_desc**, **new_rule_name** and **NewColor** with the newly created values.

```
def get_filters():
    filters = [
        ...
        Filter(name="new_rule_desc", rule=new_rule_name, color="NewColor")
    ]
    return filters
```

### Add a new GL code to a Rule

- In this example we are adding a new GL code (54208) to the "Restricted Home Equipment" rule.
- In the python script, use Ctrl+F for the search function and type **Restricted Home Equipment**  
- You will first find the line of code where the rule is defined:
``
    Filter(name="Restricted Home Equipment Purchases", rule=restricted_home_equipment_purchases, color="Olive"),
``
- Double-click to select the Python function name located to the right of **rule=**. In this example the function name is **restricted_home_equipment_purchases**. 
- Then select F12 to navigate to the Python function where the rule is implemented.
```
    def restricted_home_equipment_purchases(row: ACRow, context: ACContext) -> bool:
        
        gls = ["54260", "54206"]
        mcc = ["5021", "5712", "5719", "7641"]

```
- Modify **gls = ["54260", "54206"]** to include the new code 54208: 

Original gls:
```
        gls = ["54260", "54206"]
```

Modified gls:
```
        gls = ["54260", "54206", "54208"]
```
- Use Ctrl+S to save the python script.
- You **MUST** compile the AC tool again. (See instructions at the end).

### Delete a GL code from a Rule

- In this example we are deleting a GL code (54208) to the "Restricted Home Equipment" rule.
- In the python script, use Ctrl+F for the search function and type **Restricted Home Equipment**  
- You will first find the line of code where the rule is defined:
``
    Filter(name="Restricted Home Equipment Purchases", rule=restricted_home_equipment_purchases, color="Olive"),
``
- Double-click to select the Python function name located to the right of **rule=**. In this example the function name is **restricted_home_equipment_purchases**. 
- Then select F12 to navigate to the Python function where the rule is implemented.
```
    def restricted_home_equipment_purchases(row: ACRow, context: ACContext) -> bool:
        
        gls = ["54260", "54206", "54208"]
        mcc = ["5021", "5712", "5719", "7641"]

```
- Modify **gls = ["54260", "54206", "54208"]** to delete the "54208" code: 

Original gls:
```
        gls = ["54260", "54206", "54208"]
```

Modified gls:
```
        gls = ["54260", "54206"]
```
- Use Ctrl+S to save the python script.
- You **MUST** compile the AC tool again. (See instructions at the end).

### Install PyInstaller

**Note:** To build the ac_analysis.exe, you will need **PyInstaller** which bundles the code into an executable file (.exe). By default **PyInstaller** is not installed in Python, so you will have to do a one-time installation. 

If you already have **PyInstaller** installed, go to the **Compile the ac_analysis.exe tool** instructions below.

- Open a New Terminal in Visual Studio Code by selecting the "Terminal" menu and then the "New Terminal" option.
- Type the following command:
```
pip install pyinstaller
```
- When the command finishes running, you can close the Terminal window.

### Compile the ac_analysis.exe tool
- Ensure you have **PyInstaller** installed. If you do not, see the **Install PyInstaller** instructions above. 
- Open a New Terminal in Visual Studio Code by selecting the "Terminal" menu and then the "New Terminal" option.
- Type the following command:
```
pyinstaller -F .\ac_analysis.py
```
- When the command finishes running, do not close the Terminal and now run the following command:
```
copy .\dist\ac_analysis.exe
```
- Your new ac_analysis.exe tool is ready to use and located in the same folder as the ac_analysis.py script.
