# Auditing POC App

## Usage
audit_sample <file_path> <last_column_letter> <"filter_condition">

__Sample Sheet__
| A | B | C | D | E | F | G | I |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Header** | **Header** | **Header** | **Header** | **Header** | **Header** | **Header** | **Header** |
| data | data | data | data | data | data | data | data |
| data | data | data | data | data | data | data | data |
| data | data | data | data | data | data | data | data |

**file_path**: file name to process.

**last_column_letter**: last colum with data in the sheet, the program will modify the column after this one. For the sample sheet above must choose **I**, then the app will place the results in colum **J**. The data in the column J will be Yes. No or INVALID

**filter_condition**: condition used to highligh the rows, must be inside double-quotes. The language used to write the condition is Python-like, but don't worry if you are not familiar with programming, this is very simple and the following examples will explain how to do it.

__Sample Command Lines:__

- Highlight all rows where the value in colum G more than $1000.
audit_sample finances.xlsx I "G > 1000.00"

- Highlight all rows where the value in colum G is in the rage $1000 to $10,000.
audit_sample finances.xlsx I "G >= 1000.00 and G <= 10000.00"

- Highlight all rows where the value is exactly $5000 on colum D. Notice the equal sign is doubled in the condition. 
audit_sample finances.xlsx I "D == 1000.00"

- Highlight all rows where the value in colum D is greater than $50 and column E is equal to 'Meal'. 
audit_sample finances.xlsx I "D > 50 and E == 'Meal'"
