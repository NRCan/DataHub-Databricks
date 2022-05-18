""" CA Processor Rules """

from dataclasses import dataclass
from typing import Callable
from ac_analysis_utils import ACRow, ACContext, get_selected_transactions, get_valid_splits, update_splits

class Filter:
    name: str
    rule: Callable
    rows: list

    def __init__(self, name: str, rule: Callable) -> None:
        self.name = name
        self.rule = rule
        self.rows = list()

###### filter rules - start ######
def mcc_9399(row: ACRow, context: ACContext) -> bool:
    """ Restricted IS/OGD transactions => column N = 9399 """
    return row["N"] == '9399'

def restricted_travel_expense(row: ACRow, context: ACContext) -> bool:
    """ Restricted - Travel Status Related Expenses """
    return row["N"] in ['7011', '4111', '7513'] or row["AE"] in ["54260", "54206", "54208"]

def mcc_7932(row: ACRow, context: ACContext) -> bool:
    """ Event/Hospitality => column N = 7932 """
    return row["N"] == '7932'

def ta_limit_exceeded(row: ACRow, context: ACContext) -> bool:
    """ TA Limit exceeded => column E > 5000 """
    return row["E"] > 5000.00

def empty_cheques(row: ACRow, context: ACContext) -> bool:
    """ Monitoring of Convenience Cheques => column B = CHEQUE and E is empty """
    return row["B"] == "CHEQUE" and row["E"] is None

def gl_financial_coding_error(row: ACRow, context: ACContext) -> bool:
    """ Financial Coding Error - Designated Science Conferences => column AE = 52334 or 52136 and B in DSC list"""
    return row["AE"] in ['52334', '52136'] and context.in_dsc_list(row["B"])

def gl_conference_fees(row: ACRow, context: ACContext) -> bool:
    """ Conference fees => column AE = 52334 or 52136 and B not in DSC list """
    return row["AE"] in ['52334', '52136'] and not context.in_dsc_list(row["B"])

def gl_hospitality(row: ACRow, context: ACContext) -> bool:
    """ Hospitality => column AE = 52204, 52206, 52207, 54211 """
    return row["AE"] in ['52204', '52206', '52207', '54211']

def gl_restricted_memberships(row: ACRow, context: ACContext) -> bool:
    """ Restricted Memberships => column AE = 58208, 52336, 52337 """
    return row["AE"] in ['58208', '52336', '52337']

def gl_restricted_motor_vehicles(row: ACRow, context: ACContext) -> bool:
    """ Restricted Motor Vehicles Expenses => column AE = 53332, 53334, 54285, 54287 54000, 54002, 54004  """ 
    return row["AE"] in ["53332", "53334", "54285", "54287" "54000", "54002", "54004"]

def gl_restricted_home_equipment_purchases(row: ACRow, context: ACContext) -> bool:
    """ Restricted Home Equipment Purchases => AE = 54260 and E >= $800 and CH not in FPS-CHs list """
    return row["AE"] == "54260" and row["E"] >= 800.00 and row["A"] not in context.fps_cardholders

def rcm_cardholders_vlookup(row: ACRow, context: ACContext) -> bool:
    """ SoD - TA and s.34 => CH in RCM-CHs list """
    return row["A"] in context.rcm_cardholders

###### filter rules - end ######

def get_filters():
    filters = [
        Filter(name="Restricted IS/OGD transactions", rule=mcc_9399), 
        Filter(name="Restricted Travel Status Related Expenses", rule=restricted_travel_expense),
        Filter(name="Event/Hospitality", rule=mcc_7932),
        Filter(name="TA Limit exceeded", rule=ta_limit_exceeded),
        Filter(name="Monitoring of Convenience Cheques", rule=empty_cheques),
        Filter(name="Financial Coding Error - Designated Science Conferences", rule=gl_financial_coding_error),
        Filter(name="Conference fees", rule=gl_conference_fees),
        Filter(name="Hospitality", rule=gl_hospitality),
        Filter(name="Restricted Memberships", rule=gl_restricted_memberships),
        Filter(name="Restricted Home Equipment Purchases", rule=gl_restricted_home_equipment_purchases),
        Filter(name="Restricted Motor Vehicles Expenses", rule=gl_restricted_motor_vehicles),
        Filter(name="SoD - TA and s.34", rule=rcm_cardholders_vlookup)
    ]
    return filters

class Analizer:

    filters: list[Filter]
    context: ACContext
    cardholders: dict

    def __init__(self, context: ACContext) -> None:
        self.filters = get_filters()
        self.context = context
        self.cardholders = {}

    def analize(self, row: ACRow):
        
        # add row to all filters that match
        for f in self.filters:
            if f.rule(row, self.context):
                f.rows.append(row)    

        # update splits
        if row["A"] not in self.context.fps_cardholders:
            update_splits(self.cardholders, row)

    def highlight_rows(self):

        # highlight rules
        self.highlight_filters()
        
        # highlight splits
        self.highlight_splits()

    def highlight_filters(self):
        for f in self.filters:
            color = self.context.highlight_colors[f.name]
            for r in f.rows:
                r.highlight(color, self.context.result_column, f.name)

    def highlight_splits(self):
        
        potential_splits = "Potential Splits"
        color = self.context.highlight_colors[potential_splits]

        splits = get_valid_splits(self.cardholders, 5000.0)
        trans = get_selected_transactions(splits)

        for t in trans:
            t.row.highlight(color, self.context.result_column, potential_splits)

    def __str__(self) -> str:
        return "\n".join(map(lambda f: f"{f.name}: {len(f.rows)}", self.filters))
        