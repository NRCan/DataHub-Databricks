""" AC processor utility function and classes """

from dataclasses import dataclass
from typing import Any
from datetime import date
from functools import lru_cache

def get_letter_index(letter):
    """ expects a capital letter in the range (A - Z) """

    return ord(letter) - ord('A') + 1


def get_column_index(column):
    """ expects an excel column index a string of capital letters """

    base = get_letter_index('Z')
    index = 0
    exp = 0
    for col in column[::-1]:
        index += get_letter_index(col) * (base ** exp)
        exp += 1
    return index

def levenshtein_distance(a, b):

    len_a = len(a)
    len_b = len(b)

    @lru_cache(None)
    def min_dist(s1, s2):

        if s1 == len_a or s2 == len_b:
            return len_a - s1 + len_b - s2

        if a[s1] == b[s2]:
            return min_dist(s1 + 1, s2 + 1)

        return 1 + min(
            min_dist(s1, s2 + 1),      # cost of insert
            min_dist(s1 + 1, s2),      # cost of delete
            min_dist(s1 + 1, s2 + 1),  # cost of replace
        )

    return min_dist(0, 0)

def levenshtein_distance_percent(a, b):
    a1 = a if a else ""
    b1 = b if b else ""
    max_len = max(1, len(a1), len(b1))
    dist = levenshtein_distance(a1, b1)
    return 100.0 * ((max_len - dist) / max_len)

@dataclass()
class ACRow:
    """ Wrapper class for Excel rows """

    row: Any
    index: int = 0

    def __getitem__(self, column):
        index = get_column_index(column)
        return self.row[index - 1].value

@dataclass()
class ACContext:
    fps_cardholders: set[str]
    rcm_cardholders: set[str]


@dataclass()
class Transaction:
    date: date 
    merchant: str
    desc: str = ''
    total: float = 0.0
    index: int = 0

class Split:
    transactions: list[Transaction]
    index: int = 0

    def __init__(self, trans: Transaction) -> None:
        self.transactions = [trans]
        self.index = trans.index

    def try_add(self, trans: Transaction, max_days: int, similarity: float) -> bool:
        # compare with the first transaction
        master = self.transactions[0]

        # compare dates
        delta_day = (trans.date - master.date).days
        if delta_day > max_days:
            return False

        # compare merchants
        if levenshtein_distance_percent(master.merchant, trans.merchant) < similarity:
            return False

        # compare descriptions
        if levenshtein_distance_percent(master.desc, trans.desc) < similarity:
            return False

        self.transactions.append(trans)
        
        return True

    def more_than(self, value: float):
        s = 0
        for t in self.transactions:
            s += t.total
        return s > value #return value < sum(map(lambda t: Transaction -> t.total, self.transactions), 0)

    def is_multiple(self) -> bool:
        return len(self.transactions) > 1


class CardHolder:
    
    name: str
    splits: list[Split]

    def __init__(self, name) -> None:
        self.name = name
        self.splits = []

    def add(self, trans: Transaction) -> bool:
        
        for split in self.splits:
            if split.try_add(trans, 2, 60.0):
                return False
        
        self.splits.append(Split(trans))

        return True

    def get_valid_splits(self, min_value: float) -> list[Split]:
        
        filtered = []

        for s in self.splits:
            if s.is_multiple() and s.more_than(min_value):
                filtered.append(s)

        return filtered


def update_splits(cardholders: dict[CardHolder], row: ACRow):

    ch_name = row["A"]
    transaction = Transaction(index=row.index, merchant=row["B"], desc=row["C"], date=row["D"], total=row["E"])

    if ch_name not in cardholders:
        cardholder = CardHolder(ch_name)
        cardholders[ch_name] = cardholder
    else:
        cardholder = cardholders[ch_name]

    cardholder.add(transaction)
    
def get_valid_splits(cardholders: dict[CardHolder], min_value: float) -> list[Split]:
    
    result = []
    for key in cardholders:
        result.extend(cardholders[key].get_valid_splits(min_value))

    return sorted(result, key=lambda s: s.index)

def get_selected_rows(splits: list[Split]) -> set[int]:
    result = set()
    for s in splits:
        result.update(map(lambda t: t.index, s.transactions))
    return result
