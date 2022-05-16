""" AC processor utility function and classes """

from dataclasses import dataclass
from typing import Any

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


@dataclass()
class ACRow:
    """ Wrapper class for Excel rows """

    row: Any

    def __getitem__(self, column):
        index = get_column_index(column)
        return self.row[index - 1].value

@dataclass()
class ACContext:
    fps_cardholders: set[str]
    rcm_cardholders: set[str]
