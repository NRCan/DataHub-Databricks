from dataclasses import dataclass
from unittest import main, TestCase
from datetime import date
from ac_processor_utils import ACRow, get_letter_index, get_column_index, update_splits

@dataclass
class MockRow:
    value: object

class TestACProcessorUtilsModule(TestCase):
    
    def test_get_letter_index_returns_expected(self):
        self.assertEqual(get_letter_index('A'), 1)
        self.assertEqual(get_letter_index('Z'), 26)

    def test_get_column_index_returns_expected(self):
        self.assertEqual(get_column_index("AA"), 27)
        self.assertEqual(get_column_index("XY"), 649)

    def test_row_get_returns_expected(self):
        carow = ACRow(row=[MockRow(value=100), MockRow(value=100.5), MockRow(value=350), MockRow(value="error")])
        self.assertEqual(carow["A"], 100)
        self.assertEqual(carow["D"], "error")

    def test_group_transations(self):
        cardholders = {}

        update_splits(cardholders, ACRow(row=[MockRow(value="CH1"), MockRow(value="MCH1"), MockRow(value="Desc1"), MockRow(value=date(2022, 3, 24)), MockRow(value=100.01)]))
        update_splits(cardholders, ACRow(row=[MockRow(value="CH1"), MockRow(value="MCH1"), MockRow(value="Desc1"), MockRow(value=date(2022, 3, 24)), MockRow(value=200.01)]))
        update_splits(cardholders, ACRow(row=[MockRow(value="CH1"), MockRow(value="MCH 12345"), MockRow(value="Desc 1"), MockRow(value=date(2022, 3, 24)), MockRow(value=200.01)]))
        update_splits(cardholders, ACRow(row=[MockRow(value="CH1"), MockRow(value="MCH 12346"), MockRow(value="Desc 2"), MockRow(value=date(2022, 3, 24)), MockRow(value=200.01)]))

if __name__ == '__main__':
    main()
