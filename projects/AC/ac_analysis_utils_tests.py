from dataclasses import dataclass
from unittest import main, TestCase
from datetime import date
from ac_analysis_utils import ACRow, Split, Transaction, get_letter_index, get_column_index, get_valid_splits, update_splits

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

        update_splits(cardholders, ACRow(row=[MockRow(value="CH1"), MockRow(value="MCH1"), MockRow(value="Desc1"), MockRow(value=date(2022, 3, 24)), MockRow(value=4100.01)]))
        update_splits(cardholders, ACRow(row=[MockRow(value="CH1"), MockRow(value="MCH1"), MockRow(value="Desc1"), MockRow(value=date(2022, 3, 24)), MockRow(value=1200.01)]))
        update_splits(cardholders, ACRow(row=[MockRow(value="CH1"), MockRow(value="MCH 12345"), MockRow(value="Desc 1"), MockRow(value=date(2022, 3, 24)), MockRow(value=2200.01)]))
        update_splits(cardholders, ACRow(row=[MockRow(value="CH1"), MockRow(value="MCH 12346"), MockRow(value="Desc 2"), MockRow(value=date(2022, 3, 24)), MockRow(value=3200.01)]))

        splits = get_valid_splits(cardholders, 5000.0)

        self.assertEqual(len(splits), 2)

    def test_split_more_than(self):
        row1 = ACRow(row=[MockRow(value="Me"), MockRow(value="Merchant"), MockRow(value="Desc1"), MockRow(value=date(2020, 5, 16)), MockRow(value=4100.01)])
        row2 = ACRow(row=[MockRow(value="Me"), MockRow(value="Merchant"), MockRow(value="Desc1"), MockRow(value=date(2020, 5, 16)), MockRow(value=4100.01)])

        split = Split(Transaction(row=row1))
        split.try_add(Transaction(row=row2), 2, 90)

        self.assertTrue(split.more_than(200))

if __name__ == '__main__':
    main()
