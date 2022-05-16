from dataclasses import dataclass
from unittest import main, TestCase
from ac_processor_utils import ACRow, get_letter_index, get_column_index

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


if __name__ == '__main__':
    main()
