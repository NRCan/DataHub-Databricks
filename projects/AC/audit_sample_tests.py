from unittest import main, TestCase
from audit_sample import is_column_letter, expression_column_mapper, build_test_expression, test_row
from dataclasses import dataclass

@dataclass
class MockRow:
    value: object

class TestAuditSample(TestCase):
    
    def test_is_column_letter_must_pass(self):
        self.assertTrue(is_column_letter('A'))

    def test_is_column_letter_must_not_pass(self):
        self.assertFalse(is_column_letter('1'))

    def test_expression_column_mapper_samples(self):
        for l in range(ord('A'), ord('Z') + 1):
            expected = "row[{}].value".format(l - ord('A'))
            self.assertEqual(expression_column_mapper(chr(l)), expected)    

        self.assertEqual(expression_column_mapper("a"), "a")    
        self.assertEqual(expression_column_mapper("1"), "1")    
        self.assertEqual(expression_column_mapper(">"), ">")  

    def test_build_test_expression(self):
        expected = "row[0].value > 100 and row[6].value <= 200" 
        actual = build_test_expression("A > 100 and G <= 200") 
        self.assertEqual(actual, expected)

    def test_test_row(self):
        row = [MockRow(value=100), MockRow(value=100.5), MockRow(value=350), MockRow(value="error")]
        self.assertTrue(test_row("row[0].value >= 100", row))
        self.assertTrue(test_row("row[1].value > 100", row))
        self.assertTrue(test_row("row[0].value > 50 and row[2].value < 400", row))
        self.assertFalse(test_row("row[1].value < 100", row))

    def test_test_string_expression(self):
        row = [MockRow(value='Sales')]
        expr = build_test_expression("A == 'Sales'") 
        self.assertTrue(test_row(expr, row))

    def test_test_row_corrupted_data(self):    
        row = [MockRow(value="error")]
        self.assertEqual(test_row("row[0].value < 100", row), None)

    def test_compile(self):
        code = "A > 100 and B == 'test string'"
        result = compile(code, "<string>", "eval")
        self.assertTrue(result)

if __name__ == '__main__':
    main()
