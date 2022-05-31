import sys
from unittest import main, TestCase
from ac_analysis import run_analysis

class TestACAnalysis(TestCase):
    
    def test_analysis(self):
        run_analysis([sys.argv[0], "./data/AC_sample.xlsx"])

if __name__ == '__main__':
    main()
