import sys
from unittest import main, TestCase
from ac_analysis import run_analysis

class TestACAnalysis(TestCase):
    
    def test_analysis(self):
        run_analysis([sys.argv[0], "./dist/AC May 2022 as of July 13_2022 (test).xlsx"])

if __name__ == '__main__':
    main()
