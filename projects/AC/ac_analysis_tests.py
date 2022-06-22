import sys
from unittest import main, TestCase
from ac_analysis_tool import run_analysis

class TestACAnalysis(TestCase):
    
    def test_analysis(self):
        run_analysis([sys.argv[0], "./dist/April AC (Cesar).xlsx"])

if __name__ == '__main__':
    main()
