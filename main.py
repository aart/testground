from googledataload import connect
import unittest

class Test(unittest.TestCase):
    def test_upper(self):
        self.assertUpper('aart'.upper(), 'AART')


#connect.step_1_query_and_export()
#connect.step_2a_transfer_to_lake()
unittest.main()