from googledataload import transfer
import unittest

class Test(unittest.TestCase):
    def test_upper(self):
        self.assertUpper('aart'.upper(), 'AART')


#transfer.step_1_query_and_export()
#transfer.step_2a_transfer_to_lake()
unittest.main()