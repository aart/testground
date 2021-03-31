from googledataload import transfer
from googledataload import connect
import unittest

class Test(unittest.TestCase):
    def test_upper(self):
        self.assertEqual('aart'.upper(), 'AART', 'IS NOT OK')


#transfer.step_1_query_and_export()
#transfer.step_2a_transfer_to_lake()
unittest.main()
