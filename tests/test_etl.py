import unittest
import pandas as pd
from io import StringIO

class TestTransformation(unittest.TestCase):
    
    def test_aggregation(self):
        # 1. Create dummy raw data
        csv_data = """product_id,category,amount,date
        1,Electronics,100.0,2023-10-01
        1,Electronics,120.0,2023-10-01
        2,Home,50.0,2023-10-01"""
        
        df = pd.read_csv(StringIO(csv_data))

        # 2. Simulate the logic from our main script
        result = df.groupby('category')['amount'].sum().reset_index()
        
        # 3. Check if Electronics total is 220.0 (100 + 120)
        electronics_sales = result[result['category'] == 'Electronics']['amount'].values[0]
        self.assertEqual(electronics_sales, 220.0)

if __name__ == '__main__':
    unittest.main()