import unittest
from unittest.mock import patch, MagicMock
from msal import ConfidentialClientApplication
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from operators.livysessionsoperator import CustomSessionLivyOperator

tenant_id = os.getenv("TENANT_ID")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

if not all([tenant_id, client_id, client_secret]):
    raise ValueError("Missing required environment variables: TENANT_ID, CLIENT_ID, CLIENT_SECRET")

class TestMyCustomOperator(unittest.TestCase):

    @patch('operators.livysessionsoperator.FabricHook')  # Mock FabricHook
    def setUp(self,  mock_fabric_hook):
        # Mock the _get_token method to return a real token for testing outside of Fabric UI
        mock_fabric_hook.return_value._get_token.return_value = self.get_access_token()
     
        self.operator = CustomSessionLivyOperator(
            task_id='test_task',
            fabric_conn_id='fabric',
            workspace_id='726a73e3-4abe-43ab-b58d-b4500aacab8c',
            item_id='c2250ec5-e27d-4bff-a248-63254f0472cc',
            command='spark.sql("SELECT * FROM lhJohn.green_tripdata_2022 where total_amount > 0").show()',
            dag=None,
            
        )

    # test 1
    def test_initialization(self):
        self.assertEqual(self.operator.task_id, 'test_task')
        self.assertEqual(self.operator.workspace_id, '726a73e3-4abe-43ab-b58d-b4500aacab8c')
        self.assertEqual(self.operator.item_id, 'c2250ec5-e27d-4bff-a248-63254f0472cc')
        self.assertEqual(self.operator.fabric_conn_id, 'fabric')
        self.assertEqual(self.operator.command, 'spark.sql("SELECT * FROM lhJohn.green_tripdata_2022 where total_amount > 0").show()')
        # Add more assertions as needed for other parameters

    # test 2
    def test_execute(self):
        # this is the main entry point for the operator
        result = self.operator.execute(context={})
        print("Result of execute:", result)
        self.assertIn('text/plain', result)  # look for part of the SQL result
        pass

    # test 3
    def test_additional_method(self):
        # Test any additional methods you may have in CustomSessionLivyOperator
        pass

    # get an access token directly using MSAL
    def get_access_token(self):
        # Use ConfidentialClientApplication for private client
        app = ConfidentialClientApplication(
            client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}"
        )
        # Acquire token using client credentials flow
        result = app.acquire_token_for_client(scopes=["https://api.fabric.microsoft.com/.default"]); 
        if "access_token" in result:
            return result["access_token"]
        else:
            return "Failed to acquire access token."
        
if __name__ == '__main__':
    unittest.main()