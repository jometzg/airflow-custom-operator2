from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MyCustomOperator(BaseOperator):
    @apply_defaults
    def __init__(self, param1, param2, *args, **kwargs):
        super(MyCustomOperator, self).__init__(*args, **kwargs)
        self.param1 = param1
        self.param2 = param2

    def execute(self, context):
        # Logic to be executed when the operator runs
        result = self.param1 + self.param2  # Example logic
        return result

    def helper_method(self):
        # Additional helper method for the operator's functionality
        pass