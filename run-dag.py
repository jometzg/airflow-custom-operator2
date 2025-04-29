from airflow.models import DagBag

# need some work to get going against Fabric
# This script is used to run a specific DAG task for debugging purposes.
# It loads the DAG, retrieves a specific task, and executes it with a given execution date.
# This is useful for testing and debugging the task in isolation without running the entire DAG.
# Make sure to set the path to your DAG folder correctly
# and adjust the task ID as needed.

# Load your DAG
dag_bag = DagBag(dag_folder="/home/jometzg/airflow-custom-operator/dags", include_examples=False)
dag = dag_bag.get_dag("LivySessionAPI_With_Fabric")

# Pick a task to debug
task = dag.get_task("livy_spark")

# Create a context and run the task
from airflow.utils.state import State
from airflow.utils.dates import days_ago

execution_date = days_ago(0)
task.run(start_date=execution_date, end_date=execution_date, ignore_ti_state=True)
