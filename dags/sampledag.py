from datetime import datetime
from airflow import DAG

 # Import from private package
from custom_operator.my_custom_operator import MyCustomOperator

# test dag
with DAG(
"test-custom-package",
tags=["example"],
description="A simple tutorial DAG",
schedule_interval=None,
start_date=datetime(2025, 1, 1),
) as dag:
    task = MyCustomOperator(task_id="sample-task",  param1="hello", param2="team",)

    task
