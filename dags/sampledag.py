from airflow import DAG
from datetime import datetime
from operators.livysessionsoperator import CustomSessionLivyOperator
from airflow.operators.python import PythonOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

def _notify(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids="livy_spark")
    print("the result  is ", result)


# Instantiate the DAG object
with DAG(
    'LivySessionAPI_With_Fabric',
    default_args=default_args,
    description='A DAG to run dynamic Spark against Fabric Livy',
    schedule_interval=None,
    catchup=False
) as dag:

    livy_spark = CustomSessionLivyOperator(
        task_id="livy_spark",
        fabric_conn_id = "fabric",
        workspace_id="726a73e3-4abe-43ab-b58d-b4500aacab8c",
        item_id="c2250ec5-e27d-4bff-a248-63254f0472cc",
        command="spark.sql(\"SELECT * FROM lhJohn.green_tripdata_2022 where total_amount > 0\").show()",
        dag=dag,
    )

    notify = PythonOperator(
       task_id="notify",
       python_callable=_notify,
       dag=dag,
    )

    livy_spark >> notify
