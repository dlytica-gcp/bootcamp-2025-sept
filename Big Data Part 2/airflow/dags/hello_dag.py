from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
 
def print_hello():
    print("Hello this is the beginning of the dag")
 
# Define the DAG
with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 9, 26),
    schedule_interval="@daily",  # Runs once a day
    catchup=False,
    tags=["example_bootcamp"],
) as dag:
 
    # Define a task
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )
 
    hello_task