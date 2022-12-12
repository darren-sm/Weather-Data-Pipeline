from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago



def print_hello():
    print("Hello World from Python")

with DAG(
    dag_id='first_sample_dag',
    start_date=days_ago(1),
    schedule_interval="@daily"
) as dag:

    python_hello = PythonOperator(
        task_id='start',
        python_callable=print_hello
    )

    bash_hello = BashOperator(
        task_id='print_hello_world',
        bash_command='echo "HelloWorld from Bash"'
    )

python_hello >> bash_hello
