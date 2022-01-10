from airflow import DAG
import airflow
import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from python_tasks_cypher import node_mapper, edge_mapper, run_analysis

default_args_dict = {
    'owner': 'mait',
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

cypher = DAG(
    dag_id='cypher',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow-docker/dags/']
)

task_1 = DummyOperator(
    task_id = 'start',
    dag=cypher,

)

task_2 = PythonOperator(
    task_id = 'load_in_nodes',
    dag=cypher,
    python_callable=node_mapper,
    trigger_rule='all_success'

)

task_3 = PythonOperator(
    task_id = 'load_in_edges',
    dag=cypher,
    python_callable=edge_mapper,
    trigger_rule='all_success'

)

task_4 = PythonOperator(
    task_id = 'run_analysis',
    dag=cypher,
    python_callable=run_analysis,
    trigger_rule='all_success'

)


task_1 >> task_2 >> task_3 >> task_4
