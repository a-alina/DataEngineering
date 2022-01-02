from airflow import DAG
import airflow
import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from python_tasks_injection import tag_create, insert, main_table, rel, url, reference, image_data, tags

default_args_dict = {
    'owner': 'alina',
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}


injection = DAG(
    dag_id='injection',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow-docker/dags/']
)


task_one = DummyOperator(
    task_id = 'start',
    dag=injection,

)

task_two = PythonOperator(
    task_id = 'main_fact_table',
    dag=injection,
    python_callable=main_table,
    trigger_rule='all_success'

)


task_three = PostgresOperator(
    task_id='main_fact_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='main.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_four = PythonOperator(
    task_id = 'relationship_table',
    dag=injection,
    python_callable=rel,
    trigger_rule='all_success'

)


task_five = PostgresOperator(
    task_id='relationship_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='rel.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_six = PythonOperator(
    task_id = 'url_table',
    dag=injection,
    python_callable=url,
    trigger_rule='all_success'

)


task_seven = PostgresOperator(
    task_id='url_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='url.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_eight = PythonOperator(
    task_id = 'reference_sites_table',
    dag=injection,
    python_callable=reference,
    trigger_rule='all_success'

)


task_nine = PostgresOperator(
    task_id='reference_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='reference_sites.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_ten = PostgresOperator(
    task_id='reference_combinations_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='reference_combinations.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_eleven = PythonOperator(
    task_id = 'image_data_table',
    dag=injection,
    python_callable=image_data,
    trigger_rule='all_success'

)


task_twelve = PostgresOperator(
    task_id='image_data_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='image_data.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_thirteen = PythonOperator(
    task_id = 'tags_table',
    dag=injection,
    python_callable=tags,
    trigger_rule='all_success'

)


task_fourteen = PostgresOperator(
    task_id='tags_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='tags.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_fifteen = PostgresOperator(
    task_id='tags_combinations_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='tags_combinations.sql',
    trigger_rule='all_success',
    autocommit=True

)


task_one >> task_two >> task_three >> task_four >> task_five >> task_six >> task_seven >> task_eight >> task_nine >> task_ten >> task_eleven >> task_twelve >> task_thirteen >> task_fourteen >> task_fifteen

