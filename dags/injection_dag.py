from airflow import DAG
import airflow
import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from python_tasks_injection import  \
main_table, rel, url, reference, image_data, tags

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

task_1 = DummyOperator(
    task_id = 'start',
    dag=injection,

)

task_2 = PythonOperator(
    task_id = 'main_fact_table',
    dag=injection,
    python_callable=main_table,
    trigger_rule='all_success'

)

task_3 = PostgresOperator(
    task_id='main_fact_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='main.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_4 = PythonOperator(
    task_id = 'relationship_table',
    dag=injection,
    python_callable=rel,
    trigger_rule='all_success'

)

task_5 = PostgresOperator(
    task_id='relationship_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='rel.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_6 = PythonOperator(
    task_id = 'url_table',
    dag=injection,
    python_callable=url,
    trigger_rule='all_success'

)

task_7 = PostgresOperator(
    task_id='url_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='url.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_8 = PythonOperator(
    task_id = 'reference_sites_table',
    dag=injection,
    python_callable=reference,
    trigger_rule='all_success'

)


task_9 = PostgresOperator(
    task_id='reference_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='reference_sites.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_10 = PostgresOperator(
    task_id='reference_combinations_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='reference_combinations.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_11 = PythonOperator(
    task_id = 'image_data_table',
    dag=injection,
    python_callable=image_data,
    trigger_rule='all_success'

)

task_12 = PostgresOperator(
    task_id='image_data_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='image_data.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_13 = PythonOperator(
    task_id = 'tags_table',
    dag=injection,
    python_callable=tags,
    trigger_rule='all_success'

)

task_14 = PostgresOperator(
    task_id='tags_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='tags.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_15 = PostgresOperator(
    task_id='tags_combinations_sql',
    dag=injection,
    postgres_conn_id='postgres',
    sql='tags_combinations.sql',
    trigger_rule='all_success',
    autocommit=True

)

task_16 = DummyOperator(
    task_id = 'end',
    dag=injection,
)


task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> \
task_6 >> task_7 >> task_8 >> task_9 >> task_10 >> \
task_11 >> task_12 >> task_13 >> task_14 >> \
task_15 >> task_16

