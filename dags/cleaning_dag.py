from airflow import DAG
import airflow
import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from python_tasks import convert, non_memes, dropping, \
    date_conv, feature_extraction, list_clean


default_args_dict = {
    'owner': 'alina',
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

cleaning = DAG(
    dag_id='cleaning',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow-docker/dags/']
)

task_1 = BashOperator(
    task_id='get_data',
    dag=cleaning,
    bash_command='curl https://owncloud.ut.ee/owncloud/index.' + \
        'php/s/g4qB5DZrFEz2XLm/download/kym.json --output ' + \
        '/opt/airflow/dags/kym.json',
)

task_2 = PythonOperator(
    task_id ='convert_to_csv',
    dag=cleaning,
    python_callable=convert,
    trigger_rule='all_success'
    
)

task_3 = DummyOperator(
    task_id='cleaning',
    dag=cleaning,
    trigger_rule='none_failed'
)

task_4 = PythonOperator(
    task_id='removing_non_memes',
    dag=cleaning,
    python_callable=non_memes
)

task_5 = PythonOperator(
    task_id='convert_datetime',
    dag=cleaning,
    python_callable=date_conv,
    trigger_rule='all_success'
)

task_6 = PythonOperator(
    task_id='dropping_features',
    dag=cleaning,
    python_callable=dropping,
    trigger_rule='all_success'
)

task_7 = PythonOperator(
    task_id='feature_extraction',
    dag=cleaning,
    python_callable=feature_extraction,
    trigger_rule='all_success'
)

task_8 = PythonOperator(
    task_id='mapping_list_elemets',
    dag=cleaning,
    python_callable=list_clean,
    trigger_rule='all_success'
)

task_1 >> task_2 >> task_3 >> task_4 >> \
task_5 >> task_6 >> task_7 >> task_8
