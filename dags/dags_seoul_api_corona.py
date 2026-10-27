import pendulum

from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG

with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 세척제 제조업 현황'''
    wash_manu_task = SeoulApiToCsvOperator(
        task_id='wash_manu_task',
        dataset_nm='publicHygieneBizCleaningProducts',
        path='/opt/airflow/files/publicHygieneBizCleaningProducts/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='publicHygieneBizCleaningProducts.csv'
    )

    '''서울시 위생처리업 현황'''
    hygine_manu_task = SeoulApiToCsvOperator(
        task_id='hygine_manu_task',
        dataset_nm='seoulPublicHygineBiz',
        path='/opt/airflow/files/seoulPublicHygineBiz/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='seoulPublicHygineBiz.csv'
    )

    wash_manu_task >> hygine_manu_task