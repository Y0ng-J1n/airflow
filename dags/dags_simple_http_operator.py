import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule='10 * * * *'
) as dag:

    '''서울시 대기오염물질 측정항목 정보'''
    air_pollution_info = HttpOperator(
        task_id='air_pollution_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/xml/airTestItem/1/5/',
        method='GET',
        headers={'Content-Type': 'application/json',
                 'charset': 'utf-8',
                 'Accept': '*/*'}
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='air_pollution_info')
        
        import xmltodict
        from pprint import pprint

        pprint(xmltodict.parse(rslt))

    air_pollution_info >> python_2()