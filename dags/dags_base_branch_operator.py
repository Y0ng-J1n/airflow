import pendulum

from airflow import DAG
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_base_branch_operator',
    start_date=pendulum.datetime(2026, 4, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random
            print(context)

            item_list = ['A', 'B', 'C']
            seleted_item = random.choice(item_list)
            
            if seleted_item == 'A':
                return 'task_a'
            elif seleted_item in ['B', 'C']:
                return ['task_b', 'task_c']

    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected': 'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected': 'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected': 'c'}
    )

    custom_branch_operator >> [task_a, task_b, task_c]