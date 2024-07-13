from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator



def task1():
    print("Task 1")
    return 1

def task2():
    print("Task 2")
    return 2

def get_return(ti=None):
    print("task 1:",ti.xcom_pull(task_ids="task_1"))
    print("task 2:",ti.xcom_pull(task_ids="task_2"))

dag = DAG(
    dag_id = "sample_dag",
    start_date = datetime(2024,1,1),
    schedule_interval = '@daily',
    catchup = False,
    tags = ['practice']
)
    
task_1 = PythonOperator(
    task_id = "task_1",
    python_callable=task1,
    dag=dag,
)

task_2 = PythonOperator(
    task_id = "task_2",
    python_callable=task2,
    dag=dag,
)

return_task = PythonOperator(
    task_id= "return_fun",
    python_callable=get_return,
    dag=dag,
)

task_1 >> task_2 >> return_task