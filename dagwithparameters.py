from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'start_date': datetime.utcnow(),
    'owner': 'airflow',
}

def print_hello():
    return 'Hello world!'
  
def run_this_func_withparam(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))
    
dag = DAG('dagwithparameters', description='Simple DAG with params', default_args=args,
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

param_operator = PythonOperator(task_id='param_task',provide_context=True, python_callable=run_this_func_withparam, dag=dag)

dummy_operator >> param_operator
