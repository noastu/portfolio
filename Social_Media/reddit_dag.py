from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

args = {
    'start_date': datetime(2023,1,1),
    'owner': 'admin',
    "email": ["***email***"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    }

dag = DAG('run_reddit_etl',
                default_args=args,
                schedule_interval='0 0,6,12,18 * * *',
                catchup=False
          )

api_task = BashOperator(
    task_id='api_pull',
    bash_command='python3 ***path to config file***',
    dag=dag
)  

