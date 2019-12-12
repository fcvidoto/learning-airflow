import datetime

import airflow
from airflow import models
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

yesterday = datetime.datetime.combine(
	datetime.datetime.today() - datetime.timedelta(1),
	datetime.datetime.min.time()
)

default_dag_args = {
	'start_date': yesterday,
	'project_id': models.Variable.get('gc-online-learning')
}

source_bucket = models.Variable.get('my-gcp-source')
dest_bucket = models.Variable.get('my-gcp-dest')

with DAG(
	'teste',
	schedule_interval=datetime.timedelta(days=1),
	default_args=default_dag_args) as dag:

	printaBuckets = BashOperator(
		task_id='printaBuckets',
		bash_command='echo {source} {dest}'.format(source=source_bucket, dest=dest_bucket)
	)

	printaBuckets