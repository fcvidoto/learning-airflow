import random
import datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

yesterday = datetime.datetime.combine(
	datetime.datetime.today() - datetime.timedelta(1),
	datetime.datetime.min.time()
)

default_dag_args = {
	'start_date': yesterday
	# 'retries': 2,
	# 'retry_delay': datetime.timedelta(minutes=2)
}

with DAG(
	'branching_python_operator',
	schedule_interval=datetime.timedelta(days=1),
	default_args=default_dag_args) as dag:
	
	def greeting():
		print('greeting')
		return 'greeting'

	def montaRandom():
		x = 2 #random.radint(1,5)
		if (x <= 2):
			return 'menor que 2'
		else:
			return 'maior que 2'

	runThisFirst = DummyOperator(task_id='runThisFirst')
	branching = PythonOperator(
		task_id='branching',
		python_callable=montaRandom
	)

	runThisFirst >> branching

	spikeyHello = PythonOperator(
		task_id='spikeyHello',
		python_callable=greeting
	)

	dummyDepoisPython = DummyOperator(task_id='dummyDepoisPython')

	dummy = DummyOperator(task_id='dummy')

	bashOi = BashOperator(
		task_id='bashOi',
		bash_command='echo fim',
		trigger_rule='one_success'
	)

	branching >> spikeyHello >> dummyDepoisPython >> bashOi
	branching >> dummy >> bashOi



