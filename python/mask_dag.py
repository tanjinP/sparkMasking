from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define the DAG object
default_args = {
    'owner': 'vts',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 25),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('sparkRun', default_args=default_args, schedule_interval=timedelta(1))

# task to retrieve data from postgres and put into S3
loadPgIntoS3=BashOperator(
        task_id='download-csv-from-pg',
        bash_command='sh ~/pg_csv_to_s3.sh ',
        dag=dag)

# task to run spark submit (acts on data in s3 from first task)
sparkJob=BashOperator(
        task_id='spark-mask',
        bash_command='sh ~/mask_data.sh ',
        dag=dag)
sparkJob.set_upstream(loadPgIntoS3)

# task to take spark output (csv files in s3) and put into redshift
loadToRedshift=BashOperator(
        task_id='load-spark-output-to-redshift',
        bash_command='python ~/load_to_redshift.py',
        dag=dag)
loadToRedshift.set_upstream(sparkJob)