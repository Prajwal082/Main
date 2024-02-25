import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from features.Nse_data import Nse

def launch_bronze():

    nse = Nse()

    nse.get_DeliveryData()

dag = DAG(
    dag_id = "NSE_Orchestration",
    default_args = {
        "owner": "prajwal poojary",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)


# bronze_job = BashOperator(
#         task_id='Bronze_task',
#         bash_command='python3.11 /opt/airflow/jobs/python/Nse_data.py',
#         dag=dag)


bronze_job = PythonOperator(
    task_id="NSE_API_DATAPULL",
    python_callable = launch_bronze,
    dag=dag
)
# dags/features/bronze/bronze.py

spark_job_1 = SparkSubmitOperator(
    task_id="BRONZE",
    conn_id="spark-conn",
    application="dags/features/bronze/bronze.py",
    dag=dag
)


spark_job_2 = SparkSubmitOperator(
    task_id="SILVER",
    conn_id="spark-conn",
    application="dags/features/silver/silver.py",
    dag=dag
)

spark_job_3 = SparkSubmitOperator(
    task_id="GOLD",
    conn_id="spark-conn",
    application="dags/features/gold/gold.py",
    dag=dag
)

bronze_job >> spark_job_1 >> spark_job_2 >> spark_job_3


