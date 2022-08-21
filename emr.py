import airflow
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
from datetime import datetime

# emr connection
emr_conn_id = 'emr_default'

# default args to the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 1)
}

# dag parameters
dag = DAG(
    'emr_job',
    default_args=default_args,
    schedule_interval="20 6 * * *",
    max_active_runs=1
)

dummy = DummyOperator(
    task_id='dummy',
    trigger_rule='all_done',
    retries=2,
    retry_delay=timedelta(minutes=1),
    dag=dag
)

##########################
### Create EMR Cluster ###
##########################

# EMR cluster name
job_flow_overrides = {
    'Name': 'Create EMR Cluster'
}

# creates an EMR cluster on devops account
cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=job_flow_overrides,
    aws_conn_id='aws_default',
    emr_conn_id=emr_conn_id,
    retries=4,
    retry_delay=timedelta(minutes=1),
    dag=dag
)

cluster_creator.set_downstream(dummy)