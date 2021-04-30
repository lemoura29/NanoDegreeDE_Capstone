from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PythonOperator)
from helpers import SqlQueries
from operators.etl import ETLFlow
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook



AWS_KEY = 'AKIAXOZRK35GTEBVBA5U'
AWS_SECRET = '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'


s3_bucket = "capstone-lemoura-udacity/transform"

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 4, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'email_on_retry': False,
    'catchup' : False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval = '0 * * * *',
          max_active_runs = 1
        )

def call_etl_flow(aws_credentials_id,  **kwargs):
    flow = ETLFlow(aws_credentials_id)
    flow.execute()
    
    
def create_table(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    queries =  open("/home/workspace/airflow/plugins/helpers/create_tables.sql", "r").read()
    redshift_hook.run(queries)        
    return

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_indicator_to_redshift = StageToRedshiftOperator(
    task_id='Stage_indicator',
    dag=dag,
    table="dim_indicator",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket= s3_bucket,
    s3_key='dim_indicator.parquet'
)

stage_country_to_redshift = StageToRedshiftOperator(
    task_id='Stage_countrys',
    dag=dag,
    table="dim_country",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key='dim_country.parquet'
)

stage_fact_to_redshift = StageToRedshiftOperator(
    task_id='Stage_fact_score',
    dag=dag,
    table="fact_score_staging",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key='fact_score.parquet'
)

load_fact_table = LoadFactOperator(
    task_id='Load_score_fact_table',
    dag=dag,
    table ='fact_score',
    truncate_data=True,
    sql_query= SqlQueries.user_table_insert,
    columns = "country_code , country_name ,time_year ,indicator_code ,indicator_name ,value "
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift"
)

etl_operator = PythonOperator(
    task_id='etl_spark_operator',
    python_callable=call_etl_flow,
    provide_context=True,
    op_kwargs={
        'aws_credentials_id': 'aws_credentials'
    },
    dag=dag
)

create_tables_in_redshift = PythonOperator(
    task_id = "create_tables_in_redshift",
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",    
    dag = dag,
    provide_context=True,
    python_callable=create_table
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


dag >> start_operator >> etl_operator >> \
create_tables_in_redshift >> stage_indicator_to_redshift >> load_fact_table
create_tables_in_redshift >> stage_country_to_redshift >> load_fact_table
create_tables_in_redshift >> stage_fact_to_redshift >> load_fact_table
load_fact_table >> run_quality_checks >> end_operator
