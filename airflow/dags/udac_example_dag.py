from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator,DataQualityOperator)
from helpers import SqlQueries as sq
from airflow.operators.postgres_operator import PostgresOperator

create_tables = open(os.path.join(os.path.split(os.getcwd())[0], 'workspace/airflow/create_tables.sql')).read()

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift =  PostgresOperator(
    task_id = 'Create_tables',
    dag = dag,
    redshift_conn_id = "redshift",
    sql = create_tables
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id ='redshift',
    aws_cred_id='aws_credentials',
    target_table ='public.staging_events',
    s3_bucket ='s3://udacity-dend/log_data',
    json_path='auto',
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id ='redshift',
    aws_cred_id='aws_credentials',
    target_table ='public.staging_songs',
    s3_bucket ='s3://udacity-dend/song_data',
    json_path='auto',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "public.songplays",
    load_fact_sql = sq.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "public.users",
    load_fact_sql = sq.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "public.songs",
    load_fact_sql = sq.song_table_insert
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "public.artists",
    load_fact_sql = sq.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "public.time",
    load_fact_sql = sq.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    
)#data quality check, add for loop

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
