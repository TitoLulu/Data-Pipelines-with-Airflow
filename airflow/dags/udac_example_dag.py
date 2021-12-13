from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator,DataQualityOperator)
from helpers import SqlQueries as sq
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'Tito Magero',
    'start_date': datetime.now(),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift =  PostgresOperator(
    task_id = 'Create_tables',
    dag=dag,
    postgres_conn_id = "redshift",
    sql = open('/home/workspace/airflow/create_tables.sql').read()
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id ='redshift',
    aws_cred_id='aws_credentials',
    target_table ='public.staging_events',
    s3_path ='s3://udacity-dend/log_data',
    json_path='auto',
    s3_region = 'us-west-2'
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id ='redshift',
    aws_cred_id='aws_credentials',
    target_table ='public.staging_songs',
    s3_path ='s3://udacity-dend/song_data',
    json_path='auto',
    s3_region = 'us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    fact_table = 'public.songplays',
    load_fact_sql = sq.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    dim_table ='public.users',
    load_dim_sql = sq.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    dim_table = "public.songs",
    load_dim_sql = sq.song_table_insert
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    dim_table = 'public.artists',
    load_dim_sql = sq.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    dim_table = 'public.time',
    load_dim_sql = sq.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    table = ['public.artists','public.users','public.songs','public.time','public.songplays']
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_in_redshift
create_tables_in_redshift >> [stage_events_to_redshift,stage_songs_to_redshift]
stage_events_to_redshift >> [load_songplays_table,load_user_dimension_table]
stage_songs_to_redshift >> [load_songplays_table,load_song_dimension_table,load_artist_dimension_table]
load_songplays_table  >> load_time_dimension_table
load_time_dimension_table  >> run_quality_checks
load_user_dimension_table  >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table  >>run_quality_checks
run_quality_checks >> end_operator
