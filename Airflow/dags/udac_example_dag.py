
import os
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.dummy_operator    import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators.stage_redshift            import StageToRedshiftOperator
from operators.load_fact                 import LoadFactOperator
from operators.load_dimension            import LoadDimensionOperator
from operators.data_quality              import DataQualityOperator

from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018,11,3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'wait_for_downstream': True,
    'email_on_failure': False,
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',         
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables_task = PostgresOperator(
    task_id          = "create_tables",
    dag              = dag,
    sql              = 'create_tables.sql',
    postgres_conn_id = "redshift"
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id            = 'Stage_events',
    dag                = dag,
    redshift_conn_id   = "redshift",
    aws_credentials_id = "aws_credentials",
    table              = "staging_events",
    s3_bucket          = "udacity-dend",
    s3_key             = "log_data/{execution_date.year}/{execution_date.month}/",
    json_path          = "s3://udacity-dend/log_json_path.json",
    provide_context    = True
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id            = 'Stage_songs',
    dag                = dag,
    redshift_conn_id   = "redshift",
    aws_credentials_id = "aws_credentials",
    table              = "staging_songs",
    s3_bucket          = "udacity-dend",
    s3_key             = "song_data/",
    json_path          = "auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id='redshift',
    sql=SqlQueries.user_table_insert,
    operation='delete'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id='redshift',
    sql=SqlQueries.song_table_insert,
    operation='delete'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id='redshift',
    sql=SqlQueries.artist_table_insert,
    operation='delete'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id='redshift',
    sql=SqlQueries.time_table_insert,
    operation='delete'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["songplays", "users", "artists", "songs", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator     >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift  >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table   >> run_quality_checks
load_song_dimension_table   >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table   >> run_quality_checks

run_quality_checks >> end_operator