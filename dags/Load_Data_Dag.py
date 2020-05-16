from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('Load_Data_Dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=False,
    Db_Connection="redshift",
    Table="staging_events",
    Source_Bucket="s3://udacity-dend/log_data",
    Region="us-west-2",
    Data_format="JSON",
    Aws_Keys="aws_credentials", 
    Mode="s3://udacity-dend/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    provide_context=False,
    Db_Connection="redshift",
    Table="staging_songs",
    Source_Bucket="s3://udacity-dend/song_data/",
    Region="us-west-2",
    Data_format="JSON",
    Aws_Keys="aws_credentials", 
    Mode="auto",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    Db_Connection="redshift",
    Table="songplays",
    AppendMode=False,
    sql_statement="songplay_table_insert",    
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    Db_Connection="redshift",
    Table="users",
    AppendMode=False,
    sql_statement="user_table_insert",    
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    Db_Connection="redshift",
    Table="songs",
    AppendMode=False,
    sql_statement="song_table_insert",  
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    Db_Connection="redshift",
    Table="artists",
    AppendMode=False,
    sql_statement="artist_table_insert",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    Db_Connection="redshift",
    Table="time",
    AppendMode=False,
    sql_statement="time_table_insert",
    dag=dag
)

run_quality_check_artist = DataQualityOperator(
    task_id='run_quality_check_artist',
    Db_Connection = "redshift",
    Table ="artists",
    TotalRecords = 0,
    dag=dag
)

run_quality_check_user = DataQualityOperator(
    task_id='run_quality_check_user',
    Db_Connection = "redshift",
    Table ="users",
    TotalRecords = 0,
    dag=dag
)

run_quality_check_time = DataQualityOperator(
    task_id='run_quality_check_time',
    Db_Connection = "redshift",
    Table ="time",
    TotalRecords = 0,
    dag=dag
)

run_quality_check_song = DataQualityOperator(
    task_id='run_quality_check_song',
    Db_Connection = "redshift",
    Table ="songs",
    TotalRecords = 0,
    dag=dag
)

run_quality_check_songplay = DataQualityOperator(
    task_id='run_quality_check_songplay',
    Db_Connection = "redshift",
    Table ="songplays",
    TotalRecords = 0,
    dag=dag
)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table>>run_quality_check_songplay
run_quality_check_songplay >> load_user_dimension_table >> run_quality_check_artist
run_quality_check_songplay >> load_song_dimension_table >> run_quality_check_song
run_quality_check_songplay >> load_artist_dimension_table >> run_quality_check_user
run_quality_check_songplay >> load_time_dimension_table >> run_quality_check_time
run_quality_check_artist >> end_operator
run_quality_check_song >> end_operator
run_quality_check_time >> end_operator
run_quality_check_user >> end_operator