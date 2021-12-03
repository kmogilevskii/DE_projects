from config_vars import *
from sql_queries import *
from airflow import models
from airflow.operators import bash
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators import bigquery
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor


# Define default arguments
default_args = {
    'owner': 'Konstantin Mogilevskii',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'wait_for_downstream': True,
}

with models.DAG(
        'udacity_capstone_project',
        start_date=datetime.now(),
        schedule_interval='@once',
        default_args=default_args) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')

    make_stg_dataset = bash.BashOperator(
        task_id='make_stg_dataset',
        bash_command=f'bq ls {staging_dataset} || bq mk {staging_dataset}')

    ###########################################################
    ###             LOADING TO STAGING DATASET              ###
    ###########################################################
    load_us_cities_demo = GoogleCloudStorageToBigQueryOperator(
        task_id='load_us_cities',
        bucket=gs_bucket,
        source_objects=['cities/us-cities-demographics.csv'],
        destination_project_dataset_table=f'{project_id}:{staging_dataset}.us_cities',
        schema_object='cities/us_cities_schema.json',
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        field_delimiter=';',
        skip_leading_rows=1
    )

    load_airports = GoogleCloudStorageToBigQueryOperator(
        task_id='load_airports',
        bucket=gs_bucket,
        source_objects=['airports/airport-codes_csv.csv'],
        destination_project_dataset_table=f'{project_id}:{staging_dataset}.airport_codes',
        schema_object='airports/airport_codes_schema.json',
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1
    )

    load_weather = GoogleCloudStorageToBigQueryOperator(
        task_id='load_weather',
        bucket=gs_bucket,
        source_objects=['weather_data/GlobalLandTemperaturesByCity.csv'],
        destination_project_dataset_table=f'{project_id}:{staging_dataset}.temperature_by_city',
        schema_object='weather_data/temperature_schema.json',
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1
    )

    load_immigration_data = GoogleCloudStorageToBigQueryOperator(
        task_id='load_immigration_data',
        bucket=gs_bucket,
        source_objects=['immigration_data/*.parquet'],
        destination_project_dataset_table=f'{project_id}:{staging_dataset}.immigration_data',
        source_format='parquet',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True
    )
    ### END OF LOADING TO STAGING DATASET ###


    ###########################################################
    ###             CHECKING STAGING TABLES                 ###
    ###########################################################
    check_temperature_by_city_exists = BigQueryTableExistenceSensor(
        task_id="check_temperature_by_city_exists", project_id=project_id, dataset_id=staging_dataset,
        table_id='temperature_by_city'
    )

    check_airport_codes_exists = BigQueryTableExistenceSensor(
        task_id="check_airport_codes_exists", project_id=project_id, dataset_id=staging_dataset,
        table_id='airport_codes'
    )

    check_us_cities_exists = BigQueryTableExistenceSensor(
        task_id="check_us_cities_exists", project_id=project_id, dataset_id=staging_dataset,
        table_id='us_cities'
    )

    check_immigration_exists = BigQueryTableExistenceSensor(
        task_id="check_immigration_exists", project_id=project_id, dataset_id=staging_dataset,
        table_id='immigration_data'
    )
    ### END OF CHECKING STAGING TABLES ###


    make_final_dataset = bash.BashOperator(
        task_id='make_final_dataset',
        bash_command=f'bq ls {final_dataset} || bq mk {final_dataset}')


    ###########################################################
    ###      CREATING/CHECKING DIMENSIONAL TABLES           ###
    ###########################################################
    load_dim_ports = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_ports',
        bucket=gs_bucket,
        source_objects=['ports_countries_states/ports.csv'],
        destination_project_dataset_table=f'{project_id}:{final_dataset}.dim_ports',
        schema_fields=[
            {"name": "port_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "port_name", "type": "STRING", "mode": "NULLABLE"}
        ],
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1
    )

    load_dim_countries = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_countries',
        bucket=gs_bucket,
        source_objects=['ports_countries_states/countries.csv'],
        destination_project_dataset_table=f'{project_id}:{final_dataset}.dim_countries',
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
        schema_fields=[
            {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country_name", "type": "STRING", "mode": "NULLABLE"}
        ]
    )

    load_dim_states = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_us_states',
        bucket=gs_bucket,
        source_objects=['ports_countries_states/us_states.csv'],
        destination_project_dataset_table=f'{project_id}:{final_dataset}.dim_us_states',
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
        schema_fields=[
            {"name": "state_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "abbreviation", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state_code", "type": "STRING", "mode": "NULLABLE"}
        ]
    )

    bq_facts_immigration_query = bigquery.BigQueryInsertJobOperator(
        task_id="facts_immigration_creation",
        configuration={
            "query": {
                "query": create_facts_immigration,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": final_dataset,
                    "tableId": "facts_immigration"
                }
            }
        },
        location=location,
    )

    check_dim_ports = BigQueryTableExistenceSensor(
        task_id="check_dim_ports_exist", project_id=project_id, dataset_id=final_dataset,
        table_id='dim_ports'
    )

    check_dim_countries = BigQueryTableExistenceSensor(
        task_id="check_dim_countries_exist", project_id=project_id, dataset_id=final_dataset,
        table_id='dim_countries'
    )

    check_dim_states = BigQueryTableExistenceSensor(
        task_id="check_dim_states_exist", project_id=project_id, dataset_id=final_dataset,
        table_id='dim_us_states'
    )

    check_facts_immigration = BigQueryTableExistenceSensor(
        task_id="check_facts_immigration_exist", project_id=project_id, dataset_id=final_dataset,
        table_id='facts_immigration'
    )

    intermediate_pipeline = DummyOperator(task_id='intermediate_pipeline')

    bq_dim_airports_query = bigquery.BigQueryInsertJobOperator(
        task_id="dim_airports_creation",
        configuration={
            "query": {
                "query": create_dim_airports,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": final_dataset,
                    "tableId": "dim_airports"
                }
            }
        },
        location=location,
    )

    bq_dim_cities_query = bigquery.BigQueryInsertJobOperator(
        task_id="dim_cities_creation",
        configuration={
            "query": {
                "query": create_dim_cities,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": final_dataset,
                    "tableId": "dim_cities"
                }
            }
        },
        location=location,
    )

    bq_dim_time_query = bigquery.BigQueryInsertJobOperator(
        task_id="dim_time_creation",
        configuration={
            "query": {
                "query": create_dim_time,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": final_dataset,
                    "tableId": "dim_time"
                }
            }
        },
        location=location,
    )

    bq_dim_weather_query = bigquery.BigQueryInsertJobOperator(
        task_id="dim_weather_creation",
        configuration={
            "query": {
                "query": create_dim_weather,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": final_dataset,
                    "tableId": "dim_weather"
                }
            }
        },
        location=location,
    )

    check_dim_airports = BigQueryTableExistenceSensor(
        task_id="check_dim_airports_exist", project_id=project_id, dataset_id=final_dataset,
        table_id='dim_airports'
    )

    check_dim_cities = BigQueryTableExistenceSensor(
        task_id="check_dim_cities_exist", project_id=project_id, dataset_id=final_dataset,
        table_id='dim_cities'
    )

    check_dim_time = BigQueryTableExistenceSensor(
        task_id="check_dim_time_exist", project_id=project_id, dataset_id=final_dataset,
        table_id='dim_time'
    )

    check_dim_weather = BigQueryTableExistenceSensor(
        task_id="check_dim_weather_exist", project_id=project_id, dataset_id=final_dataset,
        table_id='dim_weather'
    )

    ### END OF CREATING/CHECKING FINAL DIMENSIONAL TABLES ###

    end_pipeline = DummyOperator(task_id='end_pipeline')

    start_pipeline >> make_stg_dataset

    make_stg_dataset >> [load_us_cities_demo, load_airports, load_weather, load_immigration_data]

    load_us_cities_demo   >> check_us_cities_exists
    load_airports         >> check_airport_codes_exists
    load_weather          >> check_temperature_by_city_exists
    load_immigration_data >> check_immigration_exists

    [check_us_cities_exists, check_airport_codes_exists, check_temperature_by_city_exists, check_immigration_exists] >> make_final_dataset

    make_final_dataset >> [load_dim_ports, load_dim_countries, load_dim_states, bq_facts_immigration_query]

    load_dim_ports             >> check_dim_ports
    load_dim_countries         >> check_dim_countries
    load_dim_states            >> check_dim_states
    bq_facts_immigration_query >> check_facts_immigration

    [check_dim_ports, check_dim_countries, check_dim_states, check_facts_immigration] >> intermediate_pipeline

    intermediate_pipeline >> [bq_dim_airports_query, bq_dim_cities_query, bq_dim_time_query, bq_dim_weather_query]

    bq_dim_airports_query >> check_dim_airports
    bq_dim_cities_query   >> check_dim_cities
    bq_dim_time_query     >> check_dim_time
    bq_dim_weather_query  >> check_dim_weather

    [check_dim_airports, check_dim_cities, check_dim_time, check_dim_weather] >> end_pipeline
