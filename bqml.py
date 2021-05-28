import os
import airflow
from airflow import DAG
from airflow.models import Variable
from operators.fivetran import FivetranOperator
from operators.ssh import SSHOperator
from sensors.fivetran import FivetranSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryGetDataOperator

DATASET_NAME = "bqml"
DESTINATION_TABLE = "dbt_ads_bqml_preds"
QUERY = "SELECT string(forecast_timestamp) as forecast_timestamp, " \
        "forecast_value, " \
        "standard_error, " \
        "confidence_level, " \
        "prediction_interval_lower_bound, " \
        "prediction_interval_upper_bound, " \
        "confidence_interval_lower_bound, " \
        "confidence_interval_upper_bound " \
        "FROM ML.FORECAST(MODEL bqml.dbt_ads_bqml_model,STRUCT(30 AS horizon, 0.8 AS confidence_level));"

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='ad_reporting_bqml_dag',
    default_args=default_args
)

linkedin_sync = FivetranOperator(
    task_id='linkedin-ads-sync',
    connector_id=Variable.get("linkedin_connector_id"),
    dag=dag
)


linkedin_sensor = FivetranSensor(
    connector_id=Variable.get("linkedin_connector_id"),
    poke_interval=600,
    task_id='linkedin-sensor',
    dag=dag
)

twitter_sync = FivetranOperator(
    task_id='twitter-ads-sync',
    connector_id=Variable.get("twitter_connector_id"),
    dag=dag
)

twitter_sensor = FivetranSensor(
    connector_id=Variable.get("twitter_connector_id"),
    poke_interval=600,
    task_id='twitter-sensor',
    dag=dag
)

dbt_run = SSHOperator(
    task_id='dbt_ad_reporting',
    command='cd dbt_ad_reporting ; ~/.local/bin/dbt run -m +ad_reporting',
    ssh_conn_id='dbtvm',
    dag=dag
  )

get_preds = BigQueryExecuteQueryOperator(
    task_id="get_predictions",
    sql=QUERY,
    use_legacy_sql=False,
    destination_dataset_table=DATASET_NAME + "." + DESTINATION_TABLE,
    write_disposition="WRITE_APPEND",
    dag=dag
)

print_preds = BigQueryGetDataOperator(
    task_id="print_predictions",
    dataset_id=DATASET_NAME,
    table_id=DESTINATION_TABLE,
    dag=dag
)

linkedin_sync >> linkedin_sensor
twitter_sync >> twitter_sensor
[linkedin_sensor, twitter_sensor] >> dbt_run

dbt_run >> get_preds >> print_preds
