# BQML-in-Composer

example dag gets new data, aggregates it, trains (or retrains) a [ARIMA model for forecasting](https://cloud.google.com/bigquery-ml/docs/arima-single-time-series-forecasting-tutorial), generates predictions with it and stores them in a separate BigQuery table before returning that table.

## Prereqs

A number of requirements need to be added to run this example. In the Environment Details of your Composer instance, add the following 

requests
paramiko
sshtunnel
airflow-provider-fivetran
apache-airflow-backport-providers-google

## Fivetran

Data in this example is moved from sources to BigQuery using Fivetran for automated data ingestion via the [Fivetran Airflow Provider](https://fivetran.com/blog/announcing-the-fivetran-airflow-provider).

The Fivetran connectors used in this example are for LinkedIn Ads and Twitter Ads

## Transformations

Once the FivetranSensors return work can proceed further downstream in the data pipeline. Next, data from the two Fivetran sources is aggregates together in a single table via [Fivetran's dbt ad reporting package](https://hub.getdbt.com/fivetran/ad_reporting/latest/).

## Model creation and training

The following query is run to create and train or retrain a machine learning model to use to generate a forecast. The query is run by calling the BigQueryExecuteQueryOperator in Airflow.

```
CREATE OR REPLACE MODEL bqml.dbt_ads_airflow_model 
    OPTIONS 
        (model_type = 'ARIMA_PLUS', 
            "time_series_timestamp_col = 'parsed_date',
            "time_series_data_col = 'daily_impressions', 
            "auto_arima = TRUE,
            "data_frequency = 'AUTO_FREQUENCY', 
            "decompose_time_series = TRUE 
            ") AS 
                "SELECT 
                    "timestamp(date_day) as parsed_date,
                    "SUM(impressions) as daily_impressions
                 "FROM ` " + PROJECT_ID + ".ad_reporting`
                 "GROUP BY date_day;"
```

## Generating, storing and displaying predictions

The following query uses the model created in the previous step to generate a forecast as a series of predictions. These predictions are then stored in a table in BigQuery 

```
SELECT string(forecast_timestamp) as forecast_timestamp,
                    "forecast_value,
                    "standard_error,
                    "confidence_level, 
                    "prediction_interval_lower_bound, 
                    "prediction_interval_upper_bound, 
                    "confidence_interval_lower_bound, 
                    "confidence_interval_upper_bound 
                "FROM ML.FORECAST(MODEL bqml.dbt_ads_airflow_model,STRUCT(30 AS horizon, 0.8 AS confidence_level));
```
