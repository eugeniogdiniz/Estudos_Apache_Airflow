import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from os.path import join
from airflow.utils.dates import days_ago
from pathlib import Path

with DAG(dag_id = "TwitterDAG", start_date=days_ago(6), schedule_interval="@daily") as dag:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "datascience"

    twitter_operator = TwitterOperator(file_path=join("datalake/bronze/twitter_datascience/extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}",
                                        "datascience_{{ ds_nodash }}.json"),
                                        query=query,
                                        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                        task_id="twitter_datascience")

    twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience",
                                            application="/home/eugenio/Documents/curso/src/Spark/transformation.py",
                                            name="twitter_transformation",
                                            application_args=["--src", "datalake/bronze/twitter_datascience", 
                                                              "--dest", "datalake/silver/twitter_datascience",
                                                              "--process-date", "{{ ds }}"])
    
twitter_operator >> twitter_transform