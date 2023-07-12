import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from os.path import join
from airflow.utils.dates import days_ago

with DAG(dag_id="TwitterDAG", start_date=days_ago(2), schedule_interval="@daily") as dag:
    """
    Definição da DAG do Airflow para extrair tweets do Twitter diariamente.
    """

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "datascience"

    to = TwitterOperator(
        file_path=join("datalake/twitter_datascience",
                       "extract_date={{ ds }}",
                       "datascience_{{ ds_nodash }}.json"),
        query=query,
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        task_id="twitter_datascience"
    )

""" Nesta DAG, utilizamos o TwitterOperator para extrair tweets do Twitter com base na consulta "datascience". O caminho do arquivo de saída é construído dinamicamente usando join e incorporando variáveis de contexto do Airflow. Também utilizamos variáveis de contexto data_interval_start e data_interval_end para definir a data e hora de início e término da extração.

Certifique-se de ajustar os detalhes de caminhos de arquivo, consultas e outros parâmetros de acordo com as necessidades do seu projeto. """