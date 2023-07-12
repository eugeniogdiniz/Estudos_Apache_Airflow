
""" Neste código, estamos utilizando o Airflow para extrair tweets do Twitter e salvar os resultados em um arquivo JSON. Aqui está uma explicação detalhada das partes do código:

Importamos as bibliotecas necessárias: """

import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path

""" Definimos uma classe chamada TwitterOperator que herda da classe BaseOperator do Airflow para realizar a extração de tweets do Twitter: """

class TwitterOperator(BaseOperator):
    """
    Classe personalizada de operador para extrair tweets do Twitter e salvar em um arquivo JSON.
    Herda a classe BaseOperator do Airflow.
    """

    template_fields = ["query", "file_path", "start_time", "end_time"]

    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        """
        Inicializa a classe TwitterOperator.

        Parâmetros:
        - file_path: Caminho do arquivo onde os tweets serão salvos.
        - end_time: Data e hora de término da consulta no formato "%Y-%m-%dT%H:%M:%S.00Z".
        - start_time: Data e hora de início da consulta no formato "%Y-%m-%dT%H:%M:%S.00Z".
        - query: Consulta de pesquisa no Twitter.
        """
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path
        super().__init__(**kwargs)

    def create_parent_folder(self):
        """
        Cria a pasta pai do arquivo se ela não existir.
        """
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)
    
    def execute(self, context):
        """
        Executa a extração de tweets do Twitter e salva os resultados em um arquivo JSON.
        """
        end_time = self.end_time
        start_time = self.start_time
        query = self.query

        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            for pg in TwitterHook(end_time, start_time, query).run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")

if __name__ == "__main__":
    # Realiza uma consulta na API do Twitter e salva os resultados em um arquivo JSON

    # Obtém a data e hora atual
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    # Cria uma DAG do Airflow com um operador TwitterOperator
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            file_path=join("datalake/twitter_datascience",
                           f"extract_date={datetime.now().date()}",
                           f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"),
            query=query, start_time=start_time, end_time=end_time, task_id="test_run"
        )

        # Cria uma instância de tarefa para o operador
        ti = TaskInstance(task=to)

        # Executa o operador
        to.execute(ti.task_id)

""" Na parte final do código, dentro do if __name__ == "__main__":, realizamos a extração de tweets e salvamos os resultados em um arquivo JSON:
Obtemos a data e hora atual.
Criamos uma DAG do Airflow chamada "TwitterTest".
Criamos uma instância do operador TwitterOperator passando os parâmetros necessários.
Criamos uma instância de tarefa para o operador.
Executamos o operador chamando o método execute.
Essa é uma breve explicação do código fornecido, mostrando como usar o Airflow para extrair tweets do Twitter e salvar os resultados em um arquivo JSON. Certifique-se de adicionar mais detalhes e informações relevantes aos comentários de acordo com as necessidades do seu projeto. """
