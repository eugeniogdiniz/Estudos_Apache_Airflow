from airflow.providers.http.hooks.http import HttpHook
import requests
from datetime import datetime, timedelta
import json


class TwitterHook(HttpHook):
    """
    Classe personalizada para interagir com a API do Twitter usando o Airflow.
    Estende a classe HttpHook do Airflow para fornecer funcionalidades específicas do Twitter.
    """

    def __init__(self, end_time, start_time, query, conn_id=None):
        """
        Inicializa a classe TwitterHook.

        Parâmetros:
        - end_time: Data e hora de término da consulta no formato "%Y-%m-%dT%H:%M:%S.00Z".
        - start_time: Data e hora de início da consulta no formato "%Y-%m-%dT%H:%M:%S.00Z".
        - query: Consulta de pesquisa no Twitter.
        - conn_id: ID da conexão no Airflow. Se não for fornecido, será utilizado "twitter_default".
        """
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        """
        Cria a URL de consulta para a API do Twitter.

        Retorna:
        - url_raw: URL completa para a consulta na API do Twitter.
        """
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

        end_time = self.end_time
        start_time = self.start_time
        query = self.query

        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

        return url_raw

    def connect_to_endpoint(self, url, session):
        """
        Conecta-se ao endpoint da API do Twitter e retorna a resposta.

        Parâmetros:
        - url: URL completa para a consulta na API do Twitter.
        - session: Sessão de requisição HTTP.

        Retorna:
        - response: Resposta da requisição HTTP ao endpoint da API do Twitter.
        """
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})

    def paginate(self, url_raw, session):
        """
        Pagina os resultados da consulta e retorna uma lista de respostas JSON.

        Parâmetros:
        - url_raw: URL completa para a consulta na API do Twitter.
        - session: Sessão de requisição HTTP.

        Retorna:
        - lista_json_response: Lista contendo as respostas JSON paginadas.
        """
        lista_json_response = []

        # Realiza a primeira requisição
        response = self.connect_to_endpoint(url_raw, session)
        json_response = response.json()
        lista_json_response.append(json_response)
        contador = 1

        # Pagina os resultados enquanto houver um próximo token disponível e o contador for menor que 10
        while "next_token" in json_response.get("meta", {}) and contador < 10:
            next_token = json_response['meta']['next_token']
            url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(url, session)
            json_response = response.json()
            lista_json_response.append(json_response)
            contador += 1

        return lista_json_response
    
    def run(self):
        """
        Executa a consulta na API do Twitter.

        Retorna:
        - lista_json_response: Lista contendo as respostas JSON paginadas.
        """
        session = self.get_conn()
        url_raw = self.create_url()

        return self.paginate(url_raw, session)


if __name__ == "__main__":
    # Realiza uma consulta na API do Twitter e imprime os resultados em formato JSON

    # Obtém a data e hora atual
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    # Executa a consulta e imprime os resultados paginados
    for pg in TwitterHook(end_time, start_time, query).run():
        print(json.dumps(pg, indent=4, sort_keys=True))

