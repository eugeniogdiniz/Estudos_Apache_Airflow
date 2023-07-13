import time
import datetime
import pandas as pd
import pyautogui as pyag
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        "Dag_01_Extracao_Bacen",
        start_date = datetime.datetime(2023, 7, 7, 15, 0, 0),
        schedule_interval='0 18-20 * * 1-5', 
    ) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/eugenio/Documentos/selenium_airflow/semana={{data_interval_end}}"'
    )

    def extrai_dados():

        df = pd.read_excel('/home/eugenio/Documentos/selenium_airflow/docs/Extracao.xlsx')

        df_cod_moeda = df.Cód_BCB
        df_pais = df.Pais
        dt_in = days_ago(0).replace(day=1).strftime("%d/%m/%Y")
        dt_fim = days_ago(0).strftime("%d/%m/%Y")
        
        lg = len(df_pais)
        l = 0
        c = 0

        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.prompt_for_download": True,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        driver = webdriver.Chrome(options=chrome_options)

        while (l < lg ):
            link_download = f'https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=gerarCSVFechamentoMoedaNoPeriodo&{df_cod_moeda[l]}&DATAINI={dt_in}&DATAFIM={dt_fim}'
            driver.get(link_download)
            time.sleep(2)
            pyag.write(df_pais[l])
            time.sleep(0.5)
            pyag.press(['Tab'])
            pyag.press(['Tab'])
            pyag.press(['Enter'])
            l += 1
            c += 1

        time.sleep(1)    
        driver.quit()
    
    tarefa_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    



    tarefa_1 >> tarefa_2