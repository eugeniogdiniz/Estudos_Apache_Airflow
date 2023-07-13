import time
import datetime
import shutil
import pandas as pd
import os
import pyautogui as pyag
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dt_pasta = days_ago(0)

with DAG(
        "Dag_02_Extracao_Bacen",
        start_date = datetime.datetime(2023, 7, 7, 17, 0, 0),
        schedule_interval='0 17-20 * * 1-5', 
    ) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = f'mkdir -p "/home/eugenio/Documentos/selenium_airflow/semana={dt_pasta}"'
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

    def reorganiza_pastas():
        caminho_download = '/home/eugenio/Downloads'
        caminho_novo = f'/home/eugenio/Documentos/selenium_airflow/semana={dt_pasta}'

        df = pd.read_excel('/home/eugenio/Documentos/selenium_airflow/docs/Extracao.xlsx')
        df_pais = df.Pais

        l = 0
        lg = len(df_pais)
        while (l < lg ):
            d = f'{df_pais[l]}.csv'
            caminho_original = f'{caminho_download}/{d}'
            caminho_nova_pasta = f'{caminho_novo}/{d}'
            shutil.move(caminho_original, caminho_nova_pasta)
            l += 1

    tarefa_3 = PythonOperator(
        task_id = 'reorganiza_pastas',
        python_callable = reorganiza_pastas,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    def consolidacao():
        # Definindo o mês e ano desejados
        ano = 2023
        mes = 7
        # Obtendo o número de dias no mês
        num_dias = pd.Period(f'{ano}-{mes}').days_in_month
        # Criando uma lista de datas para o mês
        datas = [datetime.date(ano, mes, dia) for dia in range(1, num_dias+1)]
        # Criando o DataFrame de calendário
        df_calendario = pd.DataFrame({'Data': datas})
        # Exibindo o DataFrame de calendário
        df_calendario
        df_calendario['Data'] = df_calendario['Data'].astype(str)
        
        def tratar_data(data):
            if len(data) == 7:
                data_tratada = "0" + data[0:1] + "-" + data[1:3] + "-" + data[3:7]
            else:
                data_tratada = data[0:2] + "-" + data[2:4] + "-" + data[4:8]
            return pd.to_datetime(data_tratada, format="%d-%m-%Y")
        
        caminho_novo = f'/home/eugenio/Documentos/selenium_airflow/semana={dt_pasta}'

        df = pd.read_excel('/home/eugenio/Documentos/selenium_airflow/docs/Extracao.xlsx')
        df_pais = df.Pais
        l = 0    
        
        df_final = pd.DataFrame()
        v = caminho_novo
        for arquivo in df_pais:
            d = f'{df_pais[l]}.csv'
            df = pd.read_csv(f'{v}/{d}', delimiter=';', header=None)  # Ler o arquivo Excel    
            novos_nomes = ['Data', 'Cod', 'Tipo', 'Simbolo', 'Tx_Compra', 'Tx_Venda', 'Parid_Compra', 'Parid_Venda']
            df.columns = novos_nomes
            df['Data'] = df['Data'].astype(str)
            df['Data'] = df['Data'].apply(tratar_data)
            df['Data'] = df['Data'].astype(str)
            df_agrupado = pd.merge(df_calendario, df, left_on= 'Data', right_on= 'Data', how='outer')
            df_cascata = df_agrupado.fillna(method='ffill')
            df_cascata
            df_final = pd.concat([df_final, df_cascata], ignore_index=True)# Adicionar as linhas ao DataFrame final
            df_final['Data'] = df_final['Data'].astype('object')
            l += 1

        # Salvar o DataFrame final em um novo arquivo Excel
        df_final.to_excel(f'{caminho_novo}/planilha_final.xlsx', index=False)

    tarefa_4 = PythonOperator(
        task_id = 'consolidacao_planilha_final',
        python_callable = consolidacao,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    def excluir_arquivos():
        diretorio_exclusao = f'/home/eugenio/Documentos/selenium_airflow/semana={dt_pasta}'

        df = pd.read_excel('/home/eugenio/Documentos/selenium_airflow/docs/Extracao.xlsx')
        df_pais = df.Pais

        l = 0
        lg = len(df_pais)
        while (l < lg ):
            d = f'{df_pais[l]}.csv'
            arquivo_caminho_exclusao = f'{diretorio_exclusao}/{d}'
            os.remove(arquivo_caminho_exclusao)
            l += 1

    tarefa_5 = PythonOperator(
        task_id = 'excluir_arquivos',
        python_callable = excluir_arquivos,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2
    tarefa_2 >> tarefa_3
    tarefa_3 >> tarefa_4
    tarefa_4 >> tarefa_5
    

    