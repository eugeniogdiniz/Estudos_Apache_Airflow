import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

data_inicio = "2022-01-01"
data_fim = "2022-02-01"
city = "RiodeJaneiro"
key = "C2RF9MB8QSH68WRHQG5XJUDWT"
URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
          f"{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv")

dados = pd.read_csv(URL)
print(dados.head())

#C2RF9MB8QSH68WRHQG5XJUDWT

    