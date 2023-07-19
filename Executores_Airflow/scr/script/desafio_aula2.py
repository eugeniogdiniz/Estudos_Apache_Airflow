import yfinance

def extrai_dados(ticker, start_date, end_date):
    caminho = f"/home/eugenio/Documents/Executores_Airflow/acoes/{ticker}.csv"
    yfinance.Ticker(ticker).history(
        period = "1d",
        interval = "1h",
        start = start_date,
        end = end_date,
        prepost = True
    ).to_csv(caminho)

extrai_dados("AAPL", "2022-07-25", "2022-07-29")
extrai_dados("GOOG", "2022-06-06", "2022-06-08")