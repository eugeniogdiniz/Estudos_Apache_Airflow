# bibliotecas importadas

Durante a criação do nosso DAG foram importadas diferentes bibliotecas para que pudéssemos desenvolver todo o código das nossas funções. Vamos conhecer melhor um pouco de cada uma dessas bibliotecas:

yfinance: essa biblioteca do python extrai dados da API do Yahoo Finance e nos permite acessar esses dados de forma bem prática utilizando módulos e métodos.

airflow.decorators: esse módulo disponibiliza os decorators do Airflow. Nós importamos os decorators @task e @dag por meio do código from airflow.decorators import DAG, task. Esses decorators são importados para que possamos criar nossa task e nosso DAG utilizando taskflow.

airflow.macros: macros são uma maneira de passar dados dinâmicos para os DAGs em tempo de execução. Esse módulo possui métodos que podemos utilizar para trabalhar com esses macros, como a função ds_add que nós utilizamos na nossa task para acessar o dia anterior à data de execução no Airflow.

pathlib: esse módulo é utilizado para trabalhar com caminhos (path) do sistema. Nós importamos a classe Path desse módulo, que é utilizada para instanciar um caminho concreto de acordo com o que for passado como argumento e possui métodos como o mkdir que utilizamos para criação de novas pastas.

pendulum: essa biblioteca facilita o nosso trabalho com datas e horas. Então ela basicamente é utilizada para facilitar a manipulação dos tipos datetime no python.

# Executor Local

Para isso, fizemos a instalação do banco de dados Postgres e criamos um novo banco de dados utilizando os comandos:

Atualizando os pacotes do Ubuntu
    sudo apt updateC

    sudo apt upgradeC

Instalando o Postgres
    sudo apt install postgresql postgresql-contribC

Acessando o usuário postgres
    sudo -i -u postgresC

Criando um banco de dados
    createdb airflow_dbC

Acessando o banco de dados criado
    psql airflow_dbC

Criando um usuário e uma senha pro banco de dados
    CREATE USER airflow_user WITH PASSWORD 'airflow_pass';C

Garantindo privilégios pro usuário criado ao banco de dados
    GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;C

Além disso, conhecemos e alteramos os seguintes parâmetros no arquivo de configuração airflow.cfg:

Parâmetro executor
    executor = LocalExecutorC

Parâmetro sql_alchemy_conn
    sql_alchemy_conn = postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_dbC

Parâmetro max_active_tasks_per_dag
    max_active_tasks_per_dag = 2C

Parâmetro max_active_runs_per_dag
    max_active_runs_per_dag = 4C
