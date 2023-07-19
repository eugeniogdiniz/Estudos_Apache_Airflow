# bibliotecas importadas

Durante a criação do nosso DAG foram importadas diferentes bibliotecas para que pudéssemos desenvolver todo o código das nossas funções. Vamos conhecer melhor um pouco de cada uma dessas bibliotecas:

yfinance: essa biblioteca do python extrai dados da API do Yahoo Finance e nos permite acessar esses dados de forma bem prática utilizando módulos e métodos.

airflow.decorators: esse módulo disponibiliza os decorators do Airflow. Nós importamos os decorators @task e @dag por meio do código from airflow.decorators import DAG, task. Esses decorators são importados para que possamos criar nossa task e nosso DAG utilizando taskflow.

airflow.macros: macros são uma maneira de passar dados dinâmicos para os DAGs em tempo de execução. Esse módulo possui métodos que podemos utilizar para trabalhar com esses macros, como a função ds_add que nós utilizamos na nossa task para acessar o dia anterior à data de execução no Airflow.

pathlib: esse módulo é utilizado para trabalhar com caminhos (path) do sistema. Nós importamos a classe Path desse módulo, que é utilizada para instanciar um caminho concreto de acordo com o que for passado como argumento e possui métodos como o mkdir que utilizamos para criação de novas pastas.

pendulum: essa biblioteca facilita o nosso trabalho com datas e horas. Então ela basicamente é utilizada para facilitar a manipulação dos tipos datetime no python.