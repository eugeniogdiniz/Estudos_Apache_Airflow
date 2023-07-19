# Construindo o ambiente do zero

### Observação sobre a API
Recentemente, a API do Twitter se tornou paga, o que dificultou o seu uso como fonte de dados para o projeto do curso. Por isso, vamos utilizar uma nova API que tem o mesmo comportamento da API do Twitter.

Você pode usar o campo query para informar os tweets que deseja buscar e os campos start_time e end_time para definir o intervalo de tweets que deseja, ou seja, tweets publicados nesse período.

No entanto, é importante destacar que os tweets gerados por essa API são fictícios e servem apenas para aprender como integrar uma API para ingestão de dados em um pipeline de dados. Por isso, sempre que no projeto for orientado o uso da API do Twitter, devemos trocá-la pela API alternativa, que está localizada em https://labdados.com/.

https://api.twitter.com/ ------> por https://labdados.com/

Os dois lugares principais em que essa URL é utilizada são:

No script de extração de dados (extracao_api_twitter.py).Isso acontece pela primeira vez na aula 2 - Atividade 6

Na linha em que definimos a url_raw, temos a URL da API do Twitter:

### Para utilizarmos a versão do Python e suas bibliotecas é importante seguir alguns passos.

    sudo apt install python3.9

    sudo apt install python3.9-venv

    python3.9 -m venv venv

    source venv/bin/activate

    pip install requests==2.27.1

### Configurando Conexão com API Twitter

Admin/Connections
Add
Connection Id = twitter_default
Connection Type = Http
Host = https://labdados.com
Extra = {"Authorization": "Bearer XXXXXXXXXXXXXXXXXXXXXX"}

### Instalando o PySpark

O Spark tem uma particularidade, pois precisa ter o Java instalado na máquina para funcionar. Para verificarmos se ele está instalado, podemos executar o seguinte comando no terminal:

    java -version

Se tivermos o Java na máquina, nos será retornada a versão existente. Caso contrário, o sistema avisará que não reconhece este comando indicando que o Java não está instalado. Para instalá-lo, podemos usar o comando apt-get install openjdk-8-jdk-headless -qq.

    apt-get install openjdk-8-jdk-headless -qq

Caso ele retorne um erro, significa que não estamos utilizando o comando de administrador, então basta adicionar sudo ao início do comando.

    sudo apt-get install openjdk-8-jdk-headless -qq

Ao executá-lo, pedirá a senha da sua máquina, então basta digitá-la, teclar "Enter" e aguardar a instalação.

Com o Java preparado, podemos focar no Spark. Para utilizá-lo, optaremos pelo uso de um Jupyter Notebook, portanto, teclamos "Ctrl + J" para fechar o terminal.

Na lateral esquerda, criaremos uma subpasta de "src". Para isso, clique sobre ela com o botão direito do mouse e selecione "New Folder". Vamos nomeá-la como "scripts" e moveremos o arquivo de extração da API para dentro dela.

Ainda em "src", criaremos outra subpasta chamada "notebooks". Nela, criaremos um novo arquivo (opção "New File") chamado "exploracao_spark.ipynb", no qual faremos a exploração dos dados utilizando o Spark. A extensão ipynb corresponde ao Jupyter Notebook. O notebook será preparado, então vamos nos ater a ele.

Queremos concentrar nossas instalações no ambiente virtual, então teclaremos "Ctrl + Shift + P" e selecionaremos a opção "Python: Select Interpreter". Depois, vamos em "Enter path to a Python interpreter > Find" e selecionamos "pyhton3.9", dentro de "bin", subpasta de "venv", na pasta "src" (curso2 > src > venv > bin > python3.9). Assim, definimos que nosso interpretador Python padrão no ambiente do VS Code, será o do nosso ambiente virtual.

Agora, partiremos para as instalações do Jupyter Notebook e do PySpark.

Ao teclarmos "Enter" na barra superior, poderemos selecionar a opção "venv(Python 3.9.15)". Após a conexão, é possível que apareça uma mensagem informando que para rodar o notebook nessa venv, precisamos fazer a instalação de um pacote Python, então clicaremos em "Install" e o Jupyter Notebook será instalado.

Finalizada a instalação, rodaremos o comando responsável pela instalação da versão do PySpark que queremos utilizar:

    pip install pyspark==3.3.1

Para executar a célula, podemos clicar no botão de play, que localiza´-se à esquerda da célula, ou utilizar o comando "Ctrl + Enter" que, além de executá-la, cria uma célula logo abaixo.

A instalação será realizada dentro do ambiente virtual e poderia ser feita, também, pelo terminal, mas optamos pelo notebook para nos familiarizarmos melhor. Quando o carregamento encerrar, poderemos começar a usar o PySpark.

Na próxima célula, criaremos uma sessão Spark. Para isso, precisamos primeiro importar o SparkSession, portanto, execute o comando a seguir:

    from pyspark.sql import SparkSession

Em seguida, criamos a sessão em uma variável spark. Nela, passamos o construtor builder, damos o nome "twitter_transformation" para a nossa sessão e passamos o método .getOrCreate(), que deve criá-la caso já não exista.

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()
    
Aguarde o carregamento e as mensagens de êxito.

Agora que temos o Spark preparado e a sessão criada, podemos explorar nossos dados. Na aba lateral, temos acesso às nossas pastas. No momento, estamos em "exploracao_spark.ipynb", dentro da pasta "notebooks", que é uma subpasta de "src" - de mesmo nível da pasta "datalake". Então, para fazer a leitura temos que subir duas pastas e depois entrar em "datalake", este é o nosso caminho.

Vamos definir o dataframe de leitura em uma variável df que receberá o comando spark.read.json() com o caminho da pasta. Para pegar este caminho, clicamos com o botão direito do mouse sobre a pasta "datalake/twitter_datascience" e selecionar "Copy Relative Path".

Incluiremos o trecho ../../ ao início do caminho indicando que queremos subir duas pastas, entrar em "datalake" e depois em "twitter_datascience".

    df = spark.read.json("../../datalake/twitter_datascience")

É importante notar que não precisamos selecionar especificamente o arquivo JSON, pois a nossa estrutura de dados com os nomes de pastas permite a leitura de diversos dias. Sendo assim, conseguiremos ler os dias que possuem dados extraídos (no nosso caso, 2 dias) e o Spark será capaz de entender que essa informação pode ser uma coluna e separá-las em cada linha.

Execute a célula e após o carregamento do processo rode o comando de visualizalçao df.show() em uma nova célula.

    df.show()

O retorno deste comando deve exibir as 20 primeiras linhas do nosso dataframe. Note que temos uma coluna "extract_date", correspondente à data de extração, além de outras 3 colunas: "data", "includes" e "meta". São essas as informações com as quais as pessoas cientistas e analistas de dados estão trabalhando.

### Preparando o ambiente: instalando SparkSubmitOperator

A partir da versão 2.0 do Airflow os módulos dos provedores começaram a vir separados da instalação principal do Spark, tornando necessário instalar cada módulo individualmente.

Isso fez com que a instalação do Airflow ficasse menos pesada, porém agora você deve ficar atento e sempre que precisar de um módulo específico, como, por exemplo, o Spark, você deve fazer essa instalação.

Então, para instalar a versão 4.0.0 do provedor spark para o Airflow, precisamos rodar o seguinte comando de instalação:

    pip install apache-airflow-providers-apache-spark==4.0.0

Com essa instalação teremos acesso a novas opções de conexões, configurações e classes no Airflow relacionadas a ferramenta Spark.

### Conexão Spark-Airflow

Para configurar nossa conexão entre o Airflow e o Spark, precisaremos voltar à interface web do Airflow.

Tecle "Ctrl + J" para abrir o terminal.

ATENÇÃO: Sempre que for iniciar o Airflow, lembre-se de verificar se está no ambiente virtual. Para isso, basta observar se o nome do ambiente está aparecendo entre parênteses no início da linha de comando.

Se o terminal foi encerrado, é necessário exportar a variável de ambiente novamente para que possamos apontar nossas pastas e arquivos.

O comando cd.. nos permite subir uma pasta, portanto, se vamos rodar o Airflow a partir da pasta raiz e estamos em uma interna, podemos utilizá-lo.

Precisamos definir uma outra variável de ambiente na qual ficará o Spark, pois o Airflow buscará pela variável chamada "SPARK_HOME". Para isso, basta utilizarmos export SPARK_HOME e defini-la com o caminho da pasta "spark-3.1.3-bin-hadoop3.2" onde está nosso Spark.

    export SPARK_HOME=/home/aluno/Documents/curso2/spark-3.1.3-bin-hadoop3.2

Agora podemos inicializar o Airflow com o seguinte comando:

    airflow standalone

Quando o Airflow iniciar, acessamos sua interface pelo navegador através do endereço "localhost:8080/home".

Em "Admin", na barra superior, selecionamos a opção "Connections", mas, ao invés de criarmos uma nova conexão, como fizemos há algumas aulas atrás, apenas editaremos uma já existente: a do Spark, que nos foi trazida quando instalação dos provides do Spark.

Tecle "Ctrl + F", digite "spark" para buscar essa ocorrência na listagem e clique no botão "Edit record" à esquerda de "spark_default". O nome deve permanecer "spark_default"; enquanto o tipo deve ser "Spark"; e o host, "local". Feito isso, clique em salvar.

### Instalar o módulo do provedor Spark.

Para fazer com que o Airflow execute nosso script Spark, precisamos de um Operador que execute nosso script utilizando o SparkSubmit, já que esse tipo de tarefa é comum termos ao nosso dispor um operador pronto.

Esse operador faz parte de um módulo do Airflow, dentro de provedores temos o de Apache Spark, esse módulo permite que utilizemos configurações, conexões e novas classes para trazer soluções do Spark no Airflow.

Então, nosso primeiro passo é instalar esse módulo do Airflow.

    pip install apache-airflow-providers-apache-spark==4.0.0

#### Configurar a conexão do Spark com o Airflow
Já que a responsabilidade de rodar nosso código Spark é agora do Airflow, precisamos indicar para ele como queremos que o Spark seja utilizado e onde ele está instalado.

Para isso, precisamos criar a variável de ambiente SPARK_HOME onde vamos indicar a localização do Spark.

Depois podemos configurar uma conexão através da interface web do Airflow.

#### Rodar nossa DAG atualizada
Com tudo preparado, nossa última etapa é atualizar a nossa DAG. Nossa DAG agora deve incluir um novo passo, e vamos representá-lo usando um operador, o SparkSubitOperator.

Os dois principais parâmetros deste operador vão ser o application e o application_args, que são os locais onde informamos a localização do nosso script de transformação e seus parâmetros de funcionamento.

Não esqueça também de informar a ordem que as nossas operações devem acontecer, primeiro a extração dos dados e depois a transformação.

### Criando camadas

Indicamos que dentro do DataLake teríamos uma pasta chamada "Bronze" e ela contaria com o nome da extração, data da extração e o arquivo extraído.

Em relação aos dados transformados, teríamos uma pasta chamada "Silver" e dentro dela o nome da extração, nome do dataframe, que nesse caso são os dataframes de usuários e de Tweets, além da data da transformação e o arquivo refinado.

Agora vamos voltar na nossa DAG e atualizar os caminhos para que ela possa refletir essa estrutura que construímos. Para isso, abrimos o "twitter_dag" no VS Code.

Começamos modificando o twitter_operator, que faz a extração dos dados. Ele recebe um parâmetro chamado file_path e faz o join de alguns caminhos, como o datalake.

Essa é a estrutura que tinhamos planejado para o nosso DataLake. Agora vamos ativar o Airflow.

Apertamos "Ctrl + J" para abrir o terminal. Feito isso, é importante lembrar que temos que estar no nosso ambiente virtual, e ter definido as duas variáveis de ambiente AIRFLOW_HOME e SPARK_HOME. Agora sim podemos rodar o airflow standalone.

    export SPARK_HOME=/home/eugenio/Documents/curso/spark-3.1.3-bin-hadoop3.2
    export AIRFLOW_HOME=$(pwd)/airflow_pipeline

Enquanto a ferramenta inicializa o Airflow, vamos aproveitar para deixar nosso projeto mais limpo.

Apertamos "Ctrl + J" e abrimos o "Explorer" para apagar o datalake atual e o dados_transformation. Podemos fazer isso de duas formas, clicando na pasta e apertando a tecla "del" do teclado, ou clicando com o botão direito em cima dela e depois em "delete". Feito isso abre uma tela de confirmação, para concluir a ação clicamos no botão "Move to trash". Fazemos isso para rodar a DAG e checar se está tudo funcionando corretamente.

Notamos algo curioso no nosso projeto. Em "Curso 2" apareceu uma pasta chamada "(pwd)". Isso aconteceu, pois o Airflor home foi definido errado, isso fez com que ele criasse outra pasta.

Para corrigir isso apertamos "Ctrl + J" e encerramos o Airflow apertando "Ctrl + C". Depois, clicamos com o botão direito em cima da pasta "(pwd)", seguido do botão "delete" e "Move to trash".

Notamos também que em export esquecemos de colocar o cifrão antes do (pwd), então o incluímos no código.

    export SPARK_HOME=/home/eugenio/Documents/curso/spark-3.1.3-bin-hadoop3.2
    export AIRFLOW_HOME=$(pwd)/airflow_pipeline

Agora apertamos a tecla "Enter" para inicializar o airflow standalone. Feito isso ele não vai criar uma pasta nova e sim utilizar a que criamos anteriormente e que já está com nossa base de dados, como senhas e conexões.

    airflow standalone

Agora que já foi iniciado podemos voltar para nossa tela inicial do Airflow, para isso, clicamos no ícone do Airflow, localizado no canto superior esquerdo da tela.

    localhost:8080/home

Vamos apagar novamente nossa TwitterDAG, para isso clicamos nela, é a primeira que aparece na lista localizada no centro da tela. Somos direcionados para sua página. Agora, na lateral superior direita, clicamos no ícone representado pelo desenho de uma lixeira e para confirmar a ação clicamos no botão "OK".

Aguardamos o Airflow carregar uma nova DAG. Quando ela aparecer novamente na lista, clicamos nela. Agora, para inicializá-la, clicamos no botão localizado no canto superior esquerdo, ao lado do texto "DAG". Abaixo, também no canto esquerdo, ligamos o "Auto-refresh", apertando o botão.

Agora o DataLake será inicializado do zero, fazendo todas as extrações e transformações desses dados também no DataLake.

Como nossa primeira transformação já aconteceu, voltamos para o VS Code para confirmar se está tudo indo conforme o esperado.

No canto superior esquerdo da ferramenta, identificamos a pasta "datalake" que foi recriada. Abaixo também visualizamos a estrutura de pasta "bronze" que temos o nome do projeto e datas da extração, além da pasta "silver", que também tem o nome do projeto e nossos dataframes.

Agora, além do Airflow estar orquestrando essa nova etapa que, após a extração, deve acontecer a transformação dos dados, ele também está construindo o DataLake como projetamos.

Mas, se voltarmos no código, notamos algumas coisas que podem ser melhoradas. Para visualizá-lo apertamos "Ctrl + J", depois "Ctrl + B".

Ao invés de deixar a estrutura do DataLake evidente em cada etapa, seria interessante deixá-la mais dinâmica e centralizada. Isso, pois, se o DataLake precisasse passar por modificações de localização ou estrutura de pastas, por exemplo, não precisaríamos mudar diversos pontos.

### Preparando o ambiente: instalando Spark e SparkSubmit

Para utilizar o SparkSubmit vamos precisar instalar o Spark completo.

Primeiro, vamos fazer o download do Spark, que está disponível através do site oficial. Já deixamos o link preparado para fazer o download da versão 3.1.3 do Spark e a versão 3.2 do Hadoop.

Vamos baixar através do comando wget no terminal com a URL de download.

    wget https://archive.apache.org/dist/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz

Depois de terminado o download podemos descompactar o arquivo através do comando tar e passando por parâmetros as configurações -xvzf e o nome do arquivo.

    tar -xvzf spark-3.1.3-bin-hadoop3.2.tgz

Apenas com esses passos já somos capazes de utilizar o SparkSubmit.

Vamos exemplificar isso rodando o script transformation.py.

    cd spark-3.1.3-bin-hadoop3.2

Para isso, executamos no terminal dentro da pasta descompactada o caminho bin e o arquivo spark-submit.

    ./bin/spark-submit 

Esse executável recebe como parâmetro a localização do script a ser executado, no nosso caso é o caminho até o arquivo transformation.py.

    ./bin/spark-submit …/src/spark/transformation.py

Também vamos precisar passar mais três parâmetros, mas esses são pré requisitos do nosso script. Os parâmetros são: src, dest e process date.

    ./bin/spark-submit …/src/spark/transformation.py --src --dest --process-date

Para o src colocamos o caminho até os dados brutos no nosso data lake. Para o dest podemos colocar dentro de uma pasta temporária de output e o process-date você pode colocar a data em que está rodando o seu processo.

./bin/spark-submit …/src/spark/transformation.py --src …/datalake/twitter_datasciencie --dest   …/src/spark/output --process-date 2022-08-15

Executando esse comando vamos ter os dados processados na pasta output.

Viu como foi simples instalar o Spark e usar o SparkSubmit?

### Código para habilitar programas do Spark


    wget https://archive.apache.org/dist/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz

    tar -xvzf spark-3.1.3-bin-hadoop3.2.tgz

    dir

    ./bin/spark-submit /home/eugenio/Documents/curso/src/Spark/transformation.py --src /home/eugenio/Documents/curso/curso2/datalake/twitter_datascience --dest /home/eugenio/Documents/curso/src/Spark --process-date 2023-07-16

