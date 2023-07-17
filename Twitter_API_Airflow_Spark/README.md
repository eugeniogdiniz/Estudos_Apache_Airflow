# Separando a estrutura de dados

Perceba que nosso dataframe possui 4 colunas com seus respectivos dados.

Separamos as informações simplificando o esquema, embora ainda seja possível compreender as correspondências entre elas no momento da leitura.

Para finalizar, salvaremos as estruturas que criamos com o seguinte comando:

    tweet_df.coalesce(1).write.mode("overwrite").json('output/tweet')

O método .coalesce(1) com o parâmetro 1 é responsável por pegar as informações que o Spark quebra em pedaços e as une permitindo que sejam salvas em um só arquivo.

ATENÇÃO: Quando estamos trabalhando com um grande volume de dados, é preferível salvá-los em diversos arquivos, o que não é o nosso caso.

Em seguida, temos o método .write.mode("overwrite"). O parâmetro utilizado serve para que, ao rodarmos o notebook novamente, não ocorra nenhum problema ao encontrar os dados já salvos, e, portanto, apenas os sobrescreva. Depois, definimos o tipo .json() passando como parâmetro o nome da pasta na qual queremos que sejam salvos 'output/tweet'. Este é o comando para os dados dos tuítes.

Usaremos uma estrutura semelhante para os dados dos usuários:

    user_df.coalesce(1).write.mode("overwrite").json('output/user')

Ao final, executaremos ambos os comandos em uma mesma célula, da seguinte forma:

    tweet_df.coalesce(1).write.mode("overwrite").json('output/tweet')
    user_df.coalesce(1).write.mode("overwrite").json('output/user')

Execute-a e aguarde o carregamento dos comandos. Em seguida, na barra lateral esquerda, note que temos uma pasta chamada "output" com as subpastas "tweet" e "user" com seus respectivos arquivos.

Definimos o tratamento ideal para os nossos dados e os salvamos em um arquivo. O próximo passo, agora, é transferir esses dados para um script que deve ser orquestrado pelo Airflow para que ele aplique o processo de transformação.

## Preparando o ambiente: instalando Spark e SparkSubmit

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

    ./bin/spark-submit …/src/spark/transformation.py --src …/datalake/twitter_datasciencie --dest     …/src/spark/output --process-date 2022-08-15

Executando esse comando vamos ter os dados processados na pasta output.

Viu como foi simples instalar o Spark e usar o SparkSubmit?


### Código para habilitar programas do Spark


    wget https://archive.apache.org/dist/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz

    tar -xvzf spark-3.1.3-bin-hadoop3.2.tgz

    dir

    ./bin/spark-submit /home/eugenio/Documents/curso/src/Spark/transformation.py --src /home/eugenio/Documents/curso/curso2/datalake/twitter_datascience --dest /home/eugenio/Documents/curso/src/Spark --process-date 2023-07-16

