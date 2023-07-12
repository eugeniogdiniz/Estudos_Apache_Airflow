# Anotações para execução de API e criaçao de um Data Lake

Para o uso do airflow e de outras ferramentas da APACHE é recomendado o uso de uma VM (maquina virtual) com o sistema linux integrado nele. Para o uso do airflow no linux, os comandos abaixo devem ser rodados no terminal do linux:

#
sudo apt update; sudo apt upgrade; sudo add-apt-repository ppa:deadsnakes/ppa; sudo apt install python3.9; sudo apt install python3.9-venv; cd Documentos; mkdir selenium_airflow; cd selenium_airflow; python3.9 -m venv venv; source venv/bin/activate; pip install 'apache-airflow==2.3.2' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.2/constraints-3.9.txt"; pip install --upgrade pip; export AIRFLOW_HOME=~/Documentos/selenium_airflow; airflow standalone
#

Feito isso, ao criar o ambiente virtual 'venv' algumas coisas devem ser anotadas.

### Criar uma conexão na interface web do Airflow
A interface web do Airflow tem uma página dedicada a administração de conexões, onde podemos consultar e editar conexões existentes, mas podemos também criar novas. No nosso caso, queremos criar uma conexão para a API do twitter e nessa conexão vamos precisar informar um nome, o tipo de conexão, url e o nosso token de autenticação.

### Construir o TwitterHook
Os Hooks são ótimas soluções para centralizarmos toda a parte de conexão. No nosso Hook, vamos precisar herdar o HttpHook, que tem toda a implementação necessária para fazer nossa requisição configurada na interface web do Airflow.

### Criar o método de criação de URL
Precisamos também criar um método que será responsável por construir a nossa URL com os parâmetros definidos no construtor do Hook. Um ponto importante é que a base da nossa URL agora está armazenada no airflow e devemos requisitá-la pelo HttpHook.

### Criar o método que prepara a requisição
Embora o HttpHook seja responsável por fazer de efetivamente a requisição, ainda é necessário preparar nossa requisição, informando a sessão e a URL que será utilizada, para isso vamos criar o método de conexão com o endpoint.

### Criar o método de paginação
O método de paginação será responsável por utilizar a função de conexão, fornecendo a URL e armazenando os retornos. Ele vai iterar utilizando o next_token para criar novas URLs e assim ter acesso a todas as páginas de resposta da API.

### Conectar todas os métodos
Com todos os métodos prontos, podemos criar o método padrão run que é responsável por pegar a sessão que utilizaremos na conexão, criar a URL inicial e utilizar o método de paginação que vai completar todos os passos da nossa extração.

## Sobre operadores
### O que são?

No Airflow, operadores determinam o que será executado em uma tarefa, descrevendo um único passo. Um operador possui três características:

![image](https://github.com/eugeniogdiniz/Estudos_Apache_Airflow/assets/55545471/e7855fe5-fdac-4571-b3e6-0a307b707ecc)

1) Idempotência: Independentemente de quantas vezes uma tarefa for executada com os mesmos parâmetros, o resultado final deve ser sempre o mesmo;

2) Isolamento: A tarefa não compartilha recursos com outras tarefas de qualquer outro operador;

3) Atomicidade: A tarefa é um processo indivisível e bem determinado.

Operadores geralmente executam de forma independente, e o DAG vai garantir que operadores sejam executados na ordem correta. Quando um operador é instanciado, ele se torna parte de um nó do no DAG.

Todos os operadores derivam do operador base chamado BaseOperator, e herdam vários atributos e métodos. Existem 3 tipos de operadores:

Operadores que fazem uma ação ou chamam uma ação em outro sistema;
Operadores usados para mover dados de um sistema para outro;
Operadores usados como sensores, que ficam executando até que um certo critério é atingido. Sensores derivam da BaseSensorOperator e utilizam o método poke para testar o critério até que este se torne verdadeiro ou True, e usam o poke_interval para determinar a frequência de teste.

## Sobre Data Lakes

Data Lake é um local de armazenamento onde podemos centralizar dados de diversas fontes, como APIs, banco de dados e logs. O valor de centralizar todas essas informações está em trazer uma fonte rica de dados para as pessoas analistas e cientistas de dados. Com esse grande volume de dados estruturados e não estruturados as possibilidades da criação de dashboards e modelos aumentam muito.

Essa solução de armazenamento traz diversas vantagens como centralizar a informação de negócios em um só lugar e também permite dados estruturados e não estruturados.

Existem também desvantagens, como necessidades de um grande local de armazenamento e processamento.

No nosso projeto de extração de dados a solução do Data Lake é a ideal já que vamos armazenar nossos dados brutos.

### Definição de Data Warehouse e Data Lake

Bases de dados são tipicamente estruturadas e organizadas para eventuais consultas, mas não são desenvolvidas para que possamos realizar a análise dos dados que as compõem.

A escolha de um Data Warehouse ou Data Lake pode estar relacionada aos dados que iremos extrair e aos procedimentos que iremos realizar na sequência.

Por isso, precisamos entender em detalhe como cada um funciona.

#### Data Warehouse
Um Data Warehouse é composto por diversas bases de dados - normalmente bases estruturadas, e é utilizado para o business intelligence (BI). Por armazenarem dados processados, economizam espaço de armazenamento com dados que correm o risco de nunca serem usados.

Através do Data Warehouse, conseguimos consumir todas essas bases de dados e criar uma camada otimizada para realizar a análise de dados com ferramentas como o Power BI.

A organização dessas bases de dados (esquema) é feita durante a sua importação.

#### Data lake
O Data Lake pode ser visto como um repositório centralizado para armazenar dados estruturados e não estruturados.

Ele pode armazenar dados não processados sem que haja a necessidade de nenhum tipo de transformação. Além disso, pode guardar qualquer tipo de formato, como imagens, textos, vídeos, modelos de machine learning e dados coletados em tempo real, implicando diretamente na sua capacidade de armazenamento.

O processamento pode ser feito na exportação e, dessa forma, a organização dos dados é feita na leitura. Por mais que ele seja de rápida leitura, pode armazenar tantos dados variados que acaba gerando um pântano de dados se não forem implementadas técnicas para manter a qualidade e a governança dos dados.

Geralmente são explorados pelos cientistas de dados e analistas de BI.

#### Data Lake vs Data Warehouse
Tanto data warehouses quanto data lakes podem armazenar dados, mas existem diferenças em relação a escala e número de fontes de dados. Uma organização pode precisar de um data lake, data warehouse e bases de dados para diferentes usos.

Em resumo:

O Data Warehouse é um local centralizado para dados estruturados e possui características conhecidas com antecedência para a sua construção e processo de extração, transformação e carregamento de dados.

O Data Lake tem uma proporção muito maior onde dados de várias fontes podem se encontrar. Todos os tipos de dados são permitidos, não importando se são estruturados ou não.

## Sobre DAG's
DAG é uma abreviação para “Directed Acyclic Graphs” - Grafos Acíclicos Dirigidos, em tradução livre - sendo que cada uma dessas palavras significam as seguintes coisas:

Grafos: ferramenta matemática na qual há nós e arestas responsáveis por conectar esses nós;
Direcionado: indica que o fluxo de trabalho se dá apenas em uma direção; e
Acíclico: significa que a execução não entrará em um laço de repetição. Então, eventualmente, acabaremos em um nó final que não estará conectado com o nó inicial.
A grande ideia do Airflow é dividir um trabalho grande, com uma ou mais etapas, em “tarefas” individuais que chamaremos de tasks que juntas formam um DAG. Cada task é responsável por implementar determinada lógica no pipeline.

Dessa forma, podemos dizer que um DAG é basicamente um conjunto de tarefas. Sendo cada tarefa a unidade mais básica de um DAG.

Caso queira se aprofundar nesse conceito, sugiro a leitura da documentação e de um artigo que temos aqui na plataforma:

### Criar a nossa DAG
Criamos uma DAG quando testamos o operador, mas agora vamos criar nossa DAG definitiva. Ela precisará ter seu próprio arquivo e pasta para que o Airflow consiga encontrá-la e registrá-la na sua interface web e schedule. Nossa DAG será configurada com duas principais informações:

### data de início e frequência em que a DAG será ativada.
Definir todos os parâmetros necessários para a extração
Dentro da nossa DAG vamos instanciar os operadores que ela será responsável por executar. No nosso caso será apenas um. Nele vamos passar todos os parâmetros de configuração da nossa extração. Data início e final, query e local de armazenamento.

### Deixar o Airflow responsável por definir os parâmetros de data necessários.
Em relação às datas e localizações da pasta será necessário usar um recurso que faça esses valores serem definidos no momento que o código ou DAG for executado. Permitindo assim nosso código ser dinâmico, bastando apenas alterar os valores da DAG para a mudança afetar todo o restante da cadeia, criação das pastas pelo operador e extração dos dados pelo Hook.


