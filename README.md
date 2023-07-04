Apache Airflow é um orquestrador de fluxos, ou seja, com ele você é capaz de decidir em qual momento e em quais condições algum programa seu irá rodar. Ele é um sistema de gerenciamento de fluxo de trabalho de código aberto (open source) projetado para criar, agendar e monitorar, de forma programática, pipelines de dados e fluxos de trabalho.

Essa ferramenta foi criada em 2014 pelo Airbnb para orquestrar os complexos fluxos de trabalho da empresa. Mas, desde o início de 2019, o Airflow se tornou um projeto de alto nível da Apache Software Foundation.

O Airflow é todo escrito em Python e possui características como: dinamicidade, extensível, escalável e adequado para lidar com a orquestração de pipelines de dados complexos.

Principais conceitos do Airflow
O Airflow organiza seus fluxos de trabalho em DAGs, que são basicamente pipelines de dados definidos utilizando a linguagem Python. Cada DAG é composto por um conjunto de tarefas que são utilizadas para implementar uma determinada lógica no pipeline. Sendo que cada tarefa é definida pela instância de um operador.

De forma resumida, os principais conceitos do Airflow são:

DAG: fluxo de trabalho definido em Python.
Task: unidade mais básica de um DAG.
Operator: encapsula a lógica para fazer uma unidade de trabalho (task).

O Airflow possui 4 componentes principais que devem estar em execução para que ele funcione corretamente. São eles o web server, scheduler, banco de dados e executor.

O web server é um servidor feito em Flask, um framework web Pyhton, que serve para nos apresentar a interface de usuário do Airflow, portanto, é por meio dele que acessamos esta interface.

O scheduler ("agendador" em português) é responsável pelo agendamento da execução das tarefas dos DAGs, então ele determina quais tarefas serão realizadas, onde serão executadas e em qual ordem isso acontecerá.

O banco de dados, por sua vez, serve para armazenar todos os metadados referentes aos DAGs. Sendo assim, ele registra o horário em que as tarefas foram executadas, quanto tempo cada task levou para ser realizada e o estado de cada uma - se foram executadas com sucesso ou falha, e outras informações relacionadas.

Por fim, temos o executor, que é o mecanismo de execução das tarefas. Ou seja, ele é responsável por descobrir quais recursos serão necessários para executar essas tasks.

Vamos entender como esses componentes funcionam.

A princípio, temos um usuário que pode acessar tanto a pasta onde criaremos nossos DAGs quanto a interface do Airflow por meio da execução do web server. Há, ainda, um banco de dados que armazena as informações sobre cada uma das tarefas dos DAGs. O scheduler será responsável por ler o status das tarefas no banco de dados e decidir qual será executada e em qual ordem. Em estreita colaboração com o scheduler, o executor decide quais recursos serão utilizados para executar essas tarefas à medida que forem agendadas pelo scheduler.

Agora que entendemos os conceitos e componentes principais do Airflow, vamos iniciar sua instalação!


# Vamos explorar a interface do Airflow!

Note que há duas mensagens na aba superior da página. Elas avisam que algumas configurações que estamos utilizando não podemos ser usadas se quisermos colocar algo em produção, mas, como não é nossa intenção, não precisamos nos ater a esses avisos.

Há o título "DAGs" com uma listagem dos DAGs do Airflow. Como não criamos nenhum, os DAGs listados são exemplificações disponibilizadas pelo próprio Airflow.

Perceba que há várias colunas, vamos entender do que se tratam. A coluna "Runs" diz respeito ao status de cada uma das execuções dos DAGs; "Schedule" mostra o intervalo de agendamento de cada DAG; "Last Run" traz a data e o horário da última execução do DAG, e "Next Run" a próxima data de execução; por último, "Recent Tasks" mostra as tarefas dos DAGs - quantas foram executadas, quantas obtiveram sucesso e falha, quantas ainda estão executando e alguns outros status que conheceremos mais à frente.

Vamos acessar "example_branch_datetime_operator_2" para visualizar a interface de um DAG. Observe que no canto superior esquerdo há um botão de "Pause/Unpause DAG" que nos permite ativá-lo. Em seguida, há uma listagem com opções de abas que mostram diferentes informações sobre o DAG e suas execuções. Veremos algumas delas.

Na aba "Grid", observamos que há alguns nomes, como "datetime_branch", "date_in_range" e "date_outside_range", que correspondem às tarefas do DAG. Ativando-o no botão de "Pause/Unpause" e, em seguida, clicando em "Auto-refresh", essas tarefas começam a ser executadas e nos mostra uma coluna seguida de pequenos quadrados que representam, respectivamente, o DAG run e as instâncias de tarefas. As cores dos quadros, por sua vez, definem o status de cada tarefa, e podemos entender essa definição consultando a legenda de cores que aparece próxima à lateral superior direita. Ao clicar na coluna do DAG run ou no quadrado de instância da task, temos acesso a algumas informações específicas, como o horário de início e fim da execução.

Em "Graph", também temos os nomes das tarefas do DAG, no entanto, as tasks aparecem interligadas por setas que indicam como elas se relacionam no campo de execução. Já na aba "Calendar", são mostrados os status dos DAGs run em um calendário, indicando as execuções bem sucedidas e os agendamentos de execução.

A aba "Code" apresenta o código por trás do DAG que estamos visualizando, mas não é possível editá-lo. Na parte final desta página, no trecho que utiliza dois operadores de maior >>, é onde definimos a relação entre as tarefas mostrada na aba "Graph". Perceba que as tasks possuem nomes diferentes porque estão sendo representadas por algumas variáveis.

Existem outras abas das quais não falaremos, mas há uma atividade disponibilizada para que você conheça um pouco melhor sobre cada uma.

Até aqui, já passamos pelos conceitos e componentes principais do Airflow, aprendemos a instalá-lo e nos familiarizamos com sua interface. A seguir, iniciaremos o desenvolvimento do nosso primeiro DAG. Vamos lá?