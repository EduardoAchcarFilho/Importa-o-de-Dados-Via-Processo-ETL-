Importação de Dados (Via Processo ETL)
Vou postar um outro projeto que eu usava, é um app de Importação de Dados (Via Processo ETL).
A unica interração era selecionar o arquivo .csv e quando necessário mapear novas colunas mas aqui decidi colocar com MAIS componentes para melhor visualização e compreenção dele.

Este código tem a finalidade de realizar o mapeamento(das colunas do arquivo.csv), tratamento e inserção em um banco de dados SQL, e como o banco destino não mudava sua estrutura, decidi automatizar este processo.

O mapeamentos de colunas é baseado em persistência de aprendizado ,cada vez que o sistema detecta uma nova coluna(que não esta no mapeamento padrão) e o usuário a categoriza, essa informação é salva em arquivo JSON(que é uma boa prática, pois permite flexibilidade na configuração de diferentes fontes de dados). Isso significa que na próxima vez que um arquivo com a mesma coluna for carregado, o sistema já saberá como mapeá-la corretamente.
Essa abordagem garante que o mapeamento padrão sejam atualizados sempre que novas colunas forem adicionadas, permitindo um sistema mais flexível e adaptável
Este é um pequeno exemplo de como implementar um aprendizado incremental.

Algumas abordagens de tratamento feitas:

. Remover Duplicados

. Tratar Valores Ausentes

. Conversão de Tipos de Dados
   
. Formatação de Strings

. Calcular Marguem de Lucro
    
. Cálculo de Colunas Adicionais
 
. Normalização ou Padronização

. Agrupamento e Agregação

. Filtragem de Dados
     
. Aplicação de Regras de Negócio Específicas
   

Utilizei a biblioteca pandas para manipulação de dados, pyodbc para conexão com o SQL, e Streamlit para visualização e feedback do processo.
