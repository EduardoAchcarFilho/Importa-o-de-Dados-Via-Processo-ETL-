## Importação de Dados (Via Processo ETL)

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

MAPEAMENTO:

def verificar_ou_criar_mapeamento_json(file_name='column_mappings.json'):

    default_mappings = {
        ID: [ID, Código, Identificador],
        "Produto": ["Produto", "Descrição", "Item"],
        "Unidade": ["UNIDADE", "Un", "Unidade de Medida", "Unidade"],
        "Ncm": ["NCM", "ncm", "Ncm"],
        "Cest": ["CEST", "cest", "Cest"],
        "Custo": ["Custo", "Preço de Custo", "Valor Custo"],
        "Margem": ["Margem", "MargemLucro", "Margem de Lucro"],
        "Unitário": ["Unitário", "Preço Unitário", "Valor Unitário", "Unitario"],
        "CódigoBarras": ["CodigoBarras", "Código de Barras", "EAN"]
    }
    if not os.path.exists(file_name):
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(default_mappings, f, indent=4)
    else:
        with open(file_name, 'r', encoding='utf-8') as f:
            existing_mappings = json.load(f)

        for key, possible_names in default_mappings.items():
            if key in existing_mappings:
                for new_name in possible_names:
                    if new_name not in existing_mappings[key]:
                        existing_mappings[key].append(new_name)
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(existing_mappings, f, indent=4)
    return file_name

ALGUNS DOS TRATAMENTOS:
- Remover linhas duplicadas com base no mapeamento PRODUTO
df = df.drop_duplicates(Produto_column_name)
- Remover linhas onde a coluna 'Produto' tem valores nulos
df = df.dropna(subset=[Produto_column_name])
- Remover espaços em branco extras de uma coluna de string
df[Produto_column_name] = df[Produto_column_name].str.strip()
- Remover espaços em branco à esquerda da coluna 'Produto_column_name'
df[Produto_column_name] = df[Produto_column_name].str.lstrip()
- Colocar todos os valores em maiúsculas
df[Produto_column_name] = df[Produto_column_name].str.upper()

![Capturar](https://github.com/user-attachments/assets/55cdd541-3301-45b4-ba02-5aa66e19248f)

![Capturar1](https://github.com/user-attachments/assets/6e2d84f9-ff1d-4ecc-88c7-1efe3b1b73c1)


