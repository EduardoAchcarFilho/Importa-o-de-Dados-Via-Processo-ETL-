import pandas as pd
import pyodbc
import streamlit as st
import json
import os
import numpy as np

# Configuração de conexão com o banco de dados
DADOS_CONEXAO = (
    "Driver={SQL Server};"
    "Server=DUXPC;"
    "Database=Conversao;"
    "Trusted_Connection=yes;"
)

# Função para verificar ou criar o mapeamento JSON de colunas
def verificar_ou_criar_mapeamento_json(file_name='column_mappings.json'):
    default_mappings = {
        "ID": ["ID", "Código", "Identificador"],
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

verificar_ou_criar_mapeamento_json()

# Função para carregar mapeamento de colunas
def load_mapping(file_name='column_mappings.json'):
    try:
        with open(file_name, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return verificar_ou_criar_mapeamento_json(file_name)

# Função para salvar o mapeamento atualizado
def save_mapping(updated_mapping, file_name='column_mappings.json'):
    with open(file_name, 'w') as f:
        json.dump(updated_mapping, f, indent=4)

# Carrega o mapeamento existente ou inicializa
expected_columns = load_mapping()        

# Função para obter o nome de coluna correspondente no DataFrame
def get_column_name(df, possible_names):
    for name in possible_names:
        if name in df.columns:
            return name
    return None

# Função para gerar lista de colunas válidas
def get_valid_columns(*cols):
    return [col for col in cols if col is not None]

# Função para manipular custos, unitários e margens
def resolver_custos_unitario_margem(df, mapping, column_mappings_file='column_mappings.json'):
    with open(column_mappings_file, 'r', encoding='utf-8') as f:
        column_mappings = json.load(f)

    custo_col = get_column_name(df, column_mappings["Custo"])
    unitario_col = get_column_name(df, column_mappings["Unitário"])
    margem_col = get_column_name(df, column_mappings["Margem"])

    if not custo_col or not unitario_col or not margem_col:
        raise KeyError("Coluna não encontrada no DataFrame. Verifique os mapeamentos.")

    df = df.copy()
    df['Custo_Alterado'] = False
    missing_custo_mask = df[custo_col].isna()
    df.loc[missing_custo_mask, custo_col] = df[unitario_col]
    df.loc[missing_custo_mask, 'Custo_Alterado'] = True

    df['Unitario_Alterado'] = False
    missing_unitario_mask = df[unitario_col].isna()
    df.loc[missing_unitario_mask, unitario_col] = df[custo_col]
    df.loc[missing_unitario_mask, 'Unitario_Alterado'] = True

    df['Margem_Incorreta'] = False
    df_margens_disponiveis = df[pd.notna(df[margem_col])].copy()
    df_margens_disponiveis['Margem_Calculada'] = (
        (df_margens_disponiveis[unitario_col] - df_margens_disponiveis[custo_col])
        / df_margens_disponiveis[custo_col]
    ) * 100

    margem_mask = np.abs(df_margens_disponiveis[margem_col] - df_margens_disponiveis['Margem_Calculada']) > 0.01
    df.loc[df_margens_disponiveis.index[margem_mask], margem_col] = df_margens_disponiveis.loc[margem_mask, 'Margem_Calculada']
    df.loc[df_margens_disponiveis.index[margem_mask], 'Margem_Incorreta'] = True

    # Formatar as colunas de custo e unitário
    df[custo_col] = df[custo_col].apply(lambda x: f"{float(x):,.2f}".replace(",", "_").replace(".", ",").replace("_", ""))
    df[unitario_col] = df[unitario_col].apply(lambda x: f"{float(x):,.2f}".replace(",", "_").replace(".", ",").replace("_", ""))

    return df

# Função para inserção genérica de dados no banco de dados
def inserir_dados(df, colunas_mapeadas, tabela, comando_sql):
    conn = pyodbc.connect(DADOS_CONEXAO)
    cursor = conn.cursor()
    progress_bar = st.progress(0)
    progress_text = st.empty()
    total_rows = len(df)

    try:
        for index, row in df.iterrows():
            parametros = tuple(row[col] for col in colunas_mapeadas)
            cursor.execute(comando_sql, parametros)

            progress_value = min((index + 1) / total_rows, 1.0)
            progress_bar.progress(progress_value)
            progress_text.text(f"{int(progress_value * 100)}% concluído")

        conn.commit()
        st.success(f"{tabela} inserido com sucesso!")
        progress_bar.progress(1.0)
        progress_text.text("100% concluído")

    except Exception as e:
        st.error(f"Erro ao inserir {tabela}: {e}")

    finally:
        cursor.close()
        conn.close()

def selecionar_colunas(df, colunas_selecionadas=None):
    mapping = {}
    
    if colunas_selecionadas:
        for coluna in colunas_selecionadas:
            if coluna == "ID":
                mapping['ID'] = coluna
            elif coluna == "Produto":
                mapping['Nome'] = coluna
            elif coluna == "UNIDADE":
                mapping['UN'] = coluna
            elif coluna == "NCM":
                mapping['NCM'] = coluna
            elif coluna == "CEST":
                mapping['CEST'] = coluna
            elif coluna == "Custo":
                mapping['Custo'] = coluna
            elif coluna == "Margem":
                mapping['Margem'] = coluna
            elif coluna == "Unitario":
                mapping['Unitario'] = coluna
            elif coluna == "CodigoBarras":
                mapping['CodigoBarras'] = coluna  

    return mapping

# Funções específicas de inserção
def inserir_produtos(df, mapping, column_mappings_file='column_mappings.json'):
    column_mappings = load_mapping(column_mappings_file)
    colunas = [get_column_name(df, column_mappings[key]) for key in ["ID", "Produto", "Unidade", "Ncm", "Cest"]]
    comando_sql = """INSERT INTO Produtos (ID_Prod, Descricao, UN, NCM, CEST) VALUES (?, ?, ?, ?, ?)"""
    inserir_dados(df, colunas, 'Produtos', comando_sql)

def inserir_precos(df, mapping, column_mappings_file='column_mappings.json'):
    column_mappings = load_mapping(column_mappings_file)

    colunas = [get_column_name(df, column_mappings[key]) for key in ["ID", "Custo", "Margem", "Unitário"]]

    custo_col = get_column_name(df, column_mappings["Custo"])
    unitario_col = get_column_name(df, column_mappings["Unitário"])

    # Converter os valores de custo e margem para float
    df[custo_col] = df[custo_col].str.replace(',', '.').astype(float)
    df[unitario_col] = df[unitario_col].str.replace(',', '.').astype(float)

    comando_sql = """INSERT INTO ProdPreco (ID_Prod, PrecoCusto, MargemLucro, PrecoUnitario) VALUES (?, ?, ?, ?)"""

    inserir_dados(df, colunas, 'Preços', comando_sql)

def inserir_codigo_barras(df, mapping, column_mappings_file='column_mappings.json'):
    column_mappings = load_mapping(column_mappings_file)
    colunas = [get_column_name(df, column_mappings[key]) for key in ["ID", "CódigoBarras"]]
    comando_sql = """INSERT INTO CodBarras (ID_Prod, Cod_Barras) VALUES (?, ?)"""
    
    # Converter apenas a coluna CódigoBarras para string
    #df[get_column_name(df, column_mappings["CódigoBarras"])] = df[get_column_name(df, column_mappings["CódigoBarras"])].astype(str)
    df.loc[:, get_column_name(df, column_mappings["CódigoBarras"])] = df[get_column_name(df, column_mappings["CódigoBarras"])].astype(str)
    
    inserir_dados(df, colunas, 'Código de Barras', comando_sql)

# Configurações do Streamlit
st.set_page_config(page_title="Importação de Dados(Processo ETL)", page_icon="🛠️", layout="wide")

def main():
    df1 = None
    condicao = None
    
    # Título principal
    st.markdown("<h1 style='text-align: center;'>Importação de Dados (Via Processo ETL)</h1>", unsafe_allow_html=True)
    
    # Upload do arquivo CSV
    uploaded_file = st.file_uploader("Escolha um arquivo CSV", type=["csv"])
    
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        
        mapping_file = verificar_ou_criar_mapeamento_json()
        with open(mapping_file, 'r', encoding='utf-8') as f:
          column_mappings = json.load(f)

        id_column_name = get_column_name(df, column_mappings["ID"])
        Produto_column_name = get_column_name(df, column_mappings["Produto"])
        Unidade_column_name = get_column_name(df, column_mappings["Unidade"])
        ncm_column_name = get_column_name(df, column_mappings["Ncm"])
        cest_column_name = get_column_name(df, column_mappings["Cest"])
        barras_column_name = get_column_name(df, column_mappings["CódigoBarras"])

        if Produto_column_name is not None:
           # Remover linhas duplicadas com base no mapeamento PRODUTO
           df = df.drop_duplicates(Produto_column_name)
           # Remover linhas onde a coluna 'Produto' tem valores nulos
           df = df.dropna(subset=[Produto_column_name])
           # Remover espaços em branco extras de uma coluna de string
           df[Produto_column_name] = df[Produto_column_name].str.strip()
           # Remover espaços em branco à esquerda da coluna 'Produto_column_name'
           df[Produto_column_name] = df[Produto_column_name].str.lstrip()
           # Colocar todos os valores em maiúsculas
           df[Produto_column_name] = df[Produto_column_name].str.upper()   

        if Unidade_column_name is not None:
           # Preencher valores nulos em uma coluna específica com um valor padrão
           df[Unidade_column_name] = df[Unidade_column_name].fillna('UN') 

        if ncm_column_name is not None:
           # Atualizando a coluna 'ncm_column_name' para remover o ponto decimal e converter para string
           df[ncm_column_name] = df[ncm_column_name].apply(
           lambda x: str(int(x)) if pd.notna(x) else ''
           )

        if cest_column_name is not None:
           # Substituir valores NaN ou None por vazio ('')
           df[cest_column_name] = df[cest_column_name].fillna('')

        if barras_column_name is not None:   
           # Preencher valores nulos em uma coluna específica com um valor padrão
           df[barras_column_name] = df[barras_column_name].fillna(df[id_column_name].astype(str))

           

        if id_column_name is None or Produto_column_name is None or Unidade_column_name is None or ncm_column_name is None or cest_column_name is None or barras_column_name is None:
            st.error("Colunas não encontrada no DataFrame.Faça o Mapeamento e Tente Novamente.")
            condicao = 1
        
            # Mapear colunas reais com os nomes esperados
            column_mapping = {}
            new_columns = set(df.columns)

            for key, possible_names in expected_columns.items():
                for col in df.columns:
                    if col in possible_names:
                        column_mapping[key] = col
                        new_columns.discard(col)  # Remover a coluna mapeada do conjunto de novas colunas
                        break
                if key not in column_mapping:
                    column_mapping[key] = None  # Se não encontrar nenhuma correspondência

            # Inicializar session state para novas colunas
            if 'new_columns_mapping' not in st.session_state:
                st.session_state['new_columns_mapping'] = {}

            # Verificar se há colunas novas não mapeadas e pedir ao usuário para classificá-las
            if new_columns:
                st.write("Novas colunas detectadas:")
                for new_col in new_columns:
                    # Exibir uma caixa de seleção para o usuário escolher a que categoria essa coluna pertence
                    if new_col not in st.session_state['new_columns_mapping']:
                        category = st.selectbox(
                            f"Como você categorizaria a coluna '{new_col}'?",
                            ["", "ID", "Produto", "Unidade", "Ncm", "Cest", "Custo", "Margem", "Unitário", "CódigoBarras", "Nenhuma"],
                            key=new_col  # Usa a coluna como chave para manter o estado correto
                        )
                        if category and category != "Nenhuma":
                            # Armazenar a escolha do usuário no session state
                            st.session_state['new_columns_mapping'][new_col] = category
                            expected_columns[category].append(new_col)

                # Botão para confirmar e salvar as categorias
                if st.button("Confirmar Categorias"):
                    # Atualiza o mapeamento globalmente com o que o usuário categorizou
                    for col, cat in st.session_state['new_columns_mapping'].items():
                        expected_columns[cat].append(col)
                    # Salvar o mapeamento atualizado no arquivo JSON
                    save_mapping(expected_columns)
                    verificar_ou_criar_mapeamento_json()
                    st.success("Categorias confirmadas e mapeamento salvo!")
                    condicao = 0
                    st.rerun()
        else:
            condicao = 0
            df[id_column_name] = df[id_column_name].astype(str).str.replace(',', '', regex=False)
            # Passo 1: Identificar os índices onde barras_column_name é igual a id_column_name
            indices_iguais = df.index[df[barras_column_name] == df[id_column_name]].tolist() 
            # Converter a coluna para string, remover a vírgula e depois converter para int
            # Adicionar uma pergunta para o usuário escolher o tratamento da coluna "ID"
            escolha_id = st.radio(
                "Como deseja tratar a coluna 'ID'?",
                ("Manter original", "Substituir por sequência numérica")
            )
            
            # Modificar a coluna 'ID' com base na escolha do usuário
            if escolha_id == "Substituir por sequência numérica":
              df[id_column_name] = range(1, len(df) + 1)
              for idx in indices_iguais:
                df.at[idx, barras_column_name] = df.at[idx, id_column_name]
                # Converter a coluna CodigoBarras para string
                df[barras_column_name] = df[barras_column_name].astype(str)

            column_mapping = {}
            new_columns = set(df.columns)
            for key, possible_names in expected_columns.items():
                for col in df.columns:
                    if col in possible_names:
                        column_mapping[key] = col
                        new_columns.discard(col)  # Remover a coluna mapeada do conjunto de novas colunas
                        break
                if key not in column_mapping:
                    column_mapping[key] = None 
            
            # Exibir o DataFrame
            df1 = df
            st.dataframe(df)
            st.markdown("---")    

        
    #if df1 is not None:
    if condicao == 0:
        col1, col2, col3 = st.columns([1.6, 1.6, 1]) 

        # Tabela de Produtos
        with col1:
            st.markdown("<h2 style='text-align: center;'>Tabela de Produtos</h2>", unsafe_allow_html=True)
            colunas_selecionadas_produtos = st.multiselect(
                "Colunas disponíveis (Produtos):",
                options=df.columns,
                default=get_valid_columns(column_mapping["ID"], column_mapping["Produto"], column_mapping["Unidade"], column_mapping["Ncm"], column_mapping["Cest"]),
                key="produtos_multiselect"
            )

            if colunas_selecionadas_produtos:
                novo_df_produtos = df1[colunas_selecionadas_produtos]
                st.dataframe(novo_df_produtos)
                st.markdown("---")
                if st.button("Inserir Produtos no SQL"):
                    mapping = selecionar_colunas(df1, colunas_selecionadas_produtos)
                    inserir_produtos(novo_df_produtos, mapping, "column_mappings.json")

        # Tabela de Preços
        with col2:
            st.markdown("<h2 style='text-align: center;'>Tabela de Preços</h2>", unsafe_allow_html=True)
            colunas_selecionadas_precos = st.multiselect(
                "Colunas disponíveis (Preços):",
                options=df.columns,
                default=get_valid_columns(column_mapping["ID"], column_mapping["Custo"], column_mapping["Margem"], column_mapping["Unitário"]),
                key="preco_multiselect"
            )

            if colunas_selecionadas_precos:
                novo_df_precos = df1[colunas_selecionadas_precos]
                mapping = selecionar_colunas(df1, colunas_selecionadas_precos)
                novo_df_precos1 = resolver_custos_unitario_margem(novo_df_precos, mapping, "column_mappings.json")
                st.dataframe(novo_df_precos1)
                st.markdown("---")
                if st.button("Inserir Preços no SQL"):
                    mapping = selecionar_colunas(novo_df_precos1, colunas_selecionadas_precos)
                    inserir_precos(novo_df_precos1, mapping, "column_mappings.json")

        # Tabela de Código de Barras
        with col3:
            st.markdown("<h2 style='text-align: center;'>Tabela Cód.Barras</h2>", unsafe_allow_html=True)
            colunas_selecionadas_cod_barras = st.multiselect(
                "Colunas disponíveis (Código de Barras):",
                options=df.columns,
                default=get_valid_columns(column_mapping["ID"], column_mapping["CódigoBarras"]),
                key="cod_barras_multiselect"
            )

            if colunas_selecionadas_cod_barras:
                novo_df_cod_barras = df1[colunas_selecionadas_cod_barras]
                st.dataframe(novo_df_cod_barras)
                st.markdown("---")
                if st.button("Inserir Código de Barras no SQL"):
                    mapping = selecionar_colunas(df1, colunas_selecionadas_cod_barras)
                    inserir_codigo_barras(novo_df_cod_barras, mapping, "column_mappings.json")

if __name__ == "__main__":
    main()
