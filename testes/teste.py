import pandas as pd
import pyodbc
import streamlit as st
import socket

# Função para listar os drivers ODBC instalados no sistema
def get_drivers():
    drivers = [driver for driver in pyodbc.drivers()]
    return drivers

# Função para obter o nome do servidor (nome do PC)
def get_server():
    server_name = socket.gethostname()
    return server_name

# Função para listar os bancos de dados SQL Server disponíveis em um servidor
def get_databases(server):
    conn_str = f"Driver={{SQL Server}};Server={server};Trusted_Connection=yes;"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases;")
        databases = [row.name for row in cursor.fetchall()]
        conn.close()
        return databases
    except Exception as e:
        st.error(f"Erro ao conectar ao servidor: {e}")
        return []

# Função para tentar conectar ao banco de dados
def test_db_connection(conn_str):
    try:
        conn = pyodbc.connect(conn_str)
        conn.close()
        return True  # Conexão bem-sucedida
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return False  # Falha na conexão

# Função para listar as tabelas de um banco de dados selecionado
def get_tables(conn_str):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        st.error(f"Erro ao listar tabelas: {e}")
        return []
    
# Função para obter os dados da tabela selecionada usando pyodbc e transformá-los em DataFrame
def get_table_data(conn_str, table_name):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name};"
        cursor.execute(query)

        # Obter os nomes das colunas da tabela
        columns = [desc[0] for desc in cursor.description]

        # Buscar todos os dados da tabela
        rows = cursor.fetchall()

        # Fechar a conexão
        conn.close()

        # Converter os resultados em um DataFrame do pandas
        df = pd.DataFrame.from_records(rows, columns=columns)

        return df
    except Exception as e:
        st.error(f"Erro ao obter dados da tabela: {e}")
        return pd.DataFrame() 

# Função para listar as colunas de uma tabela selecionada
def get_columns(conn_str, table_name):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';")
        columns = [row.COLUMN_NAME for row in cursor.fetchall()]
        conn.close()
        return columns
    except Exception as e:
        st.error(f"Erro ao listar colunas: {e}")
        return []    

# Configurações do Streamlit
st.set_page_config(page_title="Importação de Dados(Processo ETL)", page_icon="🛠️", layout="wide")

# Inicializa o estado da conexão, se ainda não foi inicializado
if 'db_connected' not in st.session_state:
    st.session_state['db_connected'] = False

# Inicializa a string de conexão no estado da sessão
if 'dados_conexao' not in st.session_state:
    st.session_state['dados_conexao'] = None    

# Inicializa o estado da conexão, se ainda não foi inicializado
if 'db_connected2' not in st.session_state:
    st.session_state['db_connected2'] = False

# Inicializa a string de conexão no estado da sessão
if 'dados_conexao2' not in st.session_state:
    st.session_state['dados_conexao2'] = None   

coll1, coll2 = st.columns([1, 1])  
col1, col2 = st.columns([1, 1])  

with coll1:
 # Layout da aplicação no Streamlit
 st.title("Conexão com SQL")

 # Selectbox para listar os drivers instalados
 drivers = get_drivers()
 selected_driver = st.selectbox("Selecione o Driver ODBC", drivers, key="driver")

 # Selectbox para o servidor (neste caso o nome do PC)
 server = get_server()
 selected_server = st.selectbox("Selecione o Servidor", [server], key="server")

 # Selectbox para listar os bancos de dados do servidor selecionado
 databases = get_databases(selected_server)
 selected_database = st.selectbox("Selecione o Banco de Dados", databases, key="database")

 # Indicador de status inicial (não conectado)
 st.markdown("### Status da Conexão")
 connection_status = st.empty()  # Placeholder para o status

 if st.session_state['db_connected']:
    connection_status.markdown("✅ **Banco de dados conectado**", unsafe_allow_html=True)
 else:
    connection_status.markdown("❌ **Banco de dados não conectado**", unsafe_allow_html=True)

        # Botão para realizar a conexão
 if st.button("Conectar ao Banco de Dados"):
    # Gerar a string de conexão
    st.session_state['dados_conexao'] = (
         f"Driver={{{selected_driver}}};"
         f"Server={selected_server};"
         f"Database={selected_database};"
         "Trusted_Connection=yes;"
    )
    
    # Testar a conexão
    if test_db_connection(st.session_state['dados_conexao']):
        # Se a conexão for bem-sucedida, exibir o status em verde
        st.session_state['db_connected'] = True
        connection_status.markdown("✅ **Banco de dados conectado**", unsafe_allow_html=True)
    else:
        # Se a conexão falhar, continuar exibindo o status em vermelho
        st.session_state['db_connected'] = False
        connection_status.markdown("❌ **Falha na conexão ao banco de dados**", unsafe_allow_html=True) 
 
 def main():
       with col1: 

        #st.markdown("""---""")

        #st.markdown("<h1 style='text-align: center;'>Importação de Dados(Via Processo ETL)</h1>", unsafe_allow_html=True)

        #st.markdown("""---""")
        
         # Selectbox para selecionar uma tabela após a conexão
        if st.session_state['db_connected']:
            tables = get_tables(st.session_state['dados_conexao'])
            selected_table = st.selectbox("Selecione uma Tabela", tables,key="table")

            if selected_table:
            # Carregar os dados da tabela
             table_data = get_table_data(st.session_state['dados_conexao'], selected_table)
            
            # Selecionar as colunas da tabela
            columns = get_columns(st.session_state['dados_conexao'], selected_table)
            selected_columns = st.multiselect("Selecione as Colunas", columns,key="columns")
            
            # Gerar o DataFrame dinamicamente baseado nas colunas selecionadas
            if selected_columns:
                df_selected_columns = table_data[selected_columns]
                st.dataframe(df_selected_columns)  

 with coll2:
   # Layout da aplicação no Streamlit
   st.title("Conexão com SQL 2")

   # Selectbox para listar os drivers instalados
   drivers2 = get_drivers()
   selected_driver2 = st.selectbox("Selecione o Driver ODBC", drivers2, key="driver2")

   # Selectbox para o servidor (neste caso o nome do PC)
   server2 = get_server()
   selected_server2 = st.selectbox("Selecione o Servidor", [server2],  key="server2")

   # Selectbox para listar os bancos de dados do servidor selecionado
   databases2 = get_databases(selected_server)
   selected_database2 = st.selectbox("Selecione o Banco de Dados", databases2, key="database2")

   # Indicador de status inicial (não conectado)
   st.markdown("### Status da Conexão 2")
   connection_status2 = st.empty()  # Placeholder para o status 

   # Exibir o status baseado no valor de st.session_state
   if st.session_state['db_connected2']:
       connection_status2.markdown("✅ **Banco de dados 2 conectado**", unsafe_allow_html=True)
   else:
       connection_status2.markdown("❌ **Banco de dados 2 não conectado**", unsafe_allow_html=True)

   # Botão para realizar a conexão
   if st.button("Conectar ao Banco de Dados 2"):
      # Gerar a string de conexão
      st.session_state['dados_conexao2'] = (
        f"Driver={{{selected_driver2}}};"
        f"Server={selected_server2};"
        f"Database={selected_database2};"
        "Trusted_Connection=yes;"
      )
    
      # Testar a conexão
      if test_db_connection(st.session_state['dados_conexao2']):
         # Se a conexão for bem-sucedida, exibir o status em verde
         st.session_state['db_connected2'] = True
         connection_status2.markdown("✅ **Banco de dados 2 conectado**", unsafe_allow_html=True)
      else:
         # Se a conexão falhar, continuar exibindo o status em vermelho
         st.session_state['db_connected2'] = False
         connection_status2.markdown("❌ **Falha na conexão ao banco de dados 2**", unsafe_allow_html=True) 

   with col2:
     # Selectbox para selecionar uma tabela após a conexão
     if st.session_state['db_connected2']:
        tables2 = get_tables(st.session_state['dados_conexao2'])
        selected_table2 = st.selectbox("Selecione uma Tabela", tables2, key="table2")

        if selected_table2:
           # Carregar os dados da tabela
           table_data2 = get_table_data(st.session_state['dados_conexao2'], selected_table2)
            
           # Selecionar as colunas da tabela
           columns2 = get_columns(st.session_state['dados_conexao2'], selected_table2)
           selected_columns2 = st.multiselect("Selecione as Colunas", columns2, key="columns2")
            
           # Gerar o DataFrame dinamicamente baseado nas colunas selecionadas
           if selected_columns2:
              df_selected_columns2 = table_data2[selected_columns2]
              st.dataframe(df_selected_columns2)               

st.markdown("""---""")       

st.markdown("""---""")   

if __name__ == "__main__":
    main()
