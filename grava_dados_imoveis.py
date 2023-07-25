import openpyxl
import mysql.connector

global linhas 

# Configurações do banco de dados
db_config = {
    'user': 'user',
    'password': 'password',
    'host': 'localhost',
    'database': 'banco_dados'
}


# Função para verificar se um registro está cadastrado no banco de dados
def verificar_registro_cadastrado(registro1, registro2):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Query para verificar o registro no banco de dados
        query = "select count(*) from imoveis where latitude = %s and longitude = %s"
        cursor.execute(query, (registro1, registro2))

        result = cursor.fetchone()[0]

        return result > 0  # Retorna True se o registro estiver cadastrado, False caso contrário

    except mysql.connector.Error as error:
        print(f"Erro ao verificar registro no banco de dados: {error}")
        return False

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Função para inserir os dados no banco de dados
def inserir_dados(dados):
    global reg_db
    global reg_not_db
    
    cont = 0
    
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Query de inserção
        query = "insert into imoveis (titulo, imobiliaria, endereco, numero, cep, bairro, cidade, uf, area_metragem, nr_quartos, nr_banheiros, vr_iptu, vr_aluguel, vr_metro, latitude, longitude, tempo_anuncio, link_anuncio) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        # Inserindo os dados linha por linha
        for linha in dados:
            cont = cont + 1
            
            print(f'Linha:  {cont} ')
            print(linha)
            
            #if linha[14] != 0:
            #    registro_cadastrado = verificar_registro_cadastrado(linha[14], linha[15])
            
            #if not registro_cadastrado:
            reg_not_db += 1
            cursor.execute(query, linha)
            #else:
            #    reg_db += 1

        connection.commit()
        print("Dados inseridos com sucesso!")
    except mysql.connector.Error as error:
        print(f"Erro ao inserir dados no banco de dados: {error}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Função para ler o arquivo XLSX
def ler_arquivo_xlsx(nome_arquivo):
    global linhas
    linhas = 0
    
    try:
        workbook = openpyxl.load_workbook(nome_arquivo)
        sheet = workbook.active

        # Ignorando o cabeçalho da planilha
        dados = []
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            dados.append(row)
            linhas = linhas + 1

        #print(dados[0])

        return dados
    except FileNotFoundError:
        print("Arquivo não encontrado.")
        return []
    except openpyxl.Error as error:
        print(f"Erro ao ler arquivo XLSX: {error}")
        return []

# Nome do arquivo XLSX
nome_arquivo = 'relatorio.xlsx'

# Lendo o arquivo XLSX
dados = ler_arquivo_xlsx(nome_arquivo)

global reg_db
global reg_not_db

reg_db = 0
reg_not_db = 0

if dados:
    inserir_dados(dados)

print(f'Total de registros cadastrados...: {reg_not_db}')
print(f'Total de registros já cadastrados: {reg_db}')
