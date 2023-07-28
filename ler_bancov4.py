import mysql.connector
import csv


def conectar_banco_dados():
    host = ''
    usuario = ''
    senha = ''
    database = ''

    # Conectando ao banco de dados
    conexao = mysql.connector.connect(
        host=host,
        user=usuario,
        password=senha,
        database=database
    )
    return conexao


def localiza_cidade(conexao, nome_cidade):
    # Id que será retornado
    id_retorno = 0
     
    # Criando o cursor
    cursor = conexao.cursor()

    # Definindo os valores dos parâmetros
    parametros = (nome_cidade,) # Adicionada uma vírgula aqui para criar uma tupla com um único elemento

    # Executando o SELECT SQL com os parâmetros
    consulta_sql = """
        SELECT Id FROM cities WHERE Name = %s
    """
    
    cursor.execute(consulta_sql, parametros)
    
    resultados = cursor.fetchall()
    cursor.close()
    
    if resultados:
        id_cidade = resultados[0][0]  # Acessando o valor do ID da cidade (primeiro elemento da primeira tupla)
        id_retorno = id_cidade
    else:
        id_retorno = 0

    return id_retorno


def executar_sql_endereco(conexao, street, number, cities_id):
    cursor = conexao.cursor()

    parametros = (cities_id, street, number)
    
    consulta_sql = """
        SELECT Lat, Lng 
            FROM properties 
            WHERE CitiesId = %s
              AND Street = %s
              AND Number = %s
            LIMIT 1
    """

    cursor.execute(consulta_sql, parametros)

    # Obtendo o primeiro resultado (se existir)
    resultados = cursor.fetchone()

    # Fechando o cursor
    cursor.close()
    
    return resultados


def executar_sql_com_parametro(conexao, latitude, longitude, latitudex, raio):
    # Criando o cursor
    cursor = conexao.cursor()

    # Definindo os valores dos parâmetros
    parametros = (latitude, longitude, latitudex, raio)
    
    # Executando o SELECT SQL com os parâmetros
    consulta_sql = """
        SELECT pr.id, pre.EntitiesId 'Proprietário', en.Name 'Nome proprietário', concat(pr.Street, ',', pr.Number) as 'Endereço', pr.ZipCode 'CEP', pr.CitiesId 'Id Cidade', ci.Name 'Nome Cidade', st.UF 'UF', 
        replace(replace(replace(format(pr.Lat, 7), '.', '|'), ',', '.'), '|', ',') as 'Latitude', 
        replace(replace(replace(format(pr.Lng, 7), '.', '|'), ',', '.'), '|', ',') as 'Longitude', 
        cast(pr.Rent as decimal(10, 2)) as 'Vr. Aluguel', 
        cast(pr.Iptu as decimal(10, 2)) as 'Vr. IPTU', 
        cast(pr.AllotmentArea as decimal(10, 2)) as 'Area', 
        cast(pr.M2Value as decimal(10, 2)) as 'Vr. M2', 
        pr.PropertyTypesId 'Tipo imóvel', 
        ptyp.Name 'Descrição tipo imóvel', 
        (6371 * ACOS(COS(RADIANS(%s)) * COS(RADIANS(pr.Lat)) * COS(RADIANS(%s) - RADIANS(pr.Lng)) + SIN(RADIANS(%s)) * SIN(RADIANS(pr.Lat)))) Distancia 
        FROM properties as pr 
        JOIN cities ci ON ci.Id = pr.CitiesId 
        JOIN states st ON st.Id = ci.StatesId 
        JOIN property_types ptyp ON ptyp.Id = pr.PropertyTypesId 
        JOIN properties_entities pre ON pre.PropertiesId = pr.Id 
        JOIN negotiation_contracts ngc ON ngc.PropertiesId = pr.id 
        JOIN negotiations ng ON ng.NegotiationContractsId = ngc.id 
        JOIN product_contracts pc ON pc.Id = ng.ProductContractsId 
        JOIN entities en ON en.id = pc.EntitiesId 
        WHERE (pr.Lat IS NOT NULL OR pr.Lng IS NOT NULL) 
        AND pr.PropertyTypesId NOT IN (4, 99999, 100001) 
        HAVING Distancia <= %s  
    """

    cursor.execute(consulta_sql, parametros)

    # Obtendo os resultados
    resultados = cursor.fetchall()

    # Fechando o cursor
    cursor.close()

    # Obtendo os nomes das colunas para usar como cabeçalho no CSV
    colunas = [i[0] for i in cursor.description]

    return resultados, colunas


def salvar_resultados_csv(resultados, cabecalho):
    nome_arquivo_csv = "resultados.csv"

    # Escrevendo os resultados no arquivo .csv
    with open(nome_arquivo_csv, mode='w', newline='') as arquivo_csv:
        escritor_csv = csv.writer(arquivo_csv)

        escritor_csv.writerow(cabecalho)

        for linha in resultados:
            escritor_csv.writerow(linha)


def recebe_cidade(nome_cidade):
    conexao = conectar_banco_dados()
    id_cidade = localiza_cidade(conexao, nome_cidade)
    conexao.close()

    if id_cidade == 0:
        print('Cidade não encontrada')
        quit

    return(id_cidade)


def recebe_parametros():
    raio = float(input("             Digite o valor do raio em KM: "))
    street = input("                     Digite o nome da rua: ")
    number = int(input("                Digite o número do imóvel: "))
    print('=' * 100 )
    cities_id = id_retorno_cidade
    print(f"O ID da cidade {nome_cidade} é: {cities_id}")

    # Criar uma função para ler o banco com os dados de endereço e retornar a latitude e longitude
    conexao = conectar_banco_dados()
    resultados = executar_sql_endereco(conexao, street, number, cities_id)
    conexao.close()

    latitude = resultados[0]
    longitude = resultados[1]

    # Essa leitura ao banco é para retornar os imóveis próximos (usar como referência a latitude e longitude)
    conexao = conectar_banco_dados()
    resultados, cabecalho =  executar_sql_com_parametro(conexao, latitude, longitude, latitude, raio)
    conexao.close()

    salvar_resultados_csv(resultados, cabecalho)
    print("Resultados salvos em resultados.csv")


# Inicio do programa
if __name__ == "__main__":
    nome_cidade = input("Digite o nome da cidade (incluir acentos): ")

    id_retorno_cidade = recebe_cidade(nome_cidade)
    recebe_parametros()
