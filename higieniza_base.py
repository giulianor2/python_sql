import json
import mysql.connector
from datetime import datetime
from dateutil.relativedelta import relativedelta

def read_config(file_path):
    with open(file_path) as config_file:
        return json.load(config_file)

def create_connection(config):
    return mysql.connector.connect(
        user=config['user'],
        password=config['password'],
        host=config['host'],
        database=config['database']
    )
    
def get_date():
    date_hour_current = datetime.now()
    date_hour = date_hour_current.strftime('%d/%m/%Y %H:%M:%S')
    return date_hour

def execute_query(cursor, query, params=None):
    try:
        cursor.execute(query, params)
        return True
    except mysql.connector.Error as err:
        print(f"Erro ao executar a consulta: {err}")
        log_file.write(f'--Erro ao executar a consulta: {err}\n')
        return False

def update_properties(cursor, first_result, other_results):
    placeholders = ', '.join(['%s'] * len(other_results))
    update_properties_query = "UPDATE <tabela> SET <campo> = %s WHERE <campo> IN ({placeholders})"
    update_properties_query = update_properties_query.format(placeholders=placeholders)
    update_properties_params = (first_result[0],) + tuple(other_results)

    if execute_query(cursor, update_properties_query, update_properties_params):
        print(f'  Update <tabela> {other_results}')
        return True

    return False

def update_properties_entities(cursor, first_result, other_results):
    placeholders = ', '.join(['%s'] * len(other_results))
    update_properties_entities_query = "UPDATE <tabela> SET <campo> = %s WHERE <campo> IN ({placeholders})"
    update_properties_entities_query = update_properties_entities_query.format(placeholders=placeholders)
    update_properties_entities_params = (first_result[0],) + tuple(other_results)

    if execute_query(cursor, update_properties_entities_query, update_properties_entities_params):
        print(f'  Updade <tabela> {other_results}\n')
        return True

    return False

def update_product_contracts(cursor, first_result, other_results):
    placeholders = ', '.join(['%s'] * len(other_results))
    update_product_contracts_query = "UPDATE <tabela> SET <campo> = %s WHERE <campo> IN ({placeholders})"
    update_product_contracts_query = update_product_contracts_query.format(placeholders=placeholders)
    update_product_contracts_params = (first_result[0],) + tuple(other_results)

    if execute_query(cursor, update_product_contracts_query, update_product_contracts_params):
        print(f'  Update <tabela> {other_results}')
        return True

    return False

def update_negotiation_contracts(cursor, first_result, other_results):
    placeholders = ', '.join(['%s'] * len(other_results))
    update_negotiation_contracts_query = "UPDATE <tabela> SET <campo> = %s WHERE <campo> IN ({placeholders})"
    update_negotiation_contracts_query = update_negotiation_contracts_query.format(placeholders=placeholders)
    update_negotiation_contracts_params = (first_result[0],) + tuple(other_results)

    if execute_query(cursor, update_negotiation_contracts_query, update_negotiation_contracts_params):
        print(f'  Update <tabela> {other_results}')
        return True

    return False

def delete_entities(cursor, other_results):
    placeholders = ', '.join(['%s'] * len(other_results))
    delete_entities_query = "DELETE FROM <tabela> WHERE <campo> IN ({placeholders})"
    delete_entities_query = delete_entities_query.format(placeholders=placeholders)

    if execute_query(cursor, delete_entities_query, other_results):
        print(f'  Delete <tabela> {other_results}')
        return True

    return False

def delete_contacts(cursor, other_results):
    placeholders = ', '.join(['%s'] * len(other_results))
    delete_contacts_query = "DELETE FROM <tabela> WHERE <campo> IN ({placeholders})"
    delete_contacts_query = delete_contacts_query.format(placeholders=placeholders)

    if execute_query(cursor, delete_contacts_query, other_results):
        print(f'  Delete contacts {other_results}')
        return True

    return False

def delete_addresses(cursor, other_results):
    placeholders = ', '.join(['%s'] * len(other_results))
    delete_addresses_query = "DELETE FROM <tabela> WHERE <campo> IN ({placeholders})"
    delete_addresses_query = delete_addresses_query.format(placeholders=placeholders)

    if execute_query(cursor, delete_addresses_query, other_results):
        print(f'  Delete <tabela> {other_results}')
        return True

    return False

def process_entities(config, cnpj_list):
    counter = 0

    conn = create_connection(config)
    cursor = conn.cursor()
    
    initial_date_hour = get_date()
    log_file.write(f'Hora de início: {initial_date_hour} \n\n')

    for cnpj in cnpj_list:
        counter += 1

        cursor.execute("SELECT id FROM <tabela> WHERE CpfCnpj = %s", (cnpj,))

        first_result = cursor.fetchone()

        print(f'{counter} - CPF/CNPJ: {cnpj}')
        log_file.write(f'{counter} - CPF/CNPJ: {cnpj}\n')

        if first_result:
            print(f' Primeiro Id {first_result}')
            log_file.write(f' Primeiro Id {first_result}\n')

            other_results = [row[0] for row in cursor]

            if other_results:
                if  update_properties(cursor, first_result, other_results) and \
                    log_file.write(f"  Update <tabela> {other_results} \n") and \
                    update_properties_entities(cursor, first_result, other_results) and \
                    log_file.write(f"  Update <tabela> {other_results} \n") and \
                    update_product_contracts(cursor, first_result, other_results) and \
                    log_file.write(f"  Update <tabela> {other_results} \n") and \
                    update_negotiation_contracts(cursor, first_result, other_results) and \
                    log_file.write(f"  Update <tabela> {other_results} \n") and \
                    delete_entities(cursor, other_results) and \
                    log_file.write(f"  Delete <tabela> {other_results} \n") and \
                    delete_contacts(cursor, other_results) and \
                    log_file.write(f"  Delete <tabela> {other_results} \n") and \
                    delete_addresses(cursor, other_results) and \
                    log_file.write(f"  Delete <tabela> {other_results} \n"):
                    conn.commit()
                else:
                    print("Erro ao executar as atualizações e exclusões.")
                    log_file.write(" * Erro ao executar as atualizações e exclusões.\n")
            else:
                print("Nenhum resultado encontrado.")
                log_file.write(" * Nenhum resultado encontrado.\n")
        else:
            print("Nenhum resultado encontrado na tabela.")
            log_file.write(" ** Nenhum resultado encontrado na tabela.\n")

    log_file.write(f'=======================================================================\n')

    final_date_hour = get_date()
    log_file.write(f'\nHora final: {final_date_hour} \n')

    ini = datetime.strptime(initial_date_hour, '%d/%m/%Y %H:%M:%S')
    fin = datetime.strptime(final_date_hour, '%d/%m/%Y %H:%M:%S')

    time_diff = abs(relativedelta(ini, fin))

    log_file.write(f'Tempo gasto nesta tarefa...........: Horas: {time_diff.hours}, minutos: {time_diff.minutes}, segundos: {time_diff.seconds} \n')

    cursor.close()
    conn.close()
    log_file.close()


if __name__ == '__main__':
    config_file_path = 'configdb.json'
    config = read_config(config_file_path)
    
    user = config['user']
    host_cloud = config['host']

    print(f'Usuario: {user}')
    print(f'Host: {host_cloud}')
    press_key = input('Digite <ENTER> para prosseguir.')
    
    global log_file
    log_file = open('log.txt', 'w')
    
    cnpj_list = [ ]  # Valores a serem pesquisados

    process_entities(config, cnpj_list)
